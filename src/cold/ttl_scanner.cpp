#include "abyss/cold/ttl_scanner.h"

#include <algorithm>
#include <cmath>
#include <system_error>
#include <utility>

#ifdef _WIN32
#include <windows.h>
#else
#include <pthread.h>

#include <ctime>
#endif

#include "abyss/log/log.h"
#include "abyss/metrics/names.h"

ABYSS_LOG_COMPONENT("abyss.cold.ttl_scanner")

namespace abyss::cold {

core::Result<double> DefaultDiskUsage(const std::filesystem::path& path) {
  std::error_code ec;
  const auto info = std::filesystem::space(path, ec);
  if (ec) {
    return std::unexpected(
        core::Error{core::ErrorCode::kUnavailable,
                    "filesystem::space(" + path.string() + ") failed: " + ec.message()});
  }
  if (info.capacity == 0) return 0.0;
  const auto used = static_cast<double>(info.capacity - info.available);
  return used / static_cast<double>(info.capacity);
}

std::chrono::nanoseconds DefaultThreadCpuClock() {
#ifdef _WIN32
  FILETIME creation;
  FILETIME exit;
  FILETIME kernel;
  FILETIME user;
  if (GetThreadTimes(GetCurrentThread(), &creation, &exit, &kernel, &user) == 0) {
    return std::chrono::nanoseconds{0};
  }
  ULARGE_INTEGER user_time;
  user_time.LowPart = user.dwLowDateTime;
  user_time.HighPart = user.dwHighDateTime;
  return std::chrono::nanoseconds{static_cast<int64_t>(user_time.QuadPart) * 100};
#else
  struct timespec ts{};
  if (clock_gettime(CLOCK_THREAD_CPUTIME_ID, &ts) != 0) return std::chrono::nanoseconds{0};
  return std::chrono::seconds{ts.tv_sec} + std::chrono::nanoseconds{ts.tv_nsec};
#endif
}

namespace {

core::Error InvalidArg(std::string message) {
  return core::Error{core::ErrorCode::kInvalidArgument, std::move(message)};
}

core::Result<void> ValidateConfig(const TtlScanner::Config& cfg) {
  if (cfg.high_threshold <= cfg.low_threshold) {
    return std::unexpected(InvalidArg("ttl_scanner: high_threshold must exceed low_threshold"));
  }
  if (cfg.high_threshold < 0.0 || cfg.high_threshold > 1.0) {
    return std::unexpected(InvalidArg("ttl_scanner: high_threshold must be in [0, 1]"));
  }
  if (cfg.low_threshold < 0.0 || cfg.low_threshold > 1.0) {
    return std::unexpected(InvalidArg("ttl_scanner: low_threshold must be in [0, 1]"));
  }
  if (cfg.disk_pressure_release_threshold > cfg.disk_pressure_threshold) {
    return std::unexpected(InvalidArg(
        "ttl_scanner: disk_pressure_release_threshold must be <= disk_pressure_threshold"));
  }
  if (cfg.disk_pressure_threshold < 0.0 || cfg.disk_pressure_threshold > 1.0) {
    return std::unexpected(InvalidArg("ttl_scanner: disk_pressure_threshold must be in [0, 1]"));
  }
  if (cfg.disk_pressure_release_threshold < 0.0 || cfg.disk_pressure_release_threshold > 1.0) {
    return std::unexpected(
        InvalidArg("ttl_scanner: disk_pressure_release_threshold must be in [0, 1]"));
  }
  if (cfg.base_sample_size == 0 || cfg.min_sample_size == 0) {
    return std::unexpected(InvalidArg("ttl_scanner: sample sizes must be > 0"));
  }
  if (cfg.min_sample_size > cfg.base_sample_size || cfg.base_sample_size > cfg.max_sample_size) {
    return std::unexpected(
        InvalidArg("ttl_scanner: sample size bounds must satisfy min <= base <= max"));
  }
  if (cfg.min_interval.count() <= 0 || cfg.base_interval.count() <= 0 ||
      cfg.max_interval.count() <= 0) {
    return std::unexpected(InvalidArg("ttl_scanner: intervals must be > 0"));
  }
  if (cfg.min_interval > cfg.base_interval || cfg.base_interval > cfg.max_interval) {
    return std::unexpected(
        InvalidArg("ttl_scanner: interval bounds must satisfy min <= base <= max"));
  }
  if (cfg.rate_increase_factor <= 1.0) {
    return std::unexpected(InvalidArg("ttl_scanner: rate_increase_factor must be > 1.0"));
  }
  if (cfg.rate_decrease_factor <= 0.0 || cfg.rate_decrease_factor >= 1.0) {
    return std::unexpected(InvalidArg("ttl_scanner: rate_decrease_factor must be in (0, 1)"));
  }
  if (cfg.max_cpu_fraction <= 0.0 || cfg.max_cpu_fraction > 1.0) {
    return std::unexpected(InvalidArg("ttl_scanner: max_cpu_fraction must be in (0, 1]"));
  }
  if (cfg.cpu_ewma_window.count() <= 0) {
    return std::unexpected(InvalidArg("ttl_scanner: cpu_ewma_window must be > 0"));
  }
  return {};
}

}  // namespace

core::Result<std::unique_ptr<TtlScanner>> TtlScanner::Create(TtlScannerBackend& backend,
                                                             Config config, ExecutionMode mode,
                                                             Hooks hooks) {
  if (auto v = ValidateConfig(config); !v.has_value()) return std::unexpected(v.error());
  if (!hooks.disk_usage) {
    return std::unexpected(InvalidArg("ttl_scanner: disk_usage hook is required"));
  }
  if (!hooks.cpu_clock) hooks.cpu_clock = DefaultThreadCpuClock;
  if (!hooks.steady_clock) hooks.steady_clock = core::DefaultSteadyClock;

  return std::unique_ptr<TtlScanner>(new TtlScanner(backend, config, mode, std::move(hooks)));
}

TtlScanner::TtlScanner(TtlScannerBackend& backend, Config config, ExecutionMode mode, Hooks hooks)
    : backend_(backend), config_(config), mode_(mode), hooks_(std::move(hooks)) {
  RecomputeDerived();

  auto& reg = metrics::Registry::Instance();
  for (auto subject : {metrics::TtlSubject::kString, metrics::TtlSubject::kCollection}) {
    const auto idx = static_cast<size_t>(subject);
    samples_total_.at(idx) = reg.Counter(metrics::names::kColdTtlSamplesTotal, subject);
    with_ttl_total_.at(idx) = reg.Counter(metrics::names::kColdTtlWithTtlTotal, subject);
    expired_total_.at(idx) = reg.Counter(metrics::names::kColdTtlExpiredTotal, subject);
    deleted_total_.at(idx) = reg.Counter(metrics::names::kColdTtlDeletedTotal, subject);
    conflicts_total_.at(idx) = reg.Counter(metrics::names::kColdTtlConflictsTotal, subject);
  }
  interval_ms_gauge_ = reg.Gauge(metrics::names::kColdTtlIntervalMs);
  sample_size_gauge_ = reg.Gauge(metrics::names::kColdTtlSampleSize);
  rate_multiplier_gauge_ = reg.Gauge(metrics::names::kColdTtlRateMultiplier);
  cpu_fraction_gauge_ = reg.Gauge(metrics::names::kColdTtlCpuFraction);
  disk_pressure_fraction_gauge_ = reg.Gauge(metrics::names::kColdTtlDiskPressureFraction);
  disk_pressure_active_gauge_ = reg.Gauge(metrics::names::kColdTtlDiskPressureActive);

  sample_size_gauge_.Set(static_cast<double>(sample_size_));
  interval_ms_gauge_.Set(static_cast<double>(interval_.count()));
  rate_multiplier_gauge_.Set(rate_multiplier_);
}

TtlScanner::~TtlScanner() { Stop(); }

void TtlScanner::Start() {
  if (!config_.enabled) return;
  if (running_.exchange(true)) return;
  stop_requested_.store(false);
  {
    const std::scoped_lock lock(state_mu_);
    last_tick_start_.reset();
  }
  if (mode_ == ExecutionMode::kOwnedThread) {
    thread_ = std::thread([this] { ThreadLoop(); });
  }
}

void TtlScanner::Stop() {
  if (!running_.exchange(false)) return;
  stop_requested_.store(true);
  if (mode_ == ExecutionMode::kOwnedThread) {
    {
      const std::scoped_lock lock(wake_mu_);
      wake_cv_.notify_all();
    }
    if (thread_.joinable()) thread_.join();
  }
}

core::Result<SweepReport> TtlScanner::Tick() {
  if (!config_.enabled) return SweepReport{};

  size_t sample_size_now = 0;
  {
    const std::scoped_lock lock(state_mu_);
    sample_size_now = sample_size_;
  }

  const auto cpu_start = hooks_.cpu_clock();
  auto report_or = backend_.SampleAndExpire({.sample_size = sample_size_now});
  const auto cpu_end = hooks_.cpu_clock();

  if (!report_or.has_value()) {
    return std::unexpected(report_or.error());
  }

  const auto now = hooks_.steady_clock();
  std::chrono::milliseconds wall_elapsed{0};
  {
    const std::scoped_lock lock(state_mu_);
    if (last_tick_start_.has_value()) {
      wall_elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(now - *last_tick_start_);
    }
    last_tick_start_ = now;
  }

  UpdateRateState(*report_or, cpu_end - cpu_start, wall_elapsed);
  return *report_or;
}

void TtlScanner::ThreadLoop() {
#ifdef __APPLE__
  pthread_setname_np("abyss-ttl-sweep");
#elifdef __linux__
  pthread_setname_np(pthread_self(), "abyss-ttl-sweep");
#endif

  ABYSS_LOG_INFO("ttl scanner started",
                 {"base_sample_size", static_cast<int64_t>(config_.base_sample_size)},
                 {"base_interval_ms", static_cast<int64_t>(config_.base_interval.count())},
                 {"max_cpu_fraction", config_.max_cpu_fraction});

  while (!stop_requested_.load(std::memory_order_acquire)) {
    auto r = Tick();
    if (!r.has_value()) {
      ABYSS_LOG_WARN("ttl scanner tick failed", {"err", std::string_view{r.error().message()}});
    }

    std::chrono::milliseconds interval_now{0};
    {
      const std::scoped_lock lock(state_mu_);
      interval_now = interval_;
    }

    std::unique_lock<std::mutex> lock(wake_mu_);
    wake_cv_.wait_for(lock, interval_now,
                      [this] { return stop_requested_.load(std::memory_order_acquire); });
  }

  ABYSS_LOG_INFO("ttl scanner stopped");
}

void TtlScanner::UpdateRateState(const SweepReport& report, std::chrono::nanoseconds cpu_used,
                                 std::chrono::milliseconds wall_elapsed) {
  // Read disk usage outside the lock — it may do filesystem I/O.
  double new_disk_fraction = 0.0;
  if (auto disk = hooks_.disk_usage(); disk.has_value()) {
    new_disk_fraction = *disk;
  }

  bool prev_disk_pressure = false;
  bool transitioned_into_pressure = false;
  bool transitioned_out_of_pressure = false;

  const std::scoped_lock lock(state_mu_);

  prev_disk_pressure = disk_pressure_active_;
  bool new_disk_pressure = false;
  if (prev_disk_pressure) {
    new_disk_pressure = new_disk_fraction >= config_.disk_pressure_release_threshold;
  } else {
    new_disk_pressure = new_disk_fraction >= config_.disk_pressure_threshold;
  }
  disk_pressure_active_ = new_disk_pressure;
  disk_pressure_fraction_ = new_disk_fraction;
  transitioned_into_pressure = !prev_disk_pressure && new_disk_pressure;
  transitioned_out_of_pressure = prev_disk_pressure && !new_disk_pressure;

  // CPU EWMA: alpha is bounded by tick / window, so a long sleep doesn't
  // collapse the history into a single sample.
  if (wall_elapsed.count() > 0) {
    const double instantaneous = static_cast<double>(cpu_used.count()) /
                                 (static_cast<double>(wall_elapsed.count()) * 1'000'000.0);
    const double tick_seconds = static_cast<double>(wall_elapsed.count()) / 1000.0;
    const auto window_seconds = static_cast<double>(config_.cpu_ewma_window.count());
    const double alpha = std::min(1.0, tick_seconds / window_seconds);
    cpu_fraction_ewma_ = ((1.0 - alpha) * cpu_fraction_ewma_) + (alpha * instantaneous);
  }

  // Adaptive rate from the expired ratio. Bounds derive from the configured
  // sample-size envelope so a single multiplier drives both knobs.
  const auto sampled = report.total_sampled();
  const auto expired = report.total_expired();
  const double r_min =
      static_cast<double>(config_.min_sample_size) / static_cast<double>(config_.base_sample_size);
  const double r_max =
      static_cast<double>(config_.max_sample_size) / static_cast<double>(config_.base_sample_size);
  if (sampled > 0) {
    const double ratio = static_cast<double>(expired) / static_cast<double>(sampled);
    if (ratio > config_.high_threshold) {
      rate_multiplier_ = std::min(rate_multiplier_ * config_.rate_increase_factor, r_max);
    } else if (ratio < config_.low_threshold) {
      rate_multiplier_ = std::max(rate_multiplier_ * config_.rate_decrease_factor, r_min);
    }
  }

  if (disk_pressure_active_) {
    rate_multiplier_ = r_max;
  }

  RecomputeDerived();

  // CPU budget: stretch the next interval if the EWMA is over the cap, unless
  // disk pressure is forcing aggressive scanning.
  if (!disk_pressure_active_ && cpu_fraction_ewma_ > config_.max_cpu_fraction &&
      cpu_used.count() > 0) {
    const auto cpu_used_ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(cpu_used).count();
    const double scaled = static_cast<double>(cpu_used_ms) / config_.max_cpu_fraction;
    const auto floor = std::chrono::milliseconds{static_cast<int64_t>(std::ceil(scaled))};
    interval_ = std::max(interval_, floor);
    if (interval_ > config_.max_interval) interval_ = config_.max_interval;
  }

  ++total_ticks_;
  total_sampled_ += sampled;
  total_with_ttl_ += report.total_with_ttl();
  total_expired_ += expired;
  total_deleted_ += report.total_deleted();
  total_conflicts_ += report.total_conflicts();

  const auto str_idx = static_cast<size_t>(metrics::TtlSubject::kString);
  const auto col_idx = static_cast<size_t>(metrics::TtlSubject::kCollection);
  samples_total_.at(str_idx).Increment(static_cast<double>(report.sampled_strings));
  samples_total_.at(col_idx).Increment(static_cast<double>(report.sampled_collections));
  with_ttl_total_.at(str_idx).Increment(static_cast<double>(report.with_ttl_strings));
  with_ttl_total_.at(col_idx).Increment(static_cast<double>(report.with_ttl_collections));
  expired_total_.at(str_idx).Increment(static_cast<double>(report.expired_strings));
  expired_total_.at(col_idx).Increment(static_cast<double>(report.expired_collections));
  deleted_total_.at(str_idx).Increment(static_cast<double>(report.deleted_strings));
  deleted_total_.at(col_idx).Increment(static_cast<double>(report.deleted_collections));
  conflicts_total_.at(str_idx).Increment(static_cast<double>(report.conflicts_strings));
  conflicts_total_.at(col_idx).Increment(static_cast<double>(report.conflicts_collections));
  interval_ms_gauge_.Set(static_cast<double>(interval_.count()));
  sample_size_gauge_.Set(static_cast<double>(sample_size_));
  rate_multiplier_gauge_.Set(rate_multiplier_);
  cpu_fraction_gauge_.Set(cpu_fraction_ewma_);
  disk_pressure_fraction_gauge_.Set(disk_pressure_fraction_);
  disk_pressure_active_gauge_.Set(disk_pressure_active_ ? 1.0 : 0.0);

  if (transitioned_into_pressure) {
    ABYSS_LOG_WARN("ttl scanner entering disk-pressure mode", {"fraction", new_disk_fraction},
                   {"threshold", config_.disk_pressure_threshold});
  } else if (transitioned_out_of_pressure) {
    ABYSS_LOG_INFO("ttl scanner exited disk-pressure mode", {"fraction", new_disk_fraction});
  }
}

void TtlScanner::RecomputeDerived() {
  const double size_d = static_cast<double>(config_.base_sample_size) * rate_multiplier_;
  const auto size_clamped = std::clamp<double>(size_d, static_cast<double>(config_.min_sample_size),
                                               static_cast<double>(config_.max_sample_size));
  sample_size_ = static_cast<size_t>(std::round(size_clamped));

  const double interval_ms_d =
      static_cast<double>(config_.base_interval.count()) / rate_multiplier_;
  const auto interval_clamped =
      std::clamp<double>(interval_ms_d, static_cast<double>(config_.min_interval.count()),
                         static_cast<double>(config_.max_interval.count()));
  interval_ = std::chrono::milliseconds{static_cast<int64_t>(std::round(interval_clamped))};
}

TtlScanner::Snapshot TtlScanner::CurrentSnapshot() const {
  const std::scoped_lock lock(state_mu_);
  return Snapshot{
      .running = running_.load(std::memory_order_acquire),
      .disk_pressure_active = disk_pressure_active_,
      .disk_pressure_fraction = disk_pressure_fraction_,
      .rate_multiplier = rate_multiplier_,
      .sample_size = sample_size_,
      .interval = interval_,
      .cpu_fraction = cpu_fraction_ewma_,
      .total_ticks = total_ticks_,
      .total_sampled = total_sampled_,
      .total_with_ttl = total_with_ttl_,
      .total_expired = total_expired_,
      .total_deleted = total_deleted_,
      .total_conflicts = total_conflicts_,
  };
}

}  // namespace abyss::cold
