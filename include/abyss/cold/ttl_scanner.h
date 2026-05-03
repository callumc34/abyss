#pragma once

#include <array>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <thread>

#include "abyss/core/result.h"
#include "abyss/core/types.h"
#include "abyss/metrics/metrics.h"

namespace abyss::cold {

// Per-tick request handed to the backend.
struct SweepRequest {
  size_t sample_size = 20;
};

// Per-tick outcome aggregated across the samples drawn in this cycle.
struct SweepReport {
  size_t sampled_strings = 0;
  size_t sampled_collections = 0;
  size_t with_ttl_strings = 0;
  size_t with_ttl_collections = 0;
  size_t expired_strings = 0;
  size_t expired_collections = 0;
  size_t deleted_strings = 0;
  size_t deleted_collections = 0;
  size_t conflicts_strings = 0;
  size_t conflicts_collections = 0;

  size_t total_sampled() const { return sampled_strings + sampled_collections; }
  size_t total_with_ttl() const { return with_ttl_strings + with_ttl_collections; }
  size_t total_expired() const { return expired_strings + expired_collections; }
  size_t total_deleted() const { return deleted_strings + deleted_collections; }
  size_t total_conflicts() const { return conflicts_strings + conflicts_collections; }
};

// Backend abstraction the scanner calls into for the actual sample-and-expire
// work. Each cold-store backend that wants active expiry implements this.
// The scanner is RocksDB-agnostic; backend-specific details (random sampling,
// CAS-safe deletion) live behind this interface.
class TtlScannerBackend {
 public:
  TtlScannerBackend() = default;
  virtual ~TtlScannerBackend() = default;
  TtlScannerBackend(const TtlScannerBackend&) = delete;
  TtlScannerBackend& operator=(const TtlScannerBackend&) = delete;
  TtlScannerBackend(TtlScannerBackend&&) = delete;
  TtlScannerBackend& operator=(TtlScannerBackend&&) = delete;

  virtual core::Result<SweepReport> SampleAndExpire(SweepRequest req) = 0;
};

// Returns the fraction of the filesystem under `path` that is in use, in
// [0, 1]. Default impl uses std::filesystem::space.
using DiskUsageFn = std::function<core::Result<double>()>;

// Returns the calling thread's cumulative CPU time (user). Used to enforce
// the scanner's CPU budget. Default impl uses CLOCK_THREAD_CPUTIME_ID on
// POSIX and GetThreadTimes on Windows.
using CpuClockFn = std::function<std::chrono::nanoseconds()>;

core::Result<double> DefaultDiskUsage(const std::filesystem::path& path);
std::chrono::nanoseconds DefaultThreadCpuClock();

// Periodic sweeper for cold-store TTL expiry. Drives a backend's
// SampleAndExpire on an adaptive cadence with CPU budget and disk-pressure
// override. Two execution modes:
//   - kOwnedThread: spawns its own thread on Start(), joins on Stop(). Used
//     by the embedded RocksDB backend in Phase 1.
//   - kManualTick: caller drives Tick() on its own schedule. Used by tests
//     and by per-core reactors in the shared-nothing execution model.
class TtlScanner {
 public:
  enum class ExecutionMode : uint8_t { kOwnedThread, kManualTick };

  struct Config {
    bool enabled = true;
    size_t base_sample_size = 20;
    size_t min_sample_size = 5;
    size_t max_sample_size = 200;
    std::chrono::milliseconds base_interval{1000};
    std::chrono::milliseconds min_interval{100};
    std::chrono::milliseconds max_interval{60'000};
    double high_threshold = 0.25;
    double low_threshold = 0.05;
    double rate_increase_factor = 1.5;
    double rate_decrease_factor = 0.7;
    double disk_pressure_threshold = 0.9;
    double disk_pressure_release_threshold = 0.855;
    double max_cpu_fraction = 0.10;
    std::chrono::seconds cpu_ewma_window{30};
  };

  struct Hooks {
    core::SteadyClockFn steady_clock = core::DefaultSteadyClock;
    DiskUsageFn disk_usage;
    CpuClockFn cpu_clock = DefaultThreadCpuClock;
  };

  struct Snapshot {
    bool running = false;
    bool disk_pressure_active = false;
    double disk_pressure_fraction = 0.0;
    double rate_multiplier = 1.0;
    size_t sample_size = 0;
    std::chrono::milliseconds interval{0};
    double cpu_fraction = 0.0;

    uint64_t total_ticks = 0;
    uint64_t total_sampled = 0;
    uint64_t total_with_ttl = 0;
    uint64_t total_expired = 0;
    uint64_t total_deleted = 0;
    uint64_t total_conflicts = 0;
  };

  static core::Result<std::unique_ptr<TtlScanner>> Create(TtlScannerBackend& backend, Config config,
                                                          ExecutionMode mode, Hooks hooks);

  ~TtlScanner();
  TtlScanner(const TtlScanner&) = delete;
  TtlScanner& operator=(const TtlScanner&) = delete;
  TtlScanner(TtlScanner&&) = delete;
  TtlScanner& operator=(TtlScanner&&) = delete;

  // Idempotent. Spawns the worker thread in kOwnedThread mode; flips a flag
  // in kManualTick mode.
  void Start();
  void Stop();

  // Runs one tick synchronously. Used internally by the kOwnedThread loop and
  // directly by kManualTick callers (reactors / tests). Returns the cycle's
  // report.
  core::Result<SweepReport> Tick();

  Snapshot CurrentSnapshot() const;
  const Config& config() const { return config_; }

 private:
  TtlScanner(TtlScannerBackend& backend, Config config, ExecutionMode mode, Hooks hooks);

  void ThreadLoop();
  void UpdateRateState(const SweepReport& report, std::chrono::nanoseconds cpu_used,
                       std::chrono::milliseconds wall_elapsed);
  void RecomputeDerived();

  TtlScannerBackend& backend_;
  Config config_;
  ExecutionMode mode_;
  Hooks hooks_;

  mutable std::mutex state_mu_;
  std::atomic<bool> running_{false};
  std::atomic<bool> stop_requested_{false};

  // Guarded by state_mu_.
  double rate_multiplier_ = 1.0;
  size_t sample_size_ = 0;
  std::chrono::milliseconds interval_{0};
  bool disk_pressure_active_ = false;
  double disk_pressure_fraction_ = 0.0;
  double cpu_fraction_ewma_ = 0.0;
  std::optional<core::SteadyTime> last_tick_start_;

  uint64_t total_ticks_ = 0;
  uint64_t total_sampled_ = 0;
  uint64_t total_with_ttl_ = 0;
  uint64_t total_expired_ = 0;
  uint64_t total_deleted_ = 0;
  uint64_t total_conflicts_ = 0;

  std::thread thread_;
  std::mutex wake_mu_;
  std::condition_variable wake_cv_;

  // Prometheus handles, indexed by TtlSubject (kString=0, kCollection=1).
  std::array<metrics::CounterHandle, 2> samples_total_;
  std::array<metrics::CounterHandle, 2> with_ttl_total_;
  std::array<metrics::CounterHandle, 2> expired_total_;
  std::array<metrics::CounterHandle, 2> deleted_total_;
  std::array<metrics::CounterHandle, 2> conflicts_total_;
  metrics::GaugeHandle interval_ms_gauge_;
  metrics::GaugeHandle sample_size_gauge_;
  metrics::GaugeHandle rate_multiplier_gauge_;
  metrics::GaugeHandle cpu_fraction_gauge_;
  metrics::GaugeHandle disk_pressure_fraction_gauge_;
  metrics::GaugeHandle disk_pressure_active_gauge_;
};

}  // namespace abyss::cold
