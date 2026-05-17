#include "abyss/hot/eviction_worker.h"

#include <utility>

#include "abyss/log/log.h"
#include "abyss/metrics/names.h"

ABYSS_LOG_COMPONENT("abyss.hot.eviction")

namespace abyss::hot {

EvictionWorker::EvictionWorker(ShardedHotStore& store, Config config,
                               core::SteadyClockFn steady_clock)
    : store_(store), config_(config), steady_clock_(std::move(steady_clock)) {
  auto& reg = metrics::Registry::Instance();
  evicted_total_ = reg.Counter(metrics::names::kEvictedTotal);
  ttl_expired_total_ = reg.Counter(metrics::names::kTtlExpiredTotal, metrics::Tier::kHot);
}

EvictionWorker::~EvictionWorker() { Stop(); }

void EvictionWorker::Start() {
  if (running_.exchange(true, std::memory_order_acq_rel)) return;
  stop_requested_.store(false, std::memory_order_release);
  thread_ = std::thread(&EvictionWorker::Run, this);
  ABYSS_LOG_INFO("eviction worker started",
                 {"tick_ms", static_cast<int64_t>(config_.tick.count())});
}

void EvictionWorker::Stop() {
  if (!running_.load(std::memory_order_acquire)) return;
  stop_requested_.store(true, std::memory_order_release);
  wake_cv_.notify_all();
  if (thread_.joinable()) thread_.join();
  running_.store(false, std::memory_order_release);
}

void EvictionWorker::TickOnce() {
  const auto now = steady_clock_();
  store_.DrainAccessBuffers(now);
  const auto report = store_.EvictExpired(now);
  if (report.by_deadline > 0) {
    evicted_total_.Increment(static_cast<double>(report.by_deadline));
  }
  if (report.by_ttl > 0) {
    ttl_expired_total_.Increment(static_cast<double>(report.by_ttl));
  }
}

void EvictionWorker::Run() {
  while (!stop_requested_.load(std::memory_order_acquire)) {
    TickOnce();
    std::unique_lock lock(wake_mutex_);
    wake_cv_.wait_for(lock, config_.tick,
                      [this] { return stop_requested_.load(std::memory_order_acquire); });
  }
}

}  // namespace abyss::hot
