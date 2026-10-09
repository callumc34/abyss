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
  tombstones_reclaimed_total_ = reg.Counter(metrics::names::kHotTombstonesReclaimedTotal);
  memory_evicted_total_ = reg.Counter(metrics::names::kHotMemoryEvictedTotal);
  access_buffer_dropped_total_ = reg.Counter(metrics::names::kHotAccessBufferDroppedTotal);
  stub_drops_total_ = reg.Counter(metrics::names::kHotStubDropsTotal);
  load_discards_total_ = reg.Counter(metrics::names::kHotLoadDiscardsTotal);
  hot_memory_bytes_ = reg.Gauge(metrics::names::kHotMemoryBytes);
  hot_keys_ = reg.Gauge(metrics::names::kHotKeys);
  hot_max_memory_bytes_ = reg.Gauge(metrics::names::kHotMaxMemoryBytes);
  hot_access_buffer_depth_ = reg.Gauge(metrics::names::kHotAccessBufferDepth);
  hot_stub_entries_ = reg.Gauge(metrics::names::kHotStubEntries);
  hot_negative_entries_ = reg.Gauge(metrics::names::kHotNegativeEntries);
  hot_unevictable_bytes_ = reg.Gauge(metrics::names::kHotUnevictableBytes);
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

  // Memory-pressure pass: evict LRU down to the per-shard budget. Eviction is a
  // tier transition (data stays durable in queue/cold), counted distinctly from
  // deadline evictions (HOT-1/HOT-7). Suppressed implicitly when unlimited.
  const size_t memory_evicted = store_.EvictToMemoryTarget();
  if (memory_evicted > 0) {
    memory_evicted_total_.Increment(static_cast<double>(memory_evicted));
  }

  // Reclaim delete tombstones the cold consumer has now absorbed. Bounded by
  // cold's drained seq per shard so a tombstone never outlives the window in
  // which a lagging buffer/cold could still serve the pre-delete state.
  const size_t reclaimed = store_.GcTombstones();
  if (reclaimed > 0) {
    tombstones_reclaimed_total_.Increment(static_cast<double>(reclaimed));
  }

  // Publish hot-tier usage/budget/backlog so memory pressure is observable
  // rather than silently growing (invariant 5).
  const auto stats = store_.Stats();
  if (stats.has_value()) {
    hot_memory_bytes_.Set(static_cast<double>(stats->used_bytes));
    hot_keys_.Set(static_cast<double>(stats->key_count));
    hot_max_memory_bytes_.Set(static_cast<double>(stats->max_bytes));
    hot_stub_entries_.Set(static_cast<double>(stats->stub_entries));
    hot_negative_entries_.Set(static_cast<double>(stats->negative_entries));
    hot_unevictable_bytes_.Set(static_cast<double>(stats->unevictable_bytes));
    if (stats->stub_drops > reported_stub_drops_) {
      stub_drops_total_.Increment(static_cast<double>(stats->stub_drops - reported_stub_drops_));
      reported_stub_drops_ = stats->stub_drops;
    }
    if (stats->load_discards > reported_load_discards_) {
      load_discards_total_.Increment(
          static_cast<double>(stats->load_discards - reported_load_discards_));
      reported_load_discards_ = stats->load_discards;
    }
  }
  const auto access = store_.AccessBufferSnapshot();
  hot_access_buffer_depth_.Set(static_cast<double>(access.depth));
  if (access.dropped > reported_access_dropped_) {
    access_buffer_dropped_total_.Increment(
        static_cast<double>(access.dropped - reported_access_dropped_));
    reported_access_dropped_ = access.dropped;
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
