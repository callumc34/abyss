#include "abyss/admin/metrics_snapshotter.h"

#include "abyss/metrics/names.h"

namespace abyss::admin {
namespace {

double MillisToSeconds(uint64_t ms) { return static_cast<double>(ms) / 1000.0; }

}  // namespace

MetricsSnapshotter::MetricsSnapshotter(const StatusProvider& provider) : provider_(provider) {
  auto& registry = metrics::Registry::Instance();
  queue_depth_ = registry.Gauge(metrics::names::kQueueDepth);
  queue_disk_bytes_ = registry.Gauge(metrics::names::kQueueDiskBytes);
  queue_oldest_eligible_unreaped_age_seconds_ =
      registry.Gauge(metrics::names::kQueueOldestEligibleUnreapedAgeSeconds);
  wal_unflushed_bytes_ = registry.Gauge(metrics::names::kWalUnflushedBytes);
  wal_durability_lag_seconds_ = registry.Gauge(metrics::names::kWalDurabilityLagSeconds);
  hot_keys_ = registry.Gauge(metrics::names::kHotKeys);
  hot_memory_bytes_ = registry.Gauge(metrics::names::kHotMemoryBytes);
  cold_keys_ = registry.Gauge(metrics::names::kColdKeys);
  cold_disk_bytes_ = registry.Gauge(metrics::names::kColdDiskBytes);
  cold_buffer_entries_ = registry.Gauge(metrics::names::kColdBufferEntries);
  cold_buffer_bytes_ = registry.Gauge(metrics::names::kColdBufferBytes);
  cold_buffer_oldest_entry_age_seconds_ =
      registry.Gauge(metrics::names::kColdBufferOldestEntryAgeSeconds);
  cold_consumer_lag_entries_ = registry.Gauge(metrics::names::kColdConsumerLagEntries);
  net_read_buffer_high_water_bytes_ = registry.Gauge(metrics::names::kNetReadBufferHighWaterBytes);
}

void MetricsSnapshotter::Observe() {
  const StatusSnapshot s = provider_.Snapshot();

  queue_depth_.Set(static_cast<double>(s.queue.total_entries));
  queue_disk_bytes_.Set(static_cast<double>(s.queue.total_bytes));
  queue_oldest_eligible_unreaped_age_seconds_.Set(
      MillisToSeconds(s.queue.oldest_eligible_unreaped_age_ms));
  wal_unflushed_bytes_.Set(static_cast<double>(s.queue.unflushed_bytes));
  wal_durability_lag_seconds_.Set(MillisToSeconds(s.queue.durability_lag_ms));

  hot_keys_.Set(static_cast<double>(s.hot.key_count));
  hot_memory_bytes_.Set(static_cast<double>(s.hot.memory_bytes));

  cold_keys_.Set(static_cast<double>(s.cold.key_count));
  cold_disk_bytes_.Set(static_cast<double>(s.cold.disk_bytes));
  cold_buffer_entries_.Set(static_cast<double>(s.cold.buffer.entries));
  cold_buffer_bytes_.Set(static_cast<double>(s.cold.buffer.bytes));
  cold_buffer_oldest_entry_age_seconds_.Set(MillisToSeconds(s.cold.buffer.oldest_entry_age_ms));

  cold_consumer_lag_entries_.Set(static_cast<double>(s.lag.cold_max_entries));

  net_read_buffer_high_water_bytes_.Set(
      static_cast<double>(s.connections.read_buffer_high_water_bytes));
}

}  // namespace abyss::admin
