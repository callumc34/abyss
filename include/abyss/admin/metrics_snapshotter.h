#pragma once

#include "abyss/admin/status_provider.h"
#include "abyss/metrics/metrics.h"

namespace abyss::admin {

// Snapshot-style gauges have no observation point on a hot path: their value is
// a property of the system at an instant, not an event, so nothing naturally
// writes them. They must therefore be pushed on a cadence.
//
// This is the single writer for every such gauge. Keeping it in one place is
// what stops stores and consumers from reaching into the metrics registry
// themselves, and it reads the same StatusProvider that /status renders, so the
// JSON surface and the Prometheus surface cannot disagree about the same
// number.
class MetricsSnapshotter {
 public:
  explicit MetricsSnapshotter(const StatusProvider& provider);
  ~MetricsSnapshotter() = default;

  MetricsSnapshotter(const MetricsSnapshotter&) = delete;
  MetricsSnapshotter& operator=(const MetricsSnapshotter&) = delete;
  MetricsSnapshotter(MetricsSnapshotter&&) = delete;
  MetricsSnapshotter& operator=(MetricsSnapshotter&&) = delete;

  void Observe();

 private:
  const StatusProvider& provider_;

  metrics::GaugeHandle queue_depth_;
  metrics::GaugeHandle queue_disk_bytes_;
  metrics::GaugeHandle queue_oldest_eligible_unreaped_age_seconds_;
  metrics::GaugeHandle wal_unflushed_bytes_;
  metrics::GaugeHandle wal_durability_lag_seconds_;
  metrics::GaugeHandle hot_keys_;
  metrics::GaugeHandle hot_memory_bytes_;
  metrics::GaugeHandle cold_keys_;
  metrics::GaugeHandle cold_disk_bytes_;
  metrics::GaugeHandle cold_buffer_entries_;
  metrics::GaugeHandle cold_buffer_bytes_;
  metrics::GaugeHandle cold_buffer_oldest_entry_age_seconds_;
  metrics::GaugeHandle cold_consumer_lag_entries_;
  metrics::GaugeHandle net_read_buffer_high_water_bytes_;
};

}  // namespace abyss::admin
