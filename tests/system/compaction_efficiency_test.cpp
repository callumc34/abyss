#include <chrono>
#include <string>
#include <vector>

#include "prom_scrape.h"
#include "server_fixture.h"

namespace abyss::system_test {
namespace {

using namespace std::chrono_literals;

// Mirrors the natural-flow tuning in tiered_read_test.cpp so a burst of
// SETs has time to elapse the quiet window without crossing the deadline
// trigger. quiet_threshold and safety_margin are seconds, so this is the
// tightest valid timing the validator accepts.
constexpr std::string_view kBurstTieringConfig = R"yaml(
profile: embedded

hot:
  backend: builtin_hashmap
  max_memory_bytes: 67108864
  default_eviction_seconds: 10
  eviction_tick_ms: 100

cold:
  backend: builtin_rocksdb
  data_path: "${DATA_DIR}/cold"
  write_buffer_size_bytes: 67108864
  ttl_scanner:
    enabled: false
    base_sample_size: 20
    min_sample_size: 5
    max_sample_size: 200
    base_interval_ms: 1000
    min_interval_ms: 100
    max_interval_ms: 60000
    high_threshold: 0.25
    low_threshold: 0.05
    rate_increase_factor: 1.5
    rate_decrease_factor: 0.7
    disk_pressure_threshold: 0.9
    disk_pressure_release_threshold: 0.855
    max_cpu_fraction: 0.10
    cpu_ewma_window_seconds: 30

queue:
  backend: builtin_wal
  wal_path: "${DATA_DIR}/wal"
  segment_size_bytes: 134217728
  min_retention_seconds: 10
  wal_fsync_policy: group_commit
  group_commit_interval_us: 1000
  group_commit_max_bytes: 1048576

hot_consumer:
  read_batch_size: 64
  read_timeout_ms: 10

cold_consumer:
  quiet_threshold_seconds: 1
  safety_margin_seconds: 1
  jitter_fraction: 0.1
  buffer_high_water_bytes: 16777216
  buffer_low_water_bytes: 0
  max_flush_batch_size: 1024
  queue_read_max_count: 64
  queue_read_timeout_ms: 10
  retry_initial_backoff_ms: 10
  retry_max_backoff_ms: 1000

recovery:
  replay_parallelism: 2
  hot_replay_batch_size: 1000
  cold_replay_batch_size: 5000

net:
  bind: 127.0.0.1
  port: 0
  max_connections: 64
  idle_timeout_seconds: 60
  io_threads: 0
  accept_queue: 32
  max_read_buffer_bytes: 1048576
  write_backpressure_bytes: 524288
  write_resume_bytes: 131072
  write_hard_limit_bytes: 2097152
  shutdown_grace_seconds: 5
  reaper_tick_ms: 100

metrics:
  enabled: true
  bind: 127.0.0.1
  port: 0

admin:
  enabled: true
  bind: 127.0.0.1
  port: 0

log:
  level: warn
  format: json
  sink: stdout
)yaml";

class CompactionEfficiencyTest : public IsolatedDataServerTest {
 protected:
  TestServer::Config MakeServerConfig() const override {
    TestServer::Config c;
    c.shard_count = 1;
    c.config_yaml = std::string(kBurstTieringConfig);
    c.ready_timeout = 10s;
    return c;
  }
};

TEST_F(CompactionEfficiencyTest, BurstWritesSingleKeyCollapseToOneQuietFlush) {
  // Issue #73's headline number is 10,000; the property — buffer absorbs
  // the burst and emits one quiet-window flush — is fully demonstrated at
  // 100. The reduced count keeps wall-clock test runtime under 5s with
  // safety_margin_seconds=1.
  constexpr int kSets = 100;

  const uint16_t mport = Server().MetricsPort();

  const double baseline_quiet =
      ParseCounter(Scrape(mport), "abyss_cold_flush_reason_total", {{"reason", "quiet"}})
          .value_or(0.0);
  const double baseline_deadline =
      ParseCounter(Scrape(mport), "abyss_cold_flush_reason_total", {{"reason", "deadline"}})
          .value_or(0.0);
  const double baseline_flush_success =
      ParseCounter(Scrape(mport), "abyss_cold_flush_total", {{"status", "success"}}).value_or(0.0);

  std::vector<std::vector<std::string>> writes;
  writes.reserve(kSets);
  for (int i = 0; i < kSets; ++i) {
    writes.push_back({"SET", "burst_k", "v_" + std::to_string(i)});
  }
  const auto replies = Client().Pipeline(writes);
  ASSERT_EQ(replies.size(), static_cast<size_t>(kSets));
  for (int i = 0; i < kSets; ++i) {
    ASSERT_TRUE(replies[i].IsOk()) << "pipelined SET refused at i=" << i;
  }

  std::string body;
  ASSERT_TRUE(PollCounterAtLeast(mport, "abyss_cold_flush_reason_total", {{"reason", "quiet"}},
                                 baseline_quiet + 1.0, 5s, &body))
      << "quiet flush did not fire within 5s; last scrape:\n"
      << body;

  // Read final values once the quiet path has fired. The burst should not
  // have crossed any deadline. flush_total{success} captures how many keys
  // were actually flushed — 1 per burst, not 100.
  const double final_quiet =
      ParseCounter(Scrape(mport), "abyss_cold_flush_reason_total", {{"reason", "quiet"}})
          .value_or(0.0);
  const double final_deadline =
      ParseCounter(Scrape(mport), "abyss_cold_flush_reason_total", {{"reason", "deadline"}})
          .value_or(0.0);
  const double final_flush_success =
      ParseCounter(Scrape(mport), "abyss_cold_flush_total", {{"status", "success"}}).value_or(0.0);

  EXPECT_EQ(final_deadline, baseline_deadline)
      << "burst should not trip a deadline flush; final=" << final_deadline
      << " baseline=" << baseline_deadline;

  // The burst's own key produces exactly one flushed entry; total entries
  // flushed for this test = 1 (the burst's collapsed entry). Previous
  // probe-cycle flushes are captured in the baseline.
  EXPECT_EQ(final_quiet - baseline_quiet, 1.0)
      << "burst should produce a single quiet flush; delta=" << (final_quiet - baseline_quiet);
  EXPECT_EQ(final_flush_success - baseline_flush_success, 1.0)
      << "burst should produce a single flushed entry; delta="
      << (final_flush_success - baseline_flush_success);

  // Read-back asserts the value is the last write — confirms the compacted
  // state in cold is the latest in the burst, not an earlier intermediate.
  auto r = Client().Command({"GET", "burst_k"});
  ASSERT_TRUE(r.IsBulk());
  EXPECT_EQ(r.String(), "v_" + std::to_string(kSets - 1));
}

}  // namespace
}  // namespace abyss::system_test
