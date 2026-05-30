#include <chrono>
#include <string>

#include "prom_scrape.h"
#include "server_fixture.h"

namespace abyss::system_test {
namespace {

using namespace std::chrono_literals;

// Tight tiering timings so the two flush windows plus a hot eviction complete
// inside the test deadline. Mirrors tiered_read_test's fast config: integer
// second minimums on quiet/safety, short eviction, fast eviction tick.
constexpr std::string_view kFastTieringConfig = R"yaml(
profile: embedded

hot:
  backend: builtin_hashmap
  max_memory_bytes: 67108864
  default_eviction_seconds: 3
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
  segment_size_bytes: 67108864
  min_retention_seconds: 3
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

class RemoveResurrectionTest : public IsolatedDataServerTest {
 protected:
  TestServer::Config MakeServerConfig() const override {
    TestServer::Config c;
    c.shard_count = 1;
    c.config_yaml = std::string(kFastTieringConfig);
    c.ready_timeout = 10s;
    return c;
  }
};

// A field deleted after it has been flushed to cold must not resurrect when the
// key later falls through to cold. Two fields are written so the key survives
// in hot until eviction (deleting the only field would remove the key from hot
// outright, skipping the tier-crossing read this test must exercise).
TEST_F(RemoveResurrectionTest, HashFieldDeletedAfterFlushDoesNotResurrect) {
  const uint16_t mport = Server().MetricsPort();

  // Count successful flushes irrespective of trigger (quiet vs deadline): the
  // test only needs cold to have applied each window, not a specific trigger.
  auto flushes = [&] {
    return ParseCounter(Scrape(mport), "abyss_cold_flush_total", {{"status", "success"}})
        .value_or(0.0);
  };

  // Snapshot the eviction baseline before any write: the key evicts exactly
  // once and may do so before the window-2 flush completes, so snapshotting it
  // later would race the eviction and wait for a second one that never comes.
  const double before_evicted = ParseCounter(Scrape(mport), "abyss_evicted_total").value_or(0.0);

  // Window 1: write both fields and let the flush persist them to cold.
  const double before_write_flush = flushes();
  ASSERT_TRUE(Client().Command({"HSET", "h", "f1", "v1", "f2", "v2"}).IsInteger());

  std::string body;
  ASSERT_TRUE(PollCounterAtLeast(mport, "abyss_cold_flush_total", {{"status", "success"}},
                                 before_write_flush + 1.0, 6s, &body))
      << "window-1 flush did not land within 6s; last scrape:\n"
      << body;

  // Window 2: delete f1. With the buffer entry for window 1 already flushed,
  // this absorbs a fresh entry carrying only the removal.
  const double before_del_flush = flushes();
  ASSERT_EQ(Client().Command({"HDEL", "h", "f1"}).Integer(), 1);

  ASSERT_TRUE(PollCounterAtLeast(mport, "abyss_cold_flush_total", {{"status", "success"}},
                                 before_del_flush + 1.0, 6s, &body))
      << "window-2 flush did not land within 6s; last scrape:\n"
      << body;

  // Drive the key out of hot so the reads below resolve against cold. The
  // buffer is already drained by the window-2 flush, so the read path is
  // hot-miss -> buffer-miss -> cold.
  ASSERT_TRUE(PollCounterAtLeast(mport, "abyss_evicted_total", {}, before_evicted + 1.0, 8s, &body))
      << "hot eviction did not occur within 8s; last scrape:\n"
      << body;

  // The deleted field must be gone from cold; the untouched field must survive.
  auto deleted = Client().Command({"HGET", "h", "f1"});
  EXPECT_TRUE(deleted.IsNil()) << "HGET h f1 resurrected a deleted field from cold: " << deleted;
  EXPECT_EQ(Client().Command({"HGET", "h", "f2"}).String(), "v2");
}

}  // namespace
}  // namespace abyss::system_test
