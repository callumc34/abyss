#include <chrono>
#include <string>

#include "prom_scrape.h"
#include "server_fixture.h"

namespace abyss::system_test {
namespace {

using namespace std::chrono_literals;

// Eviction and flush timings are tightened to keep the natural-flow assertion
// inside a reasonable test deadline. Values are bounded by the validator:
// quiet_threshold_seconds and safety_margin_seconds are integer seconds with
// a 1-second minimum, so the test waits ~3-4 wall seconds end-to-end.
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
  segment_size_bytes: 134217728
  min_retention_seconds: 3

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

class TieredReadTest : public IsolatedDataServerTest {
 protected:
  TestServer::Config MakeServerConfig() const override {
    TestServer::Config c;
    c.shard_count = 1;
    c.config_yaml = std::string(kFastTieringConfig);
    c.ready_timeout = 10s;
    return c;
  }
};

TEST_F(TieredReadTest, NaturalFlowHotEvictsThenColdServes) {
  const uint16_t mport = Server().MetricsPort();

  // Drain the fixture probe: if its flush satisfies the quiet-flush wait below
  // while `k` is still buffered, the post-eviction read is served from the
  // buffer and the cold-hit wait times out.
  ASSERT_FALSE(AwaitColdQuiescence(mport).empty())
      << "compaction buffer did not quiesce before baseline";

  const double baseline_hot =
      ParseCounter(Scrape(mport), "abyss_hits_total", {{"tier", "hot"}}).value_or(0.0);
  const double baseline_cold =
      ParseCounter(Scrape(mport), "abyss_hits_total", {{"tier", "cold"}}).value_or(0.0);
  const double baseline_quiet_flush =
      ParseCounter(Scrape(mport), "abyss_cold_flush_reason_total", {{"reason", "quiet"}})
          .value_or(0.0);
  const double baseline_evicted = ParseCounter(Scrape(mport), "abyss_evicted_total").value_or(0.0);

  ASSERT_TRUE(Client().Command({"SET", "k", "v"}).IsOk());

  ASSERT_EQ(Client().Command({"GET", "k"}).String(), "v");

  std::string scrape_body;
  ASSERT_TRUE(PollCounterAtLeast(mport, "abyss_hits_total", {{"tier", "hot"}}, baseline_hot + 1.0,
                                 2s, &scrape_body))
      << "hot hit metric did not increment within 2s; last scrape:\n"
      << scrape_body;

  ASSERT_TRUE(PollCounterAtLeast(mport, "abyss_cold_flush_reason_total", {{"reason", "quiet"}},
                                 baseline_quiet_flush + 1.0, 5s, &scrape_body))
      << "cold quiet-flush metric did not increment within 5s; last scrape:\n"
      << scrape_body;

  ASSERT_TRUE(PollCounterAtLeast(mport, "abyss_evicted_total", {}, baseline_evicted + 1.0, 6s,
                                 &scrape_body))
      << "hot eviction metric did not increment within 6s; last scrape:\n"
      << scrape_body;

  ASSERT_EQ(Client().Command({"GET", "k"}).String(), "v");

  ASSERT_TRUE(PollCounterAtLeast(mport, "abyss_hits_total", {{"tier", "cold"}}, baseline_cold + 1.0,
                                 2s, &scrape_body))
      << "cold hit metric did not increment within 2s; last scrape:\n"
      << scrape_body;

  // Cold keeps answering; filling hot from a cold hit is not done here.
  ASSERT_EQ(Client().Command({"GET", "k"}).String(), "v");
}

TEST_F(TieredReadTest, MissReturnsNilAndIncrementsMisses) {
  const uint16_t mport = Server().MetricsPort();
  const double baseline_misses = ParseCounter(Scrape(mport), "abyss_misses_total").value_or(0.0);

  auto r = Client().Command({"GET", "nonexistent_key"});
  EXPECT_TRUE(r.IsNil()) << "GET on missing key should return nil, got: " << r;

  std::string body;
  ASSERT_TRUE(PollCounterAtLeast(mport, "abyss_misses_total", {}, baseline_misses + 1.0, 2s, &body))
      << "miss metric did not increment within 2s; last scrape:\n"
      << body;
}

TEST_F(TieredReadTest, ReadAfterWriteReturnsValueImmediately) {
  // Pass criterion for #75: a SET followed immediately by a GET must observe
  // the written value. Guaranteed by the engine waiting on fsync+hot-apply
  // before returning OK on the write.
  ASSERT_TRUE(Client().Command({"SET", "raw_k", "raw_v"}).IsOk());
  auto r = Client().Command({"GET", "raw_k"});
  ASSERT_TRUE(r.IsBulk());
  EXPECT_EQ(r.String(), "raw_v");
}

}  // namespace
}  // namespace abyss::system_test
