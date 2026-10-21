#include <chrono>
#include <string>
#include <string_view>

#include "prom_scrape.h"
#include "server_fixture.h"

namespace abyss::system_test {
namespace {

using namespace std::chrono_literals;

// Issue #74: two-TTL model end-to-end against the live server binary.
// Eviction moves tier; absolute TTL deletes entirely. The integration tier
// proves the same property with TestClock; this tier proves it through the
// hiredis-over-TCP path so the wired-up server gets exercised.
//
// Per-prefix eviction is the only knob the server accepts for eviction (the
// architecture rules out per-command eviction). The fixture installs a short
// eviction for the `ev_short:` prefix (scenarios 1 + 3) and a long default
// for the rest of the keyspace (scenario 2, where TTL must fire first).
//
// Timings follow the existing system-test precedents: eviction_seconds and
// quiet_threshold_seconds are integer seconds (validator), the smallest
// stable values are 2 / 1 / 1, and PXAT TTLs are sized so the read budget
// absorbs TSAN/ASAN slowdown. The 2 s / 2.5 s precedent in
// TieringTest.TtlExpiryRemovesKey is the floor.
constexpr std::string_view kTwoTtlConfig = R"yaml(
profile: embedded

hot:
  backend: builtin_hashmap
  max_memory_bytes: 67108864
  default_eviction_seconds: 30
  eviction_tick_ms: 100
  eviction_overrides:
    - prefix: "ev_short:"
      eviction_seconds: 2

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
  min_retention_seconds: 30

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

class TwoTtlSystemTest : public IsolatedDataServerTest {
 protected:
  TestServer::Config MakeServerConfig() const override {
    TestServer::Config c;
    c.shard_count = 1;
    c.config_yaml = std::string(kTwoTtlConfig);
    c.ready_timeout = 10s;
    return c;
  }

  static uint64_t WallNowMs() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
  }
};

// --- Scenario 1: eviction moves tier, not delete ---------------------------

TEST_F(TwoTtlSystemTest, EvictionMovesEveryTypeToCold) {
  // Pre-write every type under the short-eviction prefix, then verify all
  // are visible from hot. The four writes hit a single shard (shard_count=1)
  // and share the same eviction worker, so a single poll on
  // abyss_evicted_total covers all four removals.
  const uint16_t mport = Server().MetricsPort();
  // The flush precondition below is "at least one batch since baseline", which
  // the fixture probe's own flush would satisfy without any of this test's
  // writes reaching cold -- leaving the reads to be served from the buffer.
  ASSERT_FALSE(AwaitColdQuiescence(mport).empty())
      << "compaction buffer did not quiesce before baseline";
  const double evicted_before = ParseCounter(Scrape(mport), "abyss_evicted_total").value_or(0.0);
  const double ttl_expired_hot_before =
      ParseCounter(Scrape(mport), "abyss_ttl_expired_total", {{"tier", "hot"}}).value_or(0.0);
  const double flush_before =
      ParseCounter(Scrape(mport), "abyss_cold_flush_total", {{"status", "success"}}).value_or(0.0);

  ASSERT_TRUE(Client().Command({"SET", "ev_short:str", "v"}).IsOk());
  ASSERT_TRUE(Client().Command({"SADD", "ev_short:set", "a", "b", "c"}).IsInteger());
  ASSERT_TRUE(Client().Command({"HSET", "ev_short:h", "f1", "v1", "f2", "v2"}).IsInteger());
  ASSERT_TRUE(Client().Command({"ZADD", "ev_short:z", "1", "x", "2", "y"}).IsInteger());

  // Sanity: hot serves the writes before any eviction or flush.
  EXPECT_EQ(Client().Command({"GET", "ev_short:str"}).String(), "v");
  EXPECT_EQ(Client().Command({"SCARD", "ev_short:set"}).Integer(), 3);
  EXPECT_EQ(Client().Command({"HLEN", "ev_short:h"}).Integer(), 2);
  EXPECT_EQ(Client().Command({"ZCARD", "ev_short:z"}).Integer(), 2);

  // Eviction is 2 s; tick is 100 ms. A 6 s budget absorbs the 4 entries +
  // sanitizer slowdown. The counter advances by 4 because each shard's
  // EvictExpired report sums to 4 deadline-driven removals on the next tick.
  std::string body;
  ASSERT_TRUE(PollCounterAtLeast(mport, "abyss_evicted_total", {}, evicted_before + 4.0, 6s, &body))
      << "all four short-eviction keys should be removed from hot within 6 s; last scrape:\n"
      << body;

  // TTL counter must not have moved — no TTL was set on any of these keys.
  EXPECT_EQ(ParseCounter(Scrape(mport), "abyss_ttl_expired_total", {{"tier", "hot"}}).value_or(0.0),
            ttl_expired_hot_before)
      << "eviction-only scenario must not bump kTtlExpiredTotal{tier=hot}";

  // Confirm the cold consumer has flushed at least one batch since baseline,
  // so the upcoming reads exercise the RocksDB path, not just the buffer.
  ASSERT_TRUE(PollCounterAtLeast(mport, "abyss_cold_flush_total", {{"status", "success"}},
                                 flush_before + 1.0, 6s, &body))
      << "cold consumer should have completed at least one flush; last scrape:\n"
      << body;

  // Property: every type is still readable after eviction.
  EXPECT_EQ(Client().Command({"GET", "ev_short:str"}).String(), "v");
  EXPECT_EQ(Client().Command({"SCARD", "ev_short:set"}).Integer(), 3);
  EXPECT_EQ(Client().Command({"SISMEMBER", "ev_short:set", "a"}).Integer(), 1);
  EXPECT_EQ(Client().Command({"HGET", "ev_short:h", "f1"}).String(), "v1");
  EXPECT_EQ(Client().Command({"HLEN", "ev_short:h"}).Integer(), 2);
  EXPECT_EQ(Client().Command({"ZCARD", "ev_short:z"}).Integer(), 2);
  EXPECT_EQ(std::stod(Client().Command({"ZSCORE", "ev_short:z", "y"}).String()), 2.0);
}

// --- Scenario 2: absolute TTL removes from every tier ----------------------

TEST_F(TwoTtlSystemTest, AbsoluteTtlDeletesEveryTypeFromAllTiers) {
  const uint16_t mport = Server().MetricsPort();
  const double evicted_before = ParseCounter(Scrape(mport), "abyss_evicted_total").value_or(0.0);
  const double ttl_expired_hot_before =
      ParseCounter(Scrape(mport), "abyss_ttl_expired_total", {{"tier", "hot"}}).value_or(0.0);

  // PXAT carries an unambiguous absolute deadline; PX/EX anchor to the
  // parser's wall clock per invocation (open issue in resp/request_pipeline),
  // which would skew this test. The 3 000 ms TTL is two seconds longer than
  // the smallest stable TtlExpiryRemovesKey precedent (2 000 ms), giving
  // pre-expiry reads enough headroom on TSAN/ASAN.
  const auto ttl_ms = WallNowMs() + 3000;
  const auto ttl_str = std::to_string(ttl_ms);
  ASSERT_TRUE(Client().Command({"SET", "ttl_str", "v", "PXAT", ttl_str}).IsOk());
  ASSERT_TRUE(Client().Command({"SADD", "ttl_set", "a", "b"}).IsInteger());
  ASSERT_TRUE(Client().Command({"PEXPIREAT", "ttl_set", ttl_str}).Integer() == 1);
  ASSERT_TRUE(Client().Command({"HSET", "ttl_h", "f1", "v1", "f2", "v2"}).IsInteger());
  ASSERT_TRUE(Client().Command({"PEXPIREAT", "ttl_h", ttl_str}).Integer() == 1);
  ASSERT_TRUE(Client().Command({"ZADD", "ttl_z", "1", "x", "2", "y"}).IsInteger());
  ASSERT_TRUE(Client().Command({"PEXPIREAT", "ttl_z", ttl_str}).Integer() == 1);

  // Pre-expiry: every read still serves from hot.
  EXPECT_EQ(Client().Command({"GET", "ttl_str"}).String(), "v");
  EXPECT_EQ(Client().Command({"SCARD", "ttl_set"}).Integer(), 2);
  EXPECT_EQ(Client().Command({"HGET", "ttl_h", "f1"}).String(), "v1");
  EXPECT_EQ(Client().Command({"ZSCORE", "ttl_z", "x"}).IsBulk(), true);

  // Eviction worker scans every 100 ms; once wall passes the TTL, the four
  // entries are TTL-expired and the worker bumps the cold tier of the
  // metric (each shard's report.by_ttl). 6 s budget covers the 3 s TTL +
  // slack on TSAN.
  std::string body;
  ASSERT_TRUE(PollCounterAtLeast(mport, "abyss_ttl_expired_total", {{"tier", "hot"}},
                                 ttl_expired_hot_before + 4.0, 6s, &body))
      << "all four TTL keys should be ttl-evicted from hot within 6 s; last scrape:\n"
      << body;

  // Negative assertion: the deadline counter must not move. Default eviction
  // is 30 s, the test runs in under 10 s, so no key should have crossed the
  // deadline path even if eviction had fired first by mistake.
  EXPECT_EQ(ParseCounter(Scrape(mport), "abyss_evicted_total").value_or(0.0), evicted_before)
      << "TTL-only scenario must not bump kEvictedTotal";

  // Property: post-TTL, every read returns the type-appropriate "absent".
  EXPECT_TRUE(Client().Command({"GET", "ttl_str"}).IsNil()) << "expired string should be nil";
  EXPECT_EQ(Client().Command({"SCARD", "ttl_set"}).Integer(), 0);
  EXPECT_EQ(Client().Command({"SISMEMBER", "ttl_set", "a"}).Integer(), 0);
  EXPECT_TRUE(Client().Command({"HGET", "ttl_h", "f1"}).IsNil());
  EXPECT_EQ(Client().Command({"HLEN", "ttl_h"}).Integer(), 0);
  EXPECT_EQ(Client().Command({"ZCARD", "ttl_z"}).Integer(), 0);
  EXPECT_TRUE(Client().Command({"ZSCORE", "ttl_z", "x"}).IsNil());
}

// --- Scenario 3: eviction first, then TTL ---------------------------------

TEST_F(TwoTtlSystemTest, EvictionMovesTierThenTtlDeletesAllTypes) {
  const uint16_t mport = Server().MetricsPort();
  ASSERT_FALSE(AwaitColdQuiescence(mport).empty())
      << "compaction buffer did not quiesce before baseline";
  const double evicted_before = ParseCounter(Scrape(mport), "abyss_evicted_total").value_or(0.0);
  const double ttl_expired_hot_before =
      ParseCounter(Scrape(mport), "abyss_ttl_expired_total", {{"tier", "hot"}}).value_or(0.0);
  const double flush_before =
      ParseCounter(Scrape(mport), "abyss_cold_flush_total", {{"status", "success"}}).value_or(0.0);

  // eviction=2 s (prefix), TTL=5 s. The 3 s window between is where each
  // type must remain readable from cold. After TTL, every read must report
  // absent.
  const auto ttl_ms = WallNowMs() + 5000;
  const auto ttl_str = std::to_string(ttl_ms);
  ASSERT_TRUE(Client().Command({"SET", "ev_short:c_str", "v", "PXAT", ttl_str}).IsOk());
  ASSERT_TRUE(Client().Command({"SADD", "ev_short:c_set", "a", "b"}).IsInteger());
  ASSERT_TRUE(Client().Command({"PEXPIREAT", "ev_short:c_set", ttl_str}).Integer() == 1);
  ASSERT_TRUE(Client().Command({"HSET", "ev_short:c_h", "f1", "v1"}).IsInteger());
  ASSERT_TRUE(Client().Command({"PEXPIREAT", "ev_short:c_h", ttl_str}).Integer() == 1);
  ASSERT_TRUE(Client().Command({"ZADD", "ev_short:c_z", "1", "x"}).IsInteger());
  ASSERT_TRUE(Client().Command({"PEXPIREAT", "ev_short:c_z", ttl_str}).Integer() == 1);

  // Eviction at ~2 s. PollCounterAtLeast budget = 6 s for the four
  // deadline-driven removals.
  std::string body;
  ASSERT_TRUE(PollCounterAtLeast(mport, "abyss_evicted_total", {}, evicted_before + 4.0, 6s, &body))
      << "all four short-eviction keys should be deadline-evicted within 6 s; last scrape:\n"
      << body;

  // The cold consumer has flushed at least once before we read.
  ASSERT_TRUE(PollCounterAtLeast(mport, "abyss_cold_flush_total", {{"status", "success"}},
                                 flush_before + 1.0, 6s, &body));

  // Mid-window (post-eviction, pre-TTL): every read still observes the
  // pre-eviction value.
  EXPECT_EQ(Client().Command({"GET", "ev_short:c_str"}).String(), "v");
  EXPECT_EQ(Client().Command({"SCARD", "ev_short:c_set"}).Integer(), 2);
  EXPECT_EQ(Client().Command({"HGET", "ev_short:c_h", "f1"}).String(), "v1");
  EXPECT_EQ(std::stod(Client().Command({"ZSCORE", "ev_short:c_z", "x"}).String()), 1.0);

  // Hot ttl-expired counter must not have moved yet — only eviction has
  // fired so far. Once TTL fires we expect this to advance.
  EXPECT_EQ(ParseCounter(Scrape(mport), "abyss_ttl_expired_total", {{"tier", "hot"}}).value_or(0.0),
            ttl_expired_hot_before)
      << "TTL has not elapsed; ttl-expired counter must not advance prematurely";

  // Past TTL (5 s); the hot/buffer/cold tiers must all surface absent.
  // Wait budget: 5 s TTL minus the ~2-3 s already spent on the eviction
  // wait + read steps, plus slack. 6 s is safe and still under the 30 s
  // CTest TIMEOUT for the file.
  std::this_thread::sleep_for(4s);

  EXPECT_TRUE(Client().Command({"GET", "ev_short:c_str"}).IsNil());
  EXPECT_EQ(Client().Command({"SCARD", "ev_short:c_set"}).Integer(), 0);
  EXPECT_TRUE(Client().Command({"HGET", "ev_short:c_h", "f1"}).IsNil());
  EXPECT_TRUE(Client().Command({"ZSCORE", "ev_short:c_z", "x"}).IsNil());
}

}  // namespace
}  // namespace abyss::system_test
