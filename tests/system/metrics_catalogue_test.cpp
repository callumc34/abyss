#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <cctype>
#include <chrono>
#include <cstddef>
#include <fstream>
#include <functional>
#include <map>
#include <sstream>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include "prom_scrape.h"
#include "server_fixture.h"

namespace abyss::system_test {
namespace {

using namespace std::chrono_literals;

// Short eviction, quiet threshold and scanner interval, so one smoke
// run flushes to cold, evicts and serves from cold.
constexpr std::string_view kSmokeConfig = R"yaml(
profile: embedded

hot:
  backend: builtin_hashmap
  max_memory_bytes: 67108864
  default_eviction_seconds: 2
  eviction_tick_ms: 100

cold:
  backend: builtin_rocksdb
  data_path: "${DATA_DIR}/cold"
  write_buffer_size_bytes: 67108864
  ttl_scanner:
    enabled: true
    base_sample_size: 20
    min_sample_size: 5
    max_sample_size: 200
    base_interval_ms: 200
    min_interval_ms: 100
    max_interval_ms: 1000
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

// Documented metrics the smoke run need not move, though each must be
// registered: errors, pressure and paths it does not take.
constexpr auto kConditional = std::to_array<std::string_view>({
    // Waits past a spin, contention and resource pressure.
    "abyss_wal_fill_wait_seconds",
    "abyss_wal_publish_wait_seconds",
    "abyss_wal_backpressure_waits_total",
    "abyss_wal_backpressure_rejections_total",
    "abyss_wal_spare_waits_total",
    "abyss_cold_durability_waits_total",
    "abyss_hot_memory_evicted_total",
    "abyss_hot_rehash_seconds",
    "abyss_hot_backpressure_waits_total",
    "abyss_hot_backpressure_rejections_total",
    "abyss_hot_stub_drops_total",
    "abyss_hot_load_discards_total",
    "abyss_sequencer_redecides_total",
    "abyss_net_connections_rejected_total",
    "abyss_net_backpressure_entered_total",
    "abyss_net_backpressure_exited_total",
    "abyss_net_orphans",
    "abyss_net_late_completions_total",
    // Errors and faults.
    "abyss_resp_parse_errors_total",
    "abyss_queue_offset_persist_failures_total",
    "abyss_queue_read_out_of_range_total",
    "abyss_wal_segment_prepare_failures_total",
    "abyss_cold_drain_truncated_total",
    "abyss_cold_read_deadline_exceeded_total",
    "abyss_cold_apply_type_conflicts_total",
    "abyss_cold_ttl_conflicts_total",
    // Keys expired in cold by its log clock; a log with a reclaimed tail.
    "abyss_cold_ttl_expired_total",
    "abyss_cold_ttl_deleted_total",
    "abyss_recovery_hot_skipped_frames_total",
});

// Every abyss_ name the metrics section documents, bar removed ones.
std::vector<std::string> DocumentedMetrics() {
  std::ifstream in(ABYSS_OBSERVABILITY_DOC);
  EXPECT_TRUE(in.good()) << "cannot read " << ABYSS_OBSERVABILITY_DOC;
  std::vector<std::string> names;
  bool in_section = false;
  for (std::string line; std::getline(in, line);) {
    if (line.starts_with("## ")) {
      in_section = line == "## Prometheus Metrics";
      continue;
    }
    if (!in_section || line.starts_with("**Removed.**")) continue;
    for (size_t at = line.find("`abyss_"); at != std::string::npos;
         at = line.find("`abyss_", at + 1)) {
      size_t end = at + 1;
      while (end < line.size() &&
             (std::islower(static_cast<unsigned char>(line[end])) != 0 ||
              std::isdigit(static_cast<unsigned char>(line[end])) != 0 || line[end] == '_')) {
        ++end;
      }
      std::string name = line.substr(at + 1, end - at - 1);
      if (std::ranges::find(names, name) == names.end()) names.push_back(std::move(name));
    }
  }
  return names;
}

struct Exposition {
  // Family name to its # TYPE.
  std::map<std::string, std::string, std::less<>> types;
  // Sample name, labels dropped, to its largest value.
  std::map<std::string, double, std::less<>> samples;
};

Exposition Parse(const std::string& body) {
  Exposition out;
  std::istringstream lines(body);
  for (std::string line; std::getline(lines, line);) {
    if (line.starts_with("# TYPE ")) {
      std::istringstream fields(line.substr(7));
      std::string name;
      std::string type;
      fields >> name >> type;
      out.types[name] = type;
      continue;
    }
    if (line.empty() || line.front() == '#') continue;
    const size_t name_end = line.find_first_of("{ ");
    const size_t value_at = line.rfind(' ');
    if (name_end == std::string::npos || value_at == std::string::npos) continue;
    const double value = std::stod(line.substr(value_at + 1));
    const std::string name = line.substr(0, name_end);
    auto [it, inserted] = out.samples.try_emplace(name, value);
    if (!inserted) it->second = std::max(it->second, value);
  }
  return out;
}

// Why `name` fails the catalogue, or empty: never registered, or a
// counter or histogram never recorded unless `conditional`.
std::string Missing(const Exposition& exposition, const std::string& name, bool conditional) {
  const auto type = exposition.types.find(name);
  if (type == exposition.types.end()) return "never registered";
  if (conditional) return {};
  const std::string sample = type->second == "histogram" ? name + "_count" : name;
  const auto value = exposition.samples.find(sample);
  if (value == exposition.samples.end()) return "registered with no series";
  if (type->second != "gauge" && value->second <= 0) return "never recorded";
  return {};
}

class MetricsCatalogueTest : public IsolatedDataServerTest {
 protected:
  TestServer::Config MakeServerConfig() const override {
    TestServer::Config c;
    c.shard_count = 2;
    c.config_yaml = std::string(kSmokeConfig);
    c.ready_timeout = 10s;
    return c;
  }
};

// A documented metric that is never registered or recorded misleads an
// operator; one that only an error or pressure moves is allowlisted.
TEST_F(MetricsCatalogueTest, EveryDocumentedMetricIsRecorded) {
  const std::vector<std::string> documented = DocumentedMetrics();
  ASSERT_GT(documented.size(), 50U) << "the doc parser found too few metrics";
  for (const std::string_view name : kConditional) {
    EXPECT_NE(std::ranges::find(documented, name), documented.end())
        << name << " is allowlisted but not documented";
  }

  // A log for recovery to scan.
  ASSERT_TRUE(Client().Command({"SET", "before", "v"}).IsOk());
  ASSERT_NO_FATAL_FAILURE(RestartServer());
  const uint16_t mport = Server().MetricsPort();

  ASSERT_TRUE(Client().Command({"HELLO", "2"}).IsArray());
  // Enough writes that the sequencer samples some, each with a TTL the
  // cold scanner samples.
  std::vector<std::vector<std::string>> sets;
  sets.reserve(1000);
  for (int i = 0; i < 1000; ++i)
    sets.push_back({"SET", "k" + std::to_string(i), "v", "EX", "3600"});
  for (const Reply& reply : Client().Pipeline(sets)) ASSERT_TRUE(reply.IsOk()) << reply;
  ASSERT_TRUE(Client().Command({"SET", "short", "v", "PX", "100"}).IsOk());
  ASSERT_TRUE(Client().Command({"HSET", "h", "f", "v"}).IsInteger());
  ASSERT_TRUE(Client().Command({"SADD", "s", "a", "b"}).IsInteger());
  ASSERT_TRUE(Client().Command({"ZADD", "z", "1", "m"}).IsInteger());
  ASSERT_TRUE(Client().Command({"DEL", "k0"}).IsInteger());
  EXPECT_EQ(Client().Command({"GET", "k1"}).String(), "v");
  EXPECT_TRUE(Client().Command({"GET", "missing"}).IsNil());
  EXPECT_TRUE(Client().Command({"NOSUCHCOMMAND"}).IsError());
  {
    RedisClient other;
    ASSERT_TRUE(other.Connect("127.0.0.1", Server().Port()));
    EXPECT_TRUE(other.Command({"PING"}).IsStatus());
  }
  std::this_thread::sleep_for(200ms);
  EXPECT_TRUE(Client().Command({"GET", "short"}).IsNil());

  std::string body;
  // Most keys evicted, so reads of untouched ones come from cold. A hot
  // hit would refresh its key's eviction, so each is read only twice:
  // a miss, then the second miss that fills.
  ASSERT_TRUE(PollCounterAtLeast(mport, "abyss_evicted_total", {}, 900.0, 30s, &body))
      << "too little was evicted; last scrape:\n"
      << body;
  ASSERT_FALSE(AwaitColdQuiescence(mport).empty()) << "the buffer never emptied";
  for (int i = 2; i < 22; ++i) {
    const std::string key = "k" + std::to_string(i);
    for (int read = 0; read < 2; ++read) EXPECT_EQ(Client().Command({"GET", key}).String(), "v");
  }
  ASSERT_TRUE(PollCounterAtLeast(mport, "abyss_hits_total", {{"tier", "cold"}}, 1.0, 5s, &body));
  ASSERT_TRUE(PollCounterAtLeast(mport, "abyss_net_connections_closed_total",
                                 {{"reason", "client"}}, 1.0, 5s, &body));

  ASSERT_TRUE(PollCounterAtLeast(mport, "abyss_cold_ttl_with_ttl_total", {{"subject", "string"}},
                                 1.0, 5s, &body))
      << "the TTL scanner never sampled a key with a TTL";

  const Exposition exposition = Parse(Scrape(mport));
  std::string failures;
  for (const auto& name : documented) {
    const bool conditional = std::ranges::find(kConditional, name) != kConditional.end();
    if (const std::string why = Missing(exposition, name, conditional); !why.empty()) {
      failures.append("  ").append(name).append(": ").append(why).append("\n");
    }
  }
  EXPECT_TRUE(failures.empty()) << "documented in observability.md but not seen in /metrics:\n"
                                << failures;
}

}  // namespace
}  // namespace abyss::system_test
