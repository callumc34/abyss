// Cold judges expiry by its shard's log clock, never the wall clock
// (ADP-004 §Expiry). A real RocksdbStore, compaction buffer and cold
// consumer run over an in-memory log while the wall clock runs ahead.

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <chrono>
#include <cstdint>
#include <memory>
#include <optional>
#include <ostream>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "abyss/cold/backends/rocksdb_store.h"
#include "abyss/cold/ttl_scanner.h"
#include "abyss/consumer/cold_consumer.h"
#include "abyss/core/cold_store.h"
#include "abyss/core/eviction_policy.h"
#include "abyss/core/ops.h"
#include "abyss/core/queue_entry.h"
#include "abyss/core/resp_types.h"
#include "abyss/core/string_hash.h"
#include "abyss/core/types.h"
#include "mock_queue.h"
#include "temp_dir.h"
#include "test_clock.h"

namespace abyss::consumer {
namespace {

core::SteadyTime ReadDeadline() { return core::SteadyClock::now() + std::chrono::seconds(10); }

using namespace std::chrono_literals;
using ::testing::_;
using ::testing::NiceMock;
using ::testing::Return;

constexpr core::ShardId kShard = 0;
// T, the seeded key's TTL, in ms since the epoch.
constexpr uint64_t kTtlMs = 1'700'000'000'000;

core::WallTime AtMs(uint64_t ms) { return core::WallTime{std::chrono::milliseconds(ms)}; }

core::QueueEntry Entry(core::SequenceId seq, uint64_t appended_at_ms,
                       std::vector<std::string> args) {
  return core::QueueEntry{
      .seq = seq,
      .appended_at = AtMs(appended_at_ms),
      .payload = core::entry::Write{.cmd = core::RespCommand{.args = std::move(args)}},
  };
}

core::SteadyTime LoadDeadline() { return core::SteadyClock::now() + 5s; }

// The cold side of one shard over an in-memory log. The store's TTL
// scanner reads the consumer's log clock, as the server wires it.
class ColdShard {
 public:
  ColdShard(core::WallTime wall_now, std::vector<core::QueueEntry> log)
      : log_(std::move(log)),
        // Keys under "kj" reach their eviction deadline 10 s after absorb,
        // ahead of everything else's 30 s quiet window.
        policy_(core::EvictionTTL{86400}, {{.prefix = "kj", .eviction = core::EvictionTTL{310}}}) {
    clock_.SetWall(wall_now);
    ON_CALL(queue_, Read(_, _, _, _, _))
        .WillByDefault([this](core::ShardId, core::SequenceId from, size_t max, core::Duration,
                              core::Durability) {
          return core::Result<std::vector<core::QueueEntry>>(testing::ReadFromLog(log_, from, max));
        });
    ON_CALL(queue_, CommitOffset(_, _, _)).WillByDefault(Return(core::Result<void>{}));
    ON_CALL(queue_, CommittedOffset(_, _))
        .WillByDefault(Return(core::Result<std::optional<core::SequenceId>>(std::nullopt)));

    cold::backends::RocksdbConfig config{
        .data_path = dir_.Sub("cold").string(),
        .shard_count = 1,
        .log_clock = [this](core::ShardId) -> uint64_t {
          return consumer_ ? consumer_->LogClockMs() : 0;
        },
    };
    config.ttl_scanner_mode = cold::TtlScanner::ExecutionMode::kManualTick;
    config.ttl_scanner.base_sample_size = 200;
    config.ttl_scanner.max_sample_size = 200;
    config.ttl_scanner_hooks = cold::TtlScanner::Hooks{};
    config.ttl_scanner_hooks->steady_clock = core::DefaultSteadyClock;
    config.ttl_scanner_hooks->cpu_clock = [] { return std::chrono::nanoseconds{0}; };
    config.ttl_scanner_hooks->disk_usage = []() -> core::Result<double> { return 0.0; };
    auto created = cold::backends::RocksdbStore::Create(std::move(config));
    EXPECT_TRUE(created.has_value()) << created.error().message();
    cold_ = std::move(*created);

    ColdConsumer::Config cfg;
    cfg.quiet_threshold = 30s;
    cfg.jitter_fraction = 0.0;
    cfg.rng_seed = 1;
    consumer_ =
        std::make_unique<ColdConsumer>(queue_, *cold_, kShard, cfg, policy_, clock_.SteadyFn());
  }

  ColdShard(const ColdShard&) = delete;
  ColdShard& operator=(const ColdShard&) = delete;
  ColdShard(ColdShard&&) = delete;
  ColdShard& operator=(ColdShard&&) = delete;
  ~ColdShard() = default;

  cold::backends::RocksdbStore& Cold() { return *cold_; }
  ColdConsumer& Consumer() { return *consumer_; }
  testing::TestClock& Clock() { return clock_; }

  void Seed(std::vector<core::ops::WriteOp> ops) {
    ASSERT_TRUE(cold_->ApplyBatch(ops, 0).has_value());
  }

  // How many sampled records carried a TTL.
  uint64_t Scan() {
    uint64_t with_ttl = 0;
    for (int i = 0; i < 3; ++i) {
      auto report = cold_->RunScannerTickForTesting();
      EXPECT_TRUE(report.has_value());
      if (report.has_value()) with_ttl += report->with_ttl_strings + report->with_ttl_collections;
    }
    return with_ttl;
  }

  // Flushes whatever is due after `after` more of the steady clock.
  void FlushAfter(std::chrono::seconds after) {
    clock_.Advance(after);
    consumer_->Flush();
  }

  void FlushAll() {
    while (consumer_->Buffer().Size() > 0) {
      ASSERT_EQ(consumer_->FlushUnscheduled(), ColdConsumer::FlushOutcome::kProgress);
    }
  }

  std::optional<core::ColdKeyState> Load(std::string_view key) {
    auto loaded = cold_->LoadKey(key, LoadDeadline());
    EXPECT_TRUE(loaded.has_value());
    return loaded.value_or(std::nullopt);
  }

 private:
  testing::TempDir dir_{"cold_log_clock"};
  std::vector<core::QueueEntry> log_;
  testing::TestClock clock_;
  core::EvictionPolicy policy_;
  NiceMock<testing::MockQueue> queue_;
  std::unique_ptr<cold::backends::RocksdbStore> cold_;
  std::unique_ptr<ColdConsumer> consumer_;
};

enum class Step : uint8_t { kScan, kRead, kFlush };
enum class Write : uint8_t { kPersist, kSadd };

struct RaceCase {
  Write write;
  bool j_flushes_first;
  std::array<Step, 3> order;
};

std::string_view StepName(Step step) {
  switch (step) {
    case Step::kScan:
      return " scan";
    case Step::kRead:
      return " read";
    case Step::kFlush:
      return " flush";
  }
  return "";
}

std::string Describe(const RaceCase& c) {
  std::string out = c.write == Write::kPersist ? "PERSIST" : "SADD";
  out += c.j_flushes_first ? ", j flushed first:" : ":";
  for (const Step s : c.order) out += StepName(s);
  return out;
}

// Names the case in ctest, which titles each instance by its value.
void PrintTo(const RaceCase& c, std::ostream* os) { *os << Describe(c); }

std::vector<RaceCase> AllRaces() {
  std::vector<RaceCase> out;
  for (const Write write : {Write::kPersist, Write::kSadd}) {
    for (const bool j_first : {false, true}) {
      std::array<Step, 3> order{Step::kScan, Step::kRead, Step::kFlush};
      do {
        out.push_back({.write = write, .j_flushes_first = j_first, .order = order});
      } while (std::ranges::next_permutation(order).found);
    }
  }
  return out;
}

// k has TTL T in cold. A write decided while k was live, at T - 1, waits
// in the buffer while the wall clock reads T + 10 s. With a write to j
// ("kj") at T + 5 s flushed first, the newest flushed appended_at is past
// T, yet the clock must hold at T - 1 until k's write lands.
class LogClockRaceTest : public ::testing::TestWithParam<RaceCase> {};

TEST_P(LogClockRaceTest, AWriteDecidedWhileItsKeyWasLiveLands) {
  const RaceCase& race = GetParam();
  std::vector<core::QueueEntry> log;
  if (race.write == Write::kPersist) {
    log.push_back(Entry(1, kTtlMs - 1, {"PERSIST", "k"}));
  } else {
    log.push_back(Entry(1, kTtlMs - 1, {"SADD", "k", "b"}));
  }
  if (race.j_flushes_first) log.push_back(Entry(2, kTtlMs + 5000, {"SET", "kj", "v"}));
  ColdShard shard(AtMs(kTtlMs + 10'000), std::move(log));

  std::vector<std::string_view> members = {"a"};
  if (race.write == Write::kPersist) {
    shard.Seed({core::ops::StringSet{.key = "k", .value = "v", .abs_ttl_ms = kTtlMs}});
  } else {
    shard.Seed({core::ops::SetAdd{.key = "k", .members = members},
                core::ops::Expire{.key = "k", .abs_ttl_ms = kTtlMs}});
  }

  // k as its write leaves it.
  const core::ColdKeyState written =
      race.write == Write::kPersist
          ? core::ColdKeyState{.type = core::KeyType::kString, .value = std::string("v")}
          : core::ColdKeyState{.type = core::KeyType::kSet,
                               .value = core::StringSet{"a", "b"},
                               .abs_ttl_ms = static_cast<int64_t>(kTtlMs)};

  ASSERT_EQ(shard.Consumer().Drain(), race.j_flushes_first ? 2U : 1U);
  if (race.j_flushes_first) {
    shard.FlushAfter(15s);
    ASSERT_TRUE(shard.Load("kj").has_value()) << "j did not flush";
    ASSERT_EQ(shard.Consumer().Buffer().Size(), 1U) << "k flushed with j";
  }
  EXPECT_EQ(shard.Consumer().LogClockMs(), kTtlMs - 1);

  bool flushed = false;
  bool scanned_after_flush = false;
  for (const Step step : race.order) {
    switch (step) {
      case Step::kScan: {
        // k sorts first, so the scanner's samples reach it.
        const uint64_t with_ttl = shard.Scan();
        if (!flushed || race.write == Write::kSadd) {
          EXPECT_GT(with_ttl, 0U) << "k went unsampled";
        }
        scanned_after_flush = flushed;
        break;
      }
      case Step::kRead: {
        const auto before = shard.Cold().RecordsForTesting();
        const auto type =
            race.write == Write::kPersist ? core::KeyType::kString : core::KeyType::kSet;
        ASSERT_TRUE(shard.Cold().LoadKeyAs("k", type, ReadDeadline()).has_value());
        EXPECT_EQ(shard.Cold().RecordsForTesting(), before) << "a read wrote";
        break;
      }
      case Step::kFlush: {
        shard.FlushAfter(31s);
        ASSERT_EQ(shard.Consumer().Buffer().Size(), 0U);
        flushed = true;
        EXPECT_EQ(shard.Load("k"), written) << "k was lost or its write was not applied";
        break;
      }
    }
  }

  if (race.write == Write::kSadd && scanned_after_flush && race.j_flushes_first) {
    // j's write at T + 5 s took the clock past T once k's write landed.
    EXPECT_FALSE(shard.Load("k").has_value()) << "k outlived its TTL by the log clock";
  } else {
    EXPECT_EQ(shard.Load("k"), written) << "k was lost";
  }
}

INSTANTIATE_TEST_SUITE_P(EveryOrder, LogClockRaceTest, ::testing::ValuesIn(AllRaces()),
                         [](const ::testing::TestParamInfo<RaceCase>& info) {
                           std::string name = Describe(info.param);
                           std::ranges::replace_if(
                               name, [](char ch) { return ch == ' ' || ch == ',' || ch == ':'; },
                               '_');
                           return name + "_" + std::to_string(info.index);
                         });

// The same log, replayed into cold with the wall clock an hour behind
// and an hour ahead, leaves the same records: cold's state follows the
// log alone, and reads along the way change nothing.
TEST(LogClockReplayTest, TheWallClockLeavesNoTraceInCold) {
  constexpr uint64_t kBase = kTtlMs;
  const std::vector<core::QueueEntry> log{
      Entry(1, kBase, {"SET", "s", "v", "PX", "1800000"}),
      Entry(2, kBase + 1, {"SADD", "set", "a", "b"}),
      Entry(3, kBase + 2, {"EXPIRE", "set", "1800"}),
      Entry(4, kBase + 3, {"HSET", "h", "f", "v", "g", "w"}),
      Entry(5, kBase + 4, {"ZADD", "z", "1", "m", "2", "n"}),
      Entry(6, kBase + 5, {"PEXPIRE", "z", "1000"}),
      Entry(7, kBase + 6, {"SET", "gone", "v", "PX", "10"}),
      Entry(8, kBase + 60'000, {"SADD", "set", "c"}),
      Entry(9, kBase + 60'001, {"PERSIST", "s"}),
      Entry(10, kBase + 60'002, {"HDEL", "h", "f"}),
      Entry(11, kBase + 60'003, {"ZREM", "z", "m"}),
      Entry(12, kBase + 60'004, {"SET", "late", "v", "PX", "5000"}),
  };
  const std::vector<std::string> keys{"s", "set", "h", "z", "gone", "late"};

  const auto replay = [&](std::chrono::hours skew) {
    ColdShard shard(AtMs(kBase) + skew, log);
    const auto read_all = [&shard, &keys] {
      const auto before = shard.Cold().RecordsForTesting();
      for (const auto& key : keys) {
        EXPECT_TRUE(shard.Cold().ProbeKey(key, ReadDeadline()).has_value());
        EXPECT_TRUE(shard.Cold().LoadKey(key, ReadDeadline()).has_value());
        EXPECT_TRUE(shard.Cold().LoadKeyAs(key, core::KeyType::kSet, ReadDeadline()).has_value());
      }
      EXPECT_EQ(shard.Cold().RecordsForTesting(), before) << "a read wrote";
    };
    EXPECT_EQ(shard.Consumer().DrainWithBatch(7), 7U);
    shard.FlushAll();
    read_all();
    EXPECT_EQ(shard.Consumer().DrainWithBatch(100), 5U);
    shard.FlushAll();
    read_all();
    return shard.Cold().RecordsForTesting();
  };

  const auto behind = replay(-1h);
  const auto ahead = replay(1h);
  EXPECT_GT(behind.size(), 1U);
  EXPECT_EQ(behind, ahead);
}

}  // namespace
}  // namespace abyss::consumer
