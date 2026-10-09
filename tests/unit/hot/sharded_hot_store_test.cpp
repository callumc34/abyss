#include "abyss/hot/sharded_hot_store.h"

#include <gtest/gtest.h>

#include <atomic>
#include <span>
#include <string>
#include <thread>
#include <vector>

#include "abyss/core/eviction_policy.h"
#include "abyss/core/types.h"
#include "latch.h"
#include "test_clock.h"

namespace abyss::hot {
namespace {

using namespace std::chrono_literals;

class ShardedHotStoreTest : public ::testing::Test {
 protected:
  // NOLINTBEGIN(cppcoreguidelines-non-private-member-variables-in-classes)
  abyss::testing::TestClock clock_;
  core::EvictionPolicy policy_{core::EvictionTTL{86400}};
  ShardedHotStore store_{ShardedHotStoreConfig{
      .max_memory_bytes = 64UL * 1024 * 1024,
      .shard_count = 8,
      .eviction_policy = &policy_,
      .steady_clock = clock_.SteadyFn(),
      .wall_clock = clock_.WallFn(),
  }};
  // NOLINTEND(cppcoreguidelines-non-private-member-variables-in-classes)

  void SetString(std::string_view key, std::string_view value) {
    core::ops::StringSet op{.key = key, .value = value};
    auto result = store_.Apply(core::ops::WriteOp{op}, /*seq=*/core::kFirstSeq);
    ASSERT_TRUE(result.has_value()) << result.error().message();
  }

  core::Result<core::RespValue> GetString(std::string_view key) {
    core::ops::StringGet op{.key = key};
    return store_.Exec(core::ops::ReadOp{op});
  }
};

// --- Routing correctness ---

TEST_F(ShardedHotStoreTest, SetAndGetVariousKeys) {
  SetString("alpha", "1");
  SetString("beta", "2");
  SetString("gamma", "3");

  auto a = GetString("alpha");
  ASSERT_TRUE(a.has_value());
  EXPECT_EQ(a->AsString(), "1");

  auto b = GetString("beta");
  ASSERT_TRUE(b.has_value());
  EXPECT_EQ(b->AsString(), "2");

  auto c = GetString("gamma");
  ASSERT_TRUE(c.has_value());
  EXPECT_EQ(c->AsString(), "3");
}

TEST_F(ShardedHotStoreTest, GetMissingKey) {
  auto result = GetString("nope");
  EXPECT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code(), core::ErrorCode::kNotFound);
}

// Multi-key MGET/MSET/DEL/EXISTS fan-out lives in the engine now — the store
// only ever sees per-key (or single-element vector) ops. Cross-shard
// aggregation coverage is in tests/component/tiering_engine_test.cpp.

TEST_F(ShardedHotStoreTest, ExistsSingleKey) {
  SetString("a", "1");

  core::ops::Exists op{.keys = {"a"}};
  auto result = store_.Exec(core::ops::ReadOp{op});
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->AsInteger(), 1);

  core::ops::Exists missing{.keys = {"missing"}};
  auto miss = store_.Exec(core::ops::ReadOp{missing});
  ASSERT_TRUE(miss.has_value());
  EXPECT_EQ(miss->AsInteger(), 0);
}

TEST_F(ShardedHotStoreTest, DelSingleKey) {
  SetString("x", "1");
  core::ops::Del del_op{.keys = {"x"}};
  ASSERT_TRUE(store_.Apply(core::ops::WriteOp{del_op}, /*seq=*/1).has_value());
  // DEL tombstones the key: a subsequent read is an authoritative nil, not a
  // miss that would fall through to a lagging overlay.
  auto result = GetString("x");
  ASSERT_TRUE(result.has_value());
  EXPECT_TRUE(result->IsNull());
}

TEST_F(ShardedHotStoreTest, ProbeRoutesPerShard) {
  SetString("live", "1");
  SetString("dead", "1");
  ASSERT_TRUE(
      store_.Apply(core::ops::WriteOp{core::ops::Del{.keys = {"dead"}}}, /*seq=*/2).has_value());

  EXPECT_EQ(store_.Probe("live"), core::HotKeyPresence::kPresent);
  EXPECT_EQ(store_.Probe("dead"), core::HotKeyPresence::kTombstoned);
  EXPECT_EQ(store_.Probe("never"), core::HotKeyPresence::kAbsent);
}

// --- Stats aggregation ---

TEST_F(ShardedHotStoreTest, StatsAggregated) {
  SetString("a", "1");
  SetString("b", "2");
  SetString("c", "3");

  auto stats = store_.Stats();
  ASSERT_TRUE(stats.has_value());
  EXPECT_EQ(stats->key_count, 3U);
  EXPECT_GT(stats->used_bytes, 0U);
}

// --- Wipe ---

TEST_F(ShardedHotStoreTest, WipeClearsOnlyItsShard) {
  for (int i = 0; i < 100; ++i) {
    SetString("key:" + std::to_string(i), "val");
  }
  ASSERT_TRUE(store_.Wipe(0, core::kFirstSeq).has_value());
  const auto after_one = store_.Stats();
  ASSERT_TRUE(after_one.has_value());
  EXPECT_LT(after_one->key_count, 100U);
  EXPECT_GT(after_one->key_count, 0U) << "one shard's wipe cleared the others";

  for (core::ShardId shard = 1; shard < store_.shard_count(); ++shard) {
    ASSERT_TRUE(store_.Wipe(shard, core::kFirstSeq).has_value());
  }
  const auto after_all = store_.Stats();
  ASSERT_TRUE(after_all.has_value());
  EXPECT_EQ(after_all->key_count, 0U);
  EXPECT_FALSE(store_.Wipe(store_.shard_count(), 0).has_value());
}

// --- Eviction ---

TEST_F(ShardedHotStoreTest, EvictExpiredAcrossShards) {
  core::EvictionPolicy short_policy{core::EvictionTTL{1}};
  ShardedHotStore short_store{ShardedHotStoreConfig{
      .max_memory_bytes = 64UL * 1024 * 1024,
      .shard_count = 8,
      .eviction_policy = &short_policy,
      .steady_clock = clock_.SteadyFn(),
      .wall_clock = clock_.WallFn(),
  }};

  core::ops::StringSet op{.key = "temp", .value = "v"};
  ASSERT_TRUE(short_store.Apply(core::ops::WriteOp{op}, /*seq=*/core::kFirstSeq).has_value());

  clock_.Advance(1100ms);
  auto evicted = short_store.EvictExpired(clock_.SteadyNow());
  EXPECT_GE(evicted.Total(), 1U);
}

// --- Concurrency ---

TEST_F(ShardedHotStoreTest, ConcurrentReadsOnDifferentKeys) {
  for (int i = 0; i < 100; ++i) {
    SetString("key:" + std::to_string(i), "val:" + std::to_string(i));
  }

  std::vector<std::thread> threads;
  threads.reserve(4);
  for (int t = 0; t < 4; ++t) {
    threads.emplace_back([this, t]() {
      for (int i = t * 25; i < (t + 1) * 25; ++i) {
        auto result = GetString("key:" + std::to_string(i));
        EXPECT_TRUE(result.has_value());
      }
    });
  }
  for (auto& t : threads) t.join();
}

TEST_F(ShardedHotStoreTest, ConcurrentWriteAndRead) {
  std::thread writer([this]() {
    for (int i = 0; i < 200; ++i) {
      auto key = "w:" + std::to_string(i);
      auto value = std::to_string(i);
      core::ops::StringSet op{.key = key, .value = value};
      EXPECT_TRUE(store_.Apply(core::ops::WriteOp{op}, /*seq=*/core::kFirstSeq).has_value());
    }
  });

  std::thread reader([this]() {
    for (int i = 0; i < 200; ++i) {
      [[maybe_unused]] const auto got = GetString("w:" + std::to_string(i));
    }
  });

  writer.join();
  reader.join();
}

TEST_F(ShardedHotStoreTest, ReadsStampWhileMaintenanceRuns) {
  for (int i = 0; i < 50; ++i) {
    SetString("d:" + std::to_string(i), "val");
  }

  std::thread reader([this]() {
    for (int i = 0; i < 50; ++i) {
      [[maybe_unused]] const auto got = GetString("d:" + std::to_string(i));
    }
  });

  std::thread maintainer([this]() {
    for (int i = 0; i < 10; ++i) {
      store_.SetAccessTime(clock_.SteadyNow() + std::chrono::seconds{i});
      [[maybe_unused]] const auto report = store_.EvictExpired(clock_.SteadyNow());
    }
  });

  reader.join();
  maintainer.join();
}

// Eight readers of one key while another read holds its shard and a
// writer holds another shard: a hit needs only a shared hold, and
// takes no other lock a reader could wait on.
TEST(ShardedHotStoreReadLockTest, HitsOfOneKeyNeedOnlyASharedHold) {
  abyss::testing::TestClock clock;
  ShardedHotStore store{ShardedHotStoreConfig{
      .max_memory_bytes = 64UL * 1024 * 1024,
      .shard_count = 2,
      .steady_clock = clock.SteadyFn(),
      .wall_clock = clock.WallFn(),
  }};
  const std::string hot = "hot";
  const core::ShardId shard = store.ShardOf(hot);
  const core::ShardId other = 1 - shard;
  core::ops::StringSet op{.key = hot, .value = "v"};
  ASSERT_TRUE(store.Apply(core::ops::WriteOp{op}, /*seq=*/core::kFirstSeq).has_value());
  store.SetAccessTime(clock.SteadyNow() + 1s);

  auto reading = store.LockSharedForTesting(shard);
  abyss::testing::Latch writing;
  abyss::testing::Latch read;
  std::thread writer([&store, other, &writing, &read] {
    ShardLocks held = store.LockExclusive(std::span(&other, 1));
    writing.Open();
    read.Wait();
  });
  ASSERT_TRUE(writing.Wait());
  constexpr int kReaders = 8;
  constexpr int kReads = 1000;
  std::atomic<int> hits{0};
  std::atomic<int> finished{0};
  std::vector<std::thread> readers;
  readers.reserve(kReaders);
  for (int r = 0; r < kReaders; ++r) {
    readers.emplace_back([&] {
      for (int i = 0; i < kReads; ++i) {
        if (store.Read(core::ops::ReadOp{core::ops::StringGet{.key = hot}}).result.has_value()) {
          hits.fetch_add(1, std::memory_order_relaxed);
        }
      }
      if (finished.fetch_add(1) + 1 == kReaders) read.Open();
    });
  }
  const bool all_read = read.Wait();
  // Released either way, so a failed run still ends.
  read.Open();
  reading.unlock();
  for (auto& reader : readers) reader.join();
  writer.join();
  EXPECT_TRUE(all_read) << "a hit waited on something the held shard blocks";
  EXPECT_EQ(hits.load(), kReaders * kReads);
}

// --- Access stamps ---

TEST(ShardedHotStoreAccessTest, AReadStampsTheTimeLastSet) {
  abyss::testing::TestClock clock;
  core::EvictionPolicy policy{core::EvictionTTL{2}};
  ShardedHotStore store{ShardedHotStoreConfig{
      .max_memory_bytes = 64UL * 1024 * 1024,
      .shard_count = 1,
      .eviction_policy = &policy,
      .steady_clock = clock.SteadyFn(),
      .wall_clock = clock.WallFn(),
  }};
  for (const std::string_view key : {"read", "idle"}) {
    core::ops::StringSet op{.key = key, .value = "v"};
    ASSERT_TRUE(store.Apply(core::ops::WriteOp{op}, /*seq=*/core::kFirstSeq).has_value());
  }

  clock.Advance(1s);
  // Reads before it stamp the older time: nothing new.
  ASSERT_TRUE(store.Exec(core::ops::ReadOp{core::ops::StringGet{.key = "idle"}}).has_value());
  store.SetAccessTime(clock.SteadyNow());
  for (int i = 0; i < 1000; ++i) {
    ASSERT_TRUE(store.Exec(core::ops::ReadOp{core::ops::StringGet{.key = "read"}}).has_value());
  }

  clock.Advance(1100ms);
  const auto report = store.EvictExpired(clock.SteadyNow());
  EXPECT_EQ(report.by_deadline, 1U) << "idle is past its 2s; read is 1.1s past its read";
  EXPECT_FALSE(store.Exec(core::ops::ReadOp{core::ops::StringGet{.key = "idle"}}).has_value());
  EXPECT_TRUE(store.Exec(core::ops::ReadOp{core::ops::StringGet{.key = "read"}}).has_value());

  clock.Advance(1s);
  EXPECT_EQ(store.EvictExpired(clock.SteadyNow()).by_deadline, 1U);
}

TEST(ShardedHotStoreAccessTest, MetaReadsAreNotUse) {
  abyss::testing::TestClock clock;
  core::EvictionPolicy policy{core::EvictionTTL{2}};
  ShardedHotStore store{ShardedHotStoreConfig{
      .max_memory_bytes = 64UL * 1024 * 1024,
      .shard_count = 1,
      .eviction_policy = &policy,
      .steady_clock = clock.SteadyFn(),
      .wall_clock = clock.WallFn(),
  }};
  core::ops::StringSet op{.key = "k", .value = "v"};
  ASSERT_TRUE(store.Apply(core::ops::WriteOp{op}, /*seq=*/core::kFirstSeq).has_value());

  clock.Advance(1s);
  store.SetAccessTime(clock.SteadyNow());
  ASSERT_TRUE(store.Read(core::ops::ReadOp{core::ops::Exists{.keys = {"k"}}}).result.has_value());
  ASSERT_TRUE(store.Read(core::ops::ReadOp{core::ops::Type{.key = "k"}}).result.has_value());
  ASSERT_TRUE(store.Read(core::ops::ReadOp{core::ops::Ttl{.key = "k"}}).result.has_value());

  clock.Advance(1100ms);
  EXPECT_EQ(store.EvictExpired(clock.SteadyNow()).by_deadline, 1U);
}

// --- Per-prefix eviction (issue #84) ---

// Per-prefix eviction must resolve per key (was a bug when MSET applied one
// eviction across the whole entry list). Now driven by per-key StringSet
// since the engine decomposes MSET upstream.
TEST(ShardedHotStorePrefixEvictionTest, MixedPrefixesResolvePerEntry) {
  abyss::testing::TestClock clock;
  core::EvictionPolicy policy{
      core::EvictionTTL{86400},
      {
          {.prefix = "session:", .eviction = core::EvictionTTL{1}},
          {.prefix = "ephemeral:", .eviction = core::EvictionTTL{1000}},
      },
  };
  ShardedHotStore store{ShardedHotStoreConfig{
      .max_memory_bytes = 16UL * 1024 * 1024,
      .shard_count = 8,
      .eviction_policy = &policy,
      .steady_clock = clock.SteadyFn(),
      .wall_clock = clock.WallFn(),
  }};

  for (const auto& [k, v] : std::initializer_list<std::pair<std::string_view, std::string_view>>{
           {"session:a", "1"}, {"ephemeral:b", "2"}, {"other:c", "3"}}) {
    core::ops::StringSet op{.key = k, .value = v};
    ASSERT_TRUE(store.Apply(core::ops::WriteOp{op}, /*seq=*/core::kFirstSeq).has_value());
  }

  // session:a expires at 1s — visible after advancing past 1s.
  clock.Advance(1100ms);
  auto evicted = store.EvictExpired(clock.SteadyNow());
  EXPECT_EQ(evicted.Total(), 1U) << "only session:a should be evicted at 1.1s";
  EXPECT_EQ(evicted.by_deadline, 1U);
  EXPECT_EQ(evicted.by_ttl, 0U);

  core::ops::StringGet get_session{.key = "session:a"};
  EXPECT_FALSE(store.Exec(core::ops::ReadOp{get_session}).has_value());
  core::ops::StringGet get_ephemeral{.key = "ephemeral:b"};
  EXPECT_TRUE(store.Exec(core::ops::ReadOp{get_ephemeral}).has_value());
  core::ops::StringGet get_other{.key = "other:c"};
  EXPECT_TRUE(store.Exec(core::ops::ReadOp{get_other}).has_value());
}

// Regression for the access-refresh bug: a read must extend the deadline
// by the per-key cached eviction, not by a single global value.
TEST(ShardedHotStorePrefixEvictionTest, RefreshUsesPerKeyEvictionFromEntry) {
  abyss::testing::TestClock clock;
  core::EvictionPolicy policy{
      core::EvictionTTL{86400},
      {{.prefix = "session:", .eviction = core::EvictionTTL{2}}},
  };
  ShardedHotStore store{ShardedHotStoreConfig{
      .max_memory_bytes = 16UL * 1024 * 1024,
      .shard_count = 1,
      .eviction_policy = &policy,
      .steady_clock = clock.SteadyFn(),
      .wall_clock = clock.WallFn(),
  }};

  core::ops::StringSet write_op{.key = "session:k", .value = "v"};
  ASSERT_TRUE(store.Apply(core::ops::WriteOp{write_op}, /*seq=*/core::kFirstSeq).has_value());

  // Read after 1s, stamped with the time then.
  clock.Advance(1s);
  store.SetAccessTime(clock.SteadyNow());
  core::ops::StringGet read_op{.key = "session:k"};
  ASSERT_TRUE(store.Exec(core::ops::ReadOp{read_op}).has_value());

  // 1.5s after the refresh, total elapsed = 2.5s. If refresh had used the
  // default (24h), the key would survive trivially. The contract under test
  // is that refresh used the per-key 2s eviction: deadline is now+2s = 3.5s
  // since write start, so the key is still alive at 2.5s but evicts at 3.5s+.
  clock.Advance(1500ms);
  EXPECT_EQ(store.EvictExpired(clock.SteadyNow()).Total(), 0U)
      << "key should still be alive at refreshed_at + 1.5s";
  clock.Advance(600ms);
  EXPECT_EQ(store.EvictExpired(clock.SteadyNow()).Total(), 1U)
      << "key must evict by refreshed_at + 2.1s (per-prefix eviction is 2s)";
}

}  // namespace
}  // namespace abyss::hot
