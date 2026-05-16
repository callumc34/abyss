#include "abyss/hot/sharded_hot_store.h"

#include <gtest/gtest.h>

#include <string>
#include <thread>
#include <vector>

#include "abyss/core/eviction_policy.h"
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
    auto result = store_.Apply(core::ops::WriteOp{op});
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
  ASSERT_TRUE(store_.Apply(core::ops::WriteOp{del_op}).has_value());
  EXPECT_FALSE(GetString("x").has_value());
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

// --- Flush ---

TEST_F(ShardedHotStoreTest, FlushClearsAllShards) {
  for (int i = 0; i < 100; ++i) {
    SetString("key:" + std::to_string(i), "val");
  }
  ASSERT_TRUE(store_.Flush().has_value());

  auto stats = store_.Stats();
  EXPECT_EQ(stats->key_count, 0U);
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
  ASSERT_TRUE(short_store.Apply(core::ops::WriteOp{op}).has_value());

  clock_.Advance(1100ms);
  auto evicted = short_store.EvictExpired(clock_.SteadyNow());
  EXPECT_GE(evicted, 1U);
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
      (void)store_.Apply(core::ops::WriteOp{op});
    }
  });

  std::thread reader([this]() {
    for (int i = 0; i < 200; ++i) {
      (void)GetString("w:" + std::to_string(i));
    }
  });

  writer.join();
  reader.join();
}

TEST_F(ShardedHotStoreTest, DrainAccessBuffersConcurrency) {
  for (int i = 0; i < 50; ++i) {
    SetString("d:" + std::to_string(i), "val");
  }

  std::thread reader([this]() {
    for (int i = 0; i < 50; ++i) {
      (void)GetString("d:" + std::to_string(i));
    }
  });

  std::thread drainer([this]() { store_.DrainAccessBuffers(clock_.SteadyNow()); });

  reader.join();
  drainer.join();
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
    ASSERT_TRUE(store.Apply(core::ops::WriteOp{op}).has_value());
  }

  // session:a expires at 1s — visible after advancing past 1s.
  clock.Advance(1100ms);
  auto evicted = store.EvictExpired(clock.SteadyNow());
  EXPECT_EQ(evicted, 1U) << "only session:a should be evicted at 1.1s";

  core::ops::StringGet get_session{.key = "session:a"};
  EXPECT_FALSE(store.Exec(core::ops::ReadOp{get_session}).has_value());
  core::ops::StringGet get_ephemeral{.key = "ephemeral:b"};
  EXPECT_TRUE(store.Exec(core::ops::ReadOp{get_ephemeral}).has_value());
  core::ops::StringGet get_other{.key = "other:c"};
  EXPECT_TRUE(store.Exec(core::ops::ReadOp{get_other}).has_value());
}

// Regression for the access-refresh bug: DrainAccessBuffers must extend the
// deadline by the per-key cached eviction, not by a single global value.
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
  ASSERT_TRUE(store.Apply(core::ops::WriteOp{write_op}).has_value());

  // Read after 1s — buffered as an access — then drain to refresh.
  clock.Advance(1s);
  core::ops::StringGet read_op{.key = "session:k"};
  ASSERT_TRUE(store.Exec(core::ops::ReadOp{read_op}).has_value());
  store.DrainAccessBuffers(clock.SteadyNow());

  // 1.5s after the refresh, total elapsed = 2.5s. If refresh had used the
  // default (24h), the key would survive trivially. The contract under test
  // is that refresh used the per-key 2s eviction: deadline is now+2s = 3.5s
  // since write start, so the key is still alive at 2.5s but evicts at 3.5s+.
  clock.Advance(1500ms);
  EXPECT_EQ(store.EvictExpired(clock.SteadyNow()), 0U)
      << "key should still be alive at refreshed_at + 1.5s";
  clock.Advance(600ms);
  EXPECT_EQ(store.EvictExpired(clock.SteadyNow()), 1U)
      << "key must evict by refreshed_at + 2.1s (per-prefix eviction is 2s)";
}

}  // namespace
}  // namespace abyss::hot
