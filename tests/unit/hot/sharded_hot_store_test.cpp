#include "abyss/hot/sharded_hot_store.h"

#include <gtest/gtest.h>

#include <string>
#include <thread>
#include <vector>

#include "test_clock.h"

namespace abyss::hot {
namespace {

using namespace std::chrono_literals;

class ShardedHotStoreTest : public ::testing::Test {
 protected:
  // NOLINTBEGIN(cppcoreguidelines-non-private-member-variables-in-classes)
  abyss::testing::TestClock clock_;
  ShardedHotStore store_{ShardedHotStoreConfig{
      .max_memory_bytes = 64UL * 1024 * 1024,
      .shard_count = 8,
      .steady_clock = clock_.SteadyFn(),
      .wall_clock = clock_.WallFn(),
  }};
  // NOLINTEND(cppcoreguidelines-non-private-member-variables-in-classes)
  static constexpr core::EvictionTTL kEviction{86400};

  void SetString(std::string_view key, std::string_view value) {
    core::ops::StringSet op{.key = key, .value = value};
    auto result = store_.Apply(core::ops::WriteOp{op}, kEviction);
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

// --- Multi-key operations ---

TEST_F(ShardedHotStoreTest, MgetAllPresent) {
  SetString("k1", "v1");
  SetString("k2", "v2");
  SetString("k3", "v3");

  core::ops::MultiStringGet op{.keys = {"k1", "k2", "k3"}};
  auto result = store_.Exec(core::ops::ReadOp{op});
  ASSERT_TRUE(result.has_value());
  ASSERT_TRUE(result->IsArray());
  ASSERT_EQ(result->AsArray().size(), 3U);
  EXPECT_EQ(result->AsArray()[0].AsString(), "v1");
  EXPECT_EQ(result->AsArray()[1].AsString(), "v2");
  EXPECT_EQ(result->AsArray()[2].AsString(), "v3");
}

TEST_F(ShardedHotStoreTest, MgetSomeMissing) {
  SetString("k1", "v1");

  core::ops::MultiStringGet op{.keys = {"k1", "missing"}};
  auto result = store_.Exec(core::ops::ReadOp{op});
  ASSERT_TRUE(result.has_value());
  ASSERT_EQ(result->AsArray().size(), 2U);
  EXPECT_EQ(result->AsArray()[0].AsString(), "v1");
  EXPECT_TRUE(result->AsArray()[1].IsNull());
}

TEST_F(ShardedHotStoreTest, ExistsCountsAcrossShards) {
  SetString("a", "1");
  SetString("b", "2");

  core::ops::Exists op{.keys = {"a", "b", "c"}};
  auto result = store_.Exec(core::ops::ReadOp{op});
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->AsInteger(), 2);
}

TEST_F(ShardedHotStoreTest, DelAcrossShards) {
  SetString("x", "1");
  SetString("y", "2");

  core::ops::Del del_op{.keys = {"x", "y"}};
  ASSERT_TRUE(store_.Apply(core::ops::WriteOp{del_op}, kEviction).has_value());

  EXPECT_FALSE(GetString("x").has_value());
  EXPECT_FALSE(GetString("y").has_value());
}

TEST_F(ShardedHotStoreTest, MsetAcrossShards) {
  core::ops::MultiStringSet op{.entries = {
                                   {.key = "a", .value = "1"},
                                   {.key = "b", .value = "2"},
                                   {.key = "c", .value = "3"},
                               }};
  ASSERT_TRUE(store_.Apply(core::ops::WriteOp{op}, kEviction).has_value());

  auto a = GetString("a");
  ASSERT_TRUE(a.has_value());
  EXPECT_EQ(a->AsString(), "1");
  auto c = GetString("c");
  ASSERT_TRUE(c.has_value());
  EXPECT_EQ(c->AsString(), "3");
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
  core::ops::StringSet op{.key = "temp", .value = "v"};
  ASSERT_TRUE(store_.Apply(core::ops::WriteOp{op}, core::EvictionTTL{1}).has_value());

  clock_.Advance(1100ms);
  auto evicted = store_.EvictExpired(clock_.SteadyNow());
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
      (void)store_.Apply(core::ops::WriteOp{op}, kEviction);
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

  std::thread drainer([this]() { store_.DrainAccessBuffers(clock_.SteadyNow(), kEviction); });

  reader.join();
  drainer.join();
}

}  // namespace
}  // namespace abyss::hot
