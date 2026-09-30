#include <gtest/gtest.h>

#ifdef _WIN32
#include <windows.h>
#else
#include <unistd.h>
#endif

#include <atomic>
#include <filesystem>
#include <memory>
#include <span>
#include <string>
#include <system_error>
#include <vector>

#include "abyss/cold/backends/rocksdb_store.h"
#include "abyss/core/ops.h"
#include "abyss/core/resp_types.h"
#include "abyss/core/result.h"
#include "abyss/core/shard_router.h"
#include "abyss/core/types.h"

// Per-shard wipe isolation (ADP-010): wiping one shard must not destroy another
// shard's data. These assertions failed under the previous global wipe.

namespace abyss::cold::backends {
namespace {

namespace {
#ifdef _WIN32
uint64_t GetProcessId() { return GetCurrentProcessId(); }
#else
uint64_t GetProcessId() { return getpid(); }
#endif
}  // namespace

constexpr uint32_t kShardCount = 4;

class WipeFixture : public ::testing::Test {
 protected:
  void SetUp() override {
    static std::atomic<int> counter{0};
    path_ = std::filesystem::temp_directory_path() /
            ("abyss_cold_wipe_test_" + std::to_string(GetProcessId()) + "_" +
             std::to_string(counter.fetch_add(1)));
    std::filesystem::remove_all(path_);
    std::filesystem::create_directories(path_);
  }

  void TearDown() override {
    std::error_code ec;
    std::filesystem::remove_all(path_, ec);
  }

  std::unique_ptr<RocksdbStore> OpenStore() {
    RocksdbConfig config;
    config.data_path = path_.string();
    config.shard_count = kShardCount;
    auto store = RocksdbStore::Create(config);
    EXPECT_TRUE(store.has_value()) << (store.has_value() ? "" : store.error().message());
    return std::move(*store);
  }

  // Returns the i-th candidate key (in generation order) that routes to `shard`.
  static std::string KeyForShard(core::ShardId shard, int nth) {
    int seen = 0;
    for (int i = 0;; ++i) {
      std::string candidate = "wk-" + std::to_string(i);
      if (core::ComputeShard(candidate, kShardCount) == shard) {
        if (seen == nth) return candidate;
        ++seen;
      }
    }
  }

  // Writes one record of every type under distinct keys that route to `shard`.
  struct TypedKeys {
    std::string str;
    std::string hash;
    std::string set;
    std::string zset;
  };

  TypedKeys SeedAllTypes(RocksdbStore& store, core::ShardId shard) {
    TypedKeys keys{KeyForShard(shard, 0), KeyForShard(shard, 1), KeyForShard(shard, 2),
                   KeyForShard(shard, 3)};

    std::vector<std::string_view> set_members = {"m1", "m2"};
    std::vector<core::ops::HashSet::FieldValue> hash_fields = {{.field = "f", .value = "hv"}};
    std::vector<core::ops::ZsetAdd::Entry> zset_entries = {{.score = 1.5, .member = "zm"}};

    std::vector<core::ops::WriteOp> ops = {
        core::ops::StringSet{.key = keys.str, .value = "sv"},
        core::ops::HashSet{.key = keys.hash, .fields = hash_fields},
        core::ops::SetAdd{.key = keys.set, .members = set_members},
        core::ops::ZsetAdd{.key = keys.zset, .entries = zset_entries},
    };
    EXPECT_TRUE(store.ApplyBatch(ops, 0).has_value());
    return keys;
  }

  // Asserts every type's record for `keys` is present (true) or absent (false).
  // Exercises the zset score-index CF via ZRANGEBYSCORE, not just the member CF.
  void ExpectAllTypes(RocksdbStore& store, const TypedKeys& keys, bool present) {
    auto str = store.Exec(core::ops::StringGet{.key = keys.str});
    ASSERT_TRUE(str.has_value());
    EXPECT_EQ(!str->IsNull(), present) << "string " << keys.str;

    auto hash = store.Exec(core::ops::HashLen{.key = keys.hash});
    ASSERT_TRUE(hash.has_value());
    EXPECT_EQ(hash->AsInteger() > 0, present) << "hash " << keys.hash;

    auto set = store.Exec(core::ops::SetCard{.key = keys.set});
    ASSERT_TRUE(set.has_value());
    EXPECT_EQ(set->AsInteger() > 0, present) << "set " << keys.set;

    auto zcard = store.Exec(core::ops::ZsetCard{.key = keys.zset});
    ASSERT_TRUE(zcard.has_value());
    EXPECT_EQ(zcard->AsInteger() > 0, present) << "zset card " << keys.zset;

    // Score-indexed read: confirms the separate zset_score_idx CF slice is
    // wiped (or retained) in step with the primary records.
    auto zrange = store.Exec(
        core::ops::ZsetRange{.key = keys.zset, .min = "-inf", .max = "+inf", .by_score = true});
    ASSERT_TRUE(zrange.has_value());
    ASSERT_TRUE(zrange->IsArray());
    EXPECT_EQ(!zrange->AsArray().empty(), present) << "zset score index " << keys.zset;
  }

  std::filesystem::path path_;
};

TEST_F(WipeFixture, WipingOneShardLeavesOtherShardsIntact) {
  auto store = OpenStore();
  constexpr core::ShardId kShardA = 0;
  constexpr core::ShardId kShardB = 1;

  const auto keys_a = SeedAllTypes(*store, kShardA);
  const auto keys_b = SeedAllTypes(*store, kShardB);

  ASSERT_TRUE(store->Wipe(kShardA).has_value());

  // The whole point: shard A is gone, shard B is untouched.
  ExpectAllTypes(*store, keys_a, /*present=*/false);
  ExpectAllTypes(*store, keys_b, /*present=*/true);
}

TEST_F(WipeFixture, WipeRemovesEveryTypeInTheShard) {
  auto store = OpenStore();
  constexpr core::ShardId kShard = 2;

  const auto keys = SeedAllTypes(*store, kShard);
  ExpectAllTypes(*store, keys, /*present=*/true);

  ASSERT_TRUE(store->Wipe(kShard).has_value());
  ExpectAllTypes(*store, keys, /*present=*/false);
}

TEST_F(WipeFixture, WipeIsIdempotent) {
  auto store = OpenStore();
  constexpr core::ShardId kShard = 0;

  const auto keys = SeedAllTypes(*store, kShard);
  ASSERT_TRUE(store->Wipe(kShard).has_value());
  // A replayed Flush re-wipes the same slice; it must stay a clean no-op.
  ASSERT_TRUE(store->Wipe(kShard).has_value());
  ExpectAllTypes(*store, keys, /*present=*/false);
}

TEST_F(WipeFixture, WipingEmptyShardIsANoOp) {
  auto store = OpenStore();
  const auto keys_b = SeedAllTypes(*store, /*shard=*/1);

  // Shard 3 never received data; wiping it must succeed and disturb nothing.
  ASSERT_TRUE(store->Wipe(/*shard=*/3).has_value());
  ExpectAllTypes(*store, keys_b, /*present=*/true);
}

TEST_F(WipeFixture, WipedShardAcceptsFreshWritesAfterward) {
  auto store = OpenStore();
  constexpr core::ShardId kShard = 0;

  const auto keys = SeedAllTypes(*store, kShard);
  ASSERT_TRUE(store->Wipe(kShard).has_value());

  // Post-wipe writes (higher seqno) must not be shadowed by the range tombstone.
  std::vector<core::ops::WriteOp> ops = {core::ops::StringSet{.key = keys.str, .value = "again"}};
  ASSERT_TRUE(store->ApplyBatch(ops, 0).has_value());

  auto got = store->Exec(core::ops::StringGet{.key = keys.str});
  ASSERT_TRUE(got.has_value());
  EXPECT_EQ(got->AsString(), "again");
}

}  // namespace
}  // namespace abyss::cold::backends
