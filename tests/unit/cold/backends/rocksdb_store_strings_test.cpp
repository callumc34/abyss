#include <gtest/gtest.h>

#ifdef _WIN32
#include <windows.h>
#else
#include <unistd.h>
#endif

#include <atomic>
#include <chrono>
#include <filesystem>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <system_error>
#include <vector>

#include "abyss/cold/backends/rocksdb_store.h"
#include "abyss/core/ops.h"
#include "abyss/core/resp_types.h"
#include "abyss/core/result.h"
#include "cold_read.h"

namespace abyss::cold::backends {
namespace {

namespace {
#ifdef _WIN32
uint64_t GetProcessId() { return GetCurrentProcessId(); }
#else
uint64_t GetProcessId() { return getpid(); }
#endif
}  // namespace

class StoreFixture : public ::testing::Test {
 protected:
  void SetUp() override {
    static std::atomic<int> counter{0};
    auto base = std::filesystem::temp_directory_path();
    path_ = base / ("abyss_cold_strings_test_" + std::to_string(GetProcessId()) + "_" +
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
    auto store = RocksdbStore::Create(config);
    EXPECT_TRUE(store.has_value()) << (store.has_value() ? "" : store.error().message());
    return std::move(*store);
  }

  static core::ops::ReadOp GetOp(std::string_view key) { return core::ops::StringGet{.key = key}; }

  // NOLINTNEXTLINE(cppcoreguidelines-non-private-member-variables-in-classes)
  std::filesystem::path path_;
};

TEST_F(StoreFixture, SetThenGetReturnsValue) {
  auto store = OpenStore();

  std::string key = "k";
  std::string val = "hello";
  core::ops::WriteOp set_op = core::ops::StringSet{.key = key, .value = val};
  auto set_result = store->ApplyBatch(std::span{&set_op, 1}, 0);
  ASSERT_TRUE(set_result.has_value()) << set_result.error().message();

  auto get_result = abyss::testing::ColdRead(*store, GetOp(key));
  ASSERT_TRUE(get_result.has_value());
  EXPECT_TRUE(get_result->IsBulkString());
  EXPECT_EQ(get_result->AsString(), "hello");
}

TEST_F(StoreFixture, GetMissingKeyReturnsNull) {
  auto store = OpenStore();
  auto result = abyss::testing::ColdRead(*store, GetOp("missing"));
  ASSERT_TRUE(result.has_value());
  EXPECT_TRUE(result->IsNull());
}

TEST_F(StoreFixture, SetOverwritesExistingValue) {
  auto store = OpenStore();

  std::string key = "k";
  std::string val1 = "first";
  std::string val2 = "second";
  core::ops::WriteOp op1 = core::ops::StringSet{.key = key, .value = val1};
  ASSERT_TRUE(store->ApplyBatch(std::span{&op1, 1}, 0).has_value());

  core::ops::WriteOp op2 = core::ops::StringSet{.key = key, .value = val2};
  ASSERT_TRUE(store->ApplyBatch(std::span{&op2, 1}, 0).has_value());

  auto result = abyss::testing::ColdRead(*store, GetOp(key));
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->AsString(), "second");
}

TEST_F(StoreFixture, SetWithEmptyValueRoundTrips) {
  auto store = OpenStore();

  std::string key = "k";
  std::string val;
  core::ops::WriteOp op = core::ops::StringSet{.key = key, .value = val};
  ASSERT_TRUE(store->ApplyBatch(std::span{&op, 1}, 0).has_value());

  auto result = abyss::testing::ColdRead(*store, GetOp(key));
  ASSERT_TRUE(result.has_value());
  EXPECT_TRUE(result->IsBulkString());
  EXPECT_TRUE(result->AsString().empty());
}

TEST_F(StoreFixture, SetWithLargeValueRoundTrips) {
  auto store = OpenStore();

  std::string key = "k";
  const std::string payload(size_t{64} * 1024, 'x');
  core::ops::WriteOp op = core::ops::StringSet{.key = key, .value = payload};
  ASSERT_TRUE(store->ApplyBatch(std::span{&op, 1}, 0).has_value());

  auto result = abyss::testing::ColdRead(*store, GetOp(key));
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->AsString(), payload);
}

TEST_F(StoreFixture, SetWithFutureTtlRoundTrips) {
  auto store = OpenStore();

  const uint64_t now_ms =
      static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(
                                std::chrono::system_clock::now().time_since_epoch())
                                .count());
  std::string key = "k";
  std::string val = "v";
  core::ops::WriteOp op =
      core::ops::StringSet{.key = key, .value = val, .abs_ttl_ms = now_ms + 60'000};
  ASSERT_TRUE(store->ApplyBatch(std::span{&op, 1}, 0).has_value());

  auto result = abyss::testing::ColdRead(*store, GetOp(key));
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->AsString(), "v");
}

TEST_F(StoreFixture, DelRemovesExistingKeys) {
  auto store = OpenStore();

  std::string ka = "a";
  std::string va = "1";
  std::string kb = "b";
  std::string vb = "2";
  core::ops::WriteOp op_a = core::ops::StringSet{.key = ka, .value = va};
  core::ops::WriteOp op_b = core::ops::StringSet{.key = kb, .value = vb};
  ASSERT_TRUE(store->ApplyBatch(std::span{&op_a, 1}, 0).has_value());
  ASSERT_TRUE(store->ApplyBatch(std::span{&op_b, 1}, 0).has_value());

  const core::ops::WriteOp del = core::ops::Del{.keys = {"a", "b", "missing"}};
  ASSERT_TRUE(store->ApplyBatch(std::span(&del, 1), 0).has_value());

  EXPECT_TRUE(abyss::testing::ColdRead(*store, GetOp("a"))->IsNull());
  EXPECT_TRUE(abyss::testing::ColdRead(*store, GetOp("b"))->IsNull());
}

TEST_F(StoreFixture, PersistenceAcrossReopen) {
  std::string key = "k";
  std::string val = "persistent";
  {
    auto store = OpenStore();
    core::ops::WriteOp op = core::ops::StringSet{.key = key, .value = val};
    ASSERT_TRUE(store->ApplyBatch(std::span{&op, 1}, 0).has_value());
  }
  auto store = OpenStore();
  auto result = abyss::testing::ColdRead(*store, GetOp(key));
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->AsString(), "persistent");
}

TEST_F(StoreFixture, BinaryUnsafeKeyRoundTrips) {
  auto store = OpenStore();

  const std::string key{'a', '\x00', 'b', '\xFF'};
  std::string val = "v";
  core::ops::WriteOp op = core::ops::StringSet{.key = key, .value = val};
  ASSERT_TRUE(store->ApplyBatch(std::span{&op, 1}, 0).has_value());

  auto result = abyss::testing::ColdRead(*store, GetOp(key));
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->AsString(), "v");
}

TEST_F(StoreFixture, StatsReflectsWrites) {
  auto store = OpenStore();
  auto before = store->Stats();
  ASSERT_TRUE(before.has_value());

  std::string ka = "a";
  std::string va = "1";
  std::string kb = "b";
  std::string vb = "2";
  core::ops::WriteOp op_a = core::ops::StringSet{.key = ka, .value = va};
  core::ops::WriteOp op_b = core::ops::StringSet{.key = kb, .value = vb};
  ASSERT_TRUE(store->ApplyBatch(std::span{&op_a, 1}, 0).has_value());
  ASSERT_TRUE(store->ApplyBatch(std::span{&op_b, 1}, 0).has_value());
  ASSERT_TRUE(store->Compact().has_value());

  auto after = store->Stats();
  ASSERT_TRUE(after.has_value());
  EXPECT_GE(after->key_count, before->key_count);
}

TEST_F(StoreFixture, CompactSucceeds) {
  auto store = OpenStore();

  std::string key = "k";
  std::string val = "v";
  core::ops::WriteOp op = core::ops::StringSet{.key = key, .value = val};
  ASSERT_TRUE(store->ApplyBatch(std::span{&op, 1}, 0).has_value());
  EXPECT_TRUE(store->Compact().has_value());
}

TEST_F(StoreFixture, ApplyBatchMultipleOps) {
  auto store = OpenStore();

  std::string ka = "a";
  std::string va = "1";
  std::string kb = "b";
  std::string vb = "2";
  std::vector<core::ops::WriteOp> ops = {
      core::ops::StringSet{.key = ka, .value = va},
      core::ops::StringSet{.key = kb, .value = vb},
  };
  auto result = store->ApplyBatch(ops, 0);
  ASSERT_TRUE(result.has_value()) << result.error().message();

  EXPECT_EQ(abyss::testing::ColdRead(*store, GetOp("a"))->AsString(), "1");
  EXPECT_EQ(abyss::testing::ColdRead(*store, GetOp("b"))->AsString(), "2");
}

// A DEL leads every flagged add, so a DEL of an absent key, the common
// case, must write nothing; one of a present key still deletes it,
// whether the batch or the store holds it.
TEST_F(StoreFixture, ADelWritesATombstoneOnlyForAKeyThatMayExist) {
  auto store = OpenStore();
  const std::vector<std::string_view> members{"m"};
  const std::vector<core::ops::WriteOp> create = {
      core::ops::Del{.keys = {"absent"}},
      core::ops::SetAdd{.key = "absent", .members = members},
  };
  ASSERT_TRUE(store->ApplyBatch(create, 1).has_value());
  EXPECT_EQ(store->MemtableDeletesForTesting(), 0U) << "a DEL of an absent key wrote a tombstone";

  const core::ops::WriteOp set = core::ops::StringSet{.key = "k", .value = "v"};
  ASSERT_TRUE(store->ApplyBatch(std::span{&set, 1}, 2).has_value());
  const core::ops::WriteOp del = core::ops::Del{.keys = {"k"}};
  ASSERT_TRUE(store->ApplyBatch(std::span{&del, 1}, 3).has_value());
  EXPECT_TRUE(abyss::testing::ColdRead(*store, GetOp("k"))->IsNull());

  const std::vector<core::ops::WriteOp> set_then_del = {
      core::ops::StringSet{.key = "j", .value = "v"},
      core::ops::Del{.keys = {"j"}},
  };
  ASSERT_TRUE(store->ApplyBatch(set_then_del, 4).has_value());
  EXPECT_TRUE(abyss::testing::ColdRead(*store, GetOp("j"))->IsNull());
  EXPECT_EQ(store->MemtableDeletesForTesting(), 2U);
}

// The cheap DEL holds only if a key in an SST that no cached block
// holds probes as maybe, never absent. Each store here is reopened over
// SSTs a Compact wrote, so its block cache starts cold.
class ColdCacheTest : public StoreFixture {
 protected:
  std::unique_ptr<RocksdbStore> FlushedAndReopened(std::span<const core::ops::WriteOp> ops) {
    // An open reads the format record, which sorts last: filler keeps
    // its data block apart from the ones holding `ops`' keys.
    const std::string filler(1024, 'f');
    constexpr int kFillers = 16;
    std::vector<std::string> filler_keys;
    filler_keys.reserve(kFillers);
    for (int i = 0; i < kFillers; ++i) filler_keys.push_back("~filler" + std::to_string(i));
    std::vector<core::ops::WriteOp> all(ops.begin(), ops.end());
    for (const auto& key : filler_keys) {
      all.emplace_back(core::ops::StringSet{.key = key, .value = filler});
    }
    {
      auto store = OpenStore();
      EXPECT_TRUE(store->ApplyBatch(all, 1).has_value());
      EXPECT_TRUE(store->Compact().has_value());
    }
    auto store = OpenStore();
    EXPECT_EQ(store->MemtableEntriesForTesting(), 0U) << "a write is not only in an SST";
    return store;
  }
};

TEST_F(ColdCacheTest, ADelOfAStringOnlyInAnSstWritesItsTombstone) {
  const core::ops::WriteOp set = core::ops::StringSet{.key = "k", .value = "v"};
  auto store = FlushedAndReopened(std::span{&set, 1});
  const core::ops::WriteOp del = core::ops::Del{.keys = {"k"}};
  ASSERT_TRUE(store->ApplyBatch(std::span{&del, 1}, 2).has_value());
  EXPECT_EQ(store->MemtableDeletesForTesting(), 1U);
  EXPECT_TRUE(abyss::testing::ColdRead(*store, GetOp("k"))->IsNull());
}

TEST_F(ColdCacheTest, AnAddOverAStringOnlyInAnSstReadsAndClearsIt) {
  const core::ops::WriteOp set = core::ops::StringSet{.key = "k", .value = "v"};
  auto store = FlushedAndReopened(std::span{&set, 1});
  const std::vector<std::string_view> members{"m"};
  const core::ops::WriteOp add = core::ops::SetAdd{.key = "k", .members = members};
  ASSERT_TRUE(store->ApplyBatch(std::span{&add, 1}, 2).has_value());
  ASSERT_EQ(store->MemtableDeletesForTesting(), 1U) << "the string was left";
  auto member = abyss::testing::ColdRead(*store, core::ops::SetIsMember{.key = "k", .member = "m"});
  ASSERT_TRUE(member.has_value());
  EXPECT_EQ(member->AsInteger(), 1);
}

// Within the SSTs' key range, so only a bloom filter can rule it out.
TEST_F(ColdCacheTest, ADelOfAnAbsentKeyAmongSstsWritesNothing) {
  constexpr int kKeys = 200;
  std::vector<std::string> keys;
  keys.reserve(kKeys);
  for (int i = 0; i < kKeys; ++i) {
    const std::string digits = std::to_string(i);
    keys.push_back("k" + std::string(3 - digits.size(), '0') + digits);
  }
  std::vector<core::ops::WriteOp> sets;
  sets.reserve(keys.size());
  for (const auto& key : keys) sets.emplace_back(core::ops::StringSet{.key = key, .value = "v"});
  auto store = FlushedAndReopened(sets);
  const core::ops::WriteOp del = core::ops::Del{.keys = {"k100x"}};
  ASSERT_TRUE(store->ApplyBatch(std::span{&del, 1}, 2).has_value());
  EXPECT_EQ(store->MemtableDeletesForTesting(), 0U) << "the filter did not answer the probe";
}

}  // namespace
}  // namespace abyss::cold::backends
