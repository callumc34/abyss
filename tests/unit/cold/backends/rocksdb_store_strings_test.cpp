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
#include <string>
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

}  // namespace
}  // namespace abyss::cold::backends
