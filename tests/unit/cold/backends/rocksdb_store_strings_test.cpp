#include <gtest/gtest.h>

#include <atomic>
#include <filesystem>
#include <memory>
#include <string>
#include <system_error>
#include <vector>

#include "abyss/cold/backends/rocksdb_store.h"
#include "abyss/core/ops.h"
#include "abyss/core/resp_types.h"
#include "abyss/core/result.h"

namespace abyss::cold::backends {
namespace {

class StoreFixture : public ::testing::Test {
 protected:
  void SetUp() override {
    static std::atomic<int> counter{0};
    auto base = std::filesystem::temp_directory_path();
    path_ = base / ("abyss_cold_strings_test_" + std::to_string(counter.fetch_add(1)));
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

  std::filesystem::path path_;
};

TEST_F(StoreFixture, SetThenGetReturnsValue) {
  auto store = OpenStore();

  std::string key = "k";
  std::string val = "hello";
  core::ops::WriteOp set_op = core::ops::StringSet{.key = key, .value = val};
  auto set_result = store->ApplyBatch(std::span{&set_op, 1});
  ASSERT_TRUE(set_result.has_value()) << set_result.error().message();

  auto get_result = store->Exec(GetOp(key));
  ASSERT_TRUE(get_result.has_value());
  EXPECT_TRUE(get_result->IsBulkString());
  EXPECT_EQ(get_result->AsString(), "hello");
}

TEST_F(StoreFixture, GetMissingKeyReturnsNull) {
  auto store = OpenStore();
  auto result = store->Exec(GetOp("missing"));
  ASSERT_TRUE(result.has_value());
  EXPECT_TRUE(result->IsNull());
}

TEST_F(StoreFixture, SetOverwritesExistingValue) {
  auto store = OpenStore();

  std::string key = "k";
  std::string val1 = "first";
  std::string val2 = "second";
  core::ops::WriteOp op1 = core::ops::StringSet{.key = key, .value = val1};
  ASSERT_TRUE(store->ApplyBatch(std::span{&op1, 1}).has_value());

  core::ops::WriteOp op2 = core::ops::StringSet{.key = key, .value = val2};
  ASSERT_TRUE(store->ApplyBatch(std::span{&op2, 1}).has_value());

  auto result = store->Exec(GetOp(key));
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->AsString(), "second");
}

TEST_F(StoreFixture, SetWithEmptyValueRoundTrips) {
  auto store = OpenStore();

  std::string key = "k";
  std::string val;
  core::ops::WriteOp op = core::ops::StringSet{.key = key, .value = val};
  ASSERT_TRUE(store->ApplyBatch(std::span{&op, 1}).has_value());

  auto result = store->Exec(GetOp(key));
  ASSERT_TRUE(result.has_value());
  EXPECT_TRUE(result->IsBulkString());
  EXPECT_TRUE(result->AsString().empty());
}

TEST_F(StoreFixture, SetWithLargeValueRoundTrips) {
  auto store = OpenStore();

  std::string key = "k";
  const std::string payload(64 * 1024, 'x');
  core::ops::WriteOp op = core::ops::StringSet{.key = key, .value = payload};
  ASSERT_TRUE(store->ApplyBatch(std::span{&op, 1}).has_value());

  auto result = store->Exec(GetOp(key));
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->AsString(), payload);
}

TEST_F(StoreFixture, SetWithTtlStoresButReadsStillSucceed) {
  auto store = OpenStore();

  std::string key = "k";
  std::string val = "v";
  core::ops::WriteOp op = core::ops::StringSet{.key = key, .value = val, .abs_ttl_ms = 99999999};
  ASSERT_TRUE(store->ApplyBatch(std::span{&op, 1}).has_value());

  auto result = store->Exec(GetOp(key));
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->AsString(), "v");
}

TEST_F(StoreFixture, DelReturnsCountOfExistingKeys) {
  auto store = OpenStore();

  std::string ka = "a";
  std::string va = "1";
  std::string kb = "b";
  std::string vb = "2";
  core::ops::WriteOp op_a = core::ops::StringSet{.key = ka, .value = va};
  core::ops::WriteOp op_b = core::ops::StringSet{.key = kb, .value = vb};
  ASSERT_TRUE(store->ApplyBatch(std::span{&op_a, 1}).has_value());
  ASSERT_TRUE(store->ApplyBatch(std::span{&op_b, 1}).has_value());

  core::ops::Del del_op;
  std::string_view keys[] = {"a", "b", "missing"};
  del_op.keys = {std::begin(keys), std::end(keys)};
  auto del = store->ExecDel(del_op);
  ASSERT_TRUE(del.has_value());
  EXPECT_TRUE(del->IsInteger());
  EXPECT_EQ(del->AsInteger(), 2);

  EXPECT_TRUE(store->Exec(GetOp("a"))->IsNull());
  EXPECT_TRUE(store->Exec(GetOp("b"))->IsNull());
}

TEST_F(StoreFixture, DelOnMissingKeyReturnsZero) {
  auto store = OpenStore();

  core::ops::Del del_op;
  std::string_view keys[] = {"never-set"};
  del_op.keys = {std::begin(keys), std::end(keys)};
  auto del = store->ExecDel(del_op);
  ASSERT_TRUE(del.has_value());
  EXPECT_EQ(del->AsInteger(), 0);
}

TEST_F(StoreFixture, PersistenceAcrossReopen) {
  std::string key = "k";
  std::string val = "persistent";
  {
    auto store = OpenStore();
    core::ops::WriteOp op = core::ops::StringSet{.key = key, .value = val};
    ASSERT_TRUE(store->ApplyBatch(std::span{&op, 1}).has_value());
  }
  auto store = OpenStore();
  auto result = store->Exec(GetOp(key));
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->AsString(), "persistent");
}

TEST_F(StoreFixture, BinaryUnsafeKeyRoundTrips) {
  auto store = OpenStore();

  const std::string key{'a', '\x00', 'b', '\xFF'};
  std::string val = "v";
  core::ops::WriteOp op = core::ops::StringSet{.key = key, .value = val};
  ASSERT_TRUE(store->ApplyBatch(std::span{&op, 1}).has_value());

  auto result = store->Exec(GetOp(key));
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
  ASSERT_TRUE(store->ApplyBatch(std::span{&op_a, 1}).has_value());
  ASSERT_TRUE(store->ApplyBatch(std::span{&op_b, 1}).has_value());
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
  ASSERT_TRUE(store->ApplyBatch(std::span{&op, 1}).has_value());
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
  auto result = store->ApplyBatch(ops);
  ASSERT_TRUE(result.has_value()) << result.error().message();

  EXPECT_EQ(store->Exec(GetOp("a"))->AsString(), "1");
  EXPECT_EQ(store->Exec(GetOp("b"))->AsString(), "2");
}

}  // namespace
}  // namespace abyss::cold::backends
