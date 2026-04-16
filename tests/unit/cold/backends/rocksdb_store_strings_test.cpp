#include <gtest/gtest.h>

#include <atomic>
#include <filesystem>
#include <memory>
#include <string>
#include <system_error>
#include <vector>

#include "abyss/cold/backends/rocksdb_store.h"
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

  static core::RespCommand Cmd(std::vector<std::string> args) {
    return core::RespCommand{std::move(args)};
  }

  std::filesystem::path path_;
};

TEST_F(StoreFixture, SetThenGetReturnsValue) {
  auto store = OpenStore();

  auto set_result = store->Exec(Cmd({"SET", "k", "hello"}));
  ASSERT_TRUE(set_result.has_value());
  EXPECT_TRUE(set_result->IsString());
  EXPECT_EQ(set_result->AsString(), "OK");

  auto get_result = store->Exec(Cmd({"GET", "k"}));
  ASSERT_TRUE(get_result.has_value());
  EXPECT_TRUE(get_result->IsString());
  EXPECT_EQ(get_result->AsString(), "hello");
}

TEST_F(StoreFixture, GetMissingKeyReturnsNull) {
  auto store = OpenStore();
  auto result = store->Exec(Cmd({"GET", "missing"}));
  ASSERT_TRUE(result.has_value());
  EXPECT_TRUE(result->IsNull());
}

TEST_F(StoreFixture, SetOverwritesExistingValue) {
  auto store = OpenStore();
  ASSERT_TRUE(store->Exec(Cmd({"SET", "k", "first"})).has_value());
  ASSERT_TRUE(store->Exec(Cmd({"SET", "k", "second"})).has_value());

  auto result = store->Exec(Cmd({"GET", "k"}));
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->AsString(), "second");
}

TEST_F(StoreFixture, SetWithEmptyValueRoundTrips) {
  auto store = OpenStore();
  ASSERT_TRUE(store->Exec(Cmd({"SET", "k", ""})).has_value());

  auto result = store->Exec(Cmd({"GET", "k"}));
  ASSERT_TRUE(result.has_value());
  EXPECT_TRUE(result->IsString());
  EXPECT_TRUE(result->AsString().empty());
}

TEST_F(StoreFixture, SetWithLargeValueRoundTrips) {
  auto store = OpenStore();
  const std::string payload(64 * 1024, 'x');
  ASSERT_TRUE(store->Exec(Cmd({"SET", "k", payload})).has_value());

  auto result = store->Exec(Cmd({"GET", "k"}));
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->AsString(), payload);
}

TEST_F(StoreFixture, SetExStoresTtlButReadStillSucceeds) {
  auto store = OpenStore();
  // With EX set, the value still reads back — TTL enforcement is #18.
  ASSERT_TRUE(store->Exec(Cmd({"SET", "k", "v", "EX", "60"})).has_value());

  auto result = store->Exec(Cmd({"GET", "k"}));
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->AsString(), "v");
}

TEST_F(StoreFixture, SetPxAcceptsMilliseconds) {
  auto store = OpenStore();
  ASSERT_TRUE(store->Exec(Cmd({"SET", "k", "v", "PX", "60000"})).has_value());

  auto result = store->Exec(Cmd({"GET", "k"}));
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->AsString(), "v");
}

TEST_F(StoreFixture, SetRejectsUnsupportedOption) {
  auto store = OpenStore();
  auto result = store->Exec(Cmd({"SET", "k", "v", "NX"}));
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code(), core::ErrorCode::kInvalidArgument);
}

TEST_F(StoreFixture, SetRejectsMissingTtlValue) {
  auto store = OpenStore();
  auto result = store->Exec(Cmd({"SET", "k", "v", "EX"}));
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code(), core::ErrorCode::kInvalidArgument);
}

TEST_F(StoreFixture, DelReturnsCountOfExistingKeys) {
  auto store = OpenStore();
  ASSERT_TRUE(store->Exec(Cmd({"SET", "a", "1"})).has_value());
  ASSERT_TRUE(store->Exec(Cmd({"SET", "b", "2"})).has_value());

  auto del = store->Exec(Cmd({"DEL", "a", "b", "missing"}));
  ASSERT_TRUE(del.has_value());
  EXPECT_TRUE(del->IsInteger());
  EXPECT_EQ(del->AsInteger(), 2);

  EXPECT_TRUE(store->Exec(Cmd({"GET", "a"}))->IsNull());
  EXPECT_TRUE(store->Exec(Cmd({"GET", "b"}))->IsNull());
}

TEST_F(StoreFixture, DelOnMissingKeyReturnsZero) {
  auto store = OpenStore();
  auto del = store->Exec(Cmd({"DEL", "never-set"}));
  ASSERT_TRUE(del.has_value());
  EXPECT_EQ(del->AsInteger(), 0);
}

TEST_F(StoreFixture, PersistenceAcrossReopen) {
  {
    auto store = OpenStore();
    ASSERT_TRUE(store->Exec(Cmd({"SET", "k", "persistent"})).has_value());
  }
  auto store = OpenStore();
  auto result = store->Exec(Cmd({"GET", "k"}));
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->AsString(), "persistent");
}

TEST_F(StoreFixture, BinaryUnsafeKeyRoundTrips) {
  auto store = OpenStore();
  const std::string key{'a', '\x00', 'b', '\xFF'};
  ASSERT_TRUE(store->Exec(Cmd({"SET", key, "v"})).has_value());

  auto result = store->Exec(Cmd({"GET", key}));
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->AsString(), "v");
}

TEST_F(StoreFixture, UnknownCommandReturnsError) {
  auto store = OpenStore();
  auto result = store->Exec(Cmd({"HSET", "h", "f", "v"}));
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code(), core::ErrorCode::kInvalidArgument);
}

TEST_F(StoreFixture, CommandNamesAreCaseInsensitive) {
  auto store = OpenStore();
  ASSERT_TRUE(store->Exec(Cmd({"set", "k", "v"})).has_value());
  auto result = store->Exec(Cmd({"get", "k"}));
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->AsString(), "v");
}

TEST_F(StoreFixture, StatsReflectsWrites) {
  auto store = OpenStore();
  auto before = store->Stats();
  ASSERT_TRUE(before.has_value());

  ASSERT_TRUE(store->Exec(Cmd({"SET", "a", "1"})).has_value());
  ASSERT_TRUE(store->Exec(Cmd({"SET", "b", "2"})).has_value());
  ASSERT_TRUE(store->Compact().has_value());

  auto after = store->Stats();
  ASSERT_TRUE(after.has_value());
  EXPECT_GE(after->key_count, before->key_count);
}

TEST_F(StoreFixture, CompactSucceeds) {
  auto store = OpenStore();
  ASSERT_TRUE(store->Exec(Cmd({"SET", "k", "v"})).has_value());
  EXPECT_TRUE(store->Compact().has_value());
}

TEST_F(StoreFixture, ApplyBatchStillUnimplemented) {
  auto store = OpenStore();
  const std::vector<core::RespCommand> cmds;
  auto result = store->ApplyBatch(cmds);
  ASSERT_FALSE(result.has_value());
}

}  // namespace
}  // namespace abyss::cold::backends
