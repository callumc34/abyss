#include <gtest/gtest.h>
#include <rocksdb/db.h>
#include <rocksdb/options.h>

#include <atomic>
#include <filesystem>
#include <memory>
#include <string>
#include <system_error>
#include <vector>

#include "abyss/cold/backends/rocksdb_store.h"
#include "abyss/cold/format/key_codec.h"
#include "abyss/core/result.h"

namespace abyss::cold::backends {
namespace {

namespace fmt = ::abyss::cold::format;

class TempDir {
 public:
  TempDir() {
    static std::atomic<int> counter{0};
    auto base = std::filesystem::temp_directory_path();
    path_ = base / ("abyss_cold_test_" + std::to_string(counter.fetch_add(1)));
    std::filesystem::remove_all(path_);
    std::filesystem::create_directories(path_);
    path_str_ = path_.string();
  }

  ~TempDir() {
    std::error_code ec;
    std::filesystem::remove_all(path_, ec);
  }

  TempDir(const TempDir&) = delete;
  TempDir& operator=(const TempDir&) = delete;
  TempDir(TempDir&&) = delete;
  TempDir& operator=(TempDir&&) = delete;

  [[nodiscard]] const std::string& path() const { return path_str_; }

 private:
  std::filesystem::path path_;
  std::string path_str_;
};

RocksdbConfig ConfigFor(const TempDir& dir) {
  RocksdbConfig c;
  c.data_path = dir.path();
  return c;
}

// Overwrite the format-version record at the given path with raw bytes. Used
// to simulate an on-disk version mismatch.
void OverwriteFormatVersionRecord(const std::string& path, std::string_view value) {
  rocksdb::Options opts;
  opts.create_if_missing = false;
  std::vector<std::string> cfs;
  ASSERT_TRUE(rocksdb::DB::ListColumnFamilies(opts, path, &cfs).ok());

  std::vector<rocksdb::ColumnFamilyDescriptor> descs;
  descs.reserve(cfs.size());
  for (const auto& name : cfs) {
    descs.emplace_back(name, rocksdb::ColumnFamilyOptions());
  }
  std::vector<rocksdb::ColumnFamilyHandle*> handles;
  std::unique_ptr<rocksdb::DB> db;
  rocksdb::DBOptions db_opts;
  ASSERT_TRUE(rocksdb::DB::Open(db_opts, path, descs, &handles, &db).ok());

  const auto key = fmt::EncodeFormatVersionKey();
  ASSERT_TRUE(
      db->Put(rocksdb::WriteOptions(), handles[0], key, rocksdb::Slice(value.data(), value.size()))
          .ok());

  for (auto* h : handles) {
    db->DestroyColumnFamilyHandle(h);
  }
}

// --- Tests ------------------------------------------------------------------

TEST(RocksdbStoreLifecycleTest, CreateFreshSucceedsAndWritesFormatVersion) {
  TempDir dir;
  auto store = RocksdbStore::Create(ConfigFor(dir));
  ASSERT_TRUE(store.has_value()) << store.error().message();
  ASSERT_NE(*store, nullptr);
  // Destruct explicitly to close the DB before we probe it.
  store->reset();

  // Reopen raw RocksDB and verify the format-version record is present.
  rocksdb::Options opts;
  opts.create_if_missing = false;
  std::vector<std::string> cfs;
  ASSERT_TRUE(rocksdb::DB::ListColumnFamilies(opts, dir.path(), &cfs).ok());
  std::vector<rocksdb::ColumnFamilyDescriptor> descs;
  descs.reserve(cfs.size());
  for (const auto& name : cfs) {
    descs.emplace_back(name, rocksdb::ColumnFamilyOptions());
  }
  std::vector<rocksdb::ColumnFamilyHandle*> handles;
  std::unique_ptr<rocksdb::DB> db;
  rocksdb::DBOptions db_opts;
  ASSERT_TRUE(rocksdb::DB::Open(db_opts, dir.path(), descs, &handles, &db).ok());

  std::string value;
  const auto status =
      db->Get(rocksdb::ReadOptions(), handles[0], fmt::EncodeFormatVersionKey(), &value);
  ASSERT_TRUE(status.ok());
  auto decoded = fmt::DecodeFormatVersionValue(value);
  ASSERT_TRUE(decoded.has_value());
  EXPECT_EQ(*decoded, fmt::kFormatVersion);

  for (auto* h : handles) {
    db->DestroyColumnFamilyHandle(h);
  }
}

TEST(RocksdbStoreLifecycleTest, ReopenSucceedsOnExistingStore) {
  TempDir dir;
  {
    auto store = RocksdbStore::Create(ConfigFor(dir));
    ASSERT_TRUE(store.has_value()) << store.error().message();
  }
  auto store = RocksdbStore::Create(ConfigFor(dir));
  ASSERT_TRUE(store.has_value()) << store.error().message();
}

TEST(RocksdbStoreLifecycleTest, RejectsMismatchedFormatVersion) {
  TempDir dir;
  {
    auto store = RocksdbStore::Create(ConfigFor(dir));
    ASSERT_TRUE(store.has_value()) << store.error().message();
  }

  // Corrupt the format version record to an unrecognised version.
  OverwriteFormatVersionRecord(dir.path(), fmt::EncodeFormatVersionValue(fmt::kFormatVersion + 1));

  auto reopened = RocksdbStore::Create(ConfigFor(dir));
  ASSERT_FALSE(reopened.has_value());
  EXPECT_EQ(reopened.error().code(), core::ErrorCode::kCorruption);
}

TEST(RocksdbStoreLifecycleTest, RejectsTruncatedFormatVersion) {
  TempDir dir;
  {
    auto store = RocksdbStore::Create(ConfigFor(dir));
    ASSERT_TRUE(store.has_value()) << store.error().message();
  }

  // Truncated format version record — too short to decode.
  OverwriteFormatVersionRecord(dir.path(), std::string_view{"\x00", 1});

  auto reopened = RocksdbStore::Create(ConfigFor(dir));
  ASSERT_FALSE(reopened.has_value());
  EXPECT_EQ(reopened.error().code(), core::ErrorCode::kCorruption);
}

TEST(RocksdbStoreLifecycleTest, CreatesMissingDirectory) {
  TempDir dir;
  RocksdbConfig config = ConfigFor(dir);
  config.data_path = (std::filesystem::path(dir.path()) / "nested" / "rocksdb").string();

  auto store = RocksdbStore::Create(config);
  ASSERT_TRUE(store.has_value()) << store.error().message();
  EXPECT_TRUE(std::filesystem::exists(config.data_path));
}

}  // namespace
}  // namespace abyss::cold::backends
