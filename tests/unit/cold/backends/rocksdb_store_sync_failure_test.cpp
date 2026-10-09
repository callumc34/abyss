// A failed sync of RocksDB's WAL is fail-stop: the checkpoint that met
// it fails, and so does every later write, checkpoint and wipe, even
// once the device recovers. A retried sync could otherwise report
// durable what the device dropped (fsyncgate). The store relies on
// paranoid_checks for this; failure-modes.md documents it.

#include <gtest/gtest.h>
#include <rocksdb/env.h>
#include <rocksdb/file_system.h>
#include <rocksdb/io_status.h>

#include <atomic>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "abyss/cold/backends/rocksdb_store.h"
#include "abyss/core/ops.h"
#include "temp_dir.h"

namespace abyss::cold::backends {
namespace {

// A WAL file whose syncs fail while `fail` is set.
class FailingSyncFile : public rocksdb::FSWritableFileOwnerWrapper {
 public:
  FailingSyncFile(std::unique_ptr<rocksdb::FSWritableFile> file, const std::atomic<bool>& fail)
      : FSWritableFileOwnerWrapper(std::move(file)), fail_(fail) {}

  rocksdb::IOStatus Sync(const rocksdb::IOOptions& options, rocksdb::IODebugContext* dbg) override {
    if (fail_.load()) return rocksdb::IOStatus::IOError("injected sync failure");
    return FSWritableFileOwnerWrapper::Sync(options, dbg);
  }
  rocksdb::IOStatus Fsync(const rocksdb::IOOptions& options,
                          rocksdb::IODebugContext* dbg) override {
    if (fail_.load()) return rocksdb::IOStatus::IOError("injected sync failure");
    return FSWritableFileOwnerWrapper::Fsync(options, dbg);
  }

 private:
  const std::atomic<bool>& fail_;
};

class FailingSyncFs : public rocksdb::FileSystemWrapper {
 public:
  explicit FailingSyncFs(const std::atomic<bool>& fail)
      : FileSystemWrapper(rocksdb::FileSystem::Default()), fail_(fail) {}

  static const char* kClassName() { return "FailingSyncFs"; }
  const char* Name() const override { return kClassName(); }

  rocksdb::IOStatus NewWritableFile(const std::string& name, const rocksdb::FileOptions& options,
                                    std::unique_ptr<rocksdb::FSWritableFile>* file,
                                    rocksdb::IODebugContext* dbg) override {
    auto status = target()->NewWritableFile(name, options, file, dbg);
    if (status.ok() && name.ends_with(".log")) {
      *file = std::make_unique<FailingSyncFile>(std::move(*file), fail_);
    }
    return status;
  }

 private:
  const std::atomic<bool>& fail_;
};

std::vector<core::ops::WriteOp> Set(std::string_view key) {
  return {core::ops::StringSet{.key = key, .value = "v"}};
}

TEST(RocksdbSyncFailureTest, AFailedWalSyncFailsEveryLaterWrite) {
  abyss::testing::TempDir dir("cold_sync");
  std::atomic<bool> fail{false};
  const auto fs = std::make_shared<FailingSyncFs>(fail);
  const std::unique_ptr<rocksdb::Env> env = rocksdb::NewCompositeEnv(fs);
  auto store = RocksdbStore::Create(RocksdbConfig{.data_path = dir.String(), .env = env.get()});
  ASSERT_TRUE(store.has_value()) << store.error().message();
  ASSERT_TRUE((*store)->ApplyBatch(Set("a"), 1).has_value());
  ASSERT_TRUE((*store)->Checkpoint(0, 1).has_value());

  ASSERT_TRUE((*store)->ApplyBatch(Set("b"), 2).has_value());
  fail = true;
  EXPECT_FALSE((*store)->Checkpoint(0, 2).has_value()) << "a failed sync reported durable";
  fail = false;
  EXPECT_FALSE((*store)->ApplyBatch(Set("c"), 3).has_value()) << "a write after a failed sync";
  EXPECT_FALSE((*store)->Checkpoint(0, 3).has_value()) << "a checkpoint after a failed sync";
  EXPECT_FALSE((*store)->Wipe(0).has_value()) << "a wipe after a failed sync";
}

}  // namespace
}  // namespace abyss::cold::backends
