#pragma once

#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>

#include "abyss/core/result.h"
#include "abyss/core/thread_annotations.h"
#include "abyss/core/types.h"
#include "abyss/queue/offset_store.h"

namespace abyss::queue {

struct FileOffsetStoreConfig {
  std::string directory;
};

// Durable OffsetStore with one file per (consumer, shard) pair to avoid
// cross-shard coordination.
class FileOffsetStore : public OffsetStore {
 public:
  static core::Result<std::unique_ptr<FileOffsetStore>> Open(FileOffsetStoreConfig config);

  ~FileOffsetStore() override = default;

  FileOffsetStore(const FileOffsetStore&) = delete;
  FileOffsetStore& operator=(const FileOffsetStore&) = delete;
  FileOffsetStore(FileOffsetStore&&) = delete;
  FileOffsetStore& operator=(FileOffsetStore&&) = delete;

  std::optional<core::SequenceId> Get(core::ConsumerId consumer,
                                      core::ShardId shard) const override;
  core::Result<void> Set(core::ConsumerId consumer, core::ShardId shard,
                         core::SequenceId seq) override;

 private:
  using ConsumerMap = std::unordered_map<core::ShardId, core::SequenceId>;

  struct Record {
    core::ShardId shard = 0;
    core::SequenceId seq = 0;
  };

  explicit FileOffsetStore(FileOffsetStoreConfig config);

  core::Result<void> LoadAll();
  static core::Result<Record> LoadShardFile(const std::string& path);
  core::Result<void> WriteShardFile(core::ConsumerId consumer, core::ShardId shard,
                                    core::SequenceId seq) const;

  std::string ConsumerDir(core::ConsumerId consumer) const;
  std::string ShardFilePath(core::ConsumerId consumer, core::ShardId shard) const;
  std::string ShardTempPath(core::ConsumerId consumer, core::ShardId shard) const;

  FileOffsetStoreConfig config_;

  mutable std::mutex mu_;
  std::unordered_map<core::ConsumerId, ConsumerMap> offsets_ ABYSS_GUARDED_BY(mu_);
};

}  // namespace abyss::queue
