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

// Durable OffsetStore, one binary file per consumer under `directory`.
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

  explicit FileOffsetStore(FileOffsetStoreConfig config);

  core::Result<void> LoadAll();
  core::Result<ConsumerMap> LoadConsumer(core::ConsumerId consumer) const;
  core::Result<void> WriteConsumer(core::ConsumerId consumer, const ConsumerMap& entries) const;
  std::string FilePath(core::ConsumerId consumer) const;
  std::string TempPath(core::ConsumerId consumer) const;

  FileOffsetStoreConfig config_;

  mutable std::mutex mu_;
  std::unordered_map<core::ConsumerId, ConsumerMap> offsets_ ABYSS_GUARDED_BY(mu_);
};

}  // namespace abyss::queue
