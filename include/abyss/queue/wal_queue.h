#pragma once

#include <atomic>
#include <cstddef>
#include <memory>
#include <string>
#include <vector>

#include "abyss/core/queue.h"
#include "abyss/core/result.h"
#include "abyss/queue/group_commit.h"
#include "abyss/queue/offset_store.h"
#include "abyss/queue/segment_reaper.h"
#include "abyss/queue/segment_registry.h"

namespace abyss::queue {

class ShardState;

struct WalConfig {
  std::string wal_path;
  size_t segment_size_bytes = 67108864;
  size_t shard_count = 1;
  GroupCommitConfig commit;
  std::chrono::seconds min_retention{86400};
  std::vector<core::ConsumerId> retention_consumers;
};

// NOLINTNEXTLINE(misc-multiple-inheritance)
class WalQueue : public core::Queue, public SegmentRegistry {
 public:
  static core::Result<std::unique_ptr<WalQueue>> Open(WalConfig config);
  ~WalQueue() override;

  WalQueue(const WalQueue&) = delete;
  WalQueue& operator=(const WalQueue&) = delete;
  WalQueue(WalQueue&&) = delete;
  WalQueue& operator=(WalQueue&&) = delete;

  core::Result<PendingAppend> BeginAppend(core::ShardId shard, core::QueueEntry entry) override;
  core::Result<PendingBatchAppend> BeginAppendBatch(
      core::ShardId shard, std::span<const core::QueueEntry> entries) override;

  core::Result<AppendResult> Append(core::ShardId shard, core::QueueEntry entry) override;
  core::Result<AppendBatchResult> AppendBatch(core::ShardId shard,
                                              std::span<const core::QueueEntry> entries) override;
  core::Result<std::vector<core::QueueEntry>> Read(core::ConsumerId consumer, core::ShardId shard,
                                                   size_t max_count,
                                                   core::Duration timeout) override;
  core::Result<void> Ack(core::ConsumerId consumer, core::ShardId shard,
                         core::SequenceId seq) override;
  core::Result<core::SequenceId> OldestRetained(core::ShardId shard) override;
  core::Result<core::QueueStats> Stats() override;

  std::vector<SegmentRegistry::SealedSegmentInfo> ListSealedSegments() const override;
  core::Result<void> RemoveSegment(core::ShardId shard, core::SequenceId base_seq) override;

  // True while Open is in progress; false once all shards have been loaded.
  bool IsRecovering() const { return recovering_.load(std::memory_order_acquire); }

 private:
  explicit WalQueue(WalConfig config);
  core::Result<void> Initialize();

  core::Result<void> ValidateShard(core::ShardId shard) const;
  void RunReaper();

  WalConfig config_;
  std::atomic<bool> recovering_{true};
  std::vector<std::unique_ptr<ShardState>> shards_;
  std::unique_ptr<OffsetStore> offsets_;
  std::unique_ptr<SegmentReaper> reaper_;
};

}  // namespace abyss::queue
