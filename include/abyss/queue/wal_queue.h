#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include "abyss/core/queue.h"
#include "abyss/core/result.h"
#include "abyss/core/thread_annotations.h"
#include "abyss/metrics/metrics.h"
#include "abyss/queue/group_commit.h"
#include "abyss/queue/offset_checkpoint.h"
#include "abyss/queue/segment_reaper.h"
#include "abyss/queue/segment_registry.h"

namespace abyss::queue {

class ShardState;

struct WalConfig {
  std::string wal_path;
  size_t segment_size_bytes = 134217728;
  // Largest single encoded entry accepted; decoupled from segment_size_bytes.
  size_t max_value_size_bytes = 67108864;
  size_t shard_count = 1;
  GroupCommitConfig commit;
  std::chrono::seconds min_retention{86400};
  // Consumers that commit offsets; their persisted offsets gate retention.
  std::vector<core::ConsumerId> retention_consumers;
  // Cadence of the background committed-offset checkpoint.
  std::chrono::milliseconds offset_fsync_interval{1000};
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
  core::Result<std::vector<core::QueueEntry>> Read(core::ShardId shard, core::SequenceId from_seq,
                                                   size_t max_count,
                                                   core::Duration timeout) override;
  core::Result<void> CommitOffset(core::ConsumerId consumer, core::ShardId shard,
                                  core::SequenceId seq) override;
  core::Result<std::optional<core::SequenceId>> CommittedOffset(core::ConsumerId consumer,
                                                                core::ShardId shard) override;
  core::Result<core::SequenceId> DurableSeq(core::ShardId shard) override;
  core::Result<bool> AwaitDurable(core::ShardId shard, core::SequenceId seq,
                                  core::Duration timeout) override;
  core::Result<core::SequenceId> FirstSeq(core::ShardId shard) override;
  core::Result<core::SequenceId> OldestRetained(core::ShardId shard) override;
  core::Result<core::SequenceId> TailSeq(core::ShardId shard) override;
  core::Result<core::QueueStats> Stats() override;

  std::vector<SegmentRegistry::SealedSegmentInfo> ListSealedSegments() const override;
  core::Result<void> RemoveSegment(core::ShardId shard, core::SequenceId base_seq) override;

  // Persists every committed offset now, then sweeps retention. The same
  // round the background persister runs each offset_fsync_interval.
  core::Result<void> FlushOffsets();

  // The offset the reaper honours: last checkpointed, not last committed.
  core::Result<std::optional<core::SequenceId>> PersistedOffset(core::ConsumerId consumer,
                                                                core::ShardId shard) const;

  // True while Open is in progress; false once all shards have been loaded.
  bool IsRecovering() const { return recovering_.load(std::memory_order_acquire); }

  uint64_t ReaperFailures() const { return reaper_failures_.load(std::memory_order_relaxed); }

  // Age of the oldest segment the last sweep found eligible but could not
  // remove; nullopt once retention reclaims everything it is allowed to.
  [[nodiscard]] std::optional<core::Duration> OldestEligibleUnreapedAge() const;

  // Test seams. A fault returned here fails persist rounds like an I/O
  // error would; skipping the final persist models a crash at teardown.
  void SetOffsetPersistFaultForTesting(std::function<core::Result<void>()> fault);
  void SkipFinalOffsetPersistForTesting();

 private:
  static constexpr int64_t kNoUnreapedEpochMs = std::numeric_limits<int64_t>::min();

  explicit WalQueue(WalConfig config);
  core::Result<void> Initialize();

  core::Result<void> ValidateShard(core::ShardId shard) const;
  // Index into committed_ for a retention consumer, or nullopt.
  std::optional<size_t> OffsetIndex(core::ConsumerId consumer, core::ShardId shard) const;

  core::Result<void> PersistOffsets() ABYSS_EXCLUDES(persist_mu_);
  void RunPersister();
  void StopPersister();

  void RunReaper() ABYSS_EXCLUDES(reaper_mu_);
  void RecordOldestEligibleUnreaped(std::optional<core::WallTime> created_at);

  WalConfig config_;
  std::atomic<bool> recovering_{true};
  std::atomic<uint64_t> reaper_failures_{0};
  std::atomic<int64_t> oldest_eligible_unreaped_epoch_ms_{kNoUnreapedEpochMs};
  std::vector<std::unique_ptr<ShardState>> shards_;
  std::unique_ptr<OffsetCheckpoint> checkpoint_;

  // In-memory committed offsets, OffsetCheckpoint::Encode()d, indexed
  // consumer-major like the checkpoint.
  std::vector<std::atomic<uint64_t>> committed_;
  // Bumped on every commit; a persist round with no change is skipped.
  std::atomic<uint64_t> commit_generation_{0};

  std::mutex persist_mu_;
  uint64_t persisted_generation_ ABYSS_GUARDED_BY(persist_mu_) = 0;
  std::function<core::Result<void>()> persist_fault_ ABYSS_GUARDED_BY(persist_mu_);
  bool persist_on_close_ ABYSS_GUARDED_BY(persist_mu_) = true;
  metrics::HistogramHandle persist_duration_;
  metrics::CounterHandle persist_failures_;

  std::mutex persister_mu_;
  std::condition_variable persister_cv_;
  bool persister_stop_ ABYSS_GUARDED_BY(persister_mu_) = false;
  std::thread persister_;

  // Sweeps run from the persister and rotating appenders, one at a time.
  std::mutex reaper_mu_;
  std::unique_ptr<SegmentReaper> reaper_ ABYSS_GUARDED_BY(reaper_mu_);
};

}  // namespace abyss::queue
