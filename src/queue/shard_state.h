#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <span>
#include <string>
#include <vector>

#include "abyss/core/durability.h"
#include "abyss/core/queue_entry.h"
#include "abyss/core/result.h"
#include "abyss/core/thread_annotations.h"
#include "abyss/core/types.h"
#include "abyss/metrics/metrics.h"
#include "abyss/queue/append_result.h"
#include "abyss/queue/durability_window.h"
#include "abyss/queue/group_commit.h"
#include "abyss/queue/pending_append.h"
#include "abyss/queue/segment_registry.h"
#include "abyss/queue/wal_queue.h"
#include "segment.h"

namespace abyss::queue {

struct ShardStateConfig {
  core::ShardId shard = 0;
  std::string directory;
  size_t segment_size_bytes = 134217728;
  // Largest single encoded entry the shard will accept. Decoupled from
  // segment_size_bytes; the validator guarantees a segment can always hold one
  // max-size entry, so segments stay fixed-size (no jumbo segments).
  size_t max_value_size_bytes = 67108864;
  core::Durability ack_durability = core::Durability::kProcessCrash;
  // Shared by every shard of the queue; outlives the shard.
  DurabilityWindow* window = nullptr;
  // Called after a successful rotation.
  std::function<void()> on_rotate;
};

// Owns one WAL shard's segments and its group committer.
class ShardState {
 public:
  static core::Result<std::unique_ptr<ShardState>> Open(ShardStateConfig config);
  ~ShardState();

  ShardState(const ShardState&) = delete;
  ShardState& operator=(const ShardState&) = delete;
  ShardState(ShardState&&) = delete;
  ShardState& operator=(ShardState&&) = delete;

  core::Result<PendingAppend> BeginAppend(core::QueueEntry entry, core::SteadyTime admit_by);
  core::Result<PendingBatchAppend> BeginAppendBatch(std::span<const core::QueueEntry> entries,
                                                    core::SteadyTime admit_by);

  core::Result<AppendResult> Append(core::QueueEntry entry, core::SteadyTime admit_by);
  core::Result<AppendBatchResult> AppendBatch(std::span<const core::QueueEntry> entries,
                                              core::SteadyTime admit_by);
  // kOutOfRange if `from_seq` is below first_seq().
  core::Result<std::vector<core::QueueEntry>> Read(core::SequenceId from_seq, size_t max_count,
                                                   core::Duration timeout,
                                                   core::Durability visible);

  // Next seq to assign.
  core::SequenceId head_seq() const;
  // Lowest seq still on disk: the oldest segment's base.
  core::SequenceId first_seq() const;
  size_t total_entries() const;
  size_t total_bytes() const;

  core::SequenceId DurableEnd(core::Durability durability) const;
  bool AwaitDurable(core::SequenceId seq, core::Durability durability,
                    core::Duration timeout) const;
  // Zero when nothing is unflushed.
  core::Duration DurabilityLag() const;

  std::vector<SegmentRegistry::SealedSegmentInfo> ListSealedSegments() const;
  core::Result<void> RemoveSegment(core::SequenceId base_seq);

  // Stops admission, wakes readers, then stops the committer. Window
  // waiters are woken by the window's owner.
  void Shutdown();

  void SetFlushHookForTesting(FlushHook hook);
  FlushedExtent FlushedExtentForTesting() const;
  void SkipFinalFlushForTesting();

  core::ShardId shard() const { return config_.shard; }

 private:
  explicit ShardState(ShardStateConfig config);

  core::Result<void> Initialize();
  // Creates and durably links the active segment starting at base_seq.
  core::Result<void> CreateActiveSegment(core::SequenceId base_seq) ABYSS_REQUIRES(append_mu_);
  core::Result<void> OpenExistingSegments() ABYSS_REQUIRES(append_mu_);
  core::Result<void> Rotate() ABYSS_REQUIRES(append_mu_);
  core::SequenceId FirstSeqLocked() const ABYSS_REQUIRES(append_mu_);
  core::Result<void> CheckReadable(core::SequenceId from_seq) const ABYSS_REQUIRES(append_mu_);
  core::Result<void> Admit(core::SteadyTime admit_by);
  // After a write of `bytes` under the lock: window accounting and, for
  // power_loss, the future the append resolves on.
  DurabilityFuture Written(core::SequenceId last_seq, uint64_t entries, uint64_t bytes)
      ABYSS_REQUIRES(append_mu_);
  core::Result<void> RunFlushHook() const;

  core::Result<GroupCommitter::Extent> Flush();
  void Flushed(GroupCommitter::Extent previous, GroupCommitter::Extent flushed);

  ShardStateConfig config_;
  metrics::CounterHandle appended_;

  mutable std::mutex append_mu_;
  mutable std::condition_variable read_cv_;
  bool shutting_down_ ABYSS_GUARDED_BY(append_mu_) = false;

  std::shared_ptr<Segment> active_ ABYSS_GUARDED_BY(append_mu_);
  std::vector<std::shared_ptr<Segment>> sealed_ ABYSS_GUARDED_BY(append_mu_);
  core::SequenceId next_seq_ ABYSS_GUARDED_BY(append_mu_) = 0;
  // Cumulative entry bytes written since Open.
  uint64_t published_bytes_ ABYSS_GUARDED_BY(append_mu_) = 0;

  DurabilityWindow::ShardAge unflushed_age_;
  // Commit thread only: the last flush's snapshot.
  core::SequenceId snapshot_base_ = 0;
  size_t snapshot_offset_ = 0;
  DurabilityWindow::Clock::time_point snapshot_time_;
  // The segment (by base seq) and offset the last completed flush reached.
  core::SequenceId flushed_base_ ABYSS_GUARDED_BY(append_mu_) = 0;
  size_t flushed_offset_ ABYSS_GUARDED_BY(append_mu_) = 0;

  mutable std::mutex hook_mu_;
  FlushHook flush_hook_ ABYSS_GUARDED_BY(hook_mu_);
  std::atomic<bool> skip_final_flush_{false};

  // Set in Initialize, before the shard is shared.
  std::unique_ptr<GroupCommitter> committer_;
};

}  // namespace abyss::queue
