#pragma once

#include <condition_variable>
#include <cstddef>
#include <functional>
#include <memory>
#include <mutex>
#include <span>
#include <string>
#include <vector>

#include "abyss/core/queue_entry.h"
#include "abyss/core/result.h"
#include "abyss/core/thread_annotations.h"
#include "abyss/core/types.h"
#include "abyss/queue/append_result.h"
#include "abyss/queue/group_commit.h"
#include "abyss/queue/pending_append.h"
#include "abyss/queue/segment_registry.h"
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
  GroupCommitConfig commit;
  // Called after a successful rotation.
  std::function<void()> on_rotate;
};

// Owns one WAL shard's segments.
class ShardState {
 public:
  static core::Result<std::unique_ptr<ShardState>> Open(ShardStateConfig config);
  ~ShardState();

  ShardState(const ShardState&) = delete;
  ShardState& operator=(const ShardState&) = delete;
  ShardState(ShardState&&) = delete;
  ShardState& operator=(ShardState&&) = delete;

  core::Result<PendingAppend> BeginAppend(core::QueueEntry entry);
  core::Result<PendingBatchAppend> BeginAppendBatch(std::span<const core::QueueEntry> entries);

  core::Result<AppendResult> Append(core::QueueEntry entry);
  core::Result<AppendBatchResult> AppendBatch(std::span<const core::QueueEntry> entries);
  // kOutOfRange if `from_seq` is below first_seq().
  core::Result<std::vector<core::QueueEntry>> Read(core::SequenceId from_seq, size_t max_count,
                                                   core::Duration timeout);

  // Next seq to assign.
  core::SequenceId head_seq() const;
  // Lowest seq still on disk: the oldest segment's base.
  core::SequenceId first_seq() const;
  size_t total_entries() const;
  size_t total_bytes() const;

  // Highest durable seq: the max of the active committer's durable seq and
  // highest_synced_seq_ (sealed segments and the tail synced at Open).
  core::SequenceId DurableSeq() const;
  // True if any seq is durable for this shard. Disambiguates the seq-0
  // watermark (0 = "none durable" vs "seq 0 durable").
  bool HasDurable() const;
  bool AwaitDurable(core::SequenceId seq, core::Duration timeout) const;

  std::vector<SegmentRegistry::SealedSegmentInfo> ListSealedSegments() const;
  core::Result<void> RemoveSegment(core::SequenceId base_seq);

  void Shutdown();

  core::ShardId shard() const { return config_.shard; }

 private:
  explicit ShardState(ShardStateConfig config);

  core::Result<void> Initialize();
  // Creates and durably links the active segment starting at base_seq.
  core::Result<void> CreateActiveSegment(core::SequenceId base_seq) ABYSS_REQUIRES(append_mu_);
  core::Result<void> OpenExistingSegments() ABYSS_REQUIRES(append_mu_);
  core::Result<void> Rotate() ABYSS_REQUIRES(append_mu_);
  core::SequenceId FirstSeqLocked() const ABYSS_REQUIRES(append_mu_);

  ShardStateConfig config_;

  mutable std::mutex append_mu_;
  std::condition_variable read_cv_;
  bool shutting_down_ ABYSS_GUARDED_BY(append_mu_) = false;

  std::shared_ptr<Segment> active_ ABYSS_GUARDED_BY(append_mu_);
  std::vector<std::shared_ptr<Segment>> sealed_ ABYSS_GUARDED_BY(append_mu_);
  core::SequenceId next_seq_ ABYSS_GUARDED_BY(append_mu_) = 0;
  // Highest seq fsynced outside the active committer: by a Seal, or by the
  // sync of the recovered tail at Open (unsynced under fsync_none).
  core::SequenceId highest_synced_seq_ ABYSS_GUARDED_BY(append_mu_) = 0;
  // True once that covers any entry, so a highest_synced_seq_ of 0 is not
  // mistaken for "none durable".
  bool has_synced_durable_ ABYSS_GUARDED_BY(append_mu_) = false;

  // shared_ptr (not unique_ptr) so a consumer waiting in AwaitDurable keeps the
  // committer alive across a concurrent Rotate that swaps it out.
  std::shared_ptr<GroupCommitter> committer_ ABYSS_GUARDED_BY(append_mu_);
};

}  // namespace abyss::queue
