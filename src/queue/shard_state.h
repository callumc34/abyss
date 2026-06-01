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
  core::Result<std::vector<core::QueueEntry>> Read(core::SequenceId from_seq, size_t max_count,
                                                   core::Duration timeout);

  core::SequenceId head_seq() const;
  core::SequenceId tail_seq() const;
  size_t total_entries() const;
  size_t total_bytes() const;

  // Highest seq whose group-commit fsync has completed for this shard. Sealed
  // segments are fully fsynced at Seal, so the watermark is the max of the
  // active committer's durable seq and the highest sealed seq.
  core::SequenceId DurableSeq() const;
  // True if any seq is durable for this shard, from either the active committer
  // or an already-fully-fsynced sealed/recovered segment. Disambiguates the
  // seq-0 watermark (0 = "none durable" vs "seq 0 durable").
  bool HasDurable() const;
  bool AwaitDurable(core::SequenceId seq, core::Duration timeout) const;

  std::vector<SegmentRegistry::SealedSegmentInfo> ListSealedSegments() const;
  core::Result<void> RemoveSegment(core::SequenceId base_seq);

  void Shutdown();

  core::ShardId shard() const { return config_.shard; }

 private:
  explicit ShardState(ShardStateConfig config);

  core::Result<void> Initialize();
  core::Result<void> CreateInitialSegment() ABYSS_REQUIRES(append_mu_);
  core::Result<void> OpenExistingSegments() ABYSS_REQUIRES(append_mu_);
  core::Result<void> Rotate() ABYSS_REQUIRES(append_mu_);

  ShardStateConfig config_;

  mutable std::mutex append_mu_;
  std::condition_variable read_cv_;
  bool shutting_down_ ABYSS_GUARDED_BY(append_mu_) = false;

  std::shared_ptr<Segment> active_ ABYSS_GUARDED_BY(append_mu_);
  std::vector<std::shared_ptr<Segment>> sealed_ ABYSS_GUARDED_BY(append_mu_);
  core::SequenceId next_seq_ ABYSS_GUARDED_BY(append_mu_) = 0;
  // Highest seq covered by a sealed (fully fsynced) segment. Durable even
  // though it no longer belongs to the active committer.
  core::SequenceId highest_sealed_seq_ ABYSS_GUARDED_BY(append_mu_) = 0;
  // True once a sealed/recovered segment carries any committed entry, so the
  // seq-0 case (highest_sealed_seq_ == 0) is not mistaken for "none durable".
  bool has_sealed_durable_ ABYSS_GUARDED_BY(append_mu_) = false;

  // shared_ptr (not unique_ptr) so a consumer waiting in AwaitDurable keeps the
  // committer alive across a concurrent Rotate that swaps it out.
  std::shared_ptr<GroupCommitter> committer_ ABYSS_GUARDED_BY(append_mu_);
};

}  // namespace abyss::queue
