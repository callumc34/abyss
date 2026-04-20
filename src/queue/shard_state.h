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
  size_t segment_size_bytes = 67108864;
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

  std::unique_ptr<GroupCommitter> committer_;
};

}  // namespace abyss::queue
