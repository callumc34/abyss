#pragma once

#include <array>
#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <optional>
#include <span>
#include <utility>
#include <vector>

#include "abyss/core/durability.h"
#include "abyss/core/queue_entry.h"
#include "abyss/core/result.h"
#include "abyss/core/thread_annotations.h"
#include "abyss/core/types.h"
#include "abyss/metrics/metrics.h"
#include "abyss/queue/append_result.h"
#include "abyss/queue/durability_window.h"
#include "abyss/queue/frame.h"
#include "abyss/queue/group_commit.h"
#include "abyss/queue/log.h"
#include "abyss/queue/pending_append.h"
#include "abyss/queue/reservation.h"

namespace abyss::queue {

// One log and what the shards on it share.
struct LogUnit {
  uint32_t id = 0;
  std::unique_ptr<Log> log;
  std::unique_ptr<GroupCommitter> committer;
  DurabilityWindow::LogAge age;
  // Held while a reclaimed segment's streams are brought up to date, so
  // a reader that found it gone sees the new floors.
  std::mutex reclaim_mu;
  // Segments that have left the log, counted under reclaim_mu.
  std::atomic<uint64_t> reclaims{0};
  // Commit thread only: shards whose power end the current flush moved.
  std::vector<core::ShardId> touched;
  std::vector<bool> is_touched;
  // Commit thread only: each shard's highest seq in the batch the flush
  // walk is inside.
  std::vector<std::pair<core::ShardId, core::SequenceId>> batch;
  // Reservations not yet completed.
  std::atomic<uint64_t> ready_to_complete{0};
};

struct ShardStreamConfig {
  core::ShardId shard = 0;
  // A power of two.
  std::size_t ring_entries = std::size_t{1} << 16;
  std::size_t max_value_size_bytes = std::size_t{64} << 20;
  core::Durability ack_durability = core::Durability::kProcessCrash;
  // Both outlive the stream.
  DurabilityWindow* window = nullptr;
  LogUnit* unit = nullptr;
};

// One shard's stream of frames in its log (ADP-015 §Shard streams).
// next_seq_ is under the append lock. A PendingAppend holds it until it
// publishes; a Reservation releases it at once and publishes in
// Complete. Either publishes only after every earlier seq has. Readers
// locate frames lock-free through the offset ring, then through the
// position hints, then from a sparse index point.
class ShardStream {
 public:
  explicit ShardStream(ShardStreamConfig config);
  ~ShardStream();

  ShardStream(const ShardStream&) = delete;
  ShardStream& operator=(const ShardStream&) = delete;
  ShardStream(ShardStream&&) = delete;
  ShardStream& operator=(ShardStream&&) = delete;

  // Open only, in log order, before the stream is shared.
  core::Result<void> Recover(const RecoveredFrame& frame);
  // Seq after the last recovered frame; nullopt if none was.
  std::optional<core::SequenceId> recovered_next() const noexcept { return recovered_next_; }
  std::optional<core::SequenceId> recovered_first() const noexcept { return recovered_first_; }
  // Open only: everything recovered is synced, so both ends start at
  // `next`, which is at least recovered_next().
  void FinishRecovery(core::SequenceId next);

  core::Result<PendingAppend> BeginAppend(core::QueueEntry entry, core::SteadyTime admit_by);
  core::Result<PendingBatchAppend> BeginAppendBatch(std::span<const core::QueueEntry> entries,
                                                    core::SteadyTime admit_by);
  // One log's share of a reservation: the parts' streams, the parts,
  // and every entry's frame size, in order.
  struct LogParts {
    std::span<ShardStream* const> streams;
    std::span<const ShardEntries> parts;
    std::span<const uint32_t> sizes;
  };
  // WalQueue::Reserve and ReserveFlush once their checks pass, never
  // waiting: every stream locked in (log, shard) order, then one batch
  // per log, as one Reservation that Complete fills in log order.
  static core::Result<Reservation> Reserve(std::span<const LogParts> logs)
      ABYSS_NO_THREAD_SAFETY_ANALYSIS;
  core::Result<std::vector<core::QueueEntry>> Read(core::SequenceId from, std::size_t max_count,
                                                   core::Duration timeout,
                                                   core::Durability visible);

  core::SequenceId next_seq() const;
  core::SequenceId first_seq() const noexcept { return first_seq_.load(std::memory_order_acquire); }
  core::SequenceId DurableEnd(core::Durability durability) const noexcept;
  bool AwaitDurable(core::SequenceId seq, core::Durability durability,
                    core::Duration timeout) const;

  // Commit thread, in the flush walk, before the log's durable prefix
  // moves: a batch that ends this shard's seqs at `end` is durable. True
  // iff that moved the power end, which never passes the published end.
  bool Durable(core::SequenceId end) noexcept;
  // After the power end moved: resolves append futures below it and
  // wakes its waiters.
  void PowerAdvanced();

  // Retention, under the unit's reclaim_mu: a segment of this log was
  // reclaimed. `max_seq` is this shard's highest seq in it, if any;
  // every retained frame lies at or after `retained_from`.
  void Reclaimed(std::optional<core::SequenceId> max_seq, LogPosition retained_from);

  // Where `seq`'s frame starts; `seq` must be published.
  core::Result<LogPosition> Locate(Log::Cursor& cursor, core::SequenceId seq) const;

  std::size_t index_points() const;

  // Stops admission and wakes readers.
  void Shutdown();
  bool stopping() const noexcept { return stopping_.load(std::memory_order_acquire); }
  // After the committer stopped: what still waits resolves
  // kUnavailable.
  void CommitterStopped();

  core::ShardId shard() const noexcept { return config_.shard; }
  // Memory a ring of `entries` slots holds.
  static std::size_t RingBytes(std::size_t entries) noexcept { return entries * sizeof(Slot); }

  void SetBatchCommitHookForTesting(std::function<void(std::size_t committed)> hook);
  std::optional<LogPosition> RingPositionForTesting(core::SequenceId seq) const noexcept {
    return RingAt(seq);
  }
  // Frames header-read while locating seqs off the ring.
  uint64_t SkippedForTesting() const noexcept { return skipped_.load(std::memory_order_relaxed); }

 private:
  struct Slot {
    std::atomic<uint64_t> stamp;
    std::atomic<LogPosition> pos{0};
  };
  struct IndexPoint {
    core::SequenceId seq = 0;
    LogPosition pos = 0;
  };
  struct Hint {
    std::atomic<core::SequenceId> seq{0};
    std::atomic<LogPosition> pos{0};
  };

  struct Begun {
    core::SequenceId first = 0;
    core::SequenceId last = 0;
    DurabilityFuture durable;
    std::unique_ptr<AppendPublisher> publisher;
  };
  class Publisher;
  class Filler;
  class MultiFiller;

  // Reserve's batch on one log, under every stream's append lock.
  // `locked` counts the bytes encoded under the caller's locks so far.
  static core::Result<std::unique_ptr<ReservationFiller>> ReserveLocked(
      const LogParts& group, uint64_t total, std::size_t& locked,
      std::vector<ReservedRange>& ranges, DurableFutures& durable) ABYSS_NO_THREAD_SAFETY_ANALYSIS;

  Log& log() const noexcept { return *config_.unit->log; }

  core::Result<Begun> Begin(std::span<core::QueueEntry> entries, core::SteadyTime admit_by);
  void Record(core::SequenceId seq, LogPosition pos, uint32_t size) ABYSS_REQUIRES(append_mu_);
  DurabilityFuture WhenPowerDurable(core::SequenceId seq);
  // Until every seq below `first` is published.
  void AwaitPublished(core::SequenceId first) noexcept;
  // Complete's publish of [first, end).
  void PublishInOrder(core::SequenceId first, core::SequenceId end) noexcept;
  void WakeReaders() const noexcept;
  void Published(LogPosition end_pos) noexcept;
  bool WaitForEnd(core::SequenceId seq, core::Durability durability, core::Duration timeout) const;

  // True iff it moved the power end.
  bool RaisePowerEnd() noexcept;
  std::optional<LogPosition> RingAt(core::SequenceId seq) const noexcept;
  std::optional<IndexPoint> Floor(Log::Cursor& cursor, core::SequenceId seq) const;
  core::Result<LogPosition> SkipTo(Log::Cursor& cursor, LogPosition pos,
                                   core::SequenceId seq) const;
  core::Result<LogPosition> LocateRetained(Log::Cursor& cursor, core::SequenceId seq) const;
  core::Result<void> CheckReadable(core::SequenceId from) const;
  core::Error ReadFailed(const core::Error& error, core::SequenceId seq) const;
  void Remember(core::SequenceId seq, LogPosition pos) const noexcept;

  ShardStreamConfig config_;
  metrics::CounterHandle appended_;
  metrics::HistogramHandle publish_wait_;
  metrics::GaugeHandle index_gauge_;
  metrics::GaugeHandle ring_gauge_;

  mutable std::mutex append_mu_;
  core::SequenceId next_seq_ ABYSS_GUARDED_BY(append_mu_) = core::kFirstSeq;
  // Frame bytes since the last index point this stream appended.
  uint64_t since_point_ ABYSS_GUARDED_BY(append_mu_) = 0;
  bool has_point_ ABYSS_GUARDED_BY(append_mu_) = false;
  std::atomic<bool> stopping_{false};
  std::function<void(std::size_t)> batch_commit_hook_ ABYSS_GUARDED_BY(append_mu_);
  mutable std::atomic<uint64_t> skipped_{0};

  const uint64_t ring_mask_;
  std::vector<Slot> ring_;
  mutable std::array<Hint, 4> hints_;
  mutable std::atomic<uint32_t> next_hint_{0};

  mutable std::mutex index_mu_;
  std::deque<IndexPoint> index_ ABYSS_GUARDED_BY(index_mu_);

  // Exclusive ends, so an empty stream's are kFirstSeq.
  std::atomic<core::SequenceId> first_seq_{core::kFirstSeq};
  std::atomic<core::SequenceId> published_end_{core::kFirstSeq};
  // Batch ends the flush walk has found durable, published or not.
  std::atomic<core::SequenceId> flushed_end_{core::kFirstSeq};
  std::atomic<core::SequenceId> power_end_{core::kFirstSeq};

  // Readers of published_end_, Dekker-counted so a publish notifies
  // only when someone waits.
  mutable std::mutex read_mu_;
  mutable std::condition_variable read_cv_;
  mutable std::atomic<uint32_t> readers_waiting_{0};

  mutable std::mutex power_mu_;
  mutable std::condition_variable power_cv_;
  mutable uint32_t power_waiting_ ABYSS_GUARDED_BY(power_mu_) = 0;
  bool committer_stopped_ ABYSS_GUARDED_BY(power_mu_) = false;
  // Seq order: registered under the append lock.
  std::deque<std::pair<core::SequenceId, std::promise<core::Result<void>>>> futures_
      ABYSS_GUARDED_BY(power_mu_);

  std::optional<core::SequenceId> recovered_first_;
  std::optional<core::SequenceId> recovered_next_;
};

}  // namespace abyss::queue
