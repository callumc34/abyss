#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <optional>
#include <span>
#include <vector>

#include "abyss/core/durability.h"
#include "abyss/core/queue_entry.h"
#include "abyss/core/result.h"
#include "abyss/core/types.h"
#include "abyss/queue/append_result.h"
#include "abyss/queue/pending_append.h"
#include "abyss/queue/reservation.h"

namespace abyss::core {

// Reserve commits frames up to this size under the caller's locks and
// leaves larger ones to Complete; the sequencer copies arguments above
// it before locking.
inline constexpr std::size_t kLockHoldFrameBytes = std::size_t{16} << 10;

struct QueueStats {
  uint64_t total_entries = 0;
  uint64_t total_bytes = 0;
  // Highest next-seq-to-assign across shards.
  SequenceId head_seq = 0;
  // Lowest first retained seq across shards.
  SequenceId first_seq = 0;
};

// Two-phase append (BeginAppend → Publish) is the race-free path for callers
// that register per-seq state. The one-shot Append is for fire-and-forget.
// An append's durability future resolves at AckDurability(). Admission
// to the durability window waits at most until `admit_by`; a deadline
// already past checks without waiting.
//
// Read position and committed offset are separate: each consumer owns its
// read cursor and passes it to Read; CommitOffset only records where it is
// safe to resume and, for retention, how far the WAL may be reclaimed.
//
// Durable ends are exclusive: seqs below DurableEnd(shard, d) are durable
// at d; 0 means none is. DurableEnd(kPowerLoss) never passes
// DurableEnd(kProcessCrash): nothing is visible before it is published.
class Queue {
 public:
  Queue() = default;
  virtual ~Queue() = default;
  Queue(const Queue&) = delete;
  Queue& operator=(const Queue&) = delete;
  Queue(Queue&&) = delete;
  Queue& operator=(Queue&&) = delete;

  virtual Result<queue::PendingAppend> BeginAppend(ShardId shard, QueueEntry entry,
                                                   SteadyTime admit_by) = 0;
  virtual Result<queue::PendingBatchAppend> BeginAppendBatch(ShardId shard,
                                                             std::span<const QueueEntry> entries,
                                                             SteadyTime admit_by) = 0;

  virtual Result<queue::AppendResult> Append(ShardId shard, QueueEntry entry,
                                             SteadyTime admit_by) = 0;
  virtual Result<queue::AppendBatchResult> AppendBatch(ShardId shard,
                                                       std::span<const QueueEntry> entries,
                                                       SteadyTime admit_by) = 0;

  // The sequencer's append (ADP-015 §Sequenced write path): Admit and
  // WaitForSpare wait under no lock, Reserve runs under the hot shard
  // locks and never waits, and Complete runs once they are released.
  //
  // Waits for room in `shard`'s durability window, as an append's
  // admission does.
  virtual Result<void> Admit(ShardId shard, SteadyTime admit_by) = 0;
  // Waits for a spare segment on `shard`'s log; false at the deadline
  // or on shutdown.
  virtual bool WaitForSpare(ShardId shard, SteadyTime deadline) = 0;
  // Assigns each part's seqs and reserves every part as one batch.
  // Parts are sorted by shard and distinct. It takes the entries it
  // leaves to Complete; on an error none is taken and no seq is used:
  //   kResourceExhausted: the window is full; Admit, then decide again.
  //   kUnavailable: no spare segment, or shutting down; WaitForSpare,
  //     then decide again.
  //   kInvalidArgument starting "CROSSSLOT": the shards span logs.
  //   kValueTooLarge: an entry or the batch exceeds what a frame or a
  //     segment holds.
  virtual Result<queue::Reservation> Reserve(std::span<const queue::ShardEntries> parts) = 0;
  // Reserve of one Flush per shard, every shard in order, across every
  // log: one batch per log, in one Reservation that Complete fills log
  // by log. Atomic to readers holding the hot locks, but not across a
  // crash: each log's batch survives or not on its own.
  virtual Result<queue::Reservation> ReserveFlush(std::span<const queue::ShardEntries> parts) = 0;
  // The log `shard`'s stream is on; a Reserve stays within one.
  virtual uint32_t LogOf(ShardId shard) const = 0;
  // Fills what Reserve left, then publishes each shard once its earlier
  // seqs are published.
  virtual queue::DurableFutures Complete(queue::Reservation&& reservation) = 0;

  // Up to `max_count` contiguous entries with seq >= `from_seq` that are
  // durable at `visible`. Waits up to `timeout` when none is yet.
  // kOutOfRange if `from_seq` < FirstSeq(shard): those were reclaimed.
  virtual Result<std::vector<QueueEntry>> Read(ShardId shard, SequenceId from_seq, size_t max_count,
                                               Duration timeout, Durability visible) = 0;

  // Records `seq` as `consumer`'s committed offset on `shard`: effective
  // in memory at once, persisted lazily. Fail-closed: kFailedPrecondition
  // unless seq < DurableEnd(shard, kPowerLoss), so no persisted offset
  // outruns the power-durable log. kInvalidArgument for a non-retention
  // consumer or a regression.
  virtual Result<void> CommitOffset(ConsumerId consumer, ShardId shard, SequenceId seq) = 0;

  // Last committed offset; nullopt if `consumer` never committed on `shard`.
  virtual Result<std::optional<SequenceId>> CommittedOffset(ConsumerId consumer, ShardId shard) = 0;

  // The class append futures resolve at, and replies are allowed to show.
  virtual Durability AckDurability() const = 0;
  // Monotonic per shard and class.
  virtual Result<SequenceId> DurableEnd(ShardId shard, Durability durability) = 0;
  // True iff `seq` is durable at `durability` within `timeout`.
  virtual Result<bool> AwaitDurable(ShardId shard, SequenceId seq, Durability durability,
                                    Duration timeout) = 0;

  // Lowest seq still readable on `shard`.
  virtual Result<SequenceId> FirstSeq(ShardId shard) = 0;
  // Min persisted committed offset across retention consumers (the retention
  // floor); FirstSeq when any retention consumer has none persisted.
  virtual Result<SequenceId> OldestRetained(ShardId shard) = 0;
  // Highest assigned seq on `shard`; 0 when empty.
  virtual Result<SequenceId> TailSeq(ShardId shard) = 0;
  virtual Result<QueueStats> Stats() = 0;

  // Receives one shard's entries in seq order; never called concurrently
  // for one shard. An error stops the scan and is returned from it.
  using ScanSink = std::function<Result<void>(ShardId, std::vector<QueueEntry>&)>;
  // Delivers every shard's entries in [from[s], end[s]) to `sink`, up to
  // `parallelism` shards at a time; every one of them must be readable.
  // A log-structured queue reads each log once; this default reads each
  // shard in turn. kUnavailable once `cancel` is set.
  virtual Result<void> Scan(std::span<const SequenceId> from, std::span<const SequenceId> end,
                            uint32_t parallelism, const ScanSink& sink,
                            const std::atomic<bool>& cancel);
};

}  // namespace abyss::core
