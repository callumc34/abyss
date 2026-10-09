#pragma once

#include <cstdint>
#include <memory>
#include <span>
#include <thread>
#include <vector>

#include "abyss/core/queue_entry.h"
#include "abyss/core/types.h"
#include "abyss/queue/append_result.h"

namespace abyss::queue {

struct ShardEntries {
  core::ShardId shard = 0;
  std::span<core::QueueEntry> entries;
};

// One shard's seqs in a reservation, inclusive.
struct ReservedRange {
  core::ShardId shard = 0;
  core::SequenceId first = 0;
  core::SequenceId last = 0;
};

struct ShardDurable {
  core::ShardId shard = 0;
  // Resolves once the shard's part is durable at AckDurability().
  DurabilityFuture durable;
};
using DurableFutures = std::vector<ShardDurable>;

// What a queue still owes a reservation: fill what Reserve left, then
// publish each shard once its earlier seqs are published.
class ReservationFiller {
 public:
  ReservationFiller() = default;
  virtual ~ReservationFiller() = default;

  ReservationFiller(const ReservationFiller&) = delete;
  ReservationFiller& operator=(const ReservationFiller&) = delete;
  ReservationFiller(ReservationFiller&&) = delete;
  ReservationFiller& operator=(ReservationFiller&&) = delete;

  virtual void Fill() noexcept = 0;
};

// A batch Queue::Reserve assigned seqs and log space, consumed by
// Queue::Complete on the thread that reserved it. Later appends to its
// shards cannot publish until it does, so dropping one is fatal. Until
// Complete, that thread takes no lock and makes no blocking call but
// Complete's own waits: a lock holder may be waiting on this fill.
class Reservation {
 public:
  Reservation(std::vector<ReservedRange> ranges, DurableFutures durable,
              std::unique_ptr<ReservationFiller> filler);
  ~Reservation();

  Reservation(Reservation&& other) noexcept = default;
  Reservation& operator=(Reservation&& other) noexcept;
  Reservation(const Reservation&) = delete;
  Reservation& operator=(const Reservation&) = delete;

  // In the order of Reserve's parts.
  const std::vector<ReservedRange>& ranges() const noexcept { return ranges_; }

  // For Queue::Complete: fills and publishes, then hands back each
  // shard's future. Once only.
  DurableFutures Finish() noexcept;

 private:
  std::vector<ReservedRange> ranges_;
  DurableFutures durable_;
  std::unique_ptr<ReservationFiller> filler_;
  // The thread whose count of held reservations this one is in.
  std::thread::id owner_;
};

// Reservations the calling thread holds. Code that may block on a lock
// checks it is 0.
uint32_t ReservationsHeld() noexcept;

}  // namespace abyss::queue
