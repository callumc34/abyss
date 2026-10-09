#pragma once

#include <gmock/gmock.h>

#include <algorithm>
#include <condition_variable>
#include <functional>
#include <future>
#include <limits>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <span>
#include <utility>
#include <vector>

#include "abyss/core/fatal.h"
#include "abyss/core/queue.h"
#include "abyss/queue/reservation.h"

namespace abyss::testing {

class MockQueue : public core::Queue {
 public:
  MockQueue() {
    // Permissive durability defaults so tests that don't care about the A1
    // watermark keep compiling and passing: DurableEnd reports "everything
    // durable" and AwaitDurable always succeeds. Tests exercising durability
    // override these with EXPECT_CALL/ON_CALL.
    using ::testing::_;
    using ::testing::Return;
    ON_CALL(*this, AckDurability()).WillByDefault(Return(core::Durability::kProcessCrash));
    ON_CALL(*this, DurableEnd(_, _))
        .WillByDefault(
            Return(core::Result<core::SequenceId>(std::numeric_limits<core::SequenceId>::max())));
    ON_CALL(*this, AwaitDurable(_, _, _, _)).WillByDefault(Return(core::Result<bool>(true)));
    ON_CALL(*this, Admit(_, _)).WillByDefault(Return(core::Result<void>{}));
    ON_CALL(*this, WaitForSpare(_, _)).WillByDefault(Return(true));
  }

  MOCK_METHOD(core::Result<queue::PendingAppend>, BeginAppend,
              (core::ShardId shard, core::QueueEntry entry, core::SteadyTime admit_by), (override));
  MOCK_METHOD(core::Result<queue::PendingBatchAppend>, BeginAppendBatch,
              (core::ShardId shard, std::span<const core::QueueEntry> entries,
               core::SteadyTime admit_by),
              (override));
  MOCK_METHOD(core::Result<queue::AppendResult>, Append,
              (core::ShardId shard, core::QueueEntry entry, core::SteadyTime admit_by), (override));
  MOCK_METHOD(core::Result<queue::AppendBatchResult>, AppendBatch,
              (core::ShardId shard, std::span<const core::QueueEntry> entries,
               core::SteadyTime admit_by),
              (override));
  MOCK_METHOD((core::Result<std::vector<core::QueueEntry>>), Read,
              (core::ShardId shard, core::SequenceId from_seq, size_t max_count,
               core::Duration timeout, core::Durability visible),
              (override));
  MOCK_METHOD(core::Result<void>, CommitOffset,
              (core::ConsumerId consumer, core::ShardId shard, core::SequenceId seq), (override));
  MOCK_METHOD((core::Result<std::optional<core::SequenceId>>), CommittedOffset,
              (core::ConsumerId consumer, core::ShardId shard), (override));
  MOCK_METHOD(core::Durability, AckDurability, (), (const, override));
  MOCK_METHOD(core::Result<core::SequenceId>, DurableEnd,
              (core::ShardId shard, core::Durability durability), (override));
  MOCK_METHOD(core::Result<bool>, AwaitDurable,
              (core::ShardId shard, core::SequenceId seq, core::Durability durability,
               core::Duration timeout),
              (override));
  MOCK_METHOD(core::Result<core::SequenceId>, FirstSeq, (core::ShardId shard), (override));
  MOCK_METHOD(core::Result<core::SequenceId>, OldestRetained, (core::ShardId shard), (override));
  MOCK_METHOD(core::Result<core::SequenceId>, TailSeq, (core::ShardId shard), (override));
  MOCK_METHOD(core::Result<core::QueueStats>, Stats, (), (override));
  MOCK_METHOD(core::Result<void>, Admit, (core::ShardId shard, core::SteadyTime admit_by),
              (override));
  MOCK_METHOD(bool, WaitForSpare, (core::ShardId shard, core::SteadyTime deadline), (override));

  // In memory: seqs per shard from 0, each shard published in seq
  // order, durable futures ready unless HoldDurable.
  core::Result<queue::Reservation> Reserve(std::span<const queue::ShardEntries> parts) override {
    ABYSS_DCHECK(queue::ReservationsHeld() == 0, "reservation by a thread holding one");
    const std::scoped_lock lock(fake_->mu);
    if (fake_->reserve_fault) {
      if (auto fault = fake_->reserve_fault(parts)) return std::unexpected(std::move(*fault));
    }
    std::vector<queue::ReservedRange> ranges;
    queue::DurableFutures durable;
    auto filler = std::make_unique<FakeFiller>(fake_);
    for (const queue::ShardEntries& part : parts) {
      FakeShard& shard = fake_->shards[part.shard];
      const core::SequenceId first = shard.next;
      for (core::QueueEntry& entry : part.entries) {
        entry.seq = shard.next++;
        filler->entries.emplace_back(part.shard, entry);
      }
      ranges.push_back({.shard = part.shard, .first = first, .last = shard.next - 1});
      filler->parts.push_back(ranges.back());
      std::promise<core::Result<void>> promise;
      durable.push_back({.shard = part.shard, .durable = promise.get_future()});
      if (fake_->hold_durable) {
        fake_->held.push_back(std::move(promise));
      } else {
        promise.set_value({});
      }
    }
    return queue::Reservation(std::move(ranges), std::move(durable), std::move(filler));
  }

  queue::DurableFutures Complete(queue::Reservation&& reservation) override {
    queue::Reservation owned = std::move(reservation);
    return owned.Finish();
  }

  // Fails Reserve with the error it returns, if any.
  void SetReserveFault(
      std::function<std::optional<core::Error>(std::span<const queue::ShardEntries>)> fault) {
    const std::scoped_lock lock(fake_->mu);
    fake_->reserve_fault = std::move(fault);
  }
  // Durable futures of later reservations stay pending until released.
  void HoldDurable() {
    const std::scoped_lock lock(fake_->mu);
    fake_->hold_durable = true;
  }
  void ReleaseDurable() {
    std::vector<std::promise<core::Result<void>>> held;
    {
      const std::scoped_lock lock(fake_->mu);
      fake_->hold_durable = false;
      held.swap(fake_->held);
    }
    for (auto& promise : held) promise.set_value({});
  }
  // Entries Complete published on `shard`, in seq order.
  std::vector<core::QueueEntry> Published(core::ShardId shard) const {
    const std::scoped_lock lock(fake_->mu);
    const auto it = fake_->shards.find(shard);
    return it == fake_->shards.end() ? std::vector<core::QueueEntry>{} : it->second.published;
  }

 private:
  struct FakeShard {
    core::SequenceId next = 0;
    std::vector<core::QueueEntry> published;
  };
  struct FakeLog {
    mutable std::mutex mu;
    std::condition_variable published_cv;
    std::map<core::ShardId, FakeShard> shards;
    bool hold_durable = false;
    std::vector<std::promise<core::Result<void>>> held;
    std::function<std::optional<core::Error>(std::span<const queue::ShardEntries>)> reserve_fault;
  };
  class FakeFiller final : public queue::ReservationFiller {
   public:
    explicit FakeFiller(std::shared_ptr<FakeLog> log) : log_(std::move(log)) {}
    // NOLINTNEXTLINE(bugprone-exception-escape): a test double.
    void Fill() noexcept override {
      std::unique_lock lock(log_->mu);
      std::size_t next = 0;
      for (const queue::ReservedRange& part : parts) {
        FakeShard& shard = log_->shards[part.shard];
        log_->published_cv.wait(lock, [&] { return shard.published.size() == part.first; });
        for (core::SequenceId seq = part.first; seq <= part.last; ++seq) {
          shard.published.push_back(std::move(entries[next++].second));
        }
        log_->published_cv.notify_all();
      }
    }

    // NOLINTBEGIN(cppcoreguidelines-non-private-member-variables-in-classes)
    std::vector<queue::ReservedRange> parts;
    std::vector<std::pair<core::ShardId, core::QueueEntry>> entries;
    // NOLINTEND(cppcoreguidelines-non-private-member-variables-in-classes)

   private:
    std::shared_ptr<FakeLog> log_;
  };

  std::shared_ptr<FakeLog> fake_ = std::make_shared<FakeLog>();
};

// The Read contract over an in-memory log sorted by seq: up to `max_count`
// entries with seq >= `from_seq`.
inline std::vector<core::QueueEntry> ReadFromLog(const std::vector<core::QueueEntry>& log,
                                                 core::SequenceId from_seq, size_t max_count) {
  auto it = std::ranges::lower_bound(log, from_seq, std::ranges::less{},
                                     [](const core::QueueEntry& e) { return e.seq; });
  std::vector<core::QueueEntry> out;
  for (; it != log.end() && out.size() < max_count; ++it) out.push_back(*it);
  return out;
}

// No-op publisher used by tests that want a PendingAppend without a real WAL.
class NoopAppendPublisher : public queue::AppendPublisher {
 public:
  void Publish() noexcept override {}
};

}  // namespace abyss::testing
