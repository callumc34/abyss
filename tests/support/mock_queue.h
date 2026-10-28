#pragma once

#include <gmock/gmock.h>

#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <functional>
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
#include "abyss/core/seq_waiters.h"
#include "abyss/core/types.h"
#include "abyss/core/wait_list.h"
#include "abyss/queue/reservation.h"

namespace abyss::testing {

class MockQueue : public core::Queue {
 public:
  MockQueue() {
    // Permissive durability defaults so tests that don't care about the A1
    // watermark keep compiling and passing: DurableEnd reports "everything
    // durable" and AwaitDurable succeeds but for reservations held or
    // failed below. Tests exercising durability override these with
    // EXPECT_CALL/ON_CALL.
    using ::testing::_;
    using ::testing::Return;
    ON_CALL(*this, AckDurability()).WillByDefault(Return(core::Durability::kProcessCrash));
    ON_CALL(*this, DurableEnd(_, _))
        .WillByDefault(
            Return(core::Result<core::SequenceId>(std::numeric_limits<core::SequenceId>::max())));
    ON_CALL(*this, AwaitDurable(_, _, _, _))
        .WillByDefault([this](core::ShardId shard, core::SequenceId seq, core::Durability,
                              core::Duration timeout) { return FakeDurable(shard, seq, timeout); });
    ON_CALL(*this, Admissible(_)).WillByDefault([this](core::ShardId) {
      return fake_->admissible.load(std::memory_order_seq_cst);
    });
    ON_CALL(*this, WhenReady(_, _, _, _))
        .WillByDefault([this](core::QueueWait wait, core::ShardId, core::Waiter& waiter, bool) {
          return wait == core::QueueWait::kAdmission
                     ? fake_->admission.Register(
                           waiter,
                           [this] { return fake_->admissible.load(std::memory_order_seq_cst); })
                     : fake_->spares.Register(waiter, [this] {
                         return fake_->spare_ready.load(std::memory_order_seq_cst);
                       });
        });
    ON_CALL(*this, CancelWait(_, _, _))
        .WillByDefault([this](core::QueueWait wait, core::ShardId, core::Waiter& waiter) {
          return ListOf(wait).Cancel(waiter);
        });
    ON_CALL(*this, FinishWait(_, _, _))
        .WillByDefault([this](core::QueueWait wait, core::ShardId, core::Waiter& waiter) {
          ListOf(wait).Finish(waiter);
        });
    ON_CALL(*this, AdmissionTimedOut(_)).WillByDefault([](core::ShardId) {
      return core::Error{core::ErrorCode::kResourceExhausted,
                         "WAL durability window full: the device is not keeping up with writes"};
    });
    ON_CALL(*this, WhenDurable(_, _, _))
        .WillByDefault([this](core::ShardId shard, core::Durability, core::SeqWaiterNode& node) {
          return FakeWhenDurable(shard, node);
        });
    ON_CALL(*this, CancelDurable(_, _, _))
        .WillByDefault([this](core::ShardId shard, core::Durability, core::SeqWaiterNode& node) {
          return Shard(shard).waiters.Cancel(node);
        });
  }

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
  // The fake never reclaims.
  core::RetentionPin PinRetention() override { return {}; }
  MOCK_METHOD(core::Result<core::SequenceId>, OldestRetained, (core::ShardId shard), (override));
  MOCK_METHOD(core::Result<core::SequenceId>, TailSeq, (core::ShardId shard), (override));
  MOCK_METHOD(core::Result<core::QueueStats>, Stats, (), (override));
  MOCK_METHOD(bool, Admissible, (core::ShardId shard), (override));
  MOCK_METHOD(std::optional<core::Result<void>>, WhenReady,
              (core::QueueWait wait, core::ShardId shard, core::Waiter& waiter, bool first),
              (override));
  // NOLINTNEXTLINE(bugprone-exception-escape): a test double.
  MOCK_METHOD(bool, CancelWait, (core::QueueWait wait, core::ShardId shard, core::Waiter& waiter),
              (noexcept, override));
  // NOLINTNEXTLINE(bugprone-exception-escape): a test double.
  MOCK_METHOD(void, FinishWait, (core::QueueWait wait, core::ShardId shard, core::Waiter& waiter),
              (noexcept, override));
  MOCK_METHOD(core::Error, AdmissionTimedOut, (core::ShardId shard), (override));
  MOCK_METHOD(std::optional<core::Result<void>>, WhenDurable,
              (core::ShardId shard, core::Durability durability, core::SeqWaiterNode& node),
              (override));
  // NOLINTNEXTLINE(bugprone-exception-escape): a test double.
  MOCK_METHOD(bool, CancelDurable,
              (core::ShardId shard, core::Durability durability, core::SeqWaiterNode& node),
              (noexcept, override));

  // The fake window: whether it admits. Opening it wakes one waiter,
  // which hands on if it passes.
  void SetAdmissible(bool admissible) {
    fake_->admissible.store(admissible, std::memory_order_seq_cst);
    if (admissible) fake_->admission.NotifyOne();
  }
  // One spare made ready, for the front waiter, as a preparer finishing
  // one segment.
  void ReadySpareOne() {
    fake_->spare_ready.store(true, std::memory_order_seq_cst);
    fake_->spares.NotifyOne();
  }
  void SetSpareReady(bool ready) {
    fake_->spare_ready.store(ready, std::memory_order_seq_cst);
    if (ready) fake_->spares.Notify();
  }
  size_t AdmissionWaiters() const { return fake_->admission.size(); }
  size_t SpareWaiters() const { return fake_->spares.size(); }
  // Links `node` on `shard` as not yet durable, whatever it is, until
  // ReleaseDurable.
  std::optional<core::Result<void>> ParkDurable(core::ShardId shard, core::SeqWaiterNode& node) {
    FakeShard& state = Shard(shard);
    {
      const std::scoped_lock lock(fake_->mu);
      if (state.end.load() > node.seq()) state.end.store(node.seq(), std::memory_order_seq_cst);
    }
    return state.waiters.Register(node, state.end);
  }

  // In memory: seqs per shard from kFirstSeq, each shard published in seq
  // order, durable unless held or failed. It takes every entry, as a
  // queue may the ones it fills in Complete.
  core::Result<queue::Reservation> Reserve(std::span<const queue::ShardEntries> parts) override {
    ABYSS_DCHECK(queue::ReservationsHeld() == 0, "reservation by a thread holding one");
    const std::scoped_lock lock(fake_->mu);
    if (fake_->reserve_fault) {
      if (auto fault = fake_->reserve_fault(parts)) return std::unexpected(std::move(*fault));
    }
    std::vector<queue::ReservedRange> ranges;
    auto filler = std::make_unique<FakeFiller>(fake_);
    for (const queue::ShardEntries& part : parts) {
      FakeShard& shard = fake_->shards[part.shard];
      const core::SequenceId first = shard.next;
      for (core::QueueEntry& entry : part.entries) {
        entry.seq = shard.next++;
        filler->entries.emplace_back(part.shard, std::move(entry));
      }
      ranges.push_back({.shard = part.shard, .first = first, .last = shard.next - 1});
      filler->parts.push_back(ranges.back());
      if (fake_->durable_error.has_value() && !shard.fail_from.has_value()) {
        shard.fail_from = first;
      } else if (fake_->hold_durable && !shard.hold_from.has_value()) {
        shard.hold_from = first;
        shard.end.store(first, std::memory_order_seq_cst);
      }
    }
    return queue::Reservation(std::move(ranges), std::move(filler));
  }

  uint32_t LogOf(core::ShardId shard) const override { return shard % log_count_; }
  // Shards on different logs are CROSSSLOT to the sequencer; the fake
  // itself reserves across them.
  void SetLogCount(uint32_t log_count) { log_count_ = log_count; }

  // As Reserve: the fake is one log.
  core::Result<queue::Reservation> ReserveFlush(
      std::span<const queue::ShardEntries> parts) override {
    return Reserve(parts);
  }

  std::vector<queue::ReservedRange> Complete(queue::Reservation&& reservation) override {
    queue::Reservation owned = std::move(reservation);
    std::function<void()> hook;
    {
      const std::scoped_lock lock(fake_->mu);
      hook = fake_->on_complete;
    }
    if (hook) hook();
    return owned.Finish();
  }
  // Runs as each Complete starts.
  void OnComplete(std::function<void()> hook) {
    const std::scoped_lock lock(fake_->mu);
    fake_->on_complete = std::move(hook);
  }

  // Fails Reserve with the error it returns, if any.
  void SetReserveFault(
      std::function<std::optional<core::Error>(std::span<const queue::ShardEntries>)> fault) {
    const std::scoped_lock lock(fake_->mu);
    fake_->reserve_fault = std::move(fault);
  }
  // AwaitDurable on later reservations' seqs fails with `error`.
  void FailDurable(core::Error error) {
    const std::scoped_lock lock(fake_->mu);
    fake_->durable_error = std::move(error);
  }
  // Makes everything parked on `shard` durable, posting to its waiters.
  void ReleaseParked(core::ShardId shard) {
    FakeShard& state = Shard(shard);
    state.end.store(kAllDurable, std::memory_order_seq_cst);
    state.waiters.Advance(kAllDurable);
  }
  // Fails every node waiting on `shard`, as its stream stopping would.
  void FailParked(core::ShardId shard, const core::Error& error) {
    Shard(shard).waiters.FailAll(error);
  }
  // AwaitDurable on later reservations' seqs waits until released.
  void HoldDurable() {
    const std::scoped_lock lock(fake_->mu);
    fake_->hold_durable = true;
  }
  void ReleaseDurable() {
    std::vector<FakeShard*> released;
    {
      const std::scoped_lock lock(fake_->mu);
      fake_->hold_durable = false;
      for (auto& [id, shard] : fake_->shards) {
        shard.hold_from.reset();
        shard.end.store(kAllDurable, std::memory_order_seq_cst);
        released.push_back(&shard);
      }
    }
    fake_->durable_cv.notify_all();
    for (FakeShard* shard : released) shard->waiters.Advance(kAllDurable);
  }
  // Entries Complete published on `shard`, in seq order.
  std::vector<core::QueueEntry> Published(core::ShardId shard) const {
    const std::scoped_lock lock(fake_->mu);
    const auto it = fake_->shards.find(shard);
    return it == fake_->shards.end() ? std::vector<core::QueueEntry>{} : it->second.published;
  }
  // The Read contract over what Complete published on `shard`.
  std::vector<core::QueueEntry> ReadPublished(core::ShardId shard, core::SequenceId from_seq,
                                              size_t max_count) const {
    // A real queue answers kOutOfRange: no entry has seq 0.
    ABYSS_DCHECK(from_seq >= core::kFirstSeq, "a read from seq 0");
    const std::scoped_lock lock(fake_->mu);
    std::vector<core::QueueEntry> out;
    const auto it = fake_->shards.find(shard);
    if (it == fake_->shards.end()) return out;
    const auto& log = it->second.published;
    for (auto i = from_seq - core::kFirstSeq; i < log.size() && out.size() < max_count; ++i) {
      out.push_back(log[i]);
    }
    return out;
  }

 private:
  static constexpr core::SequenceId kAllDurable = std::numeric_limits<core::SequenceId>::max();

  struct FakeShard {
    core::SequenceId next = core::kFirstSeq;
    // The entry at seq s is at s - kFirstSeq.
    std::vector<core::QueueEntry> published;
    // Seqs from these on are held, or fail with durable_error.
    std::optional<core::SequenceId> hold_from;
    std::optional<core::SequenceId> fail_from;
    // WhenDurable's: seqs below `end` are durable.
    std::atomic<core::SequenceId> end{kAllDurable};
    core::SeqWaiters waiters;
  };
  struct FakeLog {
    std::atomic<bool> admissible{true};
    std::atomic<bool> spare_ready{true};
    core::WaitList admission;
    core::WaitList spares;
    mutable std::mutex mu;
    std::condition_variable published_cv;
    std::condition_variable durable_cv;
    std::map<core::ShardId, FakeShard> shards;
    bool hold_durable = false;
    std::optional<core::Error> durable_error;
    std::function<void()> on_complete;
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
        log_->published_cv.wait(
            lock, [&] { return shard.published.size() + core::kFirstSeq == part.first; });
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

  core::Result<bool> FakeDurable(core::ShardId id, core::SequenceId seq, core::Duration timeout) {
    std::unique_lock lock(fake_->mu);
    FakeShard& shard = fake_->shards[id];
    if (shard.fail_from.has_value() && seq >= *shard.fail_from &&
        fake_->durable_error.has_value()) {
      return std::unexpected(*fake_->durable_error);
    }
    return fake_->durable_cv.wait_for(lock, timeout, [&shard, seq] {
      return !shard.hold_from.has_value() || seq < *shard.hold_from;
    });
  }

  std::optional<core::Result<void>> FakeWhenDurable(core::ShardId id, core::SeqWaiterNode& node) {
    FakeShard& shard = Shard(id);
    {
      const std::scoped_lock lock(fake_->mu);
      if (shard.fail_from.has_value() && node.seq() >= *shard.fail_from &&
          fake_->durable_error.has_value()) {
        return core::Result<void>{std::unexpected(*fake_->durable_error)};
      }
    }
    return shard.waiters.Register(node, shard.end);
  }
  FakeShard& Shard(core::ShardId id) {
    const std::scoped_lock lock(fake_->mu);
    return fake_->shards[id];
  }
  core::WaitList& ListOf(core::QueueWait wait) {
    return wait == core::QueueWait::kAdmission ? fake_->admission : fake_->spares;
  }

  std::shared_ptr<FakeLog> fake_ = std::make_shared<FakeLog>();
  uint32_t log_count_ = 1;
};

// The Read contract over an in-memory log sorted by seq: up to `max_count`
// entries with seq >= `from_seq`.
inline std::vector<core::QueueEntry> ReadFromLog(const std::vector<core::QueueEntry>& log,
                                                 core::SequenceId from_seq, size_t max_count) {
  ABYSS_DCHECK(from_seq >= core::kFirstSeq, "a read from seq 0");
  auto it = std::ranges::lower_bound(log, from_seq, std::ranges::less{},
                                     [](const core::QueueEntry& e) { return e.seq; });
  std::vector<core::QueueEntry> out;
  for (; it != log.end() && out.size() < max_count; ++it) out.push_back(*it);
  return out;
}

}  // namespace abyss::testing
