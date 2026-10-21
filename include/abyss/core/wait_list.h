#pragma once

#include <atomic>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <mutex>
#include <optional>
#include <span>

#include "abyss/core/result.h"
#include "abyss/core/thread_annotations.h"
#include "abyss/core/types.h"

namespace abyss::core {

class WaitList;

// Owned by whoever registered it; a WaitList holds only a raw link.
// While linked, the owner frees it only after a true Cancel, or after
// its Wake has arrived.
class Waiter {
 public:
  virtual ~Waiter();

  Waiter(const Waiter&) = delete;
  Waiter& operator=(const Waiter&) = delete;
  Waiter(Waiter&&) = delete;
  Waiter& operator=(Waiter&&) = delete;

  // Once per wake, on the notifier's thread, unlinked and outside the
  // list's lock. Touches nothing of *this after handing the wake on.
  virtual void Wake(Result<void> result) noexcept = 0;

 protected:
  Waiter() = default;

 private:
  friend class WaitList;

  Waiter* prev_ = nullptr;
  Waiter* next_ = nullptr;
  // This wait's place in ticket_list_. Kept across wakes, so a woken
  // waiter that re-blocks goes back to it; cleared when the wait ends.
  const WaitList* ticket_list_ = nullptr;
  uint64_t ticket_ = 0;
  // Whether the last wake was ok, which Finish must pass on.
  bool last_wake_ok_ = false;
  bool linked_ = false;
};

// A FIFO of waiters for capacity: admission, a spare segment, memory.
// A wait keeps the ticket it first took, so a woken waiter that
// re-blocks goes back to its FIFO place. The wait, and its place, ends
// when Register returns an outcome, on a true Cancel, or on Finish; a
// reused waiter's next wait queues afresh.
//
// No lost wake-ups: the owner publishes its state seq_cst, then calls
// Notify, which loads the count seq_cst. Register counts the waiter
// seq_cst, then runs the condition, whose loads must be seq_cst.
class WaitList {
 public:
  static constexpr size_t kWakeBatch = 64;

  struct Stats {
    uint64_t locked_pops = 0;
    uint64_t insert_steps = 0;
  };

  WaitList() = default;
  ~WaitList();

  WaitList(const WaitList&) = delete;
  WaitList& operator=(const WaitList&) = delete;
  WaitList(WaitList&&) = delete;
  WaitList& operator=(WaitList&&) = delete;

  // Links the waiter, then re-checks `condition` under the list's lock,
  // so it must take no lock a notifier holds across Notify. nullopt:
  // linked, and Notify or FailAll will wake it. Otherwise the outcome
  // now (ok: the condition held; or FailAll's error), the wait ends,
  // and no wake comes.
  template <typename Condition>
    requires std::predicate<const Condition&>
  std::optional<Result<void>> Register(Waiter& waiter, const Condition& condition) {
    const std::scoped_lock lock(mu_);
    if (failed_.has_value()) {
      EndWait(waiter);
      return Result<void>{std::unexpected(*failed_)};
    }
    Link(waiter);
    if (condition()) {
      Unlink(waiter);
      EndWait(waiter);
      return Result<void>{};
    }
    return std::nullopt;
  }

  // For a waiter whose Register returned nullopt. True: unlinked, the
  // wait ends, and no wake comes. False: a notifier took it and will
  // wake it.
  bool Cancel(Waiter& waiter) noexcept;

  // Ends the wait of a woken waiter that will not register again: it
  // gave up (a deadline, a client gone), or it passed its own check.
  // If the wake was ok, it may have been the one capacity was meant
  // for, so it hands off. The list recorded that outcome as it woke
  // the waiter, so an owner cannot pass on the wrong one.
  void Finish(Waiter& waiter) noexcept;

  // Wakes up to kWakeBatch waiters, in FIFO order.
  void Notify() noexcept {
    if (count_.load(std::memory_order_seq_cst) == 0) return;
    WakeFront(kWakeBatch);
  }

  // Called by a woken waiter whose condition held (an attempt that
  // passed admission, or found a spare or memory) before it goes on.
  // Wakes the next waiter, which hands on in turn if its own condition
  // holds. The chain never strands a batch that passes and then blocks
  // elsewhere, and each waiter that passes wakes one more, not a batch.
  void HandOff() noexcept {
    if (count_.load(std::memory_order_seq_cst) == 0) return;
    WakeFront(1);
  }

  // For shutdown: wakes every waiter with `error`, and every later
  // Register returns it.
  void FailAll(const Error& error);

  // The blocking wrapper: ok once `condition` holds, kTimeout, or
  // FailAll's error. A woken caller whose condition holds hands off,
  // so it strands no one where notify_all would have woken them. A
  // wake collected just past the deadline is still re-checked, so it
  // can return ok then.
  Result<void> Await(const std::function<bool()>& condition, Duration timeout);

  size_t size() const noexcept { return count_.load(std::memory_order_relaxed); }

  Stats StatsForTesting() const;

 private:
  static void EndWait(Waiter& waiter) noexcept {
    waiter.ticket_list_ = nullptr;
    waiter.last_wake_ok_ = false;
  }
  void Link(Waiter& waiter) ABYSS_REQUIRES(mu_);
  void Unlink(Waiter& waiter) ABYSS_REQUIRES(mu_);
  size_t PopFront(std::span<Waiter*> out, bool ok) noexcept;
  void WakeFront(size_t limit) noexcept;

  mutable std::mutex mu_;
  Waiter* head_ ABYSS_GUARDED_BY(mu_) = nullptr;
  Waiter* tail_ ABYSS_GUARDED_BY(mu_) = nullptr;
  uint64_t next_ticket_ ABYSS_GUARDED_BY(mu_) = 0;
  std::optional<Error> failed_ ABYSS_GUARDED_BY(mu_);
  Stats stats_ ABYSS_GUARDED_BY(mu_);
  // Linked waiters: written under mu_, read lock-free by Notify.
  std::atomic<size_t> count_{0};
};

}  // namespace abyss::core
