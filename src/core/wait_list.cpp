#include "abyss/core/wait_list.h"

#include <array>
#include <utility>

#include "abyss/core/fatal.h"

namespace abyss::core {

Waiter::~Waiter() {
  if (linked_) Fatal("Waiter destroyed while linked");
}

WaitList::~WaitList() {
  if (count_.load(std::memory_order_relaxed) != 0) Fatal("WaitList destroyed with waiters");
}

bool WaitList::Cancel(Waiter& waiter) noexcept {
  const std::scoped_lock lock(mu_);
  if (!waiter.linked_) return false;
  Unlink(waiter);
  EndWait(waiter);
  return true;
}

void WaitList::Finish(Waiter& waiter) noexcept {
  bool hand_off = false;
  {
    const std::scoped_lock lock(mu_);
    if (waiter.linked_) Fatal("Finish of a linked Waiter: Cancel it");
    hand_off = waiter.last_wake_ok_;
    EndWait(waiter);
  }
  if (hand_off) HandOff();
}

void WaitList::FailAll(const Error& error) {
  {
    const std::scoped_lock lock(mu_);
    if (!failed_.has_value()) failed_ = error;
  }
  // Nothing links any more, so this drains the list.
  std::array<Waiter*, kWakeBatch> woken{};
  while (const size_t n = PopFront(woken, /*ok=*/false)) {
    for (Waiter* waiter : std::span(woken).first(n)) waiter->Wake(std::unexpected(error));
  }
}

WaitList::Stats WaitList::StatsForTesting() const {
  const std::scoped_lock lock(mu_);
  return stats_;
}

// A new wait takes the next ticket and appends in O(1). A woken wait
// re-registering scans from the head, where its old place is.
void WaitList::Link(Waiter& waiter) {
  if (waiter.linked_) Fatal("Waiter registered twice");
  Waiter* before = nullptr;
  if (waiter.ticket_list_ == this) {
    before = head_;
    while (before != nullptr && before->ticket_ < waiter.ticket_) {
      before = before->next_;
      ++stats_.insert_steps;
    }
  } else {
    waiter.ticket_list_ = this;
    waiter.ticket_ = ++next_ticket_;
  }
  waiter.last_wake_ok_ = false;
  Waiter* const after = before != nullptr ? before->prev_ : tail_;
  waiter.prev_ = after;
  waiter.next_ = before;
  if (after != nullptr) {
    after->next_ = &waiter;
  } else {
    head_ = &waiter;
  }
  if (before != nullptr) {
    before->prev_ = &waiter;
  } else {
    tail_ = &waiter;
  }
  waiter.linked_ = true;
  count_.fetch_add(1, std::memory_order_seq_cst);
}

void WaitList::Unlink(Waiter& waiter) {
  if (waiter.prev_ != nullptr) {
    waiter.prev_->next_ = waiter.next_;
  } else {
    head_ = waiter.next_;
  }
  if (waiter.next_ != nullptr) {
    waiter.next_->prev_ = waiter.prev_;
  } else {
    tail_ = waiter.prev_;
  }
  waiter.prev_ = nullptr;
  waiter.next_ = nullptr;
  waiter.linked_ = false;
  count_.fetch_sub(1, std::memory_order_seq_cst);
}

// Unlinks up to out.size() waiters from the front, recording whether
// their wake is ok. Their waits, and places, go on.
size_t WaitList::PopFront(std::span<Waiter*> out, bool ok) noexcept {
  const std::scoped_lock lock(mu_);
  ++stats_.locked_pops;
  size_t n = 0;
  while (n < out.size() && head_ != nullptr) {
    head_->last_wake_ok_ = ok;
    out[n++] = head_;
    Unlink(*head_);
  }
  return n;
}

void WaitList::WakeFront(size_t limit) noexcept {
  std::array<Waiter*, kWakeBatch> woken{};
  const size_t n = PopFront(std::span(woken).first(limit), /*ok=*/true);
  for (Waiter* waiter : std::span(woken).first(n)) waiter->Wake(Result<void>{});
}

}  // namespace abyss::core
