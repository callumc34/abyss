#pragma once

#include <condition_variable>
#include <mutex>
#include <optional>
#include <span>
#include <utility>

#include "abyss/core/completion.h"
#include "abyss/core/result.h"
#include "abyss/core/seq_waiters.h"
#include "abyss/core/thread_annotations.h"
#include "abyss/core/types.h"
#include "abyss/core/wait_list.h"

// Blocking waits for tests over the self-registering primitives: a
// Waiter on a WaitList owner, or a SeqWaiterNode on a SeqWaiters one.
namespace abyss::testing {

// One delivery, to its owner's stack. Put notifies under mu_, and the
// owner lets it go only after relocking mu_ and seeing the delivery,
// so no notifier is still inside it by then.
class Mailbox {
 public:
  void Put(core::Result<void> result) noexcept {
    const std::scoped_lock lock(mu_);
    result_ = std::move(result);
    delivered_ = true;
    cv_.notify_one();
  }

  bool WaitUntil(core::SteadyTime deadline) {
    std::unique_lock lock(mu_);
    return cv_.wait_until(lock, deadline, [this] ABYSS_REQUIRES(mu_) { return delivered_; });
  }

  // Waits for the delivery, then rearms for the next.
  core::Result<void> Take() {
    std::unique_lock lock(mu_);
    cv_.wait(lock, [this] ABYSS_REQUIRES(mu_) { return delivered_; });
    delivered_ = false;
    return std::exchange(result_, {});
  }

 private:
  std::mutex mu_;
  std::condition_variable cv_;
  core::Result<void> result_ ABYSS_GUARDED_BY(mu_);
  bool delivered_ ABYSS_GUARDED_BY(mu_) = false;
};

class BlockingWaiter final : public core::Waiter {
 public:
  void Wake(core::Result<void> result) noexcept override { mailbox_.Put(std::move(result)); }
  Mailbox& mailbox() noexcept { return mailbox_; }

 private:
  Mailbox mailbox_;
};

class BlockingSink final : public core::CompletionSink {
 public:
  // NOLINTNEXTLINE(bugprone-exception-escape): an OOM may terminate.
  void Post(std::span<const core::CompletionEvent> events) noexcept override {
    mailbox_.Put(events.front().result);
  }
  Mailbox& mailbox() noexcept { return mailbox_; }

 private:
  Mailbox mailbox_;
};

// A Waiter's wait, as WaitList's protocol has it: `when(waiter, first)`
// registers, `cancel(waiter)` unlinks, and `finish(waiter)` ends a wait
// a failed wake ended. ok, kTimeout at `deadline`, or the error that
// ends every wait.
template <typename When, typename Cancel, typename Finish>
core::Result<void> AwaitWaiter(const When& when, const Cancel& cancel, const Finish& finish,
                               core::SteadyTime deadline) {
  BlockingWaiter waiter;
  bool first = true;
  for (;;) {
    // A woken pass hands off inside the registration.
    if (std::optional<core::Result<void>> ready = when(waiter, first)) return *std::move(ready);
    first = false;
    // A lost Cancel means a wake is on its way, and the waiter must
    // outlive it.
    if (!waiter.mailbox().WaitUntil(deadline) && cancel(waiter)) {
      return std::unexpected(core::Error{core::ErrorCode::kTimeout, "wait timed out"});
    }
    if (core::Result<void> wake = waiter.mailbox().Take(); !wake.has_value()) {
      finish(waiter);
      return wake;
    }
  }
}

// A wait for an owner's end to pass `seq`, as SeqWaiters' protocol has
// it: `when(node)` registers and `cancel(node)` unlinks. ok, kTimeout
// with no node left linked, or the error that fails every waiter.
template <typename When, typename Cancel>
core::Result<void> AwaitNode(core::SequenceId seq, core::Duration timeout, const When& when,
                             const Cancel& cancel) {
  BlockingSink sink;
  core::SeqWaiterNode node(seq, core::Completion{.sink = &sink, .client_id = 0, .slot_seq = seq});
  if (std::optional<core::Result<void>> now = when(node)) return *std::move(now);
  // A lost Cancel means a post is on its way, and the sink must outlive
  // it.
  if (!sink.mailbox().WaitUntil(core::SteadyClock::now() + timeout) && cancel(node)) {
    return std::unexpected(core::Error{core::ErrorCode::kTimeout, "wait timed out"});
  }
  return sink.mailbox().Take();
}

inline bool TimedOut(const core::Result<void>& waited) {
  return !waited.has_value() && waited.error().code() == core::ErrorCode::kTimeout;
}

}  // namespace abyss::testing
