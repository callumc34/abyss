#pragma once

#include <atomic>
#include <functional>
#include <memory>
#include <mutex>
#include <span>
#include <utility>
#include <vector>

#include "abyss/core/completion.h"
#include "abyss/core/thread_annotations.h"
#include "abyss/net/poller.h"

namespace abyss::net {

// A reactor's engine events. Any thread posts; the reactor takes them
// all at once. Posts between two takes wake the poller at most once:
// the first sets the flag, and the reactor clears it before it takes,
// so a post the take misses always wakes it again. The reactor frees
// it only once every poster is joined, so Post may touch it after its
// events are visible.
class Inbox final : public core::CompletionSink {
 public:
  explicit Inbox(Poller& poller) : poller_(poller) {}
  ~Inbox() override;

  Inbox(const Inbox&) = delete;
  Inbox& operator=(const Inbox&) = delete;
  Inbox(Inbox&&) = delete;
  Inbox& operator=(Inbox&&) = delete;

  void Post(std::span<const core::CompletionEvent> events) noexcept override;

  // The reactor's: swaps every event posted since the last take into
  // `out`, which it hands back empty.
  void Take(std::vector<core::CompletionEvent>& out);
  // After the reactor's last take: a later post is a bug.
  void Close();

  bool WakePendingForTesting() const { return wake_pending_.load(std::memory_order_acquire); }
  // Runs inside Take, after the flag is cleared and before the swap.
  void SetTakeHookForTesting(std::function<void()> hook) { take_hook_ = std::move(hook); }
  // Runs at the start of each Post, on the poster's thread. Set once,
  // before anything posts.
  void SetPostHookForTesting(std::function<void()> hook);

 private:
  Poller& poller_;
  std::mutex mu_;
  std::vector<core::CompletionEvent> events_ ABYSS_GUARDED_BY(mu_);
  bool closed_ ABYSS_GUARDED_BY(mu_) = false;
  std::atomic<bool> wake_pending_{false};
  std::function<void()> take_hook_;
  std::unique_ptr<std::function<void()>> post_hook_storage_;
  std::atomic<const std::function<void()>*> post_hook_{nullptr};
};

}  // namespace abyss::net
