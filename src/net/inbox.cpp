#include "inbox.h"

#include <utility>

#include "abyss/core/fatal.h"
#include "abyss/log/log.h"

ABYSS_LOG_COMPONENT("abyss.net.inbox")

namespace abyss::net {

Inbox::~Inbox() {
  const std::scoped_lock lock(mu_);
  ABYSS_DCHECK(events_.empty(), "an inbox freed with events in it");
}

// Only allocation can throw here, and an OOM while posting may
// terminate.
// NOLINTNEXTLINE(bugprone-exception-escape)
void Inbox::Post(std::span<const core::CompletionEvent> events) noexcept {
  if (const auto* hook = post_hook_.load(std::memory_order_acquire); hook != nullptr) (*hook)();
  {
    const std::scoped_lock lock(mu_);
    ABYSS_DCHECK(!closed_, "an engine event posted to a stopped reactor");
    events_.insert(events_.end(), events.begin(), events.end());
  }
  // Set since the reactor last cleared it: a wake is on its way.
  if (wake_pending_.exchange(true, std::memory_order_acq_rel)) return;
  if (auto woken = poller_.Wake(); !woken.has_value()) {
    ABYSS_LOG_WARN("reactor wake failed", {"err", std::string_view{woken.error().message()}});
  }
}

void Inbox::Take(std::vector<core::CompletionEvent>& out) {
  // Before the swap: a post after the swap then finds it clear and
  // wakes the poller, and one before it is taken here.
  wake_pending_.exchange(false, std::memory_order_acq_rel);
  if (take_hook_) take_hook_();
  const std::scoped_lock lock(mu_);
  out.swap(events_);
}

void Inbox::SetPostHookForTesting(std::function<void()> hook) {
  post_hook_storage_ = std::make_unique<std::function<void()>>(std::move(hook));
  post_hook_.store(post_hook_storage_.get(), std::memory_order_release);
}

void Inbox::Close() {
  const std::scoped_lock lock(mu_);
  closed_ = true;
}

}  // namespace abyss::net
