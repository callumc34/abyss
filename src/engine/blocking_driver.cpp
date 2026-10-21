#include "abyss/engine/blocking_driver.h"

#include <condition_variable>
#include <mutex>
#include <span>
#include <utility>

#include "abyss/core/completion.h"
#include "abyss/core/thread_annotations.h"
#include "abyss/core/types.h"
#include "abyss/engine/pending_write.h"

namespace abyss::engine {

namespace {

// The driver's sink, on its stack, for one write's posts, one at a
// time. Post notifies under mu_, and the driver lets the sink go only
// after relocking mu_ and seeing the post, so no poster is inside it.
class BlockingSink final : public core::CompletionSink {
 public:
  // NOLINTNEXTLINE(bugprone-exception-escape): an OOM may terminate.
  void Post(std::span<const core::CompletionEvent> events) noexcept override {
    const std::scoped_lock lock(mu_);
    result_ = events.back().result;
    posted_ = true;
    cv_.notify_one();
  }

  bool WaitUntil(core::SteadyTime deadline) {
    std::unique_lock lock(mu_);
    return cv_.wait_until(lock, deadline, [this] ABYSS_REQUIRES(mu_) { return posted_; });
  }

  core::Result<void> Take() {
    std::unique_lock lock(mu_);
    cv_.wait(lock, [this] ABYSS_REQUIRES(mu_) { return posted_; });
    posted_ = false;
    return std::exchange(result_, {});
  }

 private:
  std::mutex mu_;
  std::condition_variable cv_;
  core::Result<void> result_ ABYSS_GUARDED_BY(mu_);
  bool posted_ ABYSS_GUARDED_BY(mu_) = false;
};

core::Result<core::RespValue> Drive(Sequencer& sequencer, PendingWrite& write, BlockingSink& sink) {
  for (;;) {
    Step step = sequencer.Attempt(write);
    if (step.kind == Step::Kind::kReplied) return std::move(step.reply);
    if (step.kind == Step::Kind::kWait && step.reason == WaitReason::kLoads) {
      sequencer.Load(write);
      continue;
    }
    // A lost Cancel means the wake is on its way.
    if (!sink.WaitUntil(write.deadline()) && sequencer.Expire(write)) continue;
    write.Woken(sink.Take());
  }
}

core::SteadyTime DeadlineOf(const Sequencer& sequencer) {
  return core::SteadyClock::now() + sequencer.config().write_timeout;
}

}  // namespace

core::Result<core::RespValue> RunToCompletion(Sequencer& sequencer, core::RespCommand cmd,
                                              core::PredicateFlags flags) {
  BlockingSink sink;
  PendingWrite write(std::move(cmd), flags, core::Completion{.sink = &sink}, DeadlineOf(sequencer));
  return Drive(sequencer, write, sink);
}

core::Result<core::RespValue> RunFlushToCompletion(Sequencer& sequencer) {
  BlockingSink sink;
  PendingWrite write(PendingWrite::FlushAll{}, core::Completion{.sink = &sink},
                     DeadlineOf(sequencer));
  return Drive(sequencer, write, sink);
}

}  // namespace abyss::engine
