#include "abyss/core/seq_waiters.h"

#include <condition_variable>
#include <span>
#include <utility>
#include <vector>

#include "abyss/core/fatal.h"

namespace abyss::core {
namespace {

struct Pending {
  CompletionSink* sink = nullptr;
  CompletionEvent event;
};

// A notifier's scratch, reused so a warm Advance never allocates. It
// is taken out while in use, so a nested use stays correct.
struct Scratch {
  std::vector<Pending> pending;
  std::vector<CompletionEvent> batch;
};

thread_local Scratch t_scratch;

CompletionEvent DurableEvent(const Completion& completion, Result<void> result) {
  return {.client_id = completion.client_id,
          .slot_seq = completion.slot_seq,
          .kind = CompletionKind::kDurable,
          .result = std::move(result)};
}

// One Post per sink, in first-seen order, events in seq order. Only
// allocation can throw here, and an OOM while notifying may terminate.
// NOLINTNEXTLINE(bugprone-exception-escape)
void PostBySink(Scratch& scratch) noexcept {
  std::vector<Pending>& pending = scratch.pending;
  while (!pending.empty()) {
    CompletionSink* const sink = pending.front().sink;
    size_t kept = 0;
    for (size_t i = 0; i < pending.size(); ++i) {
      if (pending[i].sink == sink) {
        scratch.batch.push_back(std::move(pending[i].event));
      } else {
        if (kept != i) pending[kept] = std::move(pending[i]);
        ++kept;
      }
    }
    pending.resize(kept);
    sink->Post(scratch.batch);
    scratch.batch.clear();
  }
}

// A blocking caller's sink, on its stack. Post notifies under mu_, and
// the caller destroys the sink only after relocking mu_ and seeing
// done_, so no notifier is still inside cv_ by then.
class BlockingSink final : public CompletionSink {
 public:
  // NOLINTNEXTLINE(bugprone-exception-escape): an OOM may terminate.
  void Post(std::span<const CompletionEvent> events) noexcept override {
    const std::scoped_lock lock(mu_);
    result_ = events.front().result;
    done_ = true;
    cv_.notify_one();
  }

  bool WaitFor(Duration timeout) {
    std::unique_lock lock(mu_);
    return cv_.wait_for(lock, timeout, [this] ABYSS_REQUIRES(mu_) { return done_; });
  }

  Result<void> Take() {
    std::unique_lock lock(mu_);
    cv_.wait(lock, [this] ABYSS_REQUIRES(mu_) { return done_; });
    return result_;
  }

 private:
  std::mutex mu_;
  std::condition_variable cv_;
  Result<void> result_ ABYSS_GUARDED_BY(mu_);
  bool done_ ABYSS_GUARDED_BY(mu_) = false;
};

}  // namespace

SeqWaiterNode::~SeqWaiterNode() {
  if (linked_) Fatal("SeqWaiterNode destroyed while linked");
}

SeqWaiters::~SeqWaiters() {
  if (count_.load(std::memory_order_relaxed) != 0) Fatal("SeqWaiters destroyed with waiters");
}

std::optional<Result<void>> SeqWaiters::Register(SeqWaiterNode& node,
                                                 const std::atomic<SequenceId>& end) {
  const std::scoped_lock lock(mu_);
  if (failed_.has_value()) return Result<void>{std::unexpected(*failed_)};
  if (node.linked_) Fatal("SeqWaiterNode registered twice");
  Insert(node);
  // Re-checked under mu_, so no notifier can hold the node: only this
  // path completes it.
  if (end.load(std::memory_order_seq_cst) > node.seq_) {
    Unlink(node);
    return Result<void>{};
  }
  return std::nullopt;
}

bool SeqWaiters::Cancel(SeqWaiterNode& node) noexcept {
  const std::scoped_lock lock(mu_);
  if (!node.linked_) return false;
  Unlink(node);
  return true;
}

// Only allocation can throw here, and an OOM while notifying may
// terminate.
// NOLINTNEXTLINE(bugprone-exception-escape)
void SeqWaiters::AdvanceSlow(SequenceId new_end) noexcept {
  Scratch scratch = std::exchange(t_scratch, {});
  {
    const std::scoped_lock lock(mu_);
    ++stats_.locked_pops;
    while (head_ != nullptr && head_->seq_ < new_end) {
      SeqWaiterNode& node = *head_;
      scratch.pending.push_back({node.completion_.sink, DurableEvent(node.completion_, {})});
      Unlink(node);
    }
  }
  // Only the posts follow the unlock: a blocking waiter whose Cancel
  // lost is waiting on exactly these.
  PostBySink(scratch);
  t_scratch = std::move(scratch);
}

void SeqWaiters::FailAll(const Error& error) {
  Scratch scratch = std::exchange(t_scratch, {});
  {
    const std::scoped_lock lock(mu_);
    ++stats_.locked_pops;
    if (!failed_.has_value()) failed_ = error;
    while (head_ != nullptr) {
      SeqWaiterNode& node = *head_;
      scratch.pending.push_back(
          {node.completion_.sink, DurableEvent(node.completion_, std::unexpected(error))});
      Unlink(node);
    }
  }
  PostBySink(scratch);
  t_scratch = std::move(scratch);
}

Result<void> SeqWaiters::AwaitEnd(const std::atomic<SequenceId>& end, SequenceId seq,
                                  Duration timeout) {
  if (end.load(std::memory_order_acquire) > seq) return Result<void>{};
  BlockingSink sink;
  SeqWaiterNode node(seq, Completion{.sink = &sink, .client_id = 0, .slot_seq = seq});
  if (auto ready = Register(node, end)) return *std::move(ready);
  // A lost Cancel means a notifier has the node's Completion and posts
  // next, so the sink must outlive that post.
  if (!sink.WaitFor(timeout) && Cancel(node)) {
    return std::unexpected(Error{ErrorCode::kTimeout, "seq wait timed out"});
  }
  return sink.Take();
}

SeqWaiters::Stats SeqWaiters::StatsForTesting() const {
  const std::scoped_lock lock(mu_);
  return stats_;
}

// Scans from the tail, so the common in-order insert is O(1). Equal
// seqs keep their registration order.
void SeqWaiters::Insert(SeqWaiterNode& node) {
  SeqWaiterNode* after = tail_;
  while (after != nullptr && after->seq_ > node.seq_) {
    after = after->prev_;
    ++stats_.insert_steps;
  }
  SeqWaiterNode* const before = after != nullptr ? after->next_ : head_;
  node.prev_ = after;
  node.next_ = before;
  if (after != nullptr) {
    after->next_ = &node;
  } else {
    head_ = &node;
  }
  if (before != nullptr) {
    before->prev_ = &node;
  } else {
    tail_ = &node;
  }
  node.linked_ = true;
  count_.fetch_add(1, std::memory_order_seq_cst);
}

void SeqWaiters::Unlink(SeqWaiterNode& node) {
  if (node.prev_ != nullptr) {
    node.prev_->next_ = node.next_;
  } else {
    head_ = node.next_;
  }
  if (node.next_ != nullptr) {
    node.next_->prev_ = node.prev_;
  } else {
    tail_ = node.prev_;
  }
  node.prev_ = nullptr;
  node.next_ = nullptr;
  node.linked_ = false;
  count_.fetch_sub(1, std::memory_order_seq_cst);
}

}  // namespace abyss::core
