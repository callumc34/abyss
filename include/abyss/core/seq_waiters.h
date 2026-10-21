#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <optional>

#include "abyss/core/completion.h"
#include "abyss/core/result.h"
#include "abyss/core/thread_annotations.h"
#include "abyss/core/types.h"

namespace abyss::core {

// Owned by its waiter: a connection slot, or a blocking caller's
// stack. Once a notifier unlinks it, the list never touches it again.
class SeqWaiterNode {
 public:
  SeqWaiterNode(SequenceId seq, Completion completion) noexcept
      : seq_(seq), completion_(completion) {}
  ~SeqWaiterNode();

  SeqWaiterNode(const SeqWaiterNode&) = delete;
  SeqWaiterNode& operator=(const SeqWaiterNode&) = delete;
  SeqWaiterNode(SeqWaiterNode&&) = delete;
  SeqWaiterNode& operator=(SeqWaiterNode&&) = delete;

  SequenceId seq() const noexcept { return seq_; }

 private:
  friend class SeqWaiters;

  SequenceId seq_;
  Completion completion_;
  SeqWaiterNode* prev_ = nullptr;
  SeqWaiterNode* next_ = nullptr;
  bool linked_ = false;
};

// Waiters for an owner's monotonic exclusive end to pass their seq
// (ready once end > seq), kept in seq order. A notifier posts kDurable
// to each, one batch per sink, and never allocates once warm.
//
// No lost wake-ups: the owner stores its end seq_cst, then calls
// Advance, which loads the count seq_cst. Register counts the node
// seq_cst, then reloads the end seq_cst. One of them sees the other.
class SeqWaiters {
 public:
  struct Stats {
    uint64_t locked_pops = 0;
    uint64_t insert_steps = 0;
  };

  SeqWaiters() = default;
  ~SeqWaiters();

  SeqWaiters(const SeqWaiters&) = delete;
  SeqWaiters& operator=(const SeqWaiters&) = delete;
  SeqWaiters(SeqWaiters&&) = delete;
  SeqWaiters& operator=(SeqWaiters&&) = delete;

  // The caller checks the end first; this re-checks it once linked.
  // nullopt: linked, and a notifier will post. Otherwise the outcome
  // now (ok, or FailAll's error), and nothing will post.
  std::optional<Result<void>> Register(SeqWaiterNode& node, const std::atomic<SequenceId>& end);

  // For a node whose Register returned nullopt. True: unlinked, and
  // nothing will post. False: a notifier took the node and will post
  // to its sink; the node itself is already free.
  bool Cancel(SeqWaiterNode& node) noexcept;

  // Posts ok to every waiter below new_end.
  void Advance(SequenceId new_end) noexcept {
    if (count_.load(std::memory_order_seq_cst) == 0) return;
    AdvanceSlow(new_end);
  }

  // For shutdown: posts `error` to every waiter, and every later
  // Register returns it.
  void FailAll(const Error& error);

  // The blocking wrapper: ok once end > seq, kTimeout, or FailAll's
  // error.
  Result<void> AwaitEnd(const std::atomic<SequenceId>& end, SequenceId seq, Duration timeout);

  size_t size() const noexcept { return count_.load(std::memory_order_relaxed); }

  Stats StatsForTesting() const;

 private:
  void AdvanceSlow(SequenceId new_end) noexcept;
  void Insert(SeqWaiterNode& node) ABYSS_REQUIRES(mu_);
  void Unlink(SeqWaiterNode& node) ABYSS_REQUIRES(mu_);

  mutable std::mutex mu_;
  SeqWaiterNode* head_ ABYSS_GUARDED_BY(mu_) = nullptr;
  SeqWaiterNode* tail_ ABYSS_GUARDED_BY(mu_) = nullptr;
  std::optional<Error> failed_ ABYSS_GUARDED_BY(mu_);
  Stats stats_ ABYSS_GUARDED_BY(mu_);
  // Linked nodes: written under mu_, read lock-free by Advance.
  std::atomic<size_t> count_{0};
};

}  // namespace abyss::core
