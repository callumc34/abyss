#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <vector>

#include "abyss/core/completion.h"
#include "abyss/core/durability.h"
#include "abyss/core/resp_types.h"
#include "abyss/core/result.h"
#include "abyss/core/seq_waiters.h"
#include "abyss/core/types.h"

namespace abyss::core {

class Queue;

// A sequenced command's reply, held until what it shows is durable at
// the ack class: one seq per stream it touched. Each stream's node
// posts kDurable to the slot, and the reply goes once the last has
// arrived. Owned by the slot and reused, so once its node storage
// has grown to a command's streams it never allocates again.
class DurableWait {
 public:
  enum class Kind : uint8_t {
    // A write's own frames.
    kWrite,
    kFlush,
    // Another writer's frames that a read or a no-effect write saw.
    kFence,
  };

  // Times a sampled write's wait, for its durable phase.
  class Timer {
   public:
    Timer() = default;
    virtual ~Timer() = default;
    Timer(const Timer&) = delete;
    Timer& operator=(const Timer&) = delete;
    Timer(Timer&&) = delete;
    Timer& operator=(Timer&&) = delete;

    virtual void Waited(std::chrono::steady_clock::duration waited) noexcept = 0;
  };

  DurableWait() = default;
  ~DurableWait();

  DurableWait(const DurableWait&) = delete;
  DurableWait& operator=(const DurableWait&) = delete;
  DurableWait(DurableWait&&) = delete;
  DurableWait& operator=(DurableWait&&) = delete;

  // A new wait, on a quiescent one, for up to `streams` Adds.
  void Begin(Queue& queue, Kind kind, Completion completion, size_t streams,
             Result<RespValue> reply, Timer* timer = nullptr);
  // `seq` on `shard`'s stream: registered, or durable already.
  void Add(ShardId shard, SequenceId seq);
  // Once every Add is in: the reply, unless a post is still to come.
  std::optional<Result<RespValue>> Ready();
  // A kDurable for this wait. The reply once the last arrives, or the
  // first error, which cancels the rest. Late posts after the reply
  // only count down.
  std::optional<Result<RespValue>> Arrived(Result<void> result);
  // At the deadline, while waiting: the timeout reply. The write stays
  // applied. A post a notifier already took still arrives.
  Result<RespValue> Expire();
  // While waiting, for a caller that has gone: no reply. A post a
  // notifier already took still arrives.
  void Abandon();

  bool waiting() const { return waiting_; }
  // The reply it holds while waiting.
  const Result<RespValue>& held() const { return reply_; }
  // No node linked and no post on its way: safe to Begin or free.
  bool Quiescent() const { return outstanding_ == 0; }
  size_t CapacityForTesting() const { return 1 + more_.size(); }
  size_t StreamsForTesting() const { return added_; }

 private:
  struct Stream {
    ShardId shard = 0;
    std::optional<SeqWaiterNode> node;
  };
  Stream& At(size_t i) { return i == 0 ? first_ : *more_[i - 1]; }
  const Stream& At(size_t i) const { return i == 0 ? first_ : *more_[i - 1]; }
  // Unlinks what it can; what it cannot is on its way.
  void CancelRest();
  Result<RespValue> Release();
  Result<RespValue> Failed(Error error);

  Queue* queue_ = nullptr;
  Durability ack_ = Durability::kProcessCrash;
  Kind kind_ = Kind::kWrite;
  Completion completion_;
  Timer* timer_ = nullptr;
  std::chrono::steady_clock::time_point began_;
  Result<RespValue> reply_;
  std::optional<Error> error_;
  bool waiting_ = false;
  size_t added_ = 0;
  // Registered, and not yet arrived or cancelled.
  size_t outstanding_ = 0;
  // A single stream's node is held in place. More are grown once, each
  // on its own, so none moves, and kept. Empty, nothing is allocated
  // on any standard library (libstdc++'s deque allocates when empty).
  Stream first_;
  std::vector<std::unique_ptr<Stream>> more_;
};

}  // namespace abyss::core
