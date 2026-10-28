#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <vector>

#include "abyss/core/types.h"

namespace abyss::core {

class TimerWheel;

// One timer, held by its owner and linked into a wheel by address.
// Freed, it unlinks itself.
class TimerNode {
 public:
  explicit TimerNode(uint64_t id) noexcept : id_(id) {}
  ~TimerNode();

  TimerNode(const TimerNode&) = delete;
  TimerNode& operator=(const TimerNode&) = delete;
  TimerNode(TimerNode&&) = delete;
  TimerNode& operator=(TimerNode&&) = delete;

  uint64_t id() const noexcept { return id_; }
  bool armed() const noexcept { return wheel_ != nullptr; }
  SteadyTime deadline() const noexcept { return deadline_; }

 private:
  friend class TimerWheel;

  TimerWheel* wheel_ = nullptr;
  TimerNode* prev_ = nullptr;
  TimerNode* next_ = nullptr;
  SteadyTime deadline_{};
  uint64_t tick_ = 0;
  uint64_t id_;
};

// A hashed timing wheel: a bucket per tick, each timer in the bucket of
// the tick its deadline falls in, whatever the round. Schedule and
// Cancel are O(1); Expire visits only the buckets of the ticks passed.
// A timer never fires before its deadline, and at most one tick after
// it once Expire is called. One thread.
class TimerWheel {
 public:
  using Tick = std::chrono::steady_clock::duration;

  TimerWheel(Tick tick, size_t buckets, SteadyTime origin = SteadyClock::now());
  ~TimerWheel();

  TimerWheel(const TimerWheel&) = delete;
  TimerWheel& operator=(const TimerWheel&) = delete;
  TimerWheel(TimerWheel&&) = delete;
  TimerWheel& operator=(TimerWheel&&) = delete;

  // Arms `node` for `deadline`, moving it if armed already.
  void Schedule(TimerNode& node, SteadyTime deadline);
  void Cancel(TimerNode& node) noexcept;
  // Unlinks every timer due by `now`, appending its id to `out`.
  void Expire(SteadyTime now, std::vector<uint64_t>& out);
  // When the next bucket holding a timer comes due, which may hold
  // only timers of a later round; nullopt when none is armed.
  std::optional<SteadyTime> NextDue() const;

  size_t size() const noexcept { return size_; }

 private:
  // The first tick at or after `time`, and the last at or before it.
  uint64_t CeilTick(SteadyTime time) const;
  uint64_t FloorTick(SteadyTime time) const;
  void Link(TimerNode& node, uint64_t tick);
  void Unlink(TimerNode& node) noexcept;

  const Tick tick_;
  const SteadyTime origin_;
  std::vector<TimerNode*> buckets_;
  // A bit per bucket holding a timer.
  std::vector<uint64_t> occupied_;
  // The last tick Expire processed.
  uint64_t current_ = 0;
  size_t size_ = 0;
};

}  // namespace abyss::core
