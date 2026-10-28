#include "abyss/core/timer_wheel.h"

#include <algorithm>
#include <bit>

#include "abyss/core/fatal.h"

namespace abyss::core {

namespace {

constexpr size_t kWordBits = 64;

}  // namespace

TimerNode::~TimerNode() {
  if (wheel_ != nullptr) wheel_->Cancel(*this);
}

TimerWheel::TimerWheel(Tick tick, size_t buckets, SteadyTime origin)
    : tick_(tick),
      origin_(origin),
      buckets_(buckets, nullptr),
      occupied_((buckets + kWordBits - 1) / kWordBits, 0) {
  const bool sized = tick.count() > 0 && buckets > 0;
  ABYSS_DCHECK(sized, "a timer wheel needs a tick and a bucket");
}

TimerWheel::~TimerWheel() {
  // Whatever is still armed outlives the wheel only detached.
  for (TimerNode* head : buckets_) {
    for (TimerNode* node = head; node != nullptr;) {
      TimerNode* const next = node->next_;
      node->wheel_ = nullptr;
      node->prev_ = nullptr;
      node->next_ = nullptr;
      node = next;
    }
  }
}

void TimerWheel::Schedule(TimerNode& node, SteadyTime deadline) {
  const bool ours = node.wheel_ == nullptr || node.wheel_ == this;
  ABYSS_DCHECK(ours, "a timer armed on another wheel");
  const uint64_t tick = std::max(CeilTick(deadline), current_ + 1);
  node.deadline_ = deadline;
  if (node.wheel_ == this) {
    if (node.tick_ == tick) return;
    Unlink(node);
  }
  Link(node, tick);
}

void TimerWheel::Cancel(TimerNode& node) noexcept {
  if (node.wheel_ == this) Unlink(node);
}

void TimerWheel::Expire(SteadyTime now, std::vector<uint64_t>& out) {
  const uint64_t target = FloorTick(now);
  if (target <= current_) return;
  const uint64_t visits = std::min<uint64_t>(target - current_, buckets_.size());
  for (uint64_t step = 1; step <= visits && size_ > 0; ++step) {
    for (TimerNode* node = buckets_[(current_ + step) % buckets_.size()]; node != nullptr;) {
      TimerNode* const next = node->next_;
      // A timer of a later round stays.
      if (node->tick_ <= target) {
        Unlink(*node);
        out.push_back(node->id_);
      }
      node = next;
    }
  }
  current_ = target;
}

std::optional<SteadyTime> TimerWheel::NextDue() const {
  if (size_ == 0) return std::nullopt;
  const size_t n = buckets_.size();
  const size_t start = (current_ + 1) % n;
  // The first occupied bucket in [from, to).
  const auto first = [this](size_t from, size_t to) -> std::optional<size_t> {
    for (size_t i = from; i < to;) {
      const uint64_t bits = occupied_[i / kWordBits] >> (i % kWordBits);
      if (bits != 0) {
        const size_t found = i + static_cast<size_t>(std::countr_zero(bits));
        if (found < to) return found;
        return std::nullopt;
      }
      i = ((i / kWordBits) + 1) * kWordBits;
    }
    return std::nullopt;
  };
  std::optional<size_t> bucket = first(start, n);
  size_t distance = 0;
  if (bucket.has_value()) {
    distance = *bucket - start;
  } else {
    bucket = first(0, start);
    if (!bucket.has_value()) return std::nullopt;
    distance = (n - start) + *bucket;
  }
  const uint64_t due = current_ + 1 + distance;
  return origin_ + (tick_ * static_cast<int64_t>(due));
}

uint64_t TimerWheel::CeilTick(SteadyTime time) const {
  if (time <= origin_) return 0;
  const auto span = (time - origin_).count();
  return static_cast<uint64_t>((span + tick_.count() - 1) / tick_.count());
}

uint64_t TimerWheel::FloorTick(SteadyTime time) const {
  if (time <= origin_) return 0;
  return static_cast<uint64_t>((time - origin_).count() / tick_.count());
}

void TimerWheel::Link(TimerNode& node, uint64_t tick) {
  const size_t bucket = tick % buckets_.size();
  node.tick_ = tick;
  node.prev_ = nullptr;
  node.next_ = buckets_[bucket];
  if (node.next_ != nullptr) node.next_->prev_ = &node;
  buckets_[bucket] = &node;
  occupied_[bucket / kWordBits] |= uint64_t{1} << (bucket % kWordBits);
  node.wheel_ = this;
  ++size_;
}

void TimerWheel::Unlink(TimerNode& node) noexcept {
  const size_t bucket = node.tick_ % buckets_.size();
  if (node.prev_ != nullptr) {
    node.prev_->next_ = node.next_;
  } else {
    buckets_[bucket] = node.next_;
  }
  if (node.next_ != nullptr) node.next_->prev_ = node.prev_;
  if (buckets_[bucket] == nullptr) {
    occupied_[bucket / kWordBits] &= ~(uint64_t{1} << (bucket % kWordBits));
  }
  node.wheel_ = nullptr;
  node.prev_ = nullptr;
  node.next_ = nullptr;
  --size_;
}

}  // namespace abyss::core
