#pragma once

#include <chrono>
#include <cstdint>
#include <span>
#include <vector>

#include "abyss/core/result.h"
#include "abyss/net/poller.h"

namespace abyss::net::testing {

// Recording Poller for Connection FSM tests; single-threaded.
class FakePoller : public Poller {
 public:
  enum class Op : uint8_t { kAdd, kModify, kRemove, kWake };

  struct Call {
    Op op = Op::kAdd;
    int fd = -1;
    EventKind interest = EventKind::kNone;
    void* user_data = nullptr;
  };

  std::vector<Call> calls;
  std::vector<Event> next_events;

  core::Result<void> Add(int fd, EventKind interest, void* user_data) override {
    calls.push_back({Op::kAdd, fd, interest, user_data});
    return {};
  }
  core::Result<void> Modify(int fd, EventKind interest, void* user_data) override {
    calls.push_back({Op::kModify, fd, interest, user_data});
    return {};
  }
  core::Result<void> Remove(int fd) override {
    calls.push_back({Op::kRemove, fd, EventKind::kNone, nullptr});
    return {};
  }
  core::Result<std::span<const Event>> Wait(std::chrono::milliseconds /*timeout*/) override {
    return std::span<const Event>{next_events};
  }
  core::Result<void> Wake() override {
    calls.push_back({Op::kWake, -1, EventKind::kNone, nullptr});
    return {};
  }

  EventKind LastInterest() const noexcept {
    EventKind interest = EventKind::kNone;
    for (const auto& c : calls) {
      if (c.op == Op::kAdd || c.op == Op::kModify) interest = c.interest;
    }
    return interest;
  }

  size_t CountOf(Op op) const noexcept {
    size_t n = 0;
    for (const auto& c : calls) {
      if (c.op == op) ++n;
    }
    return n;
  }
};

}  // namespace abyss::net::testing
