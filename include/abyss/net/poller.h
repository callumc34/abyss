#pragma once

#include <chrono>
#include <cstdint>
#include <memory>
#include <span>

#include "abyss/core/result.h"
#include "abyss/platform/types.h"

namespace abyss::net {

using platform::kInvalidSocket;
using platform::Socket;

enum class EventKind : uint8_t {
  kNone = 0,
  kReadable = 1U << 0U,
  kWritable = 1U << 1U,
};

constexpr EventKind operator|(EventKind a, EventKind b) noexcept {
  // NOLINTNEXTLINE(clang-analyzer-optin.core.EnumCastOutOfRange)
  return static_cast<EventKind>(static_cast<uint8_t>(a) | static_cast<uint8_t>(b));
}
constexpr EventKind operator&(EventKind a, EventKind b) noexcept {
  // NOLINTNEXTLINE(clang-analyzer-optin.core.EnumCastOutOfRange)
  return static_cast<EventKind>(static_cast<uint8_t>(a) & static_cast<uint8_t>(b));
}
constexpr EventKind& operator|=(EventKind& a, EventKind b) noexcept {
  a = a | b;
  return a;
}
constexpr bool Has(EventKind mask, EventKind bit) noexcept {
  return (static_cast<uint8_t>(mask) & static_cast<uint8_t>(bit)) != 0;
}

struct Event {
  void* user_data = nullptr;
  EventKind kinds = EventKind::kNone;
};

// Edge-triggered I/O readiness multiplexer. One per reactor thread; only
// Wake() is safe to call across threads.
class Poller {
 public:
  Poller() = default;
  virtual ~Poller() = default;
  Poller(const Poller&) = delete;
  Poller& operator=(const Poller&) = delete;
  Poller(Poller&&) = delete;
  Poller& operator=(Poller&&) = delete;

  virtual core::Result<void> Add(Socket fd, EventKind interest, void* user_data) = 0;
  virtual core::Result<void> Modify(Socket fd, EventKind interest, void* user_data) = 0;
  virtual core::Result<void> Remove(Socket fd) = 0;

  // Returned span is valid until the next Wait() call.
  virtual core::Result<std::span<const Event>> Wait(std::chrono::milliseconds timeout) = 0;

  // Thread-safe. Coalesces bursts; not delivered as a user-visible Event.
  virtual core::Result<void> Wake() = 0;
};

core::Result<std::unique_ptr<Poller>> CreatePoller();

}  // namespace abyss::net
