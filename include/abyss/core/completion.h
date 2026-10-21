#pragma once

#include <cstdint>
#include <span>

#include "abyss/core/result.h"

namespace abyss::core {

enum class CompletionKind : uint8_t {
  kSequenced,
  kReplied,
  kDurable,
};

// `result` is kDurable's outcome: ok, or kUnavailable at shutdown.
struct CompletionEvent {
  uint64_t client_id = 0;
  uint64_t slot_seq = 0;
  CompletionKind kind = CompletionKind::kDurable;
  Result<void> result;
};

// A reactor's inbox, or a test's. Post is thread-safe and short: a
// guarded append and a wake. It never calls into a waiter list.
// A sink outlives every Completion that names it.
class CompletionSink {
 public:
  CompletionSink() = default;
  virtual ~CompletionSink() = default;

  CompletionSink(const CompletionSink&) = delete;
  CompletionSink& operator=(const CompletionSink&) = delete;
  CompletionSink(CompletionSink&&) = delete;
  CompletionSink& operator=(CompletionSink&&) = delete;

  virtual void Post(std::span<const CompletionEvent> events) noexcept = 0;
};

// A plain pointer, not shared: the sink outlives every waiter.
struct Completion {
  CompletionSink* sink = nullptr;
  uint64_t client_id = 0;
  uint64_t slot_seq = 0;
};

}  // namespace abyss::core
