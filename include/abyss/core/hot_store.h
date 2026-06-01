#pragma once

#include <cstdint>
#include <optional>
#include <span>
#include <string_view>

#include "abyss/core/ops.h"
#include "abyss/core/reader.h"
#include "abyss/core/resp_types.h"
#include "abyss/core/result.h"
#include "abyss/core/types.h"

namespace abyss::core {

struct MemoryStats {
  uint64_t used_bytes = 0;
  uint64_t key_count = 0;
  // Tier transitions (deadline + memory-pressure eviction). Distinct from
  // expired_count, which counts TTL deletions (HOT-7).
  uint64_t eviction_count = 0;
  uint64_t expired_count = 0;
  // Configured memory budget; 0 means unlimited. Lets the exporter publish the
  // ceiling alongside usage (HOT-1 observability).
  uint64_t max_bytes = 0;
};

// Existence verdict for a key in the hot tier. kTombstoned is an authoritative
// delete the caller must honour; kAbsent means hot is unaware and the caller
// should fall through. Mirrors consumer::BufferKeyPresence.
enum class HotKeyPresence : uint8_t { kAbsent, kTombstoned, kPresent };

class HotStore : public Reader {
 public:
  HotStore() = default;
  ~HotStore() override = default;
  HotStore(const HotStore&) = delete;
  HotStore& operator=(const HotStore&) = delete;
  HotStore(HotStore&&) = delete;
  HotStore& operator=(HotStore&&) = delete;

  // Deadline is accepted for interface uniformity; the in-memory hot tier
  // serves reads in microseconds and ignores the parameter.
  Result<RespValue> Exec(const ops::ReadOp& op,
                         std::optional<Duration> deadline = std::nullopt) override = 0;

  // Returns the typed reply the client observes (+OK for SET, count for SADD/etc.).
  // Per-key eviction (if any) is resolved internally by the implementation.
  // `seq` is the queue seq of the entry being applied; a delete records it on
  // the tombstone it leaves so GC can reclaim it once cold has caught up.
  virtual Result<RespValue> Apply(const ops::WriteOp& op, SequenceId seq) = 0;
  virtual Result<void> ApplyBatch(std::span<const ops::WriteOp> ops, SequenceId seq) = 0;

  // Existence probe distinguishing a recent-delete tombstone from a true miss.
  virtual HotKeyPresence Probe(std::string_view key) = 0;

  // Suppresses memory-pressure eviction while a consumer replays the queue, so
  // the rebuilt hot view does not depend on memory timing (deterministic
  // replay, invariant 4). Default no-op for stores without a memory budget.
  virtual void SetReplayMode(bool /*replaying*/) {}

  virtual Result<MemoryStats> Stats() = 0;
  virtual Result<void> Wipe() = 0;
};

}  // namespace abyss::core
