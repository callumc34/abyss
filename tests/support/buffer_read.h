#pragma once

#include <chrono>
#include <cstdint>
#include <string>
#include <string_view>

#include "abyss/consumer/compacted_state.h"
#include "abyss/consumer/compaction_buffer.h"
#include "abyss/core/resp_types.h"
#include "abyss/core/result.h"
#include "abyss/core/types.h"

namespace abyss::testing {

// A string key's buffered delta as a GET reads it at `now_ms`: kNotFound
// when the buffer holds nothing, or no string, for it; nil for a
// buffered delete or a lapsed TTL.
inline core::Result<core::RespValue> BufferRead(const consumer::CompactionBuffer& buffer,
                                                std::string_view key, uint64_t now_ms) {
  const auto state = buffer.Snapshot(key);
  if (!state.has_value()) {
    return std::unexpected(core::Error(core::ErrorCode::kNotFound, "buffer has no state for key"));
  }
  const uint64_t ttl = state->AbsTtlMs();
  if (state->IsTombstone() || (ttl > 0 && ttl <= now_ms)) return core::RespValue::Null();
  if (state->Type() != consumer::CompactedState::DataType::kString) {
    return std::unexpected(core::Error(core::ErrorCode::kNotFound, "buffer key is not a string"));
  }
  return core::RespValue::BulkString(state->StringValue());
}

// BufferRead at the wall clock's now.
inline core::Result<core::RespValue> BufferRead(const consumer::CompactionBuffer& buffer,
                                                std::string_view key) {
  return BufferRead(buffer, key,
                    static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(
                                              core::WallClock::now().time_since_epoch())
                                              .count()));
}

}  // namespace abyss::testing
