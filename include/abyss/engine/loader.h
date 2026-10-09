#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>

#include "abyss/consumer/compacted_state.h"
#include "abyss/consumer/compaction_buffer_router.h"
#include "abyss/core/cold_store.h"
#include "abyss/core/result.h"
#include "abyss/core/types.h"
#include "abyss/engine/decide.h"
#include "abyss/hot/sharded_hot_store.h"
#include "abyss/hot/single_shard_store.h"

namespace abyss::engine {

// Reads a non-resident key's state from its buffered delta, then cold.
//
// That order is never stale: (1) a non-resident key's writes were all
// drained into the buffer before it was evicted; (2) while it loads,
// non-blind writes wait and a blind write discards the load, so the
// buffer absorbs no new delta for it; (3) the merge is idempotent
// (adds, removes, overwrites, a DEL-all base invalidation), so a flush
// moving the delta into cold mid-load cannot change the result.
class Loader {
 public:
  Loader(hot::ShardedHotStore& hot, const consumer::CompactionBufferRouter& buffers,
         core::ColdStore& cold, core::WallClockFn wall_clock = core::DefaultWallClock);

  // Lock-free, and touches no hot state. kExistence reads no members:
  // the delta answers, or cold's probe does. Only when the delta's
  // removals may have emptied the key, or hot keeps no stubs to hold
  // the answer, does it load in full. An expired TTL is returned as
  // it is, for decide to log. A cold error or timeout is returned.
  core::Result<hot::LoadResult> Load(core::ShardId shard, std::string_view key, Need need,
                                     core::SteadyTime deadline) const;

  enum class Fill : uint8_t {
    // This call's load is in hot.
    kInstalled,
    // Hot holds the key, perhaps by another call's load. A miss on
    // re-reading means it was since evicted, drained: buffer and cold
    // then answer.
    kResident,
    // The flush floor makes the key absent.
    kFlushed,
    // A write overtook the load; `result` holds what was read.
    kDiscarded,
  };
  struct Filled {
    Fill fill = Fill::kInstalled;
    std::optional<hot::LoadResult> result;
  };
  // The read path's cache fill: begin, load in full and complete, each
  // in its own hold. A load already in flight is awaited, not
  // repeated, so concurrent misses share one cold read.
  core::Result<Filled> Install(std::string_view key, core::SteadyTime deadline);

  // Point reads from the delta, else one cold point read: no full load
  // and no wait on a placeholder. Absent when the key is, or is past
  // its TTL; kWrongType when it holds another type. Cardinality
  // (SCARD, ZCARD, HLEN) needs the whole key: Install, then read hot.
  core::Result<bool> IsMember(core::ShardId shard, std::string_view key, std::string_view member,
                              core::SteadyTime deadline) const;
  core::Result<std::optional<double>> Score(core::ShardId shard, std::string_view key,
                                            std::string_view member,
                                            core::SteadyTime deadline) const;
  core::Result<std::optional<std::string>> HashField(core::ShardId shard, std::string_view key,
                                                     std::string_view field,
                                                     core::SteadyTime deadline) const;

 private:
  core::Result<std::optional<core::MemberValue>> Member(core::ShardId shard, std::string_view key,
                                                        core::KeyType type, std::string_view member,
                                                        core::SteadyTime deadline) const;

  hot::ShardedHotStore& hot_;
  const consumer::CompactionBufferRouter& buffers_;
  core::ColdStore& cold_;
  core::WallClockFn wall_clock_;
};

// The key as cold will hold it once `delta` (null: none) is flushed
// over `base`; the merge Load performs.
hot::LoadResult MergeLoad(std::optional<core::ColdKeyState> base,
                          const consumer::CompactedState* delta);

}  // namespace abyss::engine
