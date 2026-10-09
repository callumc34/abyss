#pragma once

#include <atomic>
#include <cstdint>
#include <future>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <tuple>
#include <utility>
#include <vector>

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
  // Where a read found its answer.
  enum class Source : uint8_t { kBuffer, kCold };

  using Shared = std::shared_ptr<const hot::LoadResult>;
  // Load, reading members only when the key holds `type`: a key of
  // another type is LoadedExists, unless removals may have emptied it.
  // Concurrent calls for one key and type share one load, and its
  // result or error; once it completes, the next call loads afresh.
  core::Result<Shared> LoadAs(core::ShardId shard, std::string_view key, core::KeyType type,
                              core::SteadyTime deadline, Source* source = nullptr) const;
  // The key's type and TTL, reading no members: LoadedAbsent or
  // LoadedExists, or in full when removals may have emptied it.
  core::Result<hot::LoadResult> Probe(core::ShardId shard, std::string_view key,
                                      core::SteadyTime deadline, Source* source = nullptr) const;

  enum class Fill : uint8_t {
    // This call's load is in hot.
    kInstalled,
    // Hot holds the key, perhaps by another call's load. A miss on
    // re-reading means it was since evicted, drained: buffer and cold
    // then answer.
    kResident,
    // The flush floor makes the key absent.
    kFlushed,
    // Not installed, and `result` holds what was read: a write overtook
    // the load, the shard is over its backpressure limit, the key is too
    // large to fill, or one hold's eviction left no room for it
    // (ShardedHotStore::Fill).
    kDiscarded,
    kSkippedBackpressure,
    kSkippedSize,
    kSkippedEvictCap,
  };
  struct Filled {
    Fill fill = Fill::kInstalled;
    // What was read, unless it is in hot in full.
    std::optional<hot::LoadResult> result;
    Source source = Source::kBuffer;
  };
  // The read path's cache fill: begin, load as `type` and fill, each in
  // its own hold. A load already in flight is awaited, not repeated, so
  // concurrent fills share one cold read.
  core::Result<Filled> Install(std::string_view key, core::KeyType type, core::SteadyTime deadline);

  struct Members {
    // Each asked-for member's value; nullopt when absent.
    std::vector<std::optional<core::MemberValue>> values;
    // Cold's count plus the delta's adds: a bound only, for the fill
    // threshold. 0 when absent.
    uint64_t cardinality = 0;
    Source source = Source::kBuffer;
  };
  // Point reads from the delta, else one cold batch: no full load and
  // no wait on a placeholder. All absent when the key is, or is past
  // its TTL; kWrongType when it holds another type.
  core::Result<Members> ReadMembers(core::ShardId shard, std::string_view key, core::KeyType type,
                                    std::span<const std::string_view> members,
                                    core::SteadyTime deadline) const;
  struct Count {
    uint64_t members = 0;
    Source source = Source::kBuffer;
  };
  // SCARD, ZCARD and HLEN, exact without a full load: cold's count,
  // corrected by which of the delta's members cold holds, read in one
  // batch bounded by the delta. 0 when absent or past its TTL;
  // kWrongType when it holds another type.
  core::Result<Count> Cardinality(core::ShardId shard, std::string_view key, core::KeyType type,
                                  core::SteadyTime deadline) const;
  core::Result<bool> IsMember(core::ShardId shard, std::string_view key, std::string_view member,
                              core::SteadyTime deadline) const;
  core::Result<std::optional<double>> Score(core::ShardId shard, std::string_view key,
                                            std::string_view member,
                                            core::SteadyTime deadline) const;
  core::Result<std::optional<std::string>> HashField(core::ShardId shard, std::string_view key,
                                                     std::string_view field,
                                                     core::SteadyTime deadline) const;

  // Calls that waited on another's load, in flight or as a placeholder.
  uint64_t JoinsForTesting() const { return joins_.load(); }

 private:
  // LoadAs, unshared.
  core::Result<hot::LoadResult> LoadTyped(core::ShardId shard, std::string_view key,
                                          core::KeyType type, core::SteadyTime deadline,
                                          Source* source) const;
  struct Typed {
    // After the delta; nullopt when absent or past its TTL.
    std::optional<core::KeyMeta> meta;
    // Cold's, when it was read.
    std::optional<core::KeyMeta> base;
    Source source = Source::kBuffer;
  };
  // The key's meta as `type`, reading no members; kWrongType when it
  // holds another.
  core::Result<Typed> MetaAs(core::ShardId shard, std::string_view key, core::KeyType type,
                             const consumer::CompactedState* changes,
                             core::SteadyTime deadline) const;
  core::Result<std::optional<core::MemberValue>> Member(core::ShardId shard, std::string_view key,
                                                        core::KeyType type, std::string_view member,
                                                        core::SteadyTime deadline) const;

  // LoadAs, as the leader of a load in flight or a waiter on one. A
  // leader passing `owned` gets its result there, and lends waiters a
  // copy only when there are some.
  core::Result<Shared> Fly(core::ShardId shard, std::string_view key, core::KeyType type,
                           core::SteadyTime deadline, Source* source,
                           std::optional<hot::LoadResult>* owned) const;

  struct Flight {
    core::Result<Shared> result;
    Source source = Source::kBuffer;
  };
  struct Pending {
    std::shared_future<Flight> done;
    size_t waiters = 0;
  };
  // Loads in flight, per shard: LoadAs's and Install's.
  struct Flights {
    std::mutex mu;
    // By key, type and the drain horizon the leader started at.
    std::map<std::tuple<std::string, core::KeyType, core::SequenceId>, Pending> loading;
  };

  hot::ShardedHotStore& hot_;
  const consumer::CompactionBufferRouter& buffers_;
  core::ColdStore& cold_;
  core::WallClockFn wall_clock_;
  std::vector<std::unique_ptr<Flights>> flights_;
  mutable std::atomic<uint64_t> joins_{0};
};

// The key as cold will hold it once `delta` (null: none) is flushed
// over `base`; the merge Load performs.
hot::LoadResult MergeLoad(std::optional<core::ColdKeyState> base,
                          const consumer::CompactedState* delta);

}  // namespace abyss::engine
