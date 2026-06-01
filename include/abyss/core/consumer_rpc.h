#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <future>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <vector>

#include "abyss/core/resp_types.h"
#include "abyss/core/thread_annotations.h"
#include "abyss/core/types.h"

namespace abyss::core {

using RpcId = uint64_t;

// Upper bound on hot.shard_count for in-memory RpcId packing: the shard field
// occupies bits [48,62] (15 bits), keeping bit 63 reserved for the flush tag.
// The cold on-disk envelope (1<<16) is wider; that is the on-disk slot width,
// not this in-memory packing limit. Validated centrally at config load.
inline constexpr uint32_t kRpcMaxShardCount = 1U << 15;
static_assert((kRpcMaxShardCount - 1) < (1U << 15));

// Per-shard seqs collide at the registry; pack shard in bits [48,62] and seq in
// bits [0,48). Bit 63 is forced clear so the write-RpcId space [0,2^63) stays
// disjoint from the flush-tag space [2^63,2^64) for every valid shard.
inline RpcId MakeRpcId(ShardId shard, SequenceId seq) {
  return ((static_cast<RpcId>(shard) & ((RpcId{1} << 15) - 1)) << 48) |
         (static_cast<RpcId>(seq) & ((RpcId{1} << 48) - 1));
}

// Flush RPC id: [tag=1][7 bits consumer][16 bits shard][40 bits seq]. The high
// bit segregates from `MakeRpcId` (which leaves it clear) so the registry can
// disambiguate per-consumer Flush fulfilments from per-write ones.
inline constexpr RpcId kFlushRpcTagBit = RpcId{1} << 63;
inline RpcId MakeFlushRpcId(ConsumerId consumer, ShardId shard, SequenceId seq) {
  return kFlushRpcTagBit | (static_cast<RpcId>(consumer & 0x7F) << 56) |
         (static_cast<RpcId>(shard & 0xFFFF) << 40) |
         (static_cast<RpcId>(seq) & ((RpcId{1} << 40) - 1));
}

struct ConsumerRpcConfig {
  // Striped across this many mutex-protected partitions; `id % count` selects.
  uint32_t registry_shard_count = 16;
  std::chrono::milliseconds default_timeout{5000};
};

// Single-use RPC ids thread a response from a consumer back to the producer
// that originated the work. Timeout is a caller concern — the registry does
// not own a timer. Callers await via wait_for and Cancel on timeout.
class ConsumerRpc {
 public:
  explicit ConsumerRpc(ConsumerRpcConfig config = {});
  ~ConsumerRpc() = default;

  ConsumerRpc(const ConsumerRpc&) = delete;
  ConsumerRpc& operator=(const ConsumerRpc&) = delete;
  ConsumerRpc(ConsumerRpc&&) = delete;
  ConsumerRpc& operator=(ConsumerRpc&&) = delete;

  // Duplicate Register returns a broken future; the original waiter survives.
  std::future<RespValue> Register(RpcId id);
  bool Fulfill(RpcId id, RespValue value);
  // Delivers broken_promise explicitly (not via destruction).
  bool Cancel(RpcId id);

  size_t PendingCount() const;

  uint32_t registry_shard_count() const { return static_cast<uint32_t>(shards_.size()); }
  std::chrono::milliseconds default_timeout() const { return default_timeout_; }

 private:
  struct Shard {
    mutable std::mutex mu;
    std::unordered_map<RpcId, std::promise<RespValue>> pending ABYSS_GUARDED_BY(mu);
  };

  Shard& ShardFor(RpcId id) const;

  // unique_ptr so std::mutex isn't required to be movable in the vector.
  std::vector<std::unique_ptr<Shard>> shards_;
  std::chrono::milliseconds default_timeout_;
};

}  // namespace abyss::core
