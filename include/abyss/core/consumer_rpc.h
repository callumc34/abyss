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

namespace abyss::core {

using RpcId = uint64_t;

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
