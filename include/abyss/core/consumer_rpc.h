#pragma once

#include <cstdint>
#include <future>
#include <mutex>
#include <unordered_map>

#include "abyss/core/resp_types.h"
#include "abyss/core/thread_annotations.h"

namespace abyss::core {

using RpcId = uint64_t;

// Generalised in-process promise registry.
class ConsumerRpc {
 public:
  ConsumerRpc() = default;
  ~ConsumerRpc() = default;

  ConsumerRpc(const ConsumerRpc&) = delete;
  ConsumerRpc& operator=(const ConsumerRpc&) = delete;
  ConsumerRpc(ConsumerRpc&&) = delete;
  ConsumerRpc& operator=(ConsumerRpc&&) = delete;

  // Register a pending RPC.
  std::future<RespValue> Register(RpcId id);

  // Fulfil a pending RPC.
  // Returns true if the id was pending and fulfilled.
  // Returbs false if no such id was registered.
  bool Fulfill(RpcId id, RespValue value);

  // Cancel a pending RPC.
  bool Cancel(RpcId id);

  size_t PendingCount() const;

 private:
  mutable std::mutex mu_;
  std::unordered_map<RpcId, std::promise<RespValue>> pending_ ABYSS_GUARDED_BY(mu_);
};

}  // namespace abyss::core
