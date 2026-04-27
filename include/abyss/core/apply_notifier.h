#pragma once

#include <cstddef>
#include <cstdint>
#include <future>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <vector>

#include "abyss/core/consumer_rpc.h"
#include "abyss/core/thread_annotations.h"
#include "abyss/core/types.h"

namespace abyss::core {

struct ApplyNotifierConfig {
  uint32_t registry_shard_count = 16;
};

// Hot signals "applied at id"; the resolver awaits before fulfilling the
// client RPC. Required for read-your-write on conditional writes. Keyed by
// RpcId (shard-packed seq) so per-shard sequences don't collide.
class ApplyNotifier {
 public:
  explicit ApplyNotifier(ApplyNotifierConfig config = {});
  ~ApplyNotifier() = default;

  ApplyNotifier(const ApplyNotifier&) = delete;
  ApplyNotifier& operator=(const ApplyNotifier&) = delete;
  ApplyNotifier(ApplyNotifier&&) = delete;
  ApplyNotifier& operator=(ApplyNotifier&&) = delete;

  // Late Await (after Notify) returns ready within a bounded recent window;
  // older ids return a broken future.
  std::future<void> AwaitApplied(RpcId id);
  void NotifyApplied(RpcId id);
  bool Cancel(RpcId id);

  size_t PendingCount() const;

 private:
  struct Shard {
    mutable std::mutex mu;
    std::unordered_map<RpcId, std::promise<void>> pending ABYSS_GUARDED_BY(mu);
    std::vector<RpcId> recent_fired ABYSS_GUARDED_BY(mu);
    size_t recent_cursor ABYSS_GUARDED_BY(mu) = 0;
  };

  Shard& ShardFor(RpcId id) const;

  std::vector<std::unique_ptr<Shard>> shards_;
  size_t late_window_size_per_shard_ = 64;
};

}  // namespace abyss::core
