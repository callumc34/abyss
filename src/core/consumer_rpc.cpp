#include "abyss/core/consumer_rpc.h"

#include <algorithm>
#include <utility>

namespace abyss::core {

namespace {

uint32_t ClampShardCount(uint32_t requested) { return std::max<uint32_t>(requested, 1U); }

}  // namespace

ConsumerRpc::ConsumerRpc(ConsumerRpcConfig config) : default_timeout_(config.default_timeout) {
  const auto count = ClampShardCount(config.registry_shard_count);
  shards_.reserve(count);
  for (uint32_t i = 0; i < count; ++i) {
    shards_.push_back(std::make_unique<Shard>());
  }
}

ConsumerRpc::Shard& ConsumerRpc::ShardFor(RpcId id) const { return *shards_[id % shards_.size()]; }

std::future<RespValue> ConsumerRpc::Register(RpcId id) {
  auto& shard = ShardFor(id);
  const std::scoped_lock lock(shard.mu);
  auto [it, inserted] = shard.pending.try_emplace(id);
  if (!inserted) {
    // Duplicate registration. The original waiter is preserved; return a
    // broken future so the duplicate caller observes the misuse.
    std::promise<RespValue> broken;
    broken.set_exception(
        std::make_exception_ptr(std::future_error(std::future_errc::broken_promise)));
    return broken.get_future();
  }
  return it->second.get_future();
}

bool ConsumerRpc::Fulfill(RpcId id, RespValue value) {
  std::promise<RespValue> promise;
  {
    auto& shard = ShardFor(id);
    const std::scoped_lock lock(shard.mu);
    auto it = shard.pending.find(id);
    if (it == shard.pending.end()) return false;
    promise = std::move(it->second);
    shard.pending.erase(it);
  }
  promise.set_value(std::move(value));
  return true;
}

bool ConsumerRpc::Cancel(RpcId id) {
  std::promise<RespValue> promise;
  {
    auto& shard = ShardFor(id);
    const std::scoped_lock lock(shard.mu);
    auto it = shard.pending.find(id);
    if (it == shard.pending.end()) return false;
    promise = std::move(it->second);
    shard.pending.erase(it);
  }
  promise.set_exception(
      std::make_exception_ptr(std::future_error(std::future_errc::broken_promise)));
  return true;
}

size_t ConsumerRpc::PendingCount() const {
  size_t total = 0;
  for (const auto& shard : shards_) {
    const std::scoped_lock lock(shard->mu);
    total += shard->pending.size();
  }
  return total;
}

}  // namespace abyss::core
