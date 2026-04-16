#include "abyss/core/consumer_rpc.h"

#include <utility>

namespace abyss::core {

std::future<RespValue> ConsumerRpc::Register(RpcId id) {
  std::lock_guard<std::mutex> lock(mu_);
  auto [it, inserted] = pending_.try_emplace(id);
  if (!inserted) {
    std::promise<RespValue> broken;
    auto fut = broken.get_future();

    return fut;
  }
  return it->second.get_future();
}

bool ConsumerRpc::Fulfill(RpcId id, RespValue value) {
  std::promise<RespValue> promise;
  {
    std::lock_guard<std::mutex> lock(mu_);
    auto it = pending_.find(id);
    if (it == pending_.end()) {
      return false;
    }
    promise = std::move(it->second);
    pending_.erase(it);
  }
  promise.set_value(std::move(value));
  return true;
}

bool ConsumerRpc::Cancel(RpcId id) {
  std::promise<RespValue> promise;
  {
    std::lock_guard<std::mutex> lock(mu_);
    auto it = pending_.find(id);
    if (it == pending_.end()) {
      return false;
    }
    promise = std::move(it->second);
    pending_.erase(it);
  }
  // promise destructs here without set_value to broken_promise on the future.
  return true;
}

size_t ConsumerRpc::PendingCount() const {
  std::lock_guard<std::mutex> lock(mu_);
  return pending_.size();
}

}  // namespace abyss::core
