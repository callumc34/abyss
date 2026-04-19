#include "abyss/consumer/hot_consumer.h"

#include <utility>
#include <variant>

#include "abyss/core/ops.h"

namespace abyss::consumer {

namespace {

core::RespValue MapApplyError(const core::Error& err) {
  switch (err.code()) {
    case core::ErrorCode::kWrongType:
      return core::RespValue::Error(core::ErrorPrefix::kWrongType, err.message());
    case core::ErrorCode::kResourceExhausted:
      return core::RespValue::Error(core::ErrorPrefix::kOom, err.message());
    default:
      return core::RespValue::Error(core::ErrorPrefix::kErr, err.message());
  }
}

}  // namespace

HotConsumer::HotConsumer(core::Queue& queue, core::HotStore& store, core::ConsumerRpc& rpc,
                         Config config, core::EvictionPolicy eviction_policy)
    : queue_(queue),
      store_(store),
      rpc_(rpc),
      config_(config),
      eviction_policy_(std::move(eviction_policy)) {}

HotConsumer::~HotConsumer() { Stop(); }

void HotConsumer::Start() {
  if (running_.exchange(true, std::memory_order_acq_rel)) return;
  stop_requested_.store(false, std::memory_order_release);
  thread_ = std::thread(&HotConsumer::Run, this);
}

void HotConsumer::Stop() {
  if (!running_.load(std::memory_order_acquire)) return;
  stop_requested_.store(true, std::memory_order_release);
  if (thread_.joinable()) thread_.join();
  running_.store(false, std::memory_order_release);
}

void HotConsumer::Run() {
  while (!stop_requested_.load(std::memory_order_acquire)) {
    auto read = queue_.Read(core::kHotConsumer, config_.shard, config_.read_batch_size,
                            config_.read_timeout);
    if (!read.has_value()) {
      if (read.error().code() == core::ErrorCode::kUnavailable) return;
      continue;
    }
    for (auto& entry : *read) {
      ProcessEntry(entry);
      // Ack is high-water-mark: a later entry's Ack supersedes this one, so
      // swallowing a single-entry Ack failure is safe. The write itself is
      // already durable.
      [[maybe_unused]] auto ack = queue_.Ack(core::kHotConsumer, config_.shard, entry.seq);
    }
  }
}

void HotConsumer::ProcessEntry(core::QueueEntry& entry) {
  const auto seq = entry.seq;
  core::RespValue result = std::visit(
      [this, seq]<typename T>(T& payload) -> core::RespValue {
        if constexpr (std::is_same_v<T, core::entry::Write>) {
          return ApplyWriteEntry(payload.cmd);
        } else if constexpr (std::is_same_v<T, core::entry::Conditional>) {
          (void)seq;
          return core::RespValue::Error(
              core::ErrorPrefix::kErr,
              "conditional writes require resolver; not yet wired in this deployment");
        } else if constexpr (std::is_same_v<T, core::entry::Resolved>) {
          (void)seq;
          // Resolved entries are applied via block-and-scan once the Resolver
          // ships; standalone application is a no-op in this deployment.
          if (payload.decision == core::Decision::kApply && payload.materialised_op.has_value()) {
            return ApplyWriteEntry(*payload.materialised_op);
          }
          return payload.return_value;
        } else {
          static_assert(sizeof(T) == 0, "unhandled QueueEntry payload variant");
        }
      },
      entry.payload);

  (void)rpc_.Fulfill(seq, std::move(result));
}

core::RespValue HotConsumer::ApplyWriteEntry(const core::RespCommand& cmd) {
  if (cmd.args.empty()) {
    return core::RespValue::Error(core::ErrorPrefix::kErr, "empty command payload in queue entry");
  }
  auto op = core::ops::ParseWriteOp(cmd.args[0], cmd);
  if (!op.has_value()) {
    return core::RespValue::Error(core::ErrorPrefix::kErr, op.error().message());
  }
  const auto eviction = eviction_policy_.Resolve(core::ops::PrimaryKey(*op));
  auto applied = store_.Apply(*op, eviction);
  if (!applied.has_value()) {
    return MapApplyError(applied.error());
  }
  return core::RespValue::SimpleString("OK");
}

}  // namespace abyss::consumer
