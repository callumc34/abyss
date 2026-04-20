#include "abyss/consumer/hot_consumer.h"

#include <utility>
#include <variant>

#include "abyss/core/fire_and_forget.h"
#include "abyss/core/ops.h"
#include "abyss/core/queue_entry.h"
#include "abyss/log/log.h"

namespace abyss::consumer {

namespace {

const log::Logger& Log() {
  static const log::Logger l = log::Get("abyss.hot.consumer");
  return l;
}

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

void HotConsumer::RequestStop() { stop_requested_.store(true, std::memory_order_release); }

void HotConsumer::Join() {
  if (!running_.load(std::memory_order_acquire)) return;
  if (thread_.joinable()) thread_.join();
  running_.store(false, std::memory_order_release);
}

void HotConsumer::Stop() {
  RequestStop();
  Join();
}

void HotConsumer::Run() {
  ABYSS_LOG_DEBUG(Log(), "hot consumer started", {"shard", static_cast<int64_t>(config_.shard)});

  while (!stop_requested_.load(std::memory_order_acquire)) {
    auto read = queue_.Read(core::kHotConsumer, config_.shard, config_.read_batch_size,
                            config_.read_timeout);
    if (!read.has_value()) {
      if (read.error().code() == core::ErrorCode::kUnavailable) {
        ABYSS_LOG_WARN(Log(), "hot consumer stopping: queue unavailable",
                       {"shard", static_cast<int64_t>(config_.shard)});
        return;
      }
      counters_.queue_read_failures.fetch_add(1, std::memory_order_relaxed);
      continue;
    }
    for (auto& entry : *read) {
      ProcessEntry(entry);
      // Ack is high-water-mark: a later ack supersedes this one on drop.
      core::FireAndForget(queue_.Ack(core::kHotConsumer, config_.shard, entry.seq),
                          counters_.ack_failures);
    }
  }

  ABYSS_LOG_DEBUG(Log(), "hot consumer stopped", {"shard", static_cast<int64_t>(config_.shard)});
}

void HotConsumer::ProcessEntry(core::QueueEntry& entry) {
  const auto seq = entry.seq;
  auto extracted = core::entry::ExtractApplicableCommand(entry);
  core::RespValue result;
  if (extracted.has_value()) {
    result = ApplyWriteEntry(**extracted);
  } else if (extracted.error().code() == core::ErrorCode::kNotFound) {
    // Resolved-skip: no apply, use the pre-computed return_value from the resolver.
    result = std::get<core::entry::Resolved>(entry.payload).return_value;
  } else {
    counters_.apply_failures.fetch_add(1, std::memory_order_relaxed);
    result = core::RespValue::Error(core::ErrorPrefix::kErr, extracted.error().message());
  }

  (void)rpc_.Fulfill(seq, std::move(result));
}

core::RespValue HotConsumer::ApplyWriteEntry(const core::RespCommand& cmd) {
  if (cmd.args.empty()) {
    counters_.parse_failures.fetch_add(1, std::memory_order_relaxed);
    ABYSS_LOG_ERROR(Log(), "queue entry has empty command payload",
                    {"shard", static_cast<int64_t>(config_.shard)});
    return core::RespValue::Error(core::ErrorPrefix::kErr, "empty command payload in queue entry");
  }
  auto op = core::ops::ParseWriteOp(cmd.args[0], cmd);
  if (!op.has_value()) {
    counters_.parse_failures.fetch_add(1, std::memory_order_relaxed);
    ABYSS_LOG_ERROR(
        Log(), "queue entry parse failed", {"shard", static_cast<int64_t>(config_.shard)},
        {"cmd", std::string_view{cmd.args[0]}}, {"err", std::string_view{op.error().message()}});
    return core::RespValue::Error(core::ErrorPrefix::kErr, op.error().message());
  }
  const auto eviction = eviction_policy_.Resolve(core::ops::PrimaryKey(*op));
  auto applied = store_.Apply(*op, eviction);
  if (!applied.has_value()) {
    counters_.apply_failures.fetch_add(1, std::memory_order_relaxed);
    // WrongType is a legitimate user-visible outcome of replay; don't log.
    if (applied.error().code() != core::ErrorCode::kWrongType) {
      ABYSS_LOG_ERROR(Log(), "hot apply failed", {"shard", static_cast<int64_t>(config_.shard)},
                      {"cmd", std::string_view{cmd.args[0]}},
                      {"err", std::string_view{applied.error().message()}});
    }
    return MapApplyError(applied.error());
  }
  counters_.applied.fetch_add(1, std::memory_order_relaxed);
  return core::RespValue::SimpleString("OK");
}

}  // namespace abyss::consumer
