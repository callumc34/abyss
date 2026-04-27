#include "abyss/consumer/hot_consumer.h"

#include <chrono>
#include <utility>
#include <variant>

#include "abyss/core/consumer_rpc.h"
#include "abyss/core/fire_and_forget.h"
#include "abyss/core/ops.h"
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
                         core::ApplyNotifier& apply_notifier, Config config,
                         core::EvictionPolicy eviction_policy)
    : queue_(queue),
      store_(store),
      rpc_(rpc),
      apply_notifier_(apply_notifier),
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

size_t HotConsumer::PendingConditionalCount() const {
  const std::scoped_lock lock(pending_mu_);
  return pending_conditionals_.size();
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
      CheckBlockAndScanTimeout();
      continue;
    }

    for (auto& entry : *read) {
      std::visit(
          [this, &entry](auto& payload) {
            using T = std::decay_t<decltype(payload)>;
            if constexpr (std::is_same_v<T, core::entry::Write>) {
              HandleWrite(entry, payload);
            } else if constexpr (std::is_same_v<T, core::entry::Conditional>) {
              HandleConditional(std::move(entry), payload);
            } else if constexpr (std::is_same_v<T, core::entry::Resolved>) {
              HandleResolved(entry, payload);
            }
          },
          entry.payload);
    }
    CheckBlockAndScanTimeout();
  }

  ABYSS_LOG_DEBUG(Log(), "hot consumer stopped", {"shard", static_cast<int64_t>(config_.shard)});
}

void HotConsumer::HandleWrite(const core::QueueEntry& entry, const core::entry::Write& write) {
  const core::RespCommand& cmd = write.cmd;
  core::RespValue result;
  if (cmd.args.empty()) {
    counters_.parse_failures.fetch_add(1, std::memory_order_relaxed);
    ABYSS_LOG_ERROR(Log(), "queue entry has empty command payload",
                    {"shard", static_cast<int64_t>(config_.shard)});
    result =
        core::RespValue::Error(core::ErrorPrefix::kErr, "empty command payload in queue entry");
  } else {
    auto op = core::ops::ParseWriteOp(cmd.args[0], cmd);
    if (!op.has_value()) {
      counters_.parse_failures.fetch_add(1, std::memory_order_relaxed);
      ABYSS_LOG_ERROR(
          Log(), "queue entry parse failed", {"shard", static_cast<int64_t>(config_.shard)},
          {"cmd", std::string_view{cmd.args[0]}}, {"err", std::string_view{op.error().message()}});
      result = core::RespValue::Error(core::ErrorPrefix::kErr, op.error().message());
    } else {
      const auto eviction = eviction_policy_.Resolve(core::ops::PrimaryKey(*op));
      auto applied = store_.Apply(*op, eviction);
      if (!applied.has_value()) {
        counters_.apply_failures.fetch_add(1, std::memory_order_relaxed);
        if (applied.error().code() != core::ErrorCode::kWrongType) {
          ABYSS_LOG_ERROR(Log(), "hot apply failed", {"shard", static_cast<int64_t>(config_.shard)},
                          {"cmd", std::string_view{cmd.args[0]}},
                          {"err", std::string_view{applied.error().message()}});
        }
        result = MapApplyError(applied.error());
      } else {
        counters_.applied.fetch_add(1, std::memory_order_relaxed);
        result = std::move(*applied);
      }
    }
  }

  const core::RpcId rpc_id = core::MakeRpcId(config_.shard, entry.seq);
  (void)rpc_.Fulfill(rpc_id, std::move(result));
  apply_notifier_.NotifyApplied(rpc_id);
  MarkSettledAndMaybeAck(entry.seq);
}

// Block-and-scan: hold the Conditional, wait for the Resolved to apply.
// The resolver fulfils the client RPC, not hot.
void HotConsumer::HandleConditional(core::QueueEntry entry,
                                    const core::entry::Conditional& /*cond*/) {
  const auto seq = entry.seq;
  {
    const std::scoped_lock lock(pending_mu_);
    pending_conditionals_.emplace(
        seq, PendingConditional{.entry = std::move(entry),
                                .received_at = std::chrono::steady_clock::now()});
  }
  apply_notifier_.NotifyApplied(core::MakeRpcId(config_.shard, seq));
}

void HotConsumer::HandleResolved(const core::QueueEntry& entry,
                                 const core::entry::Resolved& resolved) {
  bool had_pending = false;
  {
    const std::scoped_lock lock(pending_mu_);
    had_pending = pending_conditionals_.erase(resolved.ref) > 0;
  }

  if (resolved.decision == core::Decision::kApply) {
    auto applied = ApplyOps(resolved.materialised_ops);
    if (!applied.has_value()) {
      counters_.apply_failures.fetch_add(1, std::memory_order_relaxed);
      if (applied.error().code() != core::ErrorCode::kWrongType) {
        ABYSS_LOG_ERROR(Log(), "hot resolved apply failed",
                        {"shard", static_cast<int64_t>(config_.shard)},
                        {"ref", static_cast<uint64_t>(resolved.ref)},
                        {"err", std::string_view{applied.error().message()}});
      }
    } else {
      counters_.applied.fetch_add(static_cast<uint64_t>(resolved.materialised_ops.size()),
                                  std::memory_order_relaxed);
    }
  }

  // The resolver awaits NotifyApplied on the conditional's RpcId before
  // fulfilling the client RPC. The Resolved entry's own RpcId has no
  // resolver-side waiter today, but we notify for symmetry with future RPCs.
  apply_notifier_.NotifyApplied(core::MakeRpcId(config_.shard, resolved.ref));
  apply_notifier_.NotifyApplied(core::MakeRpcId(config_.shard, entry.seq));

  if (had_pending) MarkSettledAndMaybeAck(resolved.ref);
  MarkSettledAndMaybeAck(entry.seq);
}

core::Result<void> HotConsumer::ApplyOps(const std::vector<core::RespCommand>& ops) {
  for (const auto& cmd : ops) {
    if (cmd.args.empty()) {
      counters_.parse_failures.fetch_add(1, std::memory_order_relaxed);
      return std::unexpected(
          core::Error(core::ErrorCode::kInvalidArgument, "empty materialised op"));
    }
    auto op = core::ops::ParseWriteOp(cmd.args[0], cmd);
    if (!op.has_value()) {
      counters_.parse_failures.fetch_add(1, std::memory_order_relaxed);
      return std::unexpected(op.error());
    }
    const auto eviction = eviction_policy_.Resolve(core::ops::PrimaryKey(*op));
    // Reply value is constructed by the Resolver; discard here.
    auto applied = store_.Apply(*op, eviction);
    if (!applied.has_value()) return std::unexpected(applied.error());
  }
  return {};
}

void HotConsumer::MarkSettledAndMaybeAck(core::SequenceId seq) {
  auto prev = highest_settled_seq_.load(std::memory_order_acquire);
  while (seq > prev) {
    if (highest_settled_seq_.compare_exchange_weak(prev, seq, std::memory_order_acq_rel)) {
      break;
    }
  }

  // Clamp ack behind any pending Conditional so its Resolved isn't acked-past.
  core::SequenceId target = seq;
  std::optional<core::SequenceId> oldest_pending;
  {
    const std::scoped_lock lock(pending_mu_);
    for (const auto& [pseq, _] : pending_conditionals_) {
      if (!oldest_pending.has_value() || pseq < *oldest_pending) oldest_pending = pseq;
    }
  }
  if (oldest_pending.has_value() && *oldest_pending > 0 && *oldest_pending - 1 < target) {
    target = *oldest_pending - 1;
  }

  core::FireAndForget(queue_.Ack(core::kHotConsumer, config_.shard, target),
                      counters_.ack_failures);
}

void HotConsumer::CheckBlockAndScanTimeout() {
  std::chrono::steady_clock::time_point oldest{};
  bool have_pending = false;
  {
    const std::scoped_lock lock(pending_mu_);
    for (const auto& [_, pending] : pending_conditionals_) {
      if (!have_pending || pending.received_at < oldest) {
        oldest = pending.received_at;
        have_pending = true;
      }
    }
  }
  if (!have_pending) {
    block_and_scan_warning_emitted_ = false;
    return;
  }
  const auto age = std::chrono::steady_clock::now() - oldest;
  if (age >= config_.block_and_scan_timeout && !block_and_scan_warning_emitted_) {
    ABYSS_LOG_WARN(
        Log(), "hot consumer block-and-scan timeout; resolver may be stuck",
        {"shard", static_cast<int64_t>(config_.shard)},
        {"oldest_age_ms",
         static_cast<int64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(age).count())});
    block_and_scan_warning_emitted_ = true;
    counters_.block_and_scan_timeouts.fetch_add(1, std::memory_order_relaxed);
  }
}

}  // namespace abyss::consumer
