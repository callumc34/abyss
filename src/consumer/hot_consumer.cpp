#include "abyss/consumer/hot_consumer.h"

#include <chrono>
#include <string>
#include <string_view>
#include <utility>
#include <variant>

#include "abyss/core/consumer_rpc.h"
#include "abyss/core/fire_and_forget.h"
#include "abyss/core/ops.h"
#include "abyss/log/log.h"

ABYSS_LOG_COMPONENT("abyss.hot.consumer")

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

uint64_t WallMs(core::WallTime t) {
  return static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::milliseconds>(t.time_since_epoch()).count());
}

// Returns the absolute-TTL of `op` in ms-since-epoch. 0 means "no TTL".
// Persist explicitly clears TTL so it never carries one to skip on.
uint64_t AbsTtlMs(const core::ops::WriteOp& op) {
  return std::visit(
      [](const auto& typed) -> uint64_t {
        using T = std::decay_t<decltype(typed)>;
        if constexpr (std::is_same_v<T, core::ops::StringSet>) {
          return typed.abs_ttl_ms;
        } else if constexpr (std::is_same_v<T, core::ops::Expire>) {
          return typed.abs_ttl_ms;
        } else {
          return 0;
        }
      },
      op);
}

}  // namespace

HotConsumer::HotConsumer(core::Queue& queue, core::HotStore& store, core::ConsumerRpc& rpc,
                         core::ApplyNotifier& apply_notifier, Config config,
                         const core::EvictionPolicy& eviction_policy)
    : queue_(queue),
      store_(store),
      rpc_(rpc),
      apply_notifier_(apply_notifier),
      config_(std::move(config)),
      eviction_policy_(eviction_policy) {}

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
  ABYSS_LOG_DEBUG("hot consumer started", {"shard", static_cast<int64_t>(config_.shard)});

  while (!stop_requested_.load(std::memory_order_acquire)) {
    auto read = queue_.Read(core::kHotConsumer, config_.shard, config_.read_batch_size,
                            config_.read_timeout);
    if (!read.has_value()) {
      if (read.error().code() == core::ErrorCode::kUnavailable) {
        ABYSS_LOG_WARN("hot consumer stopping: queue unavailable",
                       {"shard", static_cast<int64_t>(config_.shard)});
        return;
      }
      counters_.queue_read_failures.fetch_add(1, std::memory_order_relaxed);
      CheckBlockAndScanTimeout();
      continue;
    }

    ProcessBatch(*read);
    CheckBlockAndScanTimeout();
  }

  ABYSS_LOG_DEBUG("hot consumer stopped", {"shard", static_cast<int64_t>(config_.shard)});
}

void HotConsumer::ProcessBatch(std::vector<core::QueueEntry>& batch) {
  for (auto& entry : batch) {
    std::visit(
        [this, &entry](auto& payload) {
          using T = std::decay_t<decltype(payload)>;
          if constexpr (std::is_same_v<T, core::entry::Write>) {
            HandleWrite(entry, payload);
          } else if constexpr (std::is_same_v<T, core::entry::Conditional>) {
            HandleConditional(std::move(entry), payload);
          } else if constexpr (std::is_same_v<T, core::entry::Resolved>) {
            HandleResolved(entry, payload);
          } else if constexpr (std::is_same_v<T, core::entry::Flush>) {
            HandleFlush(entry);
          }
        },
        entry.payload);
  }
}

core::Result<void> HotConsumer::ReplayUntil(core::SequenceId target,
                                            const std::atomic<bool>& cancel) {
  ABYSS_LOG_INFO("hot replay starting", {"shard", static_cast<int64_t>(config_.shard)},
                 {"target_seq", static_cast<uint64_t>(target)});

  replay_mode_.store(true, std::memory_order_release);
  struct ReplayGuard {
    std::atomic<bool>& flag;
    explicit ReplayGuard(std::atomic<bool>& f) : flag(f) {}
    ReplayGuard(const ReplayGuard&) = delete;
    ReplayGuard& operator=(const ReplayGuard&) = delete;
    ReplayGuard(ReplayGuard&&) = delete;
    ReplayGuard& operator=(ReplayGuard&&) = delete;
    ~ReplayGuard() { flag.store(false, std::memory_order_release); }
  };
  ReplayGuard guard(replay_mode_);

  // The progress signal is "highest_settled_seq has reached target", but
  // both start at 0. To distinguish "target=0 means one entry at seq=0 to
  // process" from "target=0 means queue empty", read once before checking.
  // An empty read with progress reached → caught up; empty read with progress
  // behind → genuinely waiting on the queue, retry.
  while (!cancel.load(std::memory_order_acquire)) {
    auto read = queue_.Read(core::kHotConsumer, config_.shard, config_.replay_batch_size,
                            config_.read_timeout);
    if (!read.has_value()) {
      if (read.error().code() == core::ErrorCode::kUnavailable) {
        return std::unexpected(read.error());
      }
      counters_.queue_read_failures.fetch_add(1, std::memory_order_relaxed);
      continue;
    }

    if (read->empty()) {
      if (highest_settled_seq_.load(std::memory_order_acquire) >= target) break;
      CheckBlockAndScanTimeout();
      continue;
    }

    ProcessBatch(*read);
    CheckBlockAndScanTimeout();
    if (highest_settled_seq_.load(std::memory_order_acquire) >= target) break;
  }

  if (cancel.load(std::memory_order_acquire) &&
      highest_settled_seq_.load(std::memory_order_acquire) < target) {
    ABYSS_LOG_WARN("hot replay cancelled before reaching target",
                   {"shard", static_cast<int64_t>(config_.shard)},
                   {"target_seq", static_cast<uint64_t>(target)},
                   {"highest_settled",
                    static_cast<uint64_t>(highest_settled_seq_.load(std::memory_order_acquire))});
    return std::unexpected(core::Error{core::ErrorCode::kUnavailable, "hot replay cancelled"});
  }

  ABYSS_LOG_INFO("hot replay complete", {"shard", static_cast<int64_t>(config_.shard)},
                 {"highest_settled",
                  static_cast<uint64_t>(highest_settled_seq_.load(std::memory_order_acquire))});
  return {};
}

void HotConsumer::HandleWrite(const core::QueueEntry& entry, const core::entry::Write& write) {
  const core::RespCommand& cmd = write.cmd;
  core::RespValue result;
  if (cmd.args.empty()) {
    counters_.parse_failures.fetch_add(1, std::memory_order_relaxed);
    ABYSS_LOG_ERROR("queue entry has empty command payload",
                    {"shard", static_cast<int64_t>(config_.shard)});
    result =
        core::RespValue::Error(core::ErrorPrefix::kErr, "empty command payload in queue entry");
  } else {
    auto op = core::ops::ParseWriteOp(cmd.args[0], cmd, WallMs(entry.appended_at));
    if (!op.has_value()) {
      counters_.parse_failures.fetch_add(1, std::memory_order_relaxed);
      ABYSS_LOG_ERROR("queue entry parse failed", {"shard", static_cast<int64_t>(config_.shard)},
                      {"cmd", std::string_view{cmd.args[0]}},
                      {"err", std::string_view{op.error().message()}});
      result = core::RespValue::Error(core::ErrorPrefix::kErr, op.error().message());
    } else {
      const auto key = core::ops::PrimaryKey(*op);
      const bool replaying = replay_mode_.load(std::memory_order_acquire);
      const auto wall_now = config_.wall_clock();

      if (replaying && ShouldSkipForEvictionElapsed(entry.appended_at, key, wall_now)) {
        counters_.replay_skipped_eviction.fetch_add(1, std::memory_order_relaxed);
      } else if (replaying && ShouldSkipForAbsTtlElapsed(AbsTtlMs(*op), wall_now)) {
        counters_.replay_skipped_abs_ttl.fetch_add(1, std::memory_order_relaxed);
      } else {
        auto applied = store_.Apply(*op);
        if (!applied.has_value()) {
          counters_.apply_failures.fetch_add(1, std::memory_order_relaxed);
          if (applied.error().code() != core::ErrorCode::kWrongType) {
            ABYSS_LOG_ERROR("hot apply failed", {"shard", static_cast<int64_t>(config_.shard)},
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

void HotConsumer::HandleFlush(const core::QueueEntry& entry) {
  ABYSS_LOG_DEBUG("hot HandleFlush", {"shard", static_cast<int64_t>(config_.shard)},
                  {"seq", static_cast<uint64_t>(entry.seq)});
  // Cancel pre-Flush pending Conditionals; the awaiting client sees a broken_promise.
  std::vector<core::SequenceId> cancelled;
  {
    const std::scoped_lock lock(pending_mu_);
    cancelled.reserve(pending_conditionals_.size());
    for (auto it = pending_conditionals_.begin(); it != pending_conditionals_.end();) {
      if (it->first < entry.seq) {
        cancelled.push_back(it->first);
        it = pending_conditionals_.erase(it);
      } else {
        ++it;
      }
    }
  }
  for (auto seq : cancelled) {
    const core::RpcId rpc_id = core::MakeRpcId(config_.shard, seq);
    rpc_.Cancel(rpc_id);
    apply_notifier_.NotifyApplied(rpc_id);
  }

  auto wiped = store_.Wipe();
  if (!wiped.has_value()) {
    counters_.apply_failures.fetch_add(1, std::memory_order_relaxed);
    ABYSS_LOG_ERROR("hot wipe failed", {"shard", static_cast<int64_t>(config_.shard)},
                    {"seq", static_cast<uint64_t>(entry.seq)},
                    {"err", std::string_view{wiped.error().message()}});
    // Fulfil the RPC with the error so the engine surfaces it instead of timing out.
    const core::RpcId rpc_id = core::MakeFlushRpcId(core::kHotConsumer, config_.shard, entry.seq);
    rpc_.Fulfill(rpc_id,
                 core::RespValue::Error(core::ErrorPrefix::kErr,
                                        "hot store wipe failed: " + wiped.error().message()));
    apply_notifier_.NotifyApplied(rpc_id);
    MarkSettledAndMaybeAck(entry.seq);
    return;
  }

  latest_flush_seq_.store(entry.seq, std::memory_order_release);

  // Persist the Flush ack BEFORE fulfilling the RPC. See the equivalent comment
  // in ColdConsumer::HandleFlush.
  MarkSettledAndMaybeAck(entry.seq);

  const core::RpcId rpc_id = core::MakeFlushRpcId(core::kHotConsumer, config_.shard, entry.seq);
  (void)rpc_.Fulfill(rpc_id, core::RespValue::SimpleString("OK"));
  apply_notifier_.NotifyApplied(rpc_id);
  counters_.applied.fetch_add(1, std::memory_order_relaxed);
}

void HotConsumer::HandleResolved(const core::QueueEntry& entry,
                                 const core::entry::Resolved& resolved) {
  // Per ADP-011 invariant 7, a Resolved takes effect at the Conditional's seq
  // position. The skip-stale check uses the Conditional's wall-clock
  // appended_at (the original write time), not the Resolved's. The pending
  // map is the only place that timestamp lives; capture before erase.
  std::optional<core::WallTime> conditional_appended_at;
  bool had_pending = false;
  {
    const std::scoped_lock lock(pending_mu_);
    if (auto it = pending_conditionals_.find(resolved.ref); it != pending_conditionals_.end()) {
      conditional_appended_at = it->second.entry.appended_at;
      pending_conditionals_.erase(it);
      had_pending = true;
    }
  }
  // If we never saw the Conditional (cold replay re-entered after a partial
  // recovery, or the Conditional landed before our ack point), fall back to
  // the Resolved's own appended_at. Less precise but a safe approximation:
  // the Resolved was emitted shortly after the Conditional in steady state,
  // and during recovery re-emission they share the original appended_at
  // (see Resolver::ReplayForRecovery — `out.appended_at = entry.appended_at`).
  const core::WallTime reference_at = conditional_appended_at.value_or(entry.appended_at);

  // Drop if the Conditional ref lives on the wiped side of a Flush.
  const core::SequenceId flush_high = latest_flush_seq_.load(std::memory_order_acquire);
  const bool wiped_by_flush = flush_high > 0 && resolved.ref < flush_high;

  if (!wiped_by_flush && resolved.decision == core::Decision::kApply) {
    auto applied = ApplyResolvedOps(resolved.materialised_ops, reference_at);
    if (!applied.has_value()) {
      counters_.apply_failures.fetch_add(1, std::memory_order_relaxed);
      if (applied.error().code() != core::ErrorCode::kWrongType) {
        ABYSS_LOG_ERROR("hot resolved apply failed", {"shard", static_cast<int64_t>(config_.shard)},
                        {"ref", static_cast<uint64_t>(resolved.ref)},
                        {"err", std::string_view{applied.error().message()}});
      }
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

core::Result<void> HotConsumer::ApplyResolvedOps(const std::vector<core::RespCommand>& ops,
                                                 core::WallTime reference_at) {
  const bool replaying = replay_mode_.load(std::memory_order_acquire);
  const auto wall_now = config_.wall_clock();

  for (const auto& cmd : ops) {
    if (cmd.args.empty()) {
      counters_.parse_failures.fetch_add(1, std::memory_order_relaxed);
      return std::unexpected(
          core::Error(core::ErrorCode::kInvalidArgument, "empty materialised op"));
    }
    auto op = core::ops::ParseWriteOp(cmd.args[0], cmd, WallMs(reference_at));
    if (!op.has_value()) {
      counters_.parse_failures.fetch_add(1, std::memory_order_relaxed);
      return std::unexpected(op.error());
    }
    const auto key = core::ops::PrimaryKey(*op);

    if (replaying && ShouldSkipForEvictionElapsed(reference_at, key, wall_now)) {
      counters_.replay_skipped_eviction.fetch_add(1, std::memory_order_relaxed);
      continue;
    }
    if (replaying && ShouldSkipForAbsTtlElapsed(AbsTtlMs(*op), wall_now)) {
      counters_.replay_skipped_abs_ttl.fetch_add(1, std::memory_order_relaxed);
      continue;
    }

    // Reply value is constructed by the Resolver; discard here.
    auto applied = store_.Apply(*op);
    if (!applied.has_value()) return std::unexpected(applied.error());
    counters_.applied.fetch_add(1, std::memory_order_relaxed);
  }
  return {};
}

bool HotConsumer::ShouldSkipForEvictionElapsed(core::WallTime appended_at, std::string_view key,
                                               core::WallTime wall_now) const {
  const auto eviction = eviction_policy_.Resolve(key);
  // Cast both sides to milliseconds for an unambiguous compare; entries from
  // a clock that ran backwards across restart still skip cleanly.
  const auto appended_ms =
      std::chrono::duration_cast<std::chrono::milliseconds>(appended_at.time_since_epoch()).count();
  const auto deadline_ms =
      appended_ms + std::chrono::duration_cast<std::chrono::milliseconds>(eviction).count();
  const auto now_ms =
      std::chrono::duration_cast<std::chrono::milliseconds>(wall_now.time_since_epoch()).count();
  return now_ms > deadline_ms;
}

bool HotConsumer::ShouldSkipForAbsTtlElapsed(uint64_t abs_ttl_ms, core::WallTime wall_now) {
  if (abs_ttl_ms == 0) return false;
  return WallMs(wall_now) >= abs_ttl_ms;
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
        "hot consumer block-and-scan timeout; resolver may be stuck",
        {"shard", static_cast<int64_t>(config_.shard)},
        {"oldest_age_ms",
         static_cast<int64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(age).count())});
    block_and_scan_warning_emitted_ = true;
    counters_.block_and_scan_timeouts.fetch_add(1, std::memory_order_relaxed);
  }
}

}  // namespace abyss::consumer
