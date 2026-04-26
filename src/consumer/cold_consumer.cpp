#include "abyss/consumer/cold_consumer.h"

#include <algorithm>
#include <chrono>
#include <string>
#include <utility>
#include <variant>

#include "abyss/core/ops.h"
#include "abyss/log/log.h"

namespace abyss::consumer {

namespace {

constexpr size_t kDefaultLowWaterNumerator = 3;
constexpr size_t kDefaultLowWaterDenominator = 4;

const log::Logger& Log() {
  static const log::Logger l = log::Get("abyss.cold.consumer");
  return l;
}

}  // namespace

ColdConsumer::ColdConsumer(core::Queue& queue, core::ColdStore& cold_store, core::ShardId shard,
                           Config config, core::EvictionPolicy eviction_policy,
                           core::SteadyClockFn steady_clock, core::WallClockFn wall_clock)
    : queue_(queue),
      cold_store_(cold_store),
      shard_(shard),
      config_(config),
      eviction_policy_(std::move(eviction_policy)),
      steady_clock_(std::move(steady_clock)),
      wall_clock_(std::move(wall_clock)),
      strategy_(config_.quiet_threshold, config_.safety_margin, config_.jitter_fraction),
      buffer_(strategy_, steady_clock_, config_.rng_seed) {}

ColdConsumer::~ColdConsumer() { Stop(); }

void ColdConsumer::Start() {
  if (running_.exchange(true, std::memory_order_acq_rel)) return;
  stop_requested_.store(false, std::memory_order_release);
  thread_ = std::thread(&ColdConsumer::RunLoop, this);
}

void ColdConsumer::RequestStop() { stop_requested_.store(true, std::memory_order_release); }

void ColdConsumer::Join() {
  if (!running_.load(std::memory_order_acquire)) return;
  if (thread_.joinable()) thread_.join();
  running_.store(false, std::memory_order_release);
}

void ColdConsumer::Stop() {
  RequestStop();
  Join();
}

void ColdConsumer::RunLoop() {
  ABYSS_LOG_DEBUG(Log(), "cold consumer started", {"shard", static_cast<int64_t>(shard_)});
  while (!stop_requested_.load(std::memory_order_acquire)) {
    Drain();
    if (stop_requested_.load(std::memory_order_acquire)) break;
    Flush();
    CheckBlockAndScanTimeout();
  }
  ABYSS_LOG_DEBUG(Log(), "cold consumer stopped", {"shard", static_cast<int64_t>(shard_)},
                  {"last_ack_seq", static_cast<uint64_t>(last_ack_seq_.load())},
                  {"buffer_entries", static_cast<uint64_t>(buffer_.Size())});
}

size_t ColdConsumer::Drain() {
  auto result = queue_.Read(core::kColdConsumer, shard_, config_.queue_read_max_count,
                            config_.queue_read_timeout);
  if (!result.has_value()) {
    if (result.error().code() == core::ErrorCode::kUnavailable) {
      // Queue has shut down; signal loop exit rather than spinning on the same error.
      ABYSS_LOG_WARN(Log(), "cold consumer stopping: queue unavailable",
                     {"shard", static_cast<int64_t>(shard_)});
      stop_requested_.store(true, std::memory_order_release);
      return 0;
    }
    counters_.queue_read_failures.fetch_add(1, std::memory_order_relaxed);
    return 0;
  }

  size_t count = 0;
  for (const auto& entry : *result) {
    const auto seq = entry.seq;
    std::visit(
        [this, &entry, &count](const auto& payload) {
          using T = std::decay_t<decltype(payload)>;
          if constexpr (std::is_same_v<T, core::entry::Write>) {
            HandleWrite(entry, payload);
            ++count;
          } else if constexpr (std::is_same_v<T, core::entry::Conditional>) {
            HandleConditional(entry, payload);
          } else if constexpr (std::is_same_v<T, core::entry::Resolved>) {
            HandleResolved(entry, payload);
            ++count;
          }
        },
        entry.payload);
    latest_drained_seq_.store(seq, std::memory_order_release);
  }
  return count;
}

void ColdConsumer::HandleWrite(const core::QueueEntry& entry, const core::entry::Write& write) {
  if (write.cmd.args.empty()) {
    counters_.parse_failures.fetch_add(1, std::memory_order_relaxed);
    return;
  }
  AbsorbResolvedOp(write.cmd, entry.seq);
}

void ColdConsumer::HandleConditional(const core::QueueEntry& entry,
                                     const core::entry::Conditional& /*cond*/) {
  const auto seq = entry.seq;
  const std::scoped_lock lock(pending_mu_);
  pending_conditionals_.emplace(
      seq, PendingConditional{.seq = seq, .received_at = std::chrono::steady_clock::now()});
}

void ColdConsumer::HandleResolved(const core::QueueEntry& /*entry*/,
                                  const core::entry::Resolved& resolved) {
  {
    const std::scoped_lock lock(pending_mu_);
    pending_conditionals_.erase(resolved.ref);
  }
  if (resolved.decision != core::Decision::kApply) return;
  for (const auto& cmd : resolved.materialised_ops) {
    if (cmd.args.empty()) {
      counters_.parse_failures.fetch_add(1, std::memory_order_relaxed);
      continue;
    }
    AbsorbResolvedOp(cmd, resolved.ref);
  }
}

bool ColdConsumer::AbsorbResolvedOp(const core::RespCommand& cmd, core::SequenceId seq) {
  auto op = core::ops::ParseWriteOp(cmd.Name(), cmd);
  if (!op.has_value()) {
    counters_.parse_failures.fetch_add(1, std::memory_order_relaxed);
    return false;
  }

  // Multi-key ops expand into per-key absorbs; the buffer is keyed by single keys.
  if (const auto* del = std::get_if<core::ops::Del>(&*op)) {
    for (const auto key : del->keys) {
      const std::string key_str(key);
      auto eviction = eviction_policy_.Resolve(key_str);
      core::ops::Del single{.keys = {key}};
      buffer_.Absorb(key_str, core::ops::WriteOp{single}, eviction, seq);
    }
    return !del->keys.empty();
  }

  if (const auto* mset = std::get_if<core::ops::MultiStringSet>(&*op)) {
    for (const auto& kv : mset->entries) {
      const std::string key_str(kv.key);
      auto eviction = eviction_policy_.Resolve(key_str);
      core::ops::StringSet single{.key = kv.key, .value = kv.value, .abs_ttl_ms = 0};
      buffer_.Absorb(key_str, core::ops::WriteOp{single}, eviction, seq);
    }
    return !mset->entries.empty();
  }

  auto key = core::ops::PrimaryKey(*op);
  if (key.empty()) {
    counters_.parse_failures.fetch_add(1, std::memory_order_relaxed);
    return false;
  }

  const std::string key_str(key);
  auto eviction = eviction_policy_.Resolve(key_str);
  buffer_.Absorb(key_str, *op, eviction, seq);
  return true;
}

std::optional<core::SequenceId> ColdConsumer::OldestPendingConditional() const {
  const std::scoped_lock lock(pending_mu_);
  if (pending_conditionals_.empty()) return std::nullopt;
  core::SequenceId oldest = std::numeric_limits<core::SequenceId>::max();
  for (const auto& [seq, _] : pending_conditionals_) {
    oldest = std::min(oldest, seq);
  }
  return oldest;
}

void ColdConsumer::CheckBlockAndScanTimeout() {
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
        Log(), "cold consumer block-and-scan timeout; resolver may be stuck",
        {"shard", static_cast<int64_t>(shard_)},
        {"oldest_age_ms",
         static_cast<int64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(age).count())});
    block_and_scan_warning_emitted_ = true;
    counters_.block_and_scan_timeouts.fetch_add(1, std::memory_order_relaxed);
  }
}

bool ColdConsumer::Flush() {
  UpdateMode(buffer_.BytesEstimate());

  const auto now = steady_clock_();
  const bool aggressive = CurrentMode() == Mode::kAggressive;
  const auto flush_start = std::chrono::steady_clock::now();
  auto to_flush = aggressive ? buffer_.FlushOldest(LowWaterBytes(), config_.max_flush_batch_size)
                             : buffer_.FlushReady(now, config_.max_flush_batch_size);

  if (to_flush.empty()) {
    TryAdvanceAck();
    return false;
  }

  const auto wall_now = wall_clock_();
  std::vector<BufferEntry> surviving;
  surviving.reserve(to_flush.size());
  uint64_t dropped = 0;
  uint64_t quiet_count = 0;
  uint64_t deadline_count = 0;
  for (auto& entry : to_flush) {
    if (AbsTtlExpired(entry, wall_now)) {
      ++dropped;
      continue;
    }
    if (entry.last_trigger == FlushTrigger::kQuiet) {
      ++quiet_count;
    } else {
      ++deadline_count;
    }
    surviving.push_back(std::move(entry));
  }

  if (dropped > 0) {
    entries_dropped_abs_ttl_.fetch_add(dropped, std::memory_order_relaxed);
  }

  const size_t surviving_count = surviving.size();
  bool applied = true;
  if (!surviving.empty()) {
    applied = ApplyBatchWithRetry(std::move(surviving));
  }

  if (applied && surviving_count > 0) {
    if (aggressive) {
      flushes_aggressive_.fetch_add(surviving_count, std::memory_order_relaxed);
    } else {
      if (quiet_count > 0) flushes_quiet_.fetch_add(quiet_count, std::memory_order_relaxed);
      if (deadline_count > 0) {
        flushes_deadline_.fetch_add(deadline_count, std::memory_order_relaxed);
      }
    }

    const auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                                std::chrono::steady_clock::now() - flush_start)
                                .count();
    ABYSS_LOG_DEBUG(Log(), "cold flush", {"shard", static_cast<int64_t>(shard_)},
                    {"entries", static_cast<uint64_t>(surviving_count)},
                    {"quiet", static_cast<uint64_t>(quiet_count)},
                    {"deadline", static_cast<uint64_t>(deadline_count)}, {"aggressive", aggressive},
                    {"dropped_ttl", static_cast<uint64_t>(dropped)},
                    {"duration_ms", static_cast<int64_t>(elapsed_ms)});
  }

  TryAdvanceAck();
  return applied;
}

std::vector<core::ops::WriteOp> ColdConsumer::BuildBatchOps(
    const std::vector<BufferEntry>& entries, std::vector<core::ops::Del>& del_storage) const {
  std::vector<core::ops::WriteOp> ops;
  ops.reserve(entries.size());
  del_storage.reserve(entries.size());

  for (const auto& entry : entries) {
    if (entry.state.IsTombstone()) {
      del_storage.push_back(core::ops::Del{.keys = {entry.key}});
      ops.emplace_back(del_storage.back());
      continue;
    }
    // CompactedState::Emit leaves `.key` empty (no key state); patch from the entry.
    auto emitted = entry.state.Emit();
    for (auto& op : emitted) {
      std::visit(
          [&entry](auto& typed) {
            using T = std::decay_t<decltype(typed)>;
            if constexpr (std::is_same_v<T, core::ops::Del>) {
              typed.keys = {entry.key};
            } else if constexpr (!std::is_same_v<T, core::ops::MultiStringSet>) {
              typed.key = entry.key;
            }
          },
          op);
      ops.push_back(std::move(op));
    }
  }
  return ops;
}

bool ColdConsumer::ApplyBatchWithRetry(std::vector<BufferEntry> entries) {
  std::vector<core::ops::Del> del_storage;
  auto ops = BuildBatchOps(entries, del_storage);

  auto backoff = config_.retry_initial_backoff;
  bool logged_first_failure = false;
  while (!stop_requested_.load(std::memory_order_acquire)) {
    auto result = cold_store_.ApplyBatch(std::span<const core::ops::WriteOp>(ops));
    if (result.has_value()) {
      ops_flushed_.fetch_add(ops.size(), std::memory_order_relaxed);
      return true;
    }

    const auto code = result.error().code();
    const bool terminal =
        code == core::ErrorCode::kCorruption || code == core::ErrorCode::kInvalidArgument;
    counters_.apply_failures.fetch_add(1, std::memory_order_relaxed);
    if (terminal) {
      apply_poisoned_.fetch_add(1, std::memory_order_relaxed);
      ABYSS_LOG_CRITICAL(Log(), "cold apply batch poisoned",
                         {"shard", static_cast<int64_t>(shard_)},
                         {"batch", static_cast<uint64_t>(ops.size())},
                         {"err", std::string_view{result.error().message()}});
      // Retrying a poisoned batch burns CPU without progressing; reinsert and bail.
      buffer_.Reinsert(std::move(entries));
      return false;
    }
    retry_attempts_.fetch_add(1, std::memory_order_relaxed);
    if (!logged_first_failure) {
      logged_first_failure = true;
      ABYSS_LOG_ERROR(Log(), "cold apply batch failed; retrying",
                      {"shard", static_cast<int64_t>(shard_)},
                      {"batch", static_cast<uint64_t>(ops.size())},
                      {"err", std::string_view{result.error().message()}});
    }

    std::this_thread::sleep_for(backoff);
    backoff = std::min(backoff * 2, config_.retry_max_backoff);
  }

  buffer_.Reinsert(std::move(entries));
  return false;
}

bool ColdConsumer::AbsTtlExpired(const BufferEntry& entry, core::WallTime wall_now) const {
  const uint64_t ttl_ms = entry.state.StringTtlMs();
  if (ttl_ms == 0) return false;
  const uint64_t now_ms = static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::milliseconds>(wall_now.time_since_epoch()).count());
  return now_ms >= ttl_ms;
}

size_t ColdConsumer::LowWaterBytes() const {
  if (config_.buffer_low_water_bytes > 0) return config_.buffer_low_water_bytes;
  return (config_.buffer_high_water_bytes * kDefaultLowWaterNumerator) /
         kDefaultLowWaterDenominator;
}

void ColdConsumer::UpdateMode(size_t current_bytes) {
  const size_t high = config_.buffer_high_water_bytes;
  const size_t low = LowWaterBytes();
  const auto current = mode_.load(std::memory_order_acquire);

  const bool promote = current == Mode::kNormal && high > 0 && current_bytes >= high;
  const bool demote = current == Mode::kAggressive && current_bytes <= low;
  if (!promote && !demote) return;

  mode_.store(promote ? Mode::kAggressive : Mode::kNormal, std::memory_order_release);
  mode_transitions_.fetch_add(1, std::memory_order_relaxed);

  if (promote) {
    ABYSS_LOG_WARN(Log(), "cold buffer aggressive mode entered; high water breached",
                   {"shard", static_cast<int64_t>(shard_)},
                   {"bytes", static_cast<uint64_t>(current_bytes)},
                   {"high_water_bytes", static_cast<uint64_t>(high)});
  } else {
    ABYSS_LOG_INFO(Log(), "cold buffer back to normal mode",
                   {"shard", static_cast<int64_t>(shard_)},
                   {"bytes", static_cast<uint64_t>(current_bytes)},
                   {"low_water_bytes", static_cast<uint64_t>(low)});
  }
}

void ColdConsumer::TryAdvanceAck() {
  const auto oldest_unflushed = buffer_.OldestPendingSeq();
  const auto oldest_pending_cond = OldestPendingConditional();
  const auto drained = latest_drained_seq_.load(std::memory_order_acquire);

  core::SequenceId target = drained;
  if (oldest_unflushed.has_value() && *oldest_unflushed > 0) {
    target = std::min(target, *oldest_unflushed - 1);
  }
  if (oldest_pending_cond.has_value() && *oldest_pending_cond > 0) {
    target = std::min(target, *oldest_pending_cond - 1);
  }

  const auto last_ack = last_ack_seq_.load(std::memory_order_acquire);
  // The first queue entry has seq=0; without first_ack_recorded_ the initial
  // last_ack=0 is indistinguishable from "we already acked 0".
  if (first_ack_recorded_ && target <= last_ack) return;

  auto ack = queue_.Ack(core::kColdConsumer, shard_, target);
  if (!ack.has_value()) {
    counters_.ack_failures.fetch_add(1, std::memory_order_relaxed);
    return;
  }

  last_ack_seq_.store(target, std::memory_order_release);
  first_ack_recorded_ = true;
}

ColdConsumer::Metrics ColdConsumer::Snapshot() const {
  const auto common = metrics::SnapshotOf(counters_);
  Metrics out;
  out.buffer_entries = buffer_.Size();
  out.buffer_bytes = buffer_.BytesEstimate();
  out.mode = mode_.load(std::memory_order_acquire);
  out.flushes_quiet = flushes_quiet_.load(std::memory_order_relaxed);
  out.flushes_deadline = flushes_deadline_.load(std::memory_order_relaxed);
  out.flushes_aggressive = flushes_aggressive_.load(std::memory_order_relaxed);
  out.ops_flushed = ops_flushed_.load(std::memory_order_relaxed);
  out.entries_dropped_abs_ttl = entries_dropped_abs_ttl_.load(std::memory_order_relaxed);
  out.apply_failures = common.apply_failures;
  out.apply_poisoned = apply_poisoned_.load(std::memory_order_relaxed);
  out.retry_attempts = retry_attempts_.load(std::memory_order_relaxed);
  out.parse_failures = common.parse_failures;
  out.queue_read_failures = common.queue_read_failures;
  out.last_ack_seq = last_ack_seq_.load(std::memory_order_acquire);
  out.latest_drained_seq = latest_drained_seq_.load(std::memory_order_acquire);
  out.mode_transitions = mode_transitions_.load(std::memory_order_relaxed);
  return out;
}

}  // namespace abyss::consumer
