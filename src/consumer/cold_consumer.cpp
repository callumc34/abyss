#include "abyss/consumer/cold_consumer.h"

#include <algorithm>
#include <chrono>
#include <string>
#include <utility>
#include <variant>

#include "abyss/core/ops.h"

namespace abyss::consumer {

namespace {

constexpr size_t kDefaultLowWaterNumerator = 3;
constexpr size_t kDefaultLowWaterDenominator = 4;

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
  if (running_.exchange(true, std::memory_order_acq_rel)) {
    return;
  }
  stop_requested_.store(false, std::memory_order_release);
  thread_ = std::make_unique<std::thread>([this] { RunLoop(running_); });
}

void ColdConsumer::Stop() {
  if (!running_.exchange(false, std::memory_order_acq_rel)) return;
  stop_requested_.store(true, std::memory_order_release);
  if (thread_ && thread_->joinable()) {
    thread_->join();
  }
  thread_.reset();
}

void ColdConsumer::RunLoop(std::atomic<bool>& keep_running) {
  while (keep_running.load(std::memory_order_acquire)) {
    Drain();
    if (!keep_running.load(std::memory_order_acquire)) break;
    Flush();
  }
}

size_t ColdConsumer::Drain() {
  auto result = queue_.Read(core::kColdConsumer, shard_, config_.queue_read_max_count,
                            config_.queue_read_timeout);
  if (!result.has_value()) {
    return 0;
  }

  size_t count = 0;
  for (const auto& entry : *result) {
    if (AbsorbQueueEntry(entry)) ++count;
    latest_drained_seq_.store(entry.seq, std::memory_order_release);
  }
  return count;
}

bool ColdConsumer::AbsorbQueueEntry(const core::QueueEntry& entry) {
  const core::RespCommand* cmd = nullptr;
  if (const auto* w = std::get_if<core::entry::Write>(&entry.payload)) {
    cmd = &w->cmd;
  } else if (const auto* c = std::get_if<core::entry::Conditional>(&entry.payload)) {
    cmd = &c->cmd;
  } else if (const auto* r = std::get_if<core::entry::Resolved>(&entry.payload)) {
    if (r->decision == core::Decision::kSkip || !r->materialised_op.has_value()) {
      return false;
    }
    cmd = &*r->materialised_op;
  }

  if (cmd == nullptr || cmd->args.empty()) {
    const std::lock_guard lock(metrics_mutex_);
    ++metrics_.parse_failures;
    return false;
  }

  auto op = core::ops::ParseWriteOp(cmd->Name(), *cmd);
  if (!op.has_value()) {
    const std::lock_guard lock(metrics_mutex_);
    ++metrics_.parse_failures;
    return false;
  }

  // Multi-key ops expand into per-key absorbs; the buffer is keyed by single keys.
  if (const auto* del = std::get_if<core::ops::Del>(&*op)) {
    for (const auto key : del->keys) {
      const std::string key_str(key);
      auto eviction = eviction_policy_.Resolve(key_str);
      core::ops::Del single{.keys = {key}};
      buffer_.Absorb(key_str, core::ops::WriteOp{single}, eviction, entry.seq);
    }
    return !del->keys.empty();
  }

  if (const auto* mset = std::get_if<core::ops::MultiStringSet>(&*op)) {
    for (const auto& kv : mset->entries) {
      const std::string key_str(kv.key);
      auto eviction = eviction_policy_.Resolve(key_str);
      core::ops::StringSet single{.key = kv.key, .value = kv.value, .abs_ttl_ms = 0};
      buffer_.Absorb(key_str, core::ops::WriteOp{single}, eviction, entry.seq);
    }
    return !mset->entries.empty();
  }

  auto key = core::ops::PrimaryKey(*op);
  if (key.empty()) {
    const std::lock_guard lock(metrics_mutex_);
    ++metrics_.parse_failures;
    return false;
  }

  const std::string key_str(key);
  auto eviction = eviction_policy_.Resolve(key_str);
  buffer_.Absorb(key_str, *op, eviction, entry.seq);
  return true;
}

bool ColdConsumer::Flush() {
  const auto current_bytes = buffer_.BytesEstimate();
  {
    const std::lock_guard lock(metrics_mutex_);
    UpdateMode(current_bytes);
  }

  const auto now = steady_clock_();
  std::vector<BufferEntry> to_flush;
  const bool aggressive = CurrentMode() == Mode::kAggressive;

  // NOLINTNEXTLINE(bugprone-branch-clone)
  if (aggressive) {
    to_flush = buffer_.FlushOldest(LowWaterBytes(), config_.max_flush_batch_size);
  } else {
    to_flush = buffer_.FlushReady(now, config_.max_flush_batch_size);
  }

  if (to_flush.empty()) {
    TryAdvanceAck();
    return false;
  }

  const auto wall_now = wall_clock_();
  std::vector<BufferEntry> surviving;
  surviving.reserve(to_flush.size());
  uint64_t dropped = 0;
  for (auto& entry : to_flush) {
    if (AbsTtlExpired(entry, wall_now)) {
      ++dropped;
      continue;
    }
    surviving.push_back(std::move(entry));
  }

  if (dropped > 0) {
    const std::lock_guard lock(metrics_mutex_);
    metrics_.entries_dropped_abs_ttl += dropped;
  }

  const size_t surviving_count = surviving.size();
  bool applied = true;
  if (!surviving.empty()) {
    applied = ApplyBatchWithRetry(std::move(surviving));
  }

  if (applied && surviving_count > 0) {
    const std::lock_guard lock(metrics_mutex_);
    // Quiet vs deadline breakdown waits for metrics wiring; aggressive is distinct.
    // NOLINTNEXTLINE(bugprone-branch-clone)
    if (aggressive) {
      metrics_.flushes_aggressive += surviving_count;
    } else {
      metrics_.flushes_quiet += surviving_count;
    }
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
  while (!stop_requested_.load(std::memory_order_acquire)) {
    auto result = cold_store_.ApplyBatch(std::span<const core::ops::WriteOp>(ops));
    if (result.has_value()) {
      const std::lock_guard lock(metrics_mutex_);
      metrics_.ops_flushed += ops.size();
      return true;
    }

    {
      const std::lock_guard lock(metrics_mutex_);
      ++metrics_.apply_failures;
      ++metrics_.retry_attempts;
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
  metrics_.buffer_bytes = current_bytes;
  const size_t high = config_.buffer_high_water_bytes;
  const size_t low = LowWaterBytes();

  if (mode_ == Mode::kNormal && high > 0 && current_bytes >= high) {
    mode_ = Mode::kAggressive;
    ++metrics_.mode_transitions;
  } else if (mode_ == Mode::kAggressive && current_bytes <= low) {
    mode_ = Mode::kNormal;
    ++metrics_.mode_transitions;
  }
  metrics_.mode = mode_;
}

ColdConsumer::Mode ColdConsumer::CurrentMode() const {
  const std::lock_guard lock(metrics_mutex_);
  return mode_;
}

void ColdConsumer::TryAdvanceAck() {
  const auto oldest = buffer_.OldestPendingSeq();
  const auto drained = latest_drained_seq_.load(std::memory_order_acquire);

  core::SequenceId target = drained;
  if (oldest.has_value() && *oldest > 0) {
    target = std::min(target, *oldest - 1);
  }

  const auto last_ack = last_ack_seq_.load(std::memory_order_acquire);
  if (target <= last_ack) return;
  if (target == 0) return;

  auto ack = queue_.Ack(core::kColdConsumer, shard_, target);
  if (!ack.has_value()) return;

  last_ack_seq_.store(target, std::memory_order_release);
  const std::lock_guard lock(metrics_mutex_);
  metrics_.last_ack_seq = target;
  metrics_.latest_drained_seq = drained;
}

ColdConsumer::Metrics ColdConsumer::Snapshot() const {
  const std::lock_guard lock(metrics_mutex_);
  Metrics out = metrics_;
  out.buffer_entries = buffer_.Size();
  out.buffer_bytes = buffer_.BytesEstimate();
  out.mode = mode_;
  out.latest_drained_seq = latest_drained_seq_.load(std::memory_order_acquire);
  out.last_ack_seq = last_ack_seq_.load(std::memory_order_acquire);
  return out;
}

}  // namespace abyss::consumer
