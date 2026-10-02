#include "abyss/consumer/cold_consumer.h"

#include <algorithm>
#include <chrono>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <variant>

#include "abyss/core/fatal.h"
#include "abyss/core/ops.h"
#include "abyss/log/log.h"
#include "abyss/metrics/names.h"

ABYSS_LOG_COMPONENT("abyss.cold.consumer")

namespace abyss::consumer {

namespace {

constexpr size_t kDefaultLowWaterNumerator = 3;
constexpr size_t kDefaultLowWaterDenominator = 4;

}  // namespace

ColdConsumer::ColdConsumer(core::Queue& queue, core::ColdStore& cold_store, core::ShardId shard,
                           Config config, const core::EvictionPolicy& eviction_policy,
                           core::ConsumerRpc& rpc, core::SteadyClockFn steady_clock,
                           core::WallClockFn wall_clock)
    : queue_(queue),
      cold_store_(cold_store),
      rpc_(rpc),
      shard_(shard),
      config_(config),
      eviction_policy_(eviction_policy),
      steady_clock_(std::move(steady_clock)),
      wall_clock_(std::move(wall_clock)),
      strategy_(config_.quiet_threshold, config_.safety_margin, config_.jitter_fraction),
      buffer_(strategy_, steady_clock_, config_.rng_seed) {
  auto& reg = metrics::Registry::Instance();
  flush_reason_quiet_ =
      reg.Counter(metrics::names::kColdFlushReasonTotal, metrics::FlushReason::kQuiet);
  flush_reason_deadline_ =
      reg.Counter(metrics::names::kColdFlushReasonTotal, metrics::FlushReason::kDeadline);
  flush_reason_pressure_ =
      reg.Counter(metrics::names::kColdFlushReasonTotal, metrics::FlushReason::kPressure);
  flush_reason_drain_ =
      reg.Counter(metrics::names::kColdFlushReasonTotal, metrics::FlushReason::kDrain);
  drain_truncated_ = reg.Counter(metrics::names::kColdDrainTruncatedTotal);
  flush_total_success_ =
      reg.Counter(metrics::names::kColdFlushTotal, metrics::FlushStatus::kSuccess);
  flush_total_failure_ =
      reg.Counter(metrics::names::kColdFlushTotal, metrics::FlushStatus::kFailure);
  checkpoint_total_success_ =
      reg.Counter(metrics::names::kColdCheckpointTotal, metrics::FlushStatus::kSuccess);
  checkpoint_total_failure_ =
      reg.Counter(metrics::names::kColdCheckpointTotal, metrics::FlushStatus::kFailure);
  checkpoint_duration_ = reg.Histogram(metrics::names::kColdCheckpointDurationSeconds);
  checkpoint_interval_ = reg.Gauge(metrics::names::kColdCheckpointIntervalSeconds);
  backoff_idle_ =
      reg.Counter(metrics::names::kColdConsumerBackoffTotal, metrics::BackoffReason::kIdle);
  backoff_poisoned_ =
      reg.Counter(metrics::names::kColdConsumerBackoffTotal, metrics::BackoffReason::kPoisoned);
  backoff_backpressure_ =
      reg.Counter(metrics::names::kColdConsumerBackoffTotal, metrics::BackoffReason::kBackpressure);
  parse_poison_total_ = reg.Counter(metrics::names::kColdParsePoisonTotal);
  unsupported_op_total_ = reg.Counter(metrics::names::kColdUnsupportedOpTotal);
  flush_heap_depth_ = reg.Gauge(metrics::names::kColdFlushHeapDepth);
}

ColdConsumer::~ColdConsumer() { Stop(); }

void ColdConsumer::Start() {
  if (running_.exchange(true, std::memory_order_acq_rel)) return;
  stop_requested_.store(false, std::memory_order_release);
  thread_ = std::thread(&ColdConsumer::RunLoop, this);
}

void ColdConsumer::RequestStop() {
  {
    const std::scoped_lock lock(stop_mu_);
    stop_requested_.store(true, std::memory_order_release);
  }
  stop_cv_.notify_all();
}

void ColdConsumer::RequestStopAndDrain(std::chrono::steady_clock::time_point deadline) {
  {
    const std::scoped_lock lock(stop_mu_);
    // Publish the deadline before draining_ so RunLoop's DrainAndFlush, which
    // observes draining_ with acquire, sees a fully-written deadline.
    drain_deadline_ = deadline;
    draining_.store(true, std::memory_order_release);
    stop_requested_.store(true, std::memory_order_release);
  }
  stop_cv_.notify_all();
}

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
  ABYSS_LOG_DEBUG("cold consumer started", {"shard", static_cast<int64_t>(shard_)});
  auto backoff = config_.loop_initial_backoff;
  while (!stop_requested_.load(std::memory_order_acquire)) {
    const size_t drained = Drain();
    if (stop_requested_.load(std::memory_order_acquire)) break;
    const FlushOutcome outcome = Flush();
    CheckBlockAndScanTimeout();

    // Backoff state machine (XRES-5): re-iterate immediately on progress (the
    // cursor moved or a flush wrote), otherwise sleep on a capped exponential
    // backoff so a poisoned/unwritable/idle shard never busy-spins.
    if (drained > 0 || outcome == FlushOutcome::kProgress) {
      backoff = config_.loop_initial_backoff;
      continue;
    }
    if (wipe_pending_) {
      backoff_backpressure_.Increment();
    } else {
      switch (outcome) {
        case FlushOutcome::kPoisoned:
          backoff_poisoned_.Increment();
          break;
        case FlushOutcome::kBackpressure:
          backoff_backpressure_.Increment();
          break;
        case FlushOutcome::kIdle:
        case FlushOutcome::kProgress:
          backoff_idle_.Increment();
          break;
      }
    }
    std::unique_lock lock(stop_mu_);
    // A reader blocked on the read-consistency gate needs this shard to drain
    // NOW; its deadline is far shorter than the idle backoff ceiling. Waking on
    // that request keeps the gate honest -- it must fire only when the consumer
    // is genuinely wedged, never merely because it was asleep -- while still
    // never busy-spinning, since the wake only happens when a reader is waiting.
    const bool woken_for_reader = stop_cv_.wait_for(lock, backoff, [this] {
      return stop_requested_.load(std::memory_order_acquire) ||
             drain_wake_requested_.load(std::memory_order_acquire);
    });
    if (woken_for_reader && !stop_requested_.load(std::memory_order_acquire)) {
      drain_wake_requested_.store(false, std::memory_order_release);
      backoff = config_.loop_initial_backoff;
      continue;
    }
    backoff = std::min(backoff * 2, config_.loop_max_backoff);
  }
  // Graceful stop: drain the buffer to durable cold and advance the commit
  // before returning (G6). Abrupt stop (RequestStop) relies on the WAL.
  if (draining_.load(std::memory_order_acquire)) {
    DrainAndFlush();
  }
  ABYSS_LOG_DEBUG("cold consumer stopped", {"shard", static_cast<int64_t>(shard_)},
                  {"last_commit_seq", static_cast<uint64_t>(last_commit_seq_.load())},
                  {"buffer_entries", static_cast<uint64_t>(buffer_.Size())});
}

void ColdConsumer::DrainAndFlush() {
  const auto deadline = drain_deadline_;
  ABYSS_LOG_INFO("cold consumer draining", {"shard", static_cast<int64_t>(shard_)},
                 {"buffer_entries", static_cast<uint64_t>(buffer_.Size())});

  // One final drain of any queue entries the loop may have left behind, so the
  // flush below carries the freshest tail. Bounded; never blocks past deadline.
  if (std::chrono::steady_clock::now() < deadline) {
    DrainWithBatch(config_.queue_read_max_count);
  }

  // Flush the buffer unconditionally (bypassing the quiet/deadline strategy),
  // attributed as a drain flush. Bounded by the deadline: if the cold store is
  // wedged or the buffer is larger than one budget allows, stop and leave the
  // remainder for WAL replay rather than blocking shutdown forever (invariant 5).
  bool truncated = false;
  while (buffer_.Size() > 0) {
    if (std::chrono::steady_clock::now() >= deadline) {
      truncated = true;
      break;
    }
    if (FlushUnscheduled(metrics::FlushReason::kDrain) != FlushOutcome::kProgress) {
      // No forward progress (poisoned/unwritable batch reinserted): do not
      // busy-spin to the deadline — the queue still has the data. Bail and let
      // replay handle it.
      truncated = true;
      break;
    }
  }

  // Force a checkpoint so the advanced commit is durable-gated even though
  // the drain flushed fewer batches than the steady-state cadence (A6 +
  // XDUR-1). TryAdvanceCommit never commits past the cold checkpoint or the
  // durable WAL tail, so a partial drain is sound.
  TryAdvanceCommit(/*force_checkpoint=*/true);

  if (truncated) {
    drain_truncated_.Increment();
    ABYSS_LOG_WARN("cold drain truncated at deadline; remaining buffer replays from WAL",
                   {"shard", static_cast<int64_t>(shard_)},
                   {"buffer_entries", static_cast<uint64_t>(buffer_.Size())},
                   {"last_commit_seq", static_cast<uint64_t>(last_commit_seq_.load())});
  } else {
    ABYSS_LOG_INFO("cold drain complete", {"shard", static_cast<int64_t>(shard_)},
                   {"last_commit_seq", static_cast<uint64_t>(last_commit_seq_.load())});
  }
}

size_t ColdConsumer::Drain() { return DrainWithBatch(config_.queue_read_max_count); }

size_t ColdConsumer::DrainWithBatch(size_t max_count) {
  if (auto seeded = SeedCursor(); !seeded.has_value()) {
    counters_.queue_read_failures.fetch_add(1, std::memory_order_relaxed);
    return 0;
  }
  auto result = queue_.Read(shard_, next_read_seq_, max_count, config_.queue_read_timeout);
  if (!result.has_value()) {
    if (result.error().code() == core::ErrorCode::kUnavailable) {
      // Queue has shut down; signal loop exit rather than spinning on the same error.
      ABYSS_LOG_WARN("cold consumer stopping: queue unavailable",
                     {"shard", static_cast<int64_t>(shard_)});
      stop_requested_.store(true, std::memory_order_release);
      return 0;
    }
    if (result.error().code() == core::ErrorCode::kOutOfRange) FailOutOfRange(next_read_seq_);
    counters_.queue_read_failures.fetch_add(1, std::memory_order_relaxed);
    return 0;
  }
  if (result->empty()) return 0;
  const size_t consumed = ConsumeBatch(*result);
  // Wake any read-consistency waiters now that the drained seq has advanced.
  if (consumed > 0) NotifyDrained();
  return consumed;
}

core::Result<void> ColdConsumer::SeedCursor() {
  if (cursor_seeded_) return {};
  auto committed = queue_.CommittedOffset(core::kColdConsumer, shard_);
  if (!committed.has_value()) return std::unexpected(committed.error());
  committed_ = *committed;
  if (committed_.has_value()) {
    next_read_seq_ = *committed_ + 1;
    if (*committed_ > latest_drained_seq_.load(std::memory_order_acquire)) {
      latest_drained_seq_.store(*committed_, std::memory_order_release);
    }
    last_commit_seq_.store(*committed_, std::memory_order_release);
  }
  cursor_seeded_ = true;
  return {};
}

size_t ColdConsumer::ConsumeBatch(const std::vector<core::QueueEntry>& batch) {
  size_t consumed = 0;
  for (const auto& entry : batch) {
    // Handlers see the cursor past the entry they are applying.
    next_read_seq_ = entry.seq + 1;
    const bool applied = std::visit(
        [this, &entry](const auto& payload) {
          using T = std::decay_t<decltype(payload)>;
          if constexpr (std::is_same_v<T, core::entry::Write>) {
            HandleWrite(entry, payload);
          } else if constexpr (std::is_same_v<T, core::entry::Conditional>) {
            HandleConditional(entry, payload);
          } else if constexpr (std::is_same_v<T, core::entry::Resolved>) {
            HandleResolved(entry, payload);
          } else if constexpr (std::is_same_v<T, core::entry::Flush>) {
            return HandleFlush(entry);
          }
          return true;
        },
        entry.payload);
    // A failed wipe holds the cursor and frontier below the Flush, so
    // the commit cannot pass it and the next drain retries it.
    if (!applied) {
      next_read_seq_ = entry.seq;
      break;
    }
    // The cursor moves past a poison entry, but the drained frontier stays
    // pinned below it, so the commit (and WAL retention) does too (XERR-5).
    const core::SequenceId poison = oldest_poison_seq_.load(std::memory_order_acquire);
    core::SequenceId frontier = entry.seq;
    if (poison != kNoPoison) frontier = poison == 0 ? 0 : std::min(entry.seq, poison - 1);
    latest_drained_seq_.store(frontier, std::memory_order_release);
    ++consumed;
  }
  return consumed;
}

void ColdConsumer::FailOutOfRange(core::SequenceId requested) {
  const auto first = queue_.FirstSeq(shard_);
  const std::string first_text = first.has_value() ? std::to_string(*first) : "unknown";
  ABYSS_LOG_CRITICAL(
      "cold consumer read below the first retained WAL seq", {"consumer", std::string_view{"cold"}},
      {"shard", static_cast<int64_t>(shard_)}, {"requested_seq", static_cast<uint64_t>(requested)},
      {"first_seq", std::string_view{first_text}});
  core::Fatal("cold consumer on shard " + std::to_string(shard_) + " read seq " +
              std::to_string(requested) + " below first retained seq " + first_text +
              ": WAL entries above its persisted offset were reclaimed");
}

core::Result<void> ColdConsumer::ReplayUntil(core::SequenceId target,
                                             const std::atomic<bool>& cancel) {
  ABYSS_LOG_INFO("cold replay starting", {"shard", static_cast<int64_t>(shard_)},
                 {"target_seq", static_cast<uint64_t>(target)});

  if (auto seeded = SeedCursor(); !seeded.has_value()) return std::unexpected(seeded.error());

  // Caught up once the cursor passes target. An empty read also means caught
  // up: target was captured below the head, so nothing at the cursor proves
  // the cursor is past it (and covers the empty queue, where target 0 names
  // no entry).
  bool caught_up = false;
  auto wipe_retry_backoff = config_.loop_initial_backoff;
  while (!cancel.load(std::memory_order_acquire)) {
    if (stop_requested_.load(std::memory_order_acquire)) {
      return std::unexpected(
          core::Error{core::ErrorCode::kUnavailable, "cold replay aborted by stop"});
    }
    if (next_read_seq_ > target) {
      caught_up = true;
      break;
    }
    auto read =
        queue_.Read(shard_, next_read_seq_, config_.replay_batch_size, config_.queue_read_timeout);
    if (!read.has_value()) {
      if (read.error().code() == core::ErrorCode::kUnavailable) {
        return std::unexpected(read.error());
      }
      if (read.error().code() == core::ErrorCode::kOutOfRange) FailOutOfRange(next_read_seq_);
      counters_.queue_read_failures.fetch_add(1, std::memory_order_relaxed);
      continue;
    }
    if (read->empty()) {
      caught_up = true;
      break;
    }
    if (ConsumeBatch(*read) < read->size()) {
      // A Flush's wipe failed; retry it on a capped backoff, not a spin.
      std::unique_lock lock(stop_mu_);
      stop_cv_.wait_for(lock, wipe_retry_backoff,
                        [this] { return stop_requested_.load(std::memory_order_acquire); });
      wipe_retry_backoff = std::min(wipe_retry_backoff * 2, config_.loop_max_backoff);
      continue;
    }
    wipe_retry_backoff = config_.loop_initial_backoff;
    NotifyDrained();

    if (buffer_.BytesEstimate() >= config_.buffer_high_water_bytes) {
      Flush();
    }
  }

  if (!caught_up) {
    ABYSS_LOG_WARN("cold replay cancelled before reaching target",
                   {"shard", static_cast<int64_t>(shard_)},
                   {"target_seq", static_cast<uint64_t>(target)});
    return std::unexpected(core::Error{core::ErrorCode::kUnavailable, "cold replay cancelled"});
  }

  // Drain the buffer to disk. Replay-absorbed entries have a fresh first_seen
  // (set when DrainWithBatch absorbed them seconds ago), so the steady-state
  // FlushReady() would defer them by quiet_threshold and the loop would
  // deadlock. FlushUnscheduled() pops oldest unconditionally, which is the
  // right semantic for "make this buffer empty before declaring recovery
  // done." Bounded by max_flush_batch_size per call; loop until empty.
  while (!cancel.load(std::memory_order_acquire) && buffer_.Size() > 0) {
    if (FlushUnscheduled() != FlushOutcome::kProgress) {
      // No progress: either the buffer reported entries but FlushOldest
      // returned none (shouldn't happen for non-empty buffer with target=0),
      // or ApplyBatchWithRetry hit a poisoned batch and reinserted them. The
      // latter is unrecoverable here — operator intervention required.
      ABYSS_LOG_ERROR("cold replay flush stalled", {"shard", static_cast<int64_t>(shard_)},
                      {"buffer_entries", static_cast<uint64_t>(buffer_.Size())});
      return std::unexpected(core::Error{core::ErrorCode::kInternal, "cold replay flush stalled"});
    }
  }
  // Force a checkpoint so the post-recovery commit is durable-gated even
  // when the drain flushed fewer batches than the steady-state cadence
  // (XDUR-1).
  TryAdvanceCommit(/*force_checkpoint=*/true);

  if (cancel.load(std::memory_order_acquire) && buffer_.Size() > 0) {
    return std::unexpected(
        core::Error{core::ErrorCode::kUnavailable, "cold replay cancelled during flush"});
  }

  ABYSS_LOG_INFO(
      "cold replay complete", {"shard", static_cast<int64_t>(shard_)},
      {"latest_drained",
       static_cast<uint64_t>(latest_drained_seq_.load(std::memory_order_acquire))},
      {"last_commit_seq", static_cast<uint64_t>(last_commit_seq_.load(std::memory_order_acquire))});
  return {};
}

void ColdConsumer::HandleWrite(const core::QueueEntry& entry, const core::entry::Write& write) {
  if (write.cmd.args.empty()) {
    counters_.parse_failures.fetch_add(1, std::memory_order_relaxed);
    RecordPoison(entry.seq, "empty write command");
    return;
  }
  const uint64_t wall_now_ms = static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::milliseconds>(entry.appended_at.time_since_epoch())
          .count());
  AbsorbResolvedOp(write.cmd, entry.seq, wall_now_ms);
}

void ColdConsumer::HandleConditional(const core::QueueEntry& entry,
                                     const core::entry::Conditional& /*cond*/) {
  const auto seq = entry.seq;
  const std::scoped_lock lock(pending_mu_);
  pending_conditionals_.emplace(
      seq, PendingConditional{.seq = seq, .received_at = std::chrono::steady_clock::now()});
}

void ColdConsumer::HandleResolved(const core::QueueEntry& entry,
                                  const core::entry::Resolved& resolved) {
  {
    const std::scoped_lock lock(pending_mu_);
    pending_conditionals_.erase(resolved.ref);
  }
  // Drop if the Conditional ref lives on the wiped side of a Flush.
  const core::SequenceId flush_high = latest_flush_seq_.load(std::memory_order_acquire);
  if (flush_high > 0 && resolved.ref < flush_high) return;
  if (resolved.decision != core::Decision::kApply) return;
  // Materialised ops use PXAT so wall_now_ms is unused; pass appended_at for symmetry.
  const uint64_t wall_now_ms = static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::milliseconds>(entry.appended_at.time_since_epoch())
          .count());
  for (const auto& cmd : resolved.materialised_ops) {
    if (cmd.args.empty()) {
      counters_.parse_failures.fetch_add(1, std::memory_order_relaxed);
      RecordPoison(resolved.ref, "empty resolved materialised op");
      continue;
    }
    // Non-poison ops in the same Resolved still absorb.
    AbsorbResolvedOp(cmd, resolved.ref, wall_now_ms);
  }
}

bool ColdConsumer::HandleFlush(const core::QueueEntry& entry) {
  ABYSS_LOG_DEBUG("cold HandleFlush", {"shard", static_cast<int64_t>(shard_)},
                  {"seq", static_cast<uint64_t>(entry.seq)});

  // Erase pre-Flush pending Conditionals so block-and-scan doesn't stall on
  // them. The client-facing RPC is cancelled by the hot consumer.
  {
    const std::scoped_lock lock(pending_mu_);
    for (auto it = pending_conditionals_.begin(); it != pending_conditionals_.end();) {
      if (it->first < entry.seq) {
        it = pending_conditionals_.erase(it);
      } else {
        ++it;
      }
    }
  }

  const core::RpcId rpc_id = core::MakeFlushRpcId(core::kColdConsumer, shard_, entry.seq);
  auto wiped = cold_store_.Wipe(shard_);
  if (!wiped.has_value()) {
    counters_.apply_failures.fetch_add(1, std::memory_order_relaxed);
    ABYSS_LOG_ERROR("cold wipe failed; retrying the Flush", {"shard", static_cast<int64_t>(shard_)},
                    {"seq", static_cast<uint64_t>(entry.seq)},
                    {"err", std::string_view{wiped.error().message()}});
    // No reply yet: OK follows a successful retry, and the engine's own
    // timeout reports a FLUSHDB that is still pending.
    wipe_pending_ = true;
    return false;
  }
  wipe_pending_ = false;
  // Only after the wipe: until then the buffer is the newest pre-Flush
  // state, and dropping it would let reads fall back to older cold data.
  buffer_.Clear();

  latest_flush_seq_.store(entry.seq, std::memory_order_release);
  flushes_applied_.fetch_add(1, std::memory_order_relaxed);

  // A poison before the Flush pinned data the Wipe just discarded, and a
  // replay re-wipes it too, so it no longer holds retention.
  if (const core::SequenceId poison = oldest_poison_seq_.load(std::memory_order_acquire);
      poison < entry.seq) {
    oldest_poison_seq_.store(kNoPoison, std::memory_order_release);
    ABYSS_LOG_INFO("cold flush released the parse-poison retention pin",
                   {"shard", static_cast<int64_t>(shard_)},
                   {"poison_seq", static_cast<uint64_t>(poison)},
                   {"flush_seq", static_cast<uint64_t>(entry.seq)});
  }

  // Wipe is a synced (durable) write, so the Flush seq is on cold's stable
  // storage on return: advance the checkpoint frontier so the commit clamp
  // lets it through without a second FlushWAL. The Wipe discarded any
  // pre-Flush uncheckpointed data, so its frontier no longer matters.
  if (entry.seq > last_checkpointed_seq_.load(std::memory_order_acquire)) {
    last_checkpointed_seq_.store(entry.seq, std::memory_order_release);
    highest_applied_uncheckpointed_seq_.store(entry.seq, std::memory_order_release);
  }
  latest_drained_seq_.store(entry.seq, std::memory_order_release);
  NotifyDrained();

  // OK needs only the synced Wipe: the engine already awaited the Flush
  // entry's WAL durability, a replayed Flush re-wipes this shard, and
  // retention follows the persisted offset, so post-Flush writes stay
  // replayable whether or not this commit lands now (ADP-006).
  TryAdvanceCommit();
  (void)rpc_.Fulfill(rpc_id, core::RespValue::SimpleString("OK"));
  return true;
}

void ColdConsumer::AbsorbResolvedOp(const core::RespCommand& cmd, core::SequenceId seq,
                                    uint64_t wall_now_ms) {
  // A command with no parser at all cannot be materialised by ANY tier in this
  // build, so hot rejected it too and there is no state for cold to be missing:
  // both views agree the entry produced nothing. Poisoning here would pin WAL
  // retention forever over an entry that carries no materialisable write, which
  // any client could trigger at will. Quarantine is for genuine decoder skew --
  // a parser that exists and fails on bytes hot accepted.
  if (!core::ops::HasWriteParser(cmd.Name())) {
    unsupported_ops_.fetch_add(1, std::memory_order_relaxed);
    unsupported_op_total_.Increment();
    ABYSS_LOG_WARN("cold skipping write with no parser in this build",
                   {"shard", static_cast<int64_t>(shard_)}, {"seq", static_cast<uint64_t>(seq)},
                   {"cmd", std::string(cmd.Name())});
    return;
  }

  auto op = core::ops::ParseWriteOp(cmd.Name(), cmd, wall_now_ms);
  if (!op.has_value()) {
    counters_.parse_failures.fetch_add(1, std::memory_order_relaxed);
    RecordPoison(seq, "ParseWriteOp failed");
    return;
  }

  auto key = core::ops::PrimaryKey(*op);
  if (key.empty()) {
    counters_.parse_failures.fetch_add(1, std::memory_order_relaxed);
    RecordPoison(seq, "empty primary key");
    return;
  }

  const std::string key_str(key);
  auto eviction = eviction_policy_.Resolve(key_str);
  buffer_.Absorb(key_str, *op, eviction, seq);
}

void ColdConsumer::RecordPoison(core::SequenceId seq, std::string_view reason) {
  parse_poison_.fetch_add(1, std::memory_order_relaxed);
  parse_poison_total_.Increment();
  // Lower oldest_poison_seq_ toward seq (single-writer loop thread, but keep it
  // a CAS-min so any future concurrency stays correct).
  core::SequenceId prev = oldest_poison_seq_.load(std::memory_order_acquire);
  while (seq < prev &&
         !oldest_poison_seq_.compare_exchange_weak(prev, seq, std::memory_order_acq_rel)) {
  }
  ABYSS_LOG_CRITICAL("cold parse poison; WAL retention pinned below seq",
                     {"shard", static_cast<int64_t>(shard_)}, {"seq", static_cast<uint64_t>(seq)},
                     {"reason", reason});
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
        "cold consumer block-and-scan timeout; resolver may be stuck",
        {"shard", static_cast<int64_t>(shard_)},
        {"oldest_age_ms",
         static_cast<int64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(age).count())});
    block_and_scan_warning_emitted_ = true;
    counters_.block_and_scan_timeouts.fetch_add(1, std::memory_order_relaxed);
  }
}

ColdConsumer::FlushOutcome ColdConsumer::Flush() {
  UpdateMode(buffer_.BytesEstimate());

  const auto now = steady_clock_();
  const bool aggressive = CurrentMode() == Mode::kAggressive;
  const auto flush_start = std::chrono::steady_clock::now();
  auto to_flush = aggressive ? buffer_.FlushOldest(LowWaterBytes(), config_.max_flush_batch_size)
                             : buffer_.FlushReady(now, config_.max_flush_batch_size);

  if (to_flush.empty()) {
    // Nothing due to flush. Still try to advance the commit: a checkpoint
    // may now be due, or DurableSeq may have caught up to checkpointed data.
    TryAdvanceCommit();
    return FlushOutcome::kIdle;
  }

  return ApplyFlushBatch(std::move(to_flush),
                         aggressive
                             ? std::optional<metrics::FlushReason>{metrics::FlushReason::kPressure}
                             : std::nullopt,
                         flush_start);
}

ColdConsumer::FlushOutcome ColdConsumer::FlushUnscheduled(metrics::FlushReason reason) {
  const auto flush_start = std::chrono::steady_clock::now();
  auto to_flush = buffer_.FlushOldest(/*target_bytes=*/0, config_.max_flush_batch_size);
  if (to_flush.empty()) {
    TryAdvanceCommit();
    return FlushOutcome::kIdle;
  }
  // Bypass-the-strategy flushes (replay or graceful drain) are attributed by
  // the caller's reason rather than the quiet/deadline per-entry trigger.
  return ApplyFlushBatch(std::move(to_flush), reason, flush_start);
}

ColdConsumer::FlushOutcome ColdConsumer::ApplyFlushBatch(
    std::vector<BufferEntry> to_flush, std::optional<metrics::FlushReason> aggressive_reason,
    std::chrono::steady_clock::time_point flush_start) {
  const bool aggressive = aggressive_reason.has_value();
  const auto wall_now = wall_clock_();
  std::vector<BufferEntry> surviving;
  surviving.reserve(to_flush.size());
  uint64_t dropped = 0;
  uint64_t quiet_count = 0;
  uint64_t deadline_count = 0;
  // Highest WAL seq this flush materialises (informational; passed to
  // ApplyBatch). TryAdvanceCommit derives the commit frontier from the live
  // buffer state, not from this, so a reinserted failed batch re-pins it.
  const core::SequenceId batch_highest_seq = HighestSeqOf(to_flush);
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
  FlushOutcome outcome = FlushOutcome::kProgress;
  if (!surviving.empty()) {
    outcome = ApplyBatchWithRetry(std::move(surviving), batch_highest_seq);
  }
  const bool applied = outcome == FlushOutcome::kProgress;

  if (surviving_count > 0) {
    if (applied) {
      flush_total_success_.Increment(static_cast<double>(surviving_count));
      if (aggressive) {
        flushes_aggressive_.fetch_add(surviving_count, std::memory_order_relaxed);
        auto& reason_counter = *aggressive_reason == metrics::FlushReason::kDrain
                                   ? flush_reason_drain_
                                   : flush_reason_pressure_;
        reason_counter.Increment(static_cast<double>(surviving_count));
      } else {
        if (quiet_count > 0) {
          flushes_quiet_.fetch_add(quiet_count, std::memory_order_relaxed);
          flush_reason_quiet_.Increment(static_cast<double>(quiet_count));
        }
        if (deadline_count > 0) {
          flushes_deadline_.fetch_add(deadline_count, std::memory_order_relaxed);
          flush_reason_deadline_.Increment(static_cast<double>(deadline_count));
        }
      }
    } else {
      flush_total_failure_.Increment(static_cast<double>(surviving_count));
    }

    if (applied) {
      const auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                                  std::chrono::steady_clock::now() - flush_start)
                                  .count();
      ABYSS_LOG_DEBUG("cold flush", {"shard", static_cast<int64_t>(shard_)},
                      {"entries", static_cast<uint64_t>(surviving_count)},
                      {"quiet", static_cast<uint64_t>(quiet_count)},
                      {"deadline", static_cast<uint64_t>(deadline_count)},
                      {"aggressive", aggressive}, {"dropped_ttl", static_cast<uint64_t>(dropped)},
                      {"duration_ms", static_cast<int64_t>(elapsed_ms)});
    }
  }

  TryAdvanceCommit();
  return outcome;
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
            } else {
              typed.key = entry.key;
            }
          },
          op);
      ops.push_back(std::move(op));
    }
  }
  return ops;
}

core::SequenceId ColdConsumer::HighestSeqOf(const std::vector<BufferEntry>& entries) {
  core::SequenceId highest = 0;
  for (const auto& entry : entries) {
    highest = std::max(highest, entry.first_seen_seq);
  }
  return highest;
}

ColdConsumer::FlushOutcome ColdConsumer::ApplyBatchWithRetry(std::vector<BufferEntry> entries,
                                                             core::SequenceId highest_wal_seq) {
  std::vector<core::ops::Del> del_storage;
  auto ops = BuildBatchOps(entries, del_storage);

  auto result = cold_store_.ApplyBatch(std::span<const core::ops::WriteOp>(ops), highest_wal_seq);
  if (result.has_value()) {
    ops_flushed_.fetch_add(ops.size(), std::memory_order_relaxed);
    return FlushOutcome::kProgress;
  }

  const auto code = result.error().code();
  const bool terminal =
      code == core::ErrorCode::kCorruption || code == core::ErrorCode::kInvalidArgument;
  counters_.apply_failures.fetch_add(1, std::memory_order_relaxed);
  // Reinsert so the entries are replayed on the next iteration. The RunLoop
  // (not a blocking sleep here) applies bounded backoff between attempts so a
  // poisoned or unwritable shard never busy-spins (XRES-5).
  buffer_.Reinsert(std::move(entries));
  if (terminal) {
    apply_poisoned_.fetch_add(1, std::memory_order_relaxed);
    ABYSS_LOG_CRITICAL("cold apply batch poisoned", {"shard", static_cast<int64_t>(shard_)},
                       {"batch", static_cast<uint64_t>(ops.size())},
                       {"err", std::string_view{result.error().message()}});
    return FlushOutcome::kPoisoned;
  }
  retry_attempts_.fetch_add(1, std::memory_order_relaxed);
  ABYSS_LOG_ERROR("cold apply batch failed; will retry", {"shard", static_cast<int64_t>(shard_)},
                  {"batch", static_cast<uint64_t>(ops.size())},
                  {"err", std::string_view{result.error().message()}});
  return FlushOutcome::kBackpressure;
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
    ABYSS_LOG_WARN("cold buffer aggressive mode entered; high water breached",
                   {"shard", static_cast<int64_t>(shard_)},
                   {"bytes", static_cast<uint64_t>(current_bytes)},
                   {"high_water_bytes", static_cast<uint64_t>(high)});
  } else {
    ABYSS_LOG_INFO("cold buffer back to normal mode", {"shard", static_cast<int64_t>(shard_)},
                   {"bytes", static_cast<uint64_t>(current_bytes)},
                   {"low_water_bytes", static_cast<uint64_t>(low)});
  }
}

bool ColdConsumer::MaybeCheckpoint(core::SequenceId up_to, bool force) {
  const auto checkpointed = last_checkpointed_seq_.load(std::memory_order_acquire);
  // Nothing new to make durable since the last checkpoint: a FlushWAL would be
  // a no-op fsync. Skip it (idempotent + cheap, but pointless).
  if (up_to <= checkpointed) return true;
  // Track the highest seq applied to cold's memtable but not yet checkpointed,
  // so the cadence/observability reflects outstanding durable work.
  highest_applied_uncheckpointed_seq_.store(up_to, std::memory_order_release);

  const auto now = std::chrono::steady_clock::now();
  const bool cadence_due = ++flushes_since_checkpoint_ >= config_.checkpoint_max_flushes ||
                           (now - last_checkpoint_at_) >= config_.checkpoint_min_interval;
  if (!force && !cadence_due) return true;

  const auto start = std::chrono::steady_clock::now();
  auto result = cold_store_.Checkpoint(shard_, up_to);
  const auto elapsed = std::chrono::steady_clock::now() - start;
  checkpoint_duration_.Observe(
      std::chrono::duration_cast<std::chrono::duration<double>>(elapsed).count());

  if (!result.has_value()) {
    checkpoint_total_failure_.Increment();
    counters_.apply_failures.fetch_add(1, std::memory_order_relaxed);
    ABYSS_LOG_ERROR("cold checkpoint failed; commit pinned",
                    {"shard", static_cast<int64_t>(shard_)},
                    {"up_to_wal_seq", static_cast<uint64_t>(up_to)},
                    {"err", std::string_view{result.error().message()}});
    return false;
  }

  checkpoint_total_success_.Increment();
  if (last_checkpoint_at_.time_since_epoch().count() != 0) {
    checkpoint_interval_.Set(
        std::chrono::duration_cast<std::chrono::duration<double>>(now - last_checkpoint_at_)
            .count());
  }
  last_checkpoint_at_ = now;
  flushes_since_checkpoint_ = 0;
  last_checkpointed_seq_.store(up_to, std::memory_order_release);
  return true;
}

void ColdConsumer::TryAdvanceCommit(bool force_checkpoint) {
  // Nothing consumed and nothing committed: there is no seq to commit.
  if (next_read_seq_ == 0) return;

  const auto oldest_unflushed = buffer_.OldestPendingSeq();
  const auto oldest_pending_cond = OldestPendingConditional();
  const auto poison = oldest_poison_seq_.load(std::memory_order_acquire);
  const auto drained = latest_drained_seq_.load(std::memory_order_acquire);

  // Unsigned seq space has no "before 0": a commit below seq 0 cannot be
  // expressed, so an unfinished entry at seq 0 holds the commit back.
  if (oldest_unflushed.has_value() && *oldest_unflushed == 0) return;
  if (oldest_pending_cond.has_value() && *oldest_pending_cond == 0) return;
  // A poison at seq 0 pins the whole shard at the floor: nothing can be
  // committed without passing the un-materialised entry (XERR-5).
  if (poison == 0) return;

  // Low-water target: every seq <= this has been flushed to cold's memtable
  // (entries still in the buffer pin `oldest_unflushed`; a failed apply is
  // reinserted and re-pins it, so this never includes un-applied data).
  core::SequenceId target = drained;
  if (oldest_unflushed.has_value()) {
    target = std::min(target, *oldest_unflushed - 1);
  }
  if (oldest_pending_cond.has_value()) {
    target = std::min(target, *oldest_pending_cond - 1);
  }
  // Poison clamp (XERR-5): never commit past a structurally-undecodable
  // entry the cold view could not materialise. Pins WAL retention below the
  // poison for the whole shard until operator intervention. Composes with
  // C3's checkpoint-gated commit below: it only lowers the target.
  if (poison != kNoPoison) {
    target = std::min(target, poison - 1);
  }

  // (1) Make the cold data for `target` durable (on the bounded cadence, or
  // unconditionally when forced) so the commit below can pass it (A6). A
  // failed checkpoint pins the commit (back-pressure, not silent advance):
  // last_checkpointed_seq_ stays put.
  MaybeCheckpoint(target, force_checkpoint);

  // (2) Cold-durability clamp: the commit can never pass data not yet on
  // cold's stable storage (the FlushWAL checkpoint frontier), XDUR-1.
  target = std::min(target, last_checkpointed_seq_.load(std::memory_order_acquire));

  // (3) WAL-durability clamp (A1): the commit can never pass the durable
  // WAL tail. Clamping to DurableSeq means the fail-closed CommitOffset
  // gate never rejects us, turning WAL lag into clean back-pressure.
  if (auto durable = queue_.DurableSeq(shard_); durable.has_value()) {
    target = std::min(target, *durable);
  } else {
    counters_.commit_failures.fetch_add(1, std::memory_order_relaxed);
    return;
  }

  if (committed_.has_value() && target <= *committed_) return;
  // The checkpoint frontier's 0 is ambiguous ("seq 0" or "nothing yet").
  // With nothing committed, wait for a checkpoint above 0 to commit seq 0.
  if (!committed_.has_value() && target == 0 &&
      last_checkpointed_seq_.load(std::memory_order_acquire) == 0) {
    return;
  }

  if (auto commit = queue_.CommitOffset(core::kColdConsumer, shard_, target); !commit.has_value()) {
    counters_.commit_failures.fetch_add(1, std::memory_order_relaxed);
    return;
  }
  committed_ = target;
  last_commit_seq_.store(target, std::memory_order_release);
}

bool ColdConsumer::WaitForDrainedSeq(core::SequenceId target, std::chrono::milliseconds timeout) {
  if (latest_drained_seq_.load(std::memory_order_acquire) >= target) return true;

  // Ask the loop to drain now rather than finish its idle backoff, which may be
  // an order of magnitude longer than this wait's deadline.
  {
    const std::scoped_lock wake_lock(stop_mu_);
    drain_wake_requested_.store(true, std::memory_order_release);
  }
  stop_cv_.notify_all();

  std::unique_lock lock(drain_wait_mu_);
  return drain_wait_cv_.wait_for(lock, timeout, [this, target] {
    return latest_drained_seq_.load(std::memory_order_acquire) >= target;
  });
}

void ColdConsumer::NotifyDrained() {
  // Take and release the wait mutex so a waiter sitting between its predicate
  // check and entering wait() cannot miss this wakeup, then notify.
  {
    const std::scoped_lock lock(drain_wait_mu_);
  }
  drain_wait_cv_.notify_all();
}

ColdConsumer::Metrics ColdConsumer::Snapshot() const {
  const auto common = metrics::SnapshotOf(counters_);
  Metrics out;
  out.buffer_entries = buffer_.Size();
  out.buffer_bytes = buffer_.BytesEstimate();
  flush_heap_depth_.Set(static_cast<double>(buffer_.HeapDepth()));
  // ADP-004 lag signal: now - min(first_seen). Sampled buffer-first so a
  // concurrent Absorb cannot yield a first_seen ahead of `now`.
  const auto oldest_first_seen = buffer_.OldestFirstSeen();
  const auto now = steady_clock_();
  out.oldest_unflushed_age =
      oldest_first_seen.has_value()
          ? std::chrono::duration_cast<std::chrono::milliseconds>(now - *oldest_first_seen)
          : std::chrono::milliseconds{0};
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
  out.parse_poison = parse_poison_.load(std::memory_order_relaxed);
  out.unsupported_ops = unsupported_ops_.load(std::memory_order_relaxed);
  out.queue_read_failures = common.queue_read_failures;
  out.last_commit_seq = last_commit_seq_.load(std::memory_order_acquire);
  out.latest_drained_seq = latest_drained_seq_.load(std::memory_order_acquire);
  out.mode_transitions = mode_transitions_.load(std::memory_order_relaxed);
  return out;
}

}  // namespace abyss::consumer
