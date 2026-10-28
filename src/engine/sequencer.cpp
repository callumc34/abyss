#include "abyss/engine/sequencer.h"

#include <algorithm>
#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <numeric>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "abyss/core/fatal.h"
#include "abyss/core/queue_entry.h"
#include "abyss/log/log.h"

ABYSS_LOG_COMPONENT("abyss.engine.sequencer")

namespace abyss::engine {

namespace {

using metrics::RedecideReason;
using metrics::WritePhase;

constexpr std::string_view kWriteDurableTimeout =
    "write durable wait exceeded server timeout; the write is applied and may yet become durable";
constexpr std::string_view kFlushDurableTimeout =
    "flush durable wait exceeded server timeout; retry to complete the wipe";
constexpr std::string_view kFenceTimeout =
    "durable wait exceeded server timeout; the reply would show a write not yet durable";
constexpr std::string_view kForeignLoadTimeout = "write timed out awaiting a load of its key";

core::RespValue CrossSlot() {
  return core::RespValue::Error(core::ErrorPrefix::kCrossSlot,
                                "Keys in request don't hash to the same slot");
}

core::RespValue OutOfMemory() {
  return core::RespValue::Error(
      core::ErrorPrefix::kOom,
      "command not allowed when hot memory is over its limit and cold is behind");
}

// The reply once re-decides for `reason` have used up the deadline.
core::Result<core::RespValue> TimedOut(RedecideReason reason) {
  switch (reason) {
    case RedecideReason::kAdmission:
      return std::unexpected(
          core::Error{core::ErrorCode::kResourceExhausted,
                      "WAL durability window full: the device is not keeping up with writes"});
    case RedecideReason::kSpare:
      return std::unexpected(core::Error{core::ErrorCode::kTimeout,
                                         "write timed out waiting for a spare WAL segment"});
    case RedecideReason::kLoad:
      return std::unexpected(
          core::Error{core::ErrorCode::kTimeout, "write timed out loading its keys"});
    case RedecideReason::kBackpressure:
      return OutOfMemory();
  }
  return std::unexpected(core::Error{core::ErrorCode::kTimeout, "write timed out"});
}

core::Error SpareTimedOut(bool flush) {
  return core::Error{core::ErrorCode::kTimeout,
                     flush ? "flush timed out waiting for a spare WAL segment"
                           : "write timed out waiting for a spare WAL segment"};
}

uint64_t WallMs(core::WallTime at) {
  return static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::milliseconds>(at.time_since_epoch()).count());
}

size_t SlotOf(std::span<const core::ShardId> shards, core::ShardId shard) {
  return static_cast<size_t>(std::ranges::lower_bound(shards, shard) - shards.begin());
}

// Whether argument `i` of the canonical effect `cmd` carries written
// data: a SET's value, a member, a field or a field's value. Names,
// keys, TTLs and scores are small and not counted.
bool IsValueArg(const core::RespCommand& cmd, size_t i) {
  const std::string_view name = cmd.args[0];
  if (i < 2) return false;
  if (name == "SET") return i == 2;
  if (name == "ZADD") return i % 2 == 1;
  return name == "SADD" || name == "SREM" || name == "ZREM" || name == "HSET" || name == "HMSET" ||
         name == "HDEL";
}

// One write in 64 on average is timed, picked at random per thread: a
// fixed stride could line up with frames on a page, and no writer
// touches a shared cache line to decide.
bool Sampled() {
  thread_local uint64_t t_state = 0;
  if (t_state == 0) {
    uint64_t seed =
        std::hash<std::thread::id>{}(std::this_thread::get_id()) ^
        static_cast<uint64_t>(std::chrono::steady_clock::now().time_since_epoch().count());
    seed = (seed ^ (seed >> 30U)) * 0xBF58476D1CE4E5B9ULL;
    seed = (seed ^ (seed >> 27U)) * 0x94D049BB133111EBULL;
    // xorshift never reaches 0 from anything else.
    t_state = (seed ^ (seed >> 31U)) | 1U;
  }
  t_state ^= t_state << 13U;
  t_state ^= t_state >> 7U;
  t_state ^= t_state << 17U;
  return (t_state >> 58U) == 0;
}

}  // namespace

// The hot locks of one attempt, timed when its write is sampled.
class Sequencer::Hold {
 public:
  Hold(Sequencer& sequencer, std::span<const core::ShardId> shards, bool sampled)
      : sequencer_(sequencer),
        sampled_(sampled),
        locks_(sequencer.hot_.LockExclusive(shards)),
        start_(sampled_ ? std::chrono::steady_clock::now()
                        : std::chrono::steady_clock::time_point{}) {}
  Hold(const Hold&) = delete;
  Hold& operator=(const Hold&) = delete;
  Hold(Hold&&) = delete;
  Hold& operator=(Hold&&) = delete;
  ~Hold() { (void)Unlock(); }

  hot::ShardLocks* operator->() { return &locks_; }
  hot::ShardLocks& operator*() { return locks_; }

  // What the hold's applies replaced, for the caller to free.
  hot::Graveyard Unlock() {
    if (!locks_.held()) return {};
    if (sampled_) {
      sequencer_.lock_hold_.Observe(
          std::chrono::duration<double>(std::chrono::steady_clock::now() - start_).count());
    }
    return locks_.Unlock();
  }

 private:
  Sequencer& sequencer_;
  bool sampled_;
  hot::ShardLocks locks_;
  std::chrono::steady_clock::time_point start_;
};

// A sampled write's time in each stage, kept in the write across its
// attempts. Each lap charges the time since the last to one stage; a
// stage passed twice adds up.
class Sequencer::Phases {
 public:
  Phases(Sequencer& sequencer, PendingWrite::PhaseTimes& times)
      : sequencer_(sequencer), times_(times) {}

  bool sampled() const { return times_.sampled; }

  void Lap(WritePhase phase) {
    if (!times_.sampled) return;
    const auto now = std::chrono::steady_clock::now();
    const auto index = static_cast<size_t>(phase);
    times_.spent.at(index) += now - times_.last;
    times_.passed.at(index) = true;
    times_.last = now;
  }

  // Once the write succeeded: the stages it passed.
  void Record() const {
    if (!times_.sampled) return;
    for (size_t i = 0; i < times_.spent.size(); ++i) {
      if (!times_.passed.at(i)) continue;
      sequencer_.phase_seconds_.at(i).Observe(
          std::chrono::duration<double>(times_.spent.at(i)).count());
    }
  }

 private:
  Sequencer& sequencer_;
  PendingWrite::PhaseTimes& times_;
};

Sequencer::Sequencer(hot::ShardedHotStore& hot, core::Queue& queue, Loader& loader,
                     consumer::CompactionBufferRouter& buffers, SequencerConfig config)
    : hot_(hot), queue_(queue), loader_(loader), buffers_(buffers), config_(std::move(config)) {
  auto& reg = metrics::Registry::Instance();
  locked_copy_bytes_metric_ = reg.Counter(metrics::names::kSequencerLockedCopyBytesTotal);
  for (const RedecideReason reason : {RedecideReason::kAdmission, RedecideReason::kSpare,
                                      RedecideReason::kLoad, RedecideReason::kBackpressure}) {
    redecides_metric_.at(static_cast<size_t>(reason)) =
        reg.Counter(metrics::names::kSequencerRedecidesTotal, reason);
  }
  backpressure_waits_metric_ = reg.Counter(metrics::names::kHotBackpressureWaitsTotal);
  backpressure_rejections_metric_ = reg.Counter(metrics::names::kHotBackpressureRejectionsTotal);
  lock_hold_ = reg.Histogram(metrics::names::kSequencerLockHoldSeconds);
  for (size_t i = 0; i < phase_seconds_.size(); ++i) {
    phase_seconds_.at(i) =
        reg.Histogram(metrics::names::kSequencerPhaseSeconds, static_cast<WritePhase>(i));
  }
}

Step Sequencer::Attempt(PendingWrite& write) {
  ABYSS_DCHECK(write.owner_ == std::this_thread::get_id(),
               "a write attempted off its owner's thread");
  ABYSS_DCHECK(write.phase_ != PendingWrite::Phase::kDone, "an attempt of a write that replied");
  if (write.phase_ == PendingWrite::Phase::kDurable) return AwaitDurable(write);
  if (write.waiting_) {
    const bool ended =
        write.wake_.has_value() || write.expired_ || write.wait_reason_ == WaitReason::kLoads;
    ABYSS_DCHECK(ended, "a write attempted while its wait is open");
    // Past its deadline it is never applied, whatever woke it.
    if (write.expired_ || core::SteadyClock::now() >= write.deadline_) return Abandon(write);
    write.waiting_ = false;
  }
  return write.flush_ ? FlushAttempt(write) : WriteAttempt(write);
}

void Sequencer::Load(PendingWrite& write) {
  write.completions_.reserve(write.owned_.size());
  for (const PendingWrite::Owned& load : write.owned_) {
    auto result = loader_.Load(load.shard, load.key, load.need, write.deadline_);
    if (!result.has_value()) {
      write.load_error_ = result.error();
      return;
    }
    write.completions_.push_back(
        {.key = load.key, .token = load.token, .result = *std::move(result)});
  }
}

bool Sequencer::Expire(PendingWrite& write) {
  ABYSS_DCHECK(write.owner_ == std::this_thread::get_id(),
               "a write expired off its owner's thread");
  ABYSS_DCHECK(write.waiting_, "a write expired while not waiting");
  ABYSS_DCHECK(!write.wake_.has_value(), "a write expired with its wake in hand");
  bool cancelled = false;
  if (write.phase_ == PendingWrite::Phase::kDurable) {
    const ShardSeq at = write.DurableAt(write.durable_next_);
    cancelled = write.node_.has_value() &&
                queue_.CancelDurable(at.shard, queue_.AckDurability(), *write.node_);
    if (cancelled) write.node_.reset();
  } else {
    switch (write.wait_reason_) {
      case WaitReason::kAdmission:
        cancelled = queue_.CancelWait(core::QueueWait::kAdmission, write.wait_shard_, write);
        break;
      case WaitReason::kSpare:
        cancelled = queue_.CancelWait(core::QueueWait::kSpare, write.wait_shard_, write);
        break;
      case WaitReason::kBackpressure:
        cancelled = buffers_.CancelDrainWait(write.wait_shard_, write);
        break;
      case WaitReason::kForeignLoad:
        cancelled = hot_.CancelLoadWait(write.foreign_.at(write.foreign_next_), write.load_wake_);
        break;
      case WaitReason::kLoads:
        // A load in flight reports back when it lands.
        break;
    }
  }
  write.expired_ = cancelled;
  return cancelled;
}

// Deadlock-free: hot shard locks are taken ascending, then each
// stream's append_mu_ inside Reserve, and nothing waits under a hot
// lock: admission, spare segments, loads, cold drain and durability
// are all waited for with every lock released. A write takes every
// placeholder it owns in one hold, so its foreign waits form no cycle.
Step Sequencer::WriteAttempt(PendingWrite& write) {
  using Phase = PendingWrite::Phase;
  Phases phases(*this, write.phase_times_);
  core::RespCommand& cmd = write.cmd_;
  std::vector<core::ShardId>& shards = write.shards_;
  std::vector<PendingWrite::Spare>& spare = write.spare_;
  if (write.phase_ == Phase::kStart) {
    Start(write);
    auto keys = WriteKeys(cmd);
    if (!keys.has_value()) return Done(write, std::unexpected(keys.error()));
    shards.reserve(keys->size());
    for (const std::string_view key : *keys) shards.push_back(hot_.ShardOf(key));
    std::ranges::sort(shards);
    shards.erase(std::ranges::unique(shards).begin(), shards.end());
    if (std::ranges::any_of(shards, [&](core::ShardId shard) {
          return queue_.LogOf(shard) != queue_.LogOf(shards.front());
        })) {
      return Done(write, CrossSlot());
    }

    // Large arguments are copied for the log before any lock, once; a
    // decision moves the request's own strings into hot.
    size_t total = 0;
    for (size_t i = 1; i < cmd.args.size(); ++i) total += cmd.args[i].size();
    for (size_t i = 1; i < cmd.args.size(); ++i) {
      if (total > core::kLockHoldFrameBytes || cmd.args[i].size() > core::kLockHoldFrameBytes) {
        if (spare.empty()) spare.resize(cmd.args.size());
        spare[i].copy = cmd.args[i];
      }
    }
    write.grows_ = GrowsMemory(cmd);
    write.phase_ = Phase::kAdmit;
  }
  if (write.phase_ == Phase::kLoads) write.phase_ = Phase::kLoaded;

  for (;;) {
    if (write.phase_ == Phase::kAdmit) {
      if (auto step = AwaitAdmission(write)) return *std::move(step);
      write.phase_ = Phase::kTop;
    }
    if (write.phase_ == Phase::kSpare) {
      if (auto step = AwaitSpares(write)) return *std::move(step);
      write.phase_ = Phase::kTop;
    }
    // A shard's appended_at never goes backwards: cold expires by it.
    // One instant per hold is decide's now and every entry's stamp.
    core::WallTime wall;
    std::optional<Hold> hold;
    const auto instant_of = [&shards, &hold](core::WallTime from) {
      for (const core::ShardId shard : shards) {
        from = std::max(from, (*hold)->LastAppendedAt(shard));
      }
      return from;
    };
    core::WallTime instant;
    uint64_t now_ms = 0;
    const KeyLookup lookup = [&hold, &now_ms](std::string_view key) {
      return (*hold)->View(key, now_ms);
    };
    Decision decision;
    // Memory judged before this hold's loads went in: what they add is
    // the decision's own keys, which an eviction would only send to
    // load again.
    bool judged = false;

    if (write.phase_ == Phase::kTop) {
      // Every way round is bounded by the deadline, a re-check under the
      // locks that keeps failing after its wait succeeded included.
      // A memory wait resuming goes on inside AwaitMemory, as it would
      // have blocked there.
      if (!write.wake_.has_value() && write.redecided_.has_value() &&
          core::SteadyClock::now() >= write.deadline_) {
        if (*write.redecided_ == RedecideReason::kBackpressure) {
          backpressure_rejections_.fetch_add(1, std::memory_order_relaxed);
          backpressure_rejections_metric_.Increment();
        }
        return Done(write, TimedOut(*write.redecided_));
      }
      if (write.await_memory_) {
        if (auto step = AwaitMemory(write)) return *std::move(step);
      }
      wall = config_.wall_clock();
      phases.Lap(WritePhase::kAdmit);
      hold.emplace(*this, shards, phases.sampled());
      phases.Lap(WritePhase::kLockWait);
      instant = instant_of(wall);
      now_ms = WallMs(instant);
      decision = Decide(cmd, write.flags_, now_ms, lookup);
      phases.Lap(WritePhase::kDecide);

      if (!decision.needs_load.empty()) {
        for (KeyLoad& load : decision.needs_load) {
          const hot::LoadStart start = (*hold)->BeginLoad(load.key);
          if (start.started()) {
            write.full_load_ = write.full_load_ || load.need == Need::kState;
            const core::ShardId shard = hot_.ShardOf(load.key);
            write.owned_.push_back({.key = std::move(load.key),
                                    .shard = shard,
                                    .need = load.need,
                                    .token = start.token});
          } else {
            // Decide found the key unknown in this same hold, so it can
            // only be another's load.
            ABYSS_DCHECK(start.status == hot::LoadStart::Status::kPending,
                         "a key decide could not see is resident or flushed");
            write.foreign_.push_back(std::move(load.key));
          }
        }
        hold->Unlock();
        write.phase_ = Phase::kForeign;
      }
    }

    if (write.phase_ == Phase::kForeign) {
      if (auto step = AwaitForeignLoads(write)) return *std::move(step);
      if (!write.owned_.empty()) {
        write.phase_ = Phase::kLoads;
        return Waiting(write, WaitReason::kLoads);
      }
      write.phase_ = Phase::kLoaded;
    }

    if (write.phase_ == Phase::kLoaded) {
      if (write.load_error_.has_value()) {
        core::Error error = *std::move(write.load_error_);
        AbortLoads(write);
        return Done(write, std::unexpected(std::move(error)));
      }
      wall = config_.wall_clock();
      hold.reset();
      phases.Lap(WritePhase::kLoad);
      hold.emplace(*this, shards, phases.sampled());
      phases.Lap(WritePhase::kLockWait);
      write.phase_ = Phase::kTop;
      // A full load grows memory however the command reads. A write
      // that grows is judged here, before its own keys go in: a TTL
      // load of a string brings its value too.
      if ((write.full_load_ || write.grows_) &&
          std::ranges::any_of(
              shards, [&hold](core::ShardId shard) { return (*hold)->OverBackpressure(shard); })) {
        for (const PendingWrite::Owned& load : write.owned_) {
          (*hold)->AbortLoad(load.key, load.token);
        }
        hold->Unlock();
        ClearLoads(write);
        Again(write, RedecideReason::kBackpressure);
        write.await_memory_ = true;
        continue;
      }
      judged = write.full_load_ || write.grows_;
      for (const core::ShardId shard : shards) {
        std::vector<hot::LoadCompletion> mine;
        for (size_t i = 0; i < write.owned_.size(); ++i) {
          if (write.owned_[i].shard == shard) mine.push_back(std::move(write.completions_[i]));
        }
        if (!mine.empty()) (*hold)->CompleteLoads(shard, mine);
      }
      ClearLoads(write);
      phases.Lap(WritePhase::kLoad);
      instant = instant_of(wall);
      now_ms = WallMs(instant);
      decision = Decide(cmd, write.flags_, now_ms, lookup);
      phases.Lap(WritePhase::kDecide);
      if (!decision.needs_load.empty()) {
        // A blind write overtook a load.
        hold->Unlock();
        Again(write, RedecideReason::kLoad);
        continue;
      }
    }

    if (decision.error.has_value() || decision.effects.empty()) {
      // Nothing to log: the reply, an error included, still waits until
      // what it read is durable.
      hold->Unlock();
      write.fences_ = std::move(decision.observed);
      write.record_phases_ = !decision.error.has_value();
      if (decision.error.has_value()) {
        write.reply_ = std::unexpected(*std::move(decision.error));
      } else {
        ABYSS_DCHECK(decision.reply.has_value(), "a decision with no effect has no reply");
        write.reply_ = *std::move(decision.reply);
      }
      write.durable_kind_ = PendingWrite::DurableKind::kFence;
      write.phase_ = Phase::kDurable;
      return AwaitDurable(write);
    }
    if (write.grows_ && !judged && std::ranges::any_of(shards, [&hold](core::ShardId shard) {
          return (*hold)->OverBackpressure(shard);
        })) {
      Restore(std::move(decision), cmd);
      hold->Unlock();
      Again(write, RedecideReason::kBackpressure);
      write.await_memory_ = true;
      continue;
    }

    // The log entries, grouped by shard slot, each group in effect
    // order. One shard, the common case, keeps effect order and needs
    // no table: `order` lists the effects by slot only for several.
    const size_t count = decision.effects.size();
    std::vector<uint32_t> order;
    std::vector<uint32_t> slot_of;
    // Each effect's place in `order`.
    std::vector<uint32_t> placed;
    if (shards.size() > 1) {
      slot_of.reserve(count);
      for (const core::Effect& effect : decision.effects) {
        slot_of.push_back(static_cast<uint32_t>(SlotOf(shards, hot_.ShardOf(effect.key))));
      }
      order.resize(count);
      std::ranges::iota(order, uint32_t{0});
      std::ranges::stable_sort(order, {}, [&slot_of](uint32_t e) { return slot_of[e]; });
      placed.resize(count);
      for (size_t i = 0; i < count; ++i) placed[order[i]] = static_cast<uint32_t>(i);
    }
    const auto effect_at = [&order](size_t i) -> size_t { return order.empty() ? i : order[i]; };
    const auto slot_at = [&slot_of, &order](size_t i) -> size_t {
      return order.empty() ? 0 : slot_of[order[i]];
    };
    std::vector<core::QueueEntry> entries;
    entries.reserve(count);
    for (size_t i = 0; i < count; ++i) {
      const core::Effect& effect = decision.effects[effect_at(i)];
      entries.push_back(core::QueueEntry{
          .appended_at = instant,
          .payload = core::entry::Write{.cmd = core::RespCommand{.args = std::vector<std::string>(
                                                                     effect.cmd.args.size())}},
          .replaces_state = effect.replaces_state,
      });
    }
    const auto args_at = [&entries](size_t i) -> std::vector<std::string>& {
      return std::get<core::entry::Write>(entries[i].payload).cmd.args;
    };
    const auto entry_of = [&placed](size_t effect) -> size_t {
      return placed.empty() ? effect : placed[effect];
    };
    if (!spare.empty()) {
      for (const Moved& moved : decision.moved) {
        PendingWrite::Spare& copy = spare[moved.request_arg];
        if (!copy.copy.has_value()) continue;
        args_at(entry_of(moved.effect))[moved.arg] = *std::move(copy.copy);
        copy.copy.reset();
        copy.lent = true;
      }
    }
    uint64_t copied = 0;
    for (size_t i = 0; i < count; ++i) {
      const core::Effect& effect = decision.effects[effect_at(i)];
      std::vector<std::string>& args = args_at(i);
      for (size_t a = 0; a < args.size(); ++a) {
        // Filled from a copy made before the locks; an empty one equals
        // the request's, so copying it again changes nothing.
        if (!args[a].empty()) {
          // A lent copy's original was moved into the effect.
          ABYSS_DCHECK(args[a] == effect.cmd.args[a], "a lent copy differs from its request arg");
          continue;
        }
        args[a] = effect.cmd.args[a];
        if (IsValueArg(effect.cmd, a)) copied += args[a].size();
      }
    }
    if (copied > 0) {
      locked_copy_bytes_.fetch_add(copied, std::memory_order_relaxed);
      locked_copy_bytes_metric_.Increment(static_cast<double>(copied));
    }
    // Each slot's run of entries, and where it starts.
    std::array<queue::ShardEntries, 1> one_part;
    std::vector<queue::ShardEntries> many_parts;
    std::vector<size_t> run_start;
    std::span<const queue::ShardEntries> parts;
    if (order.empty()) {
      one_part[0] = {.shard = shards.front(), .entries = entries};
      parts = one_part;
    } else {
      for (size_t i = 0; i < count; ++i) {
        if (i > 0 && slot_at(i) == slot_at(i - 1)) continue;
        size_t end = i + 1;
        while (end < count && slot_at(end) == slot_at(i)) ++end;
        many_parts.push_back(
            {.shard = shards[slot_at(i)], .entries = std::span(entries).subspan(i, end - i)});
        run_start.push_back(i);
      }
      parts = many_parts;
    }

    phases.Lap(WritePhase::kEntries);
    auto reserved = queue_.Reserve(parts);
    phases.Lap(WritePhase::kReserve);
    if (!reserved.has_value()) {
      // Nothing was taken from the entries: the copies go back for the
      // next attempt.
      for (const Moved& moved : decision.moved) {
        if (spare.empty() || !spare[moved.request_arg].lent) continue;
        PendingWrite::Spare& copy = spare[moved.request_arg];
        copy.copy = std::move(args_at(entry_of(moved.effect))[moved.arg]);
        copy.lent = false;
      }
      Restore(std::move(decision), cmd);
      hold->Unlock();
      const core::Error& error = reserved.error();
      switch (error.code()) {
        case core::ErrorCode::kResourceExhausted:
          Again(write, RedecideReason::kAdmission);
          write.admit_next_ = 0;
          write.phase_ = Phase::kAdmit;
          continue;
        case core::ErrorCode::kUnavailable:
          Again(write, RedecideReason::kSpare);
          write.refusal_ = error;
          write.spare_next_ = 0;
          write.phase_ = Phase::kSpare;
          continue;
        case core::ErrorCode::kInvalidArgument:
          if (error.message().starts_with("CROSSSLOT")) return Done(write, CrossSlot());
          return Done(write, std::unexpected(error));
        default:
          // kValueTooLarge never fits; anything else is not retried.
          return Done(write, std::unexpected(error));
      }
    }

    queue::Reservation reservation = *std::move(reserved);
    for (const core::ShardId shard : shards) (*hold)->RaiseAppendedAt(shard, instant);
    // The reply of the last effect, a single-key command's own.
    std::optional<core::RespValue> own;
    if (order.empty()) {
      const queue::ReservedRange& range = reservation.ranges().front();
      auto replies = (*hold)->ApplyEffects(range.shard, decision.effects, range.first, instant);
      own = std::move(replies.back());
    } else {
      std::vector<core::Effect> sorted;
      sorted.reserve(count);
      for (const uint32_t e : order) sorted.push_back(std::move(decision.effects[e]));
      const size_t last = entry_of(count - 1);
      for (const queue::ReservedRange& range : reservation.ranges()) {
        const auto run = std::ranges::find(many_parts, range.shard, &queue::ShardEntries::shard);
        const size_t from = run_start[static_cast<size_t>(run - many_parts.begin())];
        auto replies =
            (*hold)->ApplyEffects(range.shard, std::span(sorted).subspan(from, run->entries.size()),
                                  range.first, instant);
        if (last >= from && last < from + replies.size()) own = std::move(replies[last - from]);
      }
    }
    // Freed once published: a large free would delay this publish, and
    // the next writer's on the shard waits on it.
    hot::Graveyard replaced = hold->Unlock();
    phases.Lap(WritePhase::kApply);

    write.ranges_ = queue_.Complete(std::move(reservation));
    replaced = {};
    phases.Lap(WritePhase::kComplete);
    if (decision.reply.has_value()) {
      write.reply_ = *std::move(decision.reply);
    } else {
      ABYSS_DCHECK(own.has_value(), "no reply for the last effect");
      write.reply_ = *std::move(own);
    }
    write.durable_kind_ = PendingWrite::DurableKind::kWrite;
    write.record_phases_ = true;
    write.phase_ = Phase::kDurable;
    return AwaitDurable(write);
  }
}

Step Sequencer::FlushAttempt(PendingWrite& write) {
  using Phase = PendingWrite::Phase;
  std::vector<core::ShardId>& shards = write.shards_;
  if (write.phase_ == Phase::kStart) {
    shards.resize(hot_.shard_count());
    for (core::ShardId shard = 0; shard < shards.size(); ++shard) shards[shard] = shard;
    write.phase_ = Phase::kAdmit;
  }
  for (;;) {
    if (write.phase_ == Phase::kAdmit) {
      if (auto step = AwaitAdmission(write)) return *std::move(step);
      write.phase_ = Phase::kTop;
    }
    if (write.phase_ == Phase::kSpare) {
      if (auto step = AwaitSpares(write)) return *std::move(step);
      write.phase_ = Phase::kTop;
    }
    if (write.redecided_.has_value() && core::SteadyClock::now() >= write.deadline_) {
      return Done(write, TimedOut(*write.redecided_));
    }
    // Built before the hold, which only swaps them in; built per pass,
    // so no pass is handed one already moved from.
    std::vector<hot::FreshMaps> fresh(hot_.shard_count());
    for (const core::ShardId shard : shards) fresh[shard] = hot_.ReservedMaps(shard);
    const core::WallTime wall = config_.wall_clock();
    Hold hold(*this, shards, Sampled());
    core::WallTime instant = wall;
    for (const core::ShardId shard : shards) {
      instant = std::max(instant, hold->LastAppendedAt(shard));
    }
    std::vector<core::QueueEntry> entries(shards.size());
    std::vector<queue::ShardEntries> parts;
    parts.reserve(shards.size());
    for (size_t slot = 0; slot < shards.size(); ++slot) {
      entries[slot] = core::QueueEntry{.appended_at = instant, .payload = core::entry::Flush{}};
      parts.push_back({.shard = shards[slot], .entries = std::span(&entries[slot], 1)});
    }
    // With several logs each log's Flushes are one batch: atomic to
    // readers, who hold these locks, but not across a crash.
    auto reserved = queue_.ReserveFlush(parts);
    if (!reserved.has_value()) {
      hold.Unlock();
      const core::Error& error = reserved.error();
      if (error.code() == core::ErrorCode::kResourceExhausted) {
        Again(write, RedecideReason::kAdmission);
        write.admit_next_ = 0;
        write.phase_ = Phase::kAdmit;
        continue;
      }
      if (error.code() == core::ErrorCode::kUnavailable) {
        Again(write, RedecideReason::kSpare);
        write.refusal_ = error;
        write.spare_next_ = 0;
        write.phase_ = Phase::kSpare;
        continue;
      }
      ABYSS_LOG_ERROR("flush append failed", {"err", std::string_view{error.message()}});
      return Done(write, std::unexpected(error));
    }
    queue::Reservation reservation = *std::move(reserved);
    for (const core::ShardId shard : shards) hold->RaiseAppendedAt(shard, instant);
    for (const queue::ReservedRange& range : reservation.ranges()) {
      hold->Wipe(range.shard, range.first, std::move(fresh[range.shard]));
    }
    hot::Graveyard wiped = hold.Unlock();

    write.ranges_ = queue_.Complete(std::move(reservation));
    wiped = {};
    write.reply_ = core::RespValue::SimpleString("OK");
    write.durable_kind_ = PendingWrite::DurableKind::kFlush;
    write.record_phases_ = false;
    write.phase_ = Phase::kDurable;
    return AwaitDurable(write);
  }
}

void Sequencer::Start(PendingWrite& write) {
  PendingWrite::PhaseTimes& times = write.phase_times_;
  times.sampled = Sampled();
  if (times.sampled) times.last = std::chrono::steady_clock::now();
}

std::optional<Step> Sequencer::AwaitAdmission(PendingWrite& write) {
  for (; write.admit_next_ < write.shards_.size(); ++write.admit_next_) {
    const core::ShardId shard = write.shards_[write.admit_next_];
    std::optional<core::Result<void>> admitted;
    if (std::optional<core::Result<void>> wake = write.TakeWake()) {
      if (wake->has_value()) {
        // Checked again, and a pass hands on to the next waiter.
        admitted = queue_.WhenReady(core::QueueWait::kAdmission, shard, write, /*first=*/false);
      } else {
        queue_.FinishWait(core::QueueWait::kAdmission, shard, write);
        admitted = std::move(wake);
      }
    } else if (queue_.Admissible(shard)) {
      continue;
    } else if (core::SteadyClock::now() >= write.deadline_) {
      return Done(write, std::unexpected(core::Error{core::ErrorCode::kResourceExhausted,
                                                     "WAL durability window full"}));
    } else {
      admitted = queue_.WhenReady(core::QueueWait::kAdmission, shard, write, /*first=*/true);
    }
    if (!admitted.has_value()) return Waiting(write, WaitReason::kAdmission, shard);
    if (!admitted->has_value()) return Done(write, std::unexpected(admitted->error()));
  }
  return std::nullopt;
}

std::optional<Step> Sequencer::AwaitSpares(PendingWrite& write) {
  // A write's shards share a log; a flush waits on each of its logs.
  const size_t end = write.flush_ ? write.shards_.size() : 1;
  for (; write.spare_next_ < end; ++write.spare_next_) {
    const core::ShardId shard = write.shards_[write.spare_next_];
    std::optional<core::Result<void>> ready;
    if (std::optional<core::Result<void>> wake = write.TakeWake()) {
      if (wake->has_value()) {
        ready = queue_.WhenReady(core::QueueWait::kSpare, shard, write, /*first=*/false);
      } else {
        queue_.FinishWait(core::QueueWait::kSpare, shard, write);
        ready = std::move(wake);
      }
    } else {
      if (write.flush_) {
        const uint32_t log = queue_.LogOf(shard);
        if (std::ranges::find(write.spare_logs_, log) != write.spare_logs_.end()) continue;
        write.spare_logs_.push_back(log);
      }
      ready = queue_.WhenReady(core::QueueWait::kSpare, shard, write, /*first=*/true);
    }
    if (!ready.has_value()) return Waiting(write, WaitReason::kSpare, shard);
    if (!ready->has_value()) {
      if (core::SteadyClock::now() >= write.deadline_) {
        return Done(write, std::unexpected(SpareTimedOut(write.flush_)));
      }
      // As before the wait: the refusal that sent it, shutting down.
      return Done(write, std::unexpected(write.refusal_.value_or(ready->error())));
    }
  }
  write.refusal_.reset();
  write.spare_logs_.clear();
  return std::nullopt;
}

std::optional<Step> Sequencer::AwaitMemory(PendingWrite& write) {
  for (; write.memory_next_ < write.shards_.size(); ++write.memory_next_) {
    const core::ShardId shard = write.shards_[write.memory_next_];
    // A woken wait not yet ended, which a pass must hand on.
    bool woken = false;
    if (std::optional<core::Result<void>> wake = write.TakeWake()) {
      if (!wake->has_value()) {
        buffers_.FinishDrainWait(shard, write);
        return Done(write, std::unexpected(wake->error()));
      }
      woken = true;
    }
    for (;;) {
      // Eviction takes only what cold has drained, so a wait is for the
      // drain to move past what this one could take.
      const core::SequenceId past = hot_.Drained(shard);
      if (hot_.EvictShardToTarget(shard)) break;
      if (!write.memory_waited_) {
        write.memory_waited_ = true;
        backpressure_waits_.fetch_add(1, std::memory_order_relaxed);
        backpressure_waits_metric_.Increment();
      }
      if (core::SteadyClock::now() >= write.deadline_) {
        if (woken) buffers_.FinishDrainWait(shard, write);
        backpressure_rejections_.fetch_add(1, std::memory_order_relaxed);
        backpressure_rejections_metric_.Increment();
        return Done(write, OutOfMemory());
      }
      auto room = buffers_.WhenDrainedPast(shard, past, write);
      if (!room.has_value()) return Waiting(write, WaitReason::kBackpressure, shard);
      // The wait ended there, and a woken one that passed handed on.
      woken = false;
      if (!room->has_value()) return Done(write, std::unexpected(room->error()));
    }
    if (woken) buffers_.FinishDrainWait(shard, write);
  }
  write.memory_next_ = 0;
  write.memory_waited_ = false;
  return std::nullopt;
}

std::optional<Step> Sequencer::AwaitForeignLoads(PendingWrite& write) {
  for (; write.foreign_next_ < write.foreign_.size(); ++write.foreign_next_) {
    if (std::optional<core::Result<void>> wake = write.TakeWake()) {
      if (!wake->has_value()) {
        AbortLoads(write);
        return Done(write, std::unexpected(wake->error()));
      }
      continue;
    }
    const auto now = hot_.WhenLoaded(write.foreign_[write.foreign_next_], write.load_wake_);
    if (!now.has_value()) return Waiting(write, WaitReason::kForeignLoad);
    if (!now->has_value()) {
      AbortLoads(write);
      return Done(write, std::unexpected(now->error()));
    }
  }
  return std::nullopt;
}

// A decision with effects needs no fence: within one log its frames
// follow everything it observed, and Complete's AwaitFilled and each
// class's durable end are prefix-ordered by position. CROSSSLOT keeps
// every multi-shard decision within one log.
Step Sequencer::AwaitDurable(PendingWrite& write) {
  const core::Durability ack = queue_.AckDurability();
  if (write.waiting_) {
    write.waiting_ = false;
    if (write.expired_) return DurableTimedOut(write);
    std::optional<core::Result<void>> wake = write.TakeWake();
    ABYSS_DCHECK(wake.has_value(), "a durable wait resumed before its post");
    write.node_.reset();
    if (!wake->has_value()) return DurableFailed(write, wake->error());
    ++write.durable_next_;
  }
  for (; write.durable_next_ < write.DurableCount(); ++write.durable_next_) {
    const ShardSeq at = write.DurableAt(write.durable_next_);
    core::SeqWaiterNode& node = write.node_.emplace(at.seq, write.completion_);
    auto durable = queue_.WhenDurable(at.shard, ack, node);
    if (!durable.has_value()) {
      // Past the deadline it was checked once more; a lost Cancel
      // means its post is on the way.
      if (core::SteadyClock::now() < write.deadline_ ||
          !queue_.CancelDurable(at.shard, ack, node)) {
        write.waiting_ = true;
        return Step{.kind = Step::Kind::kSequenced};
      }
      write.node_.reset();
      return DurableTimedOut(write);
    }
    write.node_.reset();
    if (!durable->has_value()) return DurableFailed(write, durable->error());
  }
  if (write.record_phases_) {
    Phases phases(*this, write.phase_times_);
    phases.Lap(WritePhase::kDurable);
    phases.Record();
  }
  return Done(write, std::move(write.reply_));
}

Step Sequencer::Waiting(PendingWrite& write, WaitReason reason, core::ShardId shard) {
  write.waiting_ = true;
  write.wait_reason_ = reason;
  write.wait_shard_ = shard;
  Step step{.kind = Step::Kind::kWait, .reason = reason, .shard = shard};
  if (reason == WaitReason::kForeignLoad) step.key = write.foreign_.at(write.foreign_next_);
  return step;
}

Step Sequencer::Done(PendingWrite& write, core::Result<core::RespValue> reply) {
  ABYSS_DCHECK(write.owned_.empty(), "a write replied with a load open");
  ABYSS_DCHECK(!write.node_.has_value(), "a write replied with a durable wait open");
  write.phase_ = PendingWrite::Phase::kDone;
  write.waiting_ = false;
  write.spare_.clear();
  return Step{.kind = Step::Kind::kReplied, .reply = std::move(reply)};
}

Step Sequencer::Abandon(PendingWrite& write) {
  const std::optional<core::Result<void>> wake = write.TakeWake();
  const bool woken = wake.has_value();
  const bool failed = woken && !wake->has_value();
  const core::ShardId shard = write.wait_shard_;
  write.waiting_ = false;
  write.expired_ = false;
  switch (write.wait_reason_) {
    case WaitReason::kAdmission:
      if (woken) queue_.FinishWait(core::QueueWait::kAdmission, shard, write);
      if (failed) return Done(write, std::unexpected(wake->error()));
      return Done(write, std::unexpected(queue_.AdmissionTimedOut(shard)));
    case WaitReason::kSpare:
      if (woken) queue_.FinishWait(core::QueueWait::kSpare, shard, write);
      if (failed) return Done(write, std::unexpected(wake->error()));
      return Done(write, std::unexpected(SpareTimedOut(write.flush_)));
    case WaitReason::kBackpressure:
      if (woken) buffers_.FinishDrainWait(shard, write);
      if (failed) return Done(write, std::unexpected(wake->error()));
      backpressure_rejections_.fetch_add(1, std::memory_order_relaxed);
      backpressure_rejections_metric_.Increment();
      return Done(write, OutOfMemory());
    case WaitReason::kForeignLoad:
      AbortLoads(write);
      if (failed) return Done(write, std::unexpected(wake->error()));
      return Done(write, std::unexpected(core::Error{core::ErrorCode::kTimeout,
                                                     std::string(kForeignLoadTimeout)}));
    case WaitReason::kLoads:
      if (write.load_error_.has_value()) {
        core::Error error = *std::move(write.load_error_);
        AbortLoads(write);
        return Done(write, std::unexpected(std::move(error)));
      }
      InstallLoads(write);
      return Done(write, TimedOut(RedecideReason::kLoad));
  }
  return Done(write, TimedOut(RedecideReason::kLoad));
}

void Sequencer::AbortLoads(PendingWrite& write) {
  for (const PendingWrite::Owned& load : write.owned_) hot_.AbortLoad(load.key, load.token);
  ClearLoads(write);
}

// What an abandoned write loaded still installs: its tokens judge it
// as they would any load's.
void Sequencer::InstallLoads(PendingWrite& write) {
  for (const core::ShardId shard : write.shards_) {
    std::vector<hot::LoadCompletion> mine;
    for (size_t i = 0; i < write.completions_.size(); ++i) {
      if (write.owned_[i].shard == shard) mine.push_back(std::move(write.completions_[i]));
    }
    if (!mine.empty()) hot_.CompleteLoads(shard, mine);
  }
  for (size_t i = write.completions_.size(); i < write.owned_.size(); ++i) {
    hot_.AbortLoad(write.owned_[i].key, write.owned_[i].token);
  }
  ClearLoads(write);
}

void Sequencer::ClearLoads(PendingWrite& write) {
  write.owned_.clear();
  write.foreign_.clear();
  write.foreign_next_ = 0;
  write.full_load_ = false;
  write.completions_.clear();
  write.load_error_.reset();
}

Step Sequencer::DurableTimedOut(PendingWrite& write) {
  write.expired_ = false;
  const ShardSeq at = write.DurableAt(write.durable_next_);
  if (write.durable_kind_ == PendingWrite::DurableKind::kFence) {
    ABYSS_LOG_WARN("fence durable wait timeout", {"shard", static_cast<int64_t>(at.shard)},
                   {"seq", static_cast<uint64_t>(at.seq)});
    return Done(
        write, std::unexpected(core::Error{core::ErrorCode::kTimeout, std::string(kFenceTimeout)}));
  }
  const bool flush = write.durable_kind_ == PendingWrite::DurableKind::kFlush;
  ABYSS_LOG_WARN(flush ? "flush durable wait timeout" : "write durable wait timeout",
                 {"shard", static_cast<int64_t>(at.shard)}, {"seq", static_cast<uint64_t>(at.seq)},
                 {"timeout_ms", static_cast<int64_t>(config_.write_timeout.count())});
  return Done(write, std::unexpected(core::Error{
                         core::ErrorCode::kTimeout,
                         std::string(flush ? kFlushDurableTimeout : kWriteDurableTimeout)}));
}

Step Sequencer::DurableFailed(PendingWrite& write, const core::Error& error) {
  if (write.durable_kind_ != PendingWrite::DurableKind::kFence) {
    const ShardSeq at = write.DurableAt(write.durable_next_);
    ABYSS_LOG_ERROR(
        write.durable_kind_ == PendingWrite::DurableKind::kFlush ? "flush durable failed"
                                                                 : "write durable failed",
        {"shard", static_cast<int64_t>(at.shard)}, {"seq", static_cast<uint64_t>(at.seq)},
        {"err", std::string_view{error.message()}});
  }
  return Done(write, std::unexpected(error));
}

void Sequencer::Again(PendingWrite& write, RedecideReason reason) {
  Redecided(reason);
  write.redecided_ = reason;
}

core::Result<void> Sequencer::Fence(std::span<const ShardSeq> fences, core::SteadyTime deadline) {
  for (const ShardSeq& fence : fences) {
    const auto now = core::SteadyClock::now();
    const auto timeout =
        now < deadline ? std::chrono::ceil<core::Duration>(deadline - now) : core::Duration::zero();
    auto durable = queue_.AwaitDurable(fence.shard, fence.seq, queue_.AckDurability(), timeout);
    if (!durable.has_value()) return std::unexpected(durable.error());
    if (!*durable) {
      ABYSS_LOG_WARN("fence durable wait timeout", {"shard", static_cast<int64_t>(fence.shard)},
                     {"seq", static_cast<uint64_t>(fence.seq)});
      return std::unexpected(core::Error{core::ErrorCode::kTimeout, std::string(kFenceTimeout)});
    }
  }
  return {};
}

void Sequencer::Redecided(RedecideReason reason) {
  const auto index = static_cast<size_t>(reason);
  redecides_.at(index).fetch_add(1, std::memory_order_relaxed);
  redecides_metric_.at(index).Increment();
}

SequencerStats Sequencer::Snapshot() const {
  SequencerStats stats{
      .locked_copy_bytes = locked_copy_bytes_.load(std::memory_order_relaxed),
      .backpressure_waits = backpressure_waits_.load(std::memory_order_relaxed),
      .backpressure_rejections = backpressure_rejections_.load(std::memory_order_relaxed),
  };
  for (size_t i = 0; i < stats.redecides.size(); ++i) {
    stats.redecides.at(i) = redecides_.at(i).load(std::memory_order_relaxed);
  }
  return stats;
}

}  // namespace abyss::engine
