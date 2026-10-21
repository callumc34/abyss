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

// A sampled write's time in each stage. Each lap charges the time
// since the last to one stage; a stage passed twice adds up.
class Sequencer::Phases {
 public:
  explicit Phases(Sequencer& sequencer)
      : sequencer_(sequencer),
        sampled_(Sampled()),
        last_(sampled_ ? std::chrono::steady_clock::now()
                       : std::chrono::steady_clock::time_point{}) {}

  bool sampled() const { return sampled_; }

  void Lap(WritePhase phase) {
    if (!sampled_) return;
    const auto now = std::chrono::steady_clock::now();
    const auto index = static_cast<size_t>(phase);
    spent_.at(index) += now - last_;
    passed_.at(index) = true;
    last_ = now;
  }

  // Once the write succeeded: the stages it passed.
  void Record() const {
    if (!sampled_) return;
    for (size_t i = 0; i < spent_.size(); ++i) {
      if (!passed_.at(i)) continue;
      sequencer_.phase_seconds_.at(i).Observe(std::chrono::duration<double>(spent_.at(i)).count());
    }
  }

 private:
  Sequencer& sequencer_;
  bool sampled_;
  std::chrono::steady_clock::time_point last_;
  std::array<std::chrono::steady_clock::duration, metrics::kWritePhaseCount> spent_{};
  std::array<bool, metrics::kWritePhaseCount> passed_{};
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

// Deadlock-free: hot shard locks are taken ascending, then each
// stream's append_mu_ inside Reserve, and nothing waits under a hot
// lock: admission, spare segments, loads, cold drain and durability
// are all waited for with every lock released.
core::Result<core::RespValue> Sequencer::Execute(core::RespCommand cmd,
                                                 core::PredicateFlags flags) {
  Phases phases(*this);
  const core::SteadyTime deadline = core::SteadyClock::now() + config_.write_timeout;
  auto keys = WriteKeys(cmd);
  if (!keys.has_value()) return std::unexpected(keys.error());
  std::vector<core::ShardId> shards;
  shards.reserve(keys->size());
  for (const std::string_view key : *keys) shards.push_back(hot_.ShardOf(key));
  std::ranges::sort(shards);
  shards.erase(std::ranges::unique(shards).begin(), shards.end());
  if (std::ranges::any_of(shards, [&](core::ShardId shard) {
        return queue_.LogOf(shard) != queue_.LogOf(shards.front());
      })) {
    return CrossSlot();
  }

  // Large arguments are copied for the log before any lock, once; a
  // decision moves the request's own strings into hot. `lent` marks a
  // copy an attempt's log entry holds, to take back if it fails.
  struct Spare {
    std::optional<std::string> copy;
    bool lent = false;
  };
  std::vector<Spare> spare;
  size_t total = 0;
  for (size_t i = 1; i < cmd.args.size(); ++i) total += cmd.args[i].size();
  for (size_t i = 1; i < cmd.args.size(); ++i) {
    if (total > core::kLockHoldFrameBytes || cmd.args[i].size() > core::kLockHoldFrameBytes) {
      if (spare.empty()) spare.resize(cmd.args.size());
      spare[i].copy = cmd.args[i];
    }
  }

  for (const core::ShardId shard : shards) {
    if (auto admitted = queue_.Admit(shard, deadline); !admitted) {
      return std::unexpected(admitted.error());
    }
  }
  // Set once a hold finds a shard over its memory limit.
  bool await_memory = false;
  const bool grows = GrowsMemory(cmd);
  std::optional<RedecideReason> redecided;
  const auto again = [this, &redecided](RedecideReason reason) {
    Redecided(reason);
    redecided = reason;
  };
  for (;;) {
    // Every way round is bounded by the deadline, a re-check under the
    // locks that keeps failing after its wait succeeded included.
    if (redecided.has_value() && core::SteadyClock::now() >= deadline) {
      if (*redecided == RedecideReason::kBackpressure) {
        backpressure_rejections_.fetch_add(1, std::memory_order_relaxed);
        backpressure_rejections_metric_.Increment();
      }
      return TimedOut(*redecided);
    }
    if (await_memory) {
      if (auto memory = AwaitMemory(shards, deadline); !memory) {
        if (memory.error().code() == core::ErrorCode::kResourceExhausted) return OutOfMemory();
        return std::unexpected(memory.error());
      }
    }
    // A shard's appended_at never goes backwards: cold expires by it.
    // One instant per hold is decide's now and every entry's stamp.
    core::WallTime wall = config_.wall_clock();
    std::optional<Hold> hold;
    phases.Lap(WritePhase::kAdmit);
    hold.emplace(*this, shards, phases.sampled());
    phases.Lap(WritePhase::kLockWait);
    const auto instant_of = [&shards, &hold](core::WallTime from) {
      for (const core::ShardId shard : shards) {
        from = std::max(from, (*hold)->LastAppendedAt(shard));
      }
      return from;
    };
    core::WallTime instant = instant_of(wall);
    uint64_t now_ms = WallMs(instant);
    const KeyLookup lookup = [&hold, &now_ms](std::string_view key) {
      return (*hold)->View(key, now_ms);
    };
    Decision decision = Decide(cmd, flags, now_ms, lookup);
    phases.Lap(WritePhase::kDecide);
    // Memory judged before this hold's loads went in: what they add is
    // the decision's own keys, which an eviction would only send to
    // load again.
    bool judged = false;

    if (!decision.needs_load.empty()) {
      struct Owned {
        std::string key;
        core::ShardId shard;
        Need need;
        hot::LoadToken token;
      };
      std::vector<Owned> owned;
      std::vector<std::string> foreign;
      bool full_load = false;
      for (KeyLoad& load : decision.needs_load) {
        const hot::LoadStart start = (*hold)->BeginLoad(load.key);
        if (start.started()) {
          full_load = full_load || load.need == Need::kState;
          const core::ShardId shard = hot_.ShardOf(load.key);
          owned.push_back({.key = std::move(load.key),
                           .shard = shard,
                           .need = load.need,
                           .token = start.token});
        } else {
          // Decide found the key unknown in this same hold, so it can
          // only be another's load.
          ABYSS_DCHECK(start.status == hot::LoadStart::Status::kPending,
                       "a key decide could not see is resident or flushed");
          foreign.push_back(std::move(load.key));
        }
      }
      hold->Unlock();
      const auto abort = [this, &owned] {
        for (const Owned& load : owned) hot_.AbortLoad(load.key, load.token);
      };
      for (const std::string& key : foreign) {
        if (!hot_.AwaitLoad(key, deadline)) {
          abort();
          return std::unexpected(
              core::Error{core::ErrorCode::kTimeout, "write timed out awaiting a load of its key"});
        }
      }
      std::vector<hot::LoadCompletion> completions;
      completions.reserve(owned.size());
      for (const Owned& load : owned) {
        auto result = loader_.Load(load.shard, load.key, load.need, deadline);
        if (!result.has_value()) {
          abort();
          return std::unexpected(result.error());
        }
        completions.push_back({.key = load.key, .token = load.token, .result = *std::move(result)});
      }

      wall = config_.wall_clock();
      hold.reset();
      phases.Lap(WritePhase::kLoad);
      hold.emplace(*this, shards, phases.sampled());
      phases.Lap(WritePhase::kLockWait);
      // A full load grows memory however the command reads. A write
      // that grows is judged here, before its own keys go in: a TTL
      // load of a string brings its value too.
      if ((full_load || grows) && std::ranges::any_of(shards, [&hold](core::ShardId shard) {
            return (*hold)->OverBackpressure(shard);
          })) {
        for (const Owned& load : owned) (*hold)->AbortLoad(load.key, load.token);
        hold->Unlock();
        again(RedecideReason::kBackpressure);
        await_memory = true;
        continue;
      }
      judged = full_load || grows;
      for (const core::ShardId shard : shards) {
        std::vector<hot::LoadCompletion> mine;
        for (size_t i = 0; i < owned.size(); ++i) {
          if (owned[i].shard == shard) mine.push_back(std::move(completions[i]));
        }
        if (!mine.empty()) (*hold)->CompleteLoads(shard, mine);
      }
      phases.Lap(WritePhase::kLoad);
      instant = instant_of(wall);
      now_ms = WallMs(instant);
      decision = Decide(cmd, flags, now_ms, lookup);
      phases.Lap(WritePhase::kDecide);
      if (!decision.needs_load.empty()) {
        // A blind write overtook a load.
        hold->Unlock();
        again(RedecideReason::kLoad);
        continue;
      }
    }

    if (decision.error.has_value() || decision.effects.empty()) {
      // Nothing to log: the reply, an error included, still waits until
      // what it read is durable.
      hold->Unlock();
      if (auto fenced = Fence(decision.observed, deadline); !fenced) {
        return std::unexpected(fenced.error());
      }
      if (decision.error.has_value()) return std::unexpected(*std::move(decision.error));
      ABYSS_DCHECK(decision.reply.has_value(), "a decision with no effect has no reply");
      phases.Lap(WritePhase::kDurable);
      phases.Record();
      return *std::move(decision.reply);
    }
    if (grows && !judged && std::ranges::any_of(shards, [&hold](core::ShardId shard) {
          return (*hold)->OverBackpressure(shard);
        })) {
      Restore(std::move(decision), cmd);
      hold->Unlock();
      again(RedecideReason::kBackpressure);
      await_memory = true;
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
        Spare& copy = spare[moved.request_arg];
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
        Spare& copy = spare[moved.request_arg];
        copy.copy = std::move(args_at(entry_of(moved.effect))[moved.arg]);
        copy.lent = false;
      }
      Restore(std::move(decision), cmd);
      hold->Unlock();
      const core::Error& error = reserved.error();
      switch (error.code()) {
        case core::ErrorCode::kResourceExhausted:
          again(RedecideReason::kAdmission);
          for (const core::ShardId shard : shards) {
            if (auto admitted = queue_.Admit(shard, deadline); !admitted) {
              return std::unexpected(admitted.error());
            }
          }
          continue;
        case core::ErrorCode::kUnavailable:
          again(RedecideReason::kSpare);
          if (!queue_.WaitForSpare(shards.front(), deadline)) {
            if (core::SteadyClock::now() >= deadline) {
              return std::unexpected(core::Error{
                  core::ErrorCode::kTimeout, "write timed out waiting for a spare WAL segment"});
            }
            return std::unexpected(error);
          }
          continue;
        case core::ErrorCode::kInvalidArgument:
          if (error.message().starts_with("CROSSSLOT")) return CrossSlot();
          return std::unexpected(error);
        default:
          // kValueTooLarge never fits; anything else is not retried.
          return std::unexpected(error);
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

    const std::vector<queue::ReservedRange> ranges = queue_.Complete(std::move(reservation));
    replaced = {};
    phases.Lap(WritePhase::kComplete);
    if (auto durable = AwaitDurable(ranges, deadline, /*flush=*/false); !durable) {
      return std::unexpected(durable.error());
    }
    phases.Lap(WritePhase::kDurable);
    phases.Record();
    if (decision.reply.has_value()) return *std::move(decision.reply);
    ABYSS_DCHECK(own.has_value(), "no reply for the last effect");
    return *std::move(own);
  }
}

core::Result<core::RespValue> Sequencer::Flush() {
  const core::SteadyTime deadline = core::SteadyClock::now() + config_.write_timeout;
  std::vector<core::ShardId> shards(hot_.shard_count());
  for (core::ShardId shard = 0; shard < shards.size(); ++shard) shards[shard] = shard;
  for (const core::ShardId shard : shards) {
    if (auto admitted = queue_.Admit(shard, deadline); !admitted) {
      return std::unexpected(admitted.error());
    }
  }
  std::optional<RedecideReason> redecided;
  for (;;) {
    if (redecided.has_value() && core::SteadyClock::now() >= deadline) {
      return TimedOut(*redecided);
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
        Redecided(RedecideReason::kAdmission);
        redecided = RedecideReason::kAdmission;
        for (const core::ShardId shard : shards) {
          if (auto admitted = queue_.Admit(shard, deadline); !admitted) {
            return std::unexpected(admitted.error());
          }
        }
        continue;
      }
      if (error.code() == core::ErrorCode::kUnavailable) {
        Redecided(RedecideReason::kSpare);
        redecided = RedecideReason::kSpare;
        std::vector<uint32_t> waited;
        for (const core::ShardId shard : shards) {
          if (std::ranges::find(waited, queue_.LogOf(shard)) != waited.end()) continue;
          waited.push_back(queue_.LogOf(shard));
          if (!queue_.WaitForSpare(shard, deadline)) {
            if (core::SteadyClock::now() >= deadline) {
              return std::unexpected(core::Error{
                  core::ErrorCode::kTimeout, "flush timed out waiting for a spare WAL segment"});
            }
            return std::unexpected(error);
          }
        }
        continue;
      }
      ABYSS_LOG_ERROR("flush append failed", {"err", std::string_view{error.message()}});
      return std::unexpected(error);
    }
    queue::Reservation reservation = *std::move(reserved);
    for (const core::ShardId shard : shards) hold->RaiseAppendedAt(shard, instant);
    for (const queue::ReservedRange& range : reservation.ranges()) {
      hold->Wipe(range.shard, range.first, std::move(fresh[range.shard]));
    }
    hot::Graveyard wiped = hold.Unlock();

    const std::vector<queue::ReservedRange> ranges = queue_.Complete(std::move(reservation));
    wiped = {};
    if (auto durable = AwaitDurable(ranges, deadline, /*flush=*/true); !durable) {
      return std::unexpected(durable.error());
    }
    return core::RespValue::SimpleString("OK");
  }
}

// A decision with effects needs no fence: within one log its frames
// follow everything it observed, and Complete's AwaitFilled and each
// class's durable end are prefix-ordered by position. CROSSSLOT keeps
// every multi-shard decision within one log.
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

core::Result<void> Sequencer::AwaitMemory(std::span<const core::ShardId> shards,
                                          core::SteadyTime deadline) {
  bool waited = false;
  for (const core::ShardId shard : shards) {
    // Eviction takes only what cold has drained, so each slice waits
    // for the drain to move on.
    while (!hot_.EvictShardToTarget(shard)) {
      if (!waited) {
        waited = true;
        backpressure_waits_.fetch_add(1, std::memory_order_relaxed);
        backpressure_waits_metric_.Increment();
      }
      const auto now = core::SteadyClock::now();
      if (now >= deadline) {
        backpressure_rejections_.fetch_add(1, std::memory_order_relaxed);
        backpressure_rejections_metric_.Increment();
        return std::unexpected(core::Error{core::ErrorCode::kResourceExhausted,
                                           "hot memory over its limit and cold is behind"});
      }
      const auto slice =
          std::min<core::SteadyClock::duration>(config_.backpressure_wait_slice, deadline - now);
      buffers_.WaitForDrainedSeq(shard, hot_.Drained(shard) + 1,
                                 std::chrono::ceil<std::chrono::milliseconds>(slice));
    }
  }
  return {};
}

core::Result<void> Sequencer::AwaitDurable(std::span<const queue::ReservedRange> ranges,
                                           core::SteadyTime deadline, bool flush) const {
  for (const queue::ReservedRange& range : ranges) {
    const auto now = core::SteadyClock::now();
    const auto timeout =
        now < deadline ? std::chrono::ceil<core::Duration>(deadline - now) : core::Duration::zero();
    auto durable = queue_.AwaitDurable(range.shard, range.last, queue_.AckDurability(), timeout);
    if (!durable.has_value()) {
      ABYSS_LOG_ERROR(flush ? "flush durable failed" : "write durable failed",
                      {"shard", static_cast<int64_t>(range.shard)},
                      {"seq", static_cast<uint64_t>(range.last)},
                      {"err", std::string_view{durable.error().message()}});
      return std::unexpected(durable.error());
    }
    if (!*durable) {
      ABYSS_LOG_WARN(flush ? "flush durable wait timeout" : "write durable wait timeout",
                     {"shard", static_cast<int64_t>(range.shard)},
                     {"seq", static_cast<uint64_t>(range.last)},
                     {"timeout_ms", static_cast<int64_t>(config_.write_timeout.count())});
      return std::unexpected(
          core::Error{core::ErrorCode::kTimeout,
                      std::string(flush ? kFlushDurableTimeout : kWriteDurableTimeout)});
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
