// The three-way differential property test (ADP-015): a seeded random
// command stream over a small, overlapping keyspace, run
// through the server's request pipeline, interleaved with eviction
// pressure, cold absorbs and flushes, loads raced by evictions, flushes
// and blind writes, segment starvation and restarts. After every
// command its reply is compared with a reference model, and each key it
// touched is compared in hot (resident) or in buffer plus cold (not
// resident). At the end every key is compared through hot, the read
// path, and RocksDB opened alone once everything has drained.
//
// Each seed also fixes the configuration, swarm style. A failure prints
// both; ABYSS_PROPERTY_SEED=<n> replays that seed alone. The long tier
// is opt-in: ABYSS_PROPERTY_SEEDS=<count> ABYSS_PROPERTY_OPS=<ops>.

#include <gtest/gtest.h>

#ifdef ABYSS_HAVE_ROCKSDB

#include <algorithm>
#include <array>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <deque>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <map>
#include <optional>
#include <set>
#include <sstream>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "abyss/cold/backends/rocksdb_store.h"
#include "abyss/core/ascii.h"
#include "abyss/metrics/names.h"
#include "crash_harness.h"
#include "property_harness.h"

namespace abyss::engine {
namespace {

constexpr int kKeys = 300;
constexpr int kHotKeys = 24;
constexpr int kMembers = 12;
// The bounded tier: these seeds, this many steps each; fewer under a
// sanitizer, which skips the sometimes-counter checks.
constexpr uint64_t kBoundedSeeds = kSanitizerDivisor > 1 ? 2 : 6;
constexpr int kBoundedOps = kSanitizerDivisor > 1 ? 150 : 450;

// The rare interleavings each run must reach.
// Indexes Counts.
enum Sometimes : uint8_t {  // NOLINT(cppcoreguidelines-use-enum-class)
  kReadAtDeadline,
  kExpireAtDeadline,
  kEvictionDuringLoad,
  kFlushMidBatch,
  kBlindWriteDiscardsLoad,
  kObservedExpiryDel,
  kRedecideNoSpare,
  kStubAnswersExistence,
  kFlushMidLoad,
  kRestart,
  kCrossSlot,
  kOom,
  kReclaimedRestart,
  kReplaySkipped,
  kSometimesCount,
};
constexpr std::array<std::string_view, kSometimesCount> kSometimesNames = {
    "read at a TTL deadline",
    "EXPIRE deciding at a deadline",
    "eviction during a load",
    "flush in the middle of a batch",
    "blind write discards a load",
    "observed-expiry DEL",
    "re-decide after no spare",
    "stub answers existence",
    "cold flush during a load",
    "restart",
    "CROSSSLOT reply",
    "-OOM reply",
    "restart over a reclaimed head",
    "replay skipped a key it never saw created"};

using Counts = std::array<uint64_t, kSometimesCount>;

// The keys a command names, reads included.
std::vector<std::string> KeysOf(const std::vector<std::string>& args) {
  const std::string name = core::AsciiUpper(args[0]);
  if (name == "FLUSHDB") return {};
  if (name == "MGET" || name == "EXISTS") return {args.begin() + 1, args.end()};
  return RedisModel::WriteKeys(name, args);
}

// Answered from a stub when one is held: existence alone.
bool ExistenceOnly(const std::vector<std::string>& args) {
  const std::string name = core::AsciiUpper(args[0]);
  if (name == "EXISTS" || name == "TYPE" || name == "TTL" || name == "PTTL" || name == "SETNX" ||
      name == "DEL" || name == "UNLINK" || name == "MSETNX") {
    return true;
  }
  if (name != "SET") return false;
  bool conditional = false;
  for (size_t i = 3; i < args.size(); ++i) {
    const std::string opt = core::AsciiUpper(args[i]);
    if (opt == "GET") return false;
    conditional = conditional || opt == "NX" || opt == "XX" || opt == "KEEPTTL";
  }
  return conditional;
}

class DifferentialTest : public PropertyHarness {
 protected:
  // One seed's stream; false once it has failed.
  bool RunSeed(uint64_t seed, int ops, Counts& counts) {
    Swarm swarm = SwarmFor(seed);
    if (victim_dir_.has_value()) swarm.options.run = victim_dir_->string() + "/";
    std::cout << "[differential] seed=" << seed << " " << swarm.text << '\n' << std::flush;
    seed_ = seed;
    swarm_ = swarm.text;
    relieve_ = swarm.relieve;
    restarted_after_oom_ = false;
    rng_ = Rng(seed);
    hook_rng_ = Rng(seed ^ 0xABCDULL);
    timing_rng_ = Rng(seed ^ 0x7131ULL);
    victim_ready_step_ = 100 + static_cast<int>(Rng(seed ^ 0x5EEDULL).Below(400));
    ops_ = ops;
    expired_.clear();
    counts_ = {};
    counter_ = 0;
    history_.clear();
    batches_.clear();
    model_.Clear();
    const auto start = std::chrono::duration_cast<std::chrono::milliseconds>(
                           core::WallClock::now().time_since_epoch())
                           .count();
    // A day behind real time, so the fake clock never overtakes the
    // retention reaper's real one.
    OpenAt(swarm.options, start - (start % 1000) - 86'400'000);
    hooked_.SetHook([this](std::string_view key) { OnLoad(key); });

    for (int step = 0; step < ops && !failed_; ++step) {
      step_ = step;
      // Once a stream: cold takes everything and retention drops it, a
      // few dozen writes follow, then a restart replays only those, so
      // it meets updates to keys whose creation it never sees.
      if (step == (ops * 5) / 8) Settle();
      if (step == ((ops * 5) / 8) + 30) {
        GrowCollections();
        Reopen(/*settle_first=*/false);
      }
#ifndef _WIN32
      if (victim_dir_.has_value() && step == victim_ready_step_) {
        abyss::testing::SignalReady(*victim_dir_, "victim.ready", "");
      }
#endif
      Tick();
      if (rng_.Chance(0.22)) {
        Maintain();
      } else {
        Command(Generate());
      }
    }
    if (!failed_) Finish();
    hooked_.SetHook(nullptr);
    CloseAll();
    for (size_t i = 0; i < counts.size(); ++i) counts.at(i) += counts_.at(i);
    return !failed_;
  }

  // ---- the clock

  void Tick() {
    const uint64_t roll = rng_.Below(100);
    deadline_key_.reset();
    if (roll < 55) {
      SetNow(now_ms_ + rng_.Between(0, 40));
    } else if (roll < 63) {
      SetNow(now_ms_ + rng_.Between(200, 1500));
    } else if (roll < 72) {
      // Land exactly on a TTL the model holds, and favour its key.
      std::vector<std::pair<int64_t, std::string>> ttls;
      for (const auto& [key, value] : model_.data()) {
        if (value.abs_ttl_ms > now_ms_) ttls.emplace_back(value.abs_ttl_ms, key);
      }
      if (!ttls.empty()) {
        const auto& [at, key] = ttls.at(rng_.Below(ttls.size()));
        SetNow(at);
        deadline_key_ = key;
      }
    }
  }
  // The fake wall clock never goes back: decide's now is the max of it
  // and the shard's last appended_at, so a step back would part the
  // engine from the model.
  void SetNow(int64_t ms) {
    if (ms < now_ms_) Fail("the fake clock went back");
    now_ms_ = ms;
  }

  // ---- the generator

  std::string Key() {
    if (deadline_key_.has_value() && rng_.Chance(0.5)) return *deadline_key_;
    if (rng_.Chance(0.7)) return "k" + std::to_string(rng_.Below(kHotKeys));
    return "k" + std::to_string(rng_.Below(kKeys));
  }
  std::string Member() { return "m" + std::to_string(rng_.Below(kMembers)); }
  std::string Value() {
    const uint64_t roll = rng_.Below(20);
    if (roll == 0) return "";
    if (roll == 1) return std::string(rng_.Between(50, 350), 'x') + std::to_string(++counter_);
    return "v" + std::to_string(++counter_);
  }
  std::string Score() {
    const int64_t halves = rng_.Between(-12, 12);
    return (halves < 0 ? "-" : "") + std::to_string(std::abs(halves) / 2) +
           (halves % 2 != 0 ? ".5" : "");
  }
  std::string Num(int64_t n) const { return std::to_string(n); }

  std::vector<std::string> Generate() {
    const int64_t now = now_ms_;
    const uint64_t roll = rng_.Below(1000);
    if (roll < 2) return {"FLUSHDB"};
    if (roll < 140) {
      std::vector<std::string> args = {"SET", Key(), Value()};
      const uint64_t cond = rng_.Below(6);
      if (cond == 1) args.emplace_back("NX");
      if (cond == 2) args.emplace_back("XX");
      if (rng_.Chance(0.2)) args.emplace_back("GET");
      if (rng_.Chance(0.15)) {
        args.emplace_back("KEEPTTL");
      } else {
        switch (rng_.Below(8)) {
          case 0:
            args.insert(args.end(), {"EX", Num(rng_.Between(1, 3))});
            break;
          case 1:
            args.insert(args.end(), {"PX", Num(rng_.Between(1, 3000))});
            break;
          case 2:
            args.insert(args.end(), {"PXAT", Num(now + rng_.Between(-200, 3000))});
            break;
          case 3:
            args.insert(args.end(), {"PXAT", Num(now)});
            break;
          case 4:
            args.insert(args.end(), {"EXAT", Num((now / 1000) + rng_.Between(0, 3))});
            break;
          default:
            break;
        }
      }
      return args;
    }
    if (roll < 160) {
      if (rng_.Chance(0.5)) return {"SETEX", Key(), Num(rng_.Between(1, 3)), Value()};
      return {"PSETEX", Key(), Num(rng_.Between(1, 3000)), Value()};
    }
    if (roll < 180) return {"SETNX", Key(), Value()};
    if (roll < 220) {
      std::vector<std::string> args = {rng_.Chance(0.6) ? "MSET" : "MSETNX"};
      const int64_t n = rng_.Between(1, 3);
      for (int64_t i = 0; i < n; ++i) args.insert(args.end(), {Key(), Value()});
      return args;
    }
    if (roll < 260) {
      std::vector<std::string> args = {rng_.Chance(0.8) ? "DEL" : "UNLINK"};
      const int64_t n = rng_.Between(1, 3);
      for (int64_t i = 0; i < n; ++i) args.push_back(Key());
      return args;
    }
    if (roll < 350) {
      const bool add = rng_.Chance(0.65);
      std::vector<std::string> args = {add ? "SADD" : "SREM", Key()};
      const int64_t n = rng_.Between(1, add ? 4 : 3);
      for (int64_t i = 0; i < n; ++i) args.push_back(Member());
      return args;
    }
    if (roll < 430) {
      if (rng_.Chance(0.25)) {
        std::vector<std::string> args = {"ZREM", Key()};
        const int64_t n = rng_.Between(1, 3);
        for (int64_t i = 0; i < n; ++i) args.push_back(Member());
        return args;
      }
      std::vector<std::string> args = {"ZADD", Key()};
      switch (rng_.Below(10)) {
        case 0:
          args.emplace_back("NX");
          break;
        case 1:
          args.emplace_back("XX");
          break;
        case 2:
          args.emplace_back("GT");
          break;
        case 3:
          args.emplace_back("LT");
          break;
        case 4:
          args.insert(args.end(), {"XX", "GT"});
          break;
        case 5:
          args.insert(args.end(), {"XX", "LT"});
          break;
        default:
          break;
      }
      if (rng_.Chance(0.3)) args.emplace_back("CH");
      const int64_t n = rng_.Between(1, 3);
      for (int64_t i = 0; i < n; ++i) args.insert(args.end(), {Score(), Member()});
      return args;
    }
    if (roll < 520) {
      const uint64_t kind = rng_.Below(10);
      if (kind < 2) {
        std::vector<std::string> args = {"HDEL", Key()};
        const int64_t n = rng_.Between(1, 3);
        for (int64_t i = 0; i < n; ++i) args.push_back(Member());
        return args;
      }
      if (kind < 4) return {"HSETNX", Key(), Member(), Value()};
      std::vector<std::string> args = {kind == 4 ? "HMSET" : "HSET", Key()};
      const int64_t n = rng_.Between(1, 3);
      for (int64_t i = 0; i < n; ++i) args.insert(args.end(), {Member(), Value()});
      return args;
    }
    if (roll < 590) {
      if (rng_.Chance(0.2)) return {"PERSIST", Key()};
      std::vector<std::string> args;
      switch (rng_.Below(4)) {
        case 0:
          args = {"EXPIRE", Key(), Num(rng_.Between(-1, 3))};
          break;
        case 1:
          args = {"PEXPIRE", Key(), Num(rng_.Between(-100, 3000))};
          break;
        case 2:
          args = {"EXPIREAT", Key(), Num((now / 1000) + rng_.Between(-1, 3))};
          break;
        default:
          args = {"PEXPIREAT", Key(), Num(rng_.Chance(0.3) ? now : now + rng_.Between(-100, 3000))};
          break;
      }
      constexpr std::array<std::string_view, 4> kFlags = {"NX", "XX", "GT", "LT"};
      if (rng_.Chance(0.4)) args.emplace_back(kFlags.at(rng_.Below(kFlags.size())));
      return args;
    }
    if (roll < 620) {
      if (rng_.Chance(0.5)) return {"RENAMENX", Key(), Key()};
      std::vector<std::string> args = {"COPY", Key(), Key()};
      if (args[1] == args[2]) args[2] += "c";
      if (rng_.Chance(0.4)) args.emplace_back("REPLACE");
      return args;
    }
    return GenerateRead();
  }

  std::vector<std::string> GenerateRead() {
    const std::string key = Key();
    switch (rng_.Below(20)) {
      case 0:
      case 1:
      case 2:
        return {"GET", key};
      case 3: {
        std::vector<std::string> args = {"MGET", key};
        const int64_t n = rng_.Between(0, 2);
        for (int64_t i = 0; i < n; ++i) args.push_back(Key());
        return args;
      }
      case 4:
      case 5: {
        std::vector<std::string> args = {"EXISTS", key};
        const int64_t n = rng_.Between(0, 2);
        for (int64_t i = 0; i < n; ++i) args.push_back(Key());
        return args;
      }
      case 6:
        return {"TYPE", key};
      case 7:
        return {rng_.Chance(0.5) ? "TTL" : "PTTL", key};
      case 8:
        return {"SMEMBERS", key};
      case 9:
        return {"SISMEMBER", key, Member()};
      case 10:
        return {"SCARD", key};
      case 11:
        return {"ZSCORE", key, Member()};
      case 12:
        return {"ZCARD", key};
      case 13:
        if (rng_.Chance(0.5)) return {"ZRANGE", key, "0", "-1", "WITHSCORES"};
        return {"ZRANGE", key, Num(rng_.Between(-3, 2)), Num(rng_.Between(-2, 4))};
      case 14:
        return {"HGET", key, Member()};
      case 15:
        return {"HGETALL", key};
      case 16:
        return {"HMGET", key, Member(), Member()};
      case 17:
        return {"HEXISTS", key, Member()};
      case 18:
        return {"HLEN", key};
      default:
        return {rng_.Chance(0.5) ? "HKEYS" : "HVALS", key};
    }
  }

  // ---- one command, three ways

  void Command(const std::vector<std::string>& args) {
    const std::string name = core::AsciiUpper(args.at(0));
    const bool write = IsWrite(name);
    const std::vector<std::string> keys = KeysOf(args);
    // Relieved: every involved shard drained before the write, so only
    // a value larger than a shard's limit may meet -OOM.
    if (write && relieve_) {
      if (name == "FLUSHDB") {
        for (core::ShardId shard = 0; shard < options_.shards; ++shard) Absorb(shard);
      } else {
        for (const auto& key : keys) Absorb(ShardOf(key));
      }
    }
    NoteBefore(name, args, keys, write);
    nested_.clear();
    loaded_.clear();
    outer_ = &args;
    const std::vector<core::SequenceId> ends_before = Ends();
    // What the command's own decision follows: a nested write moves it.
    ends_before_outer_ = ends_before;
    const uint64_t discards_before = hot_->Stats()->load_discards;
    Journal('C', args);
    const core::RespValue reply = Run(args);
    outer_ = nullptr;
    const std::string got = RedisModel::Canonical(name, reply);
    Remember(args, got);
    Journal('R', {got});
    if (write) {
      NoteBatch(Ends(), ends_before);
      CountObservedExpiryDels(ends_before);
    }
    if (nested_on_loaded_ && hot_->Stats()->load_discards > discards_before) {
      ++counts_.at(kBlindWriteDiscardsLoad);
    }
    nested_on_loaded_ = false;
    if (got.starts_with("-CROSSSLOT")) ++counts_.at(kCrossSlot);
    if (got.starts_with("-OOM")) {
      if (!write) {
        Fail("a read replied -OOM: " + Join(args) + " -> " + got);
        return;
      }
      for (const auto& nested : nested_) (void)model_.Execute(nested, now_ms_);
      Oom(args, keys);
      return;
    }
    if (write && reply.IsError() && LoadTimedOut(ErrorMessage(reply))) {
      // Its loads ran out of the write's deadline before anything was
      // reserved: the model skips it.
      for (const auto& nested : nested_) (void)model_.Execute(nested, now_ms_);
      (void)LoggedNothing(args, keys, got);
      return;
    }
    for (const auto& key : keys) {
      if (stubbed_.contains(key) && !loaded_.contains(key) && ExistenceOnly(args)) {
        ++counts_.at(kStubAnswersExistence);
      }
    }
    stubbed_.clear();

    const int64_t now = now_ms_;
    std::string want;
    if (write || nested_.empty()) {
      for (const auto& nested : nested_) (void)model_.Execute(nested, now);
      want = RedisModel::Canonical(name, model_.Execute(args, now));
    } else {
      // A read raced by a write linearises before it or after it.
      RedisModel before = model_;
      const std::string early = RedisModel::Canonical(name, before.Execute(args, now));
      for (const auto& nested : nested_) (void)model_.Execute(nested, now);
      want = RedisModel::Canonical(name, model_.Execute(args, now));
      if (got == early) want = early;
    }
    if (got != want) {
      Fail("reply to " + Join(args) + ": got " + got + ", model " + want);
      return;
    }
    std::vector<std::string> touched = keys;
    for (const auto& nested : nested_) touched.push_back(nested.at(1));
    if (name == "FLUSHDB") {
      for (int i = 0; i < kHotKeys; ++i) touched.push_back("k" + std::to_string(i));
    }
    for (const auto& key : touched) {
      if (const auto seen = CheckKey(key); seen.mismatch.has_value()) {
        Fail("after " + Join(args) + ": " + *seen.mismatch);
        return;
      }
    }
  }

  // Refused before anything was reserved: nothing of it is in the log
  // or hot.
  bool LoggedNothing(const std::vector<std::string>& args, const std::vector<std::string>& keys,
                     const std::string& reply) {
    if (Ends() != ends_before_outer_) {
      Fail(Join(args) + " replied " + reply + " but logged frames");
      return false;
    }
    return std::ranges::all_of(keys, [&](const std::string& key) {
      const auto seen = CheckKey(key);
      if (seen.mismatch.has_value()) {
        Fail("after " + reply + " for " + Join(args) + ": " + *seen.mismatch);
      }
      return !seen.mismatch.has_value();
    });
  }

  // -OOM: refused before anything was reserved, so the model skips it,
  // and nothing of it is in the log or hot, then or after a restart.
  void Oom(const std::vector<std::string>& args, const std::vector<std::string>& keys) {
    ++counts_.at(kOom);
    if (relieve_ && !ValueOverLimit(args, keys)) {
      Fail(Join(args) + " met -OOM with its shards drained and no value over a shard's limit " +
           MemoryOf(ShardOf(keys.empty() ? std::string("k0") : keys.front())));
      return;
    }
    if (!LoggedNothing(args, keys, "-OOM")) return;
    // "Cold is behind" must be true: once cold takes everything, hot
    // can evict down to its limit (it exceeds it only by what cold has
    // not drained).
    for (const auto& key : keys) {
      const core::ShardId shard = ShardOf(key);
      Absorb(shard);
      if (!hot_->EvictShardToTarget(shard)) {
        Fail(Join(args) + " replied -OOM, and with everything drained hot stays over its limit " +
             MemoryOf(shard));
        return;
      }
    }
    // Not while starving spares: the resumer holds the queue. When an
    // -OOM lands is timing, so its draws come from timing_rng_.
    if (!restarted_after_oom_ && !starving_) {
      restarted_after_oom_ = true;
      Reopen(timing_rng_.Chance(0.5));
    }
  }

  // Whether a write's value alone is over a shard's backpressure limit.
  bool ValueOverLimit(const std::vector<std::string>& args, const std::vector<std::string>& keys) {
    size_t largest = 0;
    for (size_t i = 2; i < args.size(); ++i) largest = std::max(largest, args[i].size());
    return std::ranges::any_of(keys, [this, largest](const std::string& key) {
      return largest >= hot_->Memory(ShardOf(key)).limit_bytes;
    });
  }

  // Each DEL frame the write logged for a key hot held past its TTL:
  // an observed expiry, logged so cold and replay see it.
  void CountObservedExpiryDels(const std::vector<core::SequenceId>& ends_before) {
    if (expired_.empty()) return;
    for (core::ShardId shard = 0; shard < options_.shards; ++shard) {
      const auto end = queue_->DurableEnd(shard, core::Durability::kProcessCrash).value_or(0);
      if (end <= ends_before[shard]) continue;
      auto logged = queue_->Read(shard, ends_before[shard], end - ends_before[shard],
                                 std::chrono::milliseconds(0), core::Durability::kProcessCrash);
      if (!logged.has_value()) continue;
      for (const auto& entry : *logged) {
        const auto args = engine::ArgsOf(entry);
        if (args.size() == 2 && args[0] == "DEL" && expired_.contains(args[1])) {
          ++counts_.at(kObservedExpiryDel);
        }
      }
    }
    expired_.clear();
  }

  void NoteBefore(const std::string& name, const std::vector<std::string>& args,
                  const std::vector<std::string>& keys, bool write) {
    const int64_t now = now_ms_;
    bool at_deadline = false;
    for (const auto& key : keys) {
      const auto it = model_.data().find(key);
      if (it != model_.data().end() && it->second.abs_ttl_ms == now) at_deadline = true;
    }
    if (at_deadline && !write) ++counts_.at(kReadAtDeadline);
    // An EXPIRE deciding a live key's deadline to be now.
    if (name.contains("EXPIRE") && model_.Find(args.at(1), now) != nullptr) {
      const int64_t at = std::stoll(args.at(2));
      int64_t when = now + (at * 1000);
      if (name == "PEXPIREAT") {
        when = at;
      } else if (name == "EXPIREAT") {
        when = at * 1000;
      } else if (name == "PEXPIRE") {
        when = now + at;
      }
      if (when == now) ++counts_.at(kExpireAtDeadline);
    }
    for (const auto& key : keys) {
      const core::ShardId shard = ShardOf(key);
      auto locks = hot_->LockExclusive(std::vector<core::ShardId>{shard});
      const auto view = locks.View(key, static_cast<uint64_t>(now));
      if (view.presence == hot::KeyView::Presence::kStub) stubbed_.insert(key);
      if (write && name != "FLUSHDB" && view.presence == hot::KeyView::Presence::kExpired) {
        expired_.insert(key);
      }
    }
  }

  // Each shard's published end.
  std::vector<core::SequenceId> Ends() {
    std::vector<core::SequenceId> ends(options_.shards);
    for (core::ShardId shard = 0; shard < options_.shards; ++shard) {
      ends[shard] = queue_->DurableEnd(shard, core::Durability::kProcessCrash).value_or(0);
    }
    return ends;
  }

  // A write that logged more than one frame is a batch.
  void NoteBatch(const std::vector<core::SequenceId>& after,
                 const std::vector<core::SequenceId>& before) {
    std::map<core::ShardId, std::pair<core::SequenceId, core::SequenceId>> ranges;
    core::SequenceId frames = 0;
    for (core::ShardId shard = 0; shard < after.size(); ++shard) {
      if (after[shard] > before[shard]) {
        ranges[shard] = {before[shard], after[shard]};
        frames += after[shard] - before[shard];
      }
    }
    if (frames < 2) return;
    batches_.push_back(std::move(ranges));
    if (batches_.size() > 64) batches_.pop_front();
  }

  // Whether flushing `shard` now flushes part of a batch: absorbed
  // partway on it, or whole on it but not on another of its shards.
  bool MidBatch(core::ShardId shard) {
    const auto drained = [this](core::ShardId s) {
      return pool_->ConsumerFor(s).LatestDrainedSeq();
    };
    for (const auto& ranges : batches_) {
      const auto it = ranges.find(shard);
      if (it == ranges.end()) continue;
      const auto [first, end] = it->second;
      const core::SequenceId at = drained(shard);
      if (at >= first && at + 1 < end) return true;
      if (at + 1 >= end) {
        for (const auto& [other, range] : ranges) {
          if (drained(other) + 1 < range.second) return true;
        }
      }
    }
    return false;
  }

  // ---- loads raced

  void OnLoad(std::string_view key) {
    loaded_.insert(std::string(key));
    if (outer_ == nullptr) return;
    switch (hook_rng_.Below(12)) {
      case 0:
      case 1: {
        if (trace_) std::cout << "[trace]   evict during load of " << key << "\n";
        const core::ShardId shard = ShardOf(key);
        const uint64_t before = hot_->Memory(shard).used_bytes;
        hot_->EvictExpired(core::SteadyClock::now() + std::chrono::hours{1'000'000});
        if (hot_->Memory(shard).used_bytes < before) ++counts_.at(kEvictionDuringLoad);
        break;
      }
      case 2: {
        if (trace_) std::cout << "[trace]   cold flush during load of " << key << "\n";
        const core::ShardId shard = ShardOf(key);
        Absorb(shard);
        FlushCold(shard);
        ++counts_.at(kFlushMidLoad);
        break;
      }
      case 3:
      case 4: {
        // Only a blind write: anything else waits on this load.
        const std::string outer = core::AsciiUpper(outer_->at(0));
        if (outer == "MGET" || outer == "EXISTS") break;
        std::vector<std::string> nested = {"SET", std::string(key),
                                           "n" + std::to_string(++counter_)};
        Absorb(ShardOf(key));
        // The engine directly: the pipeline is mid-dispatch.
        auto result = engine_->DispatchWrite("SET", core::RespCommand{.args = nested},
                                             core::PredicateFlags::kNone);
        const core::RespValue reply =
            result.has_value()
                ? *std::move(result)
                : core::RespValue::Error(core::ErrorPrefix::kErr, result.error().message());
        if (RedisModel::Render(reply) != "+OK") {
          Fail("blind write " + Join(nested) + " during a load: " + RedisModel::Render(reply) +
               " " + MemoryOf(ShardOf(key)));
          break;
        }
        nested_on_loaded_ = true;
        ends_before_outer_ = Ends();
        Journal('N', nested);
        nested_.push_back(std::move(nested));
        break;
      }
      default:
        break;
    }
  }

  // ---- maintenance

  void Maintain() {
    const uint64_t roll = rng_.Below(100);
    const auto shard = static_cast<core::ShardId>(rng_.Below(options_.shards));
    if (trace_)
      std::cout << "[trace] step " << step_ << " maintain " << roll << " shard " << shard << "\n";
    if (roll < 30) {
      Absorb(shard, rng_.Chance(0.5) ? 1 + rng_.Below(3) : SIZE_MAX);
    } else if (roll < 50) {
      if (MidBatch(shard)) ++counts_.at(kFlushMidBatch);
      FlushCold(shard);
    } else if (roll < 62) {
      hot_->EvictExpired(core::SteadyClock::now() + std::chrono::hours{1'000'000});
    } else if (roll < 70) {
      hot_->EvictToMemoryTarget();
    } else if (roll < 76) {
      hot_->GcTombstones();
    } else if (roll < 84) {
      hot_->EvictExpired(core::SteadyClock::now());
    } else if (roll < 88) {
      Settle();
    } else if (roll < 91) {
      StarveSpares();
    } else if (roll < 93) {
      Reopen(rng_.Chance(0.5));
    } else if (roll < 96) {
      CheckMemoryBound();
    } else if (roll < 98) {
      // Cold's own TTL sweep, by its log clock.
      if (auto ticked = cold_->RunScannerTickForTesting(); !ticked.has_value()) {
        Fail("cold's TTL sweep: " + ticked.error().message());
      }
    } else if (!relieve_) {
      OutrunCold(shard);
    }
  }

  // Large writes to one shard, none drained, until one meets -OOM.
  void OutrunCold(core::ShardId shard) {
    const uint64_t ooms = counts_.at(kOom);
    for (int i = 0; i < 12 && counts_.at(kOom) == ooms && !failed_; ++i) {
      Command({"SET", KeyOn(shard, static_cast<int>(rng_.Below(kHotKeys)), "k"),
               std::string(1000, 'o') + std::to_string(++counter_)});
    }
  }

  // Hot exceeds its limit only by what cold has not drained: with
  // everything drained, each shard evicts down to its limit.
  void CheckMemoryBound() {
    for (core::ShardId shard = 0; shard < options_.shards && !failed_; ++shard) {
      Absorb(shard);
      if (!hot_->EvictShardToTarget(shard)) {
        Fail("with everything drained, hot stays over its limit " + MemoryOf(shard));
      }
    }
  }

  // Everything into cold, as a quiet period would, and cold's commits
  // persisted, so retention reclaims the segments they cover.
  void Settle() {
    for (core::ShardId s = 0; s < options_.shards; ++s) {
      Absorb(s);
      FlushCold(s);
    }
    if (auto flushed = queue_->FlushOffsets(); !flushed.has_value()) {
      Fail("persisting offsets: " + flushed.error().message());
    }
  }

  // Adds to a few collections that exist: updates, not creations, so a
  // replay that never saw them created must leave them to cold.
  void GrowCollections() {
    const int64_t now = now_ms_;
    std::vector<std::vector<std::string>> writes;
    for (const auto& [key, value] : model_.data()) {
      if (writes.size() >= 4 || RedisModel::Expired(value, now)) continue;
      const std::string member = "g" + std::to_string(++counter_);
      switch (value.type) {
        case RedisModel::Type::kSet:
          writes.push_back({"SADD", key, member});
          break;
        case RedisModel::Type::kHash:
          writes.push_back({"HSET", key, member, "v"});
          break;
        case RedisModel::Type::kZset:
          writes.push_back({"ZADD", key, "1", member});
          break;
        case RedisModel::Type::kString:
          break;
      }
    }
    for (const auto& write : writes) {
      if (!failed_) Command(write);
    }
  }

  // Often after a quiet period (`settle_first`): recovery then replays
  // only what cold has not taken.
  void Reopen(bool settle_first) {
    if (settle_first) Settle();
    hooked_.SetHook(nullptr);
    auto restarted = RestartAt();
    hooked_.SetHook([this](std::string_view key) { OnLoad(key); });
    if (!restarted.has_value()) {
      Fail("restart: " + restarted.error().message());
      return;
    }
    ++counts_.at(kRestart);
    if (skipped_ > 0) ++counts_.at(kReplaySkipped);
    for (core::ShardId shard = 0; shard < options_.shards; ++shard) {
      if (queue_->FirstSeq(shard).value_or(core::kFirstSeq) > core::kFirstSeq) {
        ++counts_.at(kReclaimedRestart);
        break;
      }
    }
    CheckAll("after a restart");
  }

  // No spare segment: the preparer is paused until a write re-decides
  // for one, and large SETs use the spares up.
  void StarveSpares() {
    const auto redecides = [this] {
      return sequencer_->Snapshot().redecides.at(
          static_cast<size_t>(metrics::RedecideReason::kSpare));
    };
    const uint64_t before = redecides();
    for (uint32_t log = 0; log < options_.log_count; ++log) {
      queue_->PauseSegmentPreparerForTesting(log, true);
    }
    std::atomic<bool> done{false};
    std::thread resumer([&] {
      const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(3);
      while (!done.load() && redecides() == before && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::microseconds(200));
      }
      for (uint32_t log = 0; log < options_.log_count; ++log) {
        queue_->PauseSegmentPreparerForTesting(log, false);
      }
    });
    // A fixed count, enough to fill the active segment and both spares,
    // so the stream does not depend on when the spares ran out. Each
    // write's shards are drained first: this step is not about memory.
    const int writes = static_cast<int>((3 * options_.segment_size_bytes) / 2048) + 2;
    starving_ = true;
    const bool relieve = relieve_;
    relieve_ = true;
    for (int i = 0; i < writes && !failed_; ++i) {
      Command({"SET", Key(), std::string(2000, 's') + std::to_string(++counter_)});
    }
    relieve_ = relieve;
    starving_ = false;
    done = true;
    resumer.join();
    if (redecides() > before) ++counts_.at(kRedecideNoSpare);
  }

  // ---- the end of a stream

  void CheckAll(const std::string& when) {
    std::set<std::string> keys;
    for (int i = 0; i < kKeys; ++i) keys.insert("k" + std::to_string(i));
    for (const auto& [key, value] : model_.data()) keys.insert(key);
    for (const auto& key : keys) {
      if (const auto seen = CheckKey(key); seen.mismatch.has_value()) {
        Fail(when + ": " + *seen.mismatch);
        return;
      }
    }
  }

  void Finish() {
    hooked_.SetHook(nullptr);
    CheckAll("at the end, hot then buffer plus cold");
    if (failed_) return;
    std::set<std::string> keys;
    for (int i = 0; i < kKeys; ++i) keys.insert("k" + std::to_string(i));
    for (const auto& [key, value] : model_.data()) keys.insert(key);
    const int64_t now = now_ms_;
    for (const auto& key : keys) {
      for (const auto& read :
           {std::vector<std::string>{"TYPE", key}, std::vector<std::string>{"PTTL", key},
            FullRead(key, model_.Find(key, now))}) {
        const std::string got = RedisModel::Canonical(read[0], Run(read));
        const std::string want = RedisModel::Canonical(read[0], model_.Execute(read, now));
        if (got != want) {
          std::string what = "at the end, through the read path, ";
          what.append(Join(read)).append(": got ").append(got).append(", model ").append(want);
          Fail(what);
          return;
        }
      }
    }
    // Everything into cold, then RocksDB alone.
    for (core::ShardId shard = 0; shard < options_.shards; ++shard) DrainToCold(shard);
    const std::string cold_path = dir_.Sub(options_.run + "cold").string();
    const uint32_t shards = options_.shards;
    CloseAll();
    auto cold = cold::backends::RocksdbStore::Create(cold::backends::RocksdbConfig{
        .data_path = cold_path,
        .shard_count = shards,
        .log_clock = [](core::ShardId) -> uint64_t { return 0; },
    });
    ASSERT_TRUE(cold.has_value()) << cold.error().message();
    for (const auto& key : keys) {
      const RedisModel::Value* want = model_.Find(key, now);
      auto loaded = (*cold)->LoadKey(key, core::SteadyClock::now() + std::chrono::seconds(30));
      if (!loaded.has_value()) {
        Fail("cold read of " + key + ": " + loaded.error().message());
        return;
      }
      std::optional<RedisModel::Value> got;
      if (loaded->has_value() && ((*loaded)->abs_ttl_ms == 0 || now < (*loaded)->abs_ttl_ms)) {
        auto value = FromHot(abyss::testing::HotValueOf((*loaded)->value), (*loaded)->abs_ttl_ms);
        if (const auto* error = std::get_if<std::string>(&value)) {
          Fail("cold " + key + ": " + *error);
          return;
        }
        got = std::get<RedisModel::Value>(value);
      }
      if (got.has_value() != (want != nullptr) || (got.has_value() && !(*got == *want))) {
        Fail("cold alone after the final drain, key " + key + ": got " +
             (got.has_value() ? RedisModel::Describe(*got) : "absent") + ", model " + Show(want));
        return;
      }
    }
  }

  // ---- reporting

  std::string MemoryOf(core::ShardId shard) {
    const auto memory = hot_->Memory(shard);
    const auto stats = hot_->Stats();
    return "(shard " + std::to_string(shard) + " used " + std::to_string(memory.used_bytes) +
           " limit " + std::to_string(memory.limit_bytes) + "; store keys " +
           std::to_string(stats->key_count) + " stubs " + std::to_string(stats->stub_entries) +
           " negatives " + std::to_string(stats->negative_entries) + " unevictable " +
           std::to_string(stats->unevictable_bytes) + " drained " +
           std::to_string(hot_->Drained(shard)) + " end " +
           std::to_string(queue_->DurableEnd(shard, core::Durability::kProcessCrash).value_or(0)) +
           ")";
  }

  static std::string Join(const std::vector<std::string>& args) {
    std::string out;
    for (const auto& arg : args) {
      if (!out.empty()) out += " ";
      out += arg.size() > 40 ? arg.substr(0, 12) + "...(" + std::to_string(arg.size()) + "B)" : arg;
    }
    return out;
  }
  void Remember(const std::vector<std::string>& args, const std::string& reply) {
    history_.push_back("t=" + std::to_string(now_ms_.load()) + " " + Join(args) + " -> " + reply);
    if (trace_) std::cout << "[trace] step " << step_ << " " << history_.back() << "\n";
    if (history_.size() > 30) history_.pop_front();
  }
  // The kill -9 victim's record of what it ran, flushed line by line to
  // the page cache, which outlives the kill: C a command at its instant,
  // N a blind write a load raced, R the command's reply.
  void Journal(char kind, const std::vector<std::string>& args) {
    if (!journal_.is_open()) return;
    journal_ << kind << ' ' << now_ms_.load() << ' ' << args.size();
    for (const auto& arg : args) journal_ << ' ' << arg.size() << ':' << arg;
    journal_ << '\n';
    journal_.flush();
  }

  void Fail(const std::string& what) {
    if (failed_) return;
    failed_ = true;
    std::string recent;
    for (const auto& line : history_) recent += "\n  " + line;
    ADD_FAILURE() << "seed " << seed_ << " (" << swarm_ << ") step " << step_ << ": " << what
                  << "\nreplay: ABYSS_PROPERTY_SEED=" << seed_ << " ABYSS_PROPERTY_OPS=" << ops_
                  << "\nlast commands:" << recent;
  }

  // NOLINTBEGIN(cppcoreguidelines-non-private-member-variables-in-classes)
  // ABYSS_PROPERTY_TRACE=1 prints every command and step.
  bool trace_ = EnvNumber("ABYSS_PROPERTY_TRACE").value_or(0) != 0;
  std::optional<std::filesystem::path> victim_dir_;
  // The kill victim's step to signal ready at, drawn per seed.
  int victim_ready_step_ = 0;
  int ops_ = 0;
  std::vector<core::SequenceId> ends_before_outer_;
  std::set<std::string> expired_;
  // Draws whose number or place depends on timing.
  Rng timing_rng_{0};
  std::ofstream journal_;
  bool relieve_ = true;
  bool restarted_after_oom_ = false;
  bool starving_ = false;
  std::optional<std::string> deadline_key_;
  Rng rng_{0};
  Rng hook_rng_{0};
  uint64_t seed_ = 0;
  std::string swarm_;
  int step_ = 0;
  uint64_t counter_ = 0;
  bool failed_ = false;
  Counts counts_{};
  std::deque<std::string> history_;
  std::deque<std::map<core::ShardId, std::pair<core::SequenceId, core::SequenceId>>> batches_;
  const std::vector<std::string>* outer_ = nullptr;
  std::vector<std::vector<std::string>> nested_;
  bool nested_on_loaded_ = false;
  std::set<std::string> loaded_;
  std::set<std::string> stubbed_;
  // NOLINTEND(cppcoreguidelines-non-private-member-variables-in-classes)
};

void Report(const Counts& counts) {
  std::cout << "[differential] sometimes:";
  for (size_t i = 0; i < counts.size(); ++i) {
    std::cout << " " << kSometimesNames.at(i) << "=" << counts.at(i) << ";";
  }
  std::cout << "\n";
}

TEST_F(DifferentialTest, SeededStreamsAgreeWithTheModelInEveryTier) {
  Counts counts{};
  if (const auto seed = EnvNumber("ABYSS_PROPERTY_SEED")) {
    const int ops = static_cast<int>(EnvNumber("ABYSS_PROPERTY_OPS").value_or(kBoundedOps));
    RunSeed(*seed, ops, counts);
    Report(counts);
    return;
  }
  const auto started = std::chrono::steady_clock::now();
  for (uint64_t seed = 1; seed <= kBoundedSeeds; ++seed) {
    if (!RunSeed(seed, kBoundedOps, counts)) break;
  }
  Report(counts);
  std::cout << "[differential] bounded tier: " << kBoundedSeeds << " seeds x " << kBoundedOps
            << " steps in "
            << std::chrono::duration_cast<std::chrono::milliseconds>(
                   std::chrono::steady_clock::now() - started)
                   .count()
            << " ms\n";
  if (HasFailure() || kSanitizerDivisor > 1) return;
  for (size_t i = 0; i < counts.size(); ++i) {
    EXPECT_GT(counts.at(i), 0U) << "no run reached: " << kSometimesNames.at(i);
  }
}

#ifndef _WIN32
constexpr const char* kDifferentialVictimEnv = "ABYSS_DIFFERENTIAL_VICTIM_DIR";

struct Journalled {
  char kind = 'C';
  int64_t at_ms = 0;
  std::vector<std::string> args;
};

std::vector<Journalled> ReadJournal(const std::filesystem::path& path) {
  std::vector<Journalled> records;
  std::ifstream in(path, std::ios::binary);
  std::string line;
  while (std::getline(in, line)) {
    std::istringstream fields(line);
    Journalled record;
    size_t count = 0;
    if (!(fields >> record.kind >> record.at_ms >> count)) break;
    for (size_t i = 0; i < count; ++i) {
      size_t length = 0;
      char colon = 0;
      fields.get();
      if (!(fields >> length) || !fields.get(colon) || colon != ':') return records;
      std::string arg(length, '\0');
      fields.read(arg.data(), static_cast<std::streamsize>(length));
      record.args.push_back(std::move(arg));
    }
    if (!fields) break;
    records.push_back(std::move(record));
  }
  return records;
}

// Runs a seed's stream with a journal until the parent kills it.
TEST_F(DifferentialTest, KillNineVictim) {
  bool is_victim = false;
  const auto dir = abyss::testing::VictimDirFromEnv(kDifferentialVictimEnv, &is_victim);
  if (!is_victim) GTEST_SKIP() << "crash victim; driven out-of-process by LongTierKillNine";
  abyss::testing::ExitWithParent(std::chrono::seconds(60));
  victim_dir_ = dir;
  journal_.open(dir / "journal", std::ios::binary | std::ios::trunc);
  const uint64_t seed = std::stoull(abyss::testing::crash_internal::ReadFileOrEmpty(dir / "seed"));
  Counts counts{};
  RunSeed(seed, 1'000'000, counts);
  abyss::testing::SignalReadyAndPark(dir, "victim.done", "");
}

// Opt-in: ABYSS_PROPERTY_KILLS=<count>. A seed's stream is killed with
// SIGKILL mid-run; recovery must hold the model's state after every
// command the victim finished, with or without the one in flight.
TEST_F(DifferentialTest, LongTierKillNine) {
  bool is_victim = false;
  (void)abyss::testing::VictimDirFromEnv(kDifferentialVictimEnv, &is_victim);
  if (is_victim) GTEST_SKIP();
  const auto kills = EnvNumber("ABYSS_PROPERTY_KILLS");
  if (!kills.has_value()) GTEST_SKIP() << "long tier: set ABYSS_PROPERTY_KILLS";
  const uint64_t first = EnvNumber("ABYSS_PROPERTY_FIRST_SEED").value_or(2000);
  for (uint64_t seed = first; seed < first + *kills; ++seed) {
    const auto dir = dir_.Sub("kill" + std::to_string(seed));
    std::filesystem::create_directories(dir);
    {
      std::ofstream out(dir / "seed");
      out << seed;
    }
    const auto outcome =
        abyss::testing::SpawnAndKillVictim({.gtest_filter = "DifferentialTest.KillNineVictim",
                                            .dir_env_var = kDifferentialVictimEnv,
                                            .dir = dir,
                                            .ready_deadline = std::chrono::seconds(10)});
    ASSERT_TRUE(outcome.error.empty()) << outcome.error;
    ASSERT_TRUE(outcome.reached_ready && outcome.died_by_signal);

    const auto records = ReadJournal(dir / "journal");
    Swarm swarm = SwarmFor(seed);
    swarm.options.run = dir.string() + "/";
    int64_t last_ms = 0;
    for (const auto& record : records) last_ms = std::max(last_ms, record.at_ms);
    ASSERT_TRUE(RecoverAt(swarm.options, last_ms).has_value()) << "seed " << seed;

    // The model after every finished command, then with the one in
    // flight too.
    model_.Clear();
    std::optional<Journalled> in_flight;
    for (size_t i = 0; i < records.size(); ++i) {
      const auto& record = records[i];
      if (record.kind != 'C') continue;
      // A write a load raced finished first: the command decided after.
      std::optional<std::string> reply;
      for (size_t j = i + 1; j < records.size(); ++j) {
        if (records[j].kind == 'N') (void)model_.Execute(records[j].args, records[j].at_ms);
        if (records[j].kind == 'R') {
          reply = records[j].args.at(0);
          break;
        }
        if (records[j].kind == 'C') break;
      }
      if (!reply.has_value()) {
        in_flight = record;
        continue;
      }
      if (!reply->starts_with("-OOM")) (void)model_.Execute(record.args, record.at_ms);
    }
    RedisModel without = model_;
    RedisModel with = model_;
    if (in_flight.has_value()) (void)with.Execute(in_flight->args, in_flight->at_ms);
    const auto mismatch = [this]() -> std::optional<std::string> {
      std::set<std::string> keys;
      for (int i = 0; i < kKeys; ++i) keys.insert("k" + std::to_string(i));
      for (const auto& [key, value] : model_.data()) keys.insert(key);
      for (const auto& key : keys) {
        if (auto seen = CheckKey(key); seen.mismatch.has_value()) return seen.mismatch;
      }
      return std::nullopt;
    };
    model_ = without;
    const auto first_try = mismatch();
    std::optional<std::string> second_try;
    if (first_try.has_value()) {
      model_ = with;
      second_try = mismatch();
    }
    std::cout << "[differential] kill -9 seed=" << seed << " " << swarm.text << ": "
              << records.size() << " journal records" << '\n'
              << std::flush;
    EXPECT_TRUE(!first_try.has_value() || !second_try.has_value())
        << "seed " << seed << ": recovery matches neither model: " << *first_try << " / "
        << second_try.value_or("");
    CloseAll();
    if (HasFailure()) break;
  }
}
#endif  // !_WIN32

// Opt-in: ABYSS_PROPERTY_SEEDS=<count> [ABYSS_PROPERTY_FIRST_SEED=<n>]
// [ABYSS_PROPERTY_OPS=<steps>].
TEST_F(DifferentialTest, LongTier) {
  const auto seeds = EnvNumber("ABYSS_PROPERTY_SEEDS");
  if (!seeds.has_value()) GTEST_SKIP() << "long tier: set ABYSS_PROPERTY_SEEDS";
  const uint64_t first = EnvNumber("ABYSS_PROPERTY_FIRST_SEED").value_or(1000);
  const int ops = static_cast<int>(EnvNumber("ABYSS_PROPERTY_OPS").value_or(3000));
  Counts counts{};
  const auto started = std::chrono::steady_clock::now();
  uint64_t run = 0;
  for (uint64_t seed = first; seed < first + *seeds; ++seed, ++run) {
    if (!RunSeed(seed, ops, counts)) break;
  }
  Report(counts);
  std::cout << "[differential] long tier: " << run << " seeds x " << ops << " steps in "
            << std::chrono::duration_cast<std::chrono::seconds>(std::chrono::steady_clock::now() -
                                                                started)
                   .count()
            << " s\n";
}

}  // namespace
}  // namespace abyss::engine

#endif  // ABYSS_HAVE_ROCKSDB
