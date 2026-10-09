// Concurrent clients on one overlapping keyspace, checked for
// linearizability against the reference model (Wing-Gong search with
// Lowe's cache, as Porcupine runs it). Keys come in fixed groups of
// three that span shards on one log; every multi-key write stays in its
// group, so groups are independent objects and each group's history is
// checked alone against the model restricted to it. MGET is per key
// and not atomic (#170): each of its keys is a single-key read within
// the MGET's window.
//
// The fake wall clock is frozen for each burst of operations and moved
// only with every client joined, past TTLs, so a burst's first touches
// race on observed expiries. A drainer absorbs and flushes cold, a
// maintainer evicts, and loads are slowed or raced by evictions.
//
// Replies are classified by exact text. A timeout after the write was
// logged (the durable wait) is indeterminate: it took effect inside its
// window or not at all. A timeout before anything was reserved, or
// -OOM, is a no-op. Any other error fails the test.
//
// ABYSS_LINEARIZABILITY_SEED=<n> replays a seed's configuration and
// stream (the interleaving is the scheduler's); the replay line names
// the threads and ops to pass with it. Long tier:
// ABYSS_LINEARIZABILITY_SEEDS=<count>
// [ABYSS_LINEARIZABILITY_THREADS=<n>]
// [ABYSS_LINEARIZABILITY_OPS=<per thread>].

#include <gtest/gtest.h>

#ifdef ABYSS_HAVE_ROCKSDB

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <iostream>
#include <latch>
#include <map>
#include <mutex>
#include <optional>
#include <set>
#include <sstream>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "abyss/core/ascii.h"
#include "linearizability.h"
#include "property_harness.h"

namespace abyss::engine {
namespace {

using abyss::testing::HistoryOp;

constexpr int kGroups = 6;
constexpr int kGroupKeys = 3;
constexpr int kBursts = 5;

struct Record {
  int client = 0;
  uint64_t call = 0;
  uint64_t ret = 0;
  std::vector<std::string> args;
  core::RespValue reply;
  int64_t at_ms = 0;
};

struct Outcome {
  uint64_t ops = 0;
  // Ops whose window overlaps another op's on the same group.
  uint64_t concurrent = 0;
  std::map<std::string, uint64_t> indeterminate;
  uint64_t no_ops = 0;
  uint64_t explored = 0;
  bool ok = true;
  std::string failure;
};

struct State {
  RedisModel model;
  // Never goes back: each op runs at its burst's frozen clock.
  int64_t now = 0;
};

constexpr std::string_view kNoOpReply = "<no-op>";

// Splits the history by key group and checks each group; `groups`
// names every key. MGET is split per key, FLUSHDB goes to every group.
Outcome CheckRecords(const std::vector<Record>& records,
                     const std::vector<std::vector<std::string>>& groups, uint64_t max_explored) {
  Outcome outcome;
  std::map<std::string, size_t> group_of;
  for (size_t g = 0; g < groups.size(); ++g) {
    for (const auto& key : groups[g]) group_of[key] = g;
  }
  const auto fail = [&outcome](std::string why) {
    if (!outcome.ok) return;
    outcome.ok = false;
    outcome.failure = std::move(why);
  };
  std::vector<std::vector<HistoryOp>> histories(groups.size());
  // The original records' windows per group, for the overlap count.
  std::vector<std::vector<std::pair<uint64_t, uint64_t>>> windows(groups.size());
  for (const auto& record : records) {
    ++outcome.ops;
    const std::string name = core::AsciiUpper(record.args.at(0));
    const ReplyKind kind = Classify(record.reply, IsWrite(name));
    if (kind == ReplyKind::kUnexpected) {
      std::string args;
      for (const auto& arg : record.args) args += " " + arg;
      fail("an unexpected error:" + args + " -> " + record.reply.AsString());
      continue;
    }
    HistoryOp op{.client = record.client,
                 .call = record.call,
                 .ret = record.ret,
                 .args = record.args,
                 .reply = RedisModel::Canonical(name, record.reply),
                 .at_ms = record.at_ms,
                 .indeterminate = kind == ReplyKind::kIndeterminate};
    if (kind == ReplyKind::kIndeterminate) {
      ++outcome.indeterminate[std::string(ErrorMessage(record.reply))];
    }
    if (kind == ReplyKind::kNoOp) {
      ++outcome.no_ops;
      op.reply = std::string(kNoOpReply);
    }
    if (name == "FLUSHDB") {
      for (auto& history : histories) history.push_back(op);
      continue;
    }
    if (name == "MGET") {
      // Per key, not atomic: each a read within the MGET's window.
      windows.at(group_of.at(record.args.at(1))).emplace_back(record.call, record.ret);
      for (size_t i = 1; i < record.args.size(); ++i) {
        HistoryOp one = op;
        one.args = {"MGET", record.args[i]};
        if (kind == ReplyKind::kDefinite) {
          one.reply = "[" + RedisModel::Render(record.reply.AsArray().at(i - 1)) + "]";
        }
        histories.at(group_of.at(record.args[i])).push_back(std::move(one));
      }
      continue;
    }
    const size_t g = group_of.at(record.args.at(1));
    windows.at(g).emplace_back(record.call, record.ret);
    histories.at(g).push_back(std::move(op));
  }
  for (auto& list : windows) {
    std::ranges::sort(list);
    uint64_t reach = 0;
    for (size_t i = 0; i < list.size(); ++i) {
      const bool before = i > 0 && list[i].first < reach;
      const bool after = i + 1 < list.size() && list[i + 1].first < list[i].second;
      if (before || after) ++outcome.concurrent;
      reach = std::max(reach, list[i].second);
    }
  }

  const auto step = [](State& state, const HistoryOp& op) {
    if (op.at_ms < state.now) return false;
    state.now = op.at_ms;
    if (op.reply == kNoOpReply) return true;
    const std::string reply =
        RedisModel::Canonical(op.args.at(0), state.model.Execute(op.args, op.at_ms));
    return op.indeterminate || reply == op.reply;
  };
  for (size_t g = 0; g < groups.size() && outcome.ok; ++g) {
    const std::vector<std::string>& keys = groups[g];
    const auto digest = [&keys](const State& state) {
      return std::to_string(state.now) + "|" + state.model.Digest(keys, state.now);
    };
    const auto result =
        abyss::testing::CheckLinearizable<State>(histories[g], State{}, step, digest, max_explored);
    outcome.explored += result.explored;
    if (result.ok) continue;
    std::ostringstream dump;
    dump << "group " << g << " {";
    for (const auto& key : keys) dump << " " << key;
    dump << " }: " << result.detail << "\nhistory (" << histories[g].size()
         << " ops; thread, call, return, at, command -> reply):";
    for (const auto& op : histories[g]) {
      dump << "\n  t" << op.client << " " << op.call << " " << op.ret
           << (op.indeterminate ? " timed-out" : "") << " @" << op.at_ms << " ";
      for (const auto& arg : op.args) dump << arg << " ";
      dump << "-> " << op.reply;
    }
    fail(dump.str());
  }
  return outcome;
}

class LinearizabilityTest : public PropertyHarness {
 protected:
  Outcome RunSeed(uint64_t seed, int threads, int ops_per_thread, uint64_t max_explored,
                  bool two_logs = false) {
    Swarm swarm = SwarmFor(seed);
    swarm.options.run = "lin" + std::to_string(seed) + "-";
    // Groups must span shards.
    if (swarm.options.shards < 4) {
      swarm.options.shards = 4;
      swarm.text += " (shards forced to 4)";
    }
    if (two_logs) {
      swarm.options.shards = 8;
      swarm.options.log_count = 2;
      swarm.text += " (pinned: 8 shards, 2 logs)";
    }
    const std::string replay = "ABYSS_LINEARIZABILITY_SEED=" + std::to_string(seed) +
                               " ABYSS_LINEARIZABILITY_THREADS=" + std::to_string(threads) +
                               " ABYSS_LINEARIZABILITY_OPS=" + std::to_string(ops_per_thread);
    std::cout << "[linearizability] seed=" << seed << " threads=" << threads
              << " ops/thread=" << ops_per_thread << " " << swarm.text << '\n'
              << std::flush;
    const auto start = std::chrono::duration_cast<std::chrono::milliseconds>(
                           core::WallClock::now().time_since_epoch())
                           .count();
    OpenAt(swarm.options, start - (start % 1000));
    PickGroups();
    for (const auto& group : groups_) {
      std::set<core::ShardId> shards;
      for (const auto& key : group) shards.insert(ShardOf(key));
      EXPECT_GE(shards.size(), 2U) << "a group on one shard tests no cross-shard write";
    }
    Rng rng(seed);
    hooked_.SetHook([this](std::string_view) { OnLoad(); });

    std::vector<Record> records;
    std::mutex records_mu;
    std::atomic<uint64_t> clock{1};
    const int per_burst = std::max(1, ops_per_thread / kBursts);
    for (int burst = 0; burst < kBursts; ++burst) {
      std::atomic<bool> done{false};
      std::thread drainer([this, &done, seed, burst] { Drain(done, (seed * 31) + burst); });
      std::thread maintainer([this, &done, seed, burst] { Maintain(done, (seed * 37) + burst); });
      std::vector<std::thread> clients;
      clients.reserve(static_cast<size_t>(threads));
      std::latch start(threads);
      for (int t = 0; t < threads; ++t) {
        clients.emplace_back([&, t] {
          start.arrive_and_wait();
          Rng mine((seed * 1000) + (static_cast<uint64_t>(burst) * 100) + static_cast<uint64_t>(t));
          resp::RequestPipeline pipeline(resp::GlobalRegistry(), resp::ConnectionState{},
                                         resp::PipelineDependencies{.dispatcher = engine_.get()});
          std::vector<Record> local;
          for (int i = 0; i < per_burst; ++i) {
            Record record{.client = t, .args = Generate(mine, t), .at_ms = now_ms_};
            record.call = clock.fetch_add(1);
            record.reply = pipeline.Dispatch(core::RespCommand{.args = record.args});
            record.ret = clock.fetch_add(1);
            local.push_back(std::move(record));
          }
          const std::scoped_lock lock(records_mu);
          for (auto& record : local) records.push_back(std::move(record));
        });
      }
      for (auto& client : clients) client.join();
      done = true;
      drainer.join();
      maintainer.join();
      // Quiescent: the clock moves only now, often past TTLs set in
      // the burst.
      now_ms_ += rng.Chance(0.5) ? rng.Between(0, 50) : rng.Between(500, 2500);
    }
    hooked_.SetHook(nullptr);
    Outcome outcome = CheckRecords(records, groups_, max_explored);
    // A few timeouts and refusals are expected on a loaded machine;
    // many are not, or a server refusing most ops would pass.
    uint64_t indeterminate = 0;
    for (const auto& [text, count] : outcome.indeterminate) indeterminate += count;
    if (outcome.ok && indeterminate > std::max<uint64_t>(5, outcome.ops / 50)) {
      outcome.ok = false;
      outcome.failure = std::to_string(indeterminate) + " of " + std::to_string(outcome.ops) +
                        " ops timed out after being logged";
    }
    if (outcome.ok && outcome.no_ops > std::max<uint64_t>(10, outcome.ops / 10)) {
      outcome.ok = false;
      outcome.failure = std::to_string(outcome.no_ops) + " of " + std::to_string(outcome.ops) +
                        " ops were refused or timed out before being logged";
    }
    if (!outcome.ok) {
      ADD_FAILURE() << "seed " << seed << " (" << swarm.text << "): " << outcome.failure
                    << "\nreplay: " << replay;
    }
    CloseAll();
    return outcome;
  }

  void PickGroups() {
    groups_.assign(kGroups, {});
    for (int g = 0; g < kGroups; ++g) {
      std::set<core::ShardId> used;
      std::optional<uint32_t> log;
      for (int n = 0; std::ssize(groups_[g]) < kGroupKeys; ++n) {
        std::string key = "g" + std::to_string(g) + "k" + std::to_string(n);
        const core::ShardId shard = ShardOf(key);
        if (log.has_value() && queue_->LogOf(shard) != *log) continue;
        // Spread over shards while there are fresh ones to take.
        if (used.contains(shard) && n < 200) continue;
        log = queue_->LogOf(shard);
        used.insert(shard);
        groups_[g].push_back(std::move(key));
      }
    }
  }

  // ---- the generator: one group per command

  std::vector<std::string> Generate(Rng& rng, int thread) {
    const auto& group = groups_.at(rng.Below(kGroups));
    const auto key = [&] { return group.at(rng.Below(group.size())); };
    const auto value = [&] {
      return "t" + std::to_string(thread) + "." + std::to_string(rng.Next() % 100000);
    };
    const auto member = [&] { return "m" + std::to_string(rng.Below(5)); };
    const auto num = [](int64_t n) { return std::to_string(n); };
    const uint64_t roll = rng.Below(1000);
    if (roll < 3) return {"FLUSHDB"};
    if (roll < 120) {
      std::vector<std::string> args = {"SET", key(), value()};
      const uint64_t cond = rng.Below(5);
      if (cond == 1) args.emplace_back("NX");
      if (cond == 2) args.emplace_back("XX");
      if (rng.Chance(0.2)) args.emplace_back("GET");
      if (rng.Chance(0.3)) args.insert(args.end(), {"PX", num(rng.Between(1, 2000))});
      return args;
    }
    if (roll < 150) return {"SETNX", key(), value()};
    if (roll < 200) {
      std::vector<std::string> args = {rng.Chance(0.6) ? "MSET" : "MSETNX"};
      for (const auto& k : group) {
        if (rng.Chance(0.7)) args.insert(args.end(), {k, value()});
      }
      if (args.size() == 1) args.insert(args.end(), {key(), value()});
      return args;
    }
    if (roll < 240) {
      std::vector<std::string> args = {"DEL", key()};
      if (rng.Chance(0.5)) args.push_back(key());
      return args;
    }
    if (roll < 300) return {rng.Chance(0.7) ? "SADD" : "SREM", key(), member(), member()};
    if (roll < 350) {
      std::vector<std::string> args = {"ZADD", key()};
      const uint64_t flag = rng.Below(6);
      if (flag == 1) args.emplace_back("NX");
      if (flag == 2) args.emplace_back("XX");
      if (flag == 3) args.emplace_back("GT");
      if (rng.Chance(0.3)) args.emplace_back("CH");
      args.insert(args.end(), {num(rng.Between(-3, 3)), member()});
      return args;
    }
    if (roll < 370) return {"ZREM", key(), member()};
    if (roll < 420) return {"HSET", key(), member(), value()};
    if (roll < 440) return {"HSETNX", key(), member(), value()};
    if (roll < 455) return {"HDEL", key(), member()};
    if (roll < 495) {
      std::vector<std::string> args = {"PEXPIRE", key(), num(rng.Between(-10, 2000))};
      constexpr std::array<std::string_view, 4> kFlags = {"NX", "XX", "GT", "LT"};
      if (rng.Chance(0.4)) args.emplace_back(kFlags.at(rng.Below(kFlags.size())));
      return args;
    }
    if (roll < 505) return {"PERSIST", key()};
    if (roll < 535) {
      const std::string a = key();
      std::string b = key();
      if (rng.Chance(0.5)) return {"RENAMENX", a, b};
      if (a == b) return {"COPY", a, group.at((group_index(a, group) + 1) % group.size())};
      if (rng.Chance(0.5)) return {"COPY", a, b, "REPLACE"};
      return {"COPY", a, b};
    }
    switch (rng.Below(12)) {
      case 0:
      case 1:
        return {"GET", key()};
      case 2: {
        std::vector<std::string> args = {"MGET"};
        for (const auto& k : group) args.push_back(k);
        return args;
      }
      case 3:
        return {"EXISTS", key()};
      case 4:
        return {"TYPE", key()};
      case 5:
        return {"PTTL", key()};
      case 6:
        return {"SMEMBERS", key()};
      case 7:
        return {"SCARD", key()};
      case 8:
        return {"ZRANGE", key(), "0", "-1", "WITHSCORES"};
      case 9:
        return {"HGETALL", key()};
      case 10:
        return {"HGET", key(), member()};
      default:
        return {"SISMEMBER", key(), member()};
    }
  }
  static size_t group_index(const std::string& key, const std::vector<std::string>& group) {
    for (size_t i = 0; i < group.size(); ++i) {
      if (group[i] == key) return i;
    }
    return 0;
  }

  // ---- background: cold absorbs and flushes, hot evicts

  void Drain(const std::atomic<bool>& done, uint64_t seed) {
    Rng rng(seed);
    while (!done.load()) {
      for (core::ShardId shard = 0; shard < options_.shards; ++shard) {
        Absorb(shard, rng.Chance(0.5) ? 1 + rng.Below(4) : SIZE_MAX);
        if (rng.Chance(0.2)) FlushCold(shard);
      }
      std::this_thread::sleep_for(std::chrono::microseconds(rng.Between(50, 400)));
    }
  }

  void Maintain(const std::atomic<bool>& done, uint64_t seed) {
    Rng rng(seed);
    while (!done.load()) {
      switch (rng.Below(4)) {
        case 0:
          hot_->EvictExpired(core::SteadyClock::now() + std::chrono::hours{1'000'000});
          break;
        case 1:
          hot_->EvictToMemoryTarget();
          break;
        case 2:
          hot_->GcTombstones();
          break;
        default:
          hot_->EvictExpired(core::SteadyClock::now());
          break;
      }
      std::this_thread::sleep_for(std::chrono::microseconds(rng.Between(100, 1000)));
    }
  }

  // A load is slowed, or raced by an eviction of everything drained.
  void OnLoad() {
    thread_local Rng rng(std::hash<std::thread::id>{}(std::this_thread::get_id()));
    const uint64_t roll = rng.Below(10);
    if (roll < 3) {
      std::this_thread::sleep_for(std::chrono::microseconds(rng.Between(10, 300)));
    } else if (roll < 5) {
      hot_->EvictExpired(core::SteadyClock::now() + std::chrono::hours{1'000'000});
    }
  }

  // NOLINTBEGIN(cppcoreguidelines-non-private-member-variables-in-classes)
  std::vector<std::vector<std::string>> groups_;
  // NOLINTEND(cppcoreguidelines-non-private-member-variables-in-classes)
};

void Add(Outcome& total, const Outcome& outcome) {
  total.ops += outcome.ops;
  total.concurrent += outcome.concurrent;
  total.no_ops += outcome.no_ops;
  total.explored += outcome.explored;
  for (const auto& [text, count] : outcome.indeterminate) total.indeterminate[text] += count;
}

void Report(const std::string& tier, uint64_t seeds, const Outcome& total,
            std::chrono::steady_clock::duration took) {
  std::cout << "[linearizability] " << tier << ": " << seeds << " seeds, " << total.ops << " ops ("
            << total.concurrent << " overlapping another on their group), " << total.no_ops
            << " no-ops, " << total.explored << " states searched, "
            << std::chrono::duration_cast<std::chrono::milliseconds>(took).count() << " ms\n";
  for (const auto& [text, count] : total.indeterminate) {
    std::cout << "[linearizability]   timed out after logging: " << count << " x " << text << '\n';
  }
  std::cout << std::flush;
}

TEST_F(LinearizabilityTest, ConcurrentClientsAreLinearizablePerKeyGroup) {
  const int threads = static_cast<int>(EnvNumber("ABYSS_LINEARIZABILITY_THREADS").value_or(4));
  const int ops =
      static_cast<int>(EnvNumber("ABYSS_LINEARIZABILITY_OPS").value_or(200 / kSanitizerDivisor));
  const auto started = std::chrono::steady_clock::now();
  Outcome total;
  std::vector<uint64_t> seeds;
  if (const auto seed = EnvNumber("ABYSS_LINEARIZABILITY_SEED")) {
    seeds = {*seed};
  } else {
    seeds = {1, 2, 3};
    if (kSanitizerDivisor > 1) seeds = {3};
  }
  for (const uint64_t seed : seeds) {
    // Seed 3 runs on two logs, so groups exercise CROSSSLOT-free
    // multi-key writes on the second.
    const Outcome outcome = RunSeed(seed, threads, ops, 2'000'000, seed == 3);
    Add(total, outcome);
    if (!outcome.ok) break;
  }
  Report("bounded tier", seeds.size(), total, std::chrono::steady_clock::now() - started);
  // Clients that ran one at a time overlap almost nothing; on a loaded
  // machine about half still overlap. A sanitizer's few ops vary too
  // much to judge.
  if (!HasFailure() && kSanitizerDivisor == 1) {
    EXPECT_GE(total.concurrent * 4, total.ops) << "too few ops overlapped to test concurrency";
  }
}

TEST_F(LinearizabilityTest, LongTier) {
  const auto seeds = EnvNumber("ABYSS_LINEARIZABILITY_SEEDS");
  if (!seeds.has_value()) GTEST_SKIP() << "long tier: set ABYSS_LINEARIZABILITY_SEEDS";
  const auto threads = static_cast<int>(EnvNumber("ABYSS_LINEARIZABILITY_THREADS").value_or(8));
  const auto ops = static_cast<int>(EnvNumber("ABYSS_LINEARIZABILITY_OPS").value_or(1000));
  const uint64_t first = EnvNumber("ABYSS_LINEARIZABILITY_FIRST_SEED").value_or(1000);
  const auto started = std::chrono::steady_clock::now();
  Outcome total;
  uint64_t run = 0;
  for (uint64_t seed = first; seed < first + *seeds; ++seed, ++run) {
    const Outcome outcome = RunSeed(seed, threads, ops, 20'000'000, seed % 4 == 0);
    Add(total, outcome);
    if (!outcome.ok) break;
  }
  Report("long tier", run, total, std::chrono::steady_clock::now() - started);
}

// ---- the checker, end to end over records, with no engine

Record At(int client, uint64_t call, uint64_t ret, std::vector<std::string> args,
          core::RespValue reply, int64_t at_ms = 0) {
  return {.client = client,
          .call = call,
          .ret = ret,
          .args = std::move(args),
          .reply = std::move(reply),
          .at_ms = at_ms};
}

core::RespValue Ok() { return core::RespValue::SimpleString("OK"); }
core::RespValue Bulk(std::string text) { return core::RespValue::BulkString(std::move(text)); }
core::RespValue Error(core::ErrorPrefix prefix, std::string text) {
  return core::RespValue::Error(prefix, std::move(text));
}

// One group of two keys.
std::vector<std::vector<std::string>> AB() { return {{"a", "b"}}; }

TEST(LinearizabilityCheckerTest, AcceptsAnOverlappingReadAndRejectsAStaleOne) {
  std::vector<Record> history = {At(0, 1, 2, {"SET", "a", "1"}, Ok()),
                                 At(1, 3, 4, {"GET", "a"}, core::RespValue::Null())};
  EXPECT_FALSE(CheckRecords(history, AB(), 1000).ok);
  history[1].call = 0;
  EXPECT_TRUE(CheckRecords(history, AB(), 1000).ok);
}

// The positive control: a reader that sees an MSET's second key, then
// misses its first, saw it torn.
TEST(LinearizabilityCheckerTest, ATornMsetFails) {
  const std::vector<Record> history = {
      At(0, 1, 10, {"MSET", "a", "2", "b", "2"}, Ok()),
      At(1, 2, 3, {"MGET", "b", "a"}, core::RespValue::Array({Bulk("2"), core::RespValue::Null()})),
  };
  EXPECT_TRUE(CheckRecords(history, AB(), 1000).ok) << "MGET is not atomic: this is legal";
  const std::vector<Record> torn = {
      At(0, 1, 10, {"MSET", "a", "2", "b", "2"}, Ok()),
      At(1, 2, 3, {"GET", "b"}, Bulk("2")),
      At(1, 4, 5, {"GET", "a"}, core::RespValue::Null()),
  };
  EXPECT_FALSE(CheckRecords(torn, AB(), 1000).ok);
}

// A write that timed out after it was logged landed inside its window
// or not at all: never before its call.
TEST(LinearizabilityCheckerTest, ATimedOutWriteSeenBeforeItsCallFails) {
  const auto timed_out = Error(core::ErrorPrefix::kErr,
                               "write durable wait exceeded server timeout; the write is applied "
                               "and may yet become durable");
  const std::vector<Record> history = {At(0, 10, 11, {"SET", "a", "x"}, timed_out),
                                       At(1, 1, 2, {"GET", "a"}, Bulk("x"))};
  EXPECT_FALSE(CheckRecords(history, AB(), 1000).ok);
  const std::vector<Record> after = {At(0, 1, 2, {"SET", "a", "x"}, timed_out),
                                     At(1, 3, 4, {"GET", "a"}, Bulk("x"))};
  EXPECT_TRUE(CheckRecords(after, AB(), 1000).ok);
}

// A timed-out PEXPIRE from an earlier burst cannot land after a later
// burst's SET, at the earlier burst's clock, to explain a lost write.
TEST(LinearizabilityCheckerTest, ATimedOutOpCannotLandAfterItsWindow) {
  const auto timed_out = Error(core::ErrorPrefix::kErr,
                               "write durable wait exceeded server timeout; the write is applied "
                               "and may yet become durable");
  const std::vector<Record> history = {
      At(0, 1, 2, {"SET", "a", "v0"}, Ok(), 1000),
      At(0, 3, 4, {"PEXPIRE", "a", "100"}, timed_out, 1000),
      At(1, 5, 6, {"SET", "a", "v"}, Ok(), 3000),
      At(1, 7, 8, {"GET", "a"}, core::RespValue::Null(), 3000),
  };
  EXPECT_FALSE(CheckRecords(history, AB(), 1000).ok);
  // Within one burst, where the clock alone would not stop it.
  const std::vector<Record> one_burst = {
      At(0, 1, 2, {"SET", "a", "v0"}, Ok(), 1000),
      At(0, 3, 4, {"PEXPIRE", "a", "0"}, timed_out, 1000),
      At(1, 5, 6, {"SET", "a", "v"}, Ok(), 1000),
      At(1, 7, 8, {"GET", "a"}, core::RespValue::Null(), 1000),
  };
  EXPECT_FALSE(CheckRecords(one_burst, AB(), 1000).ok);
}

// -OOM refused the write before anything was reserved: a read that
// shows its value saw a write that never happened.
TEST(LinearizabilityCheckerTest, AReadObservingAnOomWriteFails) {
  const std::vector<Record> history = {
      At(0, 1, 2, {"SET", "a", "y"},
         Error(core::ErrorPrefix::kOom,
               "command not allowed when hot memory is over its limit and cold is behind")),
      At(1, 3, 4, {"GET", "a"}, Bulk("y"))};
  EXPECT_FALSE(CheckRecords(history, AB(), 1000).ok);
}

TEST(LinearizabilityCheckerTest, AnUnexpectedErrorFails) {
  const std::vector<Record> history = {
      At(0, 1, 2, {"SET", "a", "y"}, Error(core::ErrorPrefix::kErr, "internal server error"))};
  EXPECT_FALSE(CheckRecords(history, AB(), 1000).ok);
}

// Running out of search is an unknown, and an unknown fails.
TEST(LinearizabilityCheckerTest, AnExhaustedSearchFails) {
  const std::vector<Record> history = {At(0, 1, 2, {"SET", "a", "1"}, Ok()),
                                       At(1, 3, 4, {"GET", "a"}, Bulk("1"))};
  EXPECT_TRUE(CheckRecords(history, AB(), 1000).ok);
  const Outcome exhausted = CheckRecords(history, AB(), 1);
  EXPECT_FALSE(exhausted.ok);
  EXPECT_TRUE(exhausted.failure.contains("budget")) << exhausted.failure;
}

}  // namespace
}  // namespace abyss::engine

#endif  // ABYSS_HAVE_ROCKSDB
