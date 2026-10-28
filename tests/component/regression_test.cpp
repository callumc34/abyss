// One test per bug the sequenced write path fixed, each against the new
// path and each failing on the resolver design it replaced.

#include <gtest/gtest.h>

#ifdef ABYSS_HAVE_ROCKSDB

#include <atomic>
#include <chrono>
#include <cstdint>
#include <exception>
#include <map>
#include <mutex>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <variant>
#include <vector>

#include "abyss/consumer/cold_consumer.h"
#include "abyss/core/cold_store.h"
#include "abyss/core/command_dispatcher.h"
#include "abyss/core/durability.h"
#include "abyss/core/ops.h"
#include "abyss/core/queue_entry.h"
#include "abyss/core/types.h"
#include "abyss/engine/loader.h"
#include "abyss/hot/single_shard_store.h"
#include "cold_read.h"
#include "hooked_cold_store.h"
#include "sequenced_engine_fixture.h"

namespace abyss::engine {
namespace {

using RegressionTest = SequencedEngineTest;

// #167: a conditional's effect is logged where it was decided, so cold
// ends where hot does.
TEST_F(RegressionTest, Issue167AConditionalsEffectLandsAtItsPosition) {
  Open();
  EXPECT_EQ(Write({"SET", "k", "a"}), "OK");
  EXPECT_EQ(Write({"SET", "k", "b", "XX"}, Flags::kXx), "OK");
  EXPECT_EQ(Write({"SET", "k", "c"}), "OK");
  const auto logged = Logged(ShardOf("k"));
  ASSERT_EQ(logged.size(), 3U);
  EXPECT_EQ(ArgsOf(logged[1]), (std::vector<std::string>{"SET", "k", "b"}));
  EXPECT_EQ(ArgsOf(logged[2]), (std::vector<std::string>{"SET", "k", "c"}));

  DrainToCold(ShardOf("k"));
  EXPECT_EQ(ColdGet("k"), "c");
}

// #163: a write to a collection hot evicted loads its whole state first.
TEST_F(RegressionTest, Issue163AWriteToAnEvictedCollectionSeesItsFullState) {
  Open();
  EXPECT_EQ(Write({"SADD", "s", "a", "b", "c"}), ":3");
  DrainToCold(ShardOf("s"));
  EvictDrained();
  ASSERT_FALSE(hot_->Read(core::ops::ReadOp{core::ops::SetCard{.key = "s"}}).result.has_value())
      << "still resident";

  EXPECT_EQ(Write({"SADD", "s", "c", "d"}), ":1");
  EXPECT_EQ(Read({"SCARD", "s"}), ":4");
  EXPECT_EQ(Write({"SREM", "s", "a", "z"}), ":1");
  EXPECT_EQ(Read({"SCARD", "s"}), ":3");
}

// #165: MSETNX over two shards is one decision and one batch: a reader
// that sees its second key sees its first.
TEST_F(RegressionTest, Issue165ACrossShardMsetnxIsAtomic) {
  Open();
  constexpr int kRounds = 300;
  std::atomic<int> written{-1};
  std::atomic<int> torn{0};
  std::atomic<bool> done{false};
  std::thread reader([&] {
    while (!done.load()) {
      const int upto = written.load();
      for (int i = 0; i <= upto; ++i) {
        const std::string b = KeyOn(1, i, "b");
        const std::string a = KeyOn(0, i, "a");
        if (Read({"GET", b}) != "nil" && Read({"GET", a}) == "nil") torn.fetch_add(1);
      }
    }
  });
  for (int i = 0; i < kRounds; ++i) {
    ASSERT_EQ(Write({"MSETNX", KeyOn(0, i, "a"), "v", KeyOn(1, i, "b"), "v"}, Flags::kNx), ":1");
    written = i;
  }
  done = true;
  reader.join();
  EXPECT_EQ(torn.load(), 0) << "a reader saw one key of an MSETNX without the other";
  EXPECT_EQ(Write({"MSETNX", KeyOn(0, 0, "a"), "w", KeyOn(1, 0, "fresh"), "w"}, Flags::kNx), ":0");
  EXPECT_EQ(Read({"GET", KeyOn(1, 0, "fresh")}), "nil") << "a refused MSETNX set a key";
}

// #165: RENAMENX over two shards moves the key in one batch: a reader
// never sees it at both names, and the whole value moves.
TEST_F(RegressionTest, Issue165ACrossShardRenamenxIsAtomic) {
  Open();
  constexpr int kRounds = 300;
  std::atomic<int> renamed{-1};
  std::atomic<int> torn{0};
  std::atomic<bool> done{false};
  std::thread reader([&] {
    while (!done.load()) {
      const int upto = renamed.load();
      for (int i = 0; i <= upto + 1; ++i) {
        const std::string dst = KeyOn(1, i, "dst");
        const std::string src = KeyOn(0, i, "src");
        if (Read({"SCARD", dst}) != ":0" && Read({"SCARD", src}) != ":0") torn.fetch_add(1);
      }
    }
  });
  for (int i = 0; i < kRounds; ++i) {
    const std::string src = KeyOn(0, i, "src");
    ASSERT_EQ(Write({"SADD", src, "a", "b", "c"}), ":3");
    ASSERT_EQ(Write({"RENAMENX", src, KeyOn(1, i, "dst")}, Flags::kNx), ":1");
    renamed = i;
  }
  done = true;
  reader.join();
  EXPECT_EQ(torn.load(), 0) << "a reader saw a renamed key at both names";
  EXPECT_EQ(Members(KeyOn(1, 0, "dst")), (std::vector<std::string>{"a", "b", "c"}));
}

// #165: COPY over two shards decides on the source as it is when the
// copy runs, whatever tier holds it, and only one of two racing copies
// to an absent destination wins.
TEST_F(RegressionTest, Issue165ACrossShardCopyIsAtomic) {
  Open();
  const std::string src = KeyOn(0, 0, "src");
  for (int i = 0; i < 100; ++i) {
    const std::string value = "v" + std::to_string(i);
    ASSERT_EQ(Write({"SET", src, value}), "OK");
    if (i % 10 == 0) {
      DrainToCold(0);
      EvictDrained();
    }
    const std::string dst = KeyOn(1, i, "dst");
    ASSERT_EQ(Write({"COPY", src, dst}, Flags::kNx), ":1");
    ASSERT_EQ(Read({"GET", dst}), value) << "copied a source older than its last write";
  }
  for (int i = 0; i < 50; ++i) {
    const std::string dst = KeyOn(1, i, "race");
    std::atomic<int> won{0};
    std::thread first([&] { won += Write({"COPY", src, dst}, Flags::kNx) == ":1" ? 1 : 0; });
    std::thread second([&] { won += Write({"COPY", src, dst}, Flags::kNx) == ":1" ? 1 : 0; });
    first.join();
    second.join();
    EXPECT_EQ(won.load(), 1) << dst;
  }
}

// #194 and #195. "a" and "b" are on different shards of four, as in
// #194's failing run; the resolver logged a cross-shard conditional's
// effects on its first key's shard, behind no lock of the others.
class CrossShardConditionalTest : public SequencedEngineTest {
 protected:
  using Args = std::vector<std::vector<std::string>>;

  // a and b exist, so DEL a b logs a DEL on each key's shard; the
  // MSETNX after it sets both.
  void DelThenMsetnx(Options options = {}) {
    Open(std::move(options));
    ASSERT_NE(ShardOf("a"), ShardOf("b"));
    ASSERT_EQ(Write({"SET", "a", "0"}), "OK");
    ASSERT_EQ(Write({"SET", "b", "0"}), "OK");
    ASSERT_EQ(Write({"DEL", "a", "b"}), ":2");
    ASSERT_EQ(Write({"MSETNX", "a", "1", "b", "2"}, Flags::kNx), ":1");
  }

  // Absorbs `shard`'s log into its buffer, flushing none of it.
  void Absorb(core::ShardId shard) {
    auto& consumer = pool_->ConsumerFor(shard);
    const core::SequenceId end = queue_->DurableEnd(shard, core::Durability::kProcessCrash).value();
    for (int round = 0; round < 200 && consumer.LatestDrainedSeq() + 1 < end; ++round) {
      if (consumer.Drain() == 0) std::this_thread::sleep_for(5ms);
    }
    ASSERT_GE(consumer.LatestDrainedSeq() + 1, end) << "shard " << shard << " never absorbed";
  }
  // Flushes everything `shard`'s buffer holds to cold.
  void FlushBuffer(core::ShardId shard) {
    auto& consumer = pool_->ConsumerFor(shard);
    for (int round = 0; round < 200 && consumer.Buffer().Size() > 0; ++round) {
      if (consumer.FlushUnscheduled() != consumer::ColdConsumer::FlushOutcome::kProgress) {
        std::this_thread::sleep_for(5ms);
      }
    }
    ASSERT_EQ(consumer.Buffer().Size(), 0U) << "shard " << shard << " never flushed";
  }

  // Hot's own state of string `key`, reading no other tier.
  std::string HotGet(std::string_view key) {
    auto locks = hot_->LockExclusive(std::vector<core::ShardId>{ShardOf(key)});
    const hot::KeyView view = locks.View(key, 0);
    if (view.presence != hot::KeyView::Presence::kLive) {
      return "presence " + std::to_string(static_cast<int>(view.presence));
    }
    const auto* value = std::get_if<std::string>(view.value);
    return value != nullptr ? *value : "not a string";
  }

  Args LoggedArgs(core::ShardId shard) {
    Args out;
    for (const auto& entry : Logged(shard)) out.push_back(ArgsOf(entry));
    return out;
  }
};

// #194: each of the MSETNX's effects is logged on its own key's shard,
// after that key's DEL, so recovery rebuilds hot with a = 1 and b = 2
// whichever of the two shards it replays first.
TEST_F(CrossShardConditionalTest, Issue194HotReplayKeepsTheMsetnxInEitherShardOrder) {
  for (const bool a_first : {false, true}) {
    SCOPED_TRACE(a_first ? "a's shard replayed first" : "b's shard replayed first");
    ASSERT_NO_FATAL_FAILURE(DelThenMsetnx(Options{.run = a_first ? "a-first-" : "b-first-"}));
    const core::ShardId a = ShardOf("a");
    const core::ShardId b = ShardOf("b");
    for (core::ShardId shard = 0; shard < options_.shards; ++shard) {
      for (const auto& args : LoggedArgs(shard)) {
        EXPECT_EQ(ShardOf(args.at(1)), shard) << args.at(0) << " " << args.at(1);
      }
    }
    EXPECT_EQ(LoggedArgs(a), (Args{{"SET", "a", "0"}, {"DEL", "a"}, {"SET", "a", "1"}}));
    EXPECT_EQ(LoggedArgs(b), (Args{{"SET", "b", "0"}, {"DEL", "b"}, {"SET", "b", "2"}}));

    replay_order_ = a_first ? std::vector<core::ShardId>{a, b} : std::vector<core::ShardId>{b, a};
    ASSERT_TRUE(Restart().has_value());
    replay_order_.clear();
    EXPECT_EQ(HotGet("a"), "1");
    EXPECT_EQ(HotGet("b"), "2");
    EXPECT_EQ(Read({"GET", "b"}), "2");
    CloseAll();
  }
}

// #194: each shard's buffer absorbs only its own keys' effects, so cold
// ends with a = 1 and b = 2 whichever buffer flushes first.
TEST_F(CrossShardConditionalTest, Issue194ColdKeepsTheMsetnxInEitherFlushOrder) {
  for (const bool a_first : {false, true}) {
    SCOPED_TRACE(a_first ? "a's buffer flushed first" : "b's buffer flushed first");
    ASSERT_NO_FATAL_FAILURE(DelThenMsetnx(Options{.run = a_first ? "a-first-" : "b-first-"}));
    const core::ShardId a = ShardOf("a");
    const core::ShardId b = ShardOf("b");
    ASSERT_NO_FATAL_FAILURE(Absorb(a));
    ASSERT_NO_FATAL_FAILURE(Absorb(b));
    for (const core::ShardId shard : a_first ? std::vector{a, b} : std::vector{b, a}) {
      ASSERT_NO_FATAL_FAILURE(FlushBuffer(shard));
    }
    EXPECT_EQ(ColdGet("a"), "1");
    EXPECT_EQ(ColdGet("b"), "2");
    CloseAll();
  }
}

// #194, steady state: hot evicts b before its buffer flushes, and a
// read of b is answered by that buffer's delta over cold's older b.
TEST_F(CrossShardConditionalTest, Issue194AnEvictedKeyReadsTheMsetnxFromItsBuffer) {
  Open();
  const core::ShardId b = ShardOf("b");
  ASSERT_EQ(Write({"SET", "a", "0"}), "OK");
  ASSERT_EQ(Write({"SET", "b", "0"}), "OK");
  DrainToCold(b);
  ASSERT_EQ(Write({"DEL", "a", "b"}), ":2");
  ASSERT_EQ(Write({"MSETNX", "a", "1", "b", "2"}, Flags::kNx), ":1");
  ASSERT_NO_FATAL_FAILURE(Absorb(b));
  EvictDrained();
  ASSERT_FALSE(Resident("b"));
  ASSERT_EQ(ColdGet("b"), "0") << "cold was meant to be behind";

  auto source = Loader::Source::kCold;
  auto loaded =
      loader_->LoadAs(b, "b", core::KeyType::kString, core::SteadyClock::now() + 5s, &source);
  ASSERT_TRUE(loaded.has_value()) << loaded.error().message();
  const auto* full = std::get_if<hot::LoadedFull>(loaded->get());
  ASSERT_NE(full, nullptr);
  EXPECT_EQ(std::get<std::string>(full->value), "2");
  EXPECT_EQ(source, Loader::Source::kBuffer);
  EXPECT_EQ(Read({"GET", "b"}), "2");
  EXPECT_EQ(Read({"GET", "a"}), "1");
}

// #194: once a cross-shard MSETNX has set b, SETNX b is refused and
// logs nothing, whether b is in hot, only in its buffer, or only in
// cold. With no stub held, an evicted b's existence is read from them.
TEST_F(CrossShardConditionalTest, Issue194ASetnxAfterACrossShardMsetnxIsRefusedInEveryTier) {
  for (const std::string tier : {"hot", "buffer", "cold"}) {
    SCOPED_TRACE("b in " + tier);
    Open(Options{.stub_memory_fraction = 0.0, .run = tier + "-"});
    const core::ShardId b = ShardOf("b");
    ASSERT_NE(ShardOf("a"), b);
    ASSERT_EQ(Write({"SET", "b", "0"}), "OK");
    ASSERT_EQ(Write({"DEL", "b"}), ":1");
    ASSERT_EQ(Write({"MSETNX", "a", "1", "b", "2"}, Flags::kNx), ":1");
    if (tier != "hot") {
      if (tier == "buffer") {
        ASSERT_NO_FATAL_FAILURE(Absorb(b));
      } else {
        DrainToCold(b);
      }
      EvictDrained();
      ASSERT_FALSE(Resident("b"));
      ASSERT_EQ(ColdGet("b"), tier == "cold" ? "2" : "nil");
    }
    const size_t logged = Logged(b).size();
    EXPECT_EQ(Write({"SETNX", "b", "x"}, Flags::kNx), ":0");
    EXPECT_EQ(Logged(b).size(), logged) << "a refused SETNX logged an effect";
    EXPECT_EQ(Read({"GET", "b"}), "2");
    DrainToCold(b);
    EXPECT_EQ(ColdGet("b"), "2");
    CloseAll();
  }
}

// #195: a conditional decides on a key only cold holds as Redis does:
// the NX forms refuse it, SET XX replaces it. With no stub held, each
// decision reads the key's existence from cold through the loader, and
// a refusal leaves cold's value as it was.
TEST_F(CrossShardConditionalTest, Issue195ConditionalsSeeAKeyOnlyColdHolds) {
  std::mutex mu;
  std::map<std::string, int> cold_reads;
  abyss::testing::HookedColdStore counted([this]() -> core::ColdStore& { return *cold_; });
  counted.SetHook([&mu, &cold_reads](std::string_view key) {
    const std::scoped_lock lock(mu);
    ++cold_reads[std::string(key)];
  });
  const auto reads_of = [&mu, &cold_reads](const std::string& key) {
    const std::scoped_lock lock(mu);
    return cold_reads[key];
  };
  loader_cold_ = &counted;
  Open(Options{.stub_memory_fraction = 0.0});
  const std::vector<std::string> strings = {"setnx",  "msetnx",   "set-nx",
                                            "set-xx", "renamenx", "copy"};
  for (const auto& key : strings) ASSERT_EQ(Write({"SET", key, "cold"}), "OK");
  ASSERT_EQ(Write({"HSET", "hsetnx", "f", "cold"}), ":1");
  for (core::ShardId shard = 0; shard < options_.shards; ++shard) DrainToCold(shard);
  EvictDrained();
  for (const auto& key : strings) {
    ASSERT_FALSE(Resident(key)) << key;
    ASSERT_EQ(ColdGet(key), "cold") << key;
  }
  ASSERT_FALSE(Resident("hsetnx"));
  ASSERT_EQ(Write({"SET", "rename-src", "hot"}), "OK");
  ASSERT_EQ(Write({"SET", "copy-src", "hot"}), "OK");

  struct Case {
    std::vector<std::string> cmd;
    Flags flags;
    std::string key;
    std::string reply;
  };
  const std::vector<Case> cases = {
      {{"SETNX", "setnx", "x"}, Flags::kNx, "setnx", ":0"},
      {{"MSETNX", "fresh", "x", "msetnx", "x"}, Flags::kNx, "msetnx", ":0"},
      {{"SET", "set-nx", "x", "NX"}, Flags::kNx, "set-nx", "nil"},
      {{"SET", "set-xx", "x", "XX"}, Flags::kXx, "set-xx", "OK"},
      {{"RENAMENX", "rename-src", "renamenx"}, Flags::kNx, "renamenx", ":0"},
      {{"COPY", "copy-src", "copy"}, Flags::kNone, "copy", ":0"},
      {{"HSETNX", "hsetnx", "f", "x"}, Flags::kNx, "hsetnx", ":0"},
  };
  for (const Case& c : cases) {
    SCOPED_TRACE(c.cmd.front());
    const int before = reads_of(c.key);
    EXPECT_EQ(Write(c.cmd, c.flags), c.reply);
    EXPECT_GT(reads_of(c.key), before) << "decided without reading cold";
  }

  EXPECT_EQ(Read({"GET", "fresh"}), "nil") << "a refused MSETNX set a key";
  EXPECT_EQ(Read({"GET", "rename-src"}), "hot") << "a refused RENAMENX moved its source";
  EXPECT_EQ(Read({"GET", "copy-src"}), "hot");
  for (core::ShardId shard = 0; shard < options_.shards; ++shard) DrainToCold(shard);
  for (const auto& key : strings) {
    EXPECT_EQ(ColdGet(key), key == "set-xx" ? "x" : "cold") << key;
    EXPECT_EQ(Read({"GET", key}), key == "set-xx" ? "x" : "cold") << key;
  }
  auto field = abyss::testing::ColdRead(*cold_, core::ops::HashGet{.key = "hsetnx", .field = "f"});
  ASSERT_TRUE(field.has_value()) << field.error().message();
  EXPECT_EQ(Describe(*field), "cold");
  // The loader reads through `counted`: close before it goes.
  CloseAll();
}

// #168: a read that fills hot from cold, racing a write to the same
// key, never rolls the key back to the cold value it loaded.
TEST_F(RegressionTest, Issue168ACacheFillRacingAWriteNeverRollsColdBack) {
  Open(Options{.fill_doorkeeper = false});
  for (int i = 0; i < 100; ++i) {
    const std::string key = KeyOn(static_cast<core::ShardId>(i % 4), i, "k");
    ASSERT_EQ(Write({"SET", key, "old"}), "OK");
    DrainToCold(ShardOf(key));
    EvictDrained();
    std::thread reader([this, &key] { (void)Read({"GET", key}); });
    std::thread writer([this, &key] { EXPECT_EQ(Write({"SET", key, "new"}), "OK"); });
    reader.join();
    writer.join();
    EXPECT_EQ(Read({"GET", key}), "new") << "a fill installed what the write replaced";
    DrainToCold(ShardOf(key));
    EvictDrained();
    EXPECT_EQ(Read({"GET", key}), "new") << "cold was rolled back";
  }
}

// #126: eviction waits for cold to drain the key; before then the key
// stays resident, so a read never meets a cold store behind it.
TEST_F(RegressionTest, Issue126EvictionWaitsForColdDrain) {
  Open();
  ASSERT_EQ(Write({"SET", "k", "v"}), "OK");
  ASSERT_EQ(Write({"SADD", "s", "a"}), ":1");
  EvictDrained();
  hot_->EvictToMemoryTarget();
  EXPECT_TRUE(Resident("k")) << "evicted before cold drained it";
  EXPECT_TRUE(Resident("s"));
  EXPECT_EQ(Read({"GET", "k"}), "v");
  EXPECT_EQ(ColdGet("k"), "nil") << "cold was meant to be behind";

  DrainToCold(ShardOf("k"));
  DrainToCold(ShardOf("s"));
  EvictDrained();
  EXPECT_FALSE(Resident("k"));
  EXPECT_EQ(Read({"GET", "k"}), "v");
  EXPECT_EQ(Members("s"), std::vector<std::string>{"a"});
}

// FLUSHDB used to cancel conditionals waiting on the resolver, and the
// reply path threw std::future_error on a reactor thread. Now it races
// conditionals with every reply well formed and nothing thrown.
TEST_F(RegressionTest, FlushDbRacingConditionalsNeverThrows) {
  Open();
  std::atomic<bool> stop{false};
  std::atomic<int> bad{0};
  std::atomic<int> thrown{0};
  std::vector<std::thread> writers;
  writers.reserve(4);
  for (int w = 0; w < 4; ++w) {
    writers.emplace_back([&, w] {
      try {
        for (int i = 0; !stop.load(); ++i) {
          const std::string k = "k" + std::to_string(i % 16);
          const std::string reply =
              (w % 2 == 0)
                  ? Write({"SETNX", k, "v"}, Flags::kNx)
                  : Write({"MSETNX", k, "v", "m" + std::to_string(i % 16), "v"}, Flags::kNx);
          if (reply != ":0" && reply != ":1") bad.fetch_add(1);
        }
      } catch (const std::exception&) {
        thrown.fetch_add(1);
      }
    });
  }
  for (int f = 0; f < 20; ++f) {
    auto flushed = dispatch_->DispatchFlush();
    ASSERT_TRUE(flushed.has_value()) << flushed.error().message();
    std::this_thread::sleep_for(2ms);
  }
  stop = true;
  for (auto& writer : writers) writer.join();
  EXPECT_EQ(thrown.load(), 0);
  EXPECT_EQ(bad.load(), 0);
}

// Recovery replays effects; it decides nothing. A conditional that
// changed nothing left no frame to re-decide, one that did left its
// effect, and the state after a restart is the state before it.
TEST_F(RegressionTest, RecoveryReplaysEffectsAndDecidesNothing) {
  Open(Options{.shards = 1});
  EXPECT_EQ(Write({"SETNX", "k", "a"}, Flags::kNx), ":1");
  EXPECT_EQ(Write({"SETNX", "k", "b"}, Flags::kNx), ":0");
  EXPECT_EQ(Write({"SET", "j", "x", "NX"}, Flags::kNx), "OK");
  EXPECT_EQ(Write({"SET", "j", "y"}), "OK");
  EXPECT_EQ(Write({"SET", "j", "z", "NX"}, Flags::kNx), "nil");
  EXPECT_EQ(Write({"EXPIRE", "j", "1000", "NX"}, Flags::kNx), ":1");
  EXPECT_EQ(Write({"EXPIRE", "j", "2000", "NX"}, Flags::kNx), ":0");
  const auto logged = Logged(0);
  std::vector<std::vector<std::string>> effects;
  effects.reserve(logged.size());
  for (const auto& entry : logged) effects.push_back(ArgsOf(entry));
  ASSERT_EQ(effects.size(), 4U) << "a decision with no effect was logged";
  EXPECT_EQ(effects[0], (std::vector<std::string>{"SET", "k", "a"}));
  // In ms, as the real clock runs on across the restart: whole seconds
  // would differ at a second boundary.
  const auto pttl = [this] { return std::stoll(Read({"PTTL", "j"}).substr(1)); };
  const int64_t pttl_before = pttl();
  ASSERT_LE(pttl_before, 1'000'000) << "the second EXPIRE NX changed the TTL";
  ASSERT_GT(pttl_before, 990'000);

  ASSERT_TRUE(Restart().has_value());

  EXPECT_EQ(Read({"GET", "k"}), "a");
  EXPECT_EQ(Read({"GET", "j"}), "y");
  const int64_t pttl_after = pttl();
  EXPECT_LE(pttl_after, pttl_before);
  EXPECT_GT(pttl_after, pttl_before - 60'000) << "recovery moved the TTL";
  EXPECT_EQ(Logged(0).size(), logged.size()) << "recovery logged a decision";
  EXPECT_EQ(skipped_, 0U);
}

// #160: a conditional is decided once and logged once, as its effect:
// of many racing SETNX on one key exactly one wins, and one frame is
// logged for it.
TEST_F(RegressionTest, Issue160AConditionalDecidedOnceIsLoggedOnce) {
  Open(Options{.shards = 1});
  constexpr int kWriters = 8;
  std::atomic<int> won{0};
  std::vector<std::thread> writers;
  writers.reserve(kWriters);
  for (int w = 0; w < kWriters; ++w) {
    writers.emplace_back([&, w] {
      if (Write({"SETNX", "k", "v" + std::to_string(w)}, Flags::kNx) == ":1") won.fetch_add(1);
    });
  }
  for (auto& writer : writers) writer.join();
  EXPECT_EQ(won.load(), 1);
  const auto logged = Logged(0);
  ASSERT_EQ(logged.size(), 1U);
  EXPECT_TRUE(std::holds_alternative<core::entry::Write>(logged[0].payload));
  EXPECT_TRUE(logged[0].replaces_state);
}

}  // namespace
}  // namespace abyss::engine

#endif  // ABYSS_HAVE_ROCKSDB
