// One test per bug the sequenced write path fixed, each against the new
// path and each failing on the resolver design it replaced.

#include <gtest/gtest.h>

#ifdef ABYSS_HAVE_ROCKSDB

#include <atomic>
#include <chrono>
#include <cstdint>
#include <exception>
#include <string>
#include <thread>
#include <variant>
#include <vector>

#include "abyss/core/command_dispatcher.h"
#include "abyss/core/queue_entry.h"
#include "abyss/core/types.h"
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
    auto flushed = engine_->DispatchFlush(core::FlushTarget::kThisDb);
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
  const std::string ttl_before = Read({"TTL", "j"});

  ASSERT_TRUE(Restart().has_value());

  EXPECT_EQ(Read({"GET", "k"}), "a");
  EXPECT_EQ(Read({"GET", "j"}), "y");
  EXPECT_EQ(Read({"TTL", "j"}), ttl_before);
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
