#include "abyss/engine/sequencer.h"

#include <gtest/gtest.h>

#ifdef ABYSS_HAVE_ROCKSDB

#include <atomic>
#include <chrono>
#include <cstdint>
#include <future>
#include <string>
#include <thread>
#include <variant>
#include <vector>

#include "abyss/core/durability.h"
#include "abyss/core/ops.h"
#include "abyss/core/queue_entry.h"
#include "abyss/core/types.h"
#include "abyss/queue/reservation.h"
#include "sequenced_engine_fixture.h"

namespace abyss::engine {
namespace {

// MSET of two shards applies both under both locks: a reader that sees
// one key's new value then reads the other sees it too (or a newer).
TEST_F(SequencedEngineTest, ACrossShardMsetIsAtomicToAReader) {
  Open();
  const std::string a = KeyOn(0);
  const std::string b = KeyOn(1);
  constexpr int kWrites = 2000;
  EXPECT_EQ(Write({"MSET", a, "0", b, "0"}), "OK");
  std::atomic<bool> done{false};
  std::atomic<int> torn{0};
  const auto reader = [&](const std::string& first, const std::string& second) {
    while (!done.load()) {
      const int seen = std::stoi(Read({"GET", first}));
      const int then = std::stoi(Read({"GET", second}));
      if (then < seen) torn.fetch_add(1);
    }
  };
  std::thread ab(reader, a, b);
  std::thread ba(reader, b, a);
  for (int i = 1; i <= kWrites; ++i) {
    ASSERT_EQ(Write({"MSET", a, std::to_string(i), b, std::to_string(i)}), "OK");
  }
  done = true;
  ab.join();
  ba.join();
  EXPECT_EQ(torn.load(), 0) << "a reader saw one key of an MSET without the other";
}

// Under power_loss a read of a key whose write is applied but not yet
// durable waits for it; its reply never shows a write a power loss
// could take back.
TEST_F(SequencedEngineTest, APowerLossReadWaitsUntilTheWriteIsDurable) {
  Open(Options{.durability = core::Durability::kPowerLoss});
  const FlushStall stall;
  queue_->SetFlushHookForTesting(stall.Hook());
  const auto release = [&stall] { stall.Release(); };

  auto writer = std::async(std::launch::async, [this] { return Write({"SET", "k", "v"}); });
  // Applied to hot before its durable wait.
  bool applied = false;
  for (int i = 0; i < 500 && !applied; ++i) {
    applied = hot_->Read(core::ops::ReadOp{core::ops::StringGet{.key = "k"}}).result.has_value();
    if (!applied) std::this_thread::sleep_for(1ms);
  }
  if (!applied) release();
  ASSERT_TRUE(applied);
  auto reader = std::async(std::launch::async, [this] { return Read({"GET", "k"}); });
  EXPECT_EQ(reader.wait_for(100ms), std::future_status::timeout) << "read before durable";
  EXPECT_EQ(writer.wait_for(0ms), std::future_status::timeout);
  release();
  EXPECT_EQ(reader.get(), "v");
  EXPECT_EQ(writer.get(), "OK");
}

// M4: after FLUSHDB no earlier key is visible, evicted ones with stubs
// included, though cold still holds them until it drains the Flush.
TEST_F(SequencedEngineTest, FlushDbRacingWritersHidesEveryEarlierKey) {
  Open();
  constexpr int kKeys = 40;
  for (int i = 0; i < kKeys; ++i) {
    ASSERT_EQ(Write({"SET", "pre:" + std::to_string(i), "v"}), "OK");
  }
  for (core::ShardId shard = 0; shard < options_.shards; ++shard) DrainToCold(shard);
  EvictDrained();
  ASSERT_EQ(Exists("pre:0"), ":1") << "a stub or cold answers before the flush";

  std::atomic<bool> stop{false};
  std::vector<std::thread> writers;
  writers.reserve(2);
  for (int w = 0; w < 2; ++w) {
    writers.emplace_back([this, w, &stop] {
      for (int i = 0; !stop.load(); ++i) {
        EXPECT_EQ(Write({"SET", "post:" + std::to_string(w) + ":" + std::to_string(i), "v"}), "OK");
      }
    });
  }
  std::this_thread::sleep_for(5ms);
  auto flushed = engine_->DispatchFlush(core::FlushTarget::kThisDb);
  stop = true;
  for (auto& writer : writers) writer.join();
  ASSERT_TRUE(flushed.has_value()) << flushed.error().message();
  EXPECT_EQ(flushed->AsString(), "OK");
  for (int i = 0; i < kKeys; ++i) {
    const std::string key = "pre:" + std::to_string(i);
    EXPECT_EQ(Read({"GET", key}), "nil") << key;
    EXPECT_EQ(Exists(key), ":0") << key;
  }
}

// FLUSHDB over two logs is one reservation and publishes every shard.
TEST_F(SequencedEngineTest, FlushDbOverTwoLogsCompletes) {
  Open(Options{.log_count = 2});
  for (core::ShardId shard = 0; shard < options_.shards; ++shard) {
    ASSERT_EQ(Write({"SET", KeyOn(shard), "v"}), "OK");
  }
  auto flushed = engine_->DispatchFlush(core::FlushTarget::kThisDb);
  ASSERT_TRUE(flushed.has_value()) << flushed.error().message();
  EXPECT_EQ(queue::ReservationsHeld(), 0U);
  for (core::ShardId shard = 0; shard < options_.shards; ++shard) {
    const auto logged = Logged(shard);
    ASSERT_EQ(logged.size(), 2U) << shard;
    EXPECT_TRUE(std::holds_alternative<core::entry::Flush>(logged[1].payload)) << shard;
    EXPECT_EQ(Read({"GET", KeyOn(shard)}), "nil");
  }
}

// An MSET whose frames cannot fit one segment fails whole, never
// partly applied and never retried.
TEST_F(SequencedEngineTest, AnMsetPastASegmentFailsCleanly) {
  Open(Options{.shards = 1, .segment_size_bytes = size_t{64} << 10});
  std::vector<std::string> mset{"MSET"};
  for (int i = 0; i < 200; ++i) {
    mset.push_back("k" + std::to_string(i));
    mset.emplace_back(512, 'v');
  }
  const std::string refused = Write(mset);
  EXPECT_NE(refused.find("exceeds the segment frame space"), std::string::npos) << refused;
  EXPECT_EQ(Read({"GET", "k0"}), "nil");
  EXPECT_EQ(queue_->DurableEnd(0, core::Durability::kProcessCrash).value(), core::kFirstSeq);
  EXPECT_EQ(Write({"SET", "after", "v"}), "OK") << "the log is left usable";
}

// A WRONGTYPE reveals the key's state too, so it waits, as any reply,
// until the write it saw is durable.
TEST_F(SequencedEngineTest, AWrongTypeReplyWaitsUntilWhatItSawIsDurable) {
  Open(Options{.durability = core::Durability::kPowerLoss});
  const FlushStall stall;
  queue_->SetFlushHookForTesting(stall.Hook());
  auto setter = std::async(std::launch::async, [this] { return Write({"SET", "k", "v"}); });
  bool applied = false;
  for (int i = 0; i < 500 && !applied; ++i) {
    applied = hot_->Read(core::ops::ReadOp{core::ops::StringGet{.key = "k"}}).result.has_value();
    if (!applied) std::this_thread::sleep_for(1ms);
  }
  if (!applied) stall.Release();
  ASSERT_TRUE(applied);
  auto adder = std::async(std::launch::async, [this] { return Write({"SADD", "k", "m"}); });
  EXPECT_EQ(adder.wait_for(100ms), std::future_status::timeout) << "WRONGTYPE before durable";
  stall.Release();
  EXPECT_EQ(adder.get(), "error: Operation against a key holding the wrong kind of value");
  EXPECT_EQ(setter.get(), "OK");
}

// A resident entry is the key's latest state: past its TTL it is
// absent, though cold, behind, still holds an older value.
TEST_F(SequencedEngineTest, AnExpiredResidentKeyIsAbsentNotItsColdValue) {
  Open();
  EXPECT_EQ(Write({"SET", "k", "v1"}), "OK");
  DrainToCold(ShardOf("k"));
  EXPECT_EQ(Write({"SET", "k", "v2", "PX", "50"}), "OK");
  std::this_thread::sleep_for(80ms);
  EXPECT_EQ(Read({"GET", "k"}), "nil");
  EXPECT_EQ(Exists("k"), ":0");
  EXPECT_EQ(ColdGet("k"), "v1") << "the cold consumer was meant to be behind";
}

}  // namespace
}  // namespace abyss::engine

#endif  // ABYSS_HAVE_ROCKSDB
