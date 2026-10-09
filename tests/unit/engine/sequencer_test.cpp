#include "abyss/engine/sequencer.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <future>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <thread>
#include <unordered_set>
#include <utility>
#include <variant>
#include <vector>

#include "abyss/core/queue.h"
#include "abyss/metrics/names.h"
#include "latch.h"
#include "sequencer_fixture.h"

namespace abyss::engine {
namespace {

using metrics::RedecideReason;
using ::testing::_;
using ::testing::Return;
using namespace std::chrono_literals;
using Flags = core::PredicateFlags;

class SequencerTest : public testing::SequencerFixture {
 protected:
  uint64_t Redecides(RedecideReason reason) const {
    return sequencer_->Snapshot().redecides.at(static_cast<size_t>(reason));
  }
  // Fails the next `times` reserves with `code`.
  void FailReserves(core::ErrorCode code, int times, const std::string& message = "refused") {
    auto left = std::make_shared<std::atomic<int>>(times);
    queue_.SetReserveFault(
        // NOLINTNEXTLINE(bugprone-exception-escape): a test double.
        [left, code, message](std::span<const queue::ShardEntries>) -> std::optional<core::Error> {
          if (left->fetch_sub(1) <= 0) return std::nullopt;
          return core::Error{code, message};
        });
  }
  std::optional<std::string> HotString(std::string_view key) {
    auto read = hot_->Read(core::ops::ReadOp{core::ops::StringGet{.key = key}});
    if (!read.result.has_value() || !read.result->IsBulkString()) return std::nullopt;
    return read.result->AsString();
  }
  // What the last write logged, across shards, in shard then seq order.
  std::vector<std::vector<std::string>> NewlyLogged() {
    std::vector<std::vector<std::string>> out;
    for (core::ShardId shard = 0; shard < kShards; ++shard) {
      const auto logged = Logged(shard);
      for (size_t i = seen_.at(shard); i < logged.size(); ++i) out.push_back(logged[i]);
      seen_.at(shard) = logged.size();
    }
    return out;
  }

  // NOLINTNEXTLINE(cppcoreguidelines-non-private-member-variables-in-classes)
  std::array<size_t, kShards> seen_{};
};

// --- Every command family ---

struct FamilyCase {
  std::vector<std::string> cmd;
  Flags flags = Flags::kNone;
  std::string reply;
  std::vector<std::vector<std::string>> logged;
};

TEST_F(SequencerTest, EveryCommandFamilyRepliesLogsAndApplies) {
  const std::string ttl = std::to_string(NowMs() + 100'000);
  const std::vector<FamilyCase> cases = {
      {.cmd = {"SET", "s", "v"}, .reply = "OK", .logged = {{"SET", "s", "v"}}},
      {.cmd = {"SET", "s", "w", "NX"}, .flags = Flags::kNx, .reply = "nil"},
      {.cmd = {"SETNX", "n", "v"}, .reply = ":1", .logged = {{"SET", "n", "v"}}},
      {.cmd = {"SADD", "set", "a", "b"}, .reply = ":2", .logged = {{"SADD", "set", "a", "b"}}},
      {.cmd = {"SREM", "set", "a", "z"}, .reply = ":1", .logged = {{"SREM", "set", "a", "z"}}},
      {.cmd = {"SREM", "set", "z"}, .reply = ":0"},
      {.cmd = {"ZADD", "z", "1.50", "m"}, .reply = ":1", .logged = {{"ZADD", "z", "1.5", "m"}}},
      {.cmd = {"ZREM", "z", "m"}, .reply = ":1", .logged = {{"ZREM", "z", "m"}}},
      {.cmd = {"HSET", "h", "f", "v"}, .reply = ":1", .logged = {{"HSET", "h", "f", "v"}}},
      {.cmd = {"HMSET", "h", "g", "w"}, .reply = "OK", .logged = {{"HMSET", "h", "g", "w"}}},
      {.cmd = {"HDEL", "h", "f"}, .reply = ":1", .logged = {{"HDEL", "h", "f"}}},
      {.cmd = {"HSETNX", "h", "g", "x"}, .reply = ":0"},
      {.cmd = {"PEXPIREAT", "h", ttl}, .reply = ":1", .logged = {{"PEXPIREAT", "h", ttl}}},
      {.cmd = {"PERSIST", "h"}, .reply = ":1", .logged = {{"PERSIST", "h"}}},
      {.cmd = {"DEL", "s", "absent"}, .reply = ":1", .logged = {{"DEL", "s"}}},
      {.cmd = {"UNLINK", "set"}, .reply = ":1", .logged = {{"DEL", "set"}}},
      {.cmd = {"MSET", "a", "1", "b", "2"},
       .reply = "OK",
       .logged = {{"SET", "a", "1"}, {"SET", "b", "2"}}},
      {.cmd = {"MSETNX", "a", "9", "c", "3"}, .reply = ":0"},
      {.cmd = {"RENAMENX", "n", "n2"},
       .flags = Flags::kNx,
       .reply = ":1",
       .logged = {{"DEL", "n"}, {"SET", "n2", "v"}}},
      {.cmd = {"COPY", "n2", "n3"}, .reply = ":1", .logged = {{"SET", "n3", "v"}}},
  };
  for (const FamilyCase& c : cases) {
    SCOPED_TRACE(c.cmd.front());
    EXPECT_EQ(Reply(c.cmd, c.flags), c.reply);
    auto logged = NewlyLogged();
    auto expected = c.logged;
    std::ranges::sort(logged);
    std::ranges::sort(expected);
    EXPECT_EQ(logged, expected);
  }
  // Each shard's seqs run from 0 without a gap, in log order.
  for (core::ShardId shard = 0; shard < kShards; ++shard) {
    const auto published = queue_.Published(shard);
    for (size_t i = 0; i < published.size(); ++i) EXPECT_EQ(published[i].seq, i) << shard;
  }
  EXPECT_EQ(HotString("a"), "1");
  EXPECT_EQ(HotString("n3"), "v");
  EXPECT_EQ(HotString("n"), std::nullopt);
  EXPECT_EQ(Describe(hot_->Read(core::ops::ReadOp{core::ops::HashLen{.key = "h"}}).result.value()),
            ":1");
}

// --- Retries ---

TEST_F(SequencerTest, AdmissionFailureRetriesWithTheRequestRestored) {
  FailReserves(core::ErrorCode::kResourceExhausted, 1);
  int admits = 0;
  ON_CALL(queue_, Admit(_, _)).WillByDefault([&admits](core::ShardId, core::SteadyTime) {
    ++admits;
    return core::Result<void>{};
  });
  EXPECT_EQ(Reply({"SADD", "k", "a", "b"}), ":2");
  EXPECT_EQ(Redecides(RedecideReason::kAdmission), 1U);
  EXPECT_EQ(admits, 2) << "admitted before the lock, then again after the refusal";
  EXPECT_EQ(NewlyLogged(), (std::vector<std::vector<std::string>>{{"SADD", "k", "a", "b"}}));
}

TEST_F(SequencerTest, NoSpareWaitsThenRetries) {
  FailReserves(core::ErrorCode::kUnavailable, 1);
  EXPECT_CALL(queue_, WaitForSpare(ShardOf("k"), _)).WillOnce(Return(true));
  EXPECT_EQ(Reply({"SET", "k", "v"}), "OK");
  EXPECT_EQ(Redecides(RedecideReason::kSpare), 1U);
  EXPECT_EQ(NewlyLogged(), (std::vector<std::vector<std::string>>{{"SET", "k", "v"}}));
}

TEST_F(SequencerTest, ASpareWaitThatFailsBeforeTheDeadlineIsShutdown) {
  FailReserves(core::ErrorCode::kUnavailable, 1, "WAL queue is shutting down");
  EXPECT_CALL(queue_, WaitForSpare(_, _)).WillOnce(Return(false));
  auto result = Run({"SET", "k", "v"});
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code(), core::ErrorCode::kUnavailable);
  EXPECT_EQ(HotString("k"), std::nullopt);
}

TEST_F(SequencerTest, ReDecidesUntilTheDeadlineAndNoLonger) {
  Build(Options{.write_timeout = 60ms});
  FailReserves(core::ErrorCode::kResourceExhausted, 1'000'000);
  const auto start = std::chrono::steady_clock::now();
  auto result = Run({"SET", "k", "v"});
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code(), core::ErrorCode::kResourceExhausted);
  EXPECT_LT(std::chrono::steady_clock::now() - start, 2s);
  EXPECT_GE(Redecides(RedecideReason::kAdmission), 1U);
  EXPECT_EQ(HotString("k"), std::nullopt);
}

TEST_F(SequencerTest, ValueTooLargeIsNeverRetried) {
  FailReserves(core::ErrorCode::kValueTooLarge, 1'000'000, "batch exceeds segment capacity");
  auto result = Run({"MSET", "a", "1", "b", "2"});
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code(), core::ErrorCode::kValueTooLarge);
  EXPECT_EQ(Redecides(RedecideReason::kAdmission) + Redecides(RedecideReason::kSpare), 0U);
  EXPECT_EQ(HotString("a"), std::nullopt);
  EXPECT_EQ(PublishedCount(), 0U);
}

// A blind write replaces the placeholder of a load in flight, so the
// load is discarded; once the key is evicted again it is reloaded.
TEST_F(SequencerTest, ALoadABlindWriteDiscardedIsTakenAgain) {
  PutCold("k", core::ColdKeyState{.type = core::KeyType::kSet,
                                  .value = std::unordered_set<std::string>{"a"}});
  bool raced = false;
  on_load_ = [&](std::string_view) {
    if (std::exchange(raced, true)) return;
    // Off every lock, as a load runs: another client's blind SET, then
    // an eviction that leaves only its stub.
    std::thread([&] {
      EXPECT_EQ(Reply({"SET", "k", "x"}), "OK");
      PutCold("k", core::ColdKeyState{.type = core::KeyType::kString, .value = std::string("x")});
      hot_->EvictExpired(core::SteadyClock::now() + std::chrono::hours{1'000'000});
    }).join();
  };
  auto result = Run({"SADD", "k", "m"});
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code(), core::ErrorCode::kWrongType)
      << "the reload saw the string the blind write left";
  EXPECT_EQ(loads_, 2);
  EXPECT_EQ(Redecides(RedecideReason::kLoad), 1U);
}

// A batch's installs are kept past the stub cap: an MSETNX over more
// non-resident keys than the cap is decided after one load round.
TEST_F(SequencerTest, AnMsetnxOverTwiceTheStubCapLoadsOnce) {
  // Four stubs per shard.
  Build(Options{.stub_memory_fraction = 4.0 * kShards * static_cast<double>(hot::kStubBytes) /
                                        static_cast<double>(size_t{64} << 20)});
  std::vector<std::string> cmd{"MSETNX"};
  for (int i = 0; i < 8; ++i) {
    const std::string key = KeyOn(0, i, "nx");
    PutCold(key, core::ColdKeyState{.type = core::KeyType::kString, .value = std::string("old")});
    cmd.push_back(key);
    cmd.emplace_back("new");
  }
  EXPECT_EQ(Reply(cmd), ":0");
  EXPECT_EQ(probes_, 8);
  EXPECT_EQ(Redecides(RedecideReason::kLoad), 0U);
  EXPECT_EQ(PublishedCount(), 0U);
}

// --- Large values ---

TEST_F(SequencerTest, ALargeSetCopiesNothingUnderTheLock) {
  const std::string value(1 << 20, 'v');
  EXPECT_EQ(Reply({"SET", "k", value}), "OK");
  EXPECT_EQ(sequencer_->Snapshot().locked_copy_bytes, 0U);
  EXPECT_EQ(HotString("k"), value);
  EXPECT_EQ(NewlyLogged(), (std::vector<std::vector<std::string>>{{"SET", "k", value}}));
}

TEST_F(SequencerTest, ALargeSetKeepTtlCopiesNothingUnderTheLock) {
  EXPECT_EQ(Reply({"SET", "k", "small", "PX", "100000"}), "OK");
  const uint64_t before = sequencer_->Snapshot().locked_copy_bytes;
  const std::string value(1 << 20, 'v');
  EXPECT_EQ(Reply({"SET", "k", value, "KEEPTTL"}, Flags::kKeepTtl), "OK");
  EXPECT_EQ(sequencer_->Snapshot().locked_copy_bytes, before);
  const auto logged = NewlyLogged();
  ASSERT_EQ(logged.size(), 2U);
  EXPECT_EQ(logged[1], (std::vector<std::string>{"SET", "k", value, "PXAT",
                                                 std::to_string(NowMs() + 100000)}));
}

// COPY recreates from hot's value, so the entry needs a copy of its own.
TEST_F(SequencerTest, ALargeCopyCopiesUnderTheLockAndCountsIt) {
  const std::string value(1 << 20, 'v');
  EXPECT_EQ(Reply({"SET", "src", value}), "OK");
  EXPECT_EQ(sequencer_->Snapshot().locked_copy_bytes, 0U);
  EXPECT_EQ(Reply({"COPY", "src", "dst"}), ":1");
  EXPECT_EQ(sequencer_->Snapshot().locked_copy_bytes, value.size());
  EXPECT_EQ(HotString("dst"), value);
}

// Many arguments each under the frame threshold still total over it, so
// every one is copied before the lock.
TEST_F(SequencerTest, AnMsetOfManyMidSizeValuesCopiesWithinTheBudget) {
  std::vector<std::string> cmd{"MSET"};
  for (int i = 0; i < 1000; ++i) {
    cmd.push_back(KeyOn(0, i, "m"));
    cmd.emplace_back(size_t{15} << 10, static_cast<char>('a' + (i % 26)));
  }
  EXPECT_EQ(Reply(cmd), "OK");
  EXPECT_LE(sequencer_->Snapshot().locked_copy_bytes, core::kLockHoldFrameBytes);
  EXPECT_EQ(Logged(0).size(), 1000U);
}

// --- The fence ---

// A decision that logs nothing replies only once what it read is
// durable: here SETNX on a key whose SET is held short of durable.
TEST_F(SequencerTest, ANoEffectDecisionFencesOnWhatItRead) {
  EXPECT_EQ(Reply({"SET", "k", "v"}), "OK");
  abyss::testing::Latch entered;
  abyss::testing::Latch release;
  std::optional<std::pair<core::ShardId, core::SequenceId>> fenced;
  EXPECT_CALL(queue_, AwaitDurable(_, _, core::Durability::kProcessCrash, _))
      .WillOnce([&](core::ShardId shard, core::SequenceId seq, core::Durability,
                    core::Duration) -> core::Result<bool> {
        fenced = {shard, seq};
        entered.Open();
        return release.Wait();
      });
  auto reply = std::async(std::launch::async, [this] { return Reply({"SETNX", "k", "w"}); });
  ASSERT_TRUE(entered.Wait());
  EXPECT_EQ(reply.wait_for(50ms), std::future_status::timeout) << "replied before the fence";
  release.Open();
  EXPECT_EQ(reply.get(), ":0");
  EXPECT_EQ(fenced, (std::optional<std::pair<core::ShardId, core::SequenceId>>{{ShardOf("k"), 0}}));
}

TEST_F(SequencerTest, AFenceTimeoutRepliesWithTheTimeoutText) {
  EXPECT_EQ(Reply({"SET", "k", "v"}), "OK");
  EXPECT_CALL(queue_, AwaitDurable(_, _, _, _)).WillOnce(Return(core::Result<bool>(false)));
  auto result = Run({"SETNX", "k", "w"});
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code(), core::ErrorCode::kTimeout);
  EXPECT_TRUE(result.error().message().starts_with("durable wait exceeded server timeout"));
}

// Loaded state carries seq 0 too: with no write here at 0, nothing to
// fence, so a no-op on a key just loaded absent replies at once.
TEST_F(SequencerTest, LoadedStateNeedsNoFence) {
  EXPECT_CALL(queue_, AwaitDurable(_, _, _, _)).Times(0);
  EXPECT_EQ(Reply({"SREM", "absent", "m"}), ":0");
}

// --- CROSSSLOT ---

TEST_F(SequencerTest, MultiKeyWritesAcrossLogsAreCrossSlot) {
  Build(Options{.log_count = 2});
  const std::string even = KeyOn(0);
  const std::string odd = KeyOn(1);
  EXPECT_EQ(Reply({"SET", even, "v"}), "OK");
  NewlyLogged();
  for (const auto& cmd : std::vector<std::vector<std::string>>{{"MSET", even, "1", odd, "2"},
                                                               {"DEL", even, odd},
                                                               {"RENAMENX", even, odd},
                                                               {"COPY", even, odd}}) {
    SCOPED_TRACE(cmd.front());
    EXPECT_EQ(Reply(cmd), "-CROSSSLOT Keys in request don't hash to the same slot");
    EXPECT_TRUE(NewlyLogged().empty());
  }
  EXPECT_EQ(Reply({"MSET", KeyOn(0), "1", KeyOn(2), "2"}), "OK") << "shards 0 and 2 share a log";
  auto flushed = sequencer_->Flush();
  ASSERT_TRUE(flushed.has_value()) << flushed.error().message();
  for (core::ShardId shard = 0; shard < kShards; ++shard) {
    ASSERT_FALSE(Logged(shard).empty());
    EXPECT_EQ(Logged(shard).back(), (std::vector<std::string>{"<flush>"})) << shard;
  }
}

// --- Memory backpressure ---

class SequencerBackpressureTest : public SequencerTest {
 protected:
  void SetUp() override {
    // 16 KiB a shard, over its limit past 20 KiB; nothing drains.
    Build(Options{.max_memory_bytes = 64 << 10, .write_timeout = 300ms, .drain_gated = true});
    for (int i = 0; i < 4; ++i) {
      ASSERT_EQ(Reply({"SET", KeyOn(0, i, "fill"), std::string(8 << 10, 'f')}), "OK");
    }
    ASSERT_TRUE(Over(0));
  }
  bool Over(core::ShardId shard) {
    auto locks = hot_->LockExclusive(std::vector<core::ShardId>{shard});
    return locks.OverBackpressure(shard);
  }
};

TEST_F(SequencerBackpressureTest, AGrowingWriteWaitsForColdThenSucceeds) {
  std::thread drainer([this] {
    std::this_thread::sleep_for(50ms);
    router_.Advance(100);
  });
  EXPECT_EQ(Reply({"SET", KeyOn(0, 9, "new"), "v"}), "OK");
  drainer.join();
  EXPECT_EQ(sequencer_->Snapshot().backpressure_waits, 1U);
  EXPECT_EQ(sequencer_->Snapshot().backpressure_rejections, 0U);
  EXPECT_FALSE(Over(0));
}

TEST_F(SequencerBackpressureTest, AGrowingWriteIsRejectedAtTheDeadline) {
  const std::string key = KeyOn(0, 9, "new");
  EXPECT_EQ(Reply({"SADD", key, "m"}),
            "-OOM command not allowed when hot memory is over its limit and cold is behind");
  EXPECT_EQ(sequencer_->Snapshot().backpressure_rejections, 1U);
  EXPECT_EQ(Logged(0).size(), 4U) << "a rejected write logs nothing";
}

// A command that must load a whole key grows memory however it reads:
// an EXPIRE of a cold set waits like a SADD would.
TEST_F(SequencerBackpressureTest, AFullLoadWaitsLikeAGrowingWrite) {
  const std::string key = KeyOn(0, 9, "cold");
  PutCold(key, core::ColdKeyState{.type = core::KeyType::kSet,
                                  .value = std::unordered_set<std::string>{"a", "b"}});
  std::thread drainer([this] {
    std::this_thread::sleep_for(50ms);
    router_.Advance(100);
  });
  EXPECT_EQ(Reply({"PEXPIREAT", key, std::to_string(NowMs() + 100'000)}), ":1");
  drainer.join();
  EXPECT_GE(Redecides(RedecideReason::kBackpressure), 1U);
  EXPECT_EQ(sequencer_->Snapshot().backpressure_waits, 1U);
}

TEST_F(SequencerBackpressureTest, ADeleteNeverWaits) {
  const auto start = std::chrono::steady_clock::now();
  EXPECT_EQ(Reply({"DEL", KeyOn(0, 1, "fill")}), ":1");
  EXPECT_LT(std::chrono::steady_clock::now() - start, 200ms);
  EXPECT_EQ(sequencer_->Snapshot().backpressure_waits, 0U);
}

// --- FLUSHDB ---

TEST_F(SequencerTest, FlushWipesEveryShardAtItsSeqAndSetsTheFloor) {
  Build(Options{.drain_gated = true});
  for (core::ShardId shard = 0; shard < kShards; ++shard) {
    ASSERT_EQ(Reply({"SET", KeyOn(shard), "v"}), "OK");
  }
  auto flushed = sequencer_->Flush();
  ASSERT_TRUE(flushed.has_value()) << flushed.error().message();
  EXPECT_EQ(flushed->AsString(), "OK");
  for (core::ShardId shard = 0; shard < kShards; ++shard) {
    const auto published = queue_.Published(shard);
    ASSERT_EQ(published.size(), 2U);
    EXPECT_TRUE(std::holds_alternative<core::entry::Flush>(published[1].payload));
    EXPECT_TRUE(hot_->KnownAbsentAfterFlush(KeyOn(shard))) << "the floor is the Flush's seq";
    auto read = hot_->Read(core::ops::ReadOp{core::ops::StringGet{.key = KeyOn(shard)}});
    ASSERT_TRUE(read.result.has_value());
    EXPECT_TRUE(read.result->IsNull());
    EXPECT_EQ(read.fence, published[1].seq);
  }
}

// --- One clock read per attempt ---

TEST_F(SequencerTest, DecideApplyAndTheLogShareOneInstant) {
  const int64_t t = NowMs();
  EXPECT_EQ(Reply({"SET", "k", "v", "EX", "10"}), "OK");
  EXPECT_EQ(Reply({"EXPIRE", "k", "5"}), ":1");
  const auto shard = ShardOf("k");
  auto published = queue_.Published(shard);
  ASSERT_EQ(published.size(), 2U);
  EXPECT_EQ(Logged(shard)[0],
            (std::vector<std::string>{"SET", "k", "v", "PXAT", std::to_string(t + 10'000)}));
  EXPECT_EQ(Logged(shard)[1],
            (std::vector<std::string>{"PEXPIREAT", "k", std::to_string(t + 5000)}));
  for (const auto& entry : published) {
    EXPECT_EQ(entry.appended_at, core::WallTime{std::chrono::milliseconds{t}});
  }

  // Past k's TTL, a write that reads k logs the expiry it saw first, in
  // the same reservation, at the same instant.
  SetNowMs(t + 6000);
  EXPECT_EQ(Reply({"SET", "k", "w", "NX"}, Flags::kNx), "OK");
  published = queue_.Published(shard);
  ASSERT_EQ(published.size(), 4U);
  EXPECT_EQ(Logged(shard)[2], (std::vector<std::string>{"DEL", "k"}));
  EXPECT_EQ(Logged(shard)[3], (std::vector<std::string>{"SET", "k", "w"}));
  EXPECT_EQ(published[3].seq, published[2].seq + 1);
  EXPECT_EQ(published[2].appended_at, core::WallTime{std::chrono::milliseconds{t + 6000}});
  EXPECT_EQ(published[3].appended_at, published[2].appended_at);
}

// The instant is read under the shard lock: two writers whose wall
// clock steps back between their reads still stamp in seq order.
TEST_F(SequencerTest, StampsFollowSeqOrderWhenTheWallClockStepsBack) {
  std::atomic<int64_t> reads{0};
  const int64_t t = NowMs();
  wall_read_ = [&reads, t] {
    return core::WallTime{std::chrono::milliseconds{t - reads.fetch_add(1)}};
  };
  constexpr int kWrites = 200;
  std::vector<std::thread> writers;
  writers.reserve(2);
  for (int w = 0; w < 2; ++w) {
    writers.emplace_back([this, w] {
      for (int i = 0; i < kWrites; ++i) {
        EXPECT_EQ(Reply({"SET", KeyOn(1, i % 7, w == 0 ? "a" : "b"), "v"}), "OK");
      }
    });
  }
  for (auto& writer : writers) writer.join();
  const auto published = queue_.Published(1);
  ASSERT_EQ(published.size(), 2 * size_t{kWrites});
  for (size_t i = 1; i < published.size(); ++i) {
    EXPECT_GE(published[i].appended_at, published[i - 1].appended_at) << "seq " << i;
  }
}

// Replay raises a shard's clock to its last stamp, so a wall clock set
// back across a restart cannot stamp the next write below it.
TEST_F(SequencerTest, TheNextWriteIsStampedAtOrAfterTheReplayedTail) {
  const int64_t t = NowMs();
  const core::WallTime ahead{std::chrono::milliseconds{t + 3'600'000}};
  core::QueueEntry tail{.seq = 0,
                        .appended_at = ahead,
                        .payload = core::entry::Write{.cmd = {.args = {"SET", "x", "v"}}},
                        .replaces_state = true};
  hot_->ApplyLogged(ShardOf("x"), tail);
  EXPECT_EQ(Reply({"SET", KeyOn(ShardOf("x"), 3), "w", "PX", "1000"}), "OK");
  const auto published = queue_.Published(ShardOf("x"));
  ASSERT_EQ(published.size(), 1U);
  EXPECT_GE(published[0].appended_at, ahead);
  EXPECT_EQ(Logged(ShardOf("x"))[0][4], std::to_string(t + 3'600'000 + 1000))
      << "decide's now is the same instant";
}

}  // namespace
}  // namespace abyss::engine
