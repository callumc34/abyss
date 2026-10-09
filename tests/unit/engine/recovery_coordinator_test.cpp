#include "abyss/engine/recovery_coordinator.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

#include "abyss/consumer/cold_consumer_pool.h"
#include "abyss/consumer/hot_consumer_pool.h"
#include "abyss/consumer/resolver_pool.h"
#include "abyss/core/apply_notifier.h"
#include "abyss/core/consumer_rpc.h"
#include "abyss/core/durability.h"
#include "abyss/core/eviction_policy.h"
#include "abyss/core/ops.h"
#include "abyss/core/queue_entry.h"
#include "abyss/core/resp_types.h"
#include "abyss/core/result.h"
#include "abyss/core/types.h"
#include "abyss/log/testing.h"
#include "inline_shard_scheduler.h"
#include "mock_cold_store.h"
#include "mock_hot_store.h"
#include "mock_queue.h"

namespace abyss::engine {
namespace {

using namespace std::chrono_literals;
using ::testing::_;
using ::testing::NiceMock;
using ::testing::Return;
using ::testing::UnorderedElementsAreArray;

constexpr uint32_t kShards = 2;
constexpr core::SequenceId kFirst = core::kFirstSeq;
constexpr std::string_view kMissed = "replay after the recovery scan found entries it missed";

std::string KeyOf(core::ShardId shard, core::SequenceId seq) {
  return "s" + std::to_string(shard) + ":" + std::to_string(seq);
}

core::QueueEntry Write(core::ShardId shard, core::SequenceId seq) {
  return core::QueueEntry{
      .seq = seq,
      .appended_at = core::WallClock::now(),
      .payload = core::entry::Write{.cmd = core::RespCommand{{"SET", KeyOf(shard, seq), "v"}}},
  };
}

core::QueueEntry Flush(core::SequenceId seq) {
  return core::QueueEntry{
      .seq = seq, .appended_at = core::WallClock::now(), .payload = core::entry::Flush{}};
}

// The base class's Scan, recording its bounds, or `instead` when set.
class ScanQueue : public testing::MockQueue {
 public:
  core::Result<void> Scan(std::span<const core::SequenceId> from,
                          std::span<const core::SequenceId> end, uint32_t parallelism,
                          const ScanSink& sink, const std::atomic<bool>& cancel) override {
    from_.assign(from.begin(), from.end());
    end_.assign(end.begin(), end.end());
    if (instead) return instead();
    return core::Queue::Scan(from, end, parallelism, sink, cancel);
  }

  std::vector<core::SequenceId> ScannedFrom() const { return from_; }
  std::vector<core::SequenceId> ScannedEnd() const { return end_; }

  // NOLINTNEXTLINE(cppcoreguidelines-non-private-member-variables-in-classes)
  std::function<core::Result<void>()> instead;

 private:
  std::vector<core::SequenceId> from_;
  std::vector<core::SequenceId> end_;
};

// What each store was given, by key.
class Applied {
 public:
  void Add(std::string_view key) {
    const std::scoped_lock lock(mu_);
    keys_.emplace_back(key);
  }
  std::vector<std::string> Keys() const {
    const std::scoped_lock lock(mu_);
    return keys_;
  }

 private:
  mutable std::mutex mu_;
  std::vector<std::string> keys_;
};

std::vector<std::string> KeysOf(core::ShardId shard, core::SequenceId from, core::SequenceId end) {
  std::vector<std::string> keys;
  for (core::SequenceId seq = from; seq < end; ++seq) keys.push_back(KeyOf(shard, seq));
  return keys;
}

std::vector<std::string> Concat(std::vector<std::string> a, const std::vector<std::string>& b) {
  a.insert(a.end(), b.begin(), b.end());
  return a;
}

// A queue each shard of which holds entries from kFirstSeq, with first
// retained seqs and cold commits the test picks. The resolver has
// committed everything, so its phase replays nothing.
class RecoveryCoordinatorTest : public ::testing::Test {
 protected:
  void SetUp() override {
    ON_CALL(queue_, Read(_, _, _, _, _))
        .WillByDefault([this](core::ShardId shard, core::SequenceId from, size_t max,
                              core::Duration, core::Durability) {
          return core::Result<std::vector<core::QueueEntry>>(
              testing::ReadFromLog(logs_[shard], from, max));
        });
    ON_CALL(queue_, FirstSeq(_)).WillByDefault([this](core::ShardId shard) {
      return core::Result<core::SequenceId>(first_[shard]);
    });
    ON_CALL(queue_, DurableEnd(_, _)).WillByDefault([this](core::ShardId shard, core::Durability) {
      return core::Result<core::SequenceId>(kFirst + logs_[shard].size());
    });
    ON_CALL(queue_, TailSeq(_)).WillByDefault([this](core::ShardId shard) {
      return core::Result<core::SequenceId>(kFirst + logs_[shard].size() - 1);
    });
    ON_CALL(queue_, CommittedOffset(core::kColdConsumer, _))
        .WillByDefault([this](core::ConsumerId, core::ShardId shard) {
          return core::Result<std::optional<core::SequenceId>>(cold_commit_[shard]);
        });
    ON_CALL(queue_, CommittedOffset(core::kResolverConsumer, _))
        .WillByDefault([this](core::ConsumerId, core::ShardId shard) {
          const auto& log = logs_[shard];
          return core::Result<std::optional<core::SequenceId>>(
              log.empty() ? std::nullopt
                          : std::optional<core::SequenceId>(kFirst + log.size() - 1));
        });
    ON_CALL(queue_, CommitOffset(_, _, _)).WillByDefault(Return(core::Result<void>{}));

    ON_CALL(hot_, ApplyLogged(_, _)).WillByDefault([this](core::ShardId, core::QueueEntry& entry) {
      if (const auto* write = std::get_if<core::entry::Write>(&entry.payload)) {
        hot_applied_.Add(write->cmd.args.at(1));
      }
      return std::optional<core::RespValue>(core::RespValue::SimpleString("OK"));
    });
    ON_CALL(cold_, ApplyBatch(_, _))
        .WillByDefault([this](std::span<const core::ops::WriteOp> ops, core::SequenceId) {
          for (const auto& op : ops) cold_applied_.Add(core::ops::PrimaryKey(op));
          return core::Result<void>{};
        });
    ON_CALL(cold_, Wipe(_)).WillByDefault(Return(core::Result<void>{}));
  }

  void Fill(core::ShardId shard, core::SequenceId count) {
    for (core::SequenceId seq = kFirst; seq < kFirst + count; ++seq) {
      logs_[shard].push_back(Write(shard, seq));
    }
  }

  RecoveryCoordinator& Coordinator() {
    cold_pool_ = std::make_unique<consumer::ColdConsumerPool>(
        queue_, cold_,
        consumer::ColdConsumerPool::Config{.shard_count = kShards, .consumer = cold_config_},
        policy_, rpc_);
    hot_pool_ = std::make_unique<consumer::HotConsumerPool>(
        queue_, hot_, rpc_, notifier_, consumer::HotConsumerPool::Config{.shard_count = kShards},
        policy_);
    resolver_pool_ = std::make_unique<consumer::ResolverPool>(
        queue_, cold_, *cold_pool_, rpc_, notifier_,
        consumer::ResolverPool::Config{.shard_count = kShards});
    coordinator_ =
        std::make_unique<RecoveryCoordinator>(queue_, *resolver_pool_, *cold_pool_, *hot_pool_,
                                              scheduler_, RecoveryConfig{.replay_parallelism = 2});
    return *coordinator_;
  }

  // Runs recovery, cancelling it if it outlives `bound`, so a hang
  // fails the test instead of stalling the suite.
  core::Result<void> RunBounded(std::chrono::seconds bound = 10s) {
    auto& coordinator = Coordinator();
    auto run = std::async(std::launch::async, [&] { return coordinator.Run(cancel_); });
    if (run.wait_for(bound) != std::future_status::ready) {
      ADD_FAILURE() << "recovery did not return within " << bound.count() << " s";
      cancel_.store(true);
    }
    return run.get();
  }

  static std::vector<log::testing::CapturedRecord> Missed(const log::testing::CapturingSink& logs) {
    std::vector<log::testing::CapturedRecord> out;
    for (auto& record : logs.Records()) {
      if (record.msg == kMissed) out.push_back(std::move(record));
    }
    return out;
  }

  // NOLINTBEGIN(cppcoreguidelines-non-private-member-variables-in-classes)
  std::vector<std::vector<core::QueueEntry>> logs_ =
      std::vector<std::vector<core::QueueEntry>>(kShards);
  std::vector<core::SequenceId> first_ = std::vector<core::SequenceId>(kShards, kFirst);
  std::vector<std::optional<core::SequenceId>> cold_commit_ =
      std::vector<std::optional<core::SequenceId>>(kShards);
  consumer::ColdConsumer::Config cold_config_;
  NiceMock<ScanQueue> queue_;
  NiceMock<testing::MockHotStore> hot_;
  NiceMock<testing::MockColdStore> cold_;
  Applied hot_applied_;
  Applied cold_applied_;
  core::EvictionPolicy policy_{core::EvictionTTL{3600}};
  core::ConsumerRpc rpc_;
  core::ApplyNotifier notifier_{core::AppliedSeqNotifierConfig{.shard_count = kShards}};
  testing::InlineShardScheduler scheduler_;
  std::atomic<bool> cancel_{false};
  std::unique_ptr<consumer::ColdConsumerPool> cold_pool_;
  std::unique_ptr<consumer::HotConsumerPool> hot_pool_;
  std::unique_ptr<consumer::ResolverPool> resolver_pool_;
  std::unique_ptr<RecoveryCoordinator> coordinator_;
  // NOLINTEND(cppcoreguidelines-non-private-member-variables-in-classes)
};

// One scan from min(FirstSeq, cold commit + 1) to each shard's end: hot
// gets what is at or past FirstSeq, cold what is past its commit.
TEST_F(RecoveryCoordinatorTest, OneScanFeedsHotFromFirstSeqAndColdPastItsCommit) {
  Fill(0, 10);
  first_[0] = kFirst + 2;
  cold_commit_[0] = kFirst + 5;
  Fill(1, 4);
  const log::testing::CapturingSink logs;

  ASSERT_TRUE(RunBounded().has_value());

  EXPECT_EQ(queue_.ScannedFrom(), (std::vector<core::SequenceId>{kFirst + 2, kFirst}));
  EXPECT_EQ(queue_.ScannedEnd(), (std::vector<core::SequenceId>{kFirst + 10, kFirst + 4}));
  EXPECT_THAT(hot_applied_.Keys(),
              UnorderedElementsAreArray(
                  Concat(KeysOf(0, kFirst + 2, kFirst + 10), KeysOf(1, kFirst, kFirst + 4))));
  EXPECT_THAT(cold_applied_.Keys(),
              UnorderedElementsAreArray(
                  Concat(KeysOf(0, kFirst + 6, kFirst + 10), KeysOf(1, kFirst, kFirst + 4))));
  EXPECT_TRUE(Missed(logs).empty());
  EXPECT_EQ(cold_pool_->ConsumerFor(0).LatestDrainedSeq(), kFirst + 9);
  EXPECT_EQ(hot_pool_->ConsumerFor(1).HighestSettledSeq(), kFirst + 3);
}

// Cold trails behind hot's start, so the scan starts at cold's cursor
// and hot skips what precedes its own.
TEST_F(RecoveryCoordinatorTest, AColdCommitBehindFirstSeqStartsTheScanAtCold) {
  Fill(0, 8);
  first_[0] = kFirst + 6;
  cold_commit_[0] = kFirst + 2;

  ASSERT_TRUE(RunBounded().has_value());

  EXPECT_EQ(queue_.ScannedFrom()[0], kFirst + 3);
  EXPECT_THAT(hot_applied_.Keys(), UnorderedElementsAreArray(KeysOf(0, kFirst + 6, kFirst + 8)));
  EXPECT_THAT(cold_applied_.Keys(), UnorderedElementsAreArray(KeysOf(0, kFirst + 3, kFirst + 8)));
}

// A scan that misses entries is caught by the trailing ReplayUntil,
// which applies them and warns, naming the tier, shard and count.
TEST_F(RecoveryCoordinatorTest, TheTrailingReplayWarnsAboutWhatTheScanMissed) {
  Fill(0, 10);
  first_[0] = kFirst + 2;
  cold_commit_[0] = kFirst + 5;
  Fill(1, 4);
  queue_.instead = [] { return core::Result<void>{}; };
  const log::testing::CapturingSink logs;

  ASSERT_TRUE(RunBounded().has_value());

  std::vector<std::string> warned;
  for (const auto& record : Missed(logs)) {
    std::string line;
    for (const auto& [key, value] : record.fields)
      line.append(key).append("=").append(value).append(" ");
    warned.push_back(line);
  }
  EXPECT_THAT(warned, UnorderedElementsAreArray(
                          {"tier=cold shard=0 entries=4 ", "tier=hot shard=0 entries=8 ",
                           "tier=cold shard=1 entries=4 ", "tier=hot shard=1 entries=4 "}));
  EXPECT_THAT(hot_applied_.Keys(),
              UnorderedElementsAreArray(
                  Concat(KeysOf(0, kFirst + 2, kFirst + 10), KeysOf(1, kFirst, kFirst + 4))));
}

// A shard with nothing written gets no trailing ReplayUntil, which
// would only wait out a read timeout.
TEST_F(RecoveryCoordinatorTest, AnEmptyShardGetsNoTrailingReplay) {
  Fill(0, 3);
  const log::testing::CapturingSink logs;

  ASSERT_TRUE(RunBounded().has_value());

  EXPECT_EQ(queue_.ScannedEnd(), (std::vector<core::SequenceId>{kFirst + 3, kFirst}));
  std::vector<std::string> started;
  for (const auto& record : logs.Records()) {
    if (record.msg != "hot replay starting" && record.msg != "cold replay starting") continue;
    for (const auto& [key, value] : record.fields) {
      if (key == "shard") started.push_back(value);
    }
  }
  EXPECT_THAT(started, UnorderedElementsAreArray({"0", "0"}));
}

TEST_F(RecoveryCoordinatorTest, AScanErrorFailsRecovery) {
  Fill(0, 3);
  queue_.instead = [] {
    return core::Result<void>(
        std::unexpected(core::Error{core::ErrorCode::kCorruption, "scan found a torn frame"}));
  };

  auto run = RunBounded();
  ASSERT_FALSE(run.has_value());
  EXPECT_EQ(run.error().code(), core::ErrorCode::kCorruption);
  EXPECT_FALSE(coordinator_->IsRecovering());
}

TEST_F(RecoveryCoordinatorTest, ACancelDuringTheScanReturnsUnavailable) {
  Fill(0, 3);
  queue_.instead = [this] {
    cancel_.store(true);
    return core::Result<void>(
        std::unexpected(core::Error{core::ErrorCode::kUnavailable, "scan cancelled"}));
  };

  auto run = RunBounded();
  ASSERT_FALSE(run.has_value());
  EXPECT_EQ(run.error().code(), core::ErrorCode::kUnavailable);
  EXPECT_EQ(run.error().message(), "recovery cancelled in cold/hot phase");
}

// A wipe that never succeeds stops the scan once drain_grace is spent,
// and recovery fails rather than hanging on it.
TEST_F(RecoveryCoordinatorTest, AColdWipeThatAlwaysFailsFailsRecoveryWithinDrainGrace) {
  logs_[0] = {Write(0, kFirst), Flush(kFirst + 1), Write(0, kFirst + 2)};
  cold_config_.drain_grace = 50ms;
  cold_config_.loop_max_backoff = 10ms;
  EXPECT_CALL(cold_, Wipe(0))
      .WillRepeatedly(Return(core::Result<void>(
          std::unexpected(core::Error{core::ErrorCode::kInternal, "cold store unwritable"}))));

  auto run = RunBounded();
  ASSERT_FALSE(run.has_value());
  EXPECT_EQ(run.error().code(), core::ErrorCode::kTimeout);
  EXPECT_THAT(run.error().message(), ::testing::HasSubstr("shard 0"));
  EXPECT_THAT(run.error().message(), ::testing::HasSubstr("seq " + std::to_string(kFirst + 1)));
  EXPECT_FALSE(cancel_.load()) << "recovery returned only once cancelled";
}

}  // namespace
}  // namespace abyss::engine
