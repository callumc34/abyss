#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <filesystem>
#include <memory>
#include <mutex>
#include <optional>
#include <string_view>

#include "abyss/consumer/compaction_buffer.h"
#include "abyss/consumer/compaction_buffer_router.h"
#include "abyss/consumer/resolver.h"
#include "abyss/core/apply_notifier.h"
#include "abyss/core/consumer_rpc.h"
#include "abyss/core/durability.h"
#include "abyss/core/ops.h"
#include "abyss/core/queue_entry.h"
#include "abyss/core/resp_types.h"
#include "abyss/core/result.h"
#include "abyss/core/types.h"
#include "abyss/queue/wal_queue.h"
#include "mock_cold_store.h"
#include "temp_dir.h"
#include "wal_power_loss.h"

namespace abyss::consumer {
namespace {

using namespace std::chrono_literals;

// Replay of an already-resolved Conditional never consults the buffer.
class EmptyBufferRouter : public CompactionBufferRouter {
 public:
  core::Result<core::RespValue> Exec(const core::ops::ReadOp& /*op*/,
                                     std::optional<core::Duration> /*deadline*/) override {
    return std::unexpected(core::Error(core::ErrorCode::kNotFound, "empty buffer (test)"));
  }
  core::Result<core::RespValue> Read(std::string_view /*key*/) const override {
    return std::unexpected(core::Error(core::ErrorCode::kNotFound, "empty buffer (test)"));
  }
  BufferKeyPresence Probe(std::string_view /*key*/) const override {
    return BufferKeyPresence::kAbsent;
  }
  HashOverlay HashOverlayFor(std::string_view /*key*/) const override { return HashOverlay{}; }
  std::optional<CompactedState> Snapshot(core::ShardId /*shard*/,
                                         std::string_view /*key*/) const override {
    return std::nullopt;
  }
  bool WaitForDrainedSeq(core::ShardId /*shard*/, core::SequenceId /*target_seq*/,
                         std::chrono::milliseconds /*timeout*/) override {
    return true;
  }
};

class ResolverRecoveryTest : public ::testing::Test {
 protected:
  void SetUp() override { dir_ = std::make_unique<testing::TempDir>("resolver_recovery"); }

  queue::WalConfig Config() const {
    return queue::WalConfig{
        .wal_path = dir_->String(),
        .segment_size_bytes = 8192,
        .shard_count = 1,
        .durability = core::Durability::kProcessCrash,
        .min_retention = 1s,
        .retention_consumers = {core::kColdConsumer, core::kResolverConsumer},
        .offset_fsync_interval = std::chrono::hours{1},
    };
  }

  static core::QueueEntry Conditional() {
    return core::QueueEntry{
        .seq = 0,
        .appended_at = core::WallClock::now(),
        .payload = core::entry::Conditional{.cmd = core::RespCommand{{"SETNX", "k", "v"}},
                                            .flags = core::PredicateFlags::kNx},
    };
  }

  static core::QueueEntry ResolvedFor(core::SequenceId ref) {
    return core::QueueEntry{
        .seq = 0,
        .appended_at = core::WallClock::now(),
        .payload =
            core::entry::Resolved{
                .ref = ref,
                .decision = core::Decision::kApply,
                .materialised_ops = {core::RespCommand{{"SET", "k", "v"}}},
                .return_value = core::RespValue::Integer(1),
            },
    };
  }

  // NOLINTBEGIN(cppcoreguidelines-non-private-member-variables-in-classes)
  std::unique_ptr<testing::TempDir> dir_;
  ::testing::NiceMock<testing::MockColdStore> cold_;
  EmptyBufferRouter router_;
  core::ConsumerRpc rpc_;
  core::ApplyNotifier apply_notifier_;
  // NOLINTEND(cppcoreguidelines-non-private-member-variables-in-classes)
};

// Holds every WAL flush until released.
class FlushStall {
 public:
  queue::FlushHook Hook() const {
    return [state = state_](uint32_t) -> core::Result<void> {
      std::unique_lock lock(state->mu);
      state->cv.wait(lock, [&state] { return state->released; });
      return {};
    };
  }
  void Release() const {
    {
      const std::scoped_lock lock(state_->mu);
      state_->released = true;
    }
    state_->cv.notify_all();
  }

 private:
  struct State {
    std::mutex mu;
    std::condition_variable cv;
    bool released = false;
  };
  std::shared_ptr<State> state_ = std::make_shared<State>();
};

// Conditional X and its Resolved Y were acknowledged at process_crash and
// the process died without a final flush. The page cache survives a
// process crash and Open syncs the recovered tail, so replay commits past
// X with Y durable and re-emits nothing.
TEST_F(ResolverRecoveryTest, ReplayCommitsPastRecoveredConditionalAndResolved) {
  {
    auto crashed = queue::WalQueue::Open(Config());
    ASSERT_TRUE(crashed.has_value()) << crashed.error().message();
    auto x = (*crashed)->Append(0, Conditional());
    ASSERT_TRUE(x.has_value());
    ASSERT_TRUE((*crashed)->Append(0, ResolvedFor(x->seq)).has_value());
    (*crashed)->SkipFinalFlushForTesting();
  }

  auto reopened = queue::WalQueue::Open(Config());
  ASSERT_TRUE(reopened.has_value()) << reopened.error().message();
  auto& queue = **reopened;

  Resolver::Config cfg;
  cfg.shard = 0;
  cfg.durable_wait_timeout = 200ms;
  Resolver resolver(queue, cold_, router_, rpc_, apply_notifier_, cfg);
  const std::atomic<bool> cancel{false};
  auto replay = resolver.ReplayForRecovery(cancel);
  ASSERT_TRUE(replay.has_value()) << replay.error().message();

  EXPECT_EQ(queue.CommittedOffset(core::kResolverConsumer, 0).value(),
            std::optional<core::SequenceId>{core::kFirstSeq + 1});
  EXPECT_EQ(queue.DurableEnd(0, core::Durability::kPowerLoss).value(), core::kFirstSeq + 2)
      << "the recovered Resolved is not power-durable";
  EXPECT_EQ(queue.TailSeq(0).value(), core::kFirstSeq + 1) << "replay re-emitted a Resolved";
  EXPECT_EQ(resolver.GetSnapshot().commit_failures, 0U);
}

// The same pair, never flushed, then a power loss: the log keeps only
// what the last flush covered. X and Y vanish together, and replay of
// the shortened log commits nothing.
TEST_F(ResolverRecoveryTest, PowerLossDropsAnUnflushedConditionalAndResolvedTogether) {
  queue::DurableExtent flushed;
  {
    auto crashed = queue::WalQueue::Open(Config());
    ASSERT_TRUE(crashed.has_value()) << crashed.error().message();
    const FlushStall stall;
    (*crashed)->SetFlushHookForTesting(stall.Hook());
    auto x = (*crashed)->Append(0, Conditional());
    ASSERT_TRUE(x.has_value());
    ASSERT_TRUE((*crashed)->Append(0, ResolvedFor(x->seq)).has_value());
    ASSERT_FALSE(
        (*crashed)->AwaitDurable(0, core::kFirstSeq, core::Durability::kPowerLoss, 0ms).value());
    flushed = (*crashed)->DurableExtentForTesting(0);
    (*crashed)->SkipFinalFlushForTesting();
    stall.Release();
  }
  testing::SimulatePowerLoss(flushed);

  auto reopened = queue::WalQueue::Open(Config());
  ASSERT_TRUE(reopened.has_value()) << reopened.error().message();
  auto& queue = **reopened;
  ASSERT_EQ(queue.DurableEnd(0, core::Durability::kPowerLoss).value(), core::kFirstSeq);

  Resolver::Config cfg;
  cfg.shard = 0;
  cfg.durable_wait_timeout = 200ms;
  Resolver resolver(queue, cold_, router_, rpc_, apply_notifier_, cfg);
  const std::atomic<bool> cancel{false};
  auto replay = resolver.ReplayForRecovery(cancel);
  ASSERT_TRUE(replay.has_value()) << replay.error().message();

  EXPECT_EQ(queue.CommittedOffset(core::kResolverConsumer, 0).value(), std::nullopt);
  EXPECT_EQ(queue.DurableEnd(0, core::Durability::kProcessCrash).value(), core::kFirstSeq);
  EXPECT_EQ(resolver.GetSnapshot().commit_failures, 0U);
}

}  // namespace
}  // namespace abyss::consumer
