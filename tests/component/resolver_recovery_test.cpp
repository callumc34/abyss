#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstddef>
#include <memory>
#include <optional>
#include <string_view>

#include "abyss/consumer/compaction_buffer.h"
#include "abyss/consumer/compaction_buffer_router.h"
#include "abyss/consumer/resolver.h"
#include "abyss/core/apply_notifier.h"
#include "abyss/core/consumer_rpc.h"
#include "abyss/core/ops.h"
#include "abyss/core/queue_entry.h"
#include "abyss/core/resp_types.h"
#include "abyss/core/result.h"
#include "abyss/core/types.h"
#include "abyss/queue/fsync_policy.h"
#include "abyss/queue/wal_queue.h"
#include "mock_cold_store.h"
#include "temp_dir.h"

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
  bool WaitForDrainedSeq(core::ShardId /*shard*/, core::SequenceId /*target_seq*/,
                         std::chrono::milliseconds /*timeout*/) override {
    return true;
  }
};

class ResolverRecoveryTest : public ::testing::Test {
 protected:
  void SetUp() override { dir_ = std::make_unique<testing::TempDir>("resolver_recovery"); }

  queue::WalConfig Config(queue::FsyncPolicy policy) const {
    return queue::WalConfig{
        .wal_path = dir_->String(),
        .segment_size_bytes = 4096,
        .shard_count = 1,
        // Never fires mid-test, so only Open's sync can make a seq durable.
        .commit = {.policy = policy,
                   .interval = std::chrono::microseconds{10'000'000},
                   .max_bytes = size_t{1024} * 1024},
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

// Conditional X and its Resolved Y were never fsynced before the crash.
// Open syncs the recovered tail, so replay commits past X with Y durable.
TEST_F(ResolverRecoveryTest, ReplayCommitsPastRecoveredConditionalAndResolved) {
  {
    auto unsynced = queue::WalQueue::Open(Config(queue::FsyncPolicy::kNone));
    ASSERT_TRUE(unsynced.has_value()) << unsynced.error().message();
    auto x = (*unsynced)->Append(0, Conditional());
    ASSERT_TRUE(x.has_value());
    ASSERT_TRUE((*unsynced)->Append(0, ResolvedFor(x->seq)).has_value());
  }

  auto reopened = queue::WalQueue::Open(Config(queue::FsyncPolicy::kGroupCommit));
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
            std::optional<core::SequenceId>{1});
  EXPECT_GE(queue.DurableSeq(0).value(), 1U) << "the recovered Resolved is not durable";
  EXPECT_EQ(queue.TailSeq(0).value(), 1U) << "replay re-emitted a Resolved";
  EXPECT_EQ(resolver.GetSnapshot().commit_failures, 0U);
}

}  // namespace
}  // namespace abyss::consumer
