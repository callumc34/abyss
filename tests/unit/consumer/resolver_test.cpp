#include "abyss/consumer/resolver.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <future>
#include <memory>
#include <string>
#include <vector>

#include "abyss/consumer/compaction_buffer_router.h"
#include "abyss/core/apply_notifier.h"
#include "abyss/core/consumer_rpc.h"
#include "mock_cold_store.h"
#include "mock_queue.h"

namespace abyss::consumer {
namespace {

using ::testing::_;
using ::testing::Return;
using namespace std::chrono_literals;

// A no-op buffer router that always reports the buffer as empty. Resolver
// decisions then route entirely cache → cold.
class EmptyBufferRouter : public CompactionBufferRouter {
 public:
  core::Result<core::RespValue> Exec(const core::ops::ReadOp& /*op*/,
                                     std::optional<core::Duration> /*deadline*/) override {
    return std::unexpected(core::Error(core::ErrorCode::kNotFound, "empty buffer (test)"));
  }
  core::Result<core::RespValue> Read(std::string_view /*key*/) const override {
    return std::unexpected(core::Error(core::ErrorCode::kNotFound, "empty buffer (test)"));
  }
  consumer::HashOverlay HashOverlayFor(std::string_view /*key*/) const override {
    return consumer::HashOverlay{};
  }
};

class ResolverTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // The resolver runs as a thread; for unit tests we don't Start() it —
    // instead we drive it indirectly via ReplayForRecovery() which scans
    // the queue once, decides every Conditional, and emits Resolveds.
    config_.shard = 0;
    config_.read_batch_size = 32;
    config_.read_timeout = core::Duration{10};
    config_.cold_lookup_timeout = std::chrono::milliseconds{50};
    config_.stripe_count = 4;
    config_.hot_apply_wait = std::chrono::milliseconds{50};
  }

  // Sets up MockQueue::Read so the first call returns `entries`, subsequent
  // calls return empty (terminating the replay scan).
  void StubQueueReadOnce(std::vector<core::QueueEntry> entries) {
    auto remaining = std::make_shared<std::vector<core::QueueEntry>>(std::move(entries));
    EXPECT_CALL(queue_, Read(core::kResolverConsumer, 0, _, _))
        .WillRepeatedly([remaining](core::ConsumerId, core::ShardId, size_t,
                                    core::Duration) -> core::Result<std::vector<core::QueueEntry>> {
          if (remaining->empty()) return std::vector<core::QueueEntry>{};
          auto out = std::move(*remaining);
          remaining->clear();
          return out;
        });
  }

  // MockQueue::Append: records the appended entry and returns a synthetic seq.
  void StubQueueAppendCapture() {
    EXPECT_CALL(queue_, Append(_, _))
        .WillRepeatedly([this](core::ShardId /*shard*/,
                               core::QueueEntry entry) -> core::Result<queue::AppendResult> {
          const auto seq = next_appended_seq_++;
          entry.seq = seq;
          appended_.push_back(std::move(entry));
          std::promise<core::Result<void>> p;
          p.set_value(core::Result<void>{});
          return queue::AppendResult{.seq = seq, .durable = p.get_future()};
        });
  }

  void StubQueueAck() {
    EXPECT_CALL(queue_, Ack(_, _, _)).WillRepeatedly(Return(core::Result<void>{}));
  }

  core::QueueEntry MakeConditional(core::SequenceId seq, std::vector<std::string> args,
                                   core::PredicateFlags flags) {
    return core::QueueEntry{
        .seq = seq,
        .appended_at = core::WallClock::now(),
        .payload = core::entry::Conditional{.cmd = core::RespCommand{.args = std::move(args)},
                                            .flags = flags},
    };
  }

  // NOLINTBEGIN(cppcoreguidelines-non-private-member-variables-in-classes)
  ::testing::NiceMock<testing::MockQueue> queue_;
  ::testing::NiceMock<testing::MockColdStore> cold_;
  EmptyBufferRouter buffer_router_;
  core::ConsumerRpc rpc_;
  core::ApplyNotifier apply_notifier_;
  Resolver::Config config_;
  std::vector<core::QueueEntry> appended_;
  core::SequenceId next_appended_seq_ = 1000;
  // Tests never cancel — pass to ReplayForRecovery to satisfy the API.
  std::atomic<bool> cancel_{false};
  // NOLINTEND(cppcoreguidelines-non-private-member-variables-in-classes)
};

TEST_F(ResolverTest, SetnxOnAbsentKeyDecidesApply) {
  StubQueueReadOnce({MakeConditional(10, {"SETNX", "k", "v"}, core::PredicateFlags::kNx)});
  StubQueueAppendCapture();
  StubQueueAck();
  // Cold returns "key absent": Exists=0, StringGet=Null.
  EXPECT_CALL(cold_, Exec(_, _)).WillRepeatedly(Return(core::RespValue::Integer(0)));

  Resolver resolver(queue_, cold_, buffer_router_, rpc_, apply_notifier_, config_);
  ASSERT_TRUE(resolver.ReplayForRecovery(cancel_).has_value());

  ASSERT_EQ(appended_.size(), 1U);
  const auto* resolved = std::get_if<core::entry::Resolved>(&appended_[0].payload);
  ASSERT_NE(resolved, nullptr);
  EXPECT_EQ(resolved->ref, 10U);
  EXPECT_EQ(resolved->decision, core::Decision::kApply);
  ASSERT_EQ(resolved->materialised_ops.size(), 1U);
  EXPECT_EQ(resolved->materialised_ops[0].args[0], "SET");
  EXPECT_TRUE(resolved->return_value.IsInteger());
  EXPECT_EQ(resolved->return_value.AsInteger(), 1);
}

TEST_F(ResolverTest, SetnxOnPresentKeyDecidesSkip) {
  StubQueueReadOnce({MakeConditional(10, {"SETNX", "k", "v"}, core::PredicateFlags::kNx)});
  StubQueueAppendCapture();
  StubQueueAck();
  // Cold says key exists.
  EXPECT_CALL(cold_, Exec(_, _)).WillRepeatedly(Return(core::RespValue::Integer(1)));

  Resolver resolver(queue_, cold_, buffer_router_, rpc_, apply_notifier_, config_);
  ASSERT_TRUE(resolver.ReplayForRecovery(cancel_).has_value());

  ASSERT_EQ(appended_.size(), 1U);
  const auto* resolved = std::get_if<core::entry::Resolved>(&appended_[0].payload);
  ASSERT_NE(resolved, nullptr);
  EXPECT_EQ(resolved->decision, core::Decision::kSkip);
  EXPECT_TRUE(resolved->materialised_ops.empty());
  EXPECT_EQ(resolved->return_value.AsInteger(), 0);
}

TEST_F(ResolverTest, SetXxOnAbsentKeyDecidesSkipReturnsNull) {
  StubQueueReadOnce({MakeConditional(20, {"SET", "k", "v", "XX"}, core::PredicateFlags::kXx)});
  StubQueueAppendCapture();
  StubQueueAck();
  EXPECT_CALL(cold_, Exec(_, _)).WillRepeatedly(Return(core::RespValue::Integer(0)));

  Resolver resolver(queue_, cold_, buffer_router_, rpc_, apply_notifier_, config_);
  ASSERT_TRUE(resolver.ReplayForRecovery(cancel_).has_value());

  ASSERT_EQ(appended_.size(), 1U);
  const auto* resolved = std::get_if<core::entry::Resolved>(&appended_[0].payload);
  ASSERT_NE(resolved, nullptr);
  EXPECT_EQ(resolved->decision, core::Decision::kSkip);
  EXPECT_TRUE(resolved->return_value.IsNull());
}

TEST_F(ResolverTest, MsetnxAtomicSkipsAllIfAnyExist) {
  StubQueueReadOnce(
      {MakeConditional(30, {"MSETNX", "a", "1", "b", "2"}, core::PredicateFlags::kMsetNx)});
  StubQueueAppendCapture();
  StubQueueAck();
  // Cold says key 'b' exists by returning Integer(1) for any Exists call —
  // MSETNX requires all-absent, so any present key triggers Skip.
  EXPECT_CALL(cold_, Exec(_, _)).WillRepeatedly(Return(core::RespValue::Integer(1)));
  Resolver resolver(queue_, cold_, buffer_router_, rpc_, apply_notifier_, config_);
  ASSERT_TRUE(resolver.ReplayForRecovery(cancel_).has_value());

  ASSERT_EQ(appended_.size(), 1U);
  const auto* resolved = std::get_if<core::entry::Resolved>(&appended_[0].payload);
  ASSERT_NE(resolved, nullptr);
  EXPECT_EQ(resolved->decision, core::Decision::kSkip);
  EXPECT_EQ(resolved->return_value.AsInteger(), 0);
}

TEST_F(ResolverTest, ColdTimeoutDecidesSkipWithError) {
  StubQueueReadOnce({MakeConditional(40, {"SETNX", "k", "v"}, core::PredicateFlags::kNx)});
  StubQueueAppendCapture();
  StubQueueAck();
  EXPECT_CALL(cold_, Exec(_, _))
      .WillRepeatedly(
          Return(std::unexpected(core::Error{core::ErrorCode::kTimeout, "cold deadline elapsed"})));

  Resolver resolver(queue_, cold_, buffer_router_, rpc_, apply_notifier_, config_);
  ASSERT_TRUE(resolver.ReplayForRecovery(cancel_).has_value());

  ASSERT_EQ(appended_.size(), 1U);
  const auto* resolved = std::get_if<core::entry::Resolved>(&appended_[0].payload);
  ASSERT_NE(resolved, nullptr);
  EXPECT_EQ(resolved->decision, core::Decision::kSkip);
  EXPECT_TRUE(resolved->return_value.IsError());
}

TEST_F(ResolverTest, RecoveryRebuildsCacheFromExistingResolved) {
  // Sequence: Conditional + matching Resolved — replay should NOT emit a
  // new Resolved for this Conditional (it's already resolved).
  std::vector<core::QueueEntry> entries;
  entries.push_back(MakeConditional(50, {"SETNX", "k", "v"}, core::PredicateFlags::kNx));
  entries.push_back(core::QueueEntry{
      .seq = 51,
      .appended_at = core::WallClock::now(),
      .payload =
          core::entry::Resolved{
              .ref = 50,
              .decision = core::Decision::kApply,
              .materialised_ops = {core::RespCommand{{"SET", "k", "v"}}},
              .return_value = core::RespValue::Integer(1),
          },
  });
  StubQueueReadOnce(std::move(entries));
  StubQueueAppendCapture();
  StubQueueAck();
  EXPECT_CALL(cold_, Exec(_, _)).Times(::testing::AnyNumber());

  Resolver resolver(queue_, cold_, buffer_router_, rpc_, apply_notifier_, config_);
  ASSERT_TRUE(resolver.ReplayForRecovery(cancel_).has_value());

  EXPECT_TRUE(appended_.empty());  // no new Resolveds emitted
  EXPECT_EQ(resolver.GetSnapshot().replayed_resolveds_emitted, 0U);
}

}  // namespace
}  // namespace abyss::consumer
