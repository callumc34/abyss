#include "abyss/consumer/resolver.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <future>
#include <memory>
#include <string>
#include <thread>
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
  consumer::BufferKeyPresence Probe(std::string_view /*key*/) const override {
    return consumer::BufferKeyPresence::kAbsent;
  }
  consumer::HashOverlay HashOverlayFor(std::string_view /*key*/) const override {
    return consumer::HashOverlay{};
  }
  // Resolver tests don't depend on cold-consumer drain; report "caught up" so
  // the engine's gate never blocks.
  bool WaitForDrainedSeq(core::ShardId /*shard*/, core::SequenceId /*target_seq*/,
                         std::chrono::milliseconds /*timeout*/) override {
    return true;
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

// Drives a single Conditional through the steady-state Run() loop (NOT the
// recovery path) so ProcessEntry's lock-scope + hot-apply-wait + RPC fulfilment
// are exercised. Read returns the entries once then empties; the resolver
// thread keeps spinning on empty reads until Stop().
class ResolverRunTest : public ResolverTest {
 protected:
  void StubQueueReadRepeating(std::vector<core::QueueEntry> entries) {
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
};

// HOTC-6: when the hot-apply wait times out the client must NOT receive the
// success value — the write stays durable and applies on catch-up, but the
// reply mirrors the unconditional path's error. apply_wait_timeouts increments.
TEST_F(ResolverRunTest, ConditionalApplyTimeoutReturnsError) {
  config_.hot_apply_wait = std::chrono::milliseconds{30};
  StubQueueReadRepeating({MakeConditional(10, {"SETNX", "k", "v"}, core::PredicateFlags::kNx)});
  StubQueueAppendCapture();
  StubQueueAck();
  EXPECT_CALL(cold_, Exec(_, _)).WillRepeatedly(Return(core::RespValue::Integer(0)));

  // Register the client RPC the engine would have registered for the write.
  auto client = rpc_.Register(core::MakeRpcId(0, 10));

  Resolver resolver(queue_, cold_, buffer_router_, rpc_, apply_notifier_, config_);
  resolver.Start();

  // Never NotifyApplied -> WaitForHotApply times out.
  ASSERT_EQ(client.wait_for(5s), std::future_status::ready);
  auto reply = client.get();
  resolver.Stop();

  EXPECT_TRUE(reply.IsError()) << reply.AsString();
  EXPECT_GE(resolver.GetSnapshot().apply_wait_timeouts, 1U);
}

// HOTC-6 happy path: with hot notifying the appended Resolved seq, the client
// receives the success value (read-your-write holds).
TEST_F(ResolverRunTest, ConditionalAppliesThenSucceedsHappyPath) {
  config_.hot_apply_wait = std::chrono::milliseconds{1000};
  StubQueueReadRepeating({MakeConditional(10, {"SETNX", "k", "v"}, core::PredicateFlags::kNx)});
  StubQueueAppendCapture();
  StubQueueAck();
  EXPECT_CALL(cold_, Exec(_, _)).WillRepeatedly(Return(core::RespValue::Integer(0)));

  auto client = rpc_.Register(core::MakeRpcId(0, 10));

  Resolver resolver(queue_, cold_, buffer_router_, rpc_, apply_notifier_, config_);
  resolver.Start();

  // Hot applies the appended Resolved: notify the high-water past its seq. The
  // appended seq starts at next_appended_seq_ (1000); notify well past it.
  apply_notifier_.NotifyApplied(0, 2000);

  ASSERT_EQ(client.wait_for(5s), std::future_status::ready);
  auto reply = client.get();
  resolver.Stop();

  EXPECT_FALSE(reply.IsError()) << reply.AsString();
  EXPECT_EQ(reply.AsInteger(), 1);
  EXPECT_EQ(resolver.GetSnapshot().apply_wait_timeouts, 0U);
}

// XCONC-5: the blocking hot-apply wait runs OUTSIDE the stripe-locked region.
// Decide + Append + cache-update happen under the stripe lock; the wait does
// not. Drive a Conditional whose hot-apply never fires (long timeout): while
// the resolver thread is parked in WaitForHotApply, its Resolved has ALREADY
// been appended and the cache already updated, proving the lock was released
// before the wait. Pre-fix the lock spanned the wait, so a concurrent op on a
// colliding stripe would deadlock behind the full hot_apply_wait.
TEST_F(ResolverRunTest, StripeLocksReleasedBeforeHotApplyWait) {
  config_.stripe_count = 1;
  config_.hot_apply_wait = 10s;  // never notified; the resolver parks here
  StubQueueReadRepeating({MakeConditional(10, {"SETNX", "a", "1"}, core::PredicateFlags::kNx)});
  StubQueueAppendCapture();
  StubQueueAck();
  EXPECT_CALL(cold_, Exec(_, _)).WillRepeatedly(Return(core::RespValue::Integer(0)));

  Resolver resolver(queue_, cold_, buffer_router_, rpc_, apply_notifier_, config_);
  resolver.Start();

  // The Resolved is appended INSIDE the locked region, before the wait. Seeing
  // it appear while no notify has fired means Append+cache-update completed and
  // the thread has entered the lock-free wait. The notifier reports a pending
  // waiter, confirming the resolver is parked in AwaitApplied (not holding a
  // stripe lock).
  const auto deadline = std::chrono::steady_clock::now() + 3s;
  while ((appended_.empty() || apply_notifier_.PendingCount() == 0) &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(5ms);
  }
  EXPECT_EQ(appended_.size(), 1U);
  EXPECT_EQ(apply_notifier_.PendingCount(), 1U);

  // Release the parked wait so teardown does not block the full 10s.
  apply_notifier_.NotifyApplied(0, 1U << 20);
  resolver.RequestStop();
  resolver.Stop();
}

// XCONC-5 guard: the lock-scope refactor must NOT break per-key serialisation
// (ADP-011 inv 4). Two Conditionals on the SAME key still Decide+Append in
// queue order; the second observes the first's cache update (SETNX on the same
// key: first applies, second skips).
TEST_F(ResolverRunTest, ConditionalStillSerialisesSameKeyInQueueOrder) {
  config_.hot_apply_wait = std::chrono::milliseconds{1000};
  StubQueueReadRepeating({
      MakeConditional(10, {"SETNX", "k", "v1"}, core::PredicateFlags::kNx),
      MakeConditional(11, {"SETNX", "k", "v2"}, core::PredicateFlags::kNx),
  });
  StubQueueAppendCapture();
  StubQueueAck();
  // Cold reports absent; the first op's cache update must make the second skip.
  EXPECT_CALL(cold_, Exec(_, _)).WillRepeatedly(Return(core::RespValue::Integer(0)));

  Resolver resolver(queue_, cold_, buffer_router_, rpc_, apply_notifier_, config_);
  resolver.Start();
  apply_notifier_.NotifyApplied(0, 4000);  // let both waits resolve

  const auto deadline = std::chrono::steady_clock::now() + 3s;
  while (appended_.size() < 2 && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(5ms);
  }
  resolver.Stop();

  ASSERT_EQ(appended_.size(), 2U);
  const auto* first = std::get_if<core::entry::Resolved>(&appended_[0].payload);
  const auto* second = std::get_if<core::entry::Resolved>(&appended_[1].payload);
  ASSERT_NE(first, nullptr);
  ASSERT_NE(second, nullptr);
  EXPECT_EQ(first->ref, 10U);
  EXPECT_EQ(first->decision, core::Decision::kApply);
  EXPECT_EQ(second->ref, 11U);
  EXPECT_EQ(second->decision, core::Decision::kSkip);
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
