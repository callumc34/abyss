#include "abyss/consumer/resolver.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <future>
#include <limits>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <thread>
#include <variant>
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
  //
  // ResolverRunTest drives a live Resolver thread, so this runs off that thread
  // while the test body polls appended_ from the main one. Both sides take
  // appended_mu_; the threaded tests read through the accessors below rather
  // than touching the vector directly.
  void StubQueueAppendCapture() {
    EXPECT_CALL(queue_, Append(_, _))
        .WillRepeatedly([this](core::ShardId /*shard*/,
                               core::QueueEntry entry) -> core::Result<queue::AppendResult> {
          std::promise<core::Result<void>> p;
          p.set_value(core::Result<void>{});
          const std::scoped_lock lock(appended_mu_);
          const auto seq = next_appended_seq_++;
          entry.seq = seq;
          appended_.push_back(std::move(entry));
          return queue::AppendResult{.seq = seq, .durable = p.get_future()};
        });
  }

  size_t AppendedSize() {
    const std::scoped_lock lock(appended_mu_);
    return appended_.size();
  }

  std::vector<core::QueueEntry> AppendedSnapshot() {
    const std::scoped_lock lock(appended_mu_);
    return appended_;
  }

  void StubQueueAck() {
    EXPECT_CALL(queue_, Ack(_, _, _)).WillRepeatedly(Return(core::Result<void>{}));
  }

  // Captures the highest acked seq for kResolverConsumer. Fail-closed: an Ack
  // past durable_seq_ returns kFailedPrecondition (mirrors the real WAL gate),
  // so a clamp bug surfaces as a rejected ack rather than silent success.
  void StubQueueAckCapture() {
    EXPECT_CALL(queue_, Ack(core::kResolverConsumer, 0, _))
        .WillRepeatedly(
            [this](core::ConsumerId, core::ShardId, core::SequenceId seq) -> core::Result<void> {
              if (seq > durable_seq_.load()) {
                return std::unexpected(
                    core::Error(core::ErrorCode::kFailedPrecondition, "ack past durable (test)"));
              }
              auto cur = acked_seq_.load();
              while (seq > cur && !acked_seq_.compare_exchange_weak(cur, seq)) {
              }
              ack_called_.store(true);
              return core::Result<void>{};
            });
  }

  // Drives DurableSeq/AwaitDurable off the test-controlled durable_seq_ so a
  // test can hold a Resolved non-durable then release it.
  void StubControllableDurability() {
    EXPECT_CALL(queue_, DurableSeq(0))
        .WillRepeatedly([this](core::ShardId) -> core::Result<core::SequenceId> {
          return durable_seq_.load();
        });
    EXPECT_CALL(queue_, AwaitDurable(0, _, _))
        .WillRepeatedly(
            [this](core::ShardId, core::SequenceId seq, core::Duration) -> core::Result<bool> {
              return durable_seq_.load() >= seq;
            });
  }

  core::QueueEntry MakeConditional(core::SequenceId seq, std::vector<std::string> args,
                                   core::PredicateFlags flags) {
    return MakeConditionalAt(seq, std::move(args), flags, core::WallClock::now());
  }

  // Controls `appended_at` so the deterministic cache-apply clock (A3) and the
  // line-215 expiry guard can be exercised against a known wall-ms value.
  static core::WallTime At(uint64_t ms) { return core::WallTime{std::chrono::milliseconds{ms}}; }

  core::QueueEntry MakeConditionalAt(core::SequenceId seq, std::vector<std::string> args,
                                     core::PredicateFlags flags, core::WallTime appended_at) {
    return core::QueueEntry{
        .seq = seq,
        .appended_at = appended_at,
        .payload = core::entry::Conditional{.cmd = core::RespCommand{.args = std::move(args)},
                                            .flags = flags},
    };
  }

  core::QueueEntry MakeWriteAt(core::SequenceId seq, std::vector<std::string> args,
                               core::WallTime appended_at) {
    return core::QueueEntry{
        .seq = seq,
        .appended_at = appended_at,
        .payload = core::entry::Write{.cmd = core::RespCommand{.args = std::move(args)}},
    };
  }

  // NOLINTBEGIN(cppcoreguidelines-non-private-member-variables-in-classes)
  ::testing::NiceMock<testing::MockQueue> queue_;
  ::testing::NiceMock<testing::MockColdStore> cold_;
  EmptyBufferRouter buffer_router_;
  core::ConsumerRpc rpc_;
  core::ApplyNotifier apply_notifier_;
  Resolver::Config config_;
  std::mutex appended_mu_;
  std::vector<core::QueueEntry> appended_;
  core::SequenceId next_appended_seq_ = 1000;
  // Test-controlled durable watermark for the durability-clamp tests. Default
  // max() = "everything durable" so tests not exercising durability are
  // unaffected (matches MockQueue's permissive default).
  std::atomic<core::SequenceId> durable_seq_{std::numeric_limits<core::SequenceId>::max()};
  std::atomic<core::SequenceId> acked_seq_{0};
  std::atomic<bool> ack_called_{false};
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
  while ((AppendedSize() == 0 || apply_notifier_.PendingCount() == 0) &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(5ms);
  }
  EXPECT_EQ(AppendedSize(), 1U);
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
  while (AppendedSize() < 2 && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(5ms);
  }
  resolver.Stop();

  ASSERT_EQ(AppendedSize(), 2U);
  const auto snapshot = AppendedSnapshot();
  const auto* first = std::get_if<core::entry::Resolved>(&snapshot[0].payload);
  const auto* second = std::get_if<core::entry::Resolved>(&snapshot[1].payload);
  ASSERT_NE(first, nullptr);
  ASSERT_NE(second, nullptr);
  EXPECT_EQ(first->ref, 10U);
  EXPECT_EQ(first->decision, core::Decision::kApply);
  EXPECT_EQ(second->ref, 11U);
  EXPECT_EQ(second->decision, core::Decision::kSkip);
}

// XDUR-2: the resolver's persisted retention ack must never outrun the durable
// tail. With the group committer "paused" (durable_seq_ held below the emitted
// Resolved's seq), the resolver appends the Resolved for Conditional 10 but its
// persisted ack stays clamped below 10 — a crash here would re-read and
// re-decide the Conditional. Once the Resolved becomes durable the ack advances
// to 10. Pre-fix the resolver acked the Conditional's seq on drain progress,
// losing the Resolved across a crash in the fsync-coalescing window.
TEST_F(ResolverRunTest, AckClampedBehindNonDurableResolved) {
  config_.hot_apply_wait = std::chrono::milliseconds{30};
  config_.durable_wait_timeout = std::chrono::milliseconds{30};
  StubQueueReadRepeating({MakeConditional(10, {"SETNX", "k", "v"}, core::PredicateFlags::kNx)});
  StubQueueAppendCapture();  // Resolved is appended at seq 1000.
  StubQueueAckCapture();
  StubControllableDurability();
  EXPECT_CALL(cold_, Exec(_, _)).WillRepeatedly(Return(core::RespValue::Integer(0)));

  // Nothing durable yet: the committer is "paused".
  durable_seq_.store(0);

  Resolver resolver(queue_, cold_, buffer_router_, rpc_, apply_notifier_, config_);
  resolver.Start();

  // Wait until the Resolved has been appended (the resolver has processed the
  // Conditional). The ack must remain clamped below the Conditional's seq.
  const auto deadline = std::chrono::steady_clock::now() + 3s;
  while (AppendedSize() == 0 && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(5ms);
  }
  ASSERT_EQ(AppendedSize(), 1U);
  // Give the Run loop several ack cycles to (incorrectly) advance, if it would.
  std::this_thread::sleep_for(100ms);
  EXPECT_LT(acked_seq_.load(), 10U) << "ack advanced past Conditional before Resolved durable";

  // Release durability for the Resolved (seq 1000).
  durable_seq_.store(2000);
  apply_notifier_.NotifyApplied(0, 2000);

  const auto ack_deadline = std::chrono::steady_clock::now() + 3s;
  while (acked_seq_.load() < 10U && std::chrono::steady_clock::now() < ack_deadline) {
    std::this_thread::sleep_for(5ms);
  }
  resolver.Stop();

  EXPECT_GE(acked_seq_.load(), 10U) << "ack did not advance after Resolved became durable";
  EXPECT_GE(resolver.GetSnapshot().resolver_durable_floor, 10U);
}

// XDUR-2 / XERR-3 client path (mirror-assert): the conditional client ack is
// NOT returned success until the Resolved is durable AND hot-applied. On a
// durability timeout the client receives an ERROR (never the success value);
// the write stays durable in the WAL and re-applies on catch-up.
TEST_F(ResolverRunTest, ConditionalDurabilityTimeoutReturnsError) {
  config_.hot_apply_wait = std::chrono::milliseconds{1000};
  config_.durable_wait_timeout = std::chrono::milliseconds{30};
  StubQueueReadRepeating({MakeConditional(10, {"SETNX", "k", "v"}, core::PredicateFlags::kNx)});
  StubQueueAppendCapture();
  StubQueueAck();
  StubControllableDurability();
  EXPECT_CALL(cold_, Exec(_, _)).WillRepeatedly(Return(core::RespValue::Integer(0)));

  // The Resolved (seq 1000) never becomes durable -> durability wait times out.
  durable_seq_.store(0);
  // Hot would apply, proving durability is checked FIRST: even with apply ready,
  // the non-durable Resolved must yield an error.
  apply_notifier_.NotifyApplied(0, 2000);

  auto client = rpc_.Register(core::MakeRpcId(0, 10));

  Resolver resolver(queue_, cold_, buffer_router_, rpc_, apply_notifier_, config_);
  resolver.Start();

  ASSERT_EQ(client.wait_for(5s), std::future_status::ready);
  auto reply = client.get();
  resolver.Stop();

  EXPECT_TRUE(reply.IsError()) << reply.AsString();
  EXPECT_GE(resolver.GetSnapshot().durable_wait_timeouts, 1U);
  // The Resolved was still appended (durable in the WAL, will apply on catch-up).
  ASSERT_EQ(AppendedSize(), 1U);
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

  EXPECT_TRUE(AppendedSize() == 0);  // no new Resolveds emitted
  EXPECT_EQ(resolver.GetSnapshot().replayed_resolveds_emitted, 0U);
}

// Finds the Resolved emitted for conditional `ref` in `appended_`.
const core::entry::Resolved* FindResolvedFor(const std::vector<core::QueueEntry>& appended,
                                             core::SequenceId ref) {
  for (const auto& e : appended) {
    if (const auto* r = std::get_if<core::entry::Resolved>(&e.payload);
        r != nullptr && r->ref == ref) {
      return r;
    }
  }
  return nullptr;
}

// HOTC-1 / XCONC-1: a relative `SET k v EX 100` must cache an absolute TTL
// derived from the write's appended_at (not 0+100s ≈ 1970). A follow-up
// `SET k v2 NX` issued while the key is still live must SKIP.
TEST_F(ResolverTest, SetWithRelativeExThenSetNxSeesLiveKeyAndSkips) {
  // T0 = 1_000_000 ms since epoch; the write expires at T0 + 100s.
  StubQueueReadOnce({
      MakeWriteAt(10, {"SET", "k", "v", "EX", "100"}, At(1'000'000)),
      MakeConditionalAt(11, {"SET", "k", "v2", "NX"}, core::PredicateFlags::kNx, At(1'050'000)),
  });
  StubQueueAppendCapture();
  StubQueueAck();
  // Cache hit short-circuits cold; stub permissively anyway.
  EXPECT_CALL(cold_, Exec(_, _)).Times(::testing::AnyNumber());

  Resolver resolver(queue_, cold_, buffer_router_, rpc_, apply_notifier_, config_);
  ASSERT_TRUE(resolver.ReplayForRecovery(cancel_).has_value());

  const auto* resolved = FindResolvedFor(appended_, 11);
  ASSERT_NE(resolved, nullptr);
  EXPECT_EQ(resolved->decision, core::Decision::kSkip);  // NX on a live key
  EXPECT_EQ(resolved->return_value.AsInteger(), 0);

  auto cached = resolver.Cache().GetKey("k");
  ASSERT_TRUE(cached.has_value());
  EXPECT_EQ(cached->abs_ttl_ms, 1'000'000U + 100'000U);  // appended_at + 100s, not ~1970
}

// HOTC-1 edge: a freshly set relative-EX key must not be treated as expired by
// the line-215 guard when evaluated at a time before the TTL elapses.
TEST_F(ResolverTest, SetWithRelativeExNotTreatedAsExpiredImmediately) {
  StubQueueReadOnce({
      MakeWriteAt(10, {"SET", "k", "v", "PX", "60000"}, At(2'000'000)),
      MakeConditionalAt(11, {"SET", "k", "v2", "XX", "KEEPTTL"},
                        core::PredicateFlags::kXx | core::PredicateFlags::kKeepTtl, At(2'000'001)),
  });
  StubQueueAppendCapture();
  StubQueueAck();
  EXPECT_CALL(cold_, Exec(_, _)).Times(::testing::AnyNumber());

  Resolver resolver(queue_, cold_, buffer_router_, rpc_, apply_notifier_, config_);
  ASSERT_TRUE(resolver.ReplayForRecovery(cancel_).has_value());

  const auto* resolved = FindResolvedFor(appended_, 11);
  ASSERT_NE(resolved, nullptr);
  EXPECT_EQ(resolved->decision, core::Decision::kApply);  // XX on a live key applies
  ASSERT_EQ(resolved->materialised_ops.size(), 1U);
  // KEEPTTL must carry the original PX-derived absolute TTL through.
  const auto& set_op = resolved->materialised_ops[0];
  ASSERT_EQ(set_op.args.size(), 5U);
  EXPECT_EQ(set_op.args[3], "PXAT");
  EXPECT_EQ(set_op.args[4], std::to_string(2'000'000U + 60'000U));
}

// HOTC-2: SETEX must cache a real absolute TTL (not 0). A subsequent
// `SET k v2 NX` while live must SKIP, and the TTL is appended_at + ttl.
TEST_F(ResolverTest, SetexThenSetNxSeesLiveKeyAndSkips) {
  StubQueueReadOnce({
      MakeWriteAt(10, {"SETEX", "k", "100", "v"}, At(3'000'000)),
      MakeConditionalAt(11, {"SET", "k", "v2", "NX"}, core::PredicateFlags::kNx, At(3'050'000)),
  });
  StubQueueAppendCapture();
  StubQueueAck();
  EXPECT_CALL(cold_, Exec(_, _)).Times(::testing::AnyNumber());

  Resolver resolver(queue_, cold_, buffer_router_, rpc_, apply_notifier_, config_);
  ASSERT_TRUE(resolver.ReplayForRecovery(cancel_).has_value());

  const auto* resolved = FindResolvedFor(appended_, 11);
  ASSERT_NE(resolved, nullptr);
  EXPECT_EQ(resolved->decision, core::Decision::kSkip);
  auto cached = resolver.Cache().GetKey("k");
  ASSERT_TRUE(cached.has_value());
  EXPECT_EQ(cached->abs_ttl_ms, 3'000'000U + 100'000U);
}

// HOTC-2 edge: once a SETEX key's TTL lapses (evaluated past abs_ttl_ms), a
// later `SET k NX` must APPLY because the cache treats it as expired/absent.
TEST_F(ResolverTest, SetexKeyExpiresInCacheAfterTtl) {
  StubQueueReadOnce({
      MakeWriteAt(10, {"SETEX", "k", "1", "v"}, At(4'000'000)),
      MakeConditionalAt(11, {"SET", "k", "v2", "NX"}, core::PredicateFlags::kNx, At(4'002'000)),
  });
  StubQueueAppendCapture();
  StubQueueAck();
  // After cache expiry the lookup falls through; cold reports absent.
  EXPECT_CALL(cold_, Exec(_, _)).WillRepeatedly(Return(core::RespValue::Integer(0)));

  Resolver resolver(queue_, cold_, buffer_router_, rpc_, apply_notifier_, config_);
  ASSERT_TRUE(resolver.ReplayForRecovery(cancel_).has_value());

  const auto* resolved = FindResolvedFor(appended_, 11);
  ASSERT_NE(resolved, nullptr);
  EXPECT_EQ(resolved->decision, core::Decision::kApply);
}

// XCONC-2: EXPIRE cache TTL must derive from the entry's appended_at, never the
// replay-time wall clock — so the cached absolute TTL is independent of when
// recovery runs.
TEST_F(ResolverTest, ExpireCacheTtlEqualsAppendedAtNotWallNow) {
  StubQueueReadOnce({
      MakeWriteAt(10, {"SET", "k", "v"}, At(5'000'000)),
      MakeWriteAt(11, {"EXPIRE", "k", "100"}, At(5'000'000)),
  });
  StubQueueAppendCapture();
  StubQueueAck();
  EXPECT_CALL(cold_, Exec(_, _)).Times(::testing::AnyNumber());

  Resolver resolver(queue_, cold_, buffer_router_, rpc_, apply_notifier_, config_);
  ASSERT_TRUE(resolver.ReplayForRecovery(cancel_).has_value());

  auto cached = resolver.Cache().GetKey("k");
  ASSERT_TRUE(cached.has_value());
  EXPECT_EQ(cached->abs_ttl_ms, 5'000'000U + 100'000U);
}

// XCONC-2 determinism: replaying the same slice twice (fresh Resolver each
// time, wall clock advancing between runs) must produce a byte-identical
// existence-cache decision for a follow-up EXPIRE NX conditional.
TEST_F(ResolverTest, ExpireReplayDeterministicAcrossRecovery) {
  auto run_once = [this]() -> ExistenceCache::KeyMeta {
    appended_.clear();
    next_appended_seq_ = 1000;
    StubQueueReadOnce({
        MakeWriteAt(10, {"SET", "k", "v"}, At(6'000'000)),
        MakeWriteAt(11, {"EXPIRE", "k", "100"}, At(6'000'500)),
        MakeConditionalAt(12, {"EXPIRE", "k", "50", "NX"}, core::PredicateFlags::kNx,
                          At(6'001'000)),
    });
    StubQueueAppendCapture();
    StubQueueAck();
    EXPECT_CALL(cold_, Exec(_, _)).Times(::testing::AnyNumber());
    Resolver resolver(queue_, cold_, buffer_router_, rpc_, apply_notifier_, config_);
    EXPECT_TRUE(resolver.ReplayForRecovery(cancel_).has_value());
    const auto* resolved = FindResolvedFor(appended_, 12);
    EXPECT_NE(resolved, nullptr);
    // EXPIRE NX on a key that already has a TTL must SKIP.
    EXPECT_EQ(resolved->decision, core::Decision::kSkip);
    return resolver.Cache().GetKey("k").value();
  };

  const auto first = run_once();
  const auto second = run_once();
  EXPECT_EQ(first.abs_ttl_ms, second.abs_ttl_ms);
  EXPECT_EQ(first.abs_ttl_ms, 6'000'500U + 100'000U);
}

// HOTC-9: every EXPIRE-family variant must reach the cache TTL update exactly
// once — a mis-copied disjunct in the branch would silently drop one variant.
TEST_F(ResolverTest, ExpireFamilyAllVariantsUpdateCache) {
  StubQueueReadOnce({
      MakeWriteAt(10, {"SET", "a", "v"}, At(10'000'000)),
      MakeWriteAt(11, {"SET", "b", "v"}, At(10'000'000)),
      MakeWriteAt(12, {"SET", "c", "v"}, At(10'000'000)),
      MakeWriteAt(13, {"SET", "d", "v"}, At(10'000'000)),
      MakeWriteAt(14, {"EXPIRE", "a", "100"}, At(10'000'000)),
      MakeWriteAt(15, {"PEXPIRE", "b", "200000"}, At(10'000'000)),
      MakeWriteAt(16, {"EXPIREAT", "c", "10300"}, At(10'000'000)),
      MakeWriteAt(17, {"PEXPIREAT", "d", "10400000"}, At(10'000'000)),
  });
  StubQueueAppendCapture();
  StubQueueAck();
  EXPECT_CALL(cold_, Exec(_, _)).Times(::testing::AnyNumber());

  Resolver resolver(queue_, cold_, buffer_router_, rpc_, apply_notifier_, config_);
  ASSERT_TRUE(resolver.ReplayForRecovery(cancel_).has_value());

  auto expect_ttl = [&resolver](std::string_view key, uint64_t want) {
    auto cached = resolver.Cache().GetKey(key);
    ASSERT_TRUE(cached.has_value()) << key;
    EXPECT_EQ(cached->abs_ttl_ms, want) << key;
  };
  expect_ttl("a", 10'100'000U);  // EXPIRE: appended_at + 100s
  expect_ttl("b", 10'200'000U);  // PEXPIRE: appended_at + 200000ms
  expect_ttl("c", 10'300'000U);  // EXPIREAT: absolute seconds
  expect_ttl("d", 10'400'000U);  // PEXPIREAT: absolute milliseconds
}

// HOTC-3: a HSET-cached field, then a plain HDEL, then HSETNX on the same field
// must APPLY (the stale field was invalidated). The HDEL field contains 0x1F so
// HOTC-4's length-prefixed encoding must address the right entry.
TEST_F(ResolverTest, HdelThenHsetnxAppliesOnRemovedField) {
  const std::string field = std::string("f\x1F") + "x";
  StubQueueReadOnce({
      MakeWriteAt(10, {"HSET", "h", field, "v"}, At(7'000'000)),
      MakeWriteAt(11, {"HDEL", "h", field}, At(7'000'100)),
      MakeConditionalAt(12, {"HSETNX", "h", field, "v2"}, core::PredicateFlags::kNx, At(7'000'200)),
  });
  StubQueueAppendCapture();
  StubQueueAck();
  // After RemoveField the field is absent in cache; cold reports field absent.
  EXPECT_CALL(cold_, Exec(_, _)).WillRepeatedly(Return(core::RespValue::Null()));

  Resolver resolver(queue_, cold_, buffer_router_, rpc_, apply_notifier_, config_);
  ASSERT_TRUE(resolver.ReplayForRecovery(cancel_).has_value());

  const auto* resolved = FindResolvedFor(appended_, 12);
  ASSERT_NE(resolved, nullptr);
  EXPECT_EQ(resolved->decision, core::Decision::kApply);
}

// HOTC-3: ZADD-cached member, then ZREM, then `ZADD z NX 2 m` must APPLY.
TEST_F(ResolverTest, ZremThenZaddNxAppliesOnRemovedMember) {
  StubQueueReadOnce({
      MakeWriteAt(10, {"ZADD", "z", "1", "m"}, At(8'000'000)),
      MakeWriteAt(11, {"ZREM", "z", "m"}, At(8'000'100)),
      MakeConditionalAt(12, {"ZADD", "z", "NX", "2", "m"}, core::PredicateFlags::kNx,
                        At(8'000'200)),
  });
  StubQueueAppendCapture();
  StubQueueAck();
  EXPECT_CALL(cold_, Exec(_, _)).WillRepeatedly(Return(core::RespValue::Null()));

  Resolver resolver(queue_, cold_, buffer_router_, rpc_, apply_notifier_, config_);
  ASSERT_TRUE(resolver.ReplayForRecovery(cancel_).has_value());

  const auto* resolved = FindResolvedFor(appended_, 12);
  ASSERT_NE(resolved, nullptr);
  EXPECT_EQ(resolved->decision, core::Decision::kApply);
}

// HOTC-3: DEL must purge the key's member/field index, so HSETNX after a
// DEL+recreate window applies on a previously-cached field.
TEST_F(ResolverTest, DelPurgesFieldsThenHsetnxApplies) {
  StubQueueReadOnce({
      MakeWriteAt(10, {"HSET", "h", "f", "v"}, At(9'000'000)),
      MakeWriteAt(11, {"DEL", "h"}, At(9'000'100)),
      MakeConditionalAt(12, {"HSETNX", "h", "f", "v2"}, core::PredicateFlags::kNx, At(9'000'200)),
  });
  StubQueueAppendCapture();
  StubQueueAck();
  EXPECT_CALL(cold_, Exec(_, _)).WillRepeatedly(Return(core::RespValue::Null()));

  Resolver resolver(queue_, cold_, buffer_router_, rpc_, apply_notifier_, config_);
  ASSERT_TRUE(resolver.ReplayForRecovery(cancel_).has_value());

  // kApply proves DEL purged the stale field; otherwise HSETNX would have seen
  // the cached field and SKIPPED.
  const auto* resolved = FindResolvedFor(appended_, 12);
  ASSERT_NE(resolved, nullptr);
  EXPECT_EQ(resolved->decision, core::Decision::kApply);
}

// HOTC-5: ReplayForRecovery re-decides a dangling Conditional and appends a
// fresh Resolved. The terminal recovery ack must NOT advance past the dangling
// until that re-emitted Resolved is durable — otherwise a crash after the
// offset fsync but before the Resolved fsync loses both, leaving the conditional
// permanently unresolved. While the re-emitted Resolved is non-durable the
// terminal Ack is refused (replay returns kUnavailable, offset clamped to the
// per-scan low-water); once AwaitDurable confirms, replay succeeds and acks past
// the dangling.
TEST_F(ResolverTest, RecoveryAckGatedOnReemittedResolvedDurability) {
  // A dangling Conditional at seq 10 with no matching Resolved. Re-served from
  // the persisted offset on each replay until the ack advances past it (the
  // real WAL behaviour: Read starts at ack_offset + 1). Within a single replay,
  // the resolver's highest_seen dedup stops re-processing.
  EXPECT_CALL(queue_, Read(core::kResolverConsumer, 0, _, _))
      .WillRepeatedly([this](core::ConsumerId, core::ShardId, size_t,
                             core::Duration) -> core::Result<std::vector<core::QueueEntry>> {
        if (acked_seq_.load() >= 10U) return std::vector<core::QueueEntry>{};
        return std::vector<core::QueueEntry>{
            MakeConditional(10, {"SETNX", "k", "v"}, core::PredicateFlags::kNx)};
      });
  StubQueueAppendCapture();  // re-emitted Resolved is appended at seq 1000.
  StubQueueAckCapture();
  StubControllableDurability();
  EXPECT_CALL(cold_, Exec(_, _)).WillRepeatedly(Return(core::RespValue::Integer(0)));

  // The per-scan low-water clamp acks behind the dangling (seq 9); allow that,
  // but the re-emitted Resolved (seq 1000) is NOT yet durable.
  durable_seq_.store(9);

  Resolver resolver(queue_, cold_, buffer_router_, rpc_, apply_notifier_, config_);
  auto r1 = resolver.ReplayForRecovery(cancel_);

  // Barrier refuses to advance the terminal ack past the dangling.
  ASSERT_FALSE(r1.has_value());
  EXPECT_EQ(r1.error().code(), core::ErrorCode::kUnavailable);
  EXPECT_LT(acked_seq_.load(), 10U) << "acked past dangling before re-emitted Resolved durable";
  EXPECT_GE(resolver.GetSnapshot().durable_wait_timeouts, 1U);
  // The re-decided Resolved WAS appended (durable in the WAL, just not fsynced).
  ASSERT_FALSE(AppendedSize() == 0);

  // Now the re-emitted Resolved is durable: a retry of the replay advances the
  // terminal ack past the dangling.
  durable_seq_.store(2000);
  auto r2 = resolver.ReplayForRecovery(cancel_);
  ASSERT_TRUE(r2.has_value()) << r2.error().message();
  EXPECT_GE(acked_seq_.load(), 10U) << "ack did not advance after re-emitted Resolved durable";
}

}  // namespace
}  // namespace abyss::consumer
