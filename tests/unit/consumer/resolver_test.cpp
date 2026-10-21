#include "abyss/consumer/resolver.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <deque>
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
#include "fatal_capture.h"
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
    EXPECT_CALL(queue_, Read(0, _, _, _, _))
        .WillRepeatedly(
            [remaining](core::ShardId, core::SequenceId, size_t, core::Duration,
                        core::Durability) -> core::Result<std::vector<core::QueueEntry>> {
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
    EXPECT_CALL(queue_, Append(_, _, _))
        .WillRepeatedly([this](core::ShardId /*shard*/, core::QueueEntry entry,
                               core::SteadyTime) -> core::Result<queue::AppendResult> {
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

  void StubQueueCommit() {
    EXPECT_CALL(queue_, CommitOffset(_, _, _)).WillRepeatedly(Return(core::Result<void>{}));
  }

  // Captures the highest committed seq for kResolverConsumer and serves it
  // back from CommittedOffset. Fail-closed: a commit past durable_seq_
  // returns kFailedPrecondition (mirrors the real WAL gate), so a clamp bug
  // surfaces as a rejected commit rather than silent success.
  void StubQueueCommitCapture() {
    EXPECT_CALL(queue_, CommitOffset(core::kResolverConsumer, 0, _))
        .WillRepeatedly(
            [this](core::ConsumerId, core::ShardId, core::SequenceId seq) -> core::Result<void> {
              if (seq > durable_seq_.load()) {
                return std::unexpected(core::Error(core::ErrorCode::kFailedPrecondition,
                                                   "commit past durable (test)"));
              }
              auto cur = committed_seq_.load();
              while (seq > cur && !committed_seq_.compare_exchange_weak(cur, seq)) {
              }
              commit_called_.store(true);
              return core::Result<void>{};
            });
    EXPECT_CALL(queue_, CommittedOffset(core::kResolverConsumer, 0))
        .WillRepeatedly([this](core::ConsumerId,
                               core::ShardId) -> core::Result<std::optional<core::SequenceId>> {
          if (!commit_called_.load()) return std::nullopt;
          return committed_seq_.load();
        });
  }

  // Drives DurableEnd/AwaitDurable off the test-controlled durable_seq_
  // (the highest durable seq) so a test can hold a Resolved non-durable
  // then release it.
  void StubControllableDurability() {
    EXPECT_CALL(queue_, DurableEnd(0, _))
        .WillRepeatedly([this](core::ShardId, core::Durability) -> core::Result<core::SequenceId> {
          const auto highest = durable_seq_.load();
          return highest == std::numeric_limits<core::SequenceId>::max() ? highest : highest + 1;
        });
    EXPECT_CALL(queue_, AwaitDurable(0, _, _, _))
        .WillRepeatedly(
            [this](core::ShardId, core::SequenceId seq, core::Durability,
                   core::Duration) -> core::Result<bool> { return durable_seq_.load() >= seq; });
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
  std::atomic<core::SequenceId> committed_seq_{0};
  std::atomic<bool> commit_called_{false};
  // Tests never cancel — pass to ReplayForRecovery to satisfy the API.
  std::atomic<bool> cancel_{false};
  // NOLINTEND(cppcoreguidelines-non-private-member-variables-in-classes)
};

TEST_F(ResolverTest, SetnxOnAbsentKeyDecidesApply) {
  StubQueueReadOnce({MakeConditional(10, {"SETNX", "k", "v"}, core::PredicateFlags::kNx)});
  StubQueueAppendCapture();
  StubQueueCommit();
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
  StubQueueCommit();
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
  StubQueueCommit();
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
  StubQueueCommit();
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
  StubQueueCommit();
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
    EXPECT_CALL(queue_, Read(0, _, _, _, _))
        .WillRepeatedly(
            [remaining](core::ShardId, core::SequenceId, size_t, core::Duration,
                        core::Durability) -> core::Result<std::vector<core::QueueEntry>> {
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
  StubQueueCommit();
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
  StubQueueCommit();
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
  StubQueueCommit();
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
  StubQueueCommit();
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

// XDUR-2: the resolver's persisted retention commit must never outrun the durable
// tail. With the group committer "paused" (durable_seq_ held below the emitted
// Resolved's seq), the resolver appends the Resolved for Conditional 10 but its
// persisted commit stays clamped below 10 — a crash here would re-read and
// re-decide the Conditional. Once the Resolved becomes durable the commit advances
// to 10. Pre-fix the resolver committed the Conditional's seq on drain progress,
// losing the Resolved across a crash in the fsync-coalescing window.
TEST_F(ResolverRunTest, CommitClampedBehindNonDurableResolved) {
  config_.hot_apply_wait = std::chrono::milliseconds{30};
  config_.durable_wait_timeout = std::chrono::milliseconds{30};
  StubQueueReadRepeating({MakeConditional(10, {"SETNX", "k", "v"}, core::PredicateFlags::kNx)});
  StubQueueAppendCapture();  // Resolved is appended at seq 1000.
  StubQueueCommitCapture();
  StubControllableDurability();
  EXPECT_CALL(cold_, Exec(_, _)).WillRepeatedly(Return(core::RespValue::Integer(0)));

  // Nothing durable yet: the committer is "paused".
  durable_seq_.store(0);

  Resolver resolver(queue_, cold_, buffer_router_, rpc_, apply_notifier_, config_);
  resolver.Start();

  // Wait until the Resolved has been appended (the resolver has processed the
  // Conditional). The commit must remain clamped below the Conditional's seq.
  const auto deadline = std::chrono::steady_clock::now() + 3s;
  while (AppendedSize() == 0 && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(5ms);
  }
  ASSERT_EQ(AppendedSize(), 1U);
  // Give the Run loop several commit cycles to (incorrectly) advance, if it would.
  std::this_thread::sleep_for(100ms);
  EXPECT_LT(committed_seq_.load(), 10U)
      << "commit advanced past Conditional before Resolved durable";

  // Release durability for the Resolved (seq 1000).
  durable_seq_.store(2000);
  apply_notifier_.NotifyApplied(0, 2000);

  const auto commit_deadline = std::chrono::steady_clock::now() + 3s;
  while (committed_seq_.load() < 10U && std::chrono::steady_clock::now() < commit_deadline) {
    std::this_thread::sleep_for(5ms);
  }
  resolver.Stop();

  EXPECT_GE(committed_seq_.load(), 10U) << "commit did not advance after Resolved became durable";
  EXPECT_GE(resolver.GetSnapshot().resolver_durable_floor, 10U);
}

// The commit floor is pipelined: with the power-durable end stalled, the
// next conditional is still decided at once, and the commit catches up
// when the end moves. A per-batch durability wait would pace decisions.
TEST_F(ResolverRunTest, DecisionsNeverWaitForTheCommitFloor) {
  config_.hot_apply_wait = std::chrono::milliseconds{5};
  auto batches = std::make_shared<std::deque<std::vector<core::QueueEntry>>>();
  auto batches_mu = std::make_shared<std::mutex>();
  batches->push_back({MakeConditional(10, {"SETNX", "a", "v"}, core::PredicateFlags::kNx)});
  EXPECT_CALL(queue_, Read(0, _, _, _, _))
      .WillRepeatedly(
          [batches, batches_mu](core::ShardId, core::SequenceId, size_t, core::Duration,
                                core::Durability) -> core::Result<std::vector<core::QueueEntry>> {
            const std::scoped_lock lock(*batches_mu);
            if (batches->empty()) return std::vector<core::QueueEntry>{};
            auto out = std::move(batches->front());
            batches->pop_front();
            return out;
          });
  StubQueueAppendCapture();
  StubQueueCommitCapture();
  StubControllableDurability();
  EXPECT_CALL(queue_, AwaitDurable(0, _, core::Durability::kPowerLoss, _)).Times(0);
  EXPECT_CALL(cold_, Exec(_, _)).WillRepeatedly(Return(core::RespValue::Integer(0)));
  durable_seq_.store(0);

  Resolver resolver(queue_, cold_, buffer_router_, rpc_, apply_notifier_, config_);
  resolver.Start();
  const auto wait_for = [](const auto& done) {
    const auto deadline = std::chrono::steady_clock::now() + 3s;
    while (!done() && std::chrono::steady_clock::now() < deadline) {
      std::this_thread::sleep_for(1ms);
    }
    return done();
  };
  ASSERT_TRUE(wait_for([this] { return AppendedSize() == 1; }));
  {
    const std::scoped_lock lock(*batches_mu);
    batches->push_back({MakeConditional(11, {"SETNX", "b", "v"}, core::PredicateFlags::kNx)});
  }
  ASSERT_TRUE(wait_for([this] { return AppendedSize() == 2; }))
      << "the second conditional waited on the first batch's durability";
  EXPECT_LT(committed_seq_.load(), 10U) << "committed past a seq that is not power-durable";

  durable_seq_.store(5000);
  EXPECT_TRUE(wait_for([this] { return committed_seq_.load() >= 11U; }))
      << "commit did not catch up once the power-durable end moved";
  resolver.Stop();
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
  StubQueueCommit();
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
  StubQueueCommit();
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
  StubQueueCommit();
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
  // NOLINTNEXTLINE(bugprone-unchecked-optional-access): ASSERT_TRUE above guards.
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
  StubQueueCommit();
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
  StubQueueCommit();
  EXPECT_CALL(cold_, Exec(_, _)).Times(::testing::AnyNumber());

  Resolver resolver(queue_, cold_, buffer_router_, rpc_, apply_notifier_, config_);
  ASSERT_TRUE(resolver.ReplayForRecovery(cancel_).has_value());

  const auto* resolved = FindResolvedFor(appended_, 11);
  ASSERT_NE(resolved, nullptr);
  EXPECT_EQ(resolved->decision, core::Decision::kSkip);
  auto cached = resolver.Cache().GetKey("k");
  ASSERT_TRUE(cached.has_value());
  // NOLINTNEXTLINE(bugprone-unchecked-optional-access): ASSERT_TRUE above guards.
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
  StubQueueCommit();
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
  StubQueueCommit();
  EXPECT_CALL(cold_, Exec(_, _)).Times(::testing::AnyNumber());

  Resolver resolver(queue_, cold_, buffer_router_, rpc_, apply_notifier_, config_);
  ASSERT_TRUE(resolver.ReplayForRecovery(cancel_).has_value());

  auto cached = resolver.Cache().GetKey("k");
  ASSERT_TRUE(cached.has_value());
  // NOLINTNEXTLINE(bugprone-unchecked-optional-access): ASSERT_TRUE above guards.
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
    StubQueueCommit();
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
  StubQueueCommit();
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
  StubQueueCommit();
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
  StubQueueCommit();
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
  StubQueueCommit();
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
// fresh Resolved. The terminal recovery commit must NOT advance past the dangling
// until that re-emitted Resolved is durable — otherwise a crash after the
// offset fsync but before the Resolved fsync loses both, leaving the conditional
// permanently unresolved. While the re-emitted Resolved is non-durable the
// terminal commit is refused (replay returns kUnavailable, offset clamped
// to the per-scan low-water); once AwaitDurable confirms, a retried replay
// rescans from the committed offset, re-finds the dangling and commits.
TEST_F(ResolverTest, RecoveryCommitGatedOnReemittedResolvedDurability) {
  // A dangling Conditional at seq 10 with no matching Resolved.
  const std::vector<core::QueueEntry> log{
      MakeConditional(10, {"SETNX", "k", "v"}, core::PredicateFlags::kNx)};
  EXPECT_CALL(queue_, Read(0, _, _, _, _))
      .WillRepeatedly([&log](core::ShardId, core::SequenceId from, size_t max, core::Duration,
                             core::Durability) -> core::Result<std::vector<core::QueueEntry>> {
        return testing::ReadFromLog(log, from, max);
      });
  StubQueueAppendCapture();  // re-emitted Resolved is appended at seq 1000.
  StubQueueCommitCapture();
  StubControllableDurability();
  EXPECT_CALL(cold_, Exec(_, _)).WillRepeatedly(Return(core::RespValue::Integer(0)));

  // The per-scan low-water clamp commits behind the dangling (seq 9); allow that,
  // but the re-emitted Resolved (seq 1000) is NOT yet durable.
  durable_seq_.store(9);

  Resolver resolver(queue_, cold_, buffer_router_, rpc_, apply_notifier_, config_);
  auto r1 = resolver.ReplayForRecovery(cancel_);

  // Barrier refuses to advance the terminal commit past the dangling.
  ASSERT_FALSE(r1.has_value());
  EXPECT_EQ(r1.error().code(), core::ErrorCode::kUnavailable);
  EXPECT_LT(committed_seq_.load(), 10U)
      << "committed past dangling before re-emitted Resolved durable";
  EXPECT_GE(resolver.GetSnapshot().durable_wait_timeouts, 1U);
  // The re-decided Resolved WAS appended (durable in the WAL, just not fsynced).
  ASSERT_FALSE(AppendedSize() == 0);

  // Now the re-emitted Resolved is durable: a retry of the replay advances the
  // terminal commit past the dangling.
  durable_seq_.store(2000);
  auto r2 = resolver.ReplayForRecovery(cancel_);
  ASSERT_TRUE(r2.has_value()) << r2.error().message();
  EXPECT_GE(committed_seq_.load(), 10U)
      << "commit did not advance after re-emitted Resolved durable";
}

// A pre-flush Skip erases its dangling mid-scan, so the per-scan commit
// must not pass that Conditional before the Skip is durable.
TEST_F(ResolverTest, RecoveryScanCommitGatedOnPreFlushSkipDurability) {
  const std::vector<core::QueueEntry> log{
      MakeConditional(10, {"SETNX", "k", "v"}, core::PredicateFlags::kNx),
      core::QueueEntry{
          .seq = 11, .appended_at = core::WallClock::now(), .payload = core::entry::Flush{}},
  };
  EXPECT_CALL(queue_, Read(0, _, _, _, _))
      .WillRepeatedly([&log](core::ShardId, core::SequenceId from, size_t max, core::Duration,
                             core::Durability) -> core::Result<std::vector<core::QueueEntry>> {
        return testing::ReadFromLog(log, from, max);
      });
  StubQueueAppendCapture();  // the pre-flush Skip lands at seq 1000.
  StubQueueCommitCapture();
  StubControllableDurability();
  durable_seq_.store(11);  // the scanned log is durable; the Skip is not.

  Resolver resolver(queue_, cold_, buffer_router_, rpc_, apply_notifier_, config_);
  auto r1 = resolver.ReplayForRecovery(cancel_);
  ASSERT_FALSE(r1.has_value());
  EXPECT_EQ(r1.error().code(), core::ErrorCode::kUnavailable);
  EXPECT_FALSE(commit_called_.load()) << "committed past X before its pre-flush Skip was durable";
  EXPECT_EQ(resolver.GetSnapshot().flush_skip_resolveds_emitted, 1U);

  durable_seq_.store(2000);
  auto r2 = resolver.ReplayForRecovery(cancel_);
  ASSERT_TRUE(r2.has_value()) << r2.error().message();
  EXPECT_EQ(committed_seq_.load(), 11U);
}

TEST_F(ResolverTest, RejectedCommitCountsAsCommitFailure) {
  StubQueueReadOnce({MakeWriteAt(5, {"SET", "k", "v"}, core::WallClock::now())});
  StubQueueAppendCapture();
  EXPECT_CALL(queue_, CommitOffset(core::kResolverConsumer, 0, _))
      .WillRepeatedly(Return(core::Result<void>{
          std::unexpected(core::Error(core::ErrorCode::kFailedPrecondition, "rejected (test)"))}));

  Resolver resolver(queue_, cold_, buffer_router_, rpc_, apply_notifier_, config_);
  ASSERT_TRUE(resolver.ReplayForRecovery(cancel_).has_value());

  const auto snap = resolver.GetSnapshot();
  EXPECT_GE(snap.commit_failures, 1U);
  EXPECT_EQ(snap.append_failures, 0U);
  EXPECT_EQ(snap.last_commit_seq, 0U);
}

// A3: while the power-durable end lags, the commit does not advance. The
// steady-state loop must still read forward from its cursor and never
// re-decide the Conditional, or it appends a second Resolved for it
// (ADP-001 invariant 7).
TEST_F(ResolverRunTest, DurabilityTimeoutNeverRedecidesConditional) {
  config_.read_timeout = core::Duration{7};
  config_.hot_apply_wait = std::chrono::milliseconds{1};
  const std::vector<core::QueueEntry> log{
      MakeConditional(10, {"SETNX", "k", "v"}, core::PredicateFlags::kNx)};
  std::atomic<int> reads{0};
  EXPECT_CALL(queue_, Read(0, _, _, _, _))
      .WillRepeatedly(
          [&log, &reads](core::ShardId, core::SequenceId from, size_t max, core::Duration,
                         core::Durability) -> core::Result<std::vector<core::QueueEntry>> {
            reads.fetch_add(1);
            return testing::ReadFromLog(log, from, max);
          });
  // Nothing is power durable for the first few passes.
  std::atomic<bool> lagged{false};
  EXPECT_CALL(queue_, DurableEnd(0, core::Durability::kPowerLoss))
      .WillRepeatedly(
          [&reads, &lagged](core::ShardId, core::Durability) -> core::Result<core::SequenceId> {
            if (reads.load() < 5) {
              lagged.store(true);
              return core::SequenceId{0};
            }
            return std::numeric_limits<core::SequenceId>::max();
          });
  StubQueueAppendCapture();
  StubQueueCommitCapture();
  EXPECT_CALL(cold_, Exec(_, _)).WillRepeatedly(Return(core::RespValue::Integer(0)));

  Resolver resolver(queue_, cold_, buffer_router_, rpc_, apply_notifier_, config_);
  resolver.Start();
  const auto deadline = std::chrono::steady_clock::now() + 3s;
  while ((reads.load() < 10 || committed_seq_.load() < 10U) &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(2ms);
  }
  resolver.Stop();

  EXPECT_TRUE(lagged.load()) << "the power-durable end never lagged the drained position";
  size_t resolveds_for_ref = 0;
  for (const auto& e : AppendedSnapshot()) {
    if (const auto* r = std::get_if<core::entry::Resolved>(&e.payload); r && r->ref == 10) {
      ++resolveds_for_ref;
    }
  }
  EXPECT_EQ(resolveds_for_ref, 1U) << "the Conditional was decided more than once";
  EXPECT_EQ(committed_seq_.load(), 10U) << "the commit never advanced once durable";
}

// A failed Resolved append holds the cursor at X: X is retried and
// decided once, and the entry after X is processed only after it.
TEST_F(ResolverRunTest, FailedResolvedAppendRetriesConditionalInOrder) {
  const std::vector<core::QueueEntry> log{
      MakeConditional(10, {"SETNX", "k", "v"}, core::PredicateFlags::kNx),
      MakeConditional(11, {"SETNX", "k2", "v"}, core::PredicateFlags::kNx),
  };
  const auto x_resolved = [this] { return FindResolvedFor(AppendedSnapshot(), 10) != nullptr; };
  std::atomic<bool> read_past_x{false};
  EXPECT_CALL(queue_, Read(0, _, _, _, _))
      .WillRepeatedly([&](core::ShardId, core::SequenceId from, size_t max, core::Duration,
                          core::Durability) -> core::Result<std::vector<core::QueueEntry>> {
        if (from > 10 && !x_resolved()) read_past_x.store(true);
        return testing::ReadFromLog(log, from, max);
      });
  std::atomic<bool> committed_past_x{false};
  EXPECT_CALL(queue_, CommitOffset(core::kResolverConsumer, 0, _))
      .WillRepeatedly([&](core::ConsumerId, core::ShardId, core::SequenceId seq) {
        if (seq >= 10 && !x_resolved()) committed_past_x.store(true);
        return core::Result<void>{};
      });
  std::atomic<int> append_calls{0};
  EXPECT_CALL(queue_, Append(_, _, _))
      .WillRepeatedly([this, &append_calls](core::ShardId, core::QueueEntry entry,
                                            core::SteadyTime) -> core::Result<queue::AppendResult> {
        if (append_calls.fetch_add(1) == 0) {
          return std::unexpected(core::Error(core::ErrorCode::kInternal, "append failed (test)"));
        }
        std::promise<core::Result<void>> p;
        p.set_value(core::Result<void>{});
        const std::scoped_lock lock(appended_mu_);
        entry.seq = next_appended_seq_++;
        appended_.push_back(entry);
        return queue::AppendResult{.seq = entry.seq, .durable = p.get_future()};
      });
  EXPECT_CALL(cold_, Exec(_, _)).WillRepeatedly(Return(core::RespValue::Integer(0)));
  auto x_client = rpc_.Register(core::MakeRpcId(0, 10));
  apply_notifier_.NotifyApplied(0, 2000);

  Resolver resolver(queue_, cold_, buffer_router_, rpc_, apply_notifier_, config_);
  resolver.Start();
  const auto deadline = std::chrono::steady_clock::now() + 3s;
  while (AppendedSize() < 2 && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(2ms);
  }
  resolver.Stop();

  const auto appended = AppendedSnapshot();
  ASSERT_EQ(appended.size(), 2U);
  EXPECT_EQ(std::get<core::entry::Resolved>(appended[0].payload).ref, 10U);
  EXPECT_EQ(std::get<core::entry::Resolved>(appended[1].payload).ref, 11U);
  EXPECT_EQ(append_calls.load(), 3);
  EXPECT_FALSE(read_past_x.load()) << "the cursor passed X while it was unresolved";
  EXPECT_FALSE(committed_past_x.load()) << "committed past X while it was unresolved";
  const auto snap = resolver.GetSnapshot();
  EXPECT_EQ(snap.append_failures, 1U);
  EXPECT_EQ(snap.conditionals_resolved, 2U);
  // The client sees the decision the retry made, never a failure: a
  // SETNX lock reported as failed would leak once the retry applies.
  ASSERT_EQ(x_client.wait_for(0ms), std::future_status::ready);
  const auto reply = x_client.get();
  ASSERT_FALSE(reply.IsError()) << "the client was told an applied conditional failed";
  EXPECT_EQ(reply.AsInteger(), 1);
}

// A persistent append failure backs off (no busy spin) and the backoff
// wakes promptly on Stop.
TEST_F(ResolverRunTest, PersistentAppendFailureBacksOffAndStopsPromptly) {
  const std::vector<core::QueueEntry> log{
      MakeConditional(10, {"SETNX", "k", "v"}, core::PredicateFlags::kNx)};
  EXPECT_CALL(queue_, Read(0, _, _, _, _))
      .WillRepeatedly([&log](core::ShardId, core::SequenceId from, size_t max, core::Duration,
                             core::Durability) -> core::Result<std::vector<core::QueueEntry>> {
        return testing::ReadFromLog(log, from, max);
      });
  std::atomic<int> append_calls{0};
  EXPECT_CALL(queue_, Append(_, _, _))
      .WillRepeatedly([&append_calls](core::ShardId, const core::QueueEntry&,
                                      core::SteadyTime) -> core::Result<queue::AppendResult> {
        append_calls.fetch_add(1);
        return std::unexpected(core::Error(core::ErrorCode::kInternal, "append failed (test)"));
      });
  StubQueueCommitCapture();
  EXPECT_CALL(cold_, Exec(_, _)).WillRepeatedly(Return(core::RespValue::Integer(0)));

  Resolver resolver(queue_, cold_, buffer_router_, rpc_, apply_notifier_, config_);
  const auto start = std::chrono::steady_clock::now();
  resolver.Start();
  // Backoffs of 1+2+...+256 ms precede the 10th attempt, which then
  // waits ~512 ms more.
  const auto deadline = start + 5s;
  while (append_calls.load() < 10 && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(2ms);
  }
  const auto stop_start = std::chrono::steady_clock::now();
  resolver.Stop();
  const auto stop_took = std::chrono::steady_clock::now() - stop_start;

  ASSERT_GE(append_calls.load(), 10);
  EXPECT_GE(stop_start - start, 500ms) << "append retries busy-spun instead of backing off";
  EXPECT_LT(stop_took, 250ms) << "Stop waited out the append-retry backoff";
  EXPECT_LT(committed_seq_.load(), 10U) << "committed past the unresolved Conditional";
  EXPECT_TRUE(AppendedSnapshot().empty());
}

TEST_F(ResolverTest, ReadBelowFirstRetainedSeqIsFatal) {
  const testing::ScopedFatalCapture capture;
  EXPECT_CALL(queue_, CommittedOffset(core::kResolverConsumer, 0))
      .WillRepeatedly(Return(core::Result<std::optional<core::SequenceId>>(6)));
  EXPECT_CALL(queue_, Read(0, 7, _, _, _))
      .WillRepeatedly(Return(core::Result<std::vector<core::QueueEntry>>(
          std::unexpected(core::Error{core::ErrorCode::kOutOfRange, "below first retained seq"}))));
  EXPECT_CALL(queue_, FirstSeq(0)).WillRepeatedly(Return(core::Result<core::SequenceId>(50)));

  Resolver resolver(queue_, cold_, buffer_router_, rpc_, apply_notifier_, config_);
  try {
    const auto replayed = resolver.ReplayForRecovery(cancel_);
    FAIL() << "a read below the first retained seq must be fatal; replay returned "
           << (replayed.has_value() ? "ok" : replayed.error().message());
  } catch (const testing::FatalCalled& fatal) {
    EXPECT_NE(fatal.reason.find("resolver"), std::string::npos) << fatal.reason;
    EXPECT_NE(fatal.reason.find("shard 0"), std::string::npos) << fatal.reason;
    EXPECT_NE(fatal.reason.find("read seq 7"), std::string::npos) << fatal.reason;
    EXPECT_NE(fatal.reason.find("first retained seq 50"), std::string::npos) << fatal.reason;
  }
}

}  // namespace
}  // namespace abyss::consumer
