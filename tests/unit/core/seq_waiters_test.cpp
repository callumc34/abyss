#include "abyss/core/seq_waiters.h"

#include <gtest/gtest.h>

#include <array>
#include <atomic>
#include <barrier>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <iostream>
#include <mutex>
#include <optional>
#include <random>
#include <span>
#include <thread>
#include <vector>

#include "abyss/core/completion.h"
#include "abyss/core/result.h"
#include "abyss/core/types.h"

namespace abyss::core {
namespace {

using namespace std::chrono_literals;

class RecordingSink final : public CompletionSink {
 public:
  // NOLINTNEXTLINE(bugprone-exception-escape): an OOM may terminate.
  void Post(std::span<const CompletionEvent> events) noexcept override {
    const std::scoped_lock lock(mu_);
    posts_.emplace_back(events.begin(), events.end());
  }

  std::vector<std::vector<CompletionEvent>> Posts() const {
    const std::scoped_lock lock(mu_);
    return posts_;
  }

  std::vector<uint64_t> Slots() const {
    const std::scoped_lock lock(mu_);
    std::vector<uint64_t> slots;
    for (const auto& post : posts_) {
      for (const auto& event : post) slots.push_back(event.slot_seq);
    }
    return slots;
  }

 private:
  mutable std::mutex mu_;
  std::vector<std::vector<CompletionEvent>> posts_;
};

// Counts posts per slot, so a stress test can check exactly-once.
class CountingSink final : public CompletionSink {
 public:
  explicit CountingSink(size_t slots) : counts_(slots) {}

  // NOLINTNEXTLINE(bugprone-exception-escape): an OOM may terminate.
  void Post(std::span<const CompletionEvent> events) noexcept override {
    for (const auto& event : events) {
      if (!event.result.has_value() || event.kind != CompletionKind::kDurable) bad_.store(true);
      counts_.at(event.slot_seq).fetch_add(1);
    }
  }

  uint32_t count(size_t slot) const { return counts_.at(slot).load(); }
  bool bad() const { return bad_.load(); }

 private:
  std::vector<std::atomic<uint32_t>> counts_;
  std::atomic<bool> bad_{false};
};

// Its Post blocks until opened, holding the notifier between its
// unlock and any posts after this one.
class GateSink final : public CompletionSink {
 public:
  void Post(std::span<const CompletionEvent> /*events*/) noexcept override {
    std::unique_lock lock(mu_);
    entered_ = true;
    cv_.notify_all();
    cv_.wait(lock, [this] { return open_; });
  }

  bool AwaitEntered(std::chrono::milliseconds limit) {
    std::unique_lock lock(mu_);
    return cv_.wait_for(lock, limit, [this] { return entered_; });
  }

  void Open() {
    const std::scoped_lock lock(mu_);
    open_ = true;
    cv_.notify_all();
  }

 private:
  std::mutex mu_;
  std::condition_variable cv_;
  bool entered_ = false;
  bool open_ = false;
};

void Publish(std::atomic<SequenceId>& end, SeqWaiters& waiters, SequenceId new_end) {
  end.store(new_end, std::memory_order_seq_cst);
  waiters.Advance(new_end);
}

template <typename Pred>
bool Eventually(Pred pred, std::chrono::milliseconds limit = 10s) {
  const auto deadline = SteadyClock::now() + limit;
  while (!pred()) {
    if (SteadyClock::now() > deadline) return false;
    std::this_thread::sleep_for(1ms);
  }
  return true;
}

Error Stopped() { return {ErrorCode::kUnavailable, "WAL group committer stopped"}; }

TEST(SeqWaitersTest, AdvancePostsWaitersBelowTheEndInSeqOrder) {
  SeqWaiters waiters;
  std::atomic<SequenceId> end{0};
  RecordingSink sink;
  const std::array<SequenceId, 5> seqs{5, 3, 9, 3, 7};
  std::deque<SeqWaiterNode> nodes;
  for (size_t i = 0; i < seqs.size(); ++i) {
    nodes.emplace_back(seqs.at(i), Completion{.sink = &sink, .client_id = 42, .slot_seq = i});
    EXPECT_FALSE(waiters.Register(nodes.back(), end).has_value());
  }

  Publish(end, waiters, 4);
  EXPECT_EQ(sink.Slots(), (std::vector<uint64_t>{1, 3}));
  Publish(end, waiters, 8);
  EXPECT_EQ(sink.Slots(), (std::vector<uint64_t>{1, 3, 0, 4}));
  Publish(end, waiters, 10);
  EXPECT_EQ(sink.Slots(), (std::vector<uint64_t>{1, 3, 0, 4, 2}));
  EXPECT_EQ(waiters.size(), 0U);

  for (const auto& post : sink.Posts()) {
    for (const auto& event : post) {
      EXPECT_EQ(event.client_id, 42U);
      EXPECT_EQ(event.kind, CompletionKind::kDurable);
      EXPECT_TRUE(event.result.has_value());
    }
  }
}

// A notifier stored the end before the node was counted, so it saw no
// waiters and skipped Advance. The re-check after linking catches it.
TEST(SeqWaitersTest, EndPassedBeforeTheNodeWasCountedCompletesAtRegister) {
  SeqWaiters waiters;
  std::atomic<SequenceId> end{0};
  RecordingSink sink;
  SeqWaiterNode node(5, Completion{.sink = &sink, .client_id = 1, .slot_seq = 1});
  end.store(6, std::memory_order_seq_cst);
  const auto ready = waiters.Register(node, end);
  ASSERT_TRUE(ready.has_value() && ready->has_value());
  EXPECT_EQ(waiters.size(), 0U);
  Publish(end, waiters, 100);
  EXPECT_TRUE(sink.Posts().empty());
}

TEST(SeqWaitersTest, InOrderRegistrationInsertsAtTheTailInConstantTime) {
  SeqWaiters waiters;
  std::atomic<SequenceId> end{0};
  RecordingSink sink;
  std::deque<SeqWaiterNode> nodes;
  for (SequenceId seq = 0; seq < 1000; ++seq) {
    nodes.emplace_back(seq, Completion{.sink = &sink, .client_id = 1, .slot_seq = seq});
    ASSERT_FALSE(waiters.Register(nodes.back(), end).has_value());
  }
  EXPECT_EQ(waiters.StatsForTesting().insert_steps, 0U);

  // Out of order: walks back past the 499 larger seqs.
  nodes.emplace_back(500, Completion{.sink = &sink, .client_id = 1, .slot_seq = 1000});
  ASSERT_FALSE(waiters.Register(nodes.back(), end).has_value());
  EXPECT_EQ(waiters.StatsForTesting().insert_steps, 499U);

  Publish(end, waiters, 501);
  const auto slots = sink.Slots();
  ASSERT_EQ(slots.size(), 502U);
  EXPECT_EQ(slots.at(500), 500U);
  EXPECT_EQ(slots.at(501), 1000U);
  Publish(end, waiters, 1000);
  EXPECT_EQ(waiters.size(), 0U);
}

TEST(SeqWaitersTest, EmptyAdvanceTakesNoLock) {
  SeqWaiters waiters;
  for (SequenceId seq = 1; seq <= 1000; ++seq) waiters.Advance(seq);
  EXPECT_EQ(waiters.StatsForTesting().locked_pops, 0U);

  std::atomic<SequenceId> end{0};
  RecordingSink sink;
  SeqWaiterNode node(5000, Completion{.sink = &sink, .client_id = 1, .slot_seq = 1});
  ASSERT_FALSE(waiters.Register(node, end).has_value());
  Publish(end, waiters, 10);
  EXPECT_EQ(waiters.StatsForTesting().locked_pops, 1U);
  EXPECT_TRUE(sink.Posts().empty());
  EXPECT_TRUE(waiters.Cancel(node));
  EXPECT_FALSE(waiters.Cancel(node));
}

TEST(SeqWaitersTest, AdvanceBatchesOnePostPerSink) {
  SeqWaiters waiters;
  std::atomic<SequenceId> end{0};
  std::array<RecordingSink, 3> sinks;
  std::deque<SeqWaiterNode> nodes;
  constexpr SequenceId kNodes = 30;
  for (SequenceId seq = 0; seq < kNodes; ++seq) {
    nodes.emplace_back(seq,
                       Completion{.sink = &sinks.at(seq % 3), .client_id = 1, .slot_seq = seq});
    ASSERT_FALSE(waiters.Register(nodes.back(), end).has_value());
  }
  Publish(end, waiters, kNodes);
  for (size_t s = 0; s < sinks.size(); ++s) {
    const auto posts = sinks.at(s).Posts();
    ASSERT_EQ(posts.size(), 1U);
    ASSERT_EQ(posts.front().size(), kNodes / 3);
    for (size_t i = 0; i < posts.front().size(); ++i) {
      EXPECT_EQ(posts.front().at(i).slot_seq, s + (3 * i));
    }
  }
}

TEST(SeqWaitersTest, FailAllPostsTheErrorNowAndToEveryLaterRegister) {
  SeqWaiters waiters;
  std::atomic<SequenceId> end{0};
  RecordingSink sink;
  std::deque<SeqWaiterNode> nodes;
  for (SequenceId seq = 1; seq <= 3; ++seq) {
    nodes.emplace_back(seq, Completion{.sink = &sink, .client_id = 1, .slot_seq = seq});
    ASSERT_FALSE(waiters.Register(nodes.back(), end).has_value());
  }

  Result<void> blocked;
  std::thread waiter([&] { blocked = waiters.AwaitEnd(end, 50, 10s); });
  ASSERT_TRUE(Eventually([&] { return waiters.size() == 4; }));
  waiters.FailAll(Stopped());
  waiter.join();

  ASSERT_FALSE(blocked.has_value());
  EXPECT_EQ(blocked.error().code(), ErrorCode::kUnavailable);
  EXPECT_EQ(waiters.size(), 0U);
  const auto posts = sink.Posts();
  ASSERT_EQ(posts.size(), 1U);
  ASSERT_EQ(posts.front().size(), 3U);
  for (const auto& event : posts.front()) {
    ASSERT_FALSE(event.result.has_value());
    EXPECT_EQ(event.result.error().code(), ErrorCode::kUnavailable);
    EXPECT_EQ(event.result.error().message(), Stopped().message());
  }

  SeqWaiterNode late(10, Completion{.sink = &sink, .client_id = 1, .slot_seq = 10});
  const auto refused = waiters.Register(late, end);
  ASSERT_TRUE(refused.has_value() && !refused->has_value());
  // NOLINTNEXTLINE(bugprone-unchecked-optional-access): ASSERT_TRUE above guards.
  EXPECT_EQ(refused->error().code(), ErrorCode::kUnavailable);
  const auto awaited = waiters.AwaitEnd(end, 10, 10s);
  ASSERT_FALSE(awaited.has_value());
  EXPECT_EQ(awaited.error().code(), ErrorCode::kUnavailable);

  // A seq already past the end is still simply durable.
  Publish(end, waiters, 20);
  EXPECT_TRUE(waiters.AwaitEnd(end, 10, 10s).has_value());
}

TEST(SeqWaitersTest, AwaitEndReturnsOnceTheEndPasses) {
  SeqWaiters waiters;
  std::atomic<SequenceId> end{0};
  Result<void> result = std::unexpected(Error{ErrorCode::kInternal, "unset"});
  std::thread waiter([&] { result = waiters.AwaitEnd(end, 7, 10s); });
  ASSERT_TRUE(Eventually([&] { return waiters.size() == 1; }));
  Publish(end, waiters, 7);
  EXPECT_EQ(waiters.size(), 1U);
  Publish(end, waiters, 8);
  waiter.join();
  EXPECT_TRUE(result.has_value());
  EXPECT_EQ(waiters.size(), 0U);
}

// Every node completes exactly once: by Register, by a true Cancel, or
// by a notifier's post, never two of these.
TEST(SeqWaitersTest, CancelRacingAdvanceCompletesEachNodeExactlyOnce) {
  constexpr size_t kThreads = 8;
  constexpr size_t kIterations = 2000;
  enum class Outcome : uint8_t { kRegistered, kCancelled, kPosted };

  SeqWaiters waiters;
  std::atomic<SequenceId> end{0};
  std::atomic<bool> stop{false};
  std::deque<CountingSink> sinks;
  for (size_t t = 0; t < kThreads; ++t) sinks.emplace_back(kIterations);
  std::vector<std::vector<Outcome>> outcomes(kThreads, std::vector<Outcome>(kIterations));

  std::vector<std::thread> notifiers;
  notifiers.reserve(2);
  for (int n = 0; n < 2; ++n) {
    notifiers.emplace_back([&] {
      while (!stop.load()) {
        waiters.Advance(end.fetch_add(1, std::memory_order_seq_cst) + 1);
        std::this_thread::yield();
      }
    });
  }
  std::vector<std::thread> threads;
  threads.reserve(kThreads);
  for (size_t t = 0; t < kThreads; ++t) {
    threads.emplace_back([&, t] {
      std::minstd_rand rng(static_cast<uint32_t>(t) + 1);
      for (size_t i = 0; i < kIterations; ++i) {
        // Freed as soon as Cancel returns, posted or not.
        SeqWaiterNode node(end.load() + (rng() % 4),
                           Completion{.sink = &sinks.at(t), .client_id = t, .slot_seq = i});
        if (const auto ready = waiters.Register(node, end)) {
          EXPECT_TRUE(ready->has_value());
          outcomes.at(t).at(i) = Outcome::kRegistered;
          continue;
        }
        if (rng() % 2 == 0) std::this_thread::yield();
        outcomes.at(t).at(i) = waiters.Cancel(node) ? Outcome::kCancelled : Outcome::kPosted;
      }
    });
  }
  for (auto& thread : threads) thread.join();
  stop.store(true);
  for (auto& thread : notifiers) thread.join();

  std::array<size_t, 3> tally{};
  for (size_t t = 0; t < kThreads; ++t) {
    EXPECT_FALSE(sinks.at(t).bad());
    for (size_t i = 0; i < kIterations; ++i) {
      const Outcome outcome = outcomes.at(t).at(i);
      ++tally.at(static_cast<size_t>(outcome));
      ASSERT_EQ(sinks.at(t).count(i), outcome == Outcome::kPosted ? 1U : 0U)
          << "thread " << t << " iteration " << i;
    }
  }
  EXPECT_EQ(waiters.size(), 0U);
  std::cout << "registered " << tally.at(0) << " cancelled " << tally.at(1) << " posted "
            << tally.at(2) << '\n';
}

// A publish racing each registration never strands it.
TEST(SeqWaitersTest, PublishRacingRegistrationNeverLosesAWake) {
  constexpr SequenceId kRounds = 5000;
  SeqWaiters waiters;
  std::atomic<SequenceId> end{0};
  std::atomic<bool> lost{false};
  std::barrier sync(2);
  std::thread notifier([&] {
    std::minstd_rand rng(11);  // NOLINT(bugprone-random-generator-seed)
    for (SequenceId round = 0; round < kRounds; ++round) {
      sync.arrive_and_wait();
      if (lost.load()) return;
      for (uint32_t spin = rng() % 3; spin > 0; --spin) std::this_thread::yield();
      Publish(end, waiters, round + 1);
    }
  });
  for (SequenceId round = 0; round < kRounds; ++round) {
    sync.arrive_and_wait();
    if (lost.load()) break;
    if (!waiters.AwaitEnd(end, round, 2s).has_value()) {
      ADD_FAILURE() << "round " << round << " lost its wake";
      lost.store(true);
    }
  }
  notifier.join();
  // Unlinked when it timed out.
  EXPECT_EQ(waiters.size(), 0U);
}

// A timed-out wait whose Cancel lost must wait for the post, or the
// notifier would post into a destroyed stack sink.
TEST(SeqWaitersTest, BlockingWaitWhoseCancelLostWaitsForThePost) {
  SeqWaiters waiters;
  std::atomic<SequenceId> end{0};
  GateSink gate;
  SeqWaiterNode first(1, Completion{.sink = &gate, .client_id = 1, .slot_seq = 1});
  ASSERT_FALSE(waiters.Register(first, end).has_value());

  std::atomic<bool> returned{false};
  Result<void> result = std::unexpected(Error{ErrorCode::kInternal, "unset"});
  std::thread blocked([&] {
    result = waiters.AwaitEnd(end, 2, 50ms);
    returned.store(true);
  });
  ASSERT_TRUE(Eventually([&] { return waiters.size() == 2; }));
  // Posts the gate first, so the blocked wait is unlinked but unposted.
  std::thread notifier([&] { Publish(end, waiters, 3); });
  ASSERT_TRUE(gate.AwaitEntered(10s));
  EXPECT_EQ(waiters.size(), 0U);

  std::this_thread::sleep_for(200ms);
  EXPECT_FALSE(returned.load());
  gate.Open();
  notifier.join();
  blocked.join();
  EXPECT_TRUE(result.has_value());
}

// For the sanitizers: a notifier races each timing-out blocking wait,
// so a sink freed before its post shows as a use after scope.
TEST(SeqWaitersTest, NotifierRacingTimingOutWaitsNeverOutlivesTheSink) {
  constexpr SequenceId kRounds = 1500;
  SeqWaiters waiters;
  std::atomic<SequenceId> end{0};
  std::barrier sync(2);
  std::thread notifier([&] {
    std::minstd_rand rng(7);  // NOLINT(bugprone-random-generator-seed)
    for (SequenceId round = 0; round < kRounds; ++round) {
      sync.arrive_and_wait();
      std::this_thread::sleep_for(std::chrono::microseconds(rng() % 1500));
      Publish(end, waiters, round + 1);
    }
  });
  for (SequenceId round = 0; round < kRounds; ++round) {
    sync.arrive_and_wait();
    const auto result = waiters.AwaitEnd(end, round, Duration(round % 2));
    if (!result.has_value()) {
      EXPECT_EQ(result.error().code(), ErrorCode::kTimeout);
    }
    EXPECT_EQ(waiters.size(), 0U);
  }
  notifier.join();
}

// A retry loop of timed-out waits leaves nothing behind.
TEST(SeqWaitersTest, RetriedTimeoutsLeaveTheListEmpty) {
  SeqWaiters waiters;
  const std::atomic<SequenceId> end{0};
  for (int i = 0; i < 200; ++i) {
    const auto result = waiters.AwaitEnd(end, 5, 5ms);
    ASSERT_FALSE(result.has_value());
    ASSERT_EQ(result.error().code(), ErrorCode::kTimeout);
    ASSERT_EQ(waiters.size(), 0U);
  }
  EXPECT_EQ(waiters.size(), 0U);
}

}  // namespace
}  // namespace abyss::core
