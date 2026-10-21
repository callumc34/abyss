#include "abyss/core/wait_list.h"

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
#include <thread>
#include <vector>

#include "abyss/core/result.h"
#include "abyss/core/types.h"

namespace abyss::core {
namespace {

using namespace std::chrono_literals;

// Woken on the notifying thread; appends its id to a shared log.
class LoggingWaiter final : public Waiter {
 public:
  LoggingWaiter(size_t id, std::vector<size_t>* log) : id_(id), log_(log) {}

  // NOLINTNEXTLINE(bugprone-exception-escape): an OOM may terminate.
  void Wake(Result<void> result) noexcept override {
    if (result.has_value()) {
      ++ok_wakes_;
    } else {
      last_error_ = result.error().code();
    }
    log_->push_back(id_);
  }

  int ok_wakes() const { return ok_wakes_; }
  std::optional<ErrorCode> last_error() const { return last_error_; }

 private:
  size_t id_;
  std::vector<size_t>* log_;
  int ok_wakes_ = 0;
  std::optional<ErrorCode> last_error_;
};

// Its owner gives up on the wake at once, inside it.
class FinishingWaiter final : public Waiter {
 public:
  explicit FinishingWaiter(WaitList* list) : list_(list) {}

  void Wake(Result<void> result) noexcept override {
    ok_ = result.has_value();
    list_->Finish(*this);
  }

  bool ok() const { return ok_; }

 private:
  WaitList* list_;
  bool ok_ = false;
};

// Counts wakes per slot. Wake's last act is the count, after which the
// owner may free the waiter.
class CountingWaiter final : public Waiter {
 public:
  CountingWaiter(std::vector<std::atomic<uint32_t>>* counts, size_t slot, std::atomic<bool>* bad)
      : counts_(counts), slot_(slot), bad_(bad) {}

  // NOLINTNEXTLINE(bugprone-exception-escape): an OOM may terminate.
  void Wake(Result<void> result) noexcept override {
    if (!result.has_value()) bad_->store(true);
    counts_->at(slot_).fetch_add(1, std::memory_order_release);
  }

 private:
  std::vector<std::atomic<uint32_t>>* counts_;
  size_t slot_;
  std::atomic<bool>* bad_;
};

// Its Wake blocks until opened, holding the notifier before the wakes
// after this one.
class GateWaiter final : public Waiter {
 public:
  void Wake(Result<void> /*result*/) noexcept override {
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

template <typename Pred>
bool Eventually(Pred pred, std::chrono::milliseconds limit = 10s) {
  const auto deadline = SteadyClock::now() + limit;
  while (!pred()) {
    if (SteadyClock::now() > deadline) return false;
    std::this_thread::sleep_for(1ms);
  }
  return true;
}

Error Stopped() { return {ErrorCode::kUnavailable, "durability window stopped"}; }

TEST(WaitListTest, NotifyWakesABatchInFifoOrder) {
  WaitList list;
  std::vector<size_t> log;
  std::deque<LoggingWaiter> waiters;
  const auto never = [] { return false; };
  for (size_t id = 0; id < 70; ++id) {
    waiters.emplace_back(id, &log);
    ASSERT_FALSE(list.Register(waiters.back(), never).has_value());
  }

  list.Notify();
  ASSERT_EQ(log.size(), WaitList::kWakeBatch);
  for (size_t i = 0; i < log.size(); ++i) EXPECT_EQ(log.at(i), i);
  EXPECT_EQ(list.size(), 70 - WaitList::kWakeBatch);

  // Waiter 0 re-blocks and goes back ahead of 64..69; 70 is new.
  ASSERT_FALSE(list.Register(waiters.at(0), never).has_value());
  waiters.emplace_back(70, &log);
  ASSERT_FALSE(list.Register(waiters.back(), never).has_value());
  log.clear();
  list.Notify();
  EXPECT_EQ(log, (std::vector<size_t>{0, 64, 65, 66, 67, 68, 69, 70}));
  EXPECT_EQ(list.size(), 0U);
}

// A new wait appends in O(1). A cancelled wait has ended, so the same
// waiter's next wait queues afresh behind everyone already waiting.
TEST(WaitListTest, NewWaitsAppendAndACancelledWaiterQueuesAfresh) {
  WaitList list;
  std::vector<size_t> log;
  std::deque<LoggingWaiter> waiters;
  const auto never = [] { return false; };
  for (size_t id = 0; id < 1000; ++id) {
    waiters.emplace_back(id, &log);
    ASSERT_FALSE(list.Register(waiters.back(), never).has_value());
  }
  EXPECT_EQ(list.StatsForTesting().insert_steps, 0U);

  ASSERT_TRUE(list.Cancel(waiters.at(2)));
  ASSERT_FALSE(list.Register(waiters.at(2), never).has_value());
  EXPECT_EQ(list.StatsForTesting().insert_steps, 0U);

  list.Notify();
  ASSERT_EQ(log.size(), WaitList::kWakeBatch);
  EXPECT_EQ(log.at(0), 0U);
  EXPECT_EQ(log.at(1), 1U);
  EXPECT_EQ(log.at(2), 3U);
  list.FailAll(Stopped());
  EXPECT_EQ(log.back(), 2U);
}

// A reused waiter's next wait is a new one: it queues behind a waiter
// that registered after its last wait ended.
TEST(WaitListTest, AReusedWaitersNextWaitQueuesAfresh) {
  WaitList list;
  std::vector<size_t> log;
  LoggingWaiter reused(0, &log);
  LoggingWaiter later(1, &log);
  const auto never = [] { return false; };
  ASSERT_FALSE(list.Register(reused, never).has_value());
  list.Notify();
  // Woken, it finds the room: its wait ends here.
  ASSERT_TRUE(list.Register(reused, [] { return true; }).has_value());

  ASSERT_FALSE(list.Register(later, never).has_value());
  ASSERT_FALSE(list.Register(reused, never).has_value());
  log.clear();
  list.Notify();
  EXPECT_EQ(log, (std::vector<size_t>{1, 0}));
}

// Woken waiters that re-block keep their places, in whatever order
// they re-register, ahead of a newer wait.
TEST(WaitListTest, WokenWaitersThatReBlockKeepTheirPlaces) {
  WaitList list;
  std::vector<size_t> log;
  LoggingWaiter first(0, &log);
  LoggingWaiter second(1, &log);
  LoggingWaiter newer(2, &log);
  const auto never = [] { return false; };
  ASSERT_FALSE(list.Register(first, never).has_value());
  ASSERT_FALSE(list.Register(second, never).has_value());
  list.Notify();
  ASSERT_EQ(log.size(), 2U);

  ASSERT_FALSE(list.Register(second, never).has_value());
  ASSERT_FALSE(list.Register(first, never).has_value());
  ASSERT_FALSE(list.Register(newer, never).has_value());
  log.clear();
  list.Notify();
  EXPECT_EQ(log, (std::vector<size_t>{0, 1, 2}));
}

// A woken waiter that gives up passes its ok wake on, and its next
// wait queues afresh.
TEST(WaitListTest, FinishAfterAnOkWakeWakesTheNextWaiter) {
  WaitList list;
  std::vector<size_t> log;
  std::deque<LoggingWaiter> waiters;
  const auto never = [] { return false; };
  for (size_t id = 0; id < 3; ++id) {
    waiters.emplace_back(id, &log);
    ASSERT_FALSE(list.Register(waiters.back(), never).has_value());
  }
  list.HandOff();
  ASSERT_EQ(log, (std::vector<size_t>{0}));
  list.Finish(waiters.at(0));
  EXPECT_EQ(log, (std::vector<size_t>{0, 1}));
  EXPECT_EQ(waiters.at(1).ok_wakes(), 1);

  ASSERT_FALSE(list.Register(waiters.at(0), never).has_value());
  log.clear();
  list.Notify();
  EXPECT_EQ(log, (std::vector<size_t>{2, 0}));
}

// An error wake has no capacity to pass on. The finishing waiter is
// first in FailAll's first batch, and the last waiter is still linked
// when it finishes, so a hand-off would wake that one ok.
TEST(WaitListTest, FinishAfterAnErrorWakeHandsNothingOn) {
  WaitList list;
  FinishingWaiter finishing(&list);
  std::vector<size_t> log;
  std::deque<LoggingWaiter> waiters;
  const auto never = [] { return false; };
  ASSERT_FALSE(list.Register(finishing, never).has_value());
  for (size_t id = 0; id < WaitList::kWakeBatch; ++id) {
    waiters.emplace_back(id, &log);
    ASSERT_FALSE(list.Register(waiters.back(), never).has_value());
  }
  list.FailAll(Stopped());
  EXPECT_FALSE(finishing.ok());
  EXPECT_EQ(log.size(), WaitList::kWakeBatch);
  for (const auto& waiter : waiters) {
    EXPECT_EQ(waiter.ok_wakes(), 0);
    EXPECT_EQ(waiter.last_error(), ErrorCode::kUnavailable);
  }
}

// The owner published before the waiter was counted, so its Notify
// saw no waiters. The re-check after linking catches it.
TEST(WaitListTest, ConditionHeldAtRegisterCompletesWithoutLinking) {
  WaitList list;
  std::vector<size_t> log;
  LoggingWaiter waiter(0, &log);
  const auto ready = list.Register(waiter, [] { return true; });
  ASSERT_TRUE(ready.has_value() && ready->has_value());
  EXPECT_EQ(list.size(), 0U);
  list.Notify();
  EXPECT_TRUE(log.empty());
  EXPECT_FALSE(list.Cancel(waiter));
}

TEST(WaitListTest, EmptyNotifyAndHandOffTakeNoLock) {
  WaitList list;
  for (int i = 0; i < 1000; ++i) {
    list.Notify();
    list.HandOff();
  }
  EXPECT_EQ(list.StatsForTesting().locked_pops, 0U);

  std::vector<size_t> log;
  LoggingWaiter waiter(0, &log);
  ASSERT_FALSE(list.Register(waiter, [] { return false; }).has_value());
  list.Notify();
  EXPECT_EQ(list.StatsForTesting().locked_pops, 1U);
  EXPECT_EQ(log, (std::vector<size_t>{0}));
}

TEST(WaitListTest, FailAllWakesEveryWaiterAndRefusesLaterOnes) {
  WaitList list;
  std::vector<size_t> log;
  std::deque<LoggingWaiter> waiters;
  const auto never = [] { return false; };
  for (size_t id = 0; id < 130; ++id) {
    waiters.emplace_back(id, &log);
    ASSERT_FALSE(list.Register(waiters.back(), never).has_value());
  }
  Result<void> blocked;
  std::thread thread([&] { blocked = list.Await(never, 10s); });
  ASSERT_TRUE(Eventually([&] { return list.size() == 131; }));

  list.FailAll(Stopped());
  thread.join();
  ASSERT_FALSE(blocked.has_value());
  EXPECT_EQ(blocked.error().code(), ErrorCode::kUnavailable);
  EXPECT_EQ(log.size(), 130U);
  for (const auto& waiter : waiters) EXPECT_EQ(waiter.last_error(), ErrorCode::kUnavailable);
  EXPECT_EQ(list.size(), 0U);

  LoggingWaiter late(200, &log);
  const auto refused = list.Register(late, [] { return true; });
  ASSERT_TRUE(refused.has_value() && !refused->has_value());
  // NOLINTNEXTLINE(bugprone-unchecked-optional-access): ASSERT_TRUE above guards.
  EXPECT_EQ(refused->error().message(), Stopped().message());
  const auto awaited = list.Await(never, 10s);
  ASSERT_FALSE(awaited.has_value());
  EXPECT_EQ(awaited.error().code(), ErrorCode::kUnavailable);
}

TEST(WaitListTest, AwaitReturnsOnceTheConditionHolds) {
  WaitList list;
  std::atomic<bool> room{false};
  Result<void> result = std::unexpected(Error{ErrorCode::kInternal, "unset"});
  std::thread thread([&] { result = list.Await([&] { return room.load(); }, 10s); });
  ASSERT_TRUE(Eventually([&] { return list.size() == 1; }));
  list.Notify();  // A wake whose condition fails re-blocks.
  ASSERT_TRUE(Eventually([&] { return list.size() == 1; }));
  room.store(true);
  list.Notify();
  thread.join();
  EXPECT_TRUE(result.has_value());
  EXPECT_EQ(list.size(), 0U);
}

// Every waiter completes exactly once: by Register, by a true Cancel,
// or by a wake, never two of these.
TEST(WaitListTest, CancelRacingNotifyCompletesEachWaiterExactlyOnce) {
  constexpr size_t kThreads = 8;
  constexpr size_t kIterations = 2000;
  enum class Outcome : uint8_t { kRegistered, kCancelled, kWoken };

  WaitList list;
  std::atomic<uint64_t> generation{0};
  std::atomic<bool> stop{false};
  std::atomic<bool> bad{false};
  std::deque<std::vector<std::atomic<uint32_t>>> counts;
  for (size_t t = 0; t < kThreads; ++t) counts.emplace_back(kIterations);
  std::vector<std::vector<Outcome>> outcomes(kThreads, std::vector<Outcome>(kIterations));

  std::vector<std::thread> notifiers;
  notifiers.reserve(2);
  for (int n = 0; n < 2; ++n) {
    notifiers.emplace_back([&] {
      while (!stop.load()) {
        generation.fetch_add(1, std::memory_order_seq_cst);
        list.Notify();
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
        CountingWaiter waiter(&counts.at(t), i, &bad);
        const uint64_t target = generation.load() + (rng() % 3);
        const auto ready = list.Register(
            waiter, [&] { return generation.load(std::memory_order_seq_cst) > target; });
        if (ready.has_value()) {
          EXPECT_TRUE(ready->has_value());
          outcomes.at(t).at(i) = Outcome::kRegistered;
          continue;
        }
        if (rng() % 2 == 0) std::this_thread::yield();
        if (list.Cancel(waiter)) {
          outcomes.at(t).at(i) = Outcome::kCancelled;
          continue;
        }
        // Lost: the wake is coming, and the waiter must outlive it.
        outcomes.at(t).at(i) = Outcome::kWoken;
        ASSERT_TRUE(
            Eventually([&] { return counts.at(t).at(i).load(std::memory_order_acquire) != 0; }));
      }
    });
  }
  for (auto& thread : threads) thread.join();
  stop.store(true);
  for (auto& thread : notifiers) thread.join();

  EXPECT_FALSE(bad.load());
  std::array<size_t, 3> tally{};
  for (size_t t = 0; t < kThreads; ++t) {
    for (size_t i = 0; i < kIterations; ++i) {
      const Outcome outcome = outcomes.at(t).at(i);
      ++tally.at(static_cast<size_t>(outcome));
      ASSERT_EQ(counts.at(t).at(i).load(), outcome == Outcome::kWoken ? 1U : 0U)
          << "thread " << t << " iteration " << i;
    }
  }
  EXPECT_EQ(list.size(), 0U);
  std::cout << "registered " << tally.at(0) << " cancelled " << tally.at(1) << " woken "
            << tally.at(2) << '\n';
}

constexpr size_t kHerd = 130;

// One Notify makes room for everyone. Each woken waiter's
// condition holds and it blocks elsewhere, handing off as it goes.
TEST(WaitListTest, HandOffWakesEveryWaiterAfterOneNotify) {
  WaitList list;
  std::atomic<bool> room{false};
  std::vector<size_t> woken;
  std::deque<LoggingWaiter> waiters;
  for (size_t id = 0; id < kHerd; ++id) {
    waiters.emplace_back(id, &woken);
    ASSERT_FALSE(list.Register(waiters.back(), [&] { return room.load(); }).has_value());
  }
  room.store(true);
  list.Notify();
  ASSERT_EQ(woken.size(), WaitList::kWakeBatch);
  list.HandOff();
  ASSERT_EQ(woken.size(), WaitList::kWakeBatch + 1);  // One, not a batch.
  for (size_t handed = 1; handed < woken.size(); ++handed) list.HandOff();
  EXPECT_EQ(woken.size(), kHerd);
  EXPECT_EQ(list.size(), 0U);
}

// The control: without HandOff, all but the first batch are stranded.
TEST(WaitListTest, WithoutHandOffOneNotifyStrandsAllButOneBatch) {
  WaitList list;
  std::atomic<bool> room{false};
  std::vector<size_t> woken;
  std::deque<LoggingWaiter> waiters;
  for (size_t id = 0; id < kHerd; ++id) {
    waiters.emplace_back(id, &woken);
    ASSERT_FALSE(list.Register(waiters.back(), [&] { return room.load(); }).has_value());
  }
  room.store(true);
  list.Notify();
  EXPECT_EQ(woken.size(), WaitList::kWakeBatch);
  EXPECT_EQ(list.size(), kHerd - WaitList::kWakeBatch);
  list.FailAll(Stopped());
}

// Through the blocking wrapper: one Notify, and every caller returns.
TEST(WaitListTest, BlockingAwaitHandsOffSoOneNotifyWakesEveryCaller) {
  WaitList list;
  std::atomic<bool> room{false};
  std::atomic<size_t> admitted{0};
  std::vector<std::thread> threads;
  threads.reserve(kHerd);
  for (size_t i = 0; i < kHerd; ++i) {
    threads.emplace_back([&] {
      if (list.Await([&] { return room.load(); }, 20s).has_value()) admitted.fetch_add(1);
    });
  }
  ASSERT_TRUE(Eventually([&] { return list.size() == kHerd; }));
  room.store(true);
  list.Notify();
  for (auto& thread : threads) thread.join();
  EXPECT_EQ(admitted.load(), kHerd);
  EXPECT_EQ(list.size(), 0U);
}

// A publish racing each registration never strands it.
TEST(WaitListTest, PublishRacingRegistrationNeverLosesAWake) {
  constexpr uint64_t kRounds = 5000;
  WaitList list;
  std::atomic<uint64_t> generation{0};
  std::atomic<bool> lost{false};
  std::barrier sync(2);
  std::thread notifier([&] {
    std::minstd_rand rng(11);  // NOLINT(bugprone-random-generator-seed)
    for (uint64_t round = 0; round < kRounds; ++round) {
      sync.arrive_and_wait();
      if (lost.load()) return;
      for (uint32_t spin = rng() % 3; spin > 0; --spin) std::this_thread::yield();
      generation.store(round + 1, std::memory_order_seq_cst);
      list.Notify();
    }
  });
  for (uint64_t round = 0; round < kRounds; ++round) {
    sync.arrive_and_wait();
    if (lost.load()) break;
    if (!list.Await([&] { return generation.load() > round; }, 2s).has_value()) {
      ADD_FAILURE() << "round " << round << " lost its wake";
      lost.store(true);
    }
  }
  notifier.join();
  EXPECT_EQ(list.size(), 0U);
}

// A timed-out wait whose Cancel lost must wait for the wake, or the
// notifier would wake a destroyed stack waiter.
TEST(WaitListTest, BlockingWaitWhoseCancelLostWaitsForTheWake) {
  WaitList list;
  std::atomic<bool> room{false};
  GateWaiter gate;
  ASSERT_FALSE(list.Register(gate, [] { return false; }).has_value());

  std::atomic<bool> returned{false};
  Result<void> result = std::unexpected(Error{ErrorCode::kInternal, "unset"});
  std::thread blocked([&] {
    result = list.Await([&] { return room.load(); }, 50ms);
    returned.store(true);
  });
  ASSERT_TRUE(Eventually([&] { return list.size() == 2; }));
  // Wakes the gate first, so the blocked wait is unlinked but unwoken.
  std::thread notifier([&] {
    room.store(true);
    list.Notify();
  });
  ASSERT_TRUE(gate.AwaitEntered(10s));
  EXPECT_EQ(list.size(), 0U);

  std::this_thread::sleep_for(200ms);
  EXPECT_FALSE(returned.load());
  gate.Open();
  notifier.join();
  blocked.join();
  EXPECT_TRUE(result.has_value());
}

struct HeldWake {
  Result<void> a = std::unexpected(Error{ErrorCode::kInternal, "unset"});
  size_t b_wakes = 0;
  size_t left = 0;
};

// One Notify's batch ends with the blocking waiter A, and B is the
// first left behind. The gate holds the batch until A's deadline has
// passed, so A's Cancel loses and it waits for its wake. Whether the
// room is still free when A re-checks is `room_kept`.
HeldWake RunHeldWake(bool room_kept) {
  WaitList list;
  std::atomic<bool> room{false};
  const auto never = [] { return false; };
  const auto has_room = [&] { return room.load(); };
  GateWaiter gate;
  EXPECT_FALSE(list.Register(gate, never).has_value());
  std::vector<size_t> filler_log;
  std::deque<LoggingWaiter> fillers;
  for (size_t id = 0; id + 2 < WaitList::kWakeBatch; ++id) {
    fillers.emplace_back(id, &filler_log);
    EXPECT_FALSE(list.Register(fillers.back(), never).has_value());
  }

  HeldWake out;
  std::atomic<bool> a_returned{false};
  std::thread a([&] {
    out.a = list.Await(has_room, 50ms);
    a_returned.store(true);
  });
  EXPECT_TRUE(Eventually([&] { return list.size() == WaitList::kWakeBatch; }));
  std::vector<size_t> b_log;
  LoggingWaiter b(0, &b_log);
  EXPECT_FALSE(list.Register(b, has_room).has_value());

  room.store(true);
  std::thread notifier([&] { list.Notify(); });
  EXPECT_TRUE(gate.AwaitEntered(10s));
  EXPECT_EQ(list.size(), 1U);
  std::this_thread::sleep_for(200ms);
  EXPECT_FALSE(a_returned.load());
  if (!room_kept) room.store(false);
  gate.Open();
  notifier.join();
  a.join();

  out.b_wakes = b_log.size();
  out.left = list.size();
  list.FailAll(Stopped());
  return out;
}

// A wake held past A's deadline still finds the room free: A takes it
// and hands on, so B wakes with no further Release.
TEST(WaitListTest, WakeHeldPastTheDeadlineTakesFreeRoomAndHandsOn) {
  const HeldWake run = RunHeldWake(/*room_kept=*/true);
  EXPECT_TRUE(run.a.has_value());
  EXPECT_EQ(run.b_wakes, 1U);
  EXPECT_EQ(run.left, 0U);
}

// The room went before A re-checked: A times out, and with nothing to
// pass on, B stays asleep.
TEST(WaitListTest, WakeHeldPastTheDeadlineTimesOutWhenTheRoomIsGone) {
  const HeldWake run = RunHeldWake(/*room_kept=*/false);
  ASSERT_FALSE(run.a.has_value());
  EXPECT_EQ(run.a.error().code(), ErrorCode::kTimeout);
  EXPECT_EQ(run.b_wakes, 0U);
  EXPECT_EQ(run.left, 1U);
}

// For the sanitizers: a notifier races each timing-out blocking wait,
// so a waiter freed before its wake shows as a use after scope.
TEST(WaitListTest, NotifierRacingTimingOutWaitsNeverOutlivesTheWaiter) {
  constexpr uint64_t kRounds = 1500;
  WaitList list;
  std::atomic<uint64_t> generation{0};
  std::barrier sync(2);
  std::thread notifier([&] {
    std::minstd_rand rng(7);  // NOLINT(bugprone-random-generator-seed)
    for (uint64_t round = 0; round < kRounds; ++round) {
      sync.arrive_and_wait();
      std::this_thread::sleep_for(std::chrono::microseconds(rng() % 1500));
      generation.store(round + 1, std::memory_order_seq_cst);
      list.Notify();
    }
  });
  for (uint64_t round = 0; round < kRounds; ++round) {
    sync.arrive_and_wait();
    const auto result = list.Await([&] { return generation.load() > round; }, Duration(round % 2));
    if (!result.has_value()) {
      EXPECT_EQ(result.error().code(), ErrorCode::kTimeout);
    }
    EXPECT_EQ(list.size(), 0U);
  }
  notifier.join();
}

// A retry loop of timed-out waits leaves nothing behind.
TEST(WaitListTest, RetriedTimeoutsLeaveTheListEmpty) {
  WaitList list;
  for (int i = 0; i < 200; ++i) {
    const auto result = list.Await([] { return false; }, 5ms);
    ASSERT_FALSE(result.has_value());
    ASSERT_EQ(result.error().code(), ErrorCode::kTimeout);
    ASSERT_EQ(list.size(), 0U);
  }
  EXPECT_EQ(list.size(), 0U);
}

}  // namespace
}  // namespace abyss::core
