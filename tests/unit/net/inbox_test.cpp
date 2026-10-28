#include "inbox.h"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <memory>
#include <span>
#include <thread>
#include <vector>

#include "abyss/core/completion.h"
#include "abyss/net/poller.h"
#include "fake_poller.h"

namespace abyss::net {
namespace {

using namespace std::chrono_literals;

core::CompletionEvent Event(uint64_t seq) {
  return {.client_id = 1, .slot_seq = seq, .kind = core::CompletionKind::kDurable, .result = {}};
}

void Post(Inbox& inbox, uint64_t seq) {
  const core::CompletionEvent event = Event(seq);
  inbox.Post(std::span(&event, 1));
}

TEST(InboxTest, PostsBetweenTakesWakeThePollerOnce) {
  testing::FakePoller poller;
  Inbox inbox(poller);
  Post(inbox, 1);
  Post(inbox, 2);
  Post(inbox, 3);
  EXPECT_EQ(poller.CountOf(testing::FakePoller::Op::kWake), 1U);
  std::vector<core::CompletionEvent> taken;
  inbox.Take(taken);
  EXPECT_EQ(taken.size(), 3U);
  EXPECT_FALSE(inbox.WakePendingForTesting());
  Post(inbox, 4);
  EXPECT_EQ(poller.CountOf(testing::FakePoller::Op::kWake), 2U);
  taken.clear();
  inbox.Take(taken);
}

// A post landing while the reactor takes is never stranded: the flag
// is cleared before the swap, so the post either rides this take or
// finds the flag clear and wakes the poller. Cleared after the swap, it
// would find the flag still set by the post this take collected, skip
// its wake, and wait for the next unrelated one.
TEST(InboxTest, APostRacingTheTakeIsTakenOrWakesThePoller) {
  testing::FakePoller poller;
  Inbox inbox(poller);
  Post(inbox, 1);
  inbox.SetTakeHookForTesting([&inbox] { Post(inbox, 2); });
  std::vector<core::CompletionEvent> taken;
  inbox.Take(taken);
  inbox.SetTakeHookForTesting({});
  const bool woken = poller.CountOf(testing::FakePoller::Op::kWake) == 2;
  EXPECT_TRUE(taken.size() == 2 || woken)
      << "a post the take missed with no wake on its way: " << taken.size() << " taken";
  std::vector<core::CompletionEvent> rest;
  inbox.Take(rest);
  EXPECT_EQ(taken.size() + rest.size(), 2U);
}

// Real threads against a real poller whose wait never times out on its
// own: every post is seen, or the test hangs.
TEST(InboxTest, ConcurrentPostersAreAllSeen) {
  auto poller = CreatePoller();
  ASSERT_TRUE(poller.has_value()) << poller.error().message();
  Inbox inbox(**poller);
  constexpr int kPosters = 4;
  constexpr int kPosts = 5000;
  std::atomic<bool> go{false};
  std::vector<std::thread> posters;
  posters.reserve(kPosters);
  for (int p = 0; p < kPosters; ++p) {
    posters.emplace_back([&inbox, &go, p] {
      while (!go.load()) std::this_thread::yield();
      for (int i = 0; i < kPosts; ++i) {
        Post(inbox, (static_cast<uint64_t>(p) * kPosts) + static_cast<uint64_t>(i) + 1);
      }
    });
  }
  go = true;
  size_t seen = 0;
  std::vector<core::CompletionEvent> taken;
  const auto start = std::chrono::steady_clock::now();
  while (seen < static_cast<size_t>(kPosters) * kPosts &&
         std::chrono::steady_clock::now() - start < 30s) {
    EXPECT_TRUE((*poller)->Wait(10s).has_value());
    inbox.Take(taken);
    seen += taken.size();
    taken.clear();
  }
  for (auto& poster : posters) poster.join();
  EXPECT_EQ(seen, static_cast<size_t>(kPosters) * kPosts);
  inbox.Take(taken);
  EXPECT_TRUE(taken.empty());
}

#ifndef NDEBUG
TEST(InboxDeathTest, APostAfterTheReactorStoppedIsCaught) {
  GTEST_FLAG_SET(death_test_style, "threadsafe");
  EXPECT_DEATH(
      {
        testing::FakePoller poller;
        Inbox inbox(poller);
        inbox.Close();
        Post(inbox, 1);
      },
      "an engine event posted to a stopped reactor");
}
#endif

}  // namespace
}  // namespace abyss::net
