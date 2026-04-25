#include "abyss/net/poller.h"

#include <fcntl.h>
#include <gtest/gtest.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <thread>

namespace abyss::net {
namespace {

int MakeNonblockingPipe(int fds[2]) {
  if (::pipe(fds) < 0) return -1;
  for (int i = 0; i < 2; ++i) {
    // NOLINTBEGIN(cppcoreguidelines-pro-type-vararg)
    const int flags = ::fcntl(fds[i], F_GETFL, 0);
    if (::fcntl(fds[i], F_SETFL, flags | O_NONBLOCK) < 0) return -1;
    // NOLINTEND(cppcoreguidelines-pro-type-vararg)
  }
  return 0;
}

TEST(PollerTest, ConstructsPlatformDefault) {
  auto p = CreatePoller();
  ASSERT_TRUE(p.has_value()) << p.error().message();
}

TEST(PollerTest, ReadableFiresWhenDataAvailable) {
  auto p = CreatePoller();
  ASSERT_TRUE(p.has_value());
  int fds[2] = {-1, -1};  // NOLINT(modernize-avoid-c-arrays)
  ASSERT_EQ(MakeNonblockingPipe(fds), 0);

  int marker = 0;
  ASSERT_TRUE((*p)->Add(fds[0], EventKind::kReadable, &marker).has_value());
  ASSERT_EQ(::write(fds[1], "x", 1), 1);

  auto events = (*p)->Wait(std::chrono::milliseconds{500});
  ASSERT_TRUE(events.has_value());
  ASSERT_EQ(events->size(), 1U);
  EXPECT_EQ(events->front().user_data, &marker);
  EXPECT_TRUE(Has(events->front().kinds, EventKind::kReadable));

  ::close(fds[0]);
  ::close(fds[1]);
}

TEST(PollerTest, RemoveStopsDelivery) {
  auto p = CreatePoller();
  ASSERT_TRUE(p.has_value());
  int fds[2] = {-1, -1};  // NOLINT(modernize-avoid-c-arrays)
  ASSERT_EQ(MakeNonblockingPipe(fds), 0);

  int marker = 0;
  ASSERT_TRUE((*p)->Add(fds[0], EventKind::kReadable, &marker).has_value());
  ASSERT_TRUE((*p)->Remove(fds[0]).has_value());
  ASSERT_EQ(::write(fds[1], "x", 1), 1);

  auto events = (*p)->Wait(std::chrono::milliseconds{50});
  ASSERT_TRUE(events.has_value());
  EXPECT_EQ(events->size(), 0U);

  ::close(fds[0]);
  ::close(fds[1]);
}

TEST(PollerTest, WakeReturnsPromptlyAcrossThreads) {
  auto p = CreatePoller();
  ASSERT_TRUE(p.has_value());
  Poller* poller = p->get();

  std::atomic<bool> woke{false};
  std::thread t([poller, &woke] {
    auto events = poller->Wait(std::chrono::milliseconds{2000});
    EXPECT_TRUE(events.has_value());
    woke.store(true);
  });

  std::this_thread::sleep_for(std::chrono::milliseconds{50});
  ASSERT_TRUE(poller->Wake().has_value());
  t.join();
  EXPECT_TRUE(woke.load());
}

TEST(PollerTest, ModifyToggleReadability) {
  auto p = CreatePoller();
  ASSERT_TRUE(p.has_value());
  int fds[2] = {-1, -1};  // NOLINT(modernize-avoid-c-arrays)
  ASSERT_EQ(MakeNonblockingPipe(fds), 0);

  int marker = 0;
  ASSERT_TRUE((*p)->Add(fds[0], EventKind::kReadable, &marker).has_value());
  ASSERT_TRUE((*p)->Modify(fds[0], EventKind::kNone, &marker).has_value());
  ASSERT_EQ(::write(fds[1], "x", 1), 1);

  auto events = (*p)->Wait(std::chrono::milliseconds{50});
  ASSERT_TRUE(events.has_value());
  EXPECT_EQ(events->size(), 0U);

  ASSERT_TRUE((*p)->Modify(fds[0], EventKind::kReadable, &marker).has_value());
  events = (*p)->Wait(std::chrono::milliseconds{500});
  ASSERT_TRUE(events.has_value());
  EXPECT_GE(events->size(), 1U);

  ::close(fds[0]);
  ::close(fds[1]);
}

}  // namespace
}  // namespace abyss::net
