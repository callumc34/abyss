#include "abyss/net/poller.h"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <thread>

#include "abyss/platform/net.h"
#include "socket_pair.h"

namespace abyss::net {
namespace {

namespace pnet = abyss::platform::net;
using abyss::testing::SocketPair;

TEST(PollerTest, ConstructsPlatformDefault) {
  pnet::Scope scope;
  ASSERT_TRUE(scope.ok());
  auto p = CreatePoller();
  ASSERT_TRUE(p.has_value()) << p.error().message();
}

TEST(PollerTest, ReadableFiresWhenDataAvailable) {
  pnet::Scope scope;
  ASSERT_TRUE(scope.ok());
  auto p = CreatePoller();
  ASSERT_TRUE(p.has_value());
  auto pair = SocketPair::Make();
  ASSERT_TRUE(pair.has_value()) << pair.error().message();

  int marker = 0;
  ASSERT_TRUE((*p)->Add(pair->Read(), EventKind::kReadable, &marker).has_value());
  ASSERT_GT(pnet::Send(pair->Write(), "x", 1, 0), 0);

  auto events = (*p)->Wait(std::chrono::milliseconds{500});
  ASSERT_TRUE(events.has_value());
  ASSERT_EQ(events->size(), 1U);
  EXPECT_EQ(events->front().user_data, &marker);
  EXPECT_TRUE(Has(events->front().kinds, EventKind::kReadable));
}

TEST(PollerTest, RemoveStopsDelivery) {
  pnet::Scope scope;
  ASSERT_TRUE(scope.ok());
  auto p = CreatePoller();
  ASSERT_TRUE(p.has_value());
  auto pair = SocketPair::Make();
  ASSERT_TRUE(pair.has_value()) << pair.error().message();

  int marker = 0;
  ASSERT_TRUE((*p)->Add(pair->Read(), EventKind::kReadable, &marker).has_value());
  ASSERT_TRUE((*p)->Remove(pair->Read()).has_value());
  ASSERT_GT(pnet::Send(pair->Write(), "x", 1, 0), 0);

  auto events = (*p)->Wait(std::chrono::milliseconds{50});
  ASSERT_TRUE(events.has_value());
  EXPECT_EQ(events->size(), 0U);
}

TEST(PollerTest, WakeReturnsPromptlyAcrossThreads) {
  pnet::Scope scope;
  ASSERT_TRUE(scope.ok());
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
  pnet::Scope scope;
  ASSERT_TRUE(scope.ok());
  auto p = CreatePoller();
  ASSERT_TRUE(p.has_value());
  auto pair = SocketPair::Make();
  ASSERT_TRUE(pair.has_value()) << pair.error().message();

  int marker = 0;
  ASSERT_TRUE((*p)->Add(pair->Read(), EventKind::kReadable, &marker).has_value());
  ASSERT_TRUE((*p)->Modify(pair->Read(), EventKind::kNone, &marker).has_value());
  ASSERT_GT(pnet::Send(pair->Write(), "x", 1, 0), 0);

  auto events = (*p)->Wait(std::chrono::milliseconds{50});
  ASSERT_TRUE(events.has_value());
  EXPECT_EQ(events->size(), 0U);

  ASSERT_TRUE((*p)->Modify(pair->Read(), EventKind::kReadable, &marker).has_value());
  events = (*p)->Wait(std::chrono::milliseconds{500});
  ASSERT_TRUE(events.has_value());
  EXPECT_GE(events->size(), 1U);
}

}  // namespace
}  // namespace abyss::net
