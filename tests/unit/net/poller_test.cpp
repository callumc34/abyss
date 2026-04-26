#include "abyss/net/poller.h"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <thread>
#include <utility>

#include "abyss/platform/net.h"
#include "abyss/platform/types.h"

#ifdef _WIN32
#include <ws2tcpip.h>
#else
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#endif

namespace abyss::net {
namespace {

namespace pnet = abyss::platform::net;

// A pollable, bidirectional pair of sockets connected via TCP loopback. Used
// instead of pipe()/socketpair() so the same test path exercises every Poller
// backend: WSAPoll only polls sockets.
struct SocketPair {
  Socket read_end = kInvalidSocket;
  Socket write_end = kInvalidSocket;

  SocketPair() = default;
  ~SocketPair() {
    if (read_end != kInvalidSocket) pnet::CloseSocket(read_end);
    if (write_end != kInvalidSocket) pnet::CloseSocket(write_end);
  }
  SocketPair(const SocketPair&) = delete;
  SocketPair& operator=(const SocketPair&) = delete;
  SocketPair(SocketPair&&) = delete;
  SocketPair& operator=(SocketPair&&) = delete;
};

::testing::AssertionResult MakeSocketPair(SocketPair* out) {
  Socket listener = ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
  if (listener == kInvalidSocket) return ::testing::AssertionFailure() << "listen socket";

  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  addr.sin_port = 0;
  if (::bind(listener, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
    pnet::CloseSocket(listener);
    return ::testing::AssertionFailure() << "bind";
  }
  if (::listen(listener, 1) < 0) {
    pnet::CloseSocket(listener);
    return ::testing::AssertionFailure() << "listen";
  }
#ifdef _WIN32
  int len = sizeof(addr);
#else
  socklen_t len = sizeof(addr);
#endif
  if (::getsockname(listener, reinterpret_cast<sockaddr*>(&addr), &len) < 0) {
    pnet::CloseSocket(listener);
    return ::testing::AssertionFailure() << "getsockname";
  }

  Socket writer = ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
  if (writer == kInvalidSocket) {
    pnet::CloseSocket(listener);
    return ::testing::AssertionFailure() << "writer socket";
  }
  if (::connect(writer, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
    pnet::CloseSocket(listener);
    pnet::CloseSocket(writer);
    return ::testing::AssertionFailure() << "connect";
  }

  Socket reader = ::accept(listener, nullptr, nullptr);
  pnet::CloseSocket(listener);
  if (reader == kInvalidSocket) {
    pnet::CloseSocket(writer);
    return ::testing::AssertionFailure() << "accept";
  }

  if (auto r = pnet::SetNonBlocking(reader); !r) {
    pnet::CloseSocket(reader);
    pnet::CloseSocket(writer);
    return ::testing::AssertionFailure() << r.error().message();
  }
  if (auto r = pnet::SetNonBlocking(writer); !r) {
    pnet::CloseSocket(reader);
    pnet::CloseSocket(writer);
    return ::testing::AssertionFailure() << r.error().message();
  }

  out->read_end = reader;
  out->write_end = writer;
  return ::testing::AssertionSuccess();
}

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
  SocketPair pair;
  ASSERT_TRUE(MakeSocketPair(&pair));

  int marker = 0;
  ASSERT_TRUE((*p)->Add(pair.read_end, EventKind::kReadable, &marker).has_value());
  ASSERT_GT(pnet::Send(pair.write_end, "x", 1, 0), 0);

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
  SocketPair pair;
  ASSERT_TRUE(MakeSocketPair(&pair));

  int marker = 0;
  ASSERT_TRUE((*p)->Add(pair.read_end, EventKind::kReadable, &marker).has_value());
  ASSERT_TRUE((*p)->Remove(pair.read_end).has_value());
  ASSERT_GT(pnet::Send(pair.write_end, "x", 1, 0), 0);

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
  SocketPair pair;
  ASSERT_TRUE(MakeSocketPair(&pair));

  int marker = 0;
  ASSERT_TRUE((*p)->Add(pair.read_end, EventKind::kReadable, &marker).has_value());
  ASSERT_TRUE((*p)->Modify(pair.read_end, EventKind::kNone, &marker).has_value());
  ASSERT_GT(pnet::Send(pair.write_end, "x", 1, 0), 0);

  auto events = (*p)->Wait(std::chrono::milliseconds{50});
  ASSERT_TRUE(events.has_value());
  EXPECT_EQ(events->size(), 0U);

  ASSERT_TRUE((*p)->Modify(pair.read_end, EventKind::kReadable, &marker).has_value());
  events = (*p)->Wait(std::chrono::milliseconds{500});
  ASSERT_TRUE(events.has_value());
  EXPECT_GE(events->size(), 1U);
}

}  // namespace
}  // namespace abyss::net
