#include "abyss/net/tcp_server.h"

#include <gtest/gtest.h>
#include <sys/socket.h>

#include <atomic>
#include <chrono>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include "abyss/core/command_dispatcher.h"
#include "abyss/core/resp_types.h"
#include "abyss/core/result.h"
#include "abyss/resp/command_registry.h"
#include "sync_redis_client.h"

namespace abyss::net {
namespace {

class StubDispatcher : public core::CommandDispatcher {
 public:
  std::atomic<int> read_calls{0};
  std::atomic<int> write_calls{0};
  std::string read_payload = "value";

  core::Result<core::RespValue> DispatchRead(std::string_view /*name*/,
                                             const core::RespCommand& /*cmd*/) override {
    read_calls.fetch_add(1);
    return core::RespValue::BulkString(read_payload);
  }
  core::Result<core::RespValue> DispatchWrite(std::string_view /*name*/,
                                              core::RespCommand /*cmd*/) override {
    write_calls.fetch_add(1);
    return core::RespValue::SimpleString("OK");
  }
};

TcpServerConfig DefaultTestConfig() {
  TcpServerConfig c;
  c.bind = "127.0.0.1";
  c.port = 0;
  c.max_connections = 32;
  c.io_threads = 2;
  c.shutdown_grace = std::chrono::seconds{2};
  c.reaper_tick = std::chrono::milliseconds{100};
  c.connection.idle_timeout = std::chrono::seconds{60};
  return c;
}

TEST(TcpServerComponentTest, BindFailureSurfacesAsError) {
  TcpServerConfig good = DefaultTestConfig();
  good.port = 0;
  StubDispatcher dispatcher;
  TcpServer s1(good, resp::GlobalRegistry(), dispatcher);
  ASSERT_TRUE(s1.Start().has_value());

  TcpServerConfig dup = DefaultTestConfig();
  dup.port = s1.BoundPort();
  TcpServer s2(dup, resp::GlobalRegistry(), dispatcher);
  auto r = s2.Start();
  EXPECT_FALSE(r.has_value());

  s1.Stop();
}

TEST(TcpServerComponentTest, PingRoundTrip) {
  StubDispatcher dispatcher;
  TcpServer server(DefaultTestConfig(), resp::GlobalRegistry(), dispatcher);
  ASSERT_TRUE(server.Start().has_value());

  component_test::SyncRedisClient client;
  ASSERT_TRUE(client.Connect(server.BoundPort()));
  const std::string reply = client.Command({"PING"});
  EXPECT_EQ(reply, "+PONG\r\n");

  server.Stop();
}

TEST(TcpServerComponentTest, GetAndSetExerciseDispatcher) {
  StubDispatcher dispatcher;
  TcpServer server(DefaultTestConfig(), resp::GlobalRegistry(), dispatcher);
  ASSERT_TRUE(server.Start().has_value());

  component_test::SyncRedisClient client;
  ASSERT_TRUE(client.Connect(server.BoundPort()));
  EXPECT_EQ(client.Command({"SET", "k", "v"}), "+OK\r\n");
  EXPECT_EQ(client.Command({"GET", "k"}), "$5\r\nvalue\r\n");
  EXPECT_GE(dispatcher.write_calls.load(), 1);
  EXPECT_GE(dispatcher.read_calls.load(), 1);

  server.Stop();
}

TEST(TcpServerComponentTest, PipeliningPreservesOrder) {
  StubDispatcher dispatcher;
  TcpServer server(DefaultTestConfig(), resp::GlobalRegistry(), dispatcher);
  ASSERT_TRUE(server.Start().has_value());

  component_test::SyncRedisClient client;
  ASSERT_TRUE(client.Connect(server.BoundPort()));

  ASSERT_TRUE(client.SendRaw("*1\r\n$4\r\nPING\r\n*1\r\n$4\r\nPING\r\n*1\r\n$4\r\nPING\r\n"));

  std::string acc;
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds{2};
  while (std::chrono::steady_clock::now() < deadline) {
    auto chunk = client.ReadSome(4096, std::chrono::milliseconds{200});
    if (chunk.empty()) continue;
    acc += chunk;
    size_t count = 0;
    size_t pos = 0;
    while ((pos = acc.find("+PONG\r\n", pos)) != std::string::npos) {
      ++count;
      pos += 7;
    }
    if (count >= 3) break;
  }

  EXPECT_EQ(acc, "+PONG\r\n+PONG\r\n+PONG\r\n");
  server.Stop();
}

// #94: Stop must not block per-connection.
TEST(TcpServerComponentTest, StopJoinIsBoundedWithActiveConnections) {
  TcpServerConfig cfg = DefaultTestConfig();
  cfg.shutdown_grace = std::chrono::seconds{1};
  cfg.io_threads = 4;
  StubDispatcher dispatcher;
  TcpServer server(cfg, resp::GlobalRegistry(), dispatcher);
  ASSERT_TRUE(server.Start().has_value());

  std::vector<component_test::SyncRedisClient> clients(8);
  for (auto& c : clients) {
    ASSERT_TRUE(c.Connect(server.BoundPort()));
    EXPECT_EQ(c.Command({"PING"}), "+PONG\r\n");
  }

  const auto t0 = std::chrono::steady_clock::now();
  server.Stop();
  const auto elapsed = std::chrono::steady_clock::now() - t0;
  EXPECT_LT(elapsed, std::chrono::seconds{3});
}

TEST(TcpServerComponentTest, MaxConnectionsRejectsNewClients) {
  TcpServerConfig cfg = DefaultTestConfig();
  cfg.max_connections = 2;
  StubDispatcher dispatcher;
  TcpServer server(cfg, resp::GlobalRegistry(), dispatcher);
  ASSERT_TRUE(server.Start().has_value());

  component_test::SyncRedisClient c1;
  component_test::SyncRedisClient c2;
  ASSERT_TRUE(c1.Connect(server.BoundPort()));
  ASSERT_TRUE(c2.Connect(server.BoundPort()));

  EXPECT_EQ(c1.Command({"PING"}), "+PONG\r\n");
  EXPECT_EQ(c2.Command({"PING"}), "+PONG\r\n");

  // Third connect lands in the kernel backlog; server-side accept closes it.
  component_test::SyncRedisClient c3;
  if (c3.Connect(server.BoundPort())) {
    EXPECT_TRUE(c3.Command({"PING"}).empty());
  }

  server.Stop();
}

TEST(TcpServerComponentTest, SlowClientHardLimitsWithoutAffectingOthers) {
  TcpServerConfig cfg = DefaultTestConfig();
  cfg.connection.write_backpressure_bytes = 4096;
  cfg.connection.write_resume_bytes = 1024;
  cfg.connection.write_hard_limit_bytes = 8192;
  cfg.io_threads = 2;
  StubDispatcher dispatcher;
  // Payload large enough to overflow loopback kernel buffers on every platform.
  dispatcher.read_payload.assign(4 * 1024 * 1024, 'z');
  TcpServer server(cfg, resp::GlobalRegistry(), dispatcher);
  ASSERT_TRUE(server.Start().has_value());

  component_test::SyncRedisClient slow;
  ASSERT_TRUE(slow.Connect(server.BoundPort()));
  // Tight RCVBUF so server-side send() EAGAINs early on macOS too.
  const int rcvbuf = 4096;
  ::setsockopt(slow.Fd(), SOL_SOCKET, SO_RCVBUF, &rcvbuf, sizeof(rcvbuf));
  ASSERT_TRUE(slow.SendRaw("*2\r\n$3\r\nGET\r\n$1\r\nk\r\n"));

  component_test::SyncRedisClient healthy;
  ASSERT_TRUE(healthy.Connect(server.BoundPort()));
  EXPECT_EQ(healthy.Command({"PING"}), "+PONG\r\n");
  EXPECT_EQ(healthy.Command({"PING"}), "+PONG\r\n");

  std::string drain;
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds{2};
  while (std::chrono::steady_clock::now() < deadline) {
    auto chunk = slow.ReadSome(8192, std::chrono::milliseconds{100});
    if (chunk.empty()) break;
    drain += chunk;
  }
  EXPECT_LT(drain.size(), 1U * 1024U * 1024U);

  EXPECT_EQ(healthy.Command({"PING"}), "+PONG\r\n");

  server.Stop();
}

TEST(TcpServerComponentTest, AcceptBurstHandledWithoutDrops) {
  TcpServerConfig cfg = DefaultTestConfig();
  cfg.max_connections = 64;
  cfg.io_threads = 2;
  StubDispatcher dispatcher;
  TcpServer server(cfg, resp::GlobalRegistry(), dispatcher);
  ASSERT_TRUE(server.Start().has_value());

  std::vector<component_test::SyncRedisClient> clients(16);
  for (auto& c : clients) {
    ASSERT_TRUE(c.Connect(server.BoundPort()));
  }
  for (auto& c : clients) {
    EXPECT_EQ(c.Command({"PING"}), "+PONG\r\n");
  }
  EXPECT_EQ(server.ActiveConnections(), 16U);

  server.Stop();
}

TEST(TcpServerComponentTest, IdleConnectionsClosedByReaper) {
  TcpServerConfig cfg = DefaultTestConfig();
  cfg.connection.idle_timeout = std::chrono::seconds{1};
  cfg.reaper_tick = std::chrono::milliseconds{100};
  StubDispatcher dispatcher;
  TcpServer server(cfg, resp::GlobalRegistry(), dispatcher);
  ASSERT_TRUE(server.Start().has_value());

  component_test::SyncRedisClient client;
  ASSERT_TRUE(client.Connect(server.BoundPort()));
  EXPECT_EQ(client.Command({"PING"}), "+PONG\r\n");

  std::this_thread::sleep_for(std::chrono::milliseconds{1500});
  EXPECT_TRUE(client.ReadSome(16, std::chrono::milliseconds{500}).empty());

  server.Stop();
}

}  // namespace
}  // namespace abyss::net
