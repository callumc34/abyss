#include <gtest/gtest.h>

#include <chrono>
#include <memory>
#include <string>

#include "abyss/net/tcp_server.h"
#include "abyss/resp/command_registry.h"
#include "integration_harness.h"
#include "sync_redis_client.h"

namespace abyss::net {
namespace {

class TcpServerSmokeTest : public ::testing::Test {
 protected:
  void SetUp() override {
    TcpServerConfig cfg;
    cfg.bind = "127.0.0.1";
    cfg.port = 0;
    cfg.io_threads = 2;
    cfg.shutdown_grace = std::chrono::seconds{2};
    cfg.reaper_tick = std::chrono::milliseconds{100};
    cfg.connection.idle_timeout = std::chrono::seconds{30};

    server_ = std::make_unique<TcpServer>(cfg, resp::GlobalRegistry(), harness_.Engine());
    ASSERT_TRUE(server_->Start().has_value());
    ASSERT_TRUE(client_.Connect(server_->BoundPort()));
  }

  void TearDown() override {
    client_.Close();
    server_->Stop();
  }

  abyss::testing::IntegrationHarness harness_;
  std::unique_ptr<TcpServer> server_;
  component_test::SyncRedisClient client_;
};

TEST_F(TcpServerSmokeTest, PingPongEndToEnd) { EXPECT_EQ(client_.Command({"PING"}), "+PONG\r\n"); }

TEST_F(TcpServerSmokeTest, SetThenGet) {
  EXPECT_EQ(client_.Command({"SET", "k", "alpha"}), "+OK\r\n");
  EXPECT_EQ(client_.Command({"GET", "k"}), "$5\r\nalpha\r\n");
}

TEST_F(TcpServerSmokeTest, QuitClosesConnection) {
  EXPECT_EQ(client_.Command({"QUIT"}), "+OK\r\n");
  EXPECT_TRUE(client_.ReadSome(16, std::chrono::milliseconds{500}).empty());
}

}  // namespace
}  // namespace abyss::net
