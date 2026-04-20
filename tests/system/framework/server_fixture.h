#pragma once

#include <gtest/gtest.h>

#include <chrono>
#include <cstdint>
#include <memory>
#include <string>

#include "platform_compat.h"
#include "redis_client.h"
#include "temp_dir.h"

namespace abyss::system_test {

class TestServer {
 public:
  struct Config {
    uint32_t shard_count = 4;
    std::chrono::milliseconds ready_timeout{5000};
  };

  TestServer() : TestServer(Config{}) {}
  explicit TestServer(Config config);
  ~TestServer();

  TestServer(const TestServer&) = delete;
  TestServer& operator=(const TestServer&) = delete;
  TestServer(TestServer&&) = delete;
  TestServer& operator=(TestServer&&) = delete;

  bool Start();
  void Stop();
  void Kill();

  bool IsRunning() const { return proc_ != kInvalidProcHandle; }
  uint16_t Port() const { return port_; }
  const std::string& SkipReason() const { return skip_reason_; }

 private:
  bool WaitForReady();
  void WaitChild();

  Config config_;
  testing::TempDir data_dir_;
  proc_handle_t proc_ = kInvalidProcHandle;
  pipe_handle_t ready_read_ = kInvalidPipeHandle;
  uint16_t port_ = 0;
  std::string skip_reason_;
};

// Class-scoped shared server via SetUpTestSuite / TearDownTestSuite. One
// server per TEST_F class; different classes parallelize. Use
// IsolatedServerTest instead when a test restarts the server or asserts
// against non-keyspace state.
class SystemTest : public ::testing::Test {
 public:
  static void SetUpTestSuite();
  static void TearDownTestSuite();

 protected:
  void SetUp() override;
  void TearDown() override;

  RedisClient& Client() { return client_; }
  uint16_t ServerPort();

 private:
  static std::unique_ptr<TestServer> shared_server_;
  RedisClient client_;
};

class DataCommandTest : public SystemTest {
 protected:
  void SetUp() override;
};

// Per-test isolated server. Required for tests that restart/kill the server
// or assert metrics from a clean baseline.
class IsolatedServerTest : public ::testing::Test {
 protected:
  void SetUp() override;
  void TearDown() override;

  RedisClient& Client() { return client_; }
  const TestServer& Server() const { return server_; }

  void RestartServer();
  void KillAndRestartServer();

 private:
  TestServer server_;
  RedisClient client_;
};

class IsolatedDataServerTest : public IsolatedServerTest {
 protected:
  void SetUp() override;
};

}  // namespace abyss::system_test
