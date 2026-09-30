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

    std::string config_yaml;
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
  uint16_t AdminPort() const { return admin_port_; }
  uint16_t MetricsPort() const { return metrics_port_; }
  const std::string& SkipReason() const { return skip_reason_; }

 private:
  bool WaitForReady();
  void WaitChild();

  Config config_;
  testing::TempDir data_dir_;
  proc_handle_t proc_ = kInvalidProcHandle;
  pipe_handle_t ready_read_ = kInvalidPipeHandle;
  uint16_t port_ = 0;
  uint16_t admin_port_ = 0;
  uint16_t metrics_port_ = 0;
  std::string skip_reason_;
};

// Class-scoped shared server via SetUpTestSuite / TearDownTestSuite. One
// server per TEST_F class; different classes parallelize. Bare SystemTest has
// no keyspace cleanup between tests, so tests that mutate the keyspace must use
// a data fixture: DataCommandTest (= SharedDataServerTest: class-shared,
// FLUSHDB-in-SetUp) for light writes, or IsolatedDataServerTest (fresh server
// per test, no FLUSHDB) for restart/kill or write-heavy tests. Use
// IsolatedServerTest for restart or clean-baseline metric tests.
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

// Per-test isolated server. Required for tests that restart/kill the server
// or assert metrics from a clean baseline.
class IsolatedServerTest : public ::testing::Test {
 protected:
  void SetUp() override;
  void TearDown() override;

  RedisClient& Client() { return client_; }
  const TestServer& Server() const { return *server_; }

  void RestartServer();
  void KillAndRestartServer();

  // Override to customize the server configuration for a fixture. The default
  // produces a TestServer launched against compile-time server defaults
  // (matches pre-extension behaviour).
  virtual TestServer::Config MakeServerConfig() const { return TestServer::Config{}; }

 private:
  std::unique_ptr<TestServer> server_;
  RedisClient client_;
};

// Per-test server, with a SET/DEL readiness probe so a half-wired data path
// fails here rather than inside the test.
//
// That probe is a real write: it occupies a compaction-buffer entry that flushes
// about one quiet window after SetUp returns, i.e. during the test body. Any
// test that baselines a cold-tier flush counter must first call
// `AwaitColdQuiescence` (tests/system/framework/prom_scrape.h) or it will
// attribute the probe's flush to its own writes.
class IsolatedDataServerTest : public IsolatedServerTest {
 protected:
  void SetUp() override;
};

// Class-shared server with FLUSHDB-in-SetUp for keyspace isolation. Tests that
// restart the server or assert against non-keyspace state must use the
// `Isolated*` fixtures directly.
class SharedDataServerTest : public SystemTest {
 protected:
  void SetUp() override;
};

using DataCommandTest = SharedDataServerTest;

}  // namespace abyss::system_test
