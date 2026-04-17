#pragma once

#include <gtest/gtest.h>

#include <cstdint>
#include <filesystem>
#include <optional>
#include <string>

#include "redis_client.h"

namespace abyss::system_test {

class TestServer {
 public:
  TestServer();
  ~TestServer();

  TestServer(const TestServer&) = delete;
  TestServer& operator=(const TestServer&) = delete;
  TestServer(TestServer&&) = delete;
  TestServer& operator=(TestServer&&) = delete;

  bool Start();
  void Stop();
  void Kill();
  bool IsRunning() const { return pid_ > 0; }
  uint16_t Port() const { return port_; }
  const std::string& SkipReason() const { return skip_reason_; }

 private:
  static uint16_t AllocatePort();
  bool WaitForReady(std::chrono::seconds timeout) const;

  pid_t pid_ = -1;
  uint16_t port_ = 0;
  std::filesystem::path data_dir_;
  std::string skip_reason_;
};

class SystemTest : public ::testing::Test {
 protected:
  void SetUp() override;
  void TearDown() override;
  RedisClient& Client();
  uint16_t ServerPort();

 private:
  static TestServer& SharedServer();
  std::optional<RedisClient> client_;
};

class DataCommandTest : public SystemTest {
 protected:
  void SetUp() override;
};

class DurabilityTest : public ::testing::Test {
 protected:
  void SetUp() override;
  void TearDown() override;
  RedisClient& Client();
  void RestartServer();
  void KillAndRestartServer();

 private:
  TestServer server_;
  std::optional<RedisClient> client_;
};

class DataDurabilityTest : public DurabilityTest {
 protected:
  void SetUp() override;
};

}  // namespace abyss::system_test
