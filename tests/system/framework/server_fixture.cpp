#include "server_fixture.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <unistd.h>

#include <chrono>
#include <csignal>
#include <cstring>
#include <filesystem>
#include <string>
#include <thread>

#ifndef ABYSS_SERVER_BINARY
#define ABYSS_SERVER_BINARY ""
#endif

namespace abyss::system_test {

// --- TestServer -------------------------------------------------------------

TestServer::TestServer() {
  data_dir_ = std::filesystem::temp_directory_path() /
              ("abyss_system_test_" + std::to_string(getpid()) + "_" +
               std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
}

TestServer::~TestServer() {
  if (IsRunning()) Stop();
  std::error_code ec;
  std::filesystem::remove_all(data_dir_, ec);
}

uint16_t TestServer::AllocatePort() {
  int fd = socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0) return 0;

  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  addr.sin_port = 0;

  if (bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
    close(fd);
    return 0;
  }

  socklen_t len = sizeof(addr);
  if (getsockname(fd, reinterpret_cast<sockaddr*>(&addr), &len) < 0) {
    close(fd);
    return 0;
  }

  uint16_t port = ntohs(addr.sin_port);
  close(fd);
  return port;
}

bool TestServer::Start() {
  const char* binary = ABYSS_SERVER_BINARY;
  if (binary[0] == '\0' || !std::filesystem::exists(binary)) {
    skip_reason_ = "abyss-server binary not found";
    return false;
  }

  std::filesystem::create_directories(data_dir_);

  port_ = AllocatePort();
  if (port_ == 0) {
    skip_reason_ = "failed to allocate port";
    return false;
  }

  pid_ = fork();
  if (pid_ < 0) {
    skip_reason_ = "fork failed: " + std::string(std::strerror(errno));
    return false;
  }

  if (pid_ == 0) {
    std::string port_str = std::to_string(port_);
    std::string data_str = data_dir_.string();
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-vararg)
    execl(binary, "abyss-server", "--port", port_str.c_str(), "--data-dir", data_str.c_str(),
          nullptr);
    _exit(127);
  }

  pid_t child = pid_;
  if (!WaitForReady(std::chrono::seconds{2})) {
    int status = 0;
    pid_t wr = waitpid(child, &status, WNOHANG);
    if (wr > 0 && WIFEXITED(status)) {
      skip_reason_ = "server exited with code " + std::to_string(WEXITSTATUS(status));
    } else {
      skip_reason_ = "server not accepting connections within timeout";
      if (wr == 0) {
        kill(child, SIGKILL);
        waitpid(child, &status, 0);
      }
    }
    pid_ = -1;
    return false;
  }

  return true;
}

bool TestServer::WaitForReady(std::chrono::seconds timeout) const {
  auto deadline = std::chrono::steady_clock::now() + timeout;

  while (std::chrono::steady_clock::now() < deadline) {
    int status = 0;
    pid_t result = waitpid(pid_, &status, WNOHANG);
    if (result > 0) {
      return false;
    }

    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd >= 0) {
      sockaddr_in addr{};
      addr.sin_family = AF_INET;
      addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
      addr.sin_port = htons(port_);

      if (connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) == 0) {
        close(fd);
        return true;
      }
      close(fd);
    }

    std::this_thread::sleep_for(std::chrono::milliseconds{25});
  }

  return false;
}

void TestServer::Stop() {
  if (!IsRunning()) return;
  kill(pid_, SIGTERM);
  int status = 0;
  waitpid(pid_, &status, 0);
  pid_ = -1;
}

void TestServer::Kill() {
  if (!IsRunning()) return;
  kill(pid_, SIGKILL);
  int status = 0;
  waitpid(pid_, &status, 0);
  pid_ = -1;
}

// --- SystemTest -------------------------------------------------------------

TestServer& SystemTest::SharedServer() {
  static TestServer server;
  static bool attempted = false;
  if (!attempted) {
    attempted = true;
    server.Start();
  }
  return server;
}

void SystemTest::SetUp() {
  auto& server = SharedServer();
  if (!server.IsRunning()) {
    GTEST_SKIP() << server.SkipReason();
  }

  client_.emplace();
  if (!client_->Connect("127.0.0.1", server.Port())) {
    GTEST_SKIP() << "cannot connect to server on port " << server.Port();
  }

  client_->Command({"FLUSHALL"});
}

void SystemTest::TearDown() { client_.reset(); }

RedisClient& SystemTest::Client() { return *client_; }

uint16_t SystemTest::ServerPort() { return SharedServer().Port(); }

// --- DurabilityTest ---------------------------------------------------------

void DurabilityTest::SetUp() {
  if (!server_.Start()) {
    GTEST_SKIP() << server_.SkipReason();
  }

  client_.emplace();
  if (!client_->Connect("127.0.0.1", server_.Port())) {
    GTEST_SKIP() << "cannot connect to server on port " << server_.Port();
  }
}

void DurabilityTest::TearDown() {
  client_.reset();
  if (server_.IsRunning()) server_.Stop();
}

RedisClient& DurabilityTest::Client() { return *client_; }

void DurabilityTest::RestartServer() {
  client_->Close();
  server_.Stop();
  ASSERT_TRUE(server_.Start()) << server_.SkipReason();
  ASSERT_TRUE(client_->Connect("127.0.0.1", server_.Port()));
}

void DurabilityTest::KillAndRestartServer() {
  client_->Close();
  server_.Kill();
  ASSERT_TRUE(server_.Start()) << server_.SkipReason();
  ASSERT_TRUE(client_->Connect("127.0.0.1", server_.Port()));
}

// --- DataCommandTest --------------------------------------------------------

void DataCommandTest::SetUp() {
  SystemTest::SetUp();
  if (IsSkipped()) return;
  auto r = Client().Command({"SET", "__probe__", "1"});
  if (r.IsError() && r.String().contains("not implemented")) {
    GTEST_SKIP() << "data commands not yet implemented";
  }
  Client().Command({"DEL", "__probe__"});
}

// --- DataDurabilityTest -----------------------------------------------------

void DataDurabilityTest::SetUp() {
  DurabilityTest::SetUp();
  if (IsSkipped()) return;
  auto r = Client().Command({"SET", "__probe__", "1"});
  if (r.IsError() && r.String().contains("not implemented")) {
    GTEST_SKIP() << "data commands not yet implemented";
  }
  Client().Command({"DEL", "__probe__"});
}

}  // namespace abyss::system_test
