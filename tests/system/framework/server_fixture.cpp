#include "server_fixture.h"

#ifdef _WIN32
#include <windows.h>
#else
#include <sys/wait.h>
#include <unistd.h>

#include <csignal>
#endif

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
  uint64_t proc_id = 0;
#ifdef _WIN32
  proc_id = static_cast<uint64_t>(GetCurrentProcessId());
#else
  proc_id = static_cast<uint64_t>(getpid());
#endif

  data_dir_ = std::filesystem::temp_directory_path() /
              ("abyss_system_test_" + std::to_string(proc_id) + "_" +
               std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
}

TestServer::~TestServer() {
  if (IsRunning()) Stop();
  std::error_code ec;
  std::filesystem::remove_all(data_dir_, ec);

#ifdef _WIN32
  WSACleanup();
#endif
}

bool TestServer::IsRunning() const {
#ifdef _WIN32
  return process_handle_ != nullptr;
#else
  return pid_ > 0;
#endif
}

uint16_t TestServer::AllocatePort() {
  socket_t fd = socket(AF_INET, SOCK_STREAM, 0);
  if (fd == kInvalidSocket) return 0;

  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  addr.sin_port = 0;

  if (bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
    CLOSE_SOCKET(fd);
    return 0;
  }

  socklen_t len = sizeof(addr);
  if (getsockname(fd, reinterpret_cast<sockaddr*>(&addr), &len) < 0) {
    CLOSE_SOCKET(fd);
    return 0;
  }

  uint16_t port = ntohs(addr.sin_port);
  CLOSE_SOCKET(fd);
  return port;
}

bool TestServer::Start() {
#ifdef _WIN32
  WSADATA wsa_data;
  if (WSAStartup(MAKEWORD(2, 2), &wsa_data) != 0) {
    skip_reason_ = "WSAStartup failed in test fixture";
    return false;
  }
#endif

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

#ifdef _WIN32
  STARTUPINFOA si;
  PROCESS_INFORMATION pi;
  ZeroMemory(&si, sizeof(si));
  si.cb = sizeof(si);
  ZeroMemory(&pi, sizeof(pi));

  std::string port_str = std::to_string(port_);
  std::string data_str = data_dir_.string();

  // CreateProcessA expects a mutable string. Quote paths to handle spaces safely.
  std::string cmd_line =
      "\"" + std::string(binary) + "\" --port " + port_str + " --data-dir \"" + data_str + "\"";

  if (!CreateProcessA(nullptr, &cmd_line[0], nullptr, nullptr, FALSE, 0, nullptr, nullptr, &si,
                      &pi)) {
    skip_reason_ = "CreateProcess failed";
    return false;
  }

  CloseHandle(pi.hThread);  // We don't need the main thread handle
  process_handle_ = pi.hProcess;

  if (!WaitForReady(std::chrono::seconds{2})) {
    DWORD exit_code = 0;
    if (GetExitCodeProcess(process_handle_, &exit_code) && exit_code != STILL_ACTIVE) {
      skip_reason_ = "server exited with code " + std::to_string(exit_code);
    } else {
      skip_reason_ = "server not accepting connections within timeout";
      TerminateProcess(process_handle_, 1);
      WaitForSingleObject(process_handle_, INFINITE);
    }
    CloseHandle(process_handle_);
    process_handle_ = nullptr;
    return false;
  }
#else
  pid_t pid = fork();
  if (pid < 0) {
    skip_reason_ = "fork failed: " + std::string(std::strerror(errno));
    return false;
  }

  if (pid == 0) {
    std::string port_str = std::to_string(port_);
    std::string data_str = data_dir_.string();
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-vararg)
    execl(binary, "abyss-server", "--port", port_str.c_str(), "--data-dir", data_str.c_str(),
          nullptr);
    _exit(127);
  }

  pid_ = pid;

  if (!WaitForReady(std::chrono::seconds{2})) {
    int status = 0;
    pid_t wr = waitpid(pid_, &status, WNOHANG);
    if (wr > 0 && WIFEXITED(status)) {
      skip_reason_ = "server exited with code " + std::to_string(WEXITSTATUS(status));
    } else {
      skip_reason_ = "server not accepting connections within timeout";
      if (wr == 0) {
        kill(pid_, SIGKILL);
        waitpid(pid_, &status, 0);
      }
    }
    pid_ = -1;
    return false;
  }
#endif

  return true;
}

bool TestServer::WaitForReady(std::chrono::seconds timeout) const {
  auto deadline = std::chrono::steady_clock::now() + timeout;

  while (std::chrono::steady_clock::now() < deadline) {
    // Check if process crashed prematurely
#ifdef _WIN32
    DWORD exit_code = 0;
    if (GetExitCodeProcess(process_handle_, &exit_code) && exit_code != STILL_ACTIVE) {
      return false;
    }
#else
    int status = 0;
    pid_t result = waitpid(pid_, &status, WNOHANG);
    if (result > 0) {
      return false;
    }
#endif

    // Attempt TCP connection
    socket_t fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd != kInvalidSocket) {
      sockaddr_in addr{};
      addr.sin_family = AF_INET;
      addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
      addr.sin_port = htons(port_);

      if (connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) == 0) {
        CLOSE_SOCKET(fd);
        return true;
      }
      CLOSE_SOCKET(fd);
    }

    std::this_thread::sleep_for(std::chrono::milliseconds{25});
  }

  return false;
}

void TestServer::Stop() {
  if (!IsRunning()) return;
#ifdef _WIN32
  // NOTE(Callum): Windows lacks a SIGTERM equivalent for detached processes.
  // TerminateProcess is a hard kill.
  TerminateProcess(process_handle_, 0);
  WaitForSingleObject(process_handle_, INFINITE);
  CloseHandle(process_handle_);
  process_handle_ = nullptr;
#else
  kill(pid_, SIGTERM);
  int status = 0;
  waitpid(pid_, &status, 0);
  pid_ = -1;
#endif
}

void TestServer::Kill() {
  if (!IsRunning()) return;
#ifdef _WIN32
  TerminateProcess(process_handle_, 1);
  WaitForSingleObject(process_handle_, INFINITE);
  CloseHandle(process_handle_);
  process_handle_ = nullptr;
#else
  kill(pid_, SIGKILL);
  int status = 0;
  waitpid(pid_, &status, 0);
  pid_ = -1;
#endif
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
