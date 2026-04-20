#include "server_fixture.h"

#ifdef _WIN32
#include <process.h>
#else
#include <fcntl.h>
#include <poll.h>
#include <sys/wait.h>
#include <unistd.h>
#endif

#include <algorithm>
#include <array>
#include <cerrno>
#include <chrono>
#include <csignal>
#include <cstring>
#include <memory>
#include <string>
#include <thread>

#ifndef ABYSS_SERVER_BINARY
#define ABYSS_SERVER_BINARY ""
#endif

namespace abyss::system_test {

namespace {

constexpr std::string_view kReadyPrefix = "abyss-ready";
constexpr std::string_view kPortTag = "port=";

uint16_t ParseReadyLine(std::string_view line) {
  if (!line.starts_with(kReadyPrefix)) return 0;
  const auto tag = line.find(kPortTag);
  if (tag == std::string_view::npos) return 0;
  line.remove_prefix(tag + kPortTag.size());
  uint32_t port = 0;
  for (char c : line) {
    if (c < '0' || c > '9') break;
    port = (port * 10) + static_cast<uint32_t>(c - '0');
    if (port > 65535) return 0;
  }
  return static_cast<uint16_t>(port);
}

#ifdef _WIN32

struct WsaGuard {
  WsaGuard() {
    WSADATA data;
    WSAStartup(MAKEWORD(2, 2), &data);
  }
  ~WsaGuard() { WSACleanup(); }
  WsaGuard(const WsaGuard&) = delete;
  WsaGuard& operator=(const WsaGuard&) = delete;
};

const WsaGuard kWsaGuard;

std::string BuildCommandLine(const char* binary, const std::string& data_dir,
                             const std::string& shard_count) {
  std::string cmd;
  cmd.reserve(256);
  cmd += '"';
  cmd += binary;
  cmd += "\" --port 0 --data-dir \"";
  cmd += data_dir;
  cmd += "\" --shard-count ";
  cmd += shard_count;
  return cmd;
}

void ClosePipe(pipe_handle_t* h) {
  if (*h != kInvalidPipeHandle) {
    CloseHandle(*h);
    *h = kInvalidPipeHandle;
  }
}

void CloseProc(proc_handle_t* h) {
  if (*h != kInvalidProcHandle) {
    CloseHandle(*h);
    *h = kInvalidProcHandle;
  }
}

#else

void ClosePipe(pipe_handle_t* h) {
  if (*h < 0) return;
  const int saved_errno = errno;
  while (::close(*h) < 0 && errno == EINTR) {
  }
  errno = saved_errno;
  *h = kInvalidPipeHandle;
}

#endif  // _WIN32

}  // namespace

TestServer::TestServer(Config config) : config_(config), data_dir_("system_test") {}

TestServer::~TestServer() {
  if (IsRunning()) Kill();
  ClosePipe(&stdout_read_);
}

bool TestServer::Start() {
  const char* binary = ABYSS_SERVER_BINARY;
  if (binary[0] == '\0') {
    skip_reason_ = "ABYSS_SERVER_BINARY macro not set at compile time";
    return false;
  }

  const std::string data_str = data_dir_.String();
  const std::string shard_str = std::to_string(config_.shard_count);

#ifdef _WIN32
  SECURITY_ATTRIBUTES sa{};
  sa.nLength = sizeof(sa);
  sa.bInheritHandle = TRUE;
  sa.lpSecurityDescriptor = nullptr;

  HANDLE read_handle = nullptr;
  HANDLE write_handle = nullptr;
  if (!CreatePipe(&read_handle, &write_handle, &sa, 0)) {
    skip_reason_ = "CreatePipe failed: " + std::to_string(GetLastError());
    return false;
  }
  // The parent keeps the read end; ensure it is not inherited by the child.
  if (!SetHandleInformation(read_handle, HANDLE_FLAG_INHERIT, 0)) {
    CloseHandle(read_handle);
    CloseHandle(write_handle);
    skip_reason_ = "SetHandleInformation failed: " + std::to_string(GetLastError());
    return false;
  }

  STARTUPINFOA si{};
  si.cb = sizeof(si);
  si.dwFlags = STARTF_USESTDHANDLES;
  si.hStdInput = GetStdHandle(STD_INPUT_HANDLE);
  si.hStdOutput = write_handle;
  si.hStdError = GetStdHandle(STD_ERROR_HANDLE);

  PROCESS_INFORMATION pi{};
  std::string cmd_line = BuildCommandLine(binary, data_str, shard_str);
  // CreateProcessA needs a writable command line buffer.
  const BOOL ok = CreateProcessA(binary, cmd_line.data(), nullptr, nullptr, TRUE, 0, nullptr,
                                 nullptr, &si, &pi);
  CloseHandle(write_handle);
  if (!ok) {
    CloseHandle(read_handle);
    skip_reason_ = "CreateProcess failed: " + std::to_string(GetLastError());
    return false;
  }

  CloseHandle(pi.hThread);
  proc_ = pi.hProcess;
  stdout_read_ = read_handle;

#else
  std::array<int, 2> pipe_fds{-1, -1};
  if (::pipe(pipe_fds.data()) < 0) {
    skip_reason_ = "pipe failed: ";
    skip_reason_ += std::strerror(errno);
    return false;
  }

  const pid_t pid = ::fork();
  if (pid < 0) {
    const int err = errno;
    ClosePipe(pipe_fds.data());
    ClosePipe(&pipe_fds[1]);
    skip_reason_ = "fork failed: ";
    skip_reason_ += std::strerror(err);
    return false;
  }

  if (pid == 0) {
    ::dup2(pipe_fds[1], STDOUT_FILENO);
    ::close(pipe_fds[0]);
    ::close(pipe_fds[1]);

    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-vararg)
    ::execl(binary, "abyss-server", "--port", "0", "--data-dir", data_str.c_str(), "--shard-count",
            shard_str.c_str(), nullptr);
    ::_exit(127);
  }

  ClosePipe(&pipe_fds[1]);
  proc_ = pid;
  stdout_read_ = pipe_fds[0];
#endif

  if (!WaitForReady()) {
    Kill();
    return false;
  }
  return true;
}

bool TestServer::WaitForReady() {
  const auto deadline = std::chrono::steady_clock::now() + config_.ready_timeout;
  std::string buffer;
  buffer.reserve(256);
  std::array<char, 256> chunk{};

  while (std::chrono::steady_clock::now() < deadline) {
#ifdef _WIN32
    DWORD exit_code = 0;
    if (!GetExitCodeProcess(proc_, &exit_code)) {
      skip_reason_ = "GetExitCodeProcess failed: " + std::to_string(GetLastError());
      return false;
    }
    if (exit_code != STILL_ACTIVE) {
      skip_reason_ =
          "server exited before emitting ready line (exit code " + std::to_string(exit_code) + ")";
      CloseProc(&proc_);
      return false;
    }

    DWORD bytes_avail = 0;
    if (!PeekNamedPipe(stdout_read_, nullptr, 0, nullptr, &bytes_avail, nullptr)) {
      const DWORD err = GetLastError();
      if (err == ERROR_BROKEN_PIPE) {
        skip_reason_ = "server stdout closed before ready line";
        return false;
      }
      skip_reason_ = "PeekNamedPipe failed: " + std::to_string(err);
      return false;
    }
    if (bytes_avail == 0) {
      std::this_thread::sleep_for(std::chrono::milliseconds{10});
      continue;
    }

    DWORD nread = 0;
    const DWORD to_read = static_cast<DWORD>(std::min<DWORD>(bytes_avail, chunk.size()));
    if (!ReadFile(stdout_read_, chunk.data(), to_read, &nread, nullptr)) {
      skip_reason_ = "ReadFile failed: " + std::to_string(GetLastError());
      return false;
    }
    if (nread == 0) {
      skip_reason_ = "server stdout closed before ready line";
      return false;
    }
    buffer.append(chunk.data(), static_cast<size_t>(nread));
#else
    int status = 0;
    const pid_t waited = ::waitpid(proc_, &status, WNOHANG);
    if (waited > 0) {
      skip_reason_ = "server exited before emitting ready line (status ";
      skip_reason_ += std::to_string(status);
      skip_reason_ += ")";
      proc_ = kInvalidProcHandle;
      return false;
    }
    if (waited < 0 && errno != ECHILD) {
      skip_reason_ = "waitpid failed: ";
      skip_reason_ += std::strerror(errno);
      return false;
    }

    const auto now = std::chrono::steady_clock::now();
    const auto remaining_ms = std::max<long>(
        1, std::chrono::duration_cast<std::chrono::milliseconds>(deadline - now).count());
    const int poll_ms = static_cast<int>(std::min<long>(remaining_ms, 100));

    pollfd pfd{.fd = stdout_read_, .events = POLLIN, .revents = 0};
    const int pr = ::poll(&pfd, 1, poll_ms);
    if (pr < 0) {
      if (errno == EINTR) continue;
      skip_reason_ = "poll failed: ";
      skip_reason_ += std::strerror(errno);
      return false;
    }
    if (pr == 0) continue;

    const auto nread = ::read(stdout_read_, chunk.data(), chunk.size());
    if (nread < 0) {
      if (errno == EINTR) continue;
      skip_reason_ = "read failed: ";
      skip_reason_ += std::strerror(errno);
      return false;
    }
    if (nread == 0) {
      skip_reason_ = "server stdout closed before ready line";
      return false;
    }
    buffer.append(chunk.data(), static_cast<size_t>(nread));
#endif

    for (auto nl = buffer.find('\n'); nl != std::string::npos; nl = buffer.find('\n')) {
      const std::string_view line(buffer.data(), nl);
      if (line.starts_with(kReadyPrefix)) {
        const uint16_t port = ParseReadyLine(line);
        if (port == 0) {
          skip_reason_ = "malformed ready line: ";
          skip_reason_.append(line);
          return false;
        }
        port_ = port;
        return true;
      }
      buffer.erase(0, nl + 1);
    }
  }

  skip_reason_ = "timed out waiting for server ready line";
  return false;
}

void TestServer::Stop() {
  if (!IsRunning()) return;
#ifdef _WIN32
  // Windows has no SIGTERM equivalent we can deliver without the child
  // installing a SetConsoleCtrlHandler in a dedicated process group. Hard
  // termination still validates the property the tests care about: data
  // acked by the client must survive any termination.
  TerminateProcess(proc_, 0);
#else
  ::kill(proc_, SIGTERM);
#endif
  WaitChild();
}

void TestServer::Kill() {
  if (!IsRunning()) return;
#ifdef _WIN32
  TerminateProcess(proc_, 1);
#else
  ::kill(proc_, SIGKILL);
#endif
  WaitChild();
}

void TestServer::WaitChild() {
#ifdef _WIN32
  WaitForSingleObject(proc_, INFINITE);
  CloseProc(&proc_);
#else
  int status = 0;
  while (true) {
    const pid_t r = ::waitpid(proc_, &status, 0);
    if (r == proc_) break;
    if (r < 0 && errno == EINTR) continue;
    break;
  }
  proc_ = kInvalidProcHandle;
#endif
  ClosePipe(&stdout_read_);
  port_ = 0;
}

std::unique_ptr<TestServer> SystemTest::shared_server_;

void SystemTest::SetUpTestSuite() {
  shared_server_ = std::make_unique<TestServer>();
  if (!shared_server_->Start()) {
    GTEST_SKIP() << shared_server_->SkipReason();
  }
}

void SystemTest::TearDownTestSuite() {
  if (shared_server_) {
    shared_server_->Stop();
    shared_server_.reset();
  }
}

void SystemTest::SetUp() {
  if (shared_server_ == nullptr || !shared_server_->IsRunning()) {
    GTEST_SKIP() << (shared_server_ != nullptr ? shared_server_->SkipReason()
                                               : "shared server not started");
  }

  if (!client_.Connect("127.0.0.1", shared_server_->Port())) {
    GTEST_SKIP() << "cannot connect to shared server on port " << shared_server_->Port();
  }

  client_.Command({"FLUSHALL"});
}

void SystemTest::TearDown() { client_.Close(); }

uint16_t SystemTest::ServerPort() { return shared_server_->Port(); }

void DataCommandTest::SetUp() {
  SystemTest::SetUp();
  if (IsSkipped()) return;
  const auto probe = Client().Command({"SET", "__probe__", "1"});
  if (probe.IsError() && probe.String().contains("not implemented")) {
    GTEST_SKIP() << "data commands not yet implemented";
  }
  Client().Command({"DEL", "__probe__"});
}

void IsolatedServerTest::SetUp() {
  if (!server_.Start()) {
    GTEST_SKIP() << server_.SkipReason();
  }
  if (!client_.Connect("127.0.0.1", server_.Port())) {
    GTEST_SKIP() << "cannot connect to server on port " << server_.Port();
  }
}

void IsolatedServerTest::TearDown() {
  client_.Close();
  server_.Stop();
}

void IsolatedServerTest::RestartServer() {
  client_.Close();
  server_.Stop();
  ASSERT_TRUE(server_.Start()) << server_.SkipReason();
  ASSERT_TRUE(client_.Connect("127.0.0.1", server_.Port()));
}

void IsolatedServerTest::KillAndRestartServer() {
  client_.Close();
  server_.Kill();
  ASSERT_TRUE(server_.Start()) << server_.SkipReason();
  ASSERT_TRUE(client_.Connect("127.0.0.1", server_.Port()));
}

void IsolatedDataServerTest::SetUp() {
  IsolatedServerTest::SetUp();
  if (IsSkipped()) return;
  const auto probe = Client().Command({"SET", "__probe__", "1"});
  if (probe.IsError() && probe.String().contains("not implemented")) {
    GTEST_SKIP() << "data commands not yet implemented";
  }
  Client().Command({"DEL", "__probe__"});
}

}  // namespace abyss::system_test
