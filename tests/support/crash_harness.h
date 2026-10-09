#pragma once

// Out-of-process crash victim for durability tests.
//
// A crash test must not fork() from the gtest process: our components run
// background threads (the WAL group committer, cold consumers, RocksDB), and
// only the forking thread survives into the child, so any mutex those threads
// held at fork time is permanently locked there. That is undefined behaviour,
// and it makes a durability assertion nondeterministic rather than merely
// wrong -- the worst failure mode for a test whose job is to prove durability.
//
// Instead the parent re-execs THIS test binary filtered to a victim test. The
// victim does the writes, publishes a ready marker, and parks with everything
// still open; the parent then SIGKILLs it. A clean exit would run destructors
// and flush buffers, which is precisely the state the test must never observe.
//
// POSIX only. Windows has no equivalent here; callers skip.

#include <chrono>
#include <filesystem>
#include <string>
#include <string_view>

#ifndef _WIN32

#include <spawn.h>
#include <sys/wait.h>
#include <unistd.h>

#include <csignal>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <optional>
#include <sstream>
#include <system_error>
#include <thread>
#include <vector>

#ifdef __APPLE__
#include <crt_externs.h>
#include <mach-o/dyld.h>
#else
extern char** environ;
#endif

#endif  // !_WIN32

namespace abyss::testing {

#ifndef _WIN32

struct VictimSpec {
  // gtest filter selecting the victim test inside this same binary.
  std::string gtest_filter;
  // Environment variable carrying the victim's working directory. Its presence
  // is also how the victim test knows it is the victim rather than an ordinary
  // run of the suite.
  std::string dir_env_var;
  std::filesystem::path dir;
  // Written by the victim, relative to `dir`, once it is ready to be killed.
  std::string ready_file_name = "victim.ready";
  std::chrono::milliseconds ready_deadline{30000};
};

struct CrashOutcome {
  bool reached_ready = false;
  bool died_by_signal = false;
  int term_signal = 0;
  // Contents of the ready file: the victim's channel back to the parent.
  std::string ready_payload;
  // Non-empty if the harness itself failed, as distinct from the victim
  // behaving unexpectedly. Assert on this first.
  std::string error;
};

namespace crash_internal {

inline constexpr std::chrono::milliseconds kReadyPollInterval{10};

inline char** CurrentEnviron() {
#ifdef __APPLE__
  return *_NSGetEnviron();
#else
  return environ;
#endif
}

inline std::optional<std::filesystem::path> SelfExecutablePath() {
  std::filesystem::path raw;
#ifdef __APPLE__
  constexpr size_t kMaxPathBytes = 4096;
  std::string buf(kMaxPathBytes, '\0');
  auto size = static_cast<uint32_t>(buf.size());
  if (_NSGetExecutablePath(buf.data(), &size) != 0) return std::nullopt;
  buf.resize(std::strlen(buf.c_str()));
  raw = buf;
#else
  std::error_code link_ec;
  raw = std::filesystem::read_symlink("/proc/self/exe", link_ec);
  if (link_ec) return std::nullopt;
#endif
  std::error_code ec;
  auto resolved = std::filesystem::weakly_canonical(raw, ec);
  return ec ? raw : resolved;
}

inline std::string ReadFileOrEmpty(const std::filesystem::path& path) {
  std::ifstream in(path, std::ios::binary);
  if (!in) return {};
  std::ostringstream ss;
  ss << in.rdbuf();
  return ss.str();
}

}  // namespace crash_internal

// Spawns the victim, waits for its ready marker, then SIGKILLs and reaps it.
inline CrashOutcome SpawnAndKillVictim(const VictimSpec& spec) {
  CrashOutcome outcome;

  const auto exe = crash_internal::SelfExecutablePath();
  if (!exe.has_value()) {
    outcome.error = "could not resolve this test binary's path";
    return outcome;
  }

  std::string exe_arg = exe->string();
  std::string filter_arg = "--gtest_filter=" + spec.gtest_filter;
  std::vector<char*> argv{exe_arg.data(), filter_arg.data(), nullptr};

  // Drop inherited GTEST_* variables: a filter, shard index or output path from
  // the parent run would otherwise silently turn the victim into a no-op, and
  // the test would "pass" having killed a process that did nothing.
  std::vector<std::string> env_storage;
  for (char** e = crash_internal::CurrentEnviron(); e != nullptr && *e != nullptr; ++e) {
    const std::string_view entry{*e};
    if (entry.starts_with("GTEST_")) continue;
    env_storage.emplace_back(entry);
  }
  env_storage.emplace_back(spec.dir_env_var + "=" + spec.dir.string());

  std::vector<char*> envp;
  envp.reserve(env_storage.size() + 1);
  for (auto& entry : env_storage) envp.push_back(entry.data());
  envp.push_back(nullptr);

  pid_t pid = -1;
  if (::posix_spawn(&pid, exe_arg.c_str(), nullptr, nullptr, argv.data(), envp.data()) != 0) {
    outcome.error = "posix_spawn of the crash victim failed";
    return outcome;
  }

  const auto ready_path = spec.dir / spec.ready_file_name;
  const auto deadline = std::chrono::steady_clock::now() + spec.ready_deadline;
  while (std::chrono::steady_clock::now() < deadline) {
    if (std::filesystem::exists(ready_path)) {
      outcome.reached_ready = true;
      break;
    }
    int early_status = 0;
    if (::waitpid(pid, &early_status, WNOHANG) == pid) {
      outcome.error = "crash victim exited before signalling ready (raw status " +
                      std::to_string(early_status) + ")";
      return outcome;
    }
    std::this_thread::sleep_for(crash_internal::kReadyPollInterval);
  }

  if (::kill(pid, SIGKILL) != 0) {
    outcome.error = "failed to SIGKILL the crash victim";
    return outcome;
  }
  int status = 0;
  if (::waitpid(pid, &status, 0) != pid) {
    outcome.error = "failed to reap the crash victim";
    return outcome;
  }

  outcome.died_by_signal = WIFSIGNALED(status) != 0;
  if (outcome.died_by_signal) outcome.term_signal = WTERMSIG(status);
  if (outcome.reached_ready) {
    outcome.ready_payload = crash_internal::ReadFileOrEmpty(ready_path);
  }
  return outcome;
}

// Victim side: returns the working directory when this process was spawned as
// the victim; `is_victim` is false during an ordinary run of the suite.
inline std::filesystem::path VictimDirFromEnv(std::string_view env_var, bool* is_victim) {
  const std::string var{env_var};
  // NOLINTNEXTLINE(concurrency-mt-unsafe) -- single-threaded at victim entry.
  const char* dir = std::getenv(var.c_str());
  if (is_victim != nullptr) *is_victim = dir != nullptr;
  if (dir == nullptr) return {};
  return std::filesystem::path{dir};
}

// Victim side: publishes `payload` atomically (tmp + rename, so the parent can
// never observe a half-written marker). From this moment the victim may be
// killed at any instant, which is the point: a victim that keeps working after
// signalling gets crashed MID-operation rather than at a quiescent point.
inline void SignalReady(const std::filesystem::path& dir, std::string_view ready_file_name,
                        std::string_view payload) {
  const auto final_path = dir / ready_file_name;
  const auto tmp_path = dir / (std::string(ready_file_name) + ".tmp");
  {
    std::ofstream out(tmp_path, std::ios::binary | std::ios::trunc);
    out << payload;
    out.flush();
  }
  std::error_code ec;
  std::filesystem::rename(tmp_path, final_path, ec);
}

// Victim side: exits when its parent does, as when the parent dies
// before it can kill it, or after `limit`, so no victim outlives its
// test.
inline void ExitWithParent(std::chrono::seconds limit) {
  const pid_t parent = ::getppid();
  std::thread([parent] {
    while (::getppid() == parent) std::this_thread::sleep_for(std::chrono::milliseconds(100));
    std::_Exit(1);
  }).detach();
  ::alarm(static_cast<unsigned>(limit.count()));
}

// Victim side: signal, then park forever. For victims whose interesting state
// is what is sitting UNFLUSHED in memory at the moment of the kill -- running
// destructors here would flush the very buffers whose loss is under test.
// Victims that instead need to be crashed mid-write should call SignalReady and
// keep working.
[[noreturn]] inline void SignalReadyAndPark(const std::filesystem::path& dir,
                                            std::string_view ready_file_name,
                                            std::string_view payload) {
  SignalReady(dir, ready_file_name, payload);
  for (;;) {
    ::pause();
  }
}

#endif  // !_WIN32

}  // namespace abyss::testing
