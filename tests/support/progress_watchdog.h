#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdio>
#include <cstdlib>
#include <functional>
#include <iostream>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <thread>
#include <utility>

#ifndef _WIN32
#include <spawn.h>
#include <sys/wait.h>
#include <unistd.h>

#include <csignal>
#include <vector>
#endif
#ifdef __linux__
#include <sys/prctl.h>
#endif
#ifdef __APPLE__
#include <crt_externs.h>
#endif
#if !defined(__APPLE__) && !defined(_WIN32)
extern char** environ;
#endif

// TSan and ASan slow every step several times over.
#ifdef __has_feature
#if __has_feature(thread_sanitizer) || __has_feature(address_sanitizer)
#define ABYSS_TESTING_SLOWED_BY_SANITIZER
#endif
#endif
#if defined(__SANITIZE_THREAD__) || defined(__SANITIZE_ADDRESS__)
#define ABYSS_TESTING_SLOWED_BY_SANITIZER
#endif

namespace abyss::testing {

#ifdef ABYSS_TESTING_SLOWED_BY_SANITIZER
inline constexpr int kWatchdogScale = 3;
#else
inline constexpr int kWatchdogScale = 1;
#endif

// How long a long test may go without progress before it is stuck.
inline constexpr std::chrono::seconds kStallLimit{10 * kWatchdogScale};

// Detects a stall by progress, not by a test's total time: with no
// Beat for `limit`, it prints a WATCHDOG line naming the last place
// beaten, the test's state and every thread's stack, then aborts.
// The ctest TIMEOUT is only a backstop behind it.
class ProgressWatchdog {
 public:
  // The test's state at a stall, from the watchdog's thread: it must
  // not wait on what the stalled threads hold.
  using Dump = std::function<std::string()>;

  explicit ProgressWatchdog(std::string where, Dump dump = {},
                            std::chrono::seconds limit = kStallLimit)
      : limit_(limit),
        dump_(std::move(dump)),
        where_(std::move(where)),
        last_(Ticks()),
        thread_([this] { Watch(); }) {}

  ~ProgressWatchdog() {
    {
      const std::scoped_lock lock(mu_);
      stopped_ = true;
    }
    wake_.notify_all();
    thread_.join();
  }

  ProgressWatchdog(const ProgressWatchdog&) = delete;
  ProgressWatchdog& operator=(const ProgressWatchdog&) = delete;
  ProgressWatchdog(ProgressWatchdog&&) = delete;
  ProgressWatchdog& operator=(ProgressWatchdog&&) = delete;

  // Progress, at `where` (such as seed, step and phase).
  void Beat(std::string_view where) {
    {
      const std::scoped_lock lock(mu_);
      where_.assign(where);
    }
    Tick();
  }
  // Progress at the last place beaten: lock-free, for hot loops.
  void Tick() { last_.store(Ticks(), std::memory_order_relaxed); }

 private:
  using Clock = std::chrono::steady_clock;

  static Clock::rep Ticks() { return Clock::now().time_since_epoch().count(); }
  Clock::duration Since() const {
    return Clock::now() - Clock::time_point(Clock::duration(last_.load(std::memory_order_relaxed)));
  }

  void Watch() {
    std::unique_lock lock(mu_);
    for (;;) {
      if (wake_.wait_for(lock, limit_ - Since(), [this] { return stopped_; })) return;
      if (Since() >= limit_) break;
    }
    const std::string where = where_;
    const auto stalled = std::chrono::duration_cast<std::chrono::seconds>(Since());
    lock.unlock();
    Fire(where, stalled);
  }

  [[noreturn]] void Fire(const std::string& where, std::chrono::seconds stalled) {
    std::cout << std::flush;
    std::cerr << "WATCHDOG: no progress for " << stalled.count() << "s at " << where << '\n';
    if (dump_) DumpState();
    std::cout << std::flush;
    std::cerr << std::flush;
    std::fflush(nullptr);
    DumpStacks();
    std::fflush(nullptr);
#ifdef _WIN32
    std::_Exit(3);
#else
    std::abort();
#endif
  }

  // On its own thread, so a dump that blocks still lets the rest run.
  void DumpState() {
    struct Shared {
      std::mutex mu;
      std::condition_variable done_cv;
      bool done = false;
      std::string text;
    };
    auto shared = std::make_shared<Shared>();
    std::thread([shared, dump = dump_] {
      std::string text = dump();
      const std::scoped_lock lock(shared->mu);
      shared->text = std::move(text);
      shared->done = true;
      shared->done_cv.notify_all();
    }).detach();
    std::unique_lock lock(shared->mu);
    if (shared->done_cv.wait_for(lock, std::chrono::seconds{5 * kWatchdogScale},
                                 [&shared] { return shared->done; })) {
      std::cerr << "WATCHDOG state: " << shared->text << '\n';
    } else {
      std::cerr << "WATCHDOG state: the dump did not return" << '\n';
    }
  }

  // gdb on Linux, sample on macOS; skipped where neither runs.
  static void DumpStacks() {
#ifdef _WIN32
    std::cerr << "WATCHDOG stacks: not collected on Windows" << '\n';
#else
    std::string pid = std::to_string(::getpid());
#ifdef __linux__
    // Yama lets only an ancestor attach unless the tracee allows it.
    (void)::prctl(PR_SET_PTRACER, PR_SET_PTRACER_ANY, 0, 0, 0);
    std::string tool = "gdb";
    std::string batch = "-batch";
    std::string attach = "-p";
    std::string ex = "-ex";
    std::string all = "thread apply all bt";
    std::vector<char*> argv{tool.data(), batch.data(), attach.data(), pid.data(),
                            ex.data(),   all.data(),   nullptr};
#else
    std::string tool = "sample";
    std::string seconds = "3";
    std::vector<char*> argv{tool.data(), pid.data(), seconds.data(), nullptr};
#endif
#ifdef __APPLE__
    char** env = *_NSGetEnviron();
#else
    char** env = environ;
#endif
    pid_t child = -1;
    if (::posix_spawnp(&child, tool.c_str(), nullptr, nullptr, argv.data(), env) != 0) {
      std::cerr << "WATCHDOG stacks: " << tool << " did not start" << '\n';
      return;
    }
    const auto until = Clock::now() + std::chrono::seconds{120};
    int status = 0;
    while (::waitpid(child, &status, WNOHANG) == 0) {
      if (Clock::now() >= until) {
        (void)::kill(child, SIGKILL);
        (void)::waitpid(child, &status, 0);
        std::cerr << "WATCHDOG stacks: " << tool << " did not finish" << '\n';
        return;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds{100});
    }
#endif
  }

  std::chrono::seconds limit_;
  Dump dump_;
  std::mutex mu_;
  std::condition_variable wake_;
  std::string where_;
  std::atomic<Clock::rep> last_;
  bool stopped_ = false;
  std::thread thread_;
};

}  // namespace abyss::testing
