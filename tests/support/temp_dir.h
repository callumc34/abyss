#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>

#ifdef _WIN32
#include <process.h>
#define ABYSS_TEST_GETPID _getpid
#else
#include <unistd.h>
#define ABYSS_TEST_GETPID getpid
#endif

namespace abyss::testing {

// Unique temp directory created on construction and recursively removed on
// destruction. Name includes pid + counter + nanosecond timestamp so parallel
// tests in the same binary and across processes never collide. Constructor
// throws on create failure.
class TempDir {
 public:
  explicit TempDir(std::string_view label = "test") {
    static std::atomic<uint64_t> counter{0};
    const auto id = counter.fetch_add(1, std::memory_order_relaxed);
    const auto now = std::chrono::steady_clock::now().time_since_epoch().count();

    std::string name = "abyss_";
    name += label;
    name += "_";
    name += std::to_string(ABYSS_TEST_GETPID());
    name += "_";
    name += std::to_string(id);
    name += "_";
    name += std::to_string(now);

    path_ = std::filesystem::temp_directory_path() / name;

    std::error_code ec;
    std::filesystem::create_directories(path_, ec);
    if (ec) {
      throw std::runtime_error("TempDir: create_directories(" + path_.string() +
                               ") failed: " + ec.message());
    }
  }

  ~TempDir() {
    std::error_code ec;
    std::filesystem::remove_all(path_, ec);
  }

  TempDir(const TempDir&) = delete;
  TempDir& operator=(const TempDir&) = delete;
  TempDir(TempDir&&) = delete;
  TempDir& operator=(TempDir&&) = delete;

  const std::filesystem::path& Path() const { return path_; }

  std::filesystem::path Sub(std::string_view name) const { return path_ / name; }

  std::string String() const { return path_.string(); }

 private:
  std::filesystem::path path_;
};

}  // namespace abyss::testing

#undef ABYSS_TEST_GETPID
