#include "temp_dir.h"

#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>
#include <thread>
#include <unordered_set>
#include <vector>

namespace abyss::testing {
namespace {

TEST(TempDirTest, CreatesUniquePathAndRemovesOnDestruction) {
  std::filesystem::path captured;
  {
    TempDir dir("example");
    captured = dir.Path();
    EXPECT_TRUE(std::filesystem::exists(captured));
    EXPECT_TRUE(std::filesystem::is_directory(captured));
  }
  EXPECT_FALSE(std::filesystem::exists(captured));
}

TEST(TempDirTest, ProducesDifferentPathsForEachInstance) {
  TempDir a("x");
  TempDir b("x");
  EXPECT_NE(a.Path(), b.Path());
}

TEST(TempDirTest, SubReturnsPathUnderBase) {
  TempDir dir("x");
  const auto sub = dir.Sub("child");
  EXPECT_EQ(sub.parent_path(), dir.Path());
  EXPECT_EQ(sub.filename(), "child");
}

TEST(TempDirTest, RemoveRecursivelyCleansNestedContents) {
  std::filesystem::path captured;
  {
    TempDir dir("nested");
    captured = dir.Path();

    std::filesystem::create_directories(dir.Sub("a/b/c"));
    std::ofstream(dir.Sub("a/b/c/file.txt")) << "payload";

    EXPECT_TRUE(std::filesystem::exists(dir.Sub("a/b/c/file.txt")));
  }
  EXPECT_FALSE(std::filesystem::exists(captured));
}

TEST(TempDirTest, ConcurrentConstructionProducesUniquePaths) {
  constexpr int kThreads = 8;
  constexpr int kPerThread = 16;

  std::vector<std::thread> threads;
  std::vector<std::vector<std::filesystem::path>> per_thread_paths(kThreads);
  threads.reserve(kThreads);

  for (int t = 0; t < kThreads; ++t) {
    threads.emplace_back([&, t] {
      per_thread_paths[t].reserve(kPerThread);
      for (int i = 0; i < kPerThread; ++i) {
        TempDir dir("race");
        per_thread_paths[t].push_back(dir.Path());
      }
    });
  }
  for (auto& th : threads) th.join();

  std::unordered_set<std::string> seen;
  for (const auto& paths : per_thread_paths) {
    for (const auto& p : paths) {
      EXPECT_TRUE(seen.insert(p.string()).second) << "duplicate: " << p;
    }
  }
}

}  // namespace
}  // namespace abyss::testing
