#include "abyss/core/atomic_file.h"

#include <gtest/gtest.h>

#include <cstddef>
#include <filesystem>
#include <fstream>
#include <string>

#include "temp_dir.h"

namespace abyss::core {
namespace {

std::string ReadFile(const std::filesystem::path& p) {
  std::ifstream in(p, std::ios::binary);
  std::string out{std::istreambuf_iterator<char>(in), {}};
  return out;
}

TEST(AtomicFileTest, WritesNewFile) {
  testing::TempDir dir("atomic_file");
  const auto path = dir.Sub("a.txt");
  ASSERT_TRUE(WriteFileAtomic(path, std::string_view{"hello"}).has_value());
  EXPECT_TRUE(std::filesystem::exists(path));
  EXPECT_EQ(ReadFile(path), "hello");
}

TEST(AtomicFileTest, ReplacesExistingAtomically) {
  testing::TempDir dir("atomic_file_replace");
  const auto path = dir.Sub("a.txt");
  ASSERT_TRUE(WriteFileAtomic(path, std::string_view{"first"}).has_value());
  ASSERT_TRUE(WriteFileAtomic(path, std::string_view{"second"}).has_value());
  EXPECT_EQ(ReadFile(path), "second");
}

TEST(AtomicFileTest, CreatesParentDirectories) {
  testing::TempDir dir("atomic_file_parent");
  const auto path = dir.Sub("nested/deep/leaf.bin");
  ASSERT_TRUE(WriteFileAtomic(path, std::string_view{"x"}).has_value());
  EXPECT_TRUE(std::filesystem::exists(path));
  EXPECT_EQ(ReadFile(path), "x");
}

TEST(AtomicFileTest, NoTempLeakOnSuccess) {
  testing::TempDir dir("atomic_file_no_leak");
  const auto path = dir.Sub("payload.bin");
  ASSERT_TRUE(WriteFileAtomic(path, std::string_view{"ok"}).has_value());
  size_t entries = 0;
  for (const auto& e : std::filesystem::directory_iterator(dir.Path())) {
    (void)e;
    ++entries;
  }
  EXPECT_EQ(entries, 1U);
}

TEST(AtomicFileTest, BinarySpanRoundTrip) {
  testing::TempDir dir("atomic_file_binary");
  const auto path = dir.Sub("blob");
  const std::array<std::byte, 4> bytes{std::byte{0x00}, std::byte{0xFF}, std::byte{0x10},
                                       std::byte{0xAA}};
  ASSERT_TRUE(WriteFileAtomic(path, std::span<const std::byte>(bytes)).has_value());
  const auto got = ReadFile(path);
  ASSERT_EQ(got.size(), 4U);
  EXPECT_EQ(static_cast<unsigned char>(got[0]), 0x00U);
  EXPECT_EQ(static_cast<unsigned char>(got[1]), 0xFFU);
  EXPECT_EQ(static_cast<unsigned char>(got[2]), 0x10U);
  EXPECT_EQ(static_cast<unsigned char>(got[3]), 0xAAU);
}

}  // namespace
}  // namespace abyss::core
