#include "abyss/platform/mapped_file.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <utility>
#include <vector>

#include "abyss/platform/fs.h"
#include "temp_dir.h"

namespace abyss::platform::fs {
namespace {

using abyss::testing::TempDir;

constexpr std::size_t kMiB = std::size_t{1} << 20;

File OpenNew(const TempDir& dir, const char* name) {
  auto file = Open(dir.Sub(name), OpenOptions{.mode = OpenMode::kReadWrite, .create = true});
  EXPECT_TRUE(file.has_value()) << file.error().message();
  return file.has_value() ? std::move(*file) : File{};
}

bool AllZero(const File& file, std::uint64_t offset, std::size_t length) {
  std::vector<std::byte> buf(length, std::byte{0xFF});
  auto n = Pread(file, buf.data(), length, offset);
  return n.has_value() && *n == length &&
         std::ranges::all_of(buf, [](std::byte b) { return b == std::byte{0}; });
}

TEST(ZeroFillTest, WritesZerosToExactlyTheRequestedSize) {
  const TempDir dir("zerofill");
  const File file = OpenNew(dir, "seg");
  const std::uint64_t size = (3 * kMiB) + 123;
  ASSERT_TRUE(ZeroFill(file, size).has_value());

  auto file_size = FileSize(file);
  ASSERT_TRUE(file_size.has_value());
  EXPECT_EQ(*file_size, size);
  EXPECT_TRUE(AllZero(file, 0, 4096));
  EXPECT_TRUE(AllZero(file, (2 * kMiB) - 7, 4096));
  EXPECT_TRUE(AllZero(file, size - 123, 123));
}

TEST(ZeroFillTest, OverwritesExistingBytesAndTrimsALongerFile) {
  const TempDir dir("zerofill");
  const File file = OpenNew(dir, "seg");
  const std::vector<std::byte> junk(2 * kMiB, std::byte{0xAB});
  ASSERT_TRUE(Pwrite(file, junk.data(), junk.size(), 0).has_value());

  ASSERT_TRUE(ZeroFill(file, kMiB).has_value());
  auto file_size = FileSize(file);
  ASSERT_TRUE(file_size.has_value());
  EXPECT_EQ(*file_size, kMiB);
  EXPECT_TRUE(AllZero(file, 0, kMiB));
}

TEST(MappedFileTest, WritesThroughTheMappingReachTheFile) {
  const TempDir dir("mapped");
  const std::array<char, 5> kHello{'h', 'e', 'l', 'l', 'o'};
  {
    const File file = OpenNew(dir, "seg");
    ASSERT_TRUE(ZeroFill(file, kMiB).has_value());
    auto map = MappedFile::Map(file, kMiB);
    ASSERT_TRUE(map.has_value()) << map.error().message();
    ASSERT_EQ(map->size(), kMiB);
    std::memcpy(map->data(), kHello.data(), kHello.size());
    std::memcpy(map->data() + 700000, kHello.data(), kHello.size());
    ASSERT_TRUE(map->WriteBack(0, kMiB).has_value());
    ASSERT_TRUE(Fsync(file, SyncMode::kDurableData).has_value());
  }

  auto reopened = Open(dir.Sub("seg"), OpenOptions{.mode = OpenMode::kRead});
  ASSERT_TRUE(reopened.has_value());
  for (const std::uint64_t offset : {std::uint64_t{0}, std::uint64_t{700000}}) {
    std::array<char, 5> got{};
    auto n = Pread(*reopened, got.data(), got.size(), offset);
    ASSERT_TRUE(n.has_value());
    EXPECT_EQ(got, kHello) << "offset " << offset;
  }
  EXPECT_TRUE(AllZero(*reopened, 5, 4096));
}

TEST(MappedFileTest, RefusesAMappingLargerThanTheFile) {
  const TempDir dir("mapped");
  const File file = OpenNew(dir, "seg");
  ASSERT_TRUE(ZeroFill(file, kMiB).has_value());

  auto map = MappedFile::Map(file, 2 * kMiB);
  ASSERT_FALSE(map.has_value());
  EXPECT_EQ(map.error().code(), core::ErrorCode::kInvalidArgument);
  EXPECT_FALSE(MappedFile::Map(file, 0).has_value());
}

TEST(MappedFileTest, WriteBackRejectsARangeOutsideTheMapping) {
  const TempDir dir("mapped");
  const File file = OpenNew(dir, "seg");
  ASSERT_TRUE(ZeroFill(file, kMiB).has_value());
  auto map = MappedFile::Map(file, kMiB);
  ASSERT_TRUE(map.has_value());

  EXPECT_TRUE(map->WriteBack(kMiB, 0).has_value());
  EXPECT_FALSE(map->WriteBack(kMiB - 1, 2).has_value());
  EXPECT_FALSE(map->WriteBack(kMiB + 1, 0).has_value());
}

TEST(MappedFileTest, MoveTransfersTheMapping) {
  const TempDir dir("mapped");
  const File file = OpenNew(dir, "seg");
  ASSERT_TRUE(ZeroFill(file, kMiB).has_value());
  auto map = MappedFile::Map(file, kMiB);
  ASSERT_TRUE(map.has_value());
  std::byte* const data = map->data();

  MappedFile moved(std::move(*map));
  EXPECT_FALSE(map->valid());  // NOLINT(bugprone-use-after-move,clang-analyzer-cplusplus.Move)
  EXPECT_EQ(moved.data(), data);
  moved.data()[0] = std::byte{1};

  MappedFile assigned;
  assigned = std::move(moved);
  EXPECT_FALSE(moved.valid());  // NOLINT(bugprone-use-after-move,clang-analyzer-cplusplus.Move)
  EXPECT_EQ(assigned.data()[0], std::byte{1});

  assigned.Unmap();
  EXPECT_FALSE(assigned.valid());
  EXPECT_EQ(assigned.size(), 0U);
}

}  // namespace
}  // namespace abyss::platform::fs
