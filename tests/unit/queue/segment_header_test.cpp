#include "abyss/queue/segment_header.h"

#include <gtest/gtest.h>

#include <chrono>
#include <cstddef>
#include <vector>

#include "abyss/queue/wal_entry.h"

namespace abyss::queue {
namespace {

SegmentHeader MakeHeader() {
  SegmentHeader h;
  h.format_major = kWalFormatMajor;
  h.format_minor = kWalFormatMinor;
  h.flags = 0;
  h.shard_id = 7;
  h.base_seq = 1000;
  h.created_at = core::WallClock::now();
  return h;
}

TEST(SegmentHeaderTest, RoundTrip) {
  auto header = MakeHeader();

  std::vector<std::byte> buf;
  EncodeSegmentHeader(header, buf);
  ASSERT_EQ(buf.size(), kSegmentHeaderSize);

  auto decoded = DecodeSegmentHeader(buf);
  ASSERT_TRUE(decoded.has_value());
  EXPECT_EQ(decoded->format_major, header.format_major);
  EXPECT_EQ(decoded->format_minor, header.format_minor);
  EXPECT_EQ(decoded->flags, header.flags);
  EXPECT_EQ(decoded->shard_id, header.shard_id);
  EXPECT_EQ(decoded->base_seq, header.base_seq);

  const auto a =
      std::chrono::duration_cast<std::chrono::microseconds>(decoded->created_at.time_since_epoch())
          .count();
  const auto b =
      std::chrono::duration_cast<std::chrono::microseconds>(header.created_at.time_since_epoch())
          .count();
  EXPECT_EQ(a, b);
}

TEST(SegmentHeaderTest, FixedSize) {
  auto header = MakeHeader();
  std::vector<std::byte> buf;
  EncodeSegmentHeader(header, buf);
  EXPECT_EQ(buf.size(), 32u);
}

TEST(SegmentHeaderTest, RejectsTruncated) {
  std::vector<std::byte> buf(kSegmentHeaderSize - 1);
  auto decoded = DecodeSegmentHeader(buf);
  ASSERT_FALSE(decoded.has_value());
  EXPECT_EQ(decoded.error().code(), core::ErrorCode::kCorruption);
}

TEST(SegmentHeaderTest, RejectsBadMagic) {
  auto header = MakeHeader();
  std::vector<std::byte> buf;
  EncodeSegmentHeader(header, buf);

  buf[0] = std::byte{0x00};  // corrupt magic

  auto decoded = DecodeSegmentHeader(buf);
  ASSERT_FALSE(decoded.has_value());
}

TEST(SegmentHeaderTest, RejectsBadCrc) {
  auto header = MakeHeader();
  std::vector<std::byte> buf;
  EncodeSegmentHeader(header, buf);

  // Flip a byte inside the header body (after magic, before CRC).
  buf[8] = static_cast<std::byte>(static_cast<uint8_t>(buf[8]) ^ 0x01);

  auto decoded = DecodeSegmentHeader(buf);
  ASSERT_FALSE(decoded.has_value());
}

TEST(SegmentHeaderTest, RejectsUnsupportedMajor) {
  auto header = MakeHeader();
  header.format_major = kWalFormatMajor + 1;

  std::vector<std::byte> buf;
  EncodeSegmentHeader(header, buf);

  auto decoded = DecodeSegmentHeader(buf);
  ASSERT_FALSE(decoded.has_value());
}

TEST(SegmentHeaderTest, DecodesNewerMinor) {
  auto header = MakeHeader();
  header.format_minor = kWalFormatMinor + 5;  // pretend to be a future writer

  std::vector<std::byte> buf;
  EncodeSegmentHeader(header, buf);

  auto decoded = DecodeSegmentHeader(buf);
  ASSERT_TRUE(decoded.has_value());
  EXPECT_EQ(decoded->format_minor, header.format_minor);
}

TEST(SegmentHeaderTest, MagicIsAWAL) {
  auto header = MakeHeader();
  std::vector<std::byte> buf;
  EncodeSegmentHeader(header, buf);
  EXPECT_EQ(buf[0], std::byte{0x41});  // 'A'
  EXPECT_EQ(buf[1], std::byte{0x57});  // 'W'
  EXPECT_EQ(buf[2], std::byte{0x41});  // 'A'
  EXPECT_EQ(buf[3], std::byte{0x4C});  // 'L'
}

}  // namespace
}  // namespace abyss::queue
