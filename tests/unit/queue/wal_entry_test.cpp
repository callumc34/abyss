#include "abyss/queue/wal_entry.h"

#include <gtest/gtest.h>

#include <chrono>
#include <cstddef>
#include <cstring>
#include <string>
#include <vector>

#include "abyss/core/queue.h"
#include "binary_io.h"
#include "crc32c.h"

namespace abyss::queue {
namespace {

core::LogEntry MakeEntry(core::SequenceId seq, std::vector<std::string> args) {
  core::LogEntry e;
  e.seq = seq;
  e.appended_at = core::WallClock::now();
  e.cmd.args = std::move(args);
  return e;
}

void ExpectEqual(const core::LogEntry& a, const core::LogEntry& b) {
  EXPECT_EQ(a.seq, b.seq);
  EXPECT_EQ(a.cmd.args, b.cmd.args);
  const auto a_us =
      std::chrono::duration_cast<std::chrono::microseconds>(a.appended_at.time_since_epoch())
          .count();
  const auto b_us =
      std::chrono::duration_cast<std::chrono::microseconds>(b.appended_at.time_since_epoch())
          .count();
  EXPECT_EQ(a_us, b_us);
}

TEST(WalEntryTest, RoundTripBasic) {
  auto entry = MakeEntry(42, {"SET", "foo", "bar"});

  std::vector<std::byte> buf;
  const size_t encoded = EncodeWalEntry(entry, buf);
  EXPECT_EQ(buf.size(), encoded);

  auto decoded = DecodeWalEntry(buf);
  ASSERT_TRUE(decoded.has_value());
  EXPECT_EQ(decoded->bytes_consumed, encoded);
  ExpectEqual(entry, decoded->entry);
}

TEST(WalEntryTest, RoundTripSingleArg) {
  auto entry = MakeEntry(1, {"PING"});

  std::vector<std::byte> buf;
  EncodeWalEntry(entry, buf);

  auto decoded = DecodeWalEntry(buf);
  ASSERT_TRUE(decoded.has_value());
  ExpectEqual(entry, decoded->entry);
}

TEST(WalEntryTest, RoundTripManyArgs) {
  std::vector<std::string> args;
  args.reserve(32);
  args.emplace_back("MSET");
  for (int i = 0; i < 16; ++i) {
    args.push_back("key" + std::to_string(i));
    args.push_back("val" + std::to_string(i));
  }
  auto entry = MakeEntry(1000, std::move(args));

  std::vector<std::byte> buf;
  EncodeWalEntry(entry, buf);

  auto decoded = DecodeWalEntry(buf);
  ASSERT_TRUE(decoded.has_value());
  ExpectEqual(entry, decoded->entry);
}

TEST(WalEntryTest, RoundTripLargeArgValue) {
  std::string large(1 << 20, 'x');  // 1 MiB
  auto entry = MakeEntry(999, {"SET", "k", large});

  std::vector<std::byte> buf;
  EncodeWalEntry(entry, buf);

  auto decoded = DecodeWalEntry(buf);
  ASSERT_TRUE(decoded.has_value());
  ExpectEqual(entry, decoded->entry);
}

TEST(WalEntryTest, RoundTripEmptyArgString) {
  auto entry = MakeEntry(7, {"SET", "k", ""});

  std::vector<std::byte> buf;
  EncodeWalEntry(entry, buf);

  auto decoded = DecodeWalEntry(buf);
  ASSERT_TRUE(decoded.has_value());
  ExpectEqual(entry, decoded->entry);
}

TEST(WalEntryTest, RoundTripMaxSequenceId) {
  auto entry = MakeEntry(UINT64_MAX, {"SET", "k", "v"});

  std::vector<std::byte> buf;
  EncodeWalEntry(entry, buf);

  auto decoded = DecodeWalEntry(buf);
  ASSERT_TRUE(decoded.has_value());
  EXPECT_EQ(decoded->entry.seq, UINT64_MAX);
}

TEST(WalEntryTest, BackToBackEntries) {
  auto e1 = MakeEntry(1, {"SET", "a", "1"});
  auto e2 = MakeEntry(2, {"SET", "b", "22"});

  std::vector<std::byte> buf;
  EncodeWalEntry(e1, buf);
  EncodeWalEntry(e2, buf);

  std::span<const std::byte> bytes = buf;
  auto d1 = DecodeWalEntry(bytes);
  ASSERT_TRUE(d1.has_value());
  ExpectEqual(e1, d1->entry);

  bytes = bytes.subspan(d1->bytes_consumed);
  auto d2 = DecodeWalEntry(bytes);
  ASSERT_TRUE(d2.has_value());
  ExpectEqual(e2, d2->entry);

  EXPECT_EQ(bytes.size() - d2->bytes_consumed, 0u);
}

TEST(WalEntryTest, DecodeEmptyBufferFails) {
  std::vector<std::byte> buf;
  auto decoded = DecodeWalEntry(buf);
  ASSERT_FALSE(decoded.has_value());
  EXPECT_EQ(decoded.error().code(), core::ErrorCode::kCorruption);
}

TEST(WalEntryTest, DecodeTruncatedLengthPrefix) {
  std::vector<std::byte> buf(3);  // only 3 bytes, length prefix needs 4
  auto decoded = DecodeWalEntry(buf);
  ASSERT_FALSE(decoded.has_value());
}

TEST(WalEntryTest, DecodeTruncatedBody) {
  auto entry = MakeEntry(1, {"SET", "foo", "bar"});
  std::vector<std::byte> buf;
  EncodeWalEntry(entry, buf);

  // Drop the CRC and half the body.
  buf.resize(buf.size() - 8);

  auto decoded = DecodeWalEntry(buf);
  ASSERT_FALSE(decoded.has_value());
}

TEST(WalEntryTest, DecodeTruncatedCrc) {
  auto entry = MakeEntry(1, {"SET", "foo", "bar"});
  std::vector<std::byte> buf;
  EncodeWalEntry(entry, buf);

  // Drop the last 2 CRC bytes.
  buf.resize(buf.size() - 2);

  auto decoded = DecodeWalEntry(buf);
  ASSERT_FALSE(decoded.has_value());
}

TEST(WalEntryTest, DecodeCrcMismatchDetected) {
  auto entry = MakeEntry(1, {"SET", "foo", "bar"});
  std::vector<std::byte> buf;
  EncodeWalEntry(entry, buf);

  // Flip a bit in the body (skip the 4-byte length prefix).
  buf[4] = static_cast<std::byte>(static_cast<uint8_t>(buf[4]) ^ 0x01);

  auto decoded = DecodeWalEntry(buf);
  ASSERT_FALSE(decoded.has_value());
  EXPECT_EQ(decoded.error().code(), core::ErrorCode::kCorruption);
}

TEST(WalEntryTest, DecodeUnknownTypeRejected) {
  // Build a synthetic entry with type=0xFF.
  std::vector<std::byte> buf;
  std::vector<std::byte> body;

  binary::WriteU8(body, 0xFF);  // unknown type
  binary::WriteU64LE(body, 1);  // seq
  binary::WriteI64LE(body, 0);  // appended_us
  binary::WriteU32LE(body, 0);  // arg_count = 0

  binary::WriteU32LE(buf, static_cast<uint32_t>(body.size()));
  buf.insert(buf.end(), body.begin(), body.end());
  binary::WriteU32LE(buf, Crc32c(body));

  auto decoded = DecodeWalEntry(buf);
  ASSERT_FALSE(decoded.has_value());
  EXPECT_EQ(decoded.error().code(), core::ErrorCode::kCorruption);
}

TEST(WalEntryTest, DecodeIgnoresTrailingBodyFields) {
  // Simulate a "newer minor" entry with extra trailing fields in the body.
  // An older decoder should ignore the trailing bytes and succeed.
  auto entry = MakeEntry(7, {"SET", "k", "v"});

  std::vector<std::byte> normal;
  EncodeWalEntry(entry, normal);

  // Parse the body out of `normal`: [0..4) is length prefix, [4..4+len) is body,
  // [4+len..end) is CRC.
  uint32_t body_len = 0;
  std::memcpy(&body_len, normal.data(), sizeof(body_len));
  std::vector<std::byte> body(normal.begin() + 4, normal.begin() + 4 + body_len);

  // Append 7 fictional "future" bytes to the body.
  const std::vector<std::byte> future = {std::byte{0xDE}, std::byte{0xAD}, std::byte{0xBE},
                                         std::byte{0xEF}, std::byte{0xCA}, std::byte{0xFE},
                                         std::byte{0x00}};
  body.insert(body.end(), future.begin(), future.end());

  // Rebuild the framed entry with new length, appended bytes, and new CRC.
  std::vector<std::byte> buf;
  binary::WriteU32LE(buf, static_cast<uint32_t>(body.size()));
  buf.insert(buf.end(), body.begin(), body.end());
  binary::WriteU32LE(buf, Crc32c(body));

  auto decoded = DecodeWalEntry(buf);
  ASSERT_TRUE(decoded.has_value()) << decoded.error().message();
  ExpectEqual(entry, decoded->entry);
}

TEST(WalEntryTest, EncodedSizeSmallEntry) {
  auto entry = MakeEntry(1, {"SET", "foo", "bar"});
  std::vector<std::byte> buf;
  EncodeWalEntry(entry, buf);

  // 4 (body_len) + 1 (type) + 8 (seq) + 8 (appended_us) + 4 (arg_count)
  // + 3 * (4 (arg_len) + 3 (bytes)) + 4 (body_crc) = 50 bytes.
  EXPECT_EQ(buf.size(), 50u);
}

}  // namespace
}  // namespace abyss::queue
