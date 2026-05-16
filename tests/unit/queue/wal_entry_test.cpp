#include "abyss/queue/wal_entry.h"

#include <gtest/gtest.h>

#include <chrono>
#include <cstddef>
#include <cstring>
#include <string>
#include <vector>

#include "abyss/core/queue_entry.h"
#include "binary_io.h"
#include "crc32c.h"

namespace abyss::queue {
namespace {

core::QueueEntry MakeWriteEntry(core::SequenceId seq, std::vector<std::string> args) {
  core::QueueEntry e;
  e.seq = seq;
  e.appended_at = core::WallClock::now();
  e.payload = core::entry::Write{.cmd = core::RespCommand{std::move(args)}};
  return e;
}

void ExpectWriteEqual(const core::QueueEntry& a, const core::QueueEntry& b) {
  EXPECT_EQ(a.seq, b.seq);
  const auto a_us =
      std::chrono::duration_cast<std::chrono::microseconds>(a.appended_at.time_since_epoch())
          .count();
  const auto b_us =
      std::chrono::duration_cast<std::chrono::microseconds>(b.appended_at.time_since_epoch())
          .count();
  EXPECT_EQ(a_us, b_us);

  const auto* aw = std::get_if<core::entry::Write>(&a.payload);
  const auto* bw = std::get_if<core::entry::Write>(&b.payload);
  ASSERT_NE(aw, nullptr);
  ASSERT_NE(bw, nullptr);
  EXPECT_EQ(aw->cmd.args, bw->cmd.args);
}

TEST(WalEntryTest, RoundTripBasic) {
  auto entry = MakeWriteEntry(42, {"SET", "foo", "bar"});

  std::vector<std::byte> buf;
  // NOLINTNEXTLINE(cppcoreguidelines-init-variables)
  const size_t encoded = EncodeWalEntry(entry, entry.seq, buf);
  EXPECT_EQ(buf.size(), encoded);

  auto decoded = DecodeWalEntry(buf);
  ASSERT_TRUE(decoded.has_value());
  EXPECT_EQ(decoded->bytes_consumed, encoded);
  ExpectWriteEqual(entry, decoded->entry);
}

TEST(WalEntryTest, RoundTripSingleArg) {
  auto entry = MakeWriteEntry(1, {"PING"});

  std::vector<std::byte> buf;
  EncodeWalEntry(entry, entry.seq, buf);

  auto decoded = DecodeWalEntry(buf);
  ASSERT_TRUE(decoded.has_value());
  ExpectWriteEqual(entry, decoded->entry);
}

TEST(WalEntryTest, RoundTripManyArgs) {
  std::vector<std::string> args;
  args.reserve(32);
  args.emplace_back("MSET");
  for (int i = 0; i < 16; ++i) {
    args.push_back("key" + std::to_string(i));
    args.push_back("val" + std::to_string(i));
  }
  auto entry = MakeWriteEntry(1000, std::move(args));

  std::vector<std::byte> buf;
  EncodeWalEntry(entry, entry.seq, buf);

  auto decoded = DecodeWalEntry(buf);
  ASSERT_TRUE(decoded.has_value());
  ExpectWriteEqual(entry, decoded->entry);
}

TEST(WalEntryTest, RoundTripLargeArgValue) {
  std::string large(1 << 20, 'x');  // 1 MiB
  auto entry = MakeWriteEntry(999, {"SET", "k", large});

  std::vector<std::byte> buf;
  EncodeWalEntry(entry, entry.seq, buf);

  auto decoded = DecodeWalEntry(buf);
  ASSERT_TRUE(decoded.has_value());
  ExpectWriteEqual(entry, decoded->entry);
}

TEST(WalEntryTest, RoundTripEmptyArgString) {
  auto entry = MakeWriteEntry(7, {"SET", "k", ""});

  std::vector<std::byte> buf;
  EncodeWalEntry(entry, entry.seq, buf);

  auto decoded = DecodeWalEntry(buf);
  ASSERT_TRUE(decoded.has_value());
  ExpectWriteEqual(entry, decoded->entry);
}

TEST(WalEntryTest, RoundTripMaxSequenceId) {
  auto entry = MakeWriteEntry(UINT64_MAX, {"SET", "k", "v"});

  std::vector<std::byte> buf;
  EncodeWalEntry(entry, entry.seq, buf);

  auto decoded = DecodeWalEntry(buf);
  ASSERT_TRUE(decoded.has_value());
  EXPECT_EQ(decoded->entry.seq, UINT64_MAX);
}

TEST(WalEntryTest, BackToBackEntries) {
  auto e1 = MakeWriteEntry(1, {"SET", "a", "1"});
  auto e2 = MakeWriteEntry(2, {"SET", "b", "22"});

  std::vector<std::byte> buf;
  EncodeWalEntry(e1, e1.seq, buf);
  EncodeWalEntry(e2, e2.seq, buf);

  std::span<const std::byte> bytes = buf;
  auto d1 = DecodeWalEntry(bytes);
  ASSERT_TRUE(d1.has_value());
  ExpectWriteEqual(e1, d1->entry);

  bytes = bytes.subspan(d1->bytes_consumed);
  auto d2 = DecodeWalEntry(bytes);
  ASSERT_TRUE(d2.has_value());
  ExpectWriteEqual(e2, d2->entry);

  EXPECT_EQ(bytes.size() - d2->bytes_consumed, 0U);
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
  auto entry = MakeWriteEntry(1, {"SET", "foo", "bar"});
  std::vector<std::byte> buf;
  EncodeWalEntry(entry, entry.seq, buf);

  buf.resize(buf.size() - 8);

  auto decoded = DecodeWalEntry(buf);
  ASSERT_FALSE(decoded.has_value());
}

TEST(WalEntryTest, DecodeTruncatedCrc) {
  auto entry = MakeWriteEntry(1, {"SET", "foo", "bar"});
  std::vector<std::byte> buf;
  EncodeWalEntry(entry, entry.seq, buf);

  buf.resize(buf.size() - 2);

  auto decoded = DecodeWalEntry(buf);
  ASSERT_FALSE(decoded.has_value());
}

TEST(WalEntryTest, DecodeCrcMismatchDetected) {
  auto entry = MakeWriteEntry(1, {"SET", "foo", "bar"});
  std::vector<std::byte> buf;
  EncodeWalEntry(entry, entry.seq, buf);

  buf[4] = static_cast<std::byte>(static_cast<uint8_t>(buf[4]) ^ 0x01);

  auto decoded = DecodeWalEntry(buf);
  ASSERT_FALSE(decoded.has_value());
  EXPECT_EQ(decoded.error().code(), core::ErrorCode::kCorruption);
}

TEST(WalEntryTest, DecodeUnknownTypeRejected) {
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

TEST(WalEntryTest, RoundTripConditionalEntry) {
  const core::QueueEntry entry{
      .seq = 10,
      .appended_at = core::WallClock::now(),
      .payload =
          core::entry::Conditional{
              .cmd = core::RespCommand{{"SET", "k", "v", "NX"}},
              .flags = core::PredicateFlags::kNx,
          },
  };

  std::vector<std::byte> buf;
  EncodeWalEntry(entry, entry.seq, buf);

  auto decoded = DecodeWalEntry(buf);
  ASSERT_TRUE(decoded.has_value());
  EXPECT_EQ(decoded->entry.seq, 10U);

  auto* cond = std::get_if<core::entry::Conditional>(&decoded->entry.payload);
  ASSERT_NE(cond, nullptr);
  EXPECT_EQ(cond->cmd.args, (std::vector<std::string>{"SET", "k", "v", "NX"}));
  EXPECT_EQ(cond->flags, core::PredicateFlags::kNx);
}

TEST(WalEntryTest, RoundTripResolvedEntry) {
  const core::QueueEntry entry{
      .seq = 20,
      .appended_at = core::WallClock::now(),
      .payload =
          core::entry::Resolved{
              .ref = 10,
              .decision = core::Decision::kApply,
              .materialised_ops = {core::RespCommand{{"SET", "k", "v"}}},
              .return_value = core::RespValue::SimpleString("OK"),
          },
  };

  std::vector<std::byte> buf;
  EncodeWalEntry(entry, entry.seq, buf);

  auto decoded = DecodeWalEntry(buf);
  ASSERT_TRUE(decoded.has_value());
  EXPECT_EQ(decoded->entry.seq, 20U);

  auto* resolved = std::get_if<core::entry::Resolved>(&decoded->entry.payload);
  ASSERT_NE(resolved, nullptr);
  EXPECT_EQ(resolved->ref, 10U);
  EXPECT_EQ(resolved->decision, core::Decision::kApply);
  ASSERT_EQ(resolved->materialised_ops.size(), 1U);
  EXPECT_EQ(resolved->materialised_ops[0].args, (std::vector<std::string>{"SET", "k", "v"}));
  EXPECT_TRUE(resolved->return_value.IsSimpleString());
  EXPECT_EQ(resolved->return_value.AsString(), "OK");
}

TEST(WalEntryTest, RoundTripResolvedSkipNoMaterialisedOp) {
  const core::QueueEntry entry{
      .seq = 21,
      .appended_at = core::WallClock::now(),
      .payload =
          core::entry::Resolved{
              .ref = 10,
              .decision = core::Decision::kSkip,
              .materialised_ops = {},
              .return_value = core::RespValue::Null(),
          },
  };

  std::vector<std::byte> buf;
  EncodeWalEntry(entry, entry.seq, buf);

  auto decoded = DecodeWalEntry(buf);
  ASSERT_TRUE(decoded.has_value());

  auto* resolved = std::get_if<core::entry::Resolved>(&decoded->entry.payload);
  ASSERT_NE(resolved, nullptr);
  EXPECT_EQ(resolved->decision, core::Decision::kSkip);
  EXPECT_TRUE(resolved->materialised_ops.empty());
  EXPECT_TRUE(resolved->return_value.IsNull());
}

TEST(WalEntryTest, RoundTripResolvedMultipleMaterialisedOps) {
  const core::QueueEntry entry{
      .seq = 30,
      .appended_at = core::WallClock::now(),
      .payload =
          core::entry::Resolved{
              .ref = 25,
              .decision = core::Decision::kApply,
              .materialised_ops = {core::RespCommand{{"DEL", "src"}},
                                   core::RespCommand{{"SET", "dst", "v"}},
                                   core::RespCommand{{"PEXPIREAT", "dst", "1700000000000"}}},
              .return_value = core::RespValue::Integer(1),
          },
  };

  std::vector<std::byte> buf;
  EncodeWalEntry(entry, entry.seq, buf);

  auto decoded = DecodeWalEntry(buf);
  ASSERT_TRUE(decoded.has_value());

  auto* resolved = std::get_if<core::entry::Resolved>(&decoded->entry.payload);
  ASSERT_NE(resolved, nullptr);
  ASSERT_EQ(resolved->materialised_ops.size(), 3U);
  EXPECT_EQ(resolved->materialised_ops[0].args[0], "DEL");
  EXPECT_EQ(resolved->materialised_ops[1].args[0], "SET");
  EXPECT_EQ(resolved->materialised_ops[2].args[0], "PEXPIREAT");
}

TEST(WalEntryTest, BatchLastSeqEncodedAndDecoded) {
  auto entry = MakeWriteEntry(10, {"SET", "k", "v"});
  std::vector<std::byte> buf;
  EncodeWalEntry(entry, 15, buf);

  auto decoded = DecodeWalEntry(buf);
  ASSERT_TRUE(decoded.has_value());
  EXPECT_EQ(decoded->entry.seq, 10U);
  EXPECT_EQ(decoded->batch_last_seq, 15U);
}

TEST(WalEntryTest, BatchLastSeqDefaultsToSelfForSingleEntry) {
  auto entry = MakeWriteEntry(7, {"SET", "k", "v"});
  std::vector<std::byte> buf;
  EncodeWalEntry(entry, entry.seq, buf);

  auto decoded = DecodeWalEntry(buf);
  ASSERT_TRUE(decoded.has_value());
  EXPECT_EQ(decoded->batch_last_seq, 7U);
}

TEST(WalEntryTest, DecodeMinor10OmitsBatchLastSeq) {
  // Hand-build a pre-1.1 entry (no trailing batch_last_seq field) so we can
  // verify the decoder still accepts it when told the segment is minor 1.0.
  core::QueueEntry entry = MakeWriteEntry(42, {"SET", "a", "1"});
  std::vector<std::byte> body;
  binary::WriteU8(body, 0);
  binary::WriteU64LE(body, entry.seq);
  const auto us =
      std::chrono::duration_cast<std::chrono::microseconds>(entry.appended_at.time_since_epoch())
          .count();
  binary::WriteI64LE(body, us);
  binary::WriteU32LE(body, 3);
  for (const auto& a : std::vector<std::string>{"SET", "a", "1"}) {
    binary::WriteU32LE(body, static_cast<uint32_t>(a.size()));
    binary::AppendBytes(body, a.data(), a.size());
  }

  std::vector<std::byte> buf;
  binary::WriteU32LE(buf, static_cast<uint32_t>(body.size()));
  buf.insert(buf.end(), body.begin(), body.end());
  binary::WriteU32LE(buf, Crc32c(body));

  auto decoded = DecodeWalEntry(buf, /*format_minor=*/0);
  ASSERT_TRUE(decoded.has_value());
  EXPECT_EQ(decoded->entry.seq, 42U);
  EXPECT_EQ(decoded->batch_last_seq, 42U);
}

TEST(WalEntryTest, BatchLastSeqLessThanSelfIsCorruption) {
  core::QueueEntry entry = MakeWriteEntry(50, {"SET", "a", "1"});
  std::vector<std::byte> buf;
  EncodeWalEntry(entry, 30, buf);

  auto decoded = DecodeWalEntry(buf);
  ASSERT_FALSE(decoded.has_value());
  EXPECT_EQ(decoded.error().code(), core::ErrorCode::kCorruption);
}

TEST(WalEntryTest, RoundTripFlushEntry) {
  const core::QueueEntry entry{
      .seq = 77,
      .appended_at = core::WallClock::now(),
      .payload = core::entry::Flush{},
  };

  std::vector<std::byte> buf;
  const size_t encoded = EncodeWalEntry(entry, entry.seq, buf);
  EXPECT_EQ(buf.size(), encoded);

  auto decoded = DecodeWalEntry(buf);
  ASSERT_TRUE(decoded.has_value());
  EXPECT_EQ(decoded->entry.seq, 77U);
  EXPECT_EQ(decoded->batch_last_seq, 77U);
  EXPECT_TRUE(std::holds_alternative<core::entry::Flush>(decoded->entry.payload));
}

TEST(WalEntryTest, FlushEntryBatchedWithSibling) {
  const core::QueueEntry entry{
      .seq = 100,
      .appended_at = core::WallClock::now(),
      .payload = core::entry::Flush{},
  };

  std::vector<std::byte> buf;
  EncodeWalEntry(entry, /*batch_last_seq=*/101, buf);

  auto decoded = DecodeWalEntry(buf);
  ASSERT_TRUE(decoded.has_value());
  EXPECT_EQ(decoded->entry.seq, 100U);
  EXPECT_EQ(decoded->batch_last_seq, 101U);
}

}  // namespace
}  // namespace abyss::queue
