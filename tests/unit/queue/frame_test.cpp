#include "abyss/queue/frame.h"

#include <gtest/gtest.h>

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <span>
#include <string>
#include <utility>
#include <variant>
#include <vector>

#include "abyss/core/queue_entry.h"
#include "abyss/core/resp_types.h"
#include "binary_io.h"

namespace abyss::queue::frame {
namespace {

constexpr std::size_t kBodyAt = kCommitBytes + kCrcBytes;
constexpr uint32_t kGen = 7;
constexpr uint64_t kSalt = 0x9e37'79b9'7f4a'7c15;

core::QueueEntry WriteEntry(core::SequenceId seq, std::string value) {
  return core::QueueEntry{
      .seq = seq,
      .appended_at = core::WallTime(std::chrono::microseconds(1'700'000'000'123'456)),
      .payload = core::entry::Write{.cmd = core::RespCommand{{"SET", "k", std::move(value)}}},
  };
}

// Seals each frame of a closed batch for `gen`, as Log::Commit does.
void Seal(std::span<std::byte> frames, uint32_t gen, uint64_t salt = kSalt) {
  for (std::size_t off = 0; off < frames.size();) {
    std::byte* f = frames.data() + off;
    const uint32_t len = CommitLen(binary::LoadLE<uint64_t>(f));
    const uint64_t word = CommitWord(len, gen);
    binary::StoreLE(f + kCommitBytes,
                    SealCrc(binary::LoadLE<uint32_t>(f + kCommitBytes), salt, word));
    binary::StoreLE(f, word);
    off += FrameSize(len);
  }
}

View InspectAt(std::span<const std::byte> bytes, uint32_t gen, bool verify = true,
               uint64_t salt = kSalt) {
  return Inspect(binary::LoadLE<uint64_t>(bytes.data()), bytes, gen, salt, verify);
}

std::vector<std::byte> Closed(const core::QueueEntry& entry, core::ShardId shard = 3) {
  std::vector<std::byte> out;
  EncodeEntry(entry, shard, out);
  CloseBatch(out);
  return out;
}

int64_t Micros(core::WallTime t) {
  return std::chrono::duration_cast<std::chrono::microseconds>(t.time_since_epoch()).count();
}

TEST(FrameTest, RoundTripsEveryEntryType) {
  std::vector<core::QueueEntry> entries;
  entries.push_back(WriteEntry(10, "value"));
  entries.push_back(core::QueueEntry{
      .seq = 11,
      .payload = core::entry::Conditional{.cmd = core::RespCommand{{"SET", "k", "v", "NX"}},
                                          .flags = core::PredicateFlags::kNx}});
  entries.push_back(core::QueueEntry{
      .seq = 12,
      .payload = core::entry::Resolved{.ref = 11,
                                       .decision = core::Decision::kApply,
                                       .materialised_ops = {core::RespCommand{{"SET", "k", "v"}}},
                                       .return_value = core::RespValue::SimpleString("OK")}});
  entries.push_back(core::QueueEntry{.seq = 13, .payload = core::entry::Flush{}});

  for (const auto& entry : entries) {
    std::vector<std::byte> bytes;
    const std::size_t size = EncodeEntry(entry, 3, bytes);
    ASSERT_EQ(size, bytes.size());
    EXPECT_EQ(binary::LoadLE<uint64_t>(bytes.data()) >> 32, 0U) << "gen is sealed by Commit";
    CloseBatch(bytes);
    Seal(bytes, kGen);

    const View view = InspectAt(bytes, kGen);
    ASSERT_EQ(view.state, State::kFilled);
    EXPECT_EQ(view.header.kind, Kind::kEntry);
    EXPECT_EQ(view.header.shard, 3U);
    EXPECT_EQ(view.header.seq, entry.seq);
    EXPECT_EQ(view.header.batch_rest, size);
    EXPECT_EQ(view.size, size);

    auto decoded = DecodeEntry(view);
    ASSERT_TRUE(decoded.has_value()) << decoded.error().message();
    EXPECT_EQ(decoded->seq, entry.seq);
    EXPECT_EQ(Micros(decoded->appended_at), Micros(entry.appended_at));
    EXPECT_EQ(decoded->payload.index(), entry.payload.index());
  }

  std::vector<std::byte> bytes = Closed(entries[2]);
  Seal(bytes, kGen);
  auto resolved = DecodeEntry(InspectAt(bytes, kGen));
  ASSERT_TRUE(resolved.has_value());
  const auto& r = std::get<core::entry::Resolved>(resolved->payload);
  EXPECT_EQ(r.ref, 11U);
  ASSERT_EQ(r.materialised_ops.size(), 1U);
  EXPECT_EQ(r.materialised_ops[0].args, (std::vector<std::string>{"SET", "k", "v"}));
  EXPECT_EQ(r.return_value.AsString(), "OK");

  bytes = Closed(entries[1]);
  Seal(bytes, kGen);
  auto conditional = DecodeEntry(InspectAt(bytes, kGen));
  ASSERT_TRUE(conditional.has_value());
  EXPECT_EQ(std::get<core::entry::Conditional>(conditional->payload).flags,
            core::PredicateFlags::kNx);
}

TEST(FrameTest, BatchRestCountsTheBytesLeftInTheBatch) {
  std::vector<std::byte> batch;
  std::vector<std::size_t> sizes;
  sizes.push_back(EncodeEntry(WriteEntry(1, "a"), 0, batch));
  sizes.push_back(EncodeEntry(WriteEntry(2, std::string(100, 'b')), 0, batch));
  sizes.push_back(EncodeEntry(WriteEntry(3, "c"), 0, batch));
  CloseBatch(batch);
  Seal(batch, kGen);

  std::size_t off = 0;
  for (std::size_t i = 0; i < sizes.size(); ++i) {
    const View view = InspectAt(std::span(batch).subspan(off), kGen);
    ASSERT_EQ(view.state, State::kFilled);
    EXPECT_EQ(view.header.batch_rest, batch.size() - off);
    EXPECT_EQ(view.header.seq, i + 1);
    off += view.size;
  }
  EXPECT_EQ(off, batch.size());
  const View last = InspectAt(std::span(batch).subspan(batch.size() - sizes.back()), kGen);
  EXPECT_EQ(last.header.batch_rest, last.size);
}

TEST(FrameTest, SealedForOneGenIsUnfilledUnderAnother) {
  std::vector<std::byte> bytes = Closed(WriteEntry(1, "v"));
  Seal(bytes, kGen);
  EXPECT_EQ(InspectAt(bytes, kGen + 1).state, State::kUnfilled);
  EXPECT_EQ(InspectAt(bytes, kGen + 1, false).state, State::kUnfilled);
}

TEST(FrameTest, StaleBodyUnderANewCommitWordFailsItsCrc) {
  // A torn sector: the new gen's word over a stale frame of equal length.
  std::vector<std::byte> bytes = Closed(WriteEntry(1, "v"));
  Seal(bytes, kGen);
  const uint32_t len = CommitLen(binary::LoadLE<uint64_t>(bytes.data()));
  binary::StoreLE(bytes.data(), CommitWord(len, kGen + 1));
  EXPECT_EQ(InspectAt(bytes, kGen + 1).state, State::kTorn);
  EXPECT_EQ(InspectAt(bytes, kGen + 1, false).state, State::kFilled);
}

TEST(FrameTest, AFrameSealedUnderAnotherSaltIsTorn) {
  std::vector<std::byte> bytes = Closed(WriteEntry(1, "v"));
  Seal(bytes, kGen, kSalt + 1);
  EXPECT_EQ(InspectAt(bytes, kGen).state, State::kTorn);
  EXPECT_EQ(InspectAt(bytes, kGen, true, kSalt + 1).state, State::kFilled);

  std::vector<std::byte> padding(64);
  binary::StoreLE(padding.data(), EncodePadding(padding.size(), kGen, kSalt + 1, padding));
  EXPECT_EQ(InspectAt(padding, kGen).state, State::kTorn) << "a forged segment skip";
}

TEST(FrameTest, PaddingChecksumsOnlyItsHeader) {
  std::vector<std::byte> bytes(200, std::byte{0xAB});
  const uint64_t word = EncodePadding(bytes.size(), kGen, kSalt, bytes);
  binary::StoreLE(bytes.data(), word);
  View view = InspectAt(bytes, kGen);
  ASSERT_EQ(view.state, State::kFilled);
  EXPECT_EQ(view.header.kind, Kind::kPadding);
  EXPECT_EQ(view.size, bytes.size());

  bytes[kBodyAt + kHeaderBytes + 5] ^= std::byte{0xFF};
  EXPECT_EQ(InspectAt(bytes, kGen).state, State::kFilled) << "stale bytes are not covered";
  bytes[kBodyAt + 9] ^= std::byte{0xFF};
  EXPECT_EQ(InspectAt(bytes, kGen).state, State::kTorn);

  std::vector<std::byte> smallest(kMinFrameBytes);
  binary::StoreLE(smallest.data(), EncodePadding(smallest.size(), kGen, kSalt, smallest));
  view = InspectAt(smallest, kGen);
  EXPECT_EQ(view.state, State::kFilled);
  EXPECT_EQ(view.size, kMinFrameBytes);
}

TEST(FrameTest, AFlippedBodyByteIsTorn) {
  std::vector<std::byte> bytes = Closed(WriteEntry(1, "value"));
  Seal(bytes, kGen);
  bytes[kBodyAt + kHeaderBytes + 2] ^= std::byte{0x01};
  EXPECT_EQ(InspectAt(bytes, kGen).state, State::kTorn);
  EXPECT_EQ(InspectAt(bytes, kGen, false).state, State::kFilled);
}

TEST(FrameTest, ZeroLenIsUnfilled) {
  std::vector<std::byte> bytes = Closed(WriteEntry(1, "v"));
  Seal(bytes, kGen);
  binary::StoreLE(bytes.data(), CommitWord(0, kGen));
  EXPECT_EQ(InspectAt(bytes, kGen).state, State::kUnfilled);
  const std::vector<std::byte> zeros(64);
  EXPECT_EQ(InspectAt(zeros, 0).state, State::kUnfilled);
}

TEST(FrameTest, WrongGenIsUnfilled) {
  std::vector<std::byte> bytes = Closed(WriteEntry(1, "v"));
  Seal(bytes, kGen);
  EXPECT_EQ(InspectAt(bytes, kGen - 1).state, State::kUnfilled);
  EXPECT_EQ(InspectAt(bytes, 0).state, State::kUnfilled);
}

TEST(FrameTest, FrameSizesAreAligned) {
  EXPECT_EQ(FrameSize(kHeaderBytes), kMinFrameBytes);
  for (std::size_t len = 0; len < 200; ++len) {
    EXPECT_EQ(FrameSize(len) % kAlign, 0U);
    EXPECT_GE(FrameSize(len), kBodyAt + len);
    EXPECT_LT(FrameSize(len), kBodyAt + len + kAlign);
  }
  std::vector<std::byte> out;
  for (std::size_t n = 0; n < 20; ++n) {
    const std::size_t before = out.size();
    const std::size_t size = EncodeEntry(WriteEntry(n, std::string(n, 'x')), 0, out);
    EXPECT_EQ(size % kAlign, 0U);
    EXPECT_EQ(out.size() - before, size);
  }
}

TEST(FrameTest, OverrunAndImpossibleLengthsAreTorn) {
  std::vector<std::byte> bytes = Closed(WriteEntry(1, "value"));
  Seal(bytes, kGen);
  const std::span<const std::byte> all(bytes);
  EXPECT_EQ(InspectAt(all.first(bytes.size() - kAlign), kGen, false).state, State::kTorn);

  binary::StoreLE(bytes.data(), CommitWord(kHeaderBytes - 1, kGen));
  EXPECT_EQ(InspectAt(bytes, kGen, false).state, State::kTorn);
  binary::StoreLE(bytes.data(), CommitWord(1U << 30, kGen));
  EXPECT_EQ(InspectAt(bytes, kGen, false).state, State::kTorn);
}

TEST(FrameTest, UnknownKindUnderAValidCrcIsFilledForTheCallerToRefuse) {
  std::vector<std::byte> bytes;
  EncodeEntry(WriteEntry(1, "v"), 0, bytes);
  bytes[kBodyAt] = std::byte{9};
  CloseBatch(bytes);
  Seal(bytes, kGen);
  const View view = InspectAt(bytes, kGen);
  ASSERT_EQ(view.state, State::kFilled);
  EXPECT_EQ(static_cast<int>(view.header.kind), 9);
  auto decoded = DecodeEntry(view);
  ASSERT_FALSE(decoded.has_value());
  EXPECT_EQ(decoded.error().code(), core::ErrorCode::kInvalidArgument);
}

TEST(FrameTest, UnparseablePayloadUnderAValidCrcIsCorruption) {
  std::vector<std::byte> bytes;
  EncodeEntry(WriteEntry(1, "v"), 0, bytes);
  // The Write payload's arg count.
  binary::StoreLE<uint32_t>(bytes.data() + kBodyAt + kHeaderBytes, 0xFFFF'FFFF);
  CloseBatch(bytes);
  Seal(bytes, kGen);
  const View view = InspectAt(bytes, kGen);
  ASSERT_EQ(view.state, State::kFilled);
  auto decoded = DecodeEntry(view);
  ASSERT_FALSE(decoded.has_value());
  EXPECT_EQ(decoded.error().code(), core::ErrorCode::kCorruption);
}

TEST(FrameTest, BytesAfterThePayloadAreCorruption) {
  std::vector<std::byte> bytes;
  EncodeEntry(core::QueueEntry{.seq = 1, .payload = core::entry::Flush{}}, 0, bytes);
  const uint32_t len = CommitLen(binary::LoadLE<uint64_t>(bytes.data()));
  bytes.resize(FrameSize(len + kAlign));
  binary::StoreLE(bytes.data(), CommitWord(static_cast<uint32_t>(len + kAlign), 0));
  CloseBatch(bytes);
  Seal(bytes, kGen);
  const View view = InspectAt(bytes, kGen);
  ASSERT_EQ(view.state, State::kFilled);
  auto decoded = DecodeEntry(view);
  ASSERT_FALSE(decoded.has_value());
  EXPECT_EQ(decoded.error().code(), core::ErrorCode::kCorruption);
}

}  // namespace
}  // namespace abyss::queue::frame
