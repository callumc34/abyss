#include "abyss/queue/offset_checkpoint.h"

#include <gtest/gtest.h>

#include <array>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <optional>
#include <string>
#include <vector>

#include "abyss/core/types.h"
#include "abyss/platform/fs.h"
#include "temp_dir.h"

namespace abyss::queue {
namespace {

namespace pfs = abyss::platform::fs;

constexpr uint32_t kShards = 4;
constexpr std::array<core::ConsumerId, 2> kConsumers{core::kColdConsumer, core::kResolverConsumer};

class OffsetCheckpointTest : public ::testing::Test {
 protected:
  void SetUp() override { dir_ = std::make_unique<testing::TempDir>("offset_checkpoint"); }

  std::filesystem::path OffsetsDir() const { return dir_->Path() / "offsets"; }
  std::filesystem::path FilePath() const {
    return OffsetsDir() / std::string(OffsetCheckpoint::kFileName);
  }

  core::Result<std::unique_ptr<OffsetCheckpoint>> Open(uint32_t shards = kShards,
                                                       std::vector<core::ConsumerId> consumers = {
                                                           kConsumers.begin(),
                                                           kConsumers.end()}) const {
    return OffsetCheckpoint::Open(OffsetCheckpointConfig{
        .dir = OffsetsDir(),
        .shard_count = shards,
        .consumers = std::move(consumers),
    });
  }

  std::unique_ptr<OffsetCheckpoint> OpenOrDie() const {
    auto opened = Open();
    EXPECT_TRUE(opened.has_value()) << opened.error().message();
    return opened.has_value() ? std::move(*opened) : nullptr;
  }

  // Consumer-major encoded values: entry i committed at bias + i, except
  // every third, which is left uncommitted.
  static std::vector<uint64_t> Pattern(uint64_t bias) {
    std::vector<uint64_t> out(kConsumers.size() * kShards);
    for (size_t i = 0; i < out.size(); ++i) {
      out[i] = i % 3 == 2 ? 0 : OffsetCheckpoint::Encode(bias + i);
    }
    return out;
  }

  static void ExpectMatches(const OffsetCheckpoint& ckpt, const std::vector<uint64_t>& encoded) {
    for (size_t c = 0; c < kConsumers.size(); ++c) {
      for (uint32_t s = 0; s < kShards; ++s) {
        EXPECT_EQ(ckpt.Get(kConsumers.at(c), s),
                  OffsetCheckpoint::Decode(encoded[(c * kShards) + s]))
            << "consumer " << kConsumers.at(c) << " shard " << s;
      }
    }
  }

  void FlipByte(uint64_t offset) const {
    auto f = pfs::Open(FilePath(), {.mode = pfs::OpenMode::kReadWrite});
    ASSERT_TRUE(f.has_value()) << f.error().message();
    uint8_t byte = 0;
    ASSERT_EQ(pfs::Pread(*f, &byte, 1, offset).value_or(0), 1U);
    byte ^= 0x5AU;
    ASSERT_TRUE(pfs::Pwrite(*f, &byte, 1, offset).has_value());
  }

  // NOLINTNEXTLINE(cppcoreguidelines-non-private-member-variables-in-classes)
  std::unique_ptr<testing::TempDir> dir_;
};

TEST_F(OffsetCheckpointTest, FreshDirectoryStartsWithNothingCommitted) {
  auto ckpt = OpenOrDie();
  ASSERT_NE(ckpt, nullptr);
  EXPECT_EQ(ckpt->epoch(), 1U);
  EXPECT_EQ(std::filesystem::file_size(FilePath()), 2 * ckpt->slot_bytes());
  for (auto consumer : kConsumers) {
    for (uint32_t s = 0; s < kShards; ++s) EXPECT_FALSE(ckpt->Get(consumer, s).has_value());
  }
  EXPECT_FALSE(ckpt->Get(core::kHotConsumer, 0).has_value());
  EXPECT_FALSE(ckpt->Get(core::kColdConsumer, kShards).has_value());
}

TEST_F(OffsetCheckpointTest, WriteIsVisibleAndSurvivesReopen) {
  const auto values = Pattern(7);
  {
    auto ckpt = OpenOrDie();
    ASSERT_NE(ckpt, nullptr);
    ASSERT_TRUE(ckpt->Write(values).has_value());
    ExpectMatches(*ckpt, values);
  }
  auto reopened = OpenOrDie();
  ASSERT_NE(reopened, nullptr);
  ExpectMatches(*reopened, values);
  EXPECT_EQ(reopened->epoch(), 2U);
}

TEST_F(OffsetCheckpointTest, ReopenTakesTheHighestEpoch) {
  {
    auto ckpt = OpenOrDie();
    ASSERT_NE(ckpt, nullptr);
    for (uint64_t round = 0; round < 5; ++round) {
      ASSERT_TRUE(ckpt->Write(Pattern(round * 1000)).has_value());
    }
    EXPECT_EQ(ckpt->epoch(), 6U);
  }
  auto reopened = OpenOrDie();
  ASSERT_NE(reopened, nullptr);
  EXPECT_EQ(reopened->epoch(), 6U);
  ExpectMatches(*reopened, Pattern(4000));
}

TEST_F(OffsetCheckpointTest, CorruptNewestSlotFallsBackToThePreviousOne) {
  size_t slot_bytes = 0;
  {
    auto ckpt = OpenOrDie();
    ASSERT_NE(ckpt, nullptr);
    slot_bytes = ckpt->slot_bytes();
    ASSERT_TRUE(ckpt->Write(Pattern(10)).has_value());  // epoch 2, slot 1
    ASSERT_TRUE(ckpt->Write(Pattern(20)).has_value());  // epoch 3, slot 0
  }
  FlipByte(100);  // inside slot 0's entries

  auto reopened = OpenOrDie();
  ASSERT_NE(reopened, nullptr);
  EXPECT_EQ(reopened->epoch(), 2U);
  ExpectMatches(*reopened, Pattern(10));
  EXPECT_EQ(slot_bytes, OffsetCheckpoint::kBlockSize);
}

TEST_F(OffsetCheckpointTest, CorruptOlderSlotIsIgnored) {
  size_t slot_bytes = 0;
  {
    auto ckpt = OpenOrDie();
    ASSERT_NE(ckpt, nullptr);
    slot_bytes = ckpt->slot_bytes();
    ASSERT_TRUE(ckpt->Write(Pattern(10)).has_value());  // epoch 2, slot 1
    ASSERT_TRUE(ckpt->Write(Pattern(20)).has_value());  // epoch 3, slot 0
  }
  FlipByte(slot_bytes + 40);

  auto reopened = OpenOrDie();
  ASSERT_NE(reopened, nullptr);
  EXPECT_EQ(reopened->epoch(), 3U);
  ExpectMatches(*reopened, Pattern(20));
  // The next write lands in the corrupt slot and makes it valid again.
  ASSERT_TRUE(reopened->Write(Pattern(30)).has_value());
  reopened.reset();
  auto again = OpenOrDie();
  ASSERT_NE(again, nullptr);
  EXPECT_EQ(again->epoch(), 4U);
  ExpectMatches(*again, Pattern(30));
}

TEST_F(OffsetCheckpointTest, BothSlotsCorruptIsCorruption) {
  size_t slot_bytes = 0;
  {
    auto ckpt = OpenOrDie();
    ASSERT_NE(ckpt, nullptr);
    slot_bytes = ckpt->slot_bytes();
    ASSERT_TRUE(ckpt->Write(Pattern(10)).has_value());
  }
  FlipByte(20);
  FlipByte(slot_bytes + 20);

  auto reopened = Open();
  ASSERT_FALSE(reopened.has_value());
  EXPECT_EQ(reopened.error().code(), core::ErrorCode::kCorruption);
}

TEST_F(OffsetCheckpointTest, TruncatedFileIsCorruption) {
  {
    ASSERT_NE(OpenOrDie(), nullptr);
  }
  std::filesystem::resize_file(FilePath(), 100);

  auto reopened = Open();
  ASSERT_FALSE(reopened.has_value());
  EXPECT_EQ(reopened.error().code(), core::ErrorCode::kCorruption);
}

TEST_F(OffsetCheckpointTest, ShardCountMismatchRefuses) {
  {
    ASSERT_NE(OpenOrDie(), nullptr);
  }
  auto reopened = Open(kShards * 2);
  ASSERT_FALSE(reopened.has_value());
  EXPECT_EQ(reopened.error().code(), core::ErrorCode::kFailedPrecondition);
  EXPECT_NE(reopened.error().message().find("shard_count"), std::string::npos);
}

TEST_F(OffsetCheckpointTest, ConsumerSetMismatchRefuses) {
  {
    ASSERT_NE(OpenOrDie(), nullptr);
  }
  auto reopened = Open(kShards, {core::kColdConsumer});
  ASSERT_FALSE(reopened.has_value());
  EXPECT_EQ(reopened.error().code(), core::ErrorCode::kFailedPrecondition);
}

TEST_F(OffsetCheckpointTest, ConsumerOrderDoesNotMatter) {
  const auto values = Pattern(50);
  {
    auto ckpt = OpenOrDie();
    ASSERT_NE(ckpt, nullptr);
    ASSERT_TRUE(ckpt->Write(values).has_value());
  }
  auto reordered = Open(kShards, {core::kResolverConsumer, core::kColdConsumer});
  ASSERT_TRUE(reordered.has_value()) << reordered.error().message();
  ExpectMatches(**reordered, values);
}

TEST_F(OffsetCheckpointTest, LegacyPerFileLayoutRefuses) {
  std::filesystem::create_directories(OffsetsDir() / "1");
  std::ofstream(OffsetsDir() / "1" / "00000000000000000000.offset") << "legacy";

  auto opened = Open();
  ASSERT_FALSE(opened.has_value());
  EXPECT_EQ(opened.error().code(), core::ErrorCode::kFailedPrecondition);
  EXPECT_FALSE(std::filesystem::exists(FilePath())) << "must not start a fresh checkpoint";
}

TEST_F(OffsetCheckpointTest, InterruptedCreateLeavesNoCheckpoint) {
  std::filesystem::create_directories(OffsetsDir());
  auto tmp = FilePath();
  tmp += ".tmp";
  std::ofstream(tmp) << "half-written";

  auto ckpt = OpenOrDie();
  ASSERT_NE(ckpt, nullptr);
  EXPECT_EQ(ckpt->epoch(), 1U);
  EXPECT_FALSE(std::filesystem::exists(tmp));
}

TEST_F(OffsetCheckpointTest, SlotsAreWholeBlocksAndScaleToTheShardCeiling) {
  EXPECT_EQ(OffsetCheckpoint::SlotBytes(64, 2), OffsetCheckpoint::kBlockSize);
  constexpr uint32_t kMaxShards = 32768;
  const size_t big = OffsetCheckpoint::SlotBytes(kMaxShards, 2);
  EXPECT_EQ(big % OffsetCheckpoint::kBlockSize, 0U);
  EXPECT_GT(big, size_t{16} * kMaxShards * 2);

  std::vector<uint64_t> values(size_t{kMaxShards} * 2);
  for (size_t i = 0; i < values.size(); i += 97) values[i] = OffsetCheckpoint::Encode(i * 3);
  {
    auto ckpt = Open(kMaxShards);
    ASSERT_TRUE(ckpt.has_value()) << ckpt.error().message();
    EXPECT_EQ((*ckpt)->slot_bytes(), big);
    ASSERT_TRUE((*ckpt)->Write(values).has_value());
  }
  auto reopened = Open(kMaxShards);
  ASSERT_TRUE(reopened.has_value()) << reopened.error().message();
  for (size_t i = 0; i < values.size(); i += 97) {
    EXPECT_EQ(
        (*reopened)->Get(kConsumers.at(i / kMaxShards), static_cast<uint32_t>(i % kMaxShards)),
        OffsetCheckpoint::Decode(values[i]));
  }
}

}  // namespace
}  // namespace abyss::queue
