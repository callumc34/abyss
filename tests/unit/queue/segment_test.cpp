#include "segment.h"

#include <fcntl.h>
#include <gtest/gtest.h>
#include <unistd.h>

#include <chrono>
#include <cstddef>
#include <cstdlib>
#include <filesystem>
#include <string>
#include <vector>

#include "abyss/core/queue_entry.h"
#include "abyss/core/types.h"
#include "abyss/queue/segment_header.h"
#include "abyss/queue/wal_entry.h"

namespace abyss::queue {
namespace {

constexpr size_t kDefaultMaxSize = size_t{4} * 1024 * 1024;

// Most tests write single-entry appends where batch_last_seq == entry.seq.
// This wrapper keeps test expressions tidy; tests that exercise batches
// call Segment::Append(entry, last_seq) directly.
core::Result<size_t> AppendSingle(Segment& seg, const core::QueueEntry& entry) {
  return seg.Append(entry, entry.seq);
}

class SegmentTest : public ::testing::Test {
 protected:
  void SetUp() override {
    auto tmpl = std::filesystem::temp_directory_path() / "abyss_seg_XXXXXX";
    std::string s = tmpl.string();
    ASSERT_NE(::mkdtemp(s.data()), nullptr);
    tmp_dir_ = s;
  }

  void TearDown() override {
    if (!tmp_dir_.empty()) {
      std::filesystem::remove_all(tmp_dir_);
    }
  }

  std::string SegPath(const std::string& name = "test.wal") const {
    return (std::filesystem::path(tmp_dir_) / name).string();
  }

  static SegmentHeader MakeHeader(core::SequenceId base_seq = 0, core::ShardId shard = 0) {
    SegmentHeader h;
    h.format_major = kWalFormatMajor;
    h.format_minor = kWalFormatMinor;
    h.flags = 0;
    h.shard_id = shard;
    h.base_seq = base_seq;
    h.created_at = core::WallClock::now();
    return h;
  }

  static core::QueueEntry MakeWrite(core::SequenceId seq, std::vector<std::string> args) {
    core::QueueEntry e;
    e.seq = seq;
    e.appended_at = core::WallClock::now();
    e.payload = core::entry::Write{.cmd = core::RespCommand{std::move(args)}};
    return e;
  }

  static core::QueueEntry MakeConditional(core::SequenceId seq, std::vector<std::string> args,
                                          core::PredicateFlags flags) {
    core::QueueEntry e;
    e.seq = seq;
    e.appended_at = core::WallClock::now();
    e.payload = core::entry::Conditional{
        .cmd = core::RespCommand{std::move(args)},
        .flags = flags,
    };
    return e;
  }

  static core::QueueEntry MakeResolved(core::SequenceId seq, core::SequenceId ref,
                                       core::Decision decision) {
    core::QueueEntry e;
    e.seq = seq;
    e.appended_at = core::WallClock::now();
    e.payload = core::entry::Resolved{
        .ref = ref,
        .decision = decision,
        .materialised_op =
            decision == core::Decision::kApply
                ? std::optional<core::RespCommand>{core::RespCommand{{"SET", "k", "v"}}}
                : std::nullopt,
        .return_value = decision == core::Decision::kApply ? core::RespValue::SimpleString("OK")
                                                           : core::RespValue::Null(),
    };
    return e;
  }

  static void ExpectWriteArgs(const core::QueueEntry& entry,
                              const std::vector<std::string>& expected) {
    const auto* w = std::get_if<core::entry::Write>(&entry.payload);
    ASSERT_NE(w, nullptr);
    EXPECT_EQ(w->cmd.args, expected);
  }

  std::string tmp_dir_;  // NOLINT(cppcoreguidelines-non-private-member-variables-in-classes)
};

TEST_F(SegmentTest, CreateAndReadBack) {
  auto seg = Segment::Create(SegPath(), MakeHeader(100), kDefaultMaxSize);
  ASSERT_TRUE(seg.has_value());

  EXPECT_EQ(seg->base_seq(), 100U);
  EXPECT_EQ(seg->next_seq(), 100U);
  EXPECT_EQ(seg->entry_count(), 0U);
  EXPECT_EQ(seg->write_offset(), kSegmentHeaderSize);

  ASSERT_TRUE(AppendSingle(*seg, MakeWrite(100, {"SET", "a", "1"})).has_value());
  ASSERT_TRUE(AppendSingle(*seg, MakeWrite(101, {"SET", "b", "2"})).has_value());
  ASSERT_TRUE(AppendSingle(*seg, MakeWrite(102, {"DEL", "c"})).has_value());

  EXPECT_EQ(seg->next_seq(), 103U);
  EXPECT_EQ(seg->entry_count(), 3U);
  EXPECT_GT(seg->write_offset(), kSegmentHeaderSize);

  auto read = seg->ReadEntries(kSegmentHeaderSize, 10);
  ASSERT_TRUE(read.has_value());
  ASSERT_EQ(read->entries.size(), 3U);
  EXPECT_EQ(read->entries[0].seq, 100U);
  EXPECT_EQ(read->entries[1].seq, 101U);
  EXPECT_EQ(read->entries[2].seq, 102U);
  ExpectWriteArgs(read->entries[0], {"SET", "a", "1"});
  ExpectWriteArgs(read->entries[1], {"SET", "b", "2"});
  ExpectWriteArgs(read->entries[2], {"DEL", "c"});
  EXPECT_EQ(read->next_offset, seg->write_offset());
}

TEST_F(SegmentTest, OpenExistingRoundTrip) {
  const auto path = SegPath();
  size_t expected_offset = 0;
  {
    auto seg = Segment::Create(path, MakeHeader(50), kDefaultMaxSize);
    ASSERT_TRUE(seg.has_value());
    ASSERT_TRUE(AppendSingle(*seg, MakeWrite(50, {"SET", "x", "hello"})).has_value());
    ASSERT_TRUE(AppendSingle(*seg, MakeWrite(51, {"SET", "y", "world"})).has_value());
    expected_offset = seg->write_offset();
  }

  auto seg = Segment::Open(path, kDefaultMaxSize);
  ASSERT_TRUE(seg.has_value());
  EXPECT_EQ(seg->base_seq(), 50U);
  EXPECT_EQ(seg->next_seq(), 52U);
  EXPECT_EQ(seg->entry_count(), 2U);
  EXPECT_EQ(seg->write_offset(), expected_offset);
  EXPECT_EQ(seg->header().shard_id, 0U);

  auto read = seg->ReadEntries(kSegmentHeaderSize, 10);
  ASSERT_TRUE(read.has_value());
  ASSERT_EQ(read->entries.size(), 2U);
  ExpectWriteArgs(read->entries[0], {"SET", "x", "hello"});
  ExpectWriteArgs(read->entries[1], {"SET", "y", "world"});
}

TEST_F(SegmentTest, OpenAndContinueAppending) {
  const auto path = SegPath();
  {
    auto seg = Segment::Create(path, MakeHeader(0), kDefaultMaxSize);
    ASSERT_TRUE(seg.has_value());
    ASSERT_TRUE(AppendSingle(*seg, MakeWrite(0, {"SET", "a", "1"})).has_value());
    ASSERT_TRUE(AppendSingle(*seg, MakeWrite(1, {"SET", "b", "2"})).has_value());
  }

  auto seg = Segment::Open(path, kDefaultMaxSize);
  ASSERT_TRUE(seg.has_value());
  EXPECT_EQ(seg->next_seq(), 2U);

  ASSERT_TRUE(AppendSingle(*seg, MakeWrite(2, {"SET", "c", "3"})).has_value());
  EXPECT_EQ(seg->next_seq(), 3U);
  EXPECT_EQ(seg->entry_count(), 3U);

  auto read = seg->ReadEntries(kSegmentHeaderSize, 10);
  ASSERT_TRUE(read.has_value());
  ASSERT_EQ(read->entries.size(), 3U);
  ExpectWriteArgs(read->entries[2], {"SET", "c", "3"});
}

TEST_F(SegmentTest, TornTail_TruncatedBody) {
  const auto path = SegPath();
  size_t good_offset = 0;
  {
    auto seg = Segment::Create(path, MakeHeader(0), kDefaultMaxSize);
    ASSERT_TRUE(seg.has_value());
    ASSERT_TRUE(AppendSingle(*seg, MakeWrite(0, {"SET", "a", "1"})).has_value());
    good_offset = seg->write_offset();
    ASSERT_TRUE(AppendSingle(*seg, MakeWrite(1, {"SET", "b", "2"})).has_value());
  }

  // Truncate mid-way through the second entry's body.
  ::truncate(path.c_str(), static_cast<off_t>(good_offset) + 10);

  auto seg = Segment::Open(path, kDefaultMaxSize);
  ASSERT_TRUE(seg.has_value());
  EXPECT_EQ(seg->entry_count(), 1U);
  EXPECT_EQ(seg->next_seq(), 1U);
  EXPECT_EQ(seg->write_offset(), good_offset);

  // Verify file was physically truncated to the clean boundary.
  struct stat st{};
  ASSERT_EQ(::stat(path.c_str(), &st), 0);
  EXPECT_EQ(static_cast<size_t>(st.st_size), good_offset);

  auto read = seg->ReadEntries(kSegmentHeaderSize, 10);
  ASSERT_TRUE(read.has_value());
  ASSERT_EQ(read->entries.size(), 1U);
  ExpectWriteArgs(read->entries[0], {"SET", "a", "1"});
}

TEST_F(SegmentTest, TornTail_TruncatedCrc) {
  const auto path = SegPath();
  size_t good_offset = 0;
  {
    auto seg = Segment::Create(path, MakeHeader(0), kDefaultMaxSize);
    ASSERT_TRUE(seg.has_value());
    ASSERT_TRUE(AppendSingle(*seg, MakeWrite(0, {"SET", "a", "1"})).has_value());
    good_offset = seg->write_offset();
    ASSERT_TRUE(AppendSingle(*seg, MakeWrite(1, {"SET", "b", "2"})).has_value());
  }

  // Read file to find where the second entry's CRC starts, then truncate there.
  struct stat st{};
  ASSERT_EQ(::stat(path.c_str(), &st), 0);
  ::truncate(path.c_str(), st.st_size - 2);

  auto seg = Segment::Open(path, kDefaultMaxSize);
  ASSERT_TRUE(seg.has_value());
  EXPECT_EQ(seg->entry_count(), 1U);
  EXPECT_EQ(seg->next_seq(), 1U);
  EXPECT_EQ(seg->write_offset(), good_offset);
}

TEST_F(SegmentTest, TornTail_CrcMismatch) {
  const auto path = SegPath();
  size_t good_offset = 0;
  {
    auto seg = Segment::Create(path, MakeHeader(0), kDefaultMaxSize);
    ASSERT_TRUE(seg.has_value());
    ASSERT_TRUE(AppendSingle(*seg, MakeWrite(0, {"SET", "a", "1"})).has_value());
    good_offset = seg->write_offset();
    ASSERT_TRUE(AppendSingle(*seg, MakeWrite(1, {"SET", "b", "2"})).has_value());
  }

  // Corrupt a byte in the second entry's body.
  // NOLINTNEXTLINE(cppcoreguidelines-pro-type-vararg,cppcoreguidelines-init-variables)
  int fd = ::open(path.c_str(), O_RDWR);
  ASSERT_GE(fd, 0);
  uint8_t byte = 0;
  ASSERT_EQ(::pread(fd, &byte, 1, static_cast<off_t>(good_offset + 5)), 1);
  byte ^= 0x01;
  ASSERT_EQ(::pwrite(fd, &byte, 1, static_cast<off_t>(good_offset + 5)), 1);
  ::close(fd);

  auto seg = Segment::Open(path, kDefaultMaxSize);
  ASSERT_TRUE(seg.has_value());
  EXPECT_EQ(seg->entry_count(), 1U);
  EXPECT_EQ(seg->next_seq(), 1U);
  EXPECT_EQ(seg->write_offset(), good_offset);

  auto read = seg->ReadEntries(kSegmentHeaderSize, 10);
  ASSERT_TRUE(read.has_value());
  ASSERT_EQ(read->entries.size(), 1U);
  ExpectWriteArgs(read->entries[0], {"SET", "a", "1"});
}

TEST_F(SegmentTest, AppendAfterRecovery) {
  const auto path = SegPath();
  {
    auto seg = Segment::Create(path, MakeHeader(0), kDefaultMaxSize);
    ASSERT_TRUE(seg.has_value());
    ASSERT_TRUE(AppendSingle(*seg, MakeWrite(0, {"SET", "a", "1"})).has_value());
    ASSERT_TRUE(AppendSingle(*seg, MakeWrite(1, {"SET", "b", "2"})).has_value());
  }

  // Truncate mid-second-entry.
  struct stat st{};
  ASSERT_EQ(::stat(path.c_str(), &st), 0);
  ::truncate(path.c_str(), st.st_size - 5);

  auto seg = Segment::Open(path, kDefaultMaxSize);
  ASSERT_TRUE(seg.has_value());
  EXPECT_EQ(seg->entry_count(), 1U);
  EXPECT_EQ(seg->next_seq(), 1U);

  ASSERT_TRUE(AppendSingle(*seg, MakeWrite(1, {"SET", "c", "3"})).has_value());
  ASSERT_TRUE(AppendSingle(*seg, MakeWrite(2, {"SET", "d", "4"})).has_value());
  EXPECT_EQ(seg->entry_count(), 3U);
  EXPECT_EQ(seg->next_seq(), 3U);

  auto read = seg->ReadEntries(kSegmentHeaderSize, 10);
  ASSERT_TRUE(read.has_value());
  ASSERT_EQ(read->entries.size(), 3U);
  ExpectWriteArgs(read->entries[0], {"SET", "a", "1"});
  ExpectWriteArgs(read->entries[1], {"SET", "c", "3"});
  ExpectWriteArgs(read->entries[2], {"SET", "d", "4"});
}

TEST_F(SegmentTest, ReadFromMiddleOffset) {
  auto seg = Segment::Create(SegPath(), MakeHeader(0), kDefaultMaxSize);
  ASSERT_TRUE(seg.has_value());

  ASSERT_TRUE(AppendSingle(*seg, MakeWrite(0, {"SET", "a", "1"})).has_value());
  const size_t second_offset = seg->write_offset();
  ASSERT_TRUE(AppendSingle(*seg, MakeWrite(1, {"SET", "b", "2"})).has_value());
  ASSERT_TRUE(AppendSingle(*seg, MakeWrite(2, {"SET", "c", "3"})).has_value());

  auto read = seg->ReadEntries(second_offset, 10);
  ASSERT_TRUE(read.has_value());
  ASSERT_EQ(read->entries.size(), 2U);
  EXPECT_EQ(read->entries[0].seq, 1U);
  EXPECT_EQ(read->entries[1].seq, 2U);
  EXPECT_EQ(read->next_offset, seg->write_offset());
}

TEST_F(SegmentTest, ReadFromSequenceId) {
  auto seg = Segment::Create(SegPath(), MakeHeader(100), kDefaultMaxSize);
  ASSERT_TRUE(seg.has_value());

  for (core::SequenceId s = 100; s < 105; ++s) {
    ASSERT_TRUE(AppendSingle(*seg, MakeWrite(s, {"SET", "k", std::to_string(s)})).has_value());
  }

  auto read = seg->ReadEntriesFrom(102, 10);
  ASSERT_TRUE(read.has_value());
  ASSERT_EQ(read->entries.size(), 3U);
  EXPECT_EQ(read->entries[0].seq, 102U);
  EXPECT_EQ(read->entries[1].seq, 103U);
  EXPECT_EQ(read->entries[2].seq, 104U);
}

TEST_F(SegmentTest, ReadEntriesFromBeforeSegment) {
  auto seg = Segment::Create(SegPath(), MakeHeader(100), kDefaultMaxSize);
  ASSERT_TRUE(seg.has_value());
  ASSERT_TRUE(AppendSingle(*seg, MakeWrite(100, {"SET", "a", "1"})).has_value());

  auto read = seg->ReadEntriesFrom(50, 10);
  ASSERT_TRUE(read.has_value());
  ASSERT_EQ(read->entries.size(), 1U);
  EXPECT_EQ(read->entries[0].seq, 100U);
}

TEST_F(SegmentTest, ReadEntriesFromPastSegment) {
  auto seg = Segment::Create(SegPath(), MakeHeader(0), kDefaultMaxSize);
  ASSERT_TRUE(seg.has_value());
  ASSERT_TRUE(AppendSingle(*seg, MakeWrite(0, {"SET", "a", "1"})).has_value());

  auto read = seg->ReadEntriesFrom(999, 10);
  ASSERT_TRUE(read.has_value());
  EXPECT_TRUE(read->entries.empty());
}

TEST_F(SegmentTest, ReadAtWriteOffset) {
  auto seg = Segment::Create(SegPath(), MakeHeader(0), kDefaultMaxSize);
  ASSERT_TRUE(seg.has_value());
  ASSERT_TRUE(AppendSingle(*seg, MakeWrite(0, {"SET", "a", "1"})).has_value());

  auto read = seg->ReadEntries(seg->write_offset(), 10);
  ASSERT_TRUE(read.has_value());
  EXPECT_TRUE(read->entries.empty());
  EXPECT_EQ(read->next_offset, seg->write_offset());
}

TEST_F(SegmentTest, EmptySegment) {
  const auto path = SegPath();
  {
    auto seg = Segment::Create(path, MakeHeader(42), kDefaultMaxSize);
    ASSERT_TRUE(seg.has_value());
    EXPECT_EQ(seg->entry_count(), 0U);
    EXPECT_EQ(seg->next_seq(), 42U);
  }

  auto seg = Segment::Open(path, kDefaultMaxSize);
  ASSERT_TRUE(seg.has_value());
  EXPECT_EQ(seg->base_seq(), 42U);
  EXPECT_EQ(seg->next_seq(), 42U);
  EXPECT_EQ(seg->entry_count(), 0U);
  EXPECT_EQ(seg->write_offset(), kSegmentHeaderSize);

  auto read = seg->ReadEntries(kSegmentHeaderSize, 10);
  ASSERT_TRUE(read.has_value());
  EXPECT_TRUE(read->entries.empty());
}

TEST_F(SegmentTest, SegmentFullRejectsAppend) {
  // Create a segment barely large enough for the header + one small entry.
  auto seg = Segment::Create(SegPath(), MakeHeader(0), kSegmentHeaderSize + 60);
  ASSERT_TRUE(seg.has_value());

  auto r1 = AppendSingle(*seg, MakeWrite(0, {"SET", "k", "v"}));
  ASSERT_TRUE(r1.has_value());

  auto r2 = AppendSingle(*seg, MakeWrite(1, {"SET", "k2", "v2"}));
  ASSERT_FALSE(r2.has_value());
  EXPECT_EQ(r2.error().code(), core::ErrorCode::kResourceExhausted);

  EXPECT_EQ(seg->entry_count(), 1U);
  EXPECT_EQ(seg->next_seq(), 1U);
}

TEST_F(SegmentTest, SequenceOrderingEnforced) {
  auto seg = Segment::Create(SegPath(), MakeHeader(5), kDefaultMaxSize);
  ASSERT_TRUE(seg.has_value());

  ASSERT_TRUE(AppendSingle(*seg, MakeWrite(5, {"SET", "a", "1"})).has_value());

  auto bad = AppendSingle(*seg, MakeWrite(3, {"SET", "b", "2"}));
  ASSERT_FALSE(bad.has_value());
  EXPECT_EQ(bad.error().code(), core::ErrorCode::kInvalidArgument);

  auto gap = AppendSingle(*seg, MakeWrite(7, {"SET", "c", "3"}));
  ASSERT_FALSE(gap.has_value());
  EXPECT_EQ(gap.error().code(), core::ErrorCode::kInvalidArgument);

  ASSERT_TRUE(AppendSingle(*seg, MakeWrite(6, {"SET", "d", "4"})).has_value());
  EXPECT_EQ(seg->next_seq(), 7U);
}

TEST_F(SegmentTest, MoveSemantics) {
  auto seg = Segment::Create(SegPath(), MakeHeader(0), kDefaultMaxSize);
  ASSERT_TRUE(seg.has_value());
  ASSERT_TRUE(AppendSingle(*seg, MakeWrite(0, {"SET", "a", "1"})).has_value());

  Segment moved = std::move(*seg);
  EXPECT_EQ(moved.next_seq(), 1U);
  EXPECT_EQ(moved.entry_count(), 1U);

  ASSERT_TRUE(AppendSingle(moved, MakeWrite(1, {"SET", "b", "2"})).has_value());

  auto read = moved.ReadEntries(kSegmentHeaderSize, 10);
  ASSERT_TRUE(read.has_value());
  ASSERT_EQ(read->entries.size(), 2U);
}

TEST_F(SegmentTest, MoveAssignment) {
  auto seg1 = Segment::Create(SegPath("a.wal"), MakeHeader(0), kDefaultMaxSize);
  auto seg2 = Segment::Create(SegPath("b.wal"), MakeHeader(10), kDefaultMaxSize);
  ASSERT_TRUE(seg1.has_value());
  ASSERT_TRUE(seg2.has_value());

  ASSERT_TRUE(AppendSingle(*seg1, MakeWrite(0, {"SET", "a", "1"})).has_value());

  *seg2 = std::move(*seg1);
  EXPECT_EQ(seg2->base_seq(), 0U);
  EXPECT_EQ(seg2->next_seq(), 1U);
}

TEST_F(SegmentTest, AllEntryTypes) {
  auto seg = Segment::Create(SegPath(), MakeHeader(0), kDefaultMaxSize);
  ASSERT_TRUE(seg.has_value());

  ASSERT_TRUE(AppendSingle(*seg, MakeWrite(0, {"SET", "a", "1"})).has_value());
  ASSERT_TRUE(
      AppendSingle(*seg, MakeConditional(1, {"SET", "b", "2", "NX"}, core::PredicateFlags::kNx))
          .has_value());
  ASSERT_TRUE(AppendSingle(*seg, MakeResolved(2, 1, core::Decision::kApply)).has_value());

  auto read = seg->ReadEntries(kSegmentHeaderSize, 10);
  ASSERT_TRUE(read.has_value());
  ASSERT_EQ(read->entries.size(), 3U);

  auto* w = std::get_if<core::entry::Write>(&read->entries[0].payload);
  ASSERT_NE(w, nullptr);
  EXPECT_EQ(w->cmd.args, (std::vector<std::string>{"SET", "a", "1"}));

  auto* c = std::get_if<core::entry::Conditional>(&read->entries[1].payload);
  ASSERT_NE(c, nullptr);
  EXPECT_EQ(c->cmd.args, (std::vector<std::string>{"SET", "b", "2", "NX"}));
  EXPECT_EQ(c->flags, core::PredicateFlags::kNx);

  auto* r = std::get_if<core::entry::Resolved>(&read->entries[2].payload);
  ASSERT_NE(r, nullptr);
  EXPECT_EQ(r->ref, 1U);
  EXPECT_EQ(r->decision, core::Decision::kApply);
  ASSERT_TRUE(r->materialised_op.has_value());
  EXPECT_TRUE(r->return_value.IsSimpleString());
}

TEST_F(SegmentTest, MaxCountRespected) {
  auto seg = Segment::Create(SegPath(), MakeHeader(0), kDefaultMaxSize);
  ASSERT_TRUE(seg.has_value());

  for (core::SequenceId s = 0; s < 10; ++s) {
    ASSERT_TRUE(AppendSingle(*seg, MakeWrite(s, {"SET", "k", std::to_string(s)})).has_value());
  }

  auto read = seg->ReadEntries(kSegmentHeaderSize, 3);
  ASSERT_TRUE(read.has_value());
  ASSERT_EQ(read->entries.size(), 3U);
  EXPECT_EQ(read->entries[0].seq, 0U);
  EXPECT_EQ(read->entries[2].seq, 2U);
  EXPECT_LT(read->next_offset, seg->write_offset());

  auto read2 = seg->ReadEntries(read->next_offset, 3);
  ASSERT_TRUE(read2.has_value());
  ASSERT_EQ(read2->entries.size(), 3U);
  EXPECT_EQ(read2->entries[0].seq, 3U);
  EXPECT_EQ(read2->entries[2].seq, 5U);
}

TEST_F(SegmentTest, SpaceRemainingTracksWrites) {
  auto seg = Segment::Create(SegPath(), MakeHeader(0), kDefaultMaxSize);
  ASSERT_TRUE(seg.has_value());

  const size_t initial = seg->SpaceRemaining();
  EXPECT_EQ(initial, kDefaultMaxSize - kSegmentHeaderSize);

  auto result = AppendSingle(*seg, MakeWrite(0, {"SET", "a", "1"}));
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(seg->SpaceRemaining(), initial - *result);
}

TEST_F(SegmentTest, HeaderPreservedAcrossOpenClose) {
  const auto path = SegPath();
  auto header = MakeHeader(500, 7);
  {
    auto seg = Segment::Create(path, header, kDefaultMaxSize);
    ASSERT_TRUE(seg.has_value());
  }

  auto seg = Segment::Open(path, kDefaultMaxSize);
  ASSERT_TRUE(seg.has_value());
  EXPECT_EQ(seg->header().shard_id, 7U);
  EXPECT_EQ(seg->header().base_seq, 500U);
  EXPECT_EQ(seg->header().format_major, kWalFormatMajor);
  EXPECT_EQ(seg->header().format_minor, kWalFormatMinor);

  auto original_us =
      std::chrono::duration_cast<std::chrono::microseconds>(header.created_at.time_since_epoch())
          .count();
  auto read_us = std::chrono::duration_cast<std::chrono::microseconds>(
                     seg->header().created_at.time_since_epoch())
                     .count();
  EXPECT_EQ(original_us, read_us);
}

TEST_F(SegmentTest, CreateFailsIfFileExists) {
  const auto path = SegPath();
  auto seg1 = Segment::Create(path, MakeHeader(0), kDefaultMaxSize);
  ASSERT_TRUE(seg1.has_value());

  auto seg2 = Segment::Create(path, MakeHeader(0), kDefaultMaxSize);
  ASSERT_FALSE(seg2.has_value());
  EXPECT_EQ(seg2.error().code(), core::ErrorCode::kInternal);
}

TEST_F(SegmentTest, ReadEntriesFromWithBaseSeq) {
  auto seg = Segment::Create(SegPath(), MakeHeader(100), kDefaultMaxSize);
  ASSERT_TRUE(seg.has_value());

  for (core::SequenceId s = 100; s < 105; ++s) {
    ASSERT_TRUE(AppendSingle(*seg, MakeWrite(s, {"SET", "k", std::to_string(s)})).has_value());
  }

  auto read = seg->ReadEntriesFrom(100, 10);
  ASSERT_TRUE(read.has_value());
  ASSERT_EQ(read->entries.size(), 5U);
  EXPECT_EQ(read->entries[0].seq, 100U);
}

TEST_F(SegmentTest, InvalidFileOffsetRejected) {
  auto seg = Segment::Create(SegPath(), MakeHeader(0), kDefaultMaxSize);
  ASSERT_TRUE(seg.has_value());

  auto too_low = seg->ReadEntries(0, 10);
  ASSERT_FALSE(too_low.has_value());
  EXPECT_EQ(too_low.error().code(), core::ErrorCode::kInvalidArgument);

  auto too_high = seg->ReadEntries(seg->write_offset() + 100, 10);
  ASSERT_FALSE(too_high.has_value());
  EXPECT_EQ(too_high.error().code(), core::ErrorCode::kInvalidArgument);
}

TEST_F(SegmentTest, FsyncSucceedsOnOpenSegment) {
  auto seg = Segment::Create(SegPath(), MakeHeader(0), kDefaultMaxSize);
  ASSERT_TRUE(seg.has_value());
  ASSERT_TRUE(AppendSingle(*seg, MakeWrite(0, {"SET", "a", "1"})).has_value());
  EXPECT_TRUE(seg->Fsync().has_value());
}

TEST_F(SegmentTest, SealTruncatesToWriteOffset) {
  const auto path = SegPath();
  size_t sealed_offset = 0;
  {
    auto seg = Segment::Create(path, MakeHeader(0), kDefaultMaxSize);
    ASSERT_TRUE(seg.has_value());
    ASSERT_TRUE(AppendSingle(*seg, MakeWrite(0, {"SET", "a", "1"})).has_value());
    ASSERT_TRUE(AppendSingle(*seg, MakeWrite(1, {"SET", "b", "2"})).has_value());
    sealed_offset = seg->write_offset();
    ASSERT_TRUE(seg->Seal().has_value());
    EXPECT_TRUE(seg->sealed());
  }

  struct stat st{};
  ASSERT_EQ(::stat(path.c_str(), &st), 0);
  EXPECT_EQ(static_cast<size_t>(st.st_size), sealed_offset);
}

TEST_F(SegmentTest, AppendRejectedAfterSeal) {
  auto seg = Segment::Create(SegPath(), MakeHeader(0), kDefaultMaxSize);
  ASSERT_TRUE(seg.has_value());
  ASSERT_TRUE(AppendSingle(*seg, MakeWrite(0, {"SET", "a", "1"})).has_value());
  ASSERT_TRUE(seg->Seal().has_value());

  auto appended = AppendSingle(*seg, MakeWrite(1, {"SET", "b", "2"}));
  ASSERT_FALSE(appended.has_value());
  EXPECT_EQ(appended.error().code(), core::ErrorCode::kInvalidArgument);
}

TEST_F(SegmentTest, RecoveryKeepsCompleteBatch) {
  const auto path = SegPath();
  size_t expected_offset = 0;
  {
    auto seg = Segment::Create(path, MakeHeader(0), kDefaultMaxSize);
    ASSERT_TRUE(seg.has_value());
    // Batch of 3 entries with batch_last_seq = 2 shared.
    ASSERT_TRUE(seg->Append(MakeWrite(0, {"SET", "a", "1"}), 2).has_value());
    ASSERT_TRUE(seg->Append(MakeWrite(1, {"SET", "b", "2"}), 2).has_value());
    ASSERT_TRUE(seg->Append(MakeWrite(2, {"SET", "c", "3"}), 2).has_value());
    expected_offset = seg->write_offset();
  }

  auto seg = Segment::Open(path, kDefaultMaxSize);
  ASSERT_TRUE(seg.has_value());
  EXPECT_EQ(seg->entry_count(), 3U);
  EXPECT_EQ(seg->next_seq(), 3U);
  EXPECT_EQ(seg->write_offset(), expected_offset);
}

TEST_F(SegmentTest, RecoveryTruncatesIncompleteBatch) {
  const auto path = SegPath();
  size_t before_batch_offset = 0;
  {
    auto seg = Segment::Create(path, MakeHeader(0), kDefaultMaxSize);
    ASSERT_TRUE(seg.has_value());
    // One complete single-entry write, then a 3-entry batch we crash mid-way
    // through by writing just the first entry of the batch.
    ASSERT_TRUE(AppendSingle(*seg, MakeWrite(0, {"SET", "pre", "ok"})).has_value());
    before_batch_offset = seg->write_offset();
    // Only the first entry of a 3-entry batch lands.
    ASSERT_TRUE(seg->Append(MakeWrite(1, {"SET", "a", "1"}), 3).has_value());
  }

  auto seg = Segment::Open(path, kDefaultMaxSize);
  ASSERT_TRUE(seg.has_value());
  // The incomplete batch entry is truncated; only the pre-batch entry remains.
  EXPECT_EQ(seg->entry_count(), 1U);
  EXPECT_EQ(seg->next_seq(), 1U);
  EXPECT_EQ(seg->write_offset(), before_batch_offset);

  // File was physically truncated.
  struct stat st{};
  ASSERT_EQ(::stat(path.c_str(), &st), 0);
  EXPECT_EQ(static_cast<size_t>(st.st_size), before_batch_offset);
}

TEST_F(SegmentTest, RecoveryKeepsCompleteBatchesAfterIncompleteTruncation) {
  const auto path = SegPath();
  size_t checkpoint_offset = 0;
  {
    auto seg = Segment::Create(path, MakeHeader(0), kDefaultMaxSize);
    ASSERT_TRUE(seg.has_value());
    // Complete batch of 2.
    ASSERT_TRUE(seg->Append(MakeWrite(0, {"SET", "a", "1"}), 1).has_value());
    ASSERT_TRUE(seg->Append(MakeWrite(1, {"SET", "b", "2"}), 1).has_value());
    checkpoint_offset = seg->write_offset();
    // Begin a batch of 3, only the first entry lands.
    ASSERT_TRUE(seg->Append(MakeWrite(2, {"SET", "c", "3"}), 4).has_value());
  }

  auto seg = Segment::Open(path, kDefaultMaxSize);
  ASSERT_TRUE(seg.has_value());
  EXPECT_EQ(seg->entry_count(), 2U);
  EXPECT_EQ(seg->next_seq(), 2U);
  EXPECT_EQ(seg->write_offset(), checkpoint_offset);
}

TEST_F(SegmentTest, SealIsIdempotent) {
  auto seg = Segment::Create(SegPath(), MakeHeader(0), kDefaultMaxSize);
  ASSERT_TRUE(seg.has_value());
  ASSERT_TRUE(seg->Seal().has_value());
  EXPECT_TRUE(seg->Seal().has_value());
  EXPECT_TRUE(seg->sealed());
}

TEST_F(SegmentTest, AppendEncodedAcceptsPreEncodedBytes) {
  // AppendEncoded is the path ShardState uses on the hot write path: encode
  // once into a buffer, hand the bytes to the segment. Verify it produces the
  // same on-disk state as the encode-inside wrapper.
  auto seg = Segment::Create(SegPath(), MakeHeader(0), kDefaultMaxSize);
  ASSERT_TRUE(seg.has_value());

  const auto entry = MakeWrite(0, {"SET", "key", "value"});
  std::vector<std::byte> buf;
  EncodeWalEntry(entry, entry.seq, buf);

  auto wrote = seg->AppendEncoded(buf, entry.seq);
  ASSERT_TRUE(wrote.has_value());
  EXPECT_EQ(*wrote, buf.size());
  EXPECT_EQ(seg->next_seq(), 1U);
  EXPECT_EQ(seg->entry_count(), 1U);

  auto read = seg->ReadEntries(kSegmentHeaderSize, 10);
  ASSERT_TRUE(read.has_value());
  ASSERT_EQ(read->entries.size(), 1U);
  ExpectWriteArgs(read->entries[0], {"SET", "key", "value"});
}

TEST_F(SegmentTest, AppendEncodedRejectsOutOfOrderSeq) {
  auto seg = Segment::Create(SegPath(), MakeHeader(10), kDefaultMaxSize);
  ASSERT_TRUE(seg.has_value());

  const auto entry = MakeWrite(10, {"SET", "k", "v"});
  std::vector<std::byte> buf;
  EncodeWalEntry(entry, entry.seq, buf);

  auto r = seg->AppendEncoded(buf, 99);
  ASSERT_FALSE(r.has_value());
  EXPECT_EQ(r.error().code(), core::ErrorCode::kInvalidArgument);
}

}  // namespace
}  // namespace abyss::queue
