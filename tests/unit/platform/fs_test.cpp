#include "abyss/platform/fs.h"

#include <gtest/gtest.h>

#include <cerrno>
#include <string>

#include "temp_dir.h"

namespace {

namespace fs = abyss::platform::fs;
namespace fst = abyss::platform::fs::testing;
using abyss::testing::TempDir;

// Cross-platform parity: the durability primitive's basic file round-trip
// (open/pwrite/fsync-durable/pread) must behave identically on every OS. Before
// this cluster the primitive had no unit coverage at all (G4).
TEST(FsTest, OpenWriteDurableFsyncReadRoundtrip) {
  TempDir dir("fs");
  const auto path = dir.Sub("roundtrip.bin");

  auto file = fs::Open(
      path, fs::OpenOptions{.mode = fs::OpenMode::kWrite, .create = true, .truncate = true});
  ASSERT_TRUE(file.has_value());
  const std::string payload = "abyssal-durability";
  ASSERT_TRUE(fs::Pwrite(*file, payload.data(), payload.size(), 0).has_value());
  ASSERT_TRUE(fs::Fsync(*file, fs::SyncMode::kDurable).has_value());
  file->Close();

  auto rf = fs::Open(path, fs::OpenOptions{.mode = fs::OpenMode::kRead});
  ASSERT_TRUE(rf.has_value());
  std::string buf(payload.size(), '\0');
  auto n = fs::Pread(*rf, buf.data(), buf.size(), 0);
  ASSERT_TRUE(n.has_value());
  EXPECT_EQ(*n, payload.size());
  EXPECT_EQ(buf, payload);
}

// G5: FsyncDir reports an explicit synced/unsupported outcome rather than
// swallowing "unsupported" into a bare success. On a normal volume it is synced.
TEST(FsTest, FsyncDirReturnsSyncedOnNormalVolume) {
  TempDir dir("fs");
  auto out = fs::FsyncDir(dir.Path());
  ASSERT_TRUE(out.has_value());
  EXPECT_EQ(*out, fs::DirSyncOutcome::kSynced);
}

// G5: the startup probe reports the volume's real durability posture so it is
// observable before any data loss (invariant 5).
TEST(FsTest, ProbeDurabilityReportsCapability) {
  TempDir dir("fs");
  auto cap = fs::ProbeDurability(dir.Path());
  ASSERT_TRUE(cap.has_value());
  EXPECT_TRUE(cap->dir_sync_supported);
#ifdef __APPLE__
  EXPECT_EQ(cap->backend, fs::FsyncBackend::kFullFsync);
#elifdef _WIN32
  EXPECT_EQ(cap->backend, fs::FsyncBackend::kFlushFileBuffers);
#else
  EXPECT_EQ(cap->backend, fs::FsyncBackend::kFsync);
#endif
}

#ifdef __APPLE__
// NET-5: on Apple, kDurable MUST select F_FULLFSYNC (the only call that pushes
// the drive cache to stable media); kFlushOnly MUST NOT. Pre-fix, Fsync was a
// plain ::fsync that overstated durability on macOS.
TEST(FsTest, DurableFsyncSelectsFullFsyncOnApple) {
  TempDir dir("fs");
  const auto path = dir.Sub("ff.bin");
  auto file = fs::Open(
      path, fs::OpenOptions{.mode = fs::OpenMode::kWrite, .create = true, .truncate = true});
  ASSERT_TRUE(file.has_value());

  fst::ResetFullFsyncCallCount();
  ASSERT_TRUE(fs::Fsync(*file, fs::SyncMode::kDurable).has_value());
  EXPECT_GT(fst::FullFsyncCallCount(), 0U);

  fst::ResetFullFsyncCallCount();
  ASSERT_TRUE(fs::Fsync(*file, fs::SyncMode::kFlushOnly).has_value());
  EXPECT_EQ(fst::FullFsyncCallCount(), 0U);
}
#endif  // __APPLE__

#ifndef _WIN32
namespace {
int g_seam_calls = 0;
int FakeEopnotsupp(int /*fd*/) {
  ++g_seam_calls;
  errno = EOPNOTSUPP;
  return -1;
}
int FakeEio(int /*fd*/) {
  ++g_seam_calls;
  errno = EIO;
  return -1;
}
}  // namespace

#ifdef __APPLE__
// NET-5 (failure injection): when F_FULLFSYNC is unsupported on the volume,
// kDurable falls back to ::fsync and still succeeds, counting one degraded
// fallback. A genuine error (EIO) must propagate, never be masked by a fallback
// that could spuriously succeed and overstate durability.
TEST(FsTest, DurableFsyncFallsBackOnUnsupportedButPropagatesRealError) {
  TempDir dir("fs");
  const auto path = dir.Sub("fb.bin");
  auto file = fs::Open(
      path, fs::OpenOptions{.mode = fs::OpenMode::kWrite, .create = true, .truncate = true});
  ASSERT_TRUE(file.has_value());

  fst::ResetDurableFsyncFallbackCount();
  g_seam_calls = 0;
  fst::SetFullFsyncForTesting(&FakeEopnotsupp);
  EXPECT_TRUE(fs::Fsync(*file, fs::SyncMode::kDurable).has_value());
  EXPECT_EQ(g_seam_calls, 1);
  EXPECT_EQ(fst::DurableFsyncFallbackCount(), 1U);

  fst::SetFullFsyncForTesting(&FakeEio);
  auto r = fs::Fsync(*file, fs::SyncMode::kDurable);
  EXPECT_FALSE(r.has_value());
  // The EIO path must NOT count as a tolerable fallback.
  EXPECT_EQ(fst::DurableFsyncFallbackCount(), 1U);

  fst::SetFullFsyncForTesting(nullptr);  // restore the real implementation
}
#endif  // __APPLE__
#endif  // !_WIN32

}  // namespace
