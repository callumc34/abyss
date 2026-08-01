// libFuzzer harness for the WAL segment recovery/replay loop.
//
// wal_decoder_fuzz decodes one isolated frame. This harness materialises a whole
// segment file and drives Segment::Open — the recovery scan that decides between
// truncating a torn tail and fail-stopping on genuine corruption — then walks the
// replay reads over whatever survived.
//
// The mode byte reaches both branches of that decision:
//   * raw bytes leave the CRC adversarial, so failures land on the torn-tail path
//     (incomplete frame or CRC mismatch) which recovery truncates;
//   * the reframing mode wraps fuzzer-chosen bodies in a *correct* CRC, the only
//     practical way to reach the CRC-valid-but-structurally-invalid branch, which
//     is corruption of durably-acked data and must fail-stop with kCorruption.
//
// The harness deliberately asserts nothing about which branch fires for a given
// input: arbitrary bytes can legitimately land on either. The contract under test
// is the absence of crashes, hangs and out-of-bounds reads, which the sanitizers
// judge.
//
// POSIX/Clang only: the libFuzzer entrypoint depends on -fsanitize=fuzzer.

#include <unistd.h>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <ios>
#include <span>
#include <string>
#include <system_error>
#include <vector>

#include "abyss/queue/segment_header.h"
#include "abyss/queue/wal_entry.h"
#include "binary_io.h"
#include "crc32c.h"
#include "segment.h"

namespace queue = abyss::queue;

namespace {

constexpr uint8_t kModeSynthHeader = 0x01;
constexpr uint8_t kModeReframe = 0x02;
constexpr uint8_t kModeTornTail = 0x04;

constexpr size_t kMaxSegmentBytes = size_t{1} << 20;
constexpr size_t kMaxRecordBytes = 64;
constexpr size_t kReplayBatch = 32;
constexpr int kMaxReplayPasses = 64;

// Declared before the cleanup object so it outlives it at exit.
std::string g_segment_path;

class SegmentFileCleanup {
 public:
  SegmentFileCleanup() = default;
  ~SegmentFileCleanup() {
    if (g_segment_path.empty()) return;
    std::error_code ec;
    std::filesystem::remove(g_segment_path, ec);
  }
  SegmentFileCleanup(const SegmentFileCleanup&) = delete;
  SegmentFileCleanup& operator=(const SegmentFileCleanup&) = delete;
  SegmentFileCleanup(SegmentFileCleanup&&) = delete;
  SegmentFileCleanup& operator=(SegmentFileCleanup&&) = delete;
};

const SegmentFileCleanup kSegmentFileCleanup;

std::span<const std::byte> AsBytes(const uint8_t* data, size_t size) {
  return {reinterpret_cast<const std::byte*>(data), size};
}

// Wraps fuzzer-chosen bodies in the real WAL framing with a *correct* CRC, so
// the structural decoder is reached with the CRC already validated.
void AppendReframed(std::span<const uint8_t> body, std::vector<std::byte>& out) {
  for (size_t offset = 0; offset < body.size();) {
    const size_t width =
        std::min(1 + ((static_cast<size_t>(body[offset]) ^ (offset * 31)) % kMaxRecordBytes),
                 body.size() - offset);
    const auto record = AsBytes(body.data() + offset, width);
    queue::binary::WriteU32LE(out, static_cast<uint32_t>(width));
    queue::binary::AppendBytes(out, record.data(), width);
    queue::binary::WriteU32LE(out, queue::Crc32c(record));
    offset += width;
  }
}

bool WriteSegmentFile(const std::vector<std::byte>& bytes) {
  std::ofstream out(g_segment_path, std::ios::binary | std::ios::trunc);
  if (!out) return false;
  if (!bytes.empty()) {
    out.write(reinterpret_cast<const char*>(bytes.data()),
              static_cast<std::streamsize>(bytes.size()));
  }
  out.close();
  return out.good();
}

}  // namespace

extern "C" int LLVMFuzzerInitialize(int* /*argc*/, char*** /*argv*/) {
  std::error_code ec;
  const auto dir = std::filesystem::temp_directory_path(ec);
  if (ec) return 0;
  g_segment_path =
      (dir / ("abyss_fuzz_wal_replay_" + std::to_string(::getpid()) + ".seg")).string();
  return 0;
}

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
  if (size < 2 || g_segment_path.empty()) return 0;

  const uint8_t mode = data[0];
  const std::span<const uint8_t> body(data + 1, size - 1);

  std::vector<std::byte> file;
  file.reserve(size + queue::kSegmentHeaderSize);

  if ((mode & kModeSynthHeader) != 0) {
    queue::SegmentHeader header;
    header.format_major = queue::kWalFormatMajor;
    header.format_minor = queue::kWalFormatMinor;
    queue::EncodeSegmentHeader(header, file);
  }

  if ((mode & kModeReframe) != 0) {
    AppendReframed(body, file);
  } else {
    queue::binary::AppendBytes(file, body.data(), body.size());
  }

  if ((mode & kModeTornTail) != 0 && !file.empty()) {
    const size_t chop = std::min<size_t>(1 + ((mode >> 3) & 0x1FU), file.size());
    file.resize(file.size() - chop);
  }
  if (file.size() > kMaxSegmentBytes) {
    file.resize(kMaxSegmentBytes);
  }

  if (!WriteSegmentFile(file)) return 0;

  auto segment = queue::Segment::Open(g_segment_path, kMaxSegmentBytes);
  if (!segment.has_value()) return 0;

  size_t offset = queue::kSegmentHeaderSize;
  for (int pass = 0; pass < kMaxReplayPasses; ++pass) {
    auto read = segment->ReadEntries(offset, kReplayBatch);
    if (!read.has_value() || read->next_offset <= offset) break;
    offset = read->next_offset;
  }
  // NOLINTNEXTLINE(bugprone-unused-return-value)
  (void)segment->ReadEntriesFrom(segment->base_seq(), kReplayBatch);
  return 0;
}
