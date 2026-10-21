// libFuzzer harness for WAL format 2 log recovery.
//
// Initialisation builds a real log with Log: segment 0 holds a valid
// header and a few real frames, segments 1 and 2 are spares. Each input
// is written into frame space after those frames, then Log::Open
// recovers the directory and the harness walks what it kept.
//
// The mode byte picks how the input lands:
//   * raw bytes leave every CRC adversarial (the salt is random per
//     segment life), so they exercise the end-of-log and torn paths;
//   * kModeReframe wraps fuzzer-chosen bodies in frames with the right
//     gen and a correct CRC, the only practical way to reach recovery's
//     semantic checks and the CRC-valid-but-unparseable payload;
//   * kModeSingleBatch makes each reframed frame a whole batch;
//   * kModeTear flips a bit in the last reframed frame after sealing;
//   * kModeNextSegment pads segment 0 and writes into segment 1.
//
// Contract: Open succeeds or fails with kCorruption. After success
// every reported frame reads back CRC-verified, a walk with
// Cursor::Next visits exactly the reported frames, and each decodes or
// fails with kCorruption. Violations abort; libFuzzer reports them.
//
// POSIX and Clang only: the entrypoint needs -fsanitize=fuzzer.

#include <unistd.h>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <ios>
#include <iostream>
#include <optional>
#include <span>
#include <string>
#include <system_error>
#include <vector>

#include "abyss/core/queue_entry.h"
#include "abyss/core/result.h"
#include "abyss/queue/frame.h"
#include "abyss/queue/log.h"
#include "binary_io.h"
#include "crc32c.h"
#include "segment_header_v2.h"

namespace queue = abyss::queue;
namespace frame = abyss::queue::frame;

namespace {

constexpr uint8_t kModeReframe = 0x01;
constexpr uint8_t kModeSingleBatch = 0x02;
constexpr uint8_t kModeTear = 0x04;
constexpr uint8_t kModeNextSegment = 0x08;

constexpr uint32_t kShards = 4;
constexpr std::size_t kFrameSpace = std::size_t{16} << 10;
constexpr std::size_t kSegmentBytes = queue::kLogSegmentHeaderBytes + kFrameSpace;
constexpr uint64_t kWindowBytes = 4096;
constexpr std::size_t kMaxPayload = 64;
constexpr std::size_t kBodyAt = frame::kCommitBytes + frame::kCrcBytes;
constexpr std::size_t kBatchRestAt = 16;
// Every frame is at least kMinFrameBytes, so a walk is bounded.
constexpr std::size_t kMaxSteps = (3 * kFrameSpace) / frame::kMinFrameBytes;

void Require(bool holds) {
  if (!holds) std::abort();
}

struct Segment {
  std::string name;
  std::vector<std::byte> bytes;
  uint64_t salt = 0;
};

// The log Initialize built, as bytes on disk.
struct Template {
  std::filesystem::path root;
  std::vector<Segment> segments;
  // Frame-space offset in segment 0 just past the real frames.
  std::size_t frames_end = 0;
};

// Declared before the cleanup object so it outlives it at exit.
Template g_template;

class TemplateCleanup {
 public:
  TemplateCleanup() = default;
  ~TemplateCleanup() {
    if (g_template.root.empty()) return;
    std::error_code ec;
    std::filesystem::remove_all(g_template.root, ec);
  }
  TemplateCleanup(const TemplateCleanup&) = delete;
  TemplateCleanup& operator=(const TemplateCleanup&) = delete;
  TemplateCleanup(TemplateCleanup&&) = delete;
  TemplateCleanup& operator=(TemplateCleanup&&) = delete;
};

const TemplateCleanup kTemplateCleanup;

queue::LogConfig ConfigFor(const std::filesystem::path& dir) {
  return queue::LogConfig{
      .dir = dir,
      .shard_count = kShards,
      .segment_size_bytes = kSegmentBytes,
      .durability_window_bytes = kWindowBytes,
  };
}

abyss::core::QueueEntry Entry(abyss::core::SequenceId seq, const std::string& key) {
  return abyss::core::QueueEntry{
      .seq = seq,
      .appended_at = abyss::core::WallClock::now(),
      .payload = abyss::core::entry::Write{.cmd = abyss::core::RespCommand{{"SET", key, "v"}}},
  };
}

// Reserves `entries` as one batch of `shard` and commits each frame.
std::size_t AppendBatch(queue::Log& log, abyss::core::ShardId shard,
                        std::span<const abyss::core::QueueEntry> entries) {
  std::vector<std::byte> frames;
  std::vector<std::size_t> sizes;
  for (const auto& entry : entries) sizes.push_back(frame::EncodeEntry(entry, shard, frames));
  frame::CloseBatch(frames);
  auto reserved = log.Reserve(static_cast<uint32_t>(frames.size()));
  Require(reserved.has_value());
  std::size_t off = 0;
  for (const std::size_t size : sizes) {
    log.Commit(queue::Log::Reservation{.pos = reserved->pos + off,
                                       .size = static_cast<uint32_t>(size),
                                       .gen = reserved->gen,
                                       .salt = reserved->salt,
                                       .dst = reserved->dst + off},
               std::span<const std::byte>(frames).subspan(off, size));
    off += size;
  }
  return frames.size();
}

std::vector<std::byte> ReadFile(const std::filesystem::path& path) {
  std::error_code ec;
  const auto size = std::filesystem::file_size(path, ec);
  Require(!ec);
  std::vector<std::byte> bytes(size);
  std::ifstream in(path, std::ios::binary);
  in.read(reinterpret_cast<char*>(bytes.data()), static_cast<std::streamsize>(size));
  Require(in.good());
  return bytes;
}

bool WriteFile(const std::filesystem::path& path, const std::vector<std::byte>& bytes) {
  std::ofstream out(path, std::ios::binary | std::ios::trunc);
  out.write(reinterpret_cast<const char*>(bytes.data()),
            static_cast<std::streamsize>(bytes.size()));
  out.close();
  return out.good();
}

void BuildTemplate() {
  std::error_code ec;
  const auto dir = g_template.root / "template";
  std::filesystem::create_directories(dir, ec);
  Require(!ec);
  {
    auto opened = queue::Log::Open(ConfigFor(dir), {});
    Require(opened.has_value());
    queue::Log& log = **opened;
    std::size_t end = AppendBatch(log, 0, std::vector{Entry(0, "a")});
    end += AppendBatch(log, 1, std::vector{Entry(0, "b")});
    end += AppendBatch(log, 2, std::vector{Entry(0, "c"), Entry(1, "d")});
    end += AppendBatch(log, 0, std::vector{Entry(1, "e")});
    log.AwaitFilled(end);
    g_template.frames_end = end;
  }
  for (const auto& file : std::filesystem::directory_iterator(dir)) {
    Segment segment{.name = file.path().filename().string(), .bytes = ReadFile(file.path())};
    Require(segment.bytes.size() == kSegmentBytes);
    auto header = queue::DecodeLogSegmentHeader(segment.bytes);
    Require(header.has_value());
    segment.salt = header->salt;
    g_template.segments.push_back(std::move(segment));
  }
  std::ranges::sort(g_template.segments, {}, &Segment::name);
  Require(g_template.segments.size() >= 2);
}

// Fuzzer-chosen bodies framed with `gen` and a CRC valid under `salt`.
std::vector<std::byte> Reframe(std::span<const std::byte> input, uint32_t gen, uint64_t salt,
                               uint8_t mode) {
  std::vector<std::byte> out;
  std::size_t last = 0;
  for (std::size_t off = 0; off < input.size();) {
    const std::size_t len =
        frame::kHeaderBytes + (static_cast<std::size_t>(input[off]) % (kMaxPayload + 1));
    ++off;
    std::vector<std::byte> body(len);
    const std::size_t take = std::min(len, input.size() - off);
    std::copy_n(input.begin() + static_cast<std::ptrdiff_t>(off), take, body.begin());
    off += take;

    last = out.size();
    out.resize(last + frame::FrameSize(len));
    std::byte* at = out.data() + last;
    if ((mode & kModeSingleBatch) != 0) {
      queue::binary::StoreLE<uint64_t>(body.data() + kBatchRestAt, frame::FrameSize(len));
    }
    std::ranges::copy(body, at + kBodyAt);
    const uint64_t word = frame::CommitWord(static_cast<uint32_t>(len), gen);
    const std::size_t covered =
        static_cast<frame::Kind>(body[0]) == frame::Kind::kPadding ? frame::kHeaderBytes : len;
    const auto covered_body = std::span<const std::byte>(body).first(covered);
    queue::binary::StoreLE(at + frame::kCommitBytes,
                           frame::SealCrc(queue::Crc32c(covered_body), salt, word));
    queue::binary::StoreLE(at, word);
  }
  if ((mode & kModeTear) != 0 && !out.empty()) {
    out[last + kBodyAt + frame::kHeaderBytes - 1] ^= std::byte{0x01};
  }
  return out;
}

}  // namespace

extern "C" int LLVMFuzzerInitialize(int* /*argc*/, char*** /*argv*/) {
  std::error_code ec;
  const auto tmp = std::filesystem::temp_directory_path(ec);
  if (ec) {
    std::cerr << "log_recovery_fuzz: no temp directory: " << ec.message() << '\n';
    std::abort();
  }
  g_template.root = tmp / ("abyss_fuzz_log_recovery_" + std::to_string(::getpid()));
  std::filesystem::remove_all(g_template.root, ec);
  BuildTemplate();
  return 0;
}

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
  if (size < 1) return 0;
  const auto mode = static_cast<uint8_t>(data[0]);
  const std::span<const std::byte> input(reinterpret_cast<const std::byte*>(data) + 1, size - 1);

  // Segments are named by ordinal, which is also their gen.
  std::vector<Segment> segments = g_template.segments;
  uint32_t gen = 0;
  std::size_t at = g_template.frames_end;
  if ((mode & kModeNextSegment) != 0) {
    std::byte* pad = segments[0].bytes.data() + queue::kLogSegmentHeaderBytes + at;
    const uint64_t word =
        frame::EncodePadding(kFrameSpace - at, 0, segments[0].salt, {pad, kFrameSpace - at});
    queue::binary::StoreLE(pad, word);
    gen = 1;
    at = 0;
  }
  Segment& target = segments[gen];
  const std::vector<std::byte> region = (mode & kModeReframe) != 0
                                            ? Reframe(input, gen, target.salt, mode)
                                            : std::vector<std::byte>(input.begin(), input.end());
  const std::size_t fits = std::min(region.size(), kFrameSpace - at);
  std::copy_n(
      region.begin(), fits,
      target.bytes.begin() + static_cast<std::ptrdiff_t>(queue::kLogSegmentHeaderBytes + at));

  const auto dir = g_template.root / "run";
  std::error_code ec;
  std::filesystem::remove_all(dir, ec);
  std::filesystem::create_directories(dir, ec);
  Require(!ec);
  for (const auto& segment : segments) Require(WriteFile(dir / segment.name, segment.bytes));

  std::vector<queue::RecoveredFrame> recovered;
  auto opened = queue::Log::Open(
      ConfigFor(dir), [&recovered](const queue::RecoveredFrame& r) { recovered.push_back(r); });
  if (!opened.has_value()) {
    Require(opened.error().code() == abyss::core::ErrorCode::kCorruption);
    return 0;
  }
  queue::Log& log = **opened;

  for (const auto& r : recovered) {
    auto ref = log.ReadFrame(r.pos);
    Require(ref.has_value());
    const frame::Header& header = ref->view.header;
    Require(header.kind == frame::Kind::kEntry && header.shard == r.header.shard &&
            header.seq == r.header.seq && ref->view.size == r.size);
    auto entry = frame::DecodeEntry(ref->view);
    Require(entry.has_value() || entry.error().code() == abyss::core::ErrorCode::kCorruption);
  }

  queue::Log::Cursor cursor(log);
  std::size_t visited = 0;
  std::optional<queue::LogPosition> pos =
      recovered.empty() ? std::nullopt : std::optional<queue::LogPosition>(0);
  for (std::size_t step = 0; pos.has_value(); ++step) {
    Require(step < kMaxSteps);
    auto view = cursor.Peek(*pos);
    Require(view.has_value() && view->header.kind == frame::Kind::kEntry);
    Require(visited < recovered.size() && recovered[visited].pos == *pos);
    ++visited;
    auto next = cursor.Next(*pos);
    Require(next.has_value());
    pos = *next;
  }
  Require(visited == recovered.size());
  return 0;
}
