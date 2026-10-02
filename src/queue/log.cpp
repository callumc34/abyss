#include "abyss/queue/log.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <bit>
#include <charconv>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstring>
#include <deque>
#include <filesystem>
#include <functional>
#include <limits>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
#include <utility>
#include <vector>

#include "abyss/core/thread_annotations.h"
#include "abyss/log/log.h"
#include "abyss/metrics/metrics.h"
#include "abyss/metrics/names.h"
#include "abyss/platform/fs.h"
#include "abyss/platform/mapped_file.h"
#include "abyss/platform/random.h"
#include "binary_io.h"
#include "commit_word.h"
#include "segment_header_v2.h"

#if defined(_M_X64) || defined(_M_IX86)
#include <intrin.h>
#endif

ABYSS_LOG_COMPONENT("abyss.queue.log")

namespace abyss::queue {

namespace {

namespace pfs = abyss::platform::fs;
using frame::Kind;
using frame::State;

constexpr std::size_t kSpares = 2;
constexpr std::size_t kFreePool = 2;
constexpr std::size_t kMinRingSlots = 16;
constexpr std::size_t kCompletionSlots = 1024;
constexpr std::size_t kWaitingReserve = 1024;
constexpr std::size_t kBodyAt = frame::kCommitBytes + frame::kCrcBytes;
constexpr uint64_t kMaxFrameSpace = std::numeric_limits<uint32_t>::max();
constexpr uint32_t kMaxShards = uint32_t{1} << 16;
constexpr auto kSpinFor = std::chrono::microseconds(2);
constexpr auto kIdlePoll = std::chrono::seconds(1);
constexpr auto kDeferredPoll = std::chrono::milliseconds(20);
constexpr auto kMinBackoff = std::chrono::milliseconds(10);
constexpr auto kMaxBackoff = std::chrono::milliseconds(1000);
constexpr std::size_t kOrdinalDigits = 20;
constexpr std::string_view kSegSuffix = ".seg";
constexpr std::string_view kTmpSuffix = ".seg.tmp";
constexpr std::string_view kFreePrefix = "free-";

uint32_t Gen(uint64_t ordinal) noexcept { return static_cast<uint32_t>(ordinal); }

void CpuRelax() noexcept {
#if defined(_M_X64) || defined(_M_IX86)
  _mm_pause();
#elif defined(__x86_64__) || defined(__i386__)
  __builtin_ia32_pause();
#elifdef __aarch64__
  __asm__ __volatile__("yield");
#endif
}

std::string Padded(uint64_t ordinal) {
  const std::string digits = std::to_string(ordinal);
  return std::string(kOrdinalDigits - digits.size(), '0') + digits;
}

std::string SegmentName(uint64_t ordinal) { return Padded(ordinal) + std::string(kSegSuffix); }
std::string TmpName(uint64_t ordinal) { return Padded(ordinal) + std::string(kTmpSuffix); }
std::string FreeName(uint64_t n) {
  return std::string(kFreePrefix) + std::to_string(n) + std::string(kSegSuffix);
}

std::optional<uint64_t> ParseNumber(std::string_view text) {
  uint64_t value = 0;
  const char* first = std::to_address(text.begin());
  const char* last = std::to_address(text.end());
  const auto [ptr, ec] = std::from_chars(first, last, value);
  if (text.empty() || ec != std::errc{} || ptr != last) return std::nullopt;
  return value;
}

std::optional<uint64_t> ParseOrdinalName(std::string_view name, std::string_view suffix) {
  if (name.size() != kOrdinalDigits + suffix.size() || !name.ends_with(suffix)) return std::nullopt;
  return ParseNumber(name.substr(0, kOrdinalDigits));
}

core::Error LogError(core::ErrorCode code, uint32_t log_id, const std::string& what) {
  return {code, "WAL log " + std::to_string(log_id) + ": " + what};
}

std::string Where(uint64_t ordinal, uint64_t offset) {
  return "segment " + std::to_string(ordinal) + " offset " +
         std::to_string(kLogSegmentHeaderBytes + offset);
}

core::Error UnknownKind(uint32_t log_id, uint64_t ordinal, uint64_t offset, Kind kind) {
  return LogError(core::ErrorCode::kCorruption, log_id,
                  Where(ordinal, offset) + ": unknown frame kind " +
                      std::to_string(static_cast<unsigned>(kind)));
}

core::Result<void> SyncDir(const std::filesystem::path& dir) {
  auto out = pfs::FsyncDir(dir);
  if (!out.has_value()) return std::unexpected(out.error());
  if (*out == pfs::DirSyncOutcome::kUnsupported) {
    return std::unexpected(
        core::Error{core::ErrorCode::kFailedPrecondition,
                    "WAL directory durability unsupported on volume '" + dir.string() +
                        "'; a segment's directory entry cannot be made durable"});
  }
  return {};
}

void Discard(const std::filesystem::path& path) {
  (void)pfs::Unlink(path);  // NOLINT(bugprone-unused-return-value)
}

void Adjust(metrics::GaugeHandle& gauge, std::size_t& reported, std::size_t now) {
  if (now > reported) gauge.Increment(static_cast<double>(now - reported));
  if (now < reported) gauge.Decrement(static_cast<double>(reported - now));
  reported = now;
}

struct Completion {
  LogPosition pos = 0;
  LogPosition end = 0;
};

// Committed ranges on their way to the combiner: a bounded lock-free
// MPSC queue (Vyukov's), consumed by whoever holds the combiner flag.
// The padding is deliberate: each counter has its own cache line.
class CompletionRing {  // NOLINT(clang-analyzer-optin.performance.Padding)
 public:
  explicit CompletionRing(std::size_t capacity) : cells_(capacity), mask_(capacity - 1) {
    for (std::size_t i = 0; i < capacity; ++i) cells_[i].seq.store(i, std::memory_order_relaxed);
  }

  // False when full.
  bool TryPush(const Completion& done) {
    uint64_t pos = enqueue_.load(std::memory_order_relaxed);
    for (;;) {
      Cell& cell = cells_[pos & mask_];
      const uint64_t seq = cell.seq.load(std::memory_order_acquire);
      if (seq == pos) {
        if (enqueue_.compare_exchange_weak(pos, pos + 1, std::memory_order_relaxed)) {
          cell.done = done;
          // The publish half of the combiner's Dekker pair.
          cell.seq.store(pos + 1, std::memory_order_seq_cst);
          return true;
        }
      } else if (seq < pos) {
        return false;
      } else {
        pos = enqueue_.load(std::memory_order_relaxed);
      }
    }
  }

  // Combiner only: the next completion, once its pusher published it.
  // A pusher preempted between claiming and publishing holds back the
  // ones behind it; it combines as soon as it publishes.
  std::optional<Completion> TryPop() {
    const uint64_t pos = dequeue_.load(std::memory_order_relaxed);
    Cell& cell = cells_[pos & mask_];
    if (cell.seq.load(std::memory_order_acquire) != pos + 1) return std::nullopt;
    const Completion done = cell.done;
    cell.seq.store(pos + cells_.size(), std::memory_order_release);
    dequeue_.store(pos + 1, std::memory_order_relaxed);
    return done;
  }

  // The re-check half of the Dekker pair.
  bool Ready() const {
    const uint64_t pos = dequeue_.load(std::memory_order_relaxed);
    return cells_[pos & mask_].seq.load(std::memory_order_seq_cst) == pos + 1;
  }

 private:
  struct alignas(64) Cell {
    std::atomic<uint64_t> seq{0};
    Completion done;
  };

  std::vector<Cell> cells_;
  const std::size_t mask_;
  alignas(64) std::atomic<uint64_t> enqueue_{0};
  alignas(64) std::atomic<uint64_t> dequeue_{0};
};

struct ShardSpan {
  bool seen = false;
  core::SequenceId min = 0;
  core::SequenceId max = 0;
};
using ShardSpans = std::vector<ShardSpan>;

void Note(ShardSpans& spans, const frame::Header& header) {
  ShardSpan& span = spans[header.shard];
  if (!span.seen) {
    span = ShardSpan{.seen = true, .min = header.seq, .max = header.seq};
    return;
  }
  span.min = std::min(span.min, header.seq);
  span.max = std::max(span.max, header.seq);
}

std::vector<SegmentShardRange> Ranges(const ShardSpans& spans) {
  std::vector<SegmentShardRange> out;
  for (std::size_t shard = 0; shard < spans.size(); ++shard) {
    if (!spans[shard].seen) continue;
    out.push_back(SegmentShardRange{.shard = static_cast<core::ShardId>(shard),
                                    .min_seq = spans[shard].min,
                                    .max_seq = spans[shard].max});
  }
  return out;
}

struct PoolEntry {
  std::filesystem::path path;
  // The highest ordinal it has held. It only ever takes a higher one,
  // so every stale gen left in it is below its next gen.
  uint64_t last = 0;
};

struct LogSegment {
  uint64_t ordinal = 0;
  std::filesystem::path path;
  pfs::File file;
  pfs::MappedFile map;
  uint64_t salt = 0;
  // Set when sealed, under Log::Impl::mu.
  core::WallTime sealed_at;
  std::vector<SegmentShardRange> shards;

  std::byte* frames() const noexcept { return map.data() + kLogSegmentHeaderBytes; }
};

// A reclaimed segment's file to remove; `segment` is null once closed.
struct Removal {
  std::shared_ptr<LogSegment> segment;
  std::filesystem::path path;
  uint64_t ordinal = 0;
};

// A `.seg` file found at Open.
struct Found {
  std::filesystem::path path;
  pfs::File file;
  pfs::MappedFile map;
  std::optional<LogSegmentHeader> header;
  // The header's, once it is valid.
  uint64_t salt = 0;
  bool right_size = false;
  bool holds_frames = false;

  bool usable() const noexcept { return header.has_value() && map.valid(); }
};

struct WalkStop {
  LogPosition end = 0;
  // The ordinal the walk could not enter: missing or unusable.
  std::optional<uint64_t> gap;
};

// Walks filled frames from the start of `lowest` until `limit`, the
// first frame that is not filled (or is torn) or a segment that cannot
// be entered. `visit` returns false to stop at the frame it was given.
template <typename Visit>
core::Result<WalkStop> Walk(const std::map<uint64_t, Found>& found, uint64_t lowest,
                            uint64_t frame_space, LogPosition limit, uint64_t verify_from,
                            Visit& visit) {
  LogPosition pos = lowest * frame_space;
  for (uint64_t ordinal = lowest;; ++ordinal) {
    const auto it = found.find(ordinal);
    if (it == found.end() || !it->second.usable()) return WalkStop{.end = pos, .gap = ordinal};
    const std::byte* frames = it->second.map.data() + kLogSegmentHeaderBytes;
    for (uint64_t off = 0; off < frame_space;) {
      pos = (ordinal * frame_space) + off;
      if (pos >= limit) return WalkStop{.end = pos};
      const frame::View view =
          frame::Inspect(frame::LoadCommitWord(frames + off), {frames + off, frame_space - off},
                         Gen(ordinal), it->second.salt, ordinal >= verify_from);
      if (view.state != State::kFilled) return WalkStop{.end = pos};
      if (view.header.kind == Kind::kPadding && off + view.size != frame_space) {
        return WalkStop{.end = pos};
      }
      auto keep = visit(view, pos, ordinal);
      if (!keep.has_value()) return std::unexpected(keep.error());
      if (!*keep) return WalkStop{.end = pos};
      off += view.size;
    }
    pos = (ordinal + 1) * frame_space;
  }
}

std::optional<LogSegmentHeader> ReadHeader(const pfs::File& file) {
  std::vector<std::byte> bytes(kLogSegmentHeaderBytes);
  auto read = pfs::Pread(file, bytes.data(), bytes.size(), 0);
  if (!read.has_value() || *read != bytes.size()) return std::nullopt;
  auto header = DecodeLogSegmentHeader(bytes);
  if (!header.has_value()) return std::nullopt;
  return *header;
}

// The `.seg` files Open found, and what their headers say.
struct OnDisk {
  std::map<uint64_t, Found> found;
  uint64_t lowest = 0;
  // max(config, every header's).
  uint64_t window = 0;
  std::optional<uint64_t> last_holding;
};

struct Replayed {
  LogPosition end = 0;
  uint64_t verify_from = 0;
  uint64_t reported = 0;
  std::map<uint64_t, ShardSpans> spans;
  // Newest appended_at_us among each segment's reported entries.
  std::map<uint64_t, int64_t> newest;
};

// Recovery's second pass over each filled frame: the semantic checks,
// holding back the trailing batch until it closes, and the shard
// ranges of every frame reported.
class Replayer {
 public:
  Replayer(uint32_t log_id, uint32_t shard_count, uint64_t frame_space,
           const Log::FrameFn& on_frame)
      : log_id_(log_id),
        shard_count_(shard_count),
        frame_space_(frame_space),
        on_frame_(on_frame),
        last_seq_(shard_count) {}

  core::Result<bool> operator()(const frame::View& view, LogPosition pos, uint64_t ordinal) {
    const uint64_t off = pos - (ordinal * frame_space_);
    const frame::Header& header = view.header;
    if (header.kind == Kind::kPadding) {
      if (!batch_.empty()) return Corrupt(ordinal, off, "padding inside a batch");
      return true;
    }
    if (header.kind != Kind::kEntry) {
      return std::unexpected(UnknownKind(log_id_, ordinal, off, header.kind));
    }
    if (header.shard >= shard_count_) {
      return Corrupt(ordinal, off, "shard " + std::to_string(header.shard) + " is out of range");
    }
    if (header.batch_rest < view.size || header.batch_rest % frame::kAlign != 0 ||
        off + header.batch_rest > frame_space_ ||
        (!batch_.empty() && pos + header.batch_rest != batch_end_)) {
      return Corrupt(
          ordinal, off,
          "batch_rest " + std::to_string(header.batch_rest) + " does not fit its frame or batch");
    }
    auto& last = last_seq_[header.shard];
    if (last.has_value() && header.seq != *last + 1) {
      return Corrupt(ordinal, off,
                     "shard " + std::to_string(header.shard) + " seq " +
                         std::to_string(header.seq) + " follows " + std::to_string(*last));
    }
    last = header.seq;
    const RecoveredFrame recovered{
        .header = header, .pos = pos, .size = static_cast<uint32_t>(view.size), .ordinal = ordinal};
    if (batch_.empty() && header.batch_rest == view.size) {
      Report(recovered);
      return true;
    }
    if (batch_.empty()) batch_end_ = pos + header.batch_rest;
    batch_.push_back(recovered);
    if (pos + view.size == batch_end_) {
      for (const auto& member : batch_) Report(member);
      batch_.clear();
    }
    return true;
  }

  // A batch is one reservation, so only the trailing one can be cut.
  LogPosition End(LogPosition stop) const { return batch_.empty() ? stop : batch_.front().pos; }
  uint64_t reported() const noexcept { return reported_; }
  std::map<uint64_t, ShardSpans>& spans() noexcept { return spans_; }
  std::map<uint64_t, int64_t>& newest() noexcept { return newest_; }

 private:
  std::unexpected<core::Error> Corrupt(uint64_t ordinal, uint64_t off,
                                       const std::string& what) const {
    return std::unexpected(
        LogError(core::ErrorCode::kCorruption, log_id_, Where(ordinal, off) + ": " + what));
  }

  void Report(const RecoveredFrame& recovered) {
    if (on_frame_) on_frame_(recovered);
    auto& spans = spans_[recovered.ordinal];
    if (spans.empty()) spans.resize(shard_count_);
    Note(spans, recovered.header);
    const auto [newest, first] =
        newest_.try_emplace(recovered.ordinal, recovered.header.appended_at_us);
    if (!first) newest->second = std::max(newest->second, recovered.header.appended_at_us);
    ++reported_;
  }

  uint32_t log_id_;
  uint32_t shard_count_;
  uint64_t frame_space_;
  const Log::FrameFn& on_frame_;
  std::vector<std::optional<core::SequenceId>> last_seq_;
  std::vector<RecoveredFrame> batch_;
  LogPosition batch_end_ = 0;
  uint64_t reported_ = 0;
  std::map<uint64_t, ShardSpans> spans_;
  std::map<uint64_t, int64_t> newest_;
};

std::atomic<Log::OpenStep> g_crash_after{Log::OpenStep::kNone};

core::Result<void> CrashPoint(Log::OpenStep step) {
  if (g_crash_after.load(std::memory_order_relaxed) != step) return {};
  return std::unexpected(
      core::Error{core::ErrorCode::kInternal, "simulated crash during Open, for a test"});
}

std::size_t RingSlots(const LogConfig& config) {
  const uint64_t frame_space = config.segment_size_bytes - kLogSegmentHeaderBytes;
  const uint64_t window = config.durability_window_bytes / frame_space;
  return std::bit_ceil(std::max<uint64_t>(kMinRingSlots, window + kSpares + 4));
}

}  // namespace

// The completion ring's cache-line alignment rounds this up; the fields
// stay grouped by use.
struct Log::Impl {  // NOLINT(clang-analyzer-optin.performance.Padding)
  explicit Impl(Log& owner)
      : log(owner),
        frame_space(owner.config_.segment_size_bytes - kLogSegmentHeaderBytes),
        ring(RingSlots(owner.config_)),
        ring_mask(ring.size() - 1),
        spare_gauge(metrics::Registry::Instance().Gauge(metrics::names::kWalSpareSegments)),
        free_gauge(metrics::Registry::Instance().Gauge(metrics::names::kWalFreeSegments)),
        grown_total(metrics::Registry::Instance().Counter(metrics::names::kWalSegmentsGrownTotal)),
        spare_waits(metrics::Registry::Instance().Counter(metrics::names::kWalSpareWaitsTotal)),
        prepare_failures(
            metrics::Registry::Instance().Counter(metrics::names::kWalSegmentPrepareFailuresTotal)),
        fill_wait(metrics::Registry::Instance().Histogram(metrics::names::kWalFillWaitSeconds)) {
    waiting.reserve(kWaitingReserve);
  }

  uint32_t id() const noexcept { return log.config_.log_id; }
  uint64_t Start(uint64_t ordinal) const noexcept { return ordinal * frame_space; }
  LogSegment* RingSegment(uint64_t ordinal) const noexcept {
    return ring[ordinal & ring_mask].load(std::memory_order_acquire);
  }

  void Complete(Completion done);
  void Combine();
  void Fold(const Completion* own);
  void Roll(LogPosition at, uint64_t ordinal);
  void TailMoved();
  bool SpareReady() const noexcept;
  std::shared_ptr<LogSegment> Find(uint64_t ordinal) const;
  core::Result<void> Sync(const LogSegment& segment);
  void SealBelow(LogPosition durable);

  core::Result<void> Recover(const FrameFn& on_frame);
  core::Result<OnDisk> LoadFiles();
  core::Result<Replayed> Replay(const OnDisk& disk, const FrameFn& on_frame) const;
  core::Result<void> Adopt(OnDisk& disk, Replayed& replayed);
  core::Result<std::shared_ptr<LogSegment>> Prepare(uint64_t ordinal,
                                                    std::optional<PoolEntry>& pick, bool& grown);
  void Publish(std::shared_ptr<LogSegment> segment) ABYSS_REQUIRES(mu);
  std::optional<PoolEntry> TakeFree(uint64_t ordinal) ABYSS_REQUIRES(mu);
  core::Result<void> RecycleReady(std::unique_lock<std::mutex>& lock) ABYSS_REQUIRES(mu);
  core::Result<std::optional<std::filesystem::path>> RemoveFile(
      const Removal& removal, const std::optional<std::filesystem::path>& dest);
  std::optional<core::Error> TakeInjected(std::atomic<bool>& armed,
                                          std::optional<core::Error>& slot);
  void UpdateGauges() ABYSS_REQUIRES(mu);
  void RunPreparer();

  Log& log;
  const uint64_t frame_space;
  // Unsealed and spare segments by ordinal % size, for the lock-free
  // append path. Readers go through `segments` instead.
  std::vector<std::atomic<LogSegment*>> ring;
  const std::size_t ring_mask;

  std::atomic<LogPosition> tail{0};
  // The filled prefix advances only through these, never by reading the
  // mapping: a recycled segment's old bytes can look like a frame.
  CompletionRing completions{kCompletionSlots};
  std::atomic<bool> combining{false};
  // Combiner only: completions not yet reached by P, a min-heap by pos.
  std::vector<Completion> waiting;
  // Ordinals below this have been published.
  std::atomic<uint64_t> published_end{0};
  std::atomic<std::size_t> spares{0};
  std::atomic<std::size_t> frees{0};

  mutable std::mutex mu;
  std::condition_variable work_cv;
  std::condition_variable spare_cv;
  std::map<uint64_t, std::shared_ptr<LogSegment>> segments ABYSS_GUARDED_BY(mu);
  // Ordinals below this are sealed.
  uint64_t sealed_end ABYSS_GUARDED_BY(mu) = 0;
  core::WallTime last_sealed_at ABYSS_GUARDED_BY(mu);
  // Reclaimed, oldest first, waiting for readers to let go.
  std::deque<std::shared_ptr<LogSegment>> deferred ABYSS_GUARDED_BY(mu);
  // Reclaimed files that could not be removed yet, oldest first. They
  // hold back every later removal, so the files stay contiguous.
  std::vector<Removal> stuck ABYSS_GUARDED_BY(mu);
  // A pass is removing files with `mu` dropped.
  bool recycling ABYSS_GUARDED_BY(mu) = false;
  std::vector<PoolEntry> pool ABYSS_GUARDED_BY(mu);
  uint64_t next_free_name ABYSS_GUARDED_BY(mu) = 0;
  bool paused ABYSS_GUARDED_BY(mu) = false;
  bool stopping ABYSS_GUARDED_BY(mu) = false;
  std::size_t reported_spares ABYSS_GUARDED_BY(mu) = 0;
  std::size_t reported_free ABYSS_GUARDED_BY(mu) = 0;
  std::mutex stop_mu;
  std::thread preparer;

  // Commit thread only: shard ranges of segments not yet sealed.
  std::map<uint64_t, ShardSpans> open_spans;

  std::atomic<uint64_t> syncs{0};
  std::atomic<bool> has_sync_error{false};
  std::atomic<bool> has_remove_error{false};
  std::atomic<bool> has_flush_hook{false};
  std::mutex seam_mu;
  std::optional<core::Error> sync_error ABYSS_GUARDED_BY(seam_mu);
  std::optional<core::Error> remove_error ABYSS_GUARDED_BY(seam_mu);
  std::function<core::Result<void>()> flush_hook ABYSS_GUARDED_BY(seam_mu);

  metrics::GaugeHandle spare_gauge;
  metrics::GaugeHandle free_gauge;
  metrics::CounterHandle grown_total;
  metrics::CounterHandle spare_waits;
  metrics::CounterHandle prepare_failures;
  metrics::HistogramHandle fill_wait;
};

// A full ring is drained by its committer as the combiner, which takes
// its own completion straight into the heap: the one P waits for always
// gets in, and the heap grows as needed, so nothing is dropped.
void Log::Impl::Complete(Completion done) {
  while (!completions.TryPush(done)) {
    if (!combining.exchange(true, std::memory_order_seq_cst)) {
      Fold(&done);
      combining.store(false, std::memory_order_seq_cst);
      if (completions.Ready()) Combine();
      return;
    }
    std::this_thread::yield();
  }
  Combine();
}

// The flag exchange acquires what the last holder left, and its release
// hands it on. A pusher publishes, then tests or exchanges the flag; a
// holder releases, then re-checks the ring; all are seq_cst. So a
// pusher that finds the flag held has its completion seen by that
// holder's re-check. Testing first keeps waiters off the flag's line.
void Log::Impl::Combine() {
  do {
    if (combining.load(std::memory_order_seq_cst) ||
        combining.exchange(true, std::memory_order_seq_cst)) {
      return;
    }
    Fold(nullptr);
    combining.store(false, std::memory_order_seq_cst);
  } while (completions.Ready());
}

// Combiner only. Completions arrive in any order; P moves through those
// that start where it stands.
void Log::Impl::Fold(const Completion* own) {
  const auto later = [](const Completion& a, const Completion& b) { return a.pos > b.pos; };
  const auto add = [&](const Completion& done) {
    waiting.push_back(done);
    std::ranges::push_heap(waiting, later);
  };
  while (auto done = completions.TryPop()) add(*done);
  if (own != nullptr) add(*own);
  std::atomic<LogPosition>& filled = log.filled_;
  LogPosition pos = filled.load(std::memory_order_relaxed);
  const LogPosition from = pos;
  while (!waiting.empty() && waiting.front().pos == pos) {
    pos = waiting.front().end;
    std::ranges::pop_heap(waiting, later);
    waiting.pop_back();
  }
  if (pos != from) filled.store(pos, std::memory_order_release);
}

void Log::Impl::Roll(LogPosition at, uint64_t ordinal) {
  // The padding is the roller's own reservation, so P stays below it.
  LogSegment* segment = RingSegment(ordinal);
  const uint64_t off = at - Start(ordinal);
  const uint64_t span = frame_space - off;
  std::byte* dst = segment->frames() + off;
  const uint64_t word = frame::EncodePadding(span, Gen(ordinal), segment->salt, {dst, span});
  frame::StoreCommitWord(dst, word);
  Complete(Completion{.pos = at, .end = Start(ordinal + 1)});
}

void Log::Impl::TailMoved() {
  {
    const std::scoped_lock lock(mu);
    UpdateGauges();
  }
  work_cv.notify_one();
}

bool Log::Impl::SpareReady() const noexcept {
  const LogPosition at = tail.load(std::memory_order_acquire);
  return published_end.load(std::memory_order_acquire) > (at + frame_space - 1) / frame_space;
}

std::shared_ptr<LogSegment> Log::Impl::Find(uint64_t ordinal) const {
  const std::scoped_lock lock(mu);
  const auto it = segments.find(ordinal);
  return it == segments.end() ? nullptr : it->second;
}

std::optional<core::Error> Log::Impl::TakeInjected(std::atomic<bool>& armed,
                                                   std::optional<core::Error>& slot) {
  if (!armed.load(std::memory_order_acquire)) return std::nullopt;
  const std::scoped_lock lock(seam_mu);
  armed.store(false, std::memory_order_relaxed);
  std::optional<core::Error> error = std::move(slot);
  slot.reset();
  return error;
}

core::Result<void> Log::Impl::Sync(const LogSegment& segment) {
  if (auto injected = TakeInjected(has_sync_error, sync_error)) {
    return std::unexpected(std::move(*injected));
  }
  syncs.fetch_add(1, std::memory_order_relaxed);
  return pfs::Fsync(segment.file, pfs::SyncMode::kDurableData);
}

void Log::Impl::SealBelow(LogPosition durable) {
  const uint64_t end = durable / frame_space;
  const core::WallTime now = log.config_.wall_clock();
  {
    const std::scoped_lock lock(mu);
    for (uint64_t ordinal = sealed_end; ordinal < end; ++ordinal) {
      ring[ordinal & ring_mask].store(nullptr, std::memory_order_seq_cst);
      auto spans = open_spans.extract(ordinal);
      const auto it = segments.find(ordinal);
      if (it == segments.end()) continue;
      last_sealed_at = std::max(last_sealed_at, now);
      it->second->sealed_at = last_sealed_at;
      if (!spans.empty()) it->second->shards = Ranges(spans.mapped());
    }
    sealed_end = std::max(sealed_end, end);
  }
  work_cv.notify_one();
}

void Log::Impl::Publish(std::shared_ptr<LogSegment> segment) {
  const uint64_t ordinal = segment->ordinal;
  ring[ordinal & ring_mask].store(segment.get(), std::memory_order_release);
  segments.emplace(ordinal, std::move(segment));
  published_end.store(ordinal + 1, std::memory_order_release);
  UpdateGauges();
  spare_cv.notify_all();
}

std::optional<PoolEntry> Log::Impl::TakeFree(uint64_t ordinal) {
  const auto it =
      std::ranges::find_if(pool, [ordinal](const PoolEntry& e) { return e.last < ordinal; });
  if (it == pool.end()) return std::nullopt;
  PoolEntry entry = std::move(*it);
  pool.erase(it);
  UpdateGauges();
  return entry;
}

void Log::Impl::UpdateGauges() {
  std::size_t spare = 0;
  std::size_t free = 0;
  if (!stopping) {
    const uint64_t published = published_end.load(std::memory_order_relaxed);
    const uint64_t active = tail.load(std::memory_order_acquire) / frame_space;
    spare = published > active + 1 ? static_cast<std::size_t>(published - active - 1) : 0;
    free = pool.size();
  }
  spares.store(spare, std::memory_order_relaxed);
  frees.store(free, std::memory_order_relaxed);
  Adjust(spare_gauge, reported_spares, spare);
  Adjust(free_gauge, reported_free, free);
}

core::Result<std::shared_ptr<LogSegment>> Log::Impl::Prepare(uint64_t ordinal,
                                                             std::optional<PoolEntry>& pick,
                                                             bool& grown) {
  const LogConfig& config = log.config_;
  const std::filesystem::path tmp = config.dir / TmpName(ordinal);
  const std::filesystem::path path = config.dir / SegmentName(ordinal);
  grown = !pick.has_value();
  pfs::File file;
  if (pick.has_value()) {
    if (auto renamed = pfs::Rename(pick->path, tmp); !renamed.has_value()) {
      return std::unexpected(renamed.error());
    }
    pick.reset();
    auto opened = pfs::Open(tmp, {.mode = pfs::OpenMode::kReadWrite});
    if (!opened.has_value()) {
      Discard(tmp);
      return std::unexpected(opened.error());
    }
    file = std::move(*opened);
  } else {
    auto opened =
        pfs::Open(tmp, {.mode = pfs::OpenMode::kReadWrite, .create = true, .truncate = true});
    if (!opened.has_value()) return std::unexpected(opened.error());
    file = std::move(*opened);
  }
  const auto fail = [&file](const std::filesystem::path& leftover, core::Error error) {
    file.Close();
    Discard(leftover);
    return std::unexpected(std::move(error));
  };
  if (grown) {
    if (auto zeroed = pfs::ZeroFill(file, config.segment_size_bytes); !zeroed.has_value()) {
      return fail(tmp, zeroed.error());
    }
  }

  std::array<std::byte, sizeof(uint64_t)> random{};
  if (auto drawn = platform::RandomBytes(random); !drawn.has_value()) {
    return fail(tmp, drawn.error());
  }
  const auto salt = binary::LoadLE<uint64_t>(random.data());
  const core::WallTime created_at = config.wall_clock();
  std::vector<std::byte> header(kLogSegmentHeaderBytes);
  EncodeLogSegmentHeader(LogSegmentHeader{.log_id = config.log_id,
                                          .ordinal = ordinal,
                                          .created_at = created_at,
                                          .shard_count = config.shard_count,
                                          .durability_window_bytes = config.durability_window_bytes,
                                          .salt = salt},
                         header);
  if (auto wrote = pfs::Pwrite(file, header.data(), header.size(), 0); !wrote.has_value()) {
    return fail(tmp, wrote.error());
  }
  if (auto synced = pfs::Fsync(file, pfs::SyncMode::kDurableData); !synced.has_value()) {
    return fail(tmp, synced.error());
  }
  if (auto renamed = pfs::Rename(tmp, path); !renamed.has_value()) {
    return fail(tmp, renamed.error());
  }
  if (auto dir = SyncDir(config.dir); !dir.has_value()) return fail(path, dir.error());
  auto map = pfs::MappedFile::Map(file, config.segment_size_bytes);
  if (!map.has_value()) return fail(path, map.error());

  auto segment = std::make_shared<LogSegment>();
  segment->ordinal = ordinal;
  segment->path = path;
  segment->file = std::move(file);
  segment->map = std::move(*map);
  segment->salt = salt;
  return segment;
}

// Moves the file to `dest` for the pool if given, else unlinks it, and
// returns where it went. A file already gone counts as removed.
core::Result<std::optional<std::filesystem::path>> Log::Impl::RemoveFile(
    const Removal& removal, const std::optional<std::filesystem::path>& dest) {
  if (auto injected = TakeInjected(has_remove_error, remove_error)) {
    return std::unexpected(std::move(*injected));
  }
  if (dest.has_value()) {
    auto renamed = pfs::Rename(removal.path, *dest);
    if (renamed.has_value()) return dest;
    ABYSS_LOG_WARN("could not return a WAL segment to the free pool; unlinking it",
                   {"log", static_cast<int64_t>(id())}, {"path", removal.path.string()},
                   {"err", std::string_view{renamed.error().message()}});
  }
  auto unlinked = pfs::Unlink(removal.path);
  if (!unlinked.has_value() && unlinked.error().code() != core::ErrorCode::kNotFound) {
    return std::unexpected(unlinked.error());
  }
  return std::nullopt;
}

// Removes reclaimed files strictly oldest first, so the files left on
// disk are always one contiguous run of ordinals.
core::Result<void> Log::Impl::RecycleReady(std::unique_lock<std::mutex>& lock) {
  if (recycling) return {};
  const bool retrying = !stuck.empty();
  std::vector<Removal> work;
  if (retrying) {
    work = stuck;
  } else {
    while (!deferred.empty() && deferred.front().use_count() == 1) {
      std::shared_ptr<LogSegment> segment = std::move(deferred.front());
      deferred.pop_front();
      std::filesystem::path path = segment->path;
      const uint64_t ordinal = segment->ordinal;
      work.push_back(
          Removal{.segment = std::move(segment), .path = std::move(path), .ordinal = ordinal});
    }
    // Pairs with the release in each last reader's reference drop.
    if (!work.empty()) std::atomic_thread_fence(std::memory_order_acquire);
  }
  if (work.empty()) return {};

  std::vector<std::optional<std::filesystem::path>> dests;
  for (std::size_t i = 0; i < work.size(); ++i) {
    if (pool.size() + i < kFreePool) {
      dests.emplace_back(log.config_.dir / FreeName(next_free_name++));
    } else {
      dests.emplace_back(std::nullopt);
    }
  }
  recycling = true;
  lock.unlock();

  core::Result<void> result;
  std::vector<PoolEntry> recycled;
  std::size_t done = 0;
  for (; done < work.size(); ++done) {
    // Unmaps and closes it.
    work[done].segment.reset();
    auto removed = RemoveFile(work[done], dests[done]);
    if (!removed.has_value()) {
      result = std::unexpected(LogError(removed.error().code(), id(),
                                        "could not remove reclaimed " + work[done].path.string() +
                                            ": " + removed.error().message()));
      break;
    }
    if (const auto& pooled = *removed; pooled.has_value()) {
      recycled.push_back(PoolEntry{.path = *pooled, .last = work[done].ordinal});
    }
  }

  lock.lock();
  recycling = false;
  if (retrying) {
    stuck.erase(stuck.begin(), stuck.begin() + static_cast<std::ptrdiff_t>(done));
  } else if (done < work.size()) {
    stuck.push_back(Removal{.path = work[done].path, .ordinal = work[done].ordinal});
    for (std::size_t i = work.size(); i > done + 1; --i) {
      deferred.push_front(std::move(work[i - 1].segment));
    }
  }
  for (auto& entry : recycled) pool.push_back(std::move(entry));
  UpdateGauges();
  return result;
}

void Log::Impl::RunPreparer() {
  std::unique_lock lock(mu);
  std::chrono::milliseconds backoff{0};
  while (!stopping) {
    if (!paused) {
      if (auto recycled = RecycleReady(lock); !recycled.has_value()) {
        ABYSS_LOG_WARN("WAL segment reclaim failed; retrying", {"log", static_cast<int64_t>(id())},
                       {"err", std::string_view{recycled.error().message()}});
      }
    }
    if (stopping) break;
    const uint64_t next = published_end.load(std::memory_order_relaxed);
    const uint64_t target = (tail.load(std::memory_order_acquire) / frame_space) + 1 + kSpares;
    if (paused || next >= target || ring[next & ring_mask].load(std::memory_order_relaxed)) {
      const bool pending = !deferred.empty() || !stuck.empty();
      work_cv.wait_for(lock, pending ? std::chrono::milliseconds(kDeferredPoll)
                                     : std::chrono::milliseconds(kIdlePoll));
      continue;
    }

    std::optional<PoolEntry> pick = TakeFree(next);
    lock.unlock();
    bool grown = false;
    auto prepared = Prepare(next, pick, grown);
    lock.lock();
    if (pick.has_value()) pool.push_back(std::move(*pick));
    if (prepared.has_value()) {
      Publish(std::move(*prepared));
      if (grown) grown_total.Increment();
      backoff = std::chrono::milliseconds(0);
      continue;
    }
    prepare_failures.Increment();
    backoff = std::clamp(backoff * 2, std::chrono::milliseconds(kMinBackoff),
                         std::chrono::milliseconds(kMaxBackoff));
    ABYSS_LOG_ERROR("WAL spare segment preparation failed; retrying",
                    {"log", static_cast<int64_t>(id())}, {"ordinal", next},
                    {"retry_ms", static_cast<int64_t>(backoff.count())},
                    {"err", std::string_view{prepared.error().message()}});
    UpdateGauges();
    work_cv.wait_for(lock, backoff, [this] ABYSS_REQUIRES(mu) { return stopping; });
  }
}

core::Result<OnDisk> Log::Impl::LoadFiles() {
  const LogConfig& config = log.config_;
  OnDisk disk{.window = config.durability_window_bytes};
  std::vector<std::pair<uint64_t, std::filesystem::path>> frees_found;
  std::vector<std::filesystem::path> leftovers;
  {
    std::error_code ec;
    std::filesystem::directory_iterator it(config.dir, ec);
    if (ec) {
      return std::unexpected(LogError(core::ErrorCode::kInternal, id(),
                                      "list " + config.dir.string() + ": " + ec.message()));
    }
    for (const auto& entry : it) {
      if (!entry.is_regular_file(ec)) continue;
      const std::string name = entry.path().filename().string();
      if (auto ordinal = ParseOrdinalName(name, kSegSuffix)) {
        disk.found[*ordinal].path = entry.path();
      } else if (ParseOrdinalName(name, kTmpSuffix).has_value()) {
        leftovers.push_back(entry.path());
      } else if (name.starts_with(kFreePrefix) && name.ends_with(kSegSuffix)) {
        const std::string_view digits = std::string_view(name).substr(
            kFreePrefix.size(), name.size() - kFreePrefix.size() - kSegSuffix.size());
        if (auto n = ParseNumber(digits)) frees_found.emplace_back(*n, entry.path());
      }
    }
  }
  for (const auto& path : leftovers) {
    ABYSS_LOG_WARN("discarding an interrupted WAL segment preparation", {"path", path.string()});
    if (auto unlinked = pfs::Unlink(path); !unlinked.has_value()) {
      return std::unexpected(unlinked.error());
    }
  }

  std::ranges::sort(frees_found);
  {
    const std::scoped_lock lock(mu);
    for (const auto& [n, path] : frees_found) {
      next_free_name = std::max(next_free_name, n + 1);
      auto file = pfs::Open(path, {.mode = pfs::OpenMode::kRead});
      if (!file.has_value()) return std::unexpected(file.error());
      auto size = pfs::FileSize(*file);
      if (!size.has_value()) return std::unexpected(size.error());
      if (*size != config.segment_size_bytes) {
        file->Close();
        if (auto unlinked = pfs::Unlink(path); !unlinked.has_value()) {
          return std::unexpected(unlinked.error());
        }
        continue;
      }
      // Without its last ordinal it cannot be safely renumbered.
      const auto header = ReadHeader(*file);
      if (!header.has_value()) {
        file->Close();
        if (auto unlinked = pfs::Unlink(path); !unlinked.has_value()) {
          return std::unexpected(unlinked.error());
        }
        continue;
      }
      pool.push_back(PoolEntry{.path = path, .last = header->ordinal});
    }
  }

  for (auto& [ordinal, seg] : disk.found) {
    auto file = pfs::Open(seg.path, {.mode = pfs::OpenMode::kReadWrite});
    if (!file.has_value()) return std::unexpected(file.error());
    seg.file = std::move(*file);
    auto size = pfs::FileSize(seg.file);
    if (!size.has_value()) return std::unexpected(size.error());
    seg.right_size = *size == config.segment_size_bytes;
    auto header = ReadHeader(seg.file);
    if (header.has_value() && header->ordinal == ordinal) {
      if (header->log_id != config.log_id) {
        return std::unexpected(
            LogError(core::ErrorCode::kFailedPrecondition, id(),
                     seg.path.string() + " belongs to log " + std::to_string(header->log_id)));
      }
      if (header->shard_count != config.shard_count) {
        return std::unexpected(
            LogError(core::ErrorCode::kFailedPrecondition, id(),
                     "the log was created for " + std::to_string(header->shard_count) +
                         " shards, but " + std::to_string(config.shard_count) + " are configured"));
      }
      seg.header = header;
      seg.salt = header->salt;
      disk.window = std::max(disk.window, header->durability_window_bytes);
    }
    if (seg.header.has_value() && seg.right_size) {
      auto map = pfs::MappedFile::Map(seg.file, config.segment_size_bytes);
      if (!map.has_value()) return std::unexpected(map.error());
      seg.map = std::move(*map);
    }
    if (seg.header.has_value() && *size >= kLogSegmentHeaderBytes + frame::kCommitBytes) {
      std::array<std::byte, frame::kCommitBytes> first{};
      auto read = pfs::Pread(seg.file, first.data(), first.size(), kLogSegmentHeaderBytes);
      if (!read.has_value()) return std::unexpected(read.error());
      const auto word = binary::LoadLE<uint64_t>(first.data());
      seg.holds_frames = *read == first.size() && frame::CommitGen(word) == Gen(ordinal) &&
                         frame::CommitLen(word) != 0;
    }
    if (seg.holds_frames) disk.last_holding = ordinal;
    if (seg.holds_frames && !seg.right_size) {
      return std::unexpected(LogError(
          core::ErrorCode::kFailedPrecondition, id(),
          seg.path.string() + " holds frames but is " + std::to_string(*size) +
              " bytes, not queue.segment_size_bytes " + std::to_string(config.segment_size_bytes) +
              "; the segment size cannot change while the log holds data"));
    }
  }
  disk.lowest = disk.found.empty() ? 0 : disk.found.begin()->first;
  return disk;
}

// Pass 1 finds the end by structure alone, to place the CRC scope;
// pass 2 verifies inside it and reports.
core::Result<Replayed> Log::Impl::Replay(const OnDisk& disk, const FrameFn& on_frame) const {
  const auto any = [](const frame::View&, LogPosition, uint64_t) -> core::Result<bool> {
    return true;
  };
  auto first = Walk(disk.found, disk.lowest, frame_space, std::numeric_limits<LogPosition>::max(),
                    std::numeric_limits<uint64_t>::max(), any);
  if (!first.has_value()) return std::unexpected(first.error());
  const LogPosition structural_end = first->end;
  if (first->gap.has_value()) {
    for (auto it = disk.found.upper_bound(*first->gap); it != disk.found.end(); ++it) {
      if (it->second.holds_frames) {
        return std::unexpected(LogError(core::ErrorCode::kCorruption, id(),
                                        "segment " + std::to_string(*first->gap) +
                                            " is missing or unreadable, but segment " +
                                            std::to_string(it->first) + " after it holds frames"));
      }
    }
  }
  // Frames past the end are unacknowledged, and bounded by the window;
  // far beyond it, the end is damage in the middle of the log.
  if (disk.last_holding.has_value() &&
      Start(*disk.last_holding) > structural_end + disk.window + frame_space) {
    return std::unexpected(LogError(core::ErrorCode::kCorruption, id(),
                                    "the log ends at position " + std::to_string(structural_end) +
                                        ", but segment " + std::to_string(*disk.last_holding) +
                                        " far past it holds frames"));
  }
  const uint64_t scope_ordinal =
      (structural_end > disk.window ? structural_end - disk.window : 0) / frame_space;
  Replayed replayed{.verify_from =
                        std::max(disk.lowest, scope_ordinal > 0 ? scope_ordinal - 1 : 0)};

  Replayer replayer(id(), log.config_.shard_count, frame_space, on_frame);
  auto second =
      Walk(disk.found, disk.lowest, frame_space, structural_end, replayed.verify_from, replayer);
  if (!second.has_value()) return std::unexpected(second.error());
  replayed.end = replayer.End(second->end);
  replayed.reported = replayer.reported();
  replayed.spans = std::move(replayer.spans());
  replayed.newest = std::move(replayer.newest());
  return replayed;
}

core::Result<void> Log::Impl::Adopt(OnDisk& disk, Replayed& replayed) {
  const LogConfig& config = log.config_;
  const LogPosition end = replayed.end;
  const uint64_t active = (end + frame_space - 1) / frame_space;

  // A segment at or past `active` may have been active before the
  // crash, so frames of its own gen may sit past a hole in it. It goes
  // to the pool, to take only a higher ordinal; highest first, so a
  // crash midway leaves the ordinals contiguous. This comes before the
  // tail is padded: until the padding lands the hole stops a later
  // walk, and after it the next segment is gone.
  const std::scoped_lock lock(mu);
  bool moved_any = false;
  for (auto it = disk.found.rbegin(); it != disk.found.rend() && it->first >= active; ++it) {
    Found& seg = it->second;
    seg.map.Unmap();
    seg.file.Close();
    moved_any = true;
    if (!seg.right_size || !seg.header.has_value()) {
      if (auto unlinked = pfs::Unlink(seg.path); !unlinked.has_value()) {
        return std::unexpected(unlinked.error());
      }
      continue;
    }
    const std::filesystem::path dest = config.dir / FreeName(next_free_name++);
    if (auto moved = pfs::Rename(seg.path, dest); !moved.has_value()) {
      return std::unexpected(moved.error());
    }
    pool.push_back(PoolEntry{.path = dest, .last = seg.header->ordinal});
  }
  if (moved_any) {
    if (auto dir = SyncDir(config.dir); !dir.has_value()) return std::unexpected(dir.error());
  }
  if (auto crashed = CrashPoint(Log::OpenStep::kPastEndRenamed); !crashed.has_value()) {
    return crashed;
  }

  // Seal the recovered tail, then sync whatever may be unflushed.
  if (end % frame_space != 0) {
    const uint64_t ordinal = end / frame_space;
    const uint64_t off = end - Start(ordinal);
    if (frame_space - off < frame::kMinFrameBytes) {
      return std::unexpected(LogError(core::ErrorCode::kCorruption, id(),
                                      Where(ordinal, off) + ": the log ends too close to its "
                                                            "segment end to pad"));
    }
    Found& tail_seg = disk.found[ordinal];
    std::byte* dst = tail_seg.map.data() + kLogSegmentHeaderBytes + off;
    const uint64_t word = frame::EncodePadding(frame_space - off, Gen(ordinal), tail_seg.salt,
                                               {dst, frame_space - off});
    frame::StoreCommitWord(dst, word);
  }
  for (uint64_t ordinal = replayed.verify_from; ordinal < active; ++ordinal) {
    Found& seg = disk.found[ordinal];
    if (auto back = seg.map.WriteBack(kLogSegmentHeaderBytes, frame_space); !back.has_value()) {
      return std::unexpected(back.error());
    }
    if (auto synced = pfs::Fsync(seg.file, pfs::SyncMode::kDurableData); !synced.has_value()) {
      return std::unexpected(synced.error());
    }
  }
  if (auto crashed = CrashPoint(Log::OpenStep::kTailSynced); !crashed.has_value()) return crashed;

  for (uint64_t ordinal = disk.lowest; ordinal < active; ++ordinal) {
    Found& seg = disk.found[ordinal];
    if (!seg.header.has_value()) {
      return std::unexpected(
          LogError(core::ErrorCode::kInternal, id(),
                   "segment " + std::to_string(ordinal) + " was replayed without a header"));
    }
    auto segment = std::make_shared<LogSegment>();
    segment->ordinal = ordinal;
    segment->path = seg.path;
    segment->file = std::move(seg.file);
    segment->map = std::move(seg.map);
    segment->salt = seg.salt;
    // Its newest entry was appended about when it was last written to.
    core::WallTime sealed_at = seg.header->created_at;
    if (auto it = replayed.newest.find(ordinal); it != replayed.newest.end()) {
      sealed_at = core::WallTime(std::chrono::microseconds(it->second));
    }
    last_sealed_at = std::max(last_sealed_at, sealed_at);
    segment->sealed_at = last_sealed_at;
    if (auto it = replayed.spans.find(ordinal); it != replayed.spans.end()) {
      segment->shards = Ranges(it->second);
    }
    segments.emplace(ordinal, std::move(segment));
  }
  sealed_end = active;
  const LogPosition start = Start(active);
  log.filled_.store(start, std::memory_order_relaxed);
  log.durable_.store(start, std::memory_order_relaxed);
  tail.store(start, std::memory_order_relaxed);
  published_end.store(active, std::memory_order_relaxed);

  for (uint64_t ordinal = active; ordinal <= active + kSpares; ++ordinal) {
    std::optional<PoolEntry> pick = TakeFree(ordinal);
    bool grown = false;
    auto prepared = Prepare(ordinal, pick, grown);
    if (pick.has_value()) pool.push_back(std::move(*pick));
    if (!prepared.has_value()) return std::unexpected(prepared.error());
    Publish(std::move(*prepared));
    if (grown) grown_total.Increment();
  }
  while (pool.size() > kFreePool) {
    if (auto unlinked = pfs::Unlink(pool.back().path); !unlinked.has_value()) {
      return std::unexpected(unlinked.error());
    }
    pool.pop_back();
  }
  UpdateGauges();
  return {};
}

core::Result<void> Log::Impl::Recover(const FrameFn& on_frame) {
  auto disk = LoadFiles();
  if (!disk.has_value()) return std::unexpected(disk.error());
  auto replayed = Replay(*disk, on_frame);
  if (!replayed.has_value()) return std::unexpected(replayed.error());
  if (auto adopted = Adopt(*disk, *replayed); !adopted.has_value()) return adopted;
  const uint64_t active = (replayed->end + frame_space - 1) / frame_space;
  if (replayed->reported > 0 || active > disk->lowest) {
    ABYSS_LOG_INFO("WAL log recovered", {"log", static_cast<int64_t>(id())},
                   {"segments", active - disk->lowest}, {"frames", replayed->reported},
                   {"end", replayed->end}, {"verified_from_segment", replayed->verify_from});
  }
  return {};
}

Log::Log(LogConfig config) : config_(std::move(config)), impl_(std::make_unique<Impl>(*this)) {}

// Only a std::system_error from a lock or the join can escape, and
// terminating on that at teardown is the right outcome.
// NOLINTNEXTLINE(bugprone-exception-escape)
Log::~Log() { Shutdown(); }

core::Result<std::unique_ptr<Log>> Log::Open(LogConfig config, const FrameFn& on_frame) {
  const std::size_t size = config.segment_size_bytes;
  if (size % frame::kAlign != 0 || size < kLogSegmentHeaderBytes + (2 * frame::kMinFrameBytes) ||
      size - kLogSegmentHeaderBytes > kMaxFrameSpace) {
    return std::unexpected(LogError(core::ErrorCode::kInvalidArgument, config.log_id,
                                    "segment_size_bytes " + std::to_string(size) +
                                        " must be a multiple of 8 above 4 KiB and below 4 GiB"));
  }
  if (config.shard_count == 0 || config.shard_count > kMaxShards) {
    return std::unexpected(LogError(core::ErrorCode::kInvalidArgument, config.log_id,
                                    "shard_count must be in [1, 65536]"));
  }
  std::error_code ec;
  std::filesystem::create_directories(config.dir, ec);
  if (ec) {
    return std::unexpected(LogError(core::ErrorCode::kInternal, config.log_id,
                                    "create " + config.dir.string() + ": " + ec.message()));
  }
  std::unique_ptr<Log> log(new Log(std::move(config)));
  if (auto recovered = log->impl_->Recover(on_frame); !recovered.has_value()) {
    return std::unexpected(recovered.error());
  }
  Impl& impl = *log->impl_;
  impl.preparer = std::thread([&impl] { impl.RunPreparer(); });
  return log;
}

core::Result<Log::Reservation> Log::Reserve(uint32_t size) {
  Impl& impl = *impl_;
  const uint64_t space = impl.frame_space;
  if (size < frame::kMinFrameBytes || size % frame::kAlign != 0) {
    return std::unexpected(
        LogError(core::ErrorCode::kInvalidArgument, impl.id(),
                 "reservation of " + std::to_string(size) + " bytes is not a frame size"));
  }
  if (size > space || (size < space && space - size < frame::kMinFrameBytes)) {
    return std::unexpected(
        LogError(core::ErrorCode::kResourceExhausted, impl.id(),
                 "a " + std::to_string(size) + "-byte frame exceeds the segment frame space of " +
                     std::to_string(space) + " bytes; raise queue.segment_size_bytes"));
  }
  LogPosition at = impl.tail.load(std::memory_order_seq_cst);
  for (;;) {
    const uint64_t ordinal = at / space;
    const uint64_t room = space - (at - impl.Start(ordinal));
    // A frame may not leave a remainder too small to pad.
    const bool fits = size == room || size + frame::kMinFrameBytes <= room;
    const uint64_t target = fits ? ordinal : ordinal + 1;
    if (impl.published_end.load(std::memory_order_acquire) <= target) {
      return std::unexpected(
          LogError(core::ErrorCode::kUnavailable, impl.id(),
                   "segment " + std::to_string(target) + " is not prepared yet"));
    }
    const LogPosition pos = fits ? at : impl.Start(target);
    if (!impl.tail.compare_exchange_weak(at, pos + size, std::memory_order_seq_cst,
                                         std::memory_order_seq_cst)) {
      continue;
    }
    if (!fits) impl.Roll(at, ordinal);
    if ((pos + size) / space != ordinal) impl.TailMoved();
    // Unfilled, so P and sealing stay below it: the slot holds `target`.
    LogSegment* segment = impl.RingSegment(target);
    return Reservation{.pos = pos,
                       .size = size,
                       .gen = Gen(target),
                       .salt = segment->salt,
                       .dst = segment->frames() + (pos - impl.Start(target))};
  }
}

bool Log::WaitForSpare(core::SteadyTime deadline) {
  Impl& impl = *impl_;
  impl.spare_waits.Increment();
  std::unique_lock lock(impl.mu);
  const bool ready = impl.spare_cv.wait_until(lock, deadline, [&impl] ABYSS_REQUIRES(impl.mu) {
    return impl.stopping || impl.SpareReady();
  });
  return ready && !impl.stopping;
}

void Log::Commit(const Reservation& reservation, std::span<const std::byte> frame) {
  const uint64_t word =
      frame::CommitWord(frame::CommitLen(binary::LoadLE<uint64_t>(frame.data())), reservation.gen);
  const auto body_crc = binary::LoadLE<uint32_t>(frame.data() + frame::kCommitBytes);
  binary::StoreLE(reservation.dst + frame::kCommitBytes,
                  frame::SealCrc(body_crc, reservation.salt, word));
  std::memcpy(reservation.dst + kBodyAt, frame.data() + kBodyAt, frame.size() - kBodyAt);
  frame::StoreCommitWord(reservation.dst, word);
  impl_->Complete(Completion{.pos = reservation.pos, .end = reservation.pos + frame.size()});
}

void Log::AwaitFilled(LogPosition end) const {
  if (filled_.load(std::memory_order_acquire) >= end) return;
  const auto start = std::chrono::steady_clock::now();
  bool spinning = true;
  while (filled_.load(std::memory_order_acquire) < end) {
    if (!impl_->combining.load(std::memory_order_relaxed) && impl_->completions.Ready()) {
      impl_->Combine();
    }
    if (spinning && std::chrono::steady_clock::now() - start >= kSpinFor) spinning = false;
    if (spinning) {
      CpuRelax();
    } else {
      std::this_thread::yield();
    }
  }
  // A wait inside the spin is a neighbour mid-copy; only longer ones,
  // behind a large value or a preempted filler, are worth recording.
  if (!spinning) {
    impl_->fill_wait.Observe(
        std::chrono::duration<double>(std::chrono::steady_clock::now() - start).count());
  }
}

LogPosition Log::ReservedTail() const noexcept {
  return impl_->tail.load(std::memory_order_seq_cst);
}

core::Result<Log::Flushed> Log::Flush(const DurableFn& on_frame) {
  Impl& impl = *impl_;
  const uint64_t space = impl.frame_space;
  const LogPosition to = filled_.load(std::memory_order_acquire);
  const LogPosition from = durable_.load(std::memory_order_relaxed);
  Flushed flushed{.from = from, .to = to};
  if (to <= from) return flushed;
  if (impl.has_flush_hook.load(std::memory_order_acquire)) {
    std::function<core::Result<void>()> hook;
    {
      const std::scoped_lock lock(impl.seam_mu);
      hook = impl.flush_hook;
    }
    if (hook) {
      if (auto hooked = hook(); !hooked.has_value()) return std::unexpected(hooked.error());
    }
  }

  const auto segment_at = [&impl](uint64_t ordinal) -> core::Result<LogSegment*> {
    LogSegment* segment = impl.RingSegment(ordinal);
    if (segment == nullptr || segment->ordinal != ordinal) {
      return std::unexpected(LogError(
          core::ErrorCode::kInternal, impl.id(),
          "segment " + std::to_string(ordinal) + " below the filled prefix is not mapped"));
    }
    return segment;
  };
  for (uint64_t ordinal = from / space; impl.Start(ordinal) < to; ++ordinal) {
    auto segment = segment_at(ordinal);
    if (!segment.has_value()) return std::unexpected(segment.error());
    const uint64_t lo = std::max(from, impl.Start(ordinal)) - impl.Start(ordinal);
    const uint64_t hi = std::min(to, impl.Start(ordinal + 1)) - impl.Start(ordinal);
    // A no-op on POSIX by design: the fdatasync or F_FULLFSYNC below
    // also writes MAP_SHARED dirty pages; an msync would double it.
    if (auto back = (*segment)->map.WriteBack(kLogSegmentHeaderBytes + lo, hi - lo);
        !back.has_value()) {
      return std::unexpected(back.error());
    }
    if (auto synced = impl.Sync(**segment); !synced.has_value()) {
      return std::unexpected(synced.error());
    }
  }

  for (LogPosition pos = from; pos < to;) {
    const uint64_t ordinal = pos / space;
    const uint64_t off = pos - impl.Start(ordinal);
    auto segment = segment_at(ordinal);
    if (!segment.has_value()) return std::unexpected(segment.error());
    const std::byte* at = (*segment)->frames() + off;
    const frame::View view = frame::Inspect(frame::LoadCommitWord(at), {at, space - off},
                                            Gen(ordinal), (*segment)->salt, false);
    if (view.state != State::kFilled) {
      return std::unexpected(LogError(core::ErrorCode::kInternal, impl.id(),
                                      Where(ordinal, off) + ": unfilled below the filled prefix"));
    }
    if (view.header.kind == Kind::kEntry) {
      if (view.header.shard >= config_.shard_count) {
        return std::unexpected(LogError(core::ErrorCode::kCorruption, impl.id(),
                                        Where(ordinal, off) + ": shard " +
                                            std::to_string(view.header.shard) +
                                            " is out of range"));
      }
      if (on_frame) on_frame(view.header, static_cast<uint32_t>(view.size));
      ++flushed.entries;
      flushed.entry_bytes += view.size;
      auto& spans = impl.open_spans[ordinal];
      if (spans.empty()) spans.resize(config_.shard_count);
      Note(spans, view.header);
    } else if (view.header.kind != Kind::kPadding) {
      return std::unexpected(UnknownKind(impl.id(), ordinal, off, view.header.kind));
    }
    pos += view.size;
  }
  durable_.store(to, std::memory_order_release);
  impl.SealBelow(to);
  return flushed;
}

core::Result<frame::View> Log::Cursor::At(LogPosition pos, bool verify_crc) {
  const Impl& impl = *log_->impl_;
  const uint64_t space = impl.frame_space;
  const uint64_t ordinal = pos / space;
  if (hold_ == nullptr || ordinal_ != ordinal) {
    std::shared_ptr<LogSegment> segment = impl.Find(ordinal);
    if (segment == nullptr) {
      return std::unexpected(
          LogError(core::ErrorCode::kOutOfRange, impl.id(),
                   "segment " + std::to_string(ordinal) + " has been reclaimed"));
    }
    frames_ = segment->frames();
    ordinal_ = ordinal;
    salt_ = segment->salt;
    hold_ = std::move(segment);
  }
  const uint64_t off = pos - impl.Start(ordinal);
  const std::byte* at = frames_ + off;
  frame::View view =
      frame::Inspect(frame::LoadCommitWord(at), {at, space - off}, Gen(ordinal), salt_, verify_crc);
  if (view.state != State::kFilled) {
    return std::unexpected(
        LogError(core::ErrorCode::kCorruption, impl.id(),
                 Where(ordinal, off) + ": torn frame below the filled prefix (media corruption)"));
  }
  if (view.header.kind != Kind::kEntry && view.header.kind != Kind::kPadding) {
    return std::unexpected(UnknownKind(impl.id(), ordinal, off, view.header.kind));
  }
  return view;
}

core::Result<frame::View> Log::Cursor::Read(LogPosition pos) {
  if (pos >= log_->FilledPrefix()) {
    return std::unexpected(LogError(core::ErrorCode::kOutOfRange, log_->log_id(),
                                    "position " + std::to_string(pos) + " is not filled yet"));
  }
  return At(pos, true);
}

core::Result<frame::View> Log::Cursor::Peek(LogPosition pos) {
  if (pos >= log_->FilledPrefix()) {
    return std::unexpected(LogError(core::ErrorCode::kOutOfRange, log_->log_id(),
                                    "position " + std::to_string(pos) + " is not filled yet"));
  }
  return At(pos, false);
}

core::Result<std::optional<LogPosition>> Log::Cursor::Next(LogPosition pos) {
  const LogPosition filled = log_->FilledPrefix();
  auto current = Peek(pos);
  if (!current.has_value()) return std::unexpected(current.error());
  for (LogPosition next = pos + current->size; next < filled;) {
    auto view = At(next, false);
    if (!view.has_value()) return std::unexpected(view.error());
    if (view->header.kind != Kind::kPadding) return std::optional<LogPosition>(next);
    next += view->size;
  }
  return std::optional<LogPosition>();
}

core::Result<FrameRef> Log::ReadFrame(LogPosition pos) const {
  Cursor cursor(*this);
  auto view = cursor.Read(pos);
  if (!view.has_value()) return std::unexpected(view.error());
  return FrameRef{.view = *view, .hold = std::move(cursor.hold_)};
}

std::vector<SegmentInfo> Log::SealedSegments(std::size_t max_count) const {
  const Impl& impl = *impl_;
  const std::scoped_lock lock(impl.mu);
  std::vector<SegmentInfo> out;
  for (const auto& [ordinal, segment] : impl.segments) {
    if (ordinal >= impl.sealed_end || out.size() >= max_count) break;
    out.push_back(SegmentInfo{
        .ordinal = ordinal, .sealed_at = segment->sealed_at, .shards = segment->shards});
  }
  return out;
}

core::Result<void> Log::Reclaim(uint64_t ordinal) {
  Impl& impl = *impl_;
  std::unique_lock lock(impl.mu);
  if (!impl.stuck.empty()) {
    return std::unexpected(LogError(
        core::ErrorCode::kUnavailable, impl.id(),
        impl.stuck.front().path.string() + " from an earlier reclaim could not be removed yet"));
  }
  const auto it = impl.segments.find(ordinal);
  if (it == impl.segments.end()) {
    return std::unexpected(LogError(core::ErrorCode::kNotFound, impl.id(),
                                    "segment " + std::to_string(ordinal) + " is not in the log"));
  }
  if (ordinal >= impl.sealed_end) {
    return std::unexpected(LogError(core::ErrorCode::kFailedPrecondition, impl.id(),
                                    "segment " + std::to_string(ordinal) + " is not sealed"));
  }
  if (it != impl.segments.begin()) {
    return std::unexpected(LogError(
        core::ErrorCode::kFailedPrecondition, impl.id(),
        "segments are reclaimed oldest first; " + std::to_string(ordinal) + " is not the oldest"));
  }
  impl.deferred.push_back(std::move(it->second));
  impl.segments.erase(it);
  auto recycled = impl.RecycleReady(lock);
  impl.work_cv.notify_one();
  return recycled;
}

std::size_t Log::spare_count() const noexcept {
  return impl_->spares.load(std::memory_order_relaxed);
}

std::size_t Log::free_count() const noexcept {
  return impl_->frees.load(std::memory_order_relaxed);
}

void Log::Shutdown() {
  Impl& impl = *impl_;
  const std::scoped_lock stop(impl.stop_mu);
  {
    const std::scoped_lock lock(impl.mu);
    impl.stopping = true;
  }
  impl.work_cv.notify_all();
  impl.spare_cv.notify_all();
  if (impl.preparer.joinable()) impl.preparer.join();
  impl.Combine();
  std::unique_lock lock(impl.mu);
  if (auto recycled = impl.RecycleReady(lock); !recycled.has_value()) {
    ABYSS_LOG_WARN("WAL segment reclaim at shutdown failed",
                   {"log", static_cast<int64_t>(impl.id())},
                   {"err", std::string_view{recycled.error().message()}});
  }
  impl.UpdateGauges();
}

void Log::PausePreparerForTesting() {
  const std::scoped_lock lock(impl_->mu);
  impl_->paused = true;
}

void Log::ResumePreparerForTesting() {
  {
    const std::scoped_lock lock(impl_->mu);
    impl_->paused = false;
  }
  impl_->work_cv.notify_all();
}

uint64_t Log::SyncCountForTesting() const noexcept {
  return impl_->syncs.load(std::memory_order_relaxed);
}

void Log::PauseCombinerForTesting() {
  while (impl_->combining.exchange(true, std::memory_order_seq_cst)) std::this_thread::yield();
}

void Log::ResumeCombinerForTesting() { impl_->combining.store(false, std::memory_order_seq_cst); }

void Log::CrashOpenAfterForTesting(OpenStep step) {
  g_crash_after.store(step, std::memory_order_relaxed);
}

void Log::InjectRemoveErrorForTesting(core::Error error) {
  const std::scoped_lock lock(impl_->seam_mu);
  impl_->remove_error = std::move(error);
  impl_->has_remove_error.store(true, std::memory_order_release);
}

void Log::SetFlushHookForTesting(std::function<core::Result<void>()> hook) {
  const std::scoped_lock lock(impl_->seam_mu);
  impl_->flush_hook = std::move(hook);
  impl_->has_flush_hook.store(true, std::memory_order_release);
}

void Log::InjectSyncErrorForTesting(core::Error error) {
  const std::scoped_lock lock(impl_->seam_mu);
  impl_->sync_error = std::move(error);
  impl_->has_sync_error.store(true, std::memory_order_release);
}

}  // namespace abyss::queue
