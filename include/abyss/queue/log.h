#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <memory>
#include <optional>
#include <span>
#include <vector>

#include "abyss/core/result.h"
#include "abyss/core/types.h"
#include "abyss/queue/frame.h"

namespace abyss::queue {

// Byte position in a log: ordinal * frame space + offset. Monotonic.
using LogPosition = uint64_t;

struct LogConfig {
  std::filesystem::path dir;
  uint32_t log_id = 0;
  // Recorded in every header; Open refuses a log made for another count.
  uint32_t shard_count = 1;
  std::size_t segment_size_bytes = std::size_t{128} << 20;
  // Recorded in every header. Recovery CRC-verifies the last
  // max(this, recorded) bytes before the recovered end, plus a segment.
  uint64_t durability_window_bytes = uint64_t{64} << 20;
};

// One frame recovered at Open, in log order.
struct RecoveredFrame {
  frame::Header header;
  LogPosition pos = 0;
  uint32_t size = 0;
  uint64_t ordinal = 0;
};

// Per-shard seq range a segment holds, for retention.
struct SegmentShardRange {
  core::ShardId shard = 0;
  core::SequenceId min_seq = 0;
  core::SequenceId max_seq = 0;
};

struct SegmentInfo {
  uint64_t ordinal = 0;
  core::WallTime created_at;
  std::vector<SegmentShardRange> shards;
};

// A filled frame read back by position. `bytes` stays valid while the
// view lives, even if retention recycles the segment meanwhile.
struct FrameRef {
  frame::View view;
  std::shared_ptr<const void> hold;
};

// One physical log carrying many shards' streams (ADP-015 §Log
// durability pipeline). Appenders reserve, fill and commit frames
// concurrently; the filled prefix is the process-crash watermark and
// the flushed prefix the power-loss one. Segments are recycled with a
// per-frame generation, and zero-filled only to grow the pool.
class Log {
 public:
  using FrameFn = std::function<void(const RecoveredFrame&)>;

  // Recovers or creates the log, calling `on_frame` for every recovered
  // entry frame in log order, then pre-grows the spares. The recovered
  // tail is padded to its segment end and synced, so appends continue
  // in a fresh segment and both prefixes start at the recovered end.
  static core::Result<std::unique_ptr<Log>> Open(LogConfig config, const FrameFn& on_frame);
  ~Log();

  Log(const Log&) = delete;
  Log& operator=(const Log&) = delete;
  Log(Log&&) = delete;
  Log& operator=(Log&&) = delete;

  struct Reservation {
    LogPosition pos = 0;
    uint32_t size = 0;
    uint32_t gen = 0;
    // The segment's, for SealCrc.
    uint64_t salt = 0;
    std::byte* dst = nullptr;
  };

  // Reserves `size` bytes (a FrameSize). kUnavailable, with no state
  // changed, when the frame needs a segment that is not ready yet: wait
  // with WaitForSpare outside any lock and retry.
  core::Result<Reservation> Reserve(uint32_t size);
  // False at the deadline, or once the log is shutting down.
  bool WaitForSpare(core::SteadyTime deadline);

  // Copies `frame` (one EncodeEntry output, sized to the reservation)
  // into the log: everything after the commit word, the CRC sealed for
  // the reservation's gen, then the commit word with release ordering;
  // then advances the filled prefix past every frame already filled.
  // A batch reserves once and commits each frame through its own slice
  // {pos + off, size, gen, dst + off} of the reservation.
  void Commit(const Reservation& reservation, std::span<const std::byte> frame);
  // Spins, then yields, until the filled prefix reaches `end`.
  void AwaitFilled(LogPosition end) const;

  // End of every reservation so far, seq_cst; at least FilledPrefix().
  LogPosition ReservedTail() const noexcept;
  LogPosition FilledPrefix() const noexcept { return filled_.load(std::memory_order_acquire); }
  LogPosition DurablePrefix() const noexcept { return durable_.load(std::memory_order_acquire); }

  // Commit thread only. Syncs every segment holding bytes below the
  // filled prefix that are not yet durable, then calls `on_frame` for
  // each entry frame that became durable, in log order, and only then
  // publishes the new durable prefix. Returns [old, new) durable, and
  // the entry frames in it (padding excluded).
  struct Flushed {
    LogPosition from = 0;
    LogPosition to = 0;
    uint64_t entries = 0;
    uint64_t entry_bytes = 0;
  };
  using DurableFn = std::function<void(const frame::Header&, uint32_t size)>;
  core::Result<Flushed> Flush(const DurableFn& on_frame);

  // Reads frames by position for one thread. It holds the segment it is
  // on, so only moving to another segment takes the log's lock; a view
  // stays valid until then. Errors: kOutOfRange for a position not yet
  // filled or a reclaimed segment, kCorruption for a torn frame or an
  // unknown kind. It must not outlive the log.
  class Cursor {
   public:
    explicit Cursor(const Log& log) noexcept : log_(&log) {}

    // The filled frame at `pos`, CRC-verified.
    core::Result<frame::View> Read(LogPosition pos);
    // The frame at `pos` by its header alone, unverified, for skipping.
    core::Result<frame::View> Peek(LogPosition pos);
    // The frame after the one at `pos`, stepping over padding and into
    // the next segment; nullopt only at the filled prefix.
    core::Result<std::optional<LogPosition>> Next(LogPosition pos);

   private:
    friend class Log;
    core::Result<frame::View> At(LogPosition pos, bool verify_crc);

    const Log* log_;
    std::shared_ptr<const void> hold_;
    const std::byte* frames_ = nullptr;
    uint64_t ordinal_ = 0;
    uint64_t salt_ = 0;
  };

  // A one-shot Cursor::Read that keeps the segment alive in the result.
  core::Result<FrameRef> ReadFrame(LogPosition pos) const;

  // Up to `max_count` sealed segments, oldest first, for retention.
  std::vector<SegmentInfo> SealedSegments(std::size_t max_count) const;
  // Returns the oldest sealed segment to the free pool once no reader
  // holds it, or unlinks it when the pool is full. kUnavailable while
  // an earlier reclaim's file could not be removed yet.
  core::Result<void> Reclaim(uint64_t ordinal);

  uint64_t log_id() const noexcept { return config_.log_id; }
  std::size_t spare_count() const noexcept;
  std::size_t free_count() const noexcept;

  void Shutdown();

  // While paused the preparer prepares no spares and retries nothing.
  void PausePreparerForTesting();
  void ResumePreparerForTesting();
  // Flush's data syncs so far.
  uint64_t SyncCountForTesting() const noexcept;
  // The next flush sync fails with `error`.
  void InjectSyncErrorForTesting(core::Error error);
  // While paused no completion reaches the filled prefix: commits fill
  // the completion ring, then wait, and drain it themselves on resume.
  void PauseCombinerForTesting();
  void ResumeCombinerForTesting();
  // The next removal of a reclaimed segment's file fails with `error`.
  void InjectRemoveErrorForTesting(core::Error error);
  // Open fails right after `step`, leaving the disk as a crash would.
  enum class OpenStep : uint8_t { kNone, kPastEndRenamed, kTailSynced };
  static void CrashOpenAfterForTesting(OpenStep step);

 private:
  explicit Log(LogConfig config);

  LogConfig config_;
  std::atomic<LogPosition> filled_{0};
  std::atomic<LogPosition> durable_{0};
  // The rest is the implementation's to choose.
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace abyss::queue
