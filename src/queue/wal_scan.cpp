#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "abyss/core/thread_annotations.h"
#include "abyss/queue/wal_queue.h"
#include "shard_stream.h"

namespace abyss::queue {

namespace {

constexpr std::size_t kHandOffFrames = 1024;
// A sparse shard's frames are decoded while still in the page cache.
constexpr uint64_t kHandOffLag = uint64_t{64} << 20;
constexpr uint64_t kLagCheckBytes = uint64_t{1} << 20;
constexpr std::size_t kQueueDepth = 8;

core::Error Invalid(const std::string& what) { return {core::ErrorCode::kInvalidArgument, what}; }

struct Batch {
  core::ShardId shard = 0;
  uint32_t log = 0;
  std::vector<LogPosition> positions;
};

// What the walkers and workers of one Scan share.
class ScanRun {
 public:
  ScanRun(std::size_t workers, const std::atomic<bool>& cancel)
      : queues_(workers), cancel_(cancel) {}

  // False once the scan stops; blocks while the worker's queue is full.
  bool Push(std::size_t worker, Batch batch) {
    Queue& queue = queues_[worker];
    std::unique_lock lock(queue.mu);
    queue.cv.wait(lock, [&] ABYSS_REQUIRES(queue.mu) {
      return stopped() || queue.items.size() < kQueueDepth;
    });
    if (stopped()) return false;
    queue.items.push_back(std::move(batch));
    queue.cv.notify_all();
    return true;
  }

  // Nullopt once the scan stops, or its walkers are done and the queue
  // is drained.
  std::optional<Batch> Pop(std::size_t worker) {
    Queue& queue = queues_[worker];
    std::unique_lock lock(queue.mu);
    queue.cv.wait(lock, [&] ABYSS_REQUIRES(queue.mu) {
      return stopped() || queue.closed || !queue.items.empty();
    });
    if (stopped() || queue.items.empty()) return std::nullopt;
    Batch batch = std::move(queue.items.front());
    queue.items.pop_front();
    queue.cv.notify_all();
    return batch;
  }

  void Close() {
    for (Queue& queue : queues_) {
      const std::scoped_lock lock(queue.mu);
      queue.closed = true;
      queue.cv.notify_all();
    }
  }

  void Fail(core::Error error) {
    {
      const std::scoped_lock lock(error_mu_);
      if (!error_.has_value()) error_ = std::move(error);
    }
    stop_.store(true, std::memory_order_release);
    for (Queue& queue : queues_) {
      const std::scoped_lock lock(queue.mu);
      queue.cv.notify_all();
    }
  }

  // Fails the scan if it was cancelled; true while it may go on.
  bool Running() {
    if (stopped()) return false;
    if (!cancel_.load(std::memory_order_acquire)) return true;
    Fail(core::Error{core::ErrorCode::kUnavailable, "scan cancelled"});
    return false;
  }

  bool stopped() const noexcept { return stop_.load(std::memory_order_acquire); }

  std::optional<core::Error> TakeError() {
    const std::scoped_lock lock(error_mu_);
    return std::move(error_);
  }

 private:
  struct Queue {
    std::mutex mu;
    std::condition_variable cv;
    std::deque<Batch> items ABYSS_GUARDED_BY(mu);
    bool closed ABYSS_GUARDED_BY(mu) = false;
  };

  std::vector<Queue> queues_;
  const std::atomic<bool>& cancel_;
  std::atomic<bool> stop_{false};
  std::mutex error_mu_;
  std::optional<core::Error> error_ ABYSS_GUARDED_BY(error_mu_);
};

// Positions of one shard's frames not yet handed to its worker.
struct Pending {
  std::vector<LogPosition> positions;
  core::SequenceId next = 0;
  core::SequenceId end = 0;
};

}  // namespace

core::Result<void> WalQueue::Scan(std::span<const core::SequenceId> from,
                                  std::span<const core::SequenceId> end, uint32_t parallelism,
                                  const ScanSink& sink, const std::atomic<bool>& cancel) {
  const std::size_t shards = config_.shard_count;
  if (from.size() != shards || end.size() != shards) {
    return std::unexpected(
        Invalid("scan bounds must cover all " + std::to_string(shards) + " shards"));
  }
  // Pinned for the scan, so no position it holds is reclaimed.
  class Pin {
   public:
    explicit Pin(WalQueue& queue) : queue_(queue) {
      const std::scoped_lock lock(queue_.reaper_mu_);
      ++queue_.scans_;
    }
    ~Pin() {
      const std::scoped_lock lock(queue_.reaper_mu_);
      --queue_.scans_;
    }
    Pin(const Pin&) = delete;
    Pin& operator=(const Pin&) = delete;
    Pin(Pin&&) = delete;
    Pin& operator=(Pin&&) = delete;

   private:
    WalQueue& queue_;
  };
  const Pin pin(*this);

  const core::Durability visible = AckDurability();
  for (core::ShardId shard = 0; shard < shards; ++shard) {
    if (from[shard] > end[shard]) {
      return std::unexpected(
          Invalid("scan of shard " + std::to_string(shard) + " starts past its end"));
    }
    // Even an empty range: a from of 0 names no entry.
    if (const core::SequenceId first = streams_[shard]->first_seq(); from[shard] < first) {
      return std::unexpected(core::Error{core::ErrorCode::kOutOfRange,
                                         "scan of shard " + std::to_string(shard) + " from seq " +
                                             std::to_string(from[shard]) +
                                             " below first retained seq " + std::to_string(first)});
    }
    if (from[shard] == end[shard]) continue;
    if (const core::SequenceId readable = streams_[shard]->DurableEnd(visible);
        end[shard] > readable) {
      return std::unexpected(Invalid("scan of shard " + std::to_string(shard) + " to seq " +
                                     std::to_string(end[shard]) + " past its readable end " +
                                     std::to_string(readable)));
    }
  }
  if (cancel.load(std::memory_order_acquire)) {
    return std::unexpected(core::Error{core::ErrorCode::kUnavailable, "scan cancelled"});
  }

  const std::size_t workers = std::clamp<std::size_t>(parallelism, 1, shards);
  ScanRun run(workers, cancel);
  std::vector<uint64_t> delivered(shards, 0);

  const auto walk = [&](LogUnit& unit) {
    Log& log = *unit.log;
    Log::Cursor cursor(log);
    std::vector<Pending> pending(shards);
    uint64_t remaining = 0;
    LogPosition pos = std::numeric_limits<LogPosition>::max();
    for (core::ShardId shard = unit.id; shard < shards; shard += config_.log_count) {
      if (from[shard] == end[shard]) continue;
      auto located = streams_[shard]->Locate(cursor, from[shard]);
      if (!located.has_value()) {
        run.Fail(located.error());
        return;
      }
      pos = std::min(pos, *located);
      pending[shard].next = from[shard];
      pending[shard].end = end[shard];
      remaining += end[shard] - from[shard];
    }
    const auto hand_off = [&](core::ShardId shard) {
      std::vector<LogPosition>& positions = pending[shard].positions;
      Batch batch{.shard = shard, .log = unit.id, .positions = std::move(positions)};
      positions.clear();
      return run.Push(shard % workers, std::move(batch));
    };
    uint64_t since_check = 0;
    while (remaining > 0) {
      if (!run.Running()) return;
      if (pos >= log.FilledPrefix()) break;
      auto view = cursor.Peek(pos);
      if (!view.has_value()) {
        run.Fail(view.error());
        return;
      }
      const frame::Header& header = view->header;
      if (header.kind == frame::Kind::kEntry && header.shard < shards) {
        Pending& shard = pending[header.shard];
        if (header.seq >= shard.next && header.seq < shard.end) {
          if (header.seq != shard.next) {
            run.Fail(core::Error{core::ErrorCode::kCorruption,
                                 "scan of shard " + std::to_string(header.shard) +
                                     " expected seq " + std::to_string(shard.next) + " but found " +
                                     std::to_string(header.seq)});
            return;
          }
          ++shard.next;
          --remaining;
          shard.positions.push_back(pos);
          if (shard.positions.size() >= kHandOffFrames && !hand_off(header.shard)) return;
        }
      }
      pos += view->size;
      since_check += view->size;
      if (since_check >= kLagCheckBytes) {
        scan_bytes_.Increment(static_cast<double>(since_check));
        since_check = 0;
        for (core::ShardId shard = unit.id; shard < shards; shard += config_.log_count) {
          const auto& positions = pending[shard].positions;
          if (!positions.empty() && pos - positions.front() > kHandOffLag && !hand_off(shard)) {
            return;
          }
        }
      }
    }
    scan_bytes_.Increment(static_cast<double>(since_check));
    for (core::ShardId shard = unit.id; shard < shards; shard += config_.log_count) {
      if (!pending[shard].positions.empty() && !hand_off(shard)) return;
    }
  };

  const auto decode = [&](std::size_t worker) {
    std::vector<std::optional<Log::Cursor>> cursors(logs_.size());
    std::vector<core::QueueEntry> entries;
    while (auto batch = run.Pop(worker)) {
      if (!run.Running()) return;
      auto& cursor = cursors[batch->log];
      if (!cursor.has_value()) cursor.emplace(*logs_[batch->log]->log);
      entries.clear();
      entries.reserve(batch->positions.size());
      for (const LogPosition pos : batch->positions) {
        auto view = cursor->Read(pos);
        if (!view.has_value()) {
          run.Fail(view.error());
          return;
        }
        auto entry = frame::DecodeEntry(*view);
        if (!entry.has_value()) {
          run.Fail(entry.error());
          return;
        }
        entries.push_back(std::move(*entry));
      }
      const std::size_t count = entries.size();
      if (auto sunk = sink(batch->shard, entries); !sunk.has_value()) {
        run.Fail(sunk.error());
        return;
      }
      delivered[batch->shard] += count;
    }
  };

  {
    std::vector<std::jthread> decoders;
    decoders.reserve(workers);
    for (std::size_t worker = 0; worker < workers; ++worker) {
      decoders.emplace_back(decode, worker);
    }
    {
      std::vector<std::jthread> walkers;
      walkers.reserve(logs_.size());
      for (auto& unit : logs_) walkers.emplace_back(walk, std::ref(*unit));
    }
    run.Close();
  }

  if (auto error = run.TakeError()) return std::unexpected(std::move(*error));
  for (core::ShardId shard = 0; shard < shards; ++shard) {
    if (delivered[shard] != end[shard] - from[shard]) {
      return std::unexpected(core::Error{
          core::ErrorCode::kCorruption, "scan of shard " + std::to_string(shard) + " delivered " +
                                            std::to_string(delivered[shard]) + " of " +
                                            std::to_string(end[shard] - from[shard]) + " entries"});
    }
  }
  return {};
}

}  // namespace abyss::queue
