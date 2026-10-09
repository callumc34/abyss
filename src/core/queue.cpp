#include "abyss/core/queue.h"

#include <algorithm>
#include <cstddef>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include "abyss/core/types.h"

namespace abyss::core {

namespace {

constexpr std::size_t kScanBatch = 1024;

}  // namespace

Result<void> Queue::Scan(std::span<const SequenceId> from, std::span<const SequenceId> end,
                         uint32_t parallelism, const ScanSink& sink,
                         const std::atomic<bool>& cancel) {
  if (from.size() != end.size()) {
    return std::unexpected(
        Error{ErrorCode::kInvalidArgument, "scan bounds cover different shard counts"});
  }
  std::atomic<std::size_t> next_shard{0};
  std::atomic<bool> stop{false};
  std::mutex error_mu;
  std::optional<Error> first_error;
  const auto fail = [&](Error error) {
    const std::scoped_lock lock(error_mu);
    if (!first_error.has_value()) first_error = std::move(error);
    stop.store(true, std::memory_order_release);
  };

  const auto scan_shard = [&](ShardId shard) {
    SequenceId seq = from[shard];
    if (seq < kFirstSeq) {
      fail(Error{ErrorCode::kOutOfRange,
                 "scan of shard " + std::to_string(shard) + " from seq 0, which names no entry"});
      return;
    }
    while (seq < end[shard]) {
      if (stop.load(std::memory_order_acquire)) return;
      if (cancel.load(std::memory_order_acquire)) {
        fail(Error{ErrorCode::kUnavailable, "scan cancelled"});
        return;
      }
      const auto want =
          static_cast<std::size_t>(std::min<SequenceId>(end[shard] - seq, kScanBatch));
      auto read = Read(shard, seq, want, Duration::zero(), AckDurability());
      if (!read.has_value()) {
        fail(read.error());
        return;
      }
      if (read->empty() || read->front().seq != seq || read->back().seq != seq + read->size() - 1) {
        fail(Error{ErrorCode::kInternal, "scan of shard " + std::to_string(shard) +
                                             " found no entry at seq " + std::to_string(seq)});
        return;
      }
      std::erase_if(*read, [&](const QueueEntry& entry) { return entry.seq >= end[shard]; });
      const SequenceId last = read->back().seq;
      if (auto sunk = sink(shard, *read); !sunk.has_value()) {
        fail(sunk.error());
        return;
      }
      seq = last + 1;
    }
  };
  const auto work = [&] {
    for (std::size_t shard = next_shard.fetch_add(1); shard < from.size();
         shard = next_shard.fetch_add(1)) {
      scan_shard(static_cast<ShardId>(shard));
    }
  };

  const std::size_t workers =
      std::clamp<std::size_t>(parallelism, 1, std::max<std::size_t>(from.size(), 1));
  std::vector<std::jthread> threads;
  threads.reserve(workers - 1);
  for (std::size_t i = 1; i < workers; ++i) threads.emplace_back(work);
  work();
  threads.clear();

  if (first_error.has_value()) return std::unexpected(std::move(*first_error));
  return {};
}

}  // namespace abyss::core
