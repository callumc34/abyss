#pragma once

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <mutex>
#include <optional>
#include <span>
#include <string_view>
#include <vector>

#include "abyss/core/result.h"
#include "abyss/core/thread_annotations.h"
#include "abyss/core/types.h"
#include "abyss/platform/fs.h"
#include "abyss/queue/offset_store.h"

namespace abyss::queue {

struct OffsetCheckpointConfig {
  // Directory that holds the checkpoint file ({wal_path}/offsets).
  std::filesystem::path dir;
  uint32_t shard_count = 1;
  std::vector<core::ConsumerId> consumers;
};

// Every retention consumer's committed offset on every shard, persisted as
// one dual-slot file (the LMDB meta-page pattern). Write fills the slot
// not holding the newest epoch, in place, then fsyncs; Open takes the
// valid slot with the highest epoch, so a torn write never loses the
// previous checkpoint.
//
// Slot layout (little-endian), version 1; slot i is at i * SlotBytes():
//   0   8  magic "ABYSOCKP"
//   8   4  format version
//   12  4  slot bytes
//   16  8  epoch (>= 1)
//   24  4  shard count S
//   28  4  consumer count C
//   32  4C consumer ids
//   then C*S 16-byte entries, consumer-major: u64 seq, u8 flags
//   (bit 0 = committed), 7 reserved zero bytes
//   then u32 CRC32C over every preceding slot byte; zero padding to a
//   multiple of kBlockSize.
class OffsetCheckpoint final : public OffsetStore {
 public:
  static constexpr std::string_view kFileName = "offsets.ckpt";
  static constexpr uint32_t kFormatVersion = 1;
  static constexpr size_t kBlockSize = 4096;
  static constexpr std::array<char, 8> kMagic{'A', 'B', 'Y', 'S', 'O', 'C', 'K', 'P'};

  // In-memory form of a committed offset: seq + 1, or 0 for never committed.
  static constexpr uint64_t Encode(std::optional<core::SequenceId> seq) {
    return seq.has_value() ? *seq + 1 : 0;
  }
  static constexpr std::optional<core::SequenceId> Decode(uint64_t encoded) {
    if (encoded == 0) return std::nullopt;
    return encoded - 1;
  }

  static size_t SlotBytes(uint32_t shard_count, size_t consumer_count);

  // Opens the checkpoint in `config.dir`, creating it when absent. Refuses
  // with kFailedPrecondition a legacy per-file offsets layout or a shard
  // count or consumer set that differs from `config`; with kCorruption, a
  // file with no valid slot.
  static core::Result<std::unique_ptr<OffsetCheckpoint>> Open(OffsetCheckpointConfig config);

  ~OffsetCheckpoint() override = default;
  OffsetCheckpoint(const OffsetCheckpoint&) = delete;
  OffsetCheckpoint& operator=(const OffsetCheckpoint&) = delete;
  OffsetCheckpoint(OffsetCheckpoint&&) = delete;
  OffsetCheckpoint& operator=(OffsetCheckpoint&&) = delete;

  std::optional<core::SequenceId> Get(core::ConsumerId consumer,
                                      core::ShardId shard) const override;

  // Persists `encoded` (one Encode()d value per consumer x shard, indexed
  // consumer-major in config order); on success it becomes what Get returns.
  core::Result<void> Write(std::span<const uint64_t> encoded);

  std::optional<size_t> ConsumerIndex(core::ConsumerId consumer) const;
  const std::vector<core::ConsumerId>& consumers() const { return consumers_; }
  uint32_t shard_count() const { return shard_count_; }
  size_t slot_bytes() const { return slot_bytes_; }
  uint64_t epoch() const;

 private:
  OffsetCheckpoint(std::vector<core::ConsumerId> consumers, uint32_t shard_count,
                   platform::fs::File file, size_t active_slot, uint64_t epoch,
                   std::span<const uint64_t> persisted);

  const std::vector<core::ConsumerId> consumers_;
  const uint32_t shard_count_;
  const size_t slot_bytes_;
  // Lock-free for the reaper; advanced only after a write is durable.
  std::vector<std::atomic<uint64_t>> persisted_;

  mutable std::mutex write_mu_;
  platform::fs::File file_ ABYSS_GUARDED_BY(write_mu_);
  size_t active_slot_ ABYSS_GUARDED_BY(write_mu_);
  uint64_t epoch_ ABYSS_GUARDED_BY(write_mu_);
  std::vector<std::byte> scratch_ ABYSS_GUARDED_BY(write_mu_);
};

}  // namespace abyss::queue
