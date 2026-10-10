#include "abyss/queue/offset_checkpoint.h"

#include <algorithm>
#include <cstring>
#include <filesystem>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>
#include <vector>

#include "abyss/log/log.h"
#include "abyss/platform/fs.h"
#include "binary_io.h"
#include "crc32c.h"

ABYSS_LOG_COMPONENT("abyss.queue.offsets")

namespace abyss::queue {

namespace {

namespace pfs = abyss::platform::fs;

constexpr size_t kVersionOffset = 8;
constexpr size_t kSlotBytesOffset = 12;
constexpr size_t kEpochOffset = 16;
constexpr size_t kShardCountOffset = 24;
constexpr size_t kConsumerCountOffset = 28;
constexpr size_t kHeaderBytes = 32;
constexpr size_t kConsumerIdBytes = sizeof(uint32_t);
constexpr size_t kEntryBytes = 16;
constexpr size_t kEntryFlagsOffset = sizeof(uint64_t);
constexpr size_t kCrcBytes = sizeof(uint32_t);
constexpr uint8_t kCommittedFlag = 0x01;
constexpr uint64_t kFirstEpoch = 1;

void PutU32(std::byte* p, uint32_t v) {
  if constexpr (!binary::kNativeLittleEndian) v = std::byteswap(v);
  std::memcpy(p, &v, sizeof(v));
}

void PutU64(std::byte* p, uint64_t v) {
  if constexpr (!binary::kNativeLittleEndian) v = std::byteswap(v);
  std::memcpy(p, &v, sizeof(v));
}

uint32_t GetU32(const std::byte* p) {
  uint32_t v = 0;
  std::memcpy(&v, p, sizeof(v));
  if constexpr (!binary::kNativeLittleEndian) v = std::byteswap(v);
  return v;
}

uint64_t GetU64(const std::byte* p) {
  uint64_t v = 0;
  std::memcpy(&v, p, sizeof(v));
  if constexpr (!binary::kNativeLittleEndian) v = std::byteswap(v);
  return v;
}

size_t PayloadBytes(size_t shard_count, size_t consumer_count) {
  return kHeaderBytes + (kConsumerIdBytes * consumer_count) +
         (kEntryBytes * consumer_count * shard_count);
}

struct ParsedSlot {
  uint64_t epoch = 0;
  uint32_t shard_count = 0;
  std::vector<core::ConsumerId> consumers;
  // Encoded values, consumer-major in this slot's consumer order.
  std::vector<uint64_t> encoded;
};

enum class SlotState : uint8_t { kValid, kInvalid, kUnsupportedVersion };

SlotState ParseSlot(std::span<const std::byte> slot, ParsedSlot& out) {
  if (slot.size() < kHeaderBytes + kCrcBytes) return SlotState::kInvalid;
  if (std::memcmp(slot.data(), OffsetCheckpoint::kMagic.data(), OffsetCheckpoint::kMagic.size()) !=
      0) {
    return SlotState::kInvalid;
  }
  if (GetU32(slot.data() + kVersionOffset) != OffsetCheckpoint::kFormatVersion) {
    return SlotState::kUnsupportedVersion;
  }
  if (GetU32(slot.data() + kSlotBytesOffset) != slot.size()) return SlotState::kInvalid;

  const uint64_t epoch = GetU64(slot.data() + kEpochOffset);
  const uint32_t shards = GetU32(slot.data() + kShardCountOffset);
  const uint32_t consumers = GetU32(slot.data() + kConsumerCountOffset);
  // Bound each count by the slot before multiplying, so a corrupt header
  // cannot overflow the payload arithmetic.
  if (epoch == 0 || shards > slot.size() || consumers > slot.size()) return SlotState::kInvalid;
  const size_t payload = PayloadBytes(shards, consumers);
  if (payload + kCrcBytes > slot.size()) return SlotState::kInvalid;
  if (GetU32(slot.data() + payload) != Crc32c(slot.first(payload))) return SlotState::kInvalid;

  out.epoch = epoch;
  out.shard_count = shards;
  out.consumers.resize(consumers);
  const std::byte* ids = slot.data() + kHeaderBytes;
  for (size_t c = 0; c < consumers; ++c) out.consumers[c] = GetU32(ids + (c * kConsumerIdBytes));
  const std::byte* entries = ids + (consumers * kConsumerIdBytes);
  out.encoded.resize(static_cast<size_t>(consumers) * shards);
  for (size_t i = 0; i < out.encoded.size(); ++i) {
    const std::byte* entry = entries + (i * kEntryBytes);
    const auto flags = static_cast<uint8_t>(entry[kEntryFlagsOffset]);
    out.encoded[i] = (flags & kCommittedFlag) != 0 ? GetU64(entry) + 1 : 0;
  }
  return SlotState::kValid;
}

// Fills `slot` (already sized) with a complete, CRC-sealed slot image.
void EncodeSlot(std::span<std::byte> slot, uint64_t epoch, uint32_t shard_count,
                std::span<const core::ConsumerId> consumers, std::span<const uint64_t> encoded) {
  std::ranges::fill(slot, std::byte{0});
  std::memcpy(slot.data(), OffsetCheckpoint::kMagic.data(), OffsetCheckpoint::kMagic.size());
  PutU32(slot.data() + kVersionOffset, OffsetCheckpoint::kFormatVersion);
  PutU32(slot.data() + kSlotBytesOffset, static_cast<uint32_t>(slot.size()));
  PutU64(slot.data() + kEpochOffset, epoch);
  PutU32(slot.data() + kShardCountOffset, shard_count);
  PutU32(slot.data() + kConsumerCountOffset, static_cast<uint32_t>(consumers.size()));
  std::byte* ids = slot.data() + kHeaderBytes;
  for (size_t c = 0; c < consumers.size(); ++c) PutU32(ids + (c * kConsumerIdBytes), consumers[c]);
  std::byte* entries = ids + (consumers.size() * kConsumerIdBytes);
  for (size_t i = 0; i < encoded.size(); ++i) {
    if (encoded[i] == 0) continue;
    std::byte* entry = entries + (i * kEntryBytes);
    PutU64(entry, encoded[i] - 1);
    entry[kEntryFlagsOffset] = std::byte{kCommittedFlag};
  }
  const size_t payload = PayloadBytes(shard_count, consumers.size());
  PutU32(slot.data() + payload, Crc32c(slot.first(payload)));
}

core::Result<void> SyncDir(const std::filesystem::path& dir, bool required) {
  auto out = pfs::FsyncDir(dir);
  if (!out.has_value()) return std::unexpected(out.error());
  if (*out == pfs::DirSyncOutcome::kUnsupported) {
    if (required) {
      return std::unexpected(
          core::Error{core::ErrorCode::kFailedPrecondition,
                      "offset checkpoint directory '" + dir.string() + "' cannot be made durable"});
    }
    ABYSS_LOG_WARN("offset checkpoint directory entry is not durable", {"dir", dir.string()});
  }
  return {};
}

// The per-(consumer, shard) layout kept one subdirectory per consumer.
core::Result<void> RejectLegacyLayout(const std::filesystem::path& dir) {
  std::error_code ec;
  std::filesystem::directory_iterator it(dir, ec);
  if (ec) {
    return std::unexpected(
        core::Error{core::ErrorCode::kInternal, "list offsets dir: " + ec.message()});
  }
  for (const auto& entry : it) {
    if (!entry.is_directory(ec)) continue;
    ABYSS_LOG_CRITICAL("legacy per-file offsets layout found; refusing to start",
                       {"path", entry.path().string()});
    return std::unexpected(core::Error{
        core::ErrorCode::kFailedPrecondition,
        "'" + dir.string() +
            "' holds the legacy offsets/{consumer}/{shard}.offset layout, which this version "
            "does not read; recreate the WAL directory"});
  }
  return {};
}

// Created under a temporary name and renamed into place, so the checkpoint
// file, once visible, always holds at least one valid slot.
core::Result<void> CreateFile(const std::filesystem::path& path, size_t slot_bytes,
                              uint32_t shard_count, std::span<const core::ConsumerId> consumers,
                              bool require_durable_dir) {
  auto tmp = path;
  tmp += ".tmp";
  std::error_code ec;
  std::filesystem::remove(tmp, ec);

  std::vector<std::byte> image(2 * slot_bytes);
  const std::vector<uint64_t> none(consumers.size() * shard_count, 0);
  EncodeSlot(std::span(image).first(slot_bytes), kFirstEpoch, shard_count, consumers, none);

  auto file = pfs::Open(
      tmp,
      pfs::OpenOptions{
          .mode = pfs::OpenMode::kWrite, .create = true, .exclusive = true, .truncate = false});
  if (!file.has_value()) return std::unexpected(file.error());
  auto written = pfs::Pwrite(*file, image.data(), image.size(), 0);
  if (written.has_value()) written = pfs::Fsync(*file, pfs::SyncMode::kDurable);
  file->Close();
  if (written.has_value()) written = pfs::Rename(tmp, path);
  if (!written.has_value()) {
    std::filesystem::remove(tmp, ec);
    return std::unexpected(written.error());
  }
  return SyncDir(path.parent_path(), require_durable_dir);
}

}  // namespace

size_t OffsetCheckpoint::SlotBytes(uint32_t shard_count, size_t consumer_count) {
  const size_t used = PayloadBytes(shard_count, consumer_count) + kCrcBytes;
  return ((used + kBlockSize - 1) / kBlockSize) * kBlockSize;
}

core::Result<std::unique_ptr<OffsetCheckpoint>> OffsetCheckpoint::Open(
    OffsetCheckpointConfig config) {
  if (config.shard_count == 0) {
    return std::unexpected(
        core::Error{core::ErrorCode::kInvalidArgument, "offset checkpoint needs shard_count >= 1"});
  }
  auto sorted_consumers = config.consumers;
  std::ranges::sort(sorted_consumers);
  if (std::ranges::adjacent_find(sorted_consumers) != sorted_consumers.end()) {
    return std::unexpected(
        core::Error{core::ErrorCode::kInvalidArgument, "duplicate retention consumer id"});
  }

  std::error_code ec;
  const bool created_dir = std::filesystem::create_directories(config.dir, ec);
  if (ec) {
    return std::unexpected(
        core::Error{core::ErrorCode::kInternal, "create offsets dir: " + ec.message()});
  }
  if (created_dir) {
    if (auto r = SyncDir(config.dir.parent_path(), config.require_durable_dir); !r.has_value()) {
      return std::unexpected(r.error());
    }
  }
  if (auto r = RejectLegacyLayout(config.dir); !r.has_value()) return std::unexpected(r.error());

  const auto path = config.dir / kFileName;
  const size_t expected_slot_bytes = SlotBytes(config.shard_count, config.consumers.size());
  const bool exists = std::filesystem::exists(path, ec);
  if (ec) {
    return std::unexpected(
        core::Error{core::ErrorCode::kInternal, "stat offset checkpoint: " + ec.message()});
  }
  if (!exists) {
    if (auto r = CreateFile(path, expected_slot_bytes, config.shard_count, config.consumers,
                            config.require_durable_dir);
        !r.has_value()) {
      return std::unexpected(r.error());
    }
  } else {
    auto stale_tmp = path;
    stale_tmp += ".tmp";
    std::filesystem::remove(stale_tmp, ec);
  }

  auto file = pfs::Open(path, pfs::OpenOptions{.mode = pfs::OpenMode::kReadWrite});
  if (!file.has_value()) return std::unexpected(file.error());
  auto size = pfs::FileSize(*file);
  if (!size.has_value()) return std::unexpected(size.error());
  if (*size == 0 || *size % (2 * kBlockSize) != 0) {
    return std::unexpected(core::Error{
        core::ErrorCode::kCorruption,
        "offset checkpoint '" + path.string() + "' has invalid size " + std::to_string(*size)});
  }
  const auto slot_bytes = static_cast<size_t>(*size / 2);
  std::vector<std::byte> image(2 * slot_bytes);
  auto nread = pfs::Pread(*file, image.data(), image.size(), 0);
  if (!nread.has_value()) return std::unexpected(nread.error());
  if (*nread != image.size()) {
    return std::unexpected(
        core::Error{core::ErrorCode::kCorruption, "short read of offset checkpoint"});
  }

  ParsedSlot slot0;
  ParsedSlot slot1;
  const SlotState state0 = ParseSlot(std::span(image).first(slot_bytes), slot0);
  const SlotState state1 = ParseSlot(std::span(image).subspan(slot_bytes), slot1);
  const bool valid0 = state0 == SlotState::kValid;
  const bool valid1 = state1 == SlotState::kValid;
  if (!valid0 && !valid1) {
    if (state0 == SlotState::kUnsupportedVersion || state1 == SlotState::kUnsupportedVersion) {
      return std::unexpected(core::Error{
          core::ErrorCode::kFailedPrecondition,
          "offset checkpoint '" + path.string() + "' uses an unsupported format version"});
    }
    ABYSS_LOG_CRITICAL("offset checkpoint has no valid slot", {"path", path.string()});
    return std::unexpected(
        core::Error{core::ErrorCode::kCorruption,
                    "offset checkpoint '" + path.string() + "' has no valid slot"});
  }
  if (valid0 && valid1 && slot0.epoch == slot1.epoch) {
    return std::unexpected(
        core::Error{core::ErrorCode::kCorruption,
                    "offset checkpoint '" + path.string() + "' has two slots with the same epoch"});
  }
  const size_t active = !valid1 || (valid0 && slot0.epoch > slot1.epoch) ? 0 : 1;
  const ParsedSlot& chosen = active == 0 ? slot0 : slot1;

  if (chosen.shard_count != config.shard_count) {
    return std::unexpected(
        core::Error{core::ErrorCode::kFailedPrecondition,
                    "offset checkpoint '" + path.string() + "' was written for shard_count " +
                        std::to_string(chosen.shard_count) + " but the queue has " +
                        std::to_string(config.shard_count)});
  }
  auto chosen_sorted = chosen.consumers;
  std::ranges::sort(chosen_sorted);
  if (chosen_sorted != sorted_consumers) {
    return std::unexpected(core::Error{core::ErrorCode::kFailedPrecondition,
                                       "offset checkpoint '" + path.string() +
                                           "' was written for a different retention consumer set"});
  }
  if (slot_bytes != expected_slot_bytes) {
    return std::unexpected(
        core::Error{core::ErrorCode::kCorruption,
                    "offset checkpoint '" + path.string() + "' has an unexpected slot size"});
  }

  // Re-index into config consumer order.
  std::vector<uint64_t> persisted(chosen.encoded.size());
  for (size_t c = 0; c < config.consumers.size(); ++c) {
    const auto slot_index = static_cast<size_t>(
        std::ranges::find(chosen.consumers, config.consumers[c]) - chosen.consumers.begin());
    for (size_t s = 0; s < config.shard_count; ++s) {
      persisted[(c * config.shard_count) + s] =
          chosen.encoded[(slot_index * config.shard_count) + s];
    }
  }

  ABYSS_LOG_INFO("offset checkpoint opened", {"path", path.string()},
                 {"epoch", static_cast<uint64_t>(chosen.epoch)},
                 {"slot_bytes", static_cast<uint64_t>(slot_bytes)}, {"created", !exists});
  return std::unique_ptr<OffsetCheckpoint>(
      new OffsetCheckpoint(std::move(config.consumers), config.shard_count, std::move(*file),
                           active, chosen.epoch, persisted));
}

OffsetCheckpoint::OffsetCheckpoint(std::vector<core::ConsumerId> consumers, uint32_t shard_count,
                                   pfs::File file, size_t active_slot, uint64_t epoch,
                                   std::span<const uint64_t> persisted)
    : consumers_(std::move(consumers)),
      shard_count_(shard_count),
      slot_bytes_(SlotBytes(shard_count, consumers_.size())),
      persisted_(persisted.size()),
      file_(std::move(file)),
      active_slot_(active_slot),
      epoch_(epoch),
      scratch_(slot_bytes_) {
  for (size_t i = 0; i < persisted.size(); ++i) {
    persisted_[i].store(persisted[i], std::memory_order_relaxed);
  }
}

std::optional<size_t> OffsetCheckpoint::ConsumerIndex(core::ConsumerId consumer) const {
  const auto it = std::ranges::find(consumers_, consumer);
  if (it == consumers_.end()) return std::nullopt;
  return static_cast<size_t>(it - consumers_.begin());
}

std::optional<core::SequenceId> OffsetCheckpoint::Get(core::ConsumerId consumer,
                                                      core::ShardId shard) const {
  const auto index = ConsumerIndex(consumer);
  if (!index.has_value() || shard >= shard_count_) return std::nullopt;
  return Decode(persisted_[(*index * shard_count_) + shard].load(std::memory_order_acquire));
}

core::Result<void> OffsetCheckpoint::Write(std::span<const uint64_t> encoded) {
  if (encoded.size() != persisted_.size()) {
    return std::unexpected(
        core::Error{core::ErrorCode::kInvalidArgument, "offset checkpoint write has wrong size"});
  }
  const std::scoped_lock lock(write_mu_);
  const size_t target = 1 - active_slot_;
  const uint64_t epoch = epoch_ + 1;
  EncodeSlot(scratch_, epoch, shard_count_, consumers_, encoded);
  if (auto r = pfs::Pwrite(file_, scratch_.data(), scratch_.size(), target * slot_bytes_);
      !r.has_value()) {
    return r;
  }
  if (auto r = pfs::Fsync(file_, pfs::SyncMode::kDurable); !r.has_value()) return r;

  active_slot_ = target;
  epoch_ = epoch;
  for (size_t i = 0; i < encoded.size(); ++i) {
    persisted_[i].store(encoded[i], std::memory_order_release);
  }
  return {};
}

uint64_t OffsetCheckpoint::epoch() const {
  const std::scoped_lock lock(write_mu_);
  return epoch_;
}

}  // namespace abyss::queue
