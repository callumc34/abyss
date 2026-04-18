#pragma once

#include <cstddef>
#include <cstdint>
#include <span>
#include <vector>

#include "abyss/core/queue_entry.h"
#include "abyss/core/result.h"
#include "abyss/core/types.h"

namespace abyss::queue {

inline constexpr uint8_t kWalFormatMajor = 1;
inline constexpr uint8_t kWalFormatMinor = 1;

enum class WalEntryType : uint8_t {
  kWrite = 0x00,
  kConditional = 0x01,
  kResolved = 0x02,
};

struct DecodedWalEntry {
  core::QueueEntry entry;
  size_t bytes_consumed = 0;
  // Sequence ID of the last entry of the batch this entry belongs to.
  core::SequenceId batch_last_seq = 0;
};

// Encode one WAL entry.
size_t EncodeWalEntry(const core::QueueEntry& entry, core::SequenceId batch_last_seq,
                      std::vector<std::byte>& out);

core::Result<DecodedWalEntry> DecodeWalEntry(std::span<const std::byte> bytes,
                                             uint8_t format_minor = kWalFormatMinor);

}  // namespace abyss::queue
