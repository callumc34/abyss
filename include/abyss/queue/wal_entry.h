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
  kFlush = 0x03,
};

struct DecodedWalEntry {
  core::QueueEntry entry;
  size_t bytes_consumed = 0;
  // Sequence ID of the last entry of the batch this entry belongs to.
  core::SequenceId batch_last_seq = 0;
};

// Classifies a decode failure for the recovery scanner (Decision 6).
//   kNone        — decode succeeded.
//   kTornTail    — incomplete bytes or a failed CRC: a partially-written tail
//                  from a crash. Safe to truncate (the write was never acked).
//   kCorruptFrame— the CRC validated but the body structure could not be
//                  decoded: genuine corruption of durably-acked data. Recovery
//                  must fail-stop, never truncate-and-continue.
enum class WalDecodeFailure : uint8_t { kNone, kTornTail, kCorruptFrame };

// Encode one WAL entry.
size_t EncodeWalEntry(const core::QueueEntry& entry, core::SequenceId batch_last_seq,
                      std::vector<std::byte>& out);

core::Result<DecodedWalEntry> DecodeWalEntry(std::span<const std::byte> bytes,
                                             uint8_t format_minor = kWalFormatMinor);

// As DecodeWalEntry, but on failure sets `failure` so the scanner can decide
// between truncating a torn tail and halting on genuine corruption.
core::Result<DecodedWalEntry> DecodeWalEntry(std::span<const std::byte> bytes, uint8_t format_minor,
                                             WalDecodeFailure& failure);

}  // namespace abyss::queue
