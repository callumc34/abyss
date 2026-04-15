#pragma once

#include <cstddef>
#include <cstdint>
#include <span>
#include <vector>

#include "abyss/core/queue.h"
#include "abyss/core/result.h"

namespace abyss::queue {

inline constexpr uint8_t kWalFormatMajor = 1;
inline constexpr uint8_t kWalFormatMinor = 0;

enum class WalEntryType : uint8_t {
  kRespCommand = 0,
};

struct DecodedWalEntry {
  core::LogEntry entry;
  size_t bytes_consumed = 0;
};

size_t EncodeWalEntry(const core::LogEntry& entry, std::vector<std::byte>& out);

core::Result<DecodedWalEntry> DecodeWalEntry(std::span<const std::byte> bytes);

}  // namespace abyss::queue
