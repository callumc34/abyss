#include <cstddef>
#include <cstdint>
#include <span>

#include "abyss/queue/segment_header.h"
#include "abyss/queue/wal_entry.h"

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
  auto bytes = std::span<const std::byte>(reinterpret_cast<const std::byte*>(data), size);

  auto entry_result = abyss::queue::DecodeWalEntry(bytes);
  if (entry_result.has_value()) {
    (void)entry_result->bytes_consumed;
  }

  auto header_result = abyss::queue::DecodeSegmentHeader(bytes);
  if (header_result.has_value()) {
    (void)header_result->format_major;
  }

  return 0;
}
