#pragma once

#include <cstddef>
#include <cstdint>
#include <span>
#include <vector>

#include "abyss/core/queue_entry.h"
#include "abyss/core/result.h"

// An entry frame's payload: everything after its fixed header.
namespace abyss::queue::entry_payload {

// The frame header's type byte (ADP-009).
enum class EntryType : uint8_t {
  kWrite = 0x00,
  kConditional = 0x01,
  kResolved = 0x02,
  kFlush = 0x03,
};

using Payload = decltype(core::QueueEntry::payload);

EntryType TypeOf(const core::QueueEntry& entry);

void Encode(const core::QueueEntry& entry, std::vector<std::byte>& out);

// Consumes the payload of `type` from the front of `cursor`.
core::Result<Payload> Decode(EntryType type, std::span<const std::byte>& cursor);

}  // namespace abyss::queue::entry_payload
