#pragma once

#include <cstddef>
#include <cstdint>
#include <span>
#include <vector>

#include "abyss/core/queue_entry.h"
#include "abyss/core/result.h"
#include "binary_io.h"

// An entry frame's payload: everything after its fixed header.
namespace abyss::queue::entry_payload {

// The frame header's type byte (ADP-009).
// 0x01 and 0x02 are reserved, retired conditional and resolved types;
// those and any other type byte are corruption.
enum class EntryType : uint8_t {
  kWrite = 0x00,
  kFlush = 0x03,
};

using Payload = decltype(core::QueueEntry::payload);

EntryType TypeOf(const core::QueueEntry& entry);

// EncodedSize bytes; past the writer's end it overflows.
void Encode(const core::QueueEntry& entry, binary::SpanWriter& out);
void Encode(const core::QueueEntry& entry, std::vector<std::byte>& out);
std::size_t EncodedSize(const core::QueueEntry& entry);

// Consumes the payload of `type` from the front of `cursor`.
core::Result<Payload> Decode(EntryType type, std::span<const std::byte>& cursor);

}  // namespace abyss::queue::entry_payload
