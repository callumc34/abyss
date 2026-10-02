#pragma once

#include <cstddef>
#include <span>
#include <vector>

#include "abyss/core/queue_entry.h"
#include "abyss/core/result.h"
#include "abyss/queue/wal_entry.h"

// The per-type entry body shared by WAL formats 1 and 2: everything
// after the type byte, seq and appended_at.
namespace abyss::queue::entry_payload {

using Payload = decltype(core::QueueEntry::payload);

WalEntryType TypeOf(const core::QueueEntry& entry);

void Encode(const core::QueueEntry& entry, std::vector<std::byte>& out);

// Consumes the payload of `type` from the front of `cursor`.
core::Result<Payload> Decode(WalEntryType type, std::span<const std::byte>& cursor);

}  // namespace abyss::queue::entry_payload
