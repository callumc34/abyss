#pragma once

#include <variant>

#include "abyss/core/resp_types.h"
#include "abyss/core/types.h"

namespace abyss::core {

namespace entry {

// A decided effect, in its canonical form.
struct Write {
  RespCommand cmd;
};

// FLUSHDB / FLUSHALL tombstone.
struct Flush {};

}  // namespace entry

struct QueueEntry {
  SequenceId seq = 0;
  WallTime appended_at;
  std::variant<entry::Write, entry::Flush> payload;
  // The entry's effect alone determines its key's state.
  bool replaces_state = false;
};

}  // namespace abyss::core
