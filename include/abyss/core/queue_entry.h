#pragma once

#include <variant>
#include <vector>

#include "abyss/core/predicate.h"
#include "abyss/core/resp_types.h"
#include "abyss/core/types.h"

namespace abyss::core {

enum class Decision : uint8_t { kApply = 0, kSkip = 1 };

namespace entry {

struct Write {
  RespCommand cmd;
};

struct Conditional {
  RespCommand cmd;
  PredicateFlags flags = PredicateFlags::kNone;
};

// `materialised_ops` is empty for Skip; one or more for Apply (compound
// conditionals like RENAMENX expand into Del + recreate + Expire).
struct Resolved {
  SequenceId ref = 0;
  Decision decision = Decision::kSkip;
  std::vector<RespCommand> materialised_ops;
  RespValue return_value;
};

// FLUSHDB / FLUSHALL tombstone.
struct Flush {};

}  // namespace entry

struct QueueEntry {
  SequenceId seq = 0;
  WallTime appended_at;
  std::variant<entry::Write, entry::Conditional, entry::Resolved, entry::Flush> payload;
  // The entry's effect alone determines its key's state.
  bool replaces_state = false;
};

}  // namespace abyss::core
