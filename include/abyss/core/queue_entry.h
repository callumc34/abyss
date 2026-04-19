#pragma once

#include <optional>
#include <variant>

#include "abyss/core/predicate.h"
#include "abyss/core/resp_types.h"
#include "abyss/core/result.h"
#include "abyss/core/types.h"

namespace abyss::core {

enum class Decision : uint8_t { kApply = 0, kSkip = 1 };

namespace entry {

struct Write {
  RespCommand cmd = {};
};

struct Conditional {
  RespCommand cmd = {};
  PredicateFlags flags = PredicateFlags::kNone;
};

struct Resolved {
  SequenceId ref = 0;
  Decision decision = Decision::kSkip;
  std::optional<RespCommand> materialised_op = std::nullopt;
  RespValue return_value = {};
};

}  // namespace entry

struct QueueEntry {
  SequenceId seq = 0;
  WallTime appended_at = {};
  std::variant<entry::Write, entry::Conditional, entry::Resolved> payload = {};
};

namespace entry {

// Returns the RespCommand a consumer should dispatch.
// kNotFound signals an intentional skip; kInvalidArgument a parse-level failure.
Result<const RespCommand*> ExtractApplicableCommand(const QueueEntry& entry);

}  // namespace entry

}  // namespace abyss::core
