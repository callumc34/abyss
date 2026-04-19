#include "abyss/core/queue_entry.h"

#include <variant>

namespace abyss::core::entry {

Result<const RespCommand*> ExtractApplicableCommand(const QueueEntry& entry) {
  if (const auto* w = std::get_if<entry::Write>(&entry.payload)) {
    return &w->cmd;
  }
  if (const auto* r = std::get_if<entry::Resolved>(&entry.payload)) {
    if (r->decision == Decision::kApply && r->materialised_op.has_value()) {
      return &*r->materialised_op;
    }
    return std::unexpected(Error{ErrorCode::kNotFound, "resolved entry is a skip"});
  }
  // entry::Conditional — resolver is required before this entry can be applied.
  return std::unexpected(Error{ErrorCode::kInvalidArgument, "conditional entry requires resolver"});
}

}  // namespace abyss::core::entry
