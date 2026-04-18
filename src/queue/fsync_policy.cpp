#include "abyss/queue/fsync_policy.h"

#include <string>

namespace abyss::queue {

core::Result<FsyncPolicy> FsyncPolicyFromString(std::string_view name) {
  if (name == "per_write" || name == "fsync_per_write") {
    return FsyncPolicy::kPerWrite;
  }
  if (name == "group_commit") {
    return FsyncPolicy::kGroupCommit;
  }
  if (name == "none" || name == "fsync_none") {
    return FsyncPolicy::kNone;
  }
  return std::unexpected(core::Error{
      core::ErrorCode::kInvalidArgument,
      "unknown fsync policy: " + std::string(name) + " (expected: per_write, group_commit, none)"});
}

std::string_view FsyncPolicyToString(FsyncPolicy policy) {
  switch (policy) {
    case FsyncPolicy::kPerWrite:
      return "per_write";
    case FsyncPolicy::kGroupCommit:
      return "group_commit";
    case FsyncPolicy::kNone:
      return "none";
  }
  return "unknown";
}

}  // namespace abyss::queue
