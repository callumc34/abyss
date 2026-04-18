#pragma once

#include <string_view>

#include "abyss/core/result.h"

namespace abyss::queue {

enum class FsyncPolicy : uint8_t {
  kPerWrite,
  kGroupCommit,
  kNone,
};

core::Result<FsyncPolicy> FsyncPolicyFromString(std::string_view name);

std::string_view FsyncPolicyToString(FsyncPolicy policy);

}  // namespace abyss::queue
