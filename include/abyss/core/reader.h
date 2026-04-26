#pragma once

#include <optional>

#include "abyss/core/ops.h"
#include "abyss/core/resp_types.h"
#include "abyss/core/result.h"
#include "abyss/core/types.h"

namespace abyss::core {

// Uniform read interface across storage tiers (hot, buffer, cold). Backends
// that cannot honor `deadline` ignore it; backends that can return kTimeout
// when exceeded.
class Reader {
 public:
  Reader() = default;
  virtual ~Reader() = default;

  Reader(const Reader&) = delete;
  Reader& operator=(const Reader&) = delete;
  Reader(Reader&&) = delete;
  Reader& operator=(Reader&&) = delete;

  virtual Result<RespValue> Exec(const ops::ReadOp& op,
                                 std::optional<Duration> deadline = std::nullopt) = 0;
};

}  // namespace abyss::core
