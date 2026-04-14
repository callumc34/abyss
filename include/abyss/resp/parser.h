#pragma once

#include <cstddef>
#include <cstdint>
#include <span>

#include "abyss/core/resp_types.h"
#include "abyss/core/result.h"

namespace abyss::resp {

struct ParseResult {
  core::RespValue value;
  size_t bytes_consumed;
};

class Parser {
 public:
  static core::Result<ParseResult> Parse(std::span<const uint8_t> buffer);
  static core::Result<core::RespCommand> ParseCommand(std::span<const uint8_t> buffer);
};

}  // namespace abyss::resp
