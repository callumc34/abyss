#pragma once

#include <cstddef>
#include <cstdint>
#include <span>

#include "abyss/core/resp_types.h"
#include "abyss/core/result.h"

namespace abyss::resp {

struct ParseResult {
  core::RespValue value;
  size_t bytes_consumed = 0;
};

struct ParseCommandResult {
  core::RespCommand command;
  size_t bytes_consumed = 0;
};

class Parser {
 public:
  // Parse one RESP frame from the start of `buffer`.
  static core::Result<ParseResult> Parse(std::span<const uint8_t> buffer);

  // Parse one command from the start of `buffer`.
  static core::Result<ParseCommandResult> ParseCommand(std::span<const uint8_t> buffer);
};

}  // namespace abyss::resp
