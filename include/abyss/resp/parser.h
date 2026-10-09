#pragma once

#include <cstddef>
#include <cstdint>
#include <span>

#include "abyss/core/resp_types.h"
#include "abyss/core/result.h"

namespace abyss::resp {

// Bounds the parser before any count-driven allocation, so adversarial wire
// input (a huge multibulk/bulk header) cannot trigger length_error/bad_alloc.
// Threaded through the recursive descent and shared by the WAL-replay decode
// path, so the same bound protects recovery (no crafted inner count can abort
// or crash pure replay).
struct ParserLimits {
  // Redis parity: 1M multibulk elements, 512MB bulk string.
  size_t max_array_elements = 1'048'576;
  size_t max_bulk_len = 536'870'912;
  int max_depth = 1024;

  static constexpr ParserLimits Default() { return {}; }
};

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
  static core::Result<ParseResult> Parse(std::span<const uint8_t> buffer,
                                         const ParserLimits& limits = ParserLimits::Default());

  // Parse one command from the start of `buffer`.
  static core::Result<ParseCommandResult> ParseCommand(
      std::span<const uint8_t> buffer, const ParserLimits& limits = ParserLimits::Default());
};

}  // namespace abyss::resp
