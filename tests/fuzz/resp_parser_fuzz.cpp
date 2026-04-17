#include <cstddef>
#include <cstdint>
#include <span>

#include "abyss/resp/parser.h"

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
  auto buf = std::span<const uint8_t>(data, size);

  auto parse_result = abyss::resp::Parser::Parse(buf);
  if (parse_result.has_value()) {
    (void)parse_result->bytes_consumed;
  }

  auto cmd_result = abyss::resp::Parser::ParseCommand(buf);
  if (cmd_result.has_value()) {
    (void)cmd_result->bytes_consumed;
  }

  return 0;
}
