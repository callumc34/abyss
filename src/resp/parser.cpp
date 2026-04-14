#include "abyss/resp/parser.h"

namespace abyss::resp {

core::Result<ParseResult> Parser::Parse(std::span<const uint8_t> /*buffer*/) {
  return std::unexpected(core::Error(core::ErrorCode::kInternal, "Parser::Parse not implemented"));
}

core::Result<core::RespCommand> Parser::ParseCommand(std::span<const uint8_t> /*buffer*/) {
  return std::unexpected(
      core::Error(core::ErrorCode::kInternal, "Parser::ParseCommand not implemented"));
}

}  // namespace abyss::resp
