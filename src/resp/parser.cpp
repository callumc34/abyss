#include "abyss/resp/parser.h"

#include <array>
#include <charconv>
#include <cstddef>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "abyss/core/byte_cast.h"

namespace abyss::resp {
namespace {

using core::AsChars;
using core::Error;
using core::ErrorCode;
using core::ErrorPrefix;
using core::RespCommand;
using core::RespValue;
using core::Result;

constexpr size_t kNpos = static_cast<size_t>(-1);

Error Incomplete(const char* what) { return {ErrorCode::kIncomplete, what}; }
Error Malformed(std::string what) { return {ErrorCode::kInvalidArgument, std::move(what)}; }

// Finds the index of '\n' that terminates a CRLF line starting at `pos`.
size_t FindLineEnd(std::span<const uint8_t> buf, size_t pos) {
  for (size_t i = pos; i + 1 < buf.size(); ++i) {
    if (buf[i] == '\r' && buf[i + 1] == '\n') {
      return i + 1;
    }
  }
  return kNpos;
}

std::string_view BufSlice(std::span<const uint8_t> buf, size_t from, size_t to_exclusive) {
  return {AsChars(buf.data() + from), to_exclusive - from};
}

Result<int64_t> ParseSignedInt(std::string_view text) {
  int64_t value = 0;
  const auto* begin = text.data();
  const auto* end = text.data() + text.size();
  auto [ptr, ec] = std::from_chars(begin, end, value);
  if (ec != std::errc{} || ptr != end || text.empty()) {
    return std::unexpected(Malformed("protocol error: invalid integer"));
  }
  return value;
}

// All known ErrorPrefix values.
constexpr auto kAllErrorPrefixes = std::to_array<ErrorPrefix>({
    ErrorPrefix::kErr,
    ErrorPrefix::kWrongType,
    ErrorPrefix::kLoading,
    ErrorPrefix::kMoved,
    ErrorPrefix::kCrossSlot,
    ErrorPrefix::kOom,
    ErrorPrefix::kNoScript,
    ErrorPrefix::kNoProto,
    ErrorPrefix::kReadOnly,
    ErrorPrefix::kNoAuth,
});

std::optional<ErrorPrefix> LookupErrorPrefix(std::string_view text) {
  for (auto p : kAllErrorPrefixes) {
    if (core::ErrorPrefixString(p) == text) {
      return p;
    }
  }
  return std::nullopt;
}

// Forward declaration for array recursion.
Result<ParseResult> ParseOne(std::span<const uint8_t> buf, size_t pos, int depth,
                             const ParserLimits& limits);

Result<ParseResult> ParseSimpleString(std::span<const uint8_t> buf, size_t pos) {
  const size_t lf = FindLineEnd(buf, pos);
  if (lf == kNpos) {
    return std::unexpected(Incomplete("incomplete simple string"));
  }
  const std::string_view body = BufSlice(buf, pos, lf - 1);
  return ParseResult{.value = RespValue::SimpleString(std::string(body)), .bytes_consumed = lf + 1};
}

Result<ParseResult> ParseErrorValue(std::span<const uint8_t> buf, size_t pos) {
  const size_t lf = FindLineEnd(buf, pos);
  if (lf == kNpos) {
    return std::unexpected(Incomplete("incomplete error"));
  }
  const std::string_view body = BufSlice(buf, pos, lf - 1);
  auto space = body.find(' ');
  if (space == std::string_view::npos) {
    return ParseResult{.value = RespValue::Error(ErrorPrefix::kErr, std::string(body)),
                       .bytes_consumed = lf + 1};
  }
  auto prefix_text = body.substr(0, space);
  auto prefix = LookupErrorPrefix(prefix_text);
  std::string message;
  if (prefix.has_value()) {
    message = std::string(body.substr(space + 1));
  } else {
    // Unknown prefix — preserve entire body under kErr so AsString() round-trips.
    prefix = ErrorPrefix::kErr;
    message = std::string(body);
  }
  return ParseResult{.value = RespValue::Error(*prefix, std::move(message)),
                     .bytes_consumed = lf + 1};
}

Result<ParseResult> ParseInteger(std::span<const uint8_t> buf, size_t pos) {
  const size_t lf = FindLineEnd(buf, pos);
  if (lf == kNpos) {
    return std::unexpected(Incomplete("incomplete integer"));
  }
  auto value = ParseSignedInt(BufSlice(buf, pos, lf - 1));
  if (!value.has_value()) {
    return std::unexpected(value.error());
  }
  return ParseResult{.value = RespValue::Integer(*value), .bytes_consumed = lf + 1};
}

Result<ParseResult> ParseBulkString(std::span<const uint8_t> buf, size_t pos,
                                    const ParserLimits& limits) {
  const size_t lf = FindLineEnd(buf, pos);
  if (lf == kNpos) {
    return std::unexpected(Incomplete("incomplete bulk string length"));
  }
  auto len = ParseSignedInt(BufSlice(buf, pos, lf - 1));
  if (!len.has_value()) {
    return std::unexpected(len.error());
  }
  const size_t body_start = lf + 1;
  if (*len < -1) {
    return std::unexpected(Malformed("protocol error: negative bulk length"));
  }
  if (*len == -1) {
    return ParseResult{.value = RespValue::Null(), .bytes_consumed = body_start};
  }
  // Bound the declared length BEFORE allocating the payload.
  if (std::cmp_greater(*len, limits.max_bulk_len)) {
    return std::unexpected(Malformed("protocol error: bulk length exceeds limit"));
  }
  const auto length = static_cast<size_t>(*len);
  if (body_start + length + 2 > buf.size()) {
    return std::unexpected(Incomplete("incomplete bulk string body"));
  }
  if (buf[body_start + length] != '\r' || buf[body_start + length + 1] != '\n') {
    return std::unexpected(Malformed("protocol error: bulk not CRLF terminated"));
  }
  std::string payload(AsChars(buf.data() + body_start), length);
  return ParseResult{.value = RespValue::BulkString(std::move(payload)),
                     .bytes_consumed = body_start + length + 2};
}

Result<ParseResult> ParseArray(std::span<const uint8_t> buf, size_t pos, int depth,
                               const ParserLimits& limits) {
  const size_t lf = FindLineEnd(buf, pos);
  if (lf == kNpos) {
    return std::unexpected(Incomplete("incomplete array length"));
  }
  auto count = ParseSignedInt(BufSlice(buf, pos, lf - 1));
  if (!count.has_value()) {
    return std::unexpected(count.error());
  }
  if (*count < -1) {
    return std::unexpected(Malformed("protocol error: negative array length"));
  }
  size_t next = lf + 1;
  if (*count == -1) {
    return ParseResult{.value = RespValue::Null(), .bytes_consumed = next};
  }
  // Bound the declared element count BEFORE reserving against it.
  if (std::cmp_greater(*count, limits.max_array_elements)) {
    return std::unexpected(Malformed("protocol error: array element count exceeds limit"));
  }
  std::vector<RespValue> elements;
  elements.reserve(static_cast<size_t>(*count));
  for (int64_t i = 0; i < *count; ++i) {
    auto element = ParseOne(buf, next, depth + 1, limits);
    if (!element.has_value()) {
      return std::unexpected(element.error());
    }
    elements.push_back(std::move(element->value));
    next = element->bytes_consumed;
  }
  return ParseResult{.value = RespValue::Array(std::move(elements)), .bytes_consumed = next};
}

Result<ParseResult> ParseOne(std::span<const uint8_t> buf, size_t pos, int depth,
                             const ParserLimits& limits) {
  if (depth > limits.max_depth) {
    return std::unexpected(Malformed("protocol error: nesting too deep"));
  }
  if (pos >= buf.size()) {
    return std::unexpected(Incomplete("empty buffer"));
  }
  const uint8_t type_byte = buf[pos];
  const size_t after_type = pos + 1;
  switch (type_byte) {
    case '+':
      return ParseSimpleString(buf, after_type);
    case '-':
      return ParseErrorValue(buf, after_type);
    case ':':
      return ParseInteger(buf, after_type);
    case '$':
      return ParseBulkString(buf, after_type, limits);
    case '*':
      return ParseArray(buf, after_type, depth, limits);
    default:
      return std::unexpected(Malformed("protocol error: unexpected type byte"));
  }
}

bool IsInlineWhitespace(uint8_t c) { return c == ' ' || c == '\t'; }

int HexValue(uint8_t c) {
  if (c >= '0' && c <= '9') {
    return c - '0';
  }
  if (c >= 'a' && c <= 'f') {
    return 10 + (c - 'a');
  }
  if (c >= 'A' && c <= 'F') {
    return 10 + (c - 'A');
  }
  return -1;
}

// When `line_complete`, the span is a fully-received line: a quote that runs
// off the end can never be satisfied, so exhaustion is a protocol error
// (Malformed), not a request for more bytes (Incomplete).
Result<void> ParseDoubleQuoted(std::span<const uint8_t> buf, size_t pos, bool line_complete,
                               std::string* out_token, size_t* new_pos) {
  auto exhausted = [line_complete](const char* what) -> Error {
    return line_complete ? Malformed("unbalanced quotes in request") : Incomplete(what);
  };
  if (pos >= buf.size() || buf[pos] != '"') {
    return std::unexpected(Malformed("inline: expected double quote"));
  }
  size_t i = pos + 1;
  std::string token;
  while (i < buf.size()) {
    const uint8_t c = buf[i];
    if (c == '"') {
      *out_token = std::move(token);
      *new_pos = i + 1;
      return {};
    }
    if (c == '\\') {
      if (i + 1 >= buf.size()) {
        return std::unexpected(exhausted("inline: trailing backslash"));
      }
      const uint8_t esc = buf[i + 1];
      switch (esc) {
        case 'n':
          token.push_back('\n');
          i += 2;
          break;
        case 't':
          token.push_back('\t');
          i += 2;
          break;
        case 'r':
          token.push_back('\r');
          i += 2;
          break;
        case 'a':
          token.push_back('\a');
          i += 2;
          break;
        case 'b':
          token.push_back('\b');
          i += 2;
          break;
        case '"':
          token.push_back('"');
          i += 2;
          break;
        case '\\':
          token.push_back('\\');
          i += 2;
          break;
        case 'x': {
          if (i + 3 >= buf.size()) {
            return std::unexpected(exhausted("inline: truncated hex escape"));
          }
          const int hi = HexValue(buf[i + 2]);
          const int lo = HexValue(buf[i + 3]);
          if (hi < 0 || lo < 0) {
            return std::unexpected(Malformed("inline: bad hex escape"));
          }
          token.push_back(static_cast<char>((hi << 4) | lo));
          i += 4;
          break;
        }
        default:
          token.push_back(static_cast<char>(esc));
          i += 2;
          break;
      }
    } else {
      token.push_back(static_cast<char>(c));
      ++i;
    }
  }
  return std::unexpected(exhausted("inline: unterminated double quote"));
}

Result<void> ParseSingleQuoted(std::span<const uint8_t> buf, size_t pos, bool line_complete,
                               std::string* out_token, size_t* new_pos) {
  if (pos >= buf.size() || buf[pos] != '\'') {
    return std::unexpected(Malformed("inline: expected single quote"));
  }
  size_t i = pos + 1;
  std::string token;
  while (i < buf.size()) {
    const uint8_t c = buf[i];
    if (c == '\'') {
      *out_token = std::move(token);
      *new_pos = i + 1;
      return {};
    }
    if (c == '\\' && i + 1 < buf.size() && buf[i + 1] == '\'') {
      token.push_back('\'');
      i += 2;
    } else {
      token.push_back(static_cast<char>(c));
      ++i;
    }
  }
  return std::unexpected(line_complete ? Malformed("unbalanced quotes in request")
                                       : Incomplete("inline: unterminated single quote"));
}

Result<RespCommand> ParseInlineCommand(std::span<const uint8_t> buf, size_t* bytes_consumed) {
  // Locate the line terminator.
  size_t line_end = kNpos;
  for (size_t i = 0; i < buf.size(); ++i) {
    if (buf[i] == '\n') {
      line_end = i;
      break;
    }
  }
  if (line_end == kNpos) {
    return std::unexpected(Incomplete("inline: no line terminator"));
  }
  size_t content_end = line_end;
  if (content_end > 0 && buf[content_end - 1] == '\r') {
    --content_end;
  }

  RespCommand cmd;
  size_t i = 0;
  while (i < content_end) {
    while (i < content_end && IsInlineWhitespace(buf[i])) {
      ++i;
    }
    if (i >= content_end) {
      break;
    }
    std::string token;
    size_t after = 0;
    if (buf[i] == '"') {
      // The line terminator is already located, so the quoted token is scanned
      // over a fully-received span: an unbalanced quote is Malformed, not
      // Incomplete (otherwise the connection would stall forever).
      auto r =
          ParseDoubleQuoted(buf.subspan(0, content_end), i, /*line_complete=*/true, &token, &after);
      if (!r.has_value()) {
        return std::unexpected(r.error());
      }
      i = after;
    } else if (buf[i] == '\'') {
      auto r =
          ParseSingleQuoted(buf.subspan(0, content_end), i, /*line_complete=*/true, &token, &after);
      if (!r.has_value()) {
        return std::unexpected(r.error());
      }
      i = after;
    } else {
      const size_t start = i;
      while (i < content_end && !IsInlineWhitespace(buf[i])) {
        ++i;
      }
      token.assign(AsChars(buf.data() + start), i - start);
    }
    cmd.args.push_back(std::move(token));
  }

  if (cmd.args.empty()) {
    return std::unexpected(Malformed("inline: empty command"));
  }
  *bytes_consumed = line_end + 1;
  return cmd;
}

bool IsRespTypeByte(uint8_t b) { return b == '+' || b == '-' || b == ':' || b == '$' || b == '*'; }

}  // namespace

Result<ParseResult> Parser::Parse(std::span<const uint8_t> buffer, const ParserLimits& limits) {
  if (buffer.empty()) {
    return std::unexpected(Incomplete("empty buffer"));
  }
  return ParseOne(buffer, 0, 0, limits);
}

Result<ParseCommandResult> Parser::ParseCommand(std::span<const uint8_t> buffer,
                                                const ParserLimits& limits) {
  if (buffer.empty()) {
    return std::unexpected(Incomplete("empty buffer"));
  }

  // Inline path: first non-whitespace byte is not a RESP type marker.
  size_t first = 0;
  while (first < buffer.size() && IsInlineWhitespace(buffer[first])) {
    ++first;
  }
  if (first >= buffer.size()) {
    return std::unexpected(Incomplete("whitespace only"));
  }
  if (!IsRespTypeByte(buffer[first])) {
    size_t consumed = 0;
    auto inline_cmd = ParseInlineCommand(buffer, &consumed);
    if (!inline_cmd.has_value()) {
      return std::unexpected(inline_cmd.error());
    }
    return ParseCommandResult{.command = std::move(*inline_cmd), .bytes_consumed = consumed};
  }

  // RESP path: must be an array of bulk strings.
  auto parsed = Parse(buffer, limits);
  if (!parsed.has_value()) {
    return std::unexpected(parsed.error());
  }
  if (!parsed->value.IsArray()) {
    return std::unexpected(Malformed("command: not an array"));
  }
  const auto& elements = parsed->value.AsArray();
  if (elements.empty()) {
    return std::unexpected(Malformed("command: empty array"));
  }
  RespCommand cmd;
  cmd.args.reserve(elements.size());
  for (const auto& el : elements) {
    if (!el.IsBulkString()) {
      return std::unexpected(Malformed("command: non-bulk argument"));
    }
    cmd.args.push_back(el.AsString());
  }
  return ParseCommandResult{.command = std::move(cmd), .bytes_consumed = parsed->bytes_consumed};
}

}  // namespace abyss::resp
