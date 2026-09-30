#include "abyss/core/resp_types.h"

#include <utility>

namespace abyss::core {

std::string_view ErrorPrefixString(ErrorPrefix prefix) noexcept {
  switch (prefix) {
    case ErrorPrefix::kErr:
      return "ERR";
    case ErrorPrefix::kWrongType:
      return "WRONGTYPE";
    case ErrorPrefix::kLoading:
      return "LOADING";
    case ErrorPrefix::kMoved:
      return "MOVED";
    case ErrorPrefix::kCrossSlot:
      return "CROSSSLOT";
    case ErrorPrefix::kOom:
      return "OOM";
    case ErrorPrefix::kNoScript:
      return "NOSCRIPT";
    case ErrorPrefix::kNoProto:
      return "NOPROTO";
    case ErrorPrefix::kReadOnly:
      return "READONLY";
    case ErrorPrefix::kNoAuth:
      return "NOAUTH";
  }
  return "ERR";
}

RespValue RespValue::Null() { return {}; }

RespValue RespValue::NullArray() {
  RespValue v;
  v.type_ = Type::kNullArray;
  return v;
}

RespValue RespValue::SimpleString(std::string value) {
  RespValue v;
  v.type_ = Type::kSimpleString;
  v.str_ = std::move(value);
  return v;
}

RespValue RespValue::BulkString(std::string value) {
  RespValue v;
  v.type_ = Type::kBulkString;
  v.str_ = std::move(value);
  return v;
}

RespValue RespValue::Integer(int64_t value) {
  RespValue v;
  v.type_ = Type::kInteger;
  v.integer_ = value;
  return v;
}

RespValue RespValue::Error(ErrorPrefix prefix, std::string message) {
  auto prefix_str = ErrorPrefixString(prefix);
  message.insert(0, 1, ' ');
  message.insert(0, prefix_str);

  RespValue v;
  v.type_ = Type::kError;
  v.str_ = std::move(message);
  v.error_prefix_ = prefix;
  return v;
}

RespValue RespValue::RawError(std::string full_body) {
  RespValue v;
  v.type_ = Type::kError;
  v.str_ = std::move(full_body);
  return v;
}

std::string_view RespValue::ErrorMessage() const {
  if (type_ != Type::kError) return {};
  const std::string_view full(str_);
  const auto space = full.find(' ');
  if (space == std::string_view::npos) return {};
  return full.substr(space + 1);
}

RespValue RespValue::Array(std::vector<RespValue> elements) {
  RespValue v;
  v.type_ = Type::kArray;
  v.elements_ = std::move(elements);
  return v;
}

}  // namespace abyss::core
