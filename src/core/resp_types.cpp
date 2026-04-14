#include "abyss/core/resp_types.h"

#include <utility>

namespace abyss::core {

RespValue RespValue::Null() { return {}; }

RespValue RespValue::String(std::string value) {
  RespValue v;
  v.type_ = Type::kString;
  v.str_ = std::move(value);
  return v;
}

RespValue RespValue::Integer(int64_t value) {
  RespValue v;
  v.type_ = Type::kInteger;
  v.integer_ = value;
  return v;
}

RespValue RespValue::Error(std::string message) {
  RespValue v;
  v.type_ = Type::kError;
  v.str_ = std::move(message);
  return v;
}

RespValue RespValue::Array(std::vector<RespValue> elements) {
  RespValue v;
  v.type_ = Type::kArray;
  v.elements_ = std::move(elements);
  return v;
}

}  // namespace abyss::core
