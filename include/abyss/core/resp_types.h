#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace abyss::core {

class RespValue {
 public:
  enum class Type { kNull, kString, kInteger, kError, kArray };

  static RespValue Null();
  static RespValue String(std::string value);
  static RespValue Integer(int64_t value);
  static RespValue Error(std::string message);
  static RespValue Array(std::vector<RespValue> elements);

  Type type() const { return type_; }
  bool IsNull() const { return type_ == Type::kNull; }
  bool IsString() const { return type_ == Type::kString; }
  bool IsInteger() const { return type_ == Type::kInteger; }
  bool IsError() const { return type_ == Type::kError; }
  bool IsArray() const { return type_ == Type::kArray; }

  const std::string& AsString() const { return str_; }
  int64_t AsInteger() const { return integer_; }
  const std::vector<RespValue>& AsArray() const { return elements_; }

 private:
  Type type_ = Type::kNull;
  std::string str_;
  int64_t integer_ = 0;
  std::vector<RespValue> elements_;
};

struct RespCommand {
  std::vector<std::string> args;

  const std::string& Name() const { return args[0]; }
  size_t ArgCount() const { return args.size(); }
};

}  // namespace abyss::core
