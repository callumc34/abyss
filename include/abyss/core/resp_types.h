#pragma once

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

namespace abyss::core {

enum class ErrorPrefix : uint8_t {
  kErr,
  kWrongType,
  kLoading,
  kMoved,
  kCrossSlot,
  kOom,
  kNoScript,
  kNoProto,
  kReadOnly,
  kNoAuth,
};

std::string_view ErrorPrefixString(ErrorPrefix prefix) noexcept;

class RespValue {
 public:
  enum class Type : uint8_t {
    kNull,
    kNullArray,
    kSimpleString,
    kBulkString,
    kInteger,
    kError,
    kArray,
  };

  static RespValue Null();
  // Serialises as `*-1`, the nil Redis uses where a reply is array-shaped
  // (e.g. COMMAND INFO of an unknown command), as opposed to Null()'s `$-1`.
  static RespValue NullArray();
  static RespValue SimpleString(std::string value);
  static RespValue BulkString(std::string value);
  static RespValue Integer(int64_t value);
  static RespValue Error(ErrorPrefix prefix, std::string message);
  // Keeps `full_body` verbatim, prefix token included, so an error carrying a
  // prefix outside ErrorPrefix survives parse -> serialise byte-for-byte.
  static RespValue RawError(std::string full_body);
  static RespValue Array(std::vector<RespValue> elements);

  Type type() const { return type_; }
  bool IsNull() const { return type_ == Type::kNull; }
  bool IsNullArray() const { return type_ == Type::kNullArray; }
  bool IsSimpleString() const { return type_ == Type::kSimpleString; }
  bool IsBulkString() const { return type_ == Type::kBulkString; }
  bool IsInteger() const { return type_ == Type::kInteger; }
  bool IsError() const { return type_ == Type::kError; }
  bool IsArray() const { return type_ == Type::kArray; }

  const std::string& AsString() const { return str_; }
  int64_t AsInteger() const { return integer_; }
  const std::vector<RespValue>& AsArray() const { return elements_; }

  // Defined only when IsError(); other types return kErr.
  ErrorPrefix ErrorPrefixOf() const { return error_prefix_; }
  // The message body without the prefix token. Defined only when IsError().
  std::string_view ErrorMessage() const;

 private:
  Type type_ = Type::kNull;
  std::string str_;
  int64_t integer_ = 0;
  std::vector<RespValue> elements_;
  ErrorPrefix error_prefix_ = ErrorPrefix::kErr;
};

struct RespCommand {
  std::vector<std::string> args;

  const std::string& Name() const { return args[0]; }
  size_t ArgCount() const { return args.size(); }
};

}  // namespace abyss::core
