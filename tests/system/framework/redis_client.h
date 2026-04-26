#pragma once

#include <chrono>
#include <cstdint>
#include <initializer_list>
#include <iosfwd>
#include <string>
#include <vector>

#include "abyss/platform/types.h"

namespace abyss::system_test {

class Reply {
 public:
  enum class Type : uint8_t { kStatus, kError, kInteger, kBulk, kArray, kNil };

  static Reply Status(std::string s);
  static Reply Error(std::string s);
  static Reply Integer(int64_t n);
  static Reply Bulk(std::string s);
  static Reply Array(std::vector<Reply> elements);
  static Reply Nil();

  Type type() const { return type_; }
  const std::string& String() const { return str_; }
  int64_t Integer() const { return int_; }
  const std::vector<Reply>& Elements() const { return elements_; }

  bool IsOk() const { return type_ == Type::kStatus && str_ == "OK"; }
  bool IsNil() const { return type_ == Type::kNil; }
  bool IsError() const { return type_ == Type::kError; }
  bool IsStatus() const { return type_ == Type::kStatus; }
  bool IsBulk() const { return type_ == Type::kBulk; }
  bool IsInteger() const { return type_ == Type::kInteger; }
  bool IsArray() const { return type_ == Type::kArray; }

  friend std::ostream& operator<<(std::ostream& os, const Reply& r);
  friend std::ostream& operator<<(std::ostream& os, Type t);

 private:
  Type type_ = Type::kNil;
  std::string str_;
  int64_t int_ = 0;
  std::vector<Reply> elements_;
};

class RedisClient {
 public:
  RedisClient() = default;
  ~RedisClient();

  RedisClient(const RedisClient&) = delete;
  RedisClient& operator=(const RedisClient&) = delete;
  RedisClient(RedisClient&& other) noexcept;
  RedisClient& operator=(RedisClient&& other) noexcept;

  bool Connect(const std::string& host, uint16_t port,
               std::chrono::milliseconds timeout = std::chrono::milliseconds{5000});
  void Close();
  bool IsConnected() const { return fd_ != abyss::platform::kInvalidSocket; }

  Reply Command(std::initializer_list<std::string> args);
  std::vector<Reply> Pipeline(const std::vector<std::vector<std::string>>& commands);

 private:
  bool SendEncoded(const std::string& data);
  Reply ReadReply();
  bool ReadLine(std::string& out);
  bool ReadBytes(size_t n, std::string& out);
  bool FillBuffer();

  static std::string Encode(const std::vector<std::string>& args);

  abyss::platform::Socket fd_ = abyss::platform::kInvalidSocket;
  std::vector<char> buf_;
  size_t rpos_ = 0;
  size_t wpos_ = 0;
};

}  // namespace abyss::system_test
