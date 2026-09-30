#pragma once

#include <cstdint>
#include <expected>
#include <string>

namespace abyss::core {

enum class ErrorCode : uint8_t {
  kNotFound,
  kAlreadyExists,
  kInvalidArgument,
  kInternal,
  kUnavailable,
  kTimeout,
  kResourceExhausted,
  kCorruption,
  kIncomplete,
  kWrongType,
  // A precondition for the operation was not yet met (e.g. acking a retention
  // offset past the durable WAL tail). Retryable once the precondition holds.
  kFailedPrecondition,
  // A value exceeds the configured queue.max_value_size_bytes ceiling.
  kValueTooLarge,
};

class Error {
 public:
  Error(ErrorCode code, std::string message) : code_(code), message_(std::move(message)) {}

  ErrorCode code() const { return code_; }
  const std::string& message() const { return message_; }

 private:
  ErrorCode code_;
  std::string message_;
};

template <typename T>
using Result = std::expected<T, Error>;

}  // namespace abyss::core
