#pragma once

#include <string_view>

#include "abyss/core/resp_types.h"
#include "abyss/core/result.h"

namespace abyss::consumer {

// Abstract interface for resolving a key to its read result.
class CompactionBufferRouter {
 public:
  CompactionBufferRouter() = default;
  virtual ~CompactionBufferRouter() = default;
  CompactionBufferRouter(const CompactionBufferRouter&) = delete;
  CompactionBufferRouter& operator=(const CompactionBufferRouter&) = delete;
  CompactionBufferRouter(CompactionBufferRouter&&) = delete;
  CompactionBufferRouter& operator=(CompactionBufferRouter&&) = delete;

  virtual core::Result<core::RespValue> Read(std::string_view key) const = 0;
};

}  // namespace abyss::consumer
