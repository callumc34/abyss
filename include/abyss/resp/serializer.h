#pragma once

#include <cstdint>
#include <vector>

#include "abyss/core/resp_types.h"

namespace abyss::resp {

class Serializer {
 public:
  static std::vector<uint8_t> Serialize(const core::RespValue& value);
  static std::vector<uint8_t> SerializeCommand(const core::RespCommand& cmd);
};

}  // namespace abyss::resp
