#pragma once

#include <cstddef>
#include <cstdint>
#include <vector>

#include "abyss/core/resp_types.h"

namespace abyss::resp {

class Serializer {
 public:
  static std::vector<uint8_t> Serialize(const core::RespValue& value);
  // Appends `value`'s encoding to `out`.
  static void Append(const core::RespValue& value, std::vector<uint8_t>& out);
  // The length of `value`'s encoding.
  static size_t Size(const core::RespValue& value);
  static std::vector<uint8_t> SerializeCommand(const core::RespCommand& cmd);
};

}  // namespace abyss::resp
