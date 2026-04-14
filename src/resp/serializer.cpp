#include "abyss/resp/serializer.h"

namespace abyss::resp {

std::vector<uint8_t> Serializer::Serialize(const core::RespValue& /*value*/) { return {}; }

std::vector<uint8_t> Serializer::SerializeCommand(const core::RespCommand& /*cmd*/) { return {}; }

}  // namespace abyss::resp
