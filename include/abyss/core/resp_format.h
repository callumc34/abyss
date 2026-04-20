#pragma once

#include <string>

namespace abyss::core {

// Redis-compatible double formatter; matches `addReplyDouble` byte-for-byte.
std::string FormatRespDouble(double value);

}  // namespace abyss::core
