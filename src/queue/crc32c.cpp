#include "crc32c.h"

#include <crc32c/crc32c.h>

namespace abyss::queue {

uint32_t Crc32c(std::span<const std::byte> bytes) {
  return ::crc32c::Crc32c(reinterpret_cast<const uint8_t*>(bytes.data()), bytes.size());
}

uint32_t Crc32cExtend(uint32_t crc, std::span<const std::byte> bytes) {
  return ::crc32c::Extend(crc, reinterpret_cast<const uint8_t*>(bytes.data()), bytes.size());
}

}  // namespace abyss::queue
