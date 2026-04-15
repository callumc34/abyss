#pragma once

#include <cstddef>
#include <cstdint>
#include <span>

namespace abyss::queue {

uint32_t Crc32c(std::span<const std::byte> bytes);

}  // namespace abyss::queue
