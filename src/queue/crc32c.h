#pragma once

#include <cstddef>
#include <cstdint>
#include <span>

namespace abyss::queue {

uint32_t Crc32c(std::span<const std::byte> bytes);
// Continues `crc` over `bytes`, as if they followed its input.
uint32_t Crc32cExtend(uint32_t crc, std::span<const std::byte> bytes);

}  // namespace abyss::queue
