#pragma once

#include <cstdint>
#include <string_view>

namespace abyss::core {

inline constexpr uint32_t kSlotCount = 16384;

// CRC16-CCITT (0x1021, init 0, no reflect/xor) — matches Redis cluster.
uint16_t Crc16(std::string_view data) noexcept;

std::string_view HashtagContent(std::string_view key) noexcept;

uint16_t KeySlot(std::string_view key) noexcept;

}  // namespace abyss::core
