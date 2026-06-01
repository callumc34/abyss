#include "abyss/core/slot.h"

#include <array>
#include <cstdint>

namespace abyss::core {
namespace {

constexpr std::array<uint16_t, 256> BuildCrc16Table() {
  std::array<uint16_t, 256> table{};
  for (size_t byte = 0; byte < table.size(); ++byte) {
    auto crc = static_cast<uint16_t>(byte << 8U);
    for (int bit = 0; bit < 8; ++bit) {
      if ((crc & 0x8000U) != 0) {
        crc = static_cast<uint16_t>((crc << 1U) ^ 0x1021U);
      } else {
        crc = static_cast<uint16_t>(crc << 1U);
      }
    }
    // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-constant-array-index)
    table[byte] = crc;
  }
  return table;
}

constexpr auto kCrc16Table = BuildCrc16Table();

}  // namespace

uint16_t Crc16(std::string_view data) noexcept {
  uint16_t crc = 0;
  for (const char c : data) {
    const auto byte = static_cast<uint8_t>(c);
    const auto index = static_cast<uint8_t>((crc >> 8U) ^ byte);
    // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-constant-array-index)
    crc = static_cast<uint16_t>((crc << 8U) ^ kCrc16Table[index]);
  }
  return crc;
}

std::string_view HashtagContent(std::string_view key) noexcept {
  const auto open = key.find('{');
  if (open == std::string_view::npos) return key;
  const auto close = key.find('}', open + 1);
  if (close == std::string_view::npos || close == open + 1) return key;
  // Construct the view directly (noexcept) rather than substr, whose bounds
  // check can throw; `open`/`close` are valid positions from find by construction.
  return {key.data() + open + 1, close - open - 1};
}

uint16_t KeySlot(std::string_view key) noexcept {
  return static_cast<uint16_t>(Crc16(HashtagContent(key)) % kSlotCount);
}

}  // namespace abyss::core
