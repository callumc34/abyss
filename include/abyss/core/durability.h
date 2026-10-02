#pragma once

#include <cstdint>
#include <optional>
#include <string_view>

namespace abyss::core {

// The failure a log entry survives once it is durable at this class.
// kProcessCrash: the entry is in the OS page cache. kPowerLoss: the
// fdatasync covering it has completed. Ordered: kPowerLoss implies
// kProcessCrash.
enum class Durability : uint8_t { kProcessCrash, kPowerLoss };

// "process_crash" | "power_loss", as config, /status and CONFIG GET
// spell it.
std::string_view DurabilityName(Durability durability) noexcept;
std::optional<Durability> ParseDurability(std::string_view name) noexcept;

}  // namespace abyss::core
