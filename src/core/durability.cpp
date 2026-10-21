#include "abyss/core/durability.h"

namespace abyss::core {

namespace {

constexpr std::string_view kProcessCrashName = "process_crash";
constexpr std::string_view kPowerLossName = "power_loss";

}  // namespace

std::string_view DurabilityName(Durability durability) noexcept {
  switch (durability) {
    case Durability::kProcessCrash:
      return kProcessCrashName;
    case Durability::kPowerLoss:
      return kPowerLossName;
  }
  return kProcessCrashName;
}

std::optional<Durability> ParseDurability(std::string_view name) noexcept {
  if (name == kProcessCrashName) return Durability::kProcessCrash;
  if (name == kPowerLossName) return Durability::kPowerLoss;
  return std::nullopt;
}

}  // namespace abyss::core
