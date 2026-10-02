#include "abyss/core/durability.h"

#include <gtest/gtest.h>

#include <optional>

namespace abyss::core {
namespace {

TEST(DurabilityTest, NamesRoundTrip) {
  for (const Durability d : {Durability::kProcessCrash, Durability::kPowerLoss}) {
    EXPECT_EQ(ParseDurability(DurabilityName(d)), std::optional<Durability>{d})
        << DurabilityName(d);
  }
  EXPECT_EQ(DurabilityName(Durability::kProcessCrash), "process_crash");
  EXPECT_EQ(DurabilityName(Durability::kPowerLoss), "power_loss");
}

TEST(DurabilityTest, ParseRejectsOtherSpellings) {
  for (const char* bad : {"", "group_commit", "fsync_none", "Power_Loss", "process-crash"}) {
    EXPECT_FALSE(ParseDurability(bad).has_value()) << bad;
  }
}

}  // namespace
}  // namespace abyss::core
