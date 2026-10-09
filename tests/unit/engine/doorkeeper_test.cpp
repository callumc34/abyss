#include "abyss/engine/doorkeeper.h"

#include <gtest/gtest.h>

#include <string>

namespace abyss::engine {
namespace {

TEST(DoorkeeperTest, AdmitsAKeyOnItsSecondAccess) {
  Doorkeeper doorkeeper(1024);
  EXPECT_FALSE(doorkeeper.Admit("k"));
  EXPECT_TRUE(doorkeeper.Admit("k"));
  EXPECT_TRUE(doorkeeper.Admit("k"));
  EXPECT_FALSE(doorkeeper.Admit("other"));
}

TEST(DoorkeeperTest, FewFirstAccessesAreAdmitted) {
  constexpr int kKeys = 1000;
  Doorkeeper doorkeeper(kKeys);
  int admitted = 0;
  for (int i = 0; i < kKeys - 1; ++i) {
    if (doorkeeper.Admit("key-" + std::to_string(i))) ++admitted;
  }
  // About 1% false positives at the window's size; 3% is far out.
  EXPECT_LT(admitted, kKeys * 3 / 100);
}

TEST(DoorkeeperTest, ForgetsEverythingOnceTheWindowFills) {
  constexpr int kWindow = 64;
  Doorkeeper doorkeeper(kWindow);
  EXPECT_FALSE(doorkeeper.Admit("k"));
  int recorded = 1;
  for (int i = 0; recorded < kWindow; ++i) {
    if (!doorkeeper.Admit("filler-" + std::to_string(i))) ++recorded;
  }
  EXPECT_FALSE(doorkeeper.Admit("k")) << "the window reset";
}

}  // namespace
}  // namespace abyss::engine
