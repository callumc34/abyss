#include "server_fixture.h"

namespace abyss::system_test {
namespace {

using DurabilityTestFixture = IsolatedDataServerTest;

TEST_F(DurabilityTestFixture, DataSurvivesCleanRestart) {
  EXPECT_TRUE(Client().Command({"SET", "persist", "value"}).IsOk());
  EXPECT_TRUE(Client().Command({"SET", "persist2", "value2"}).IsOk());

  RestartServer();

  auto r1 = Client().Command({"GET", "persist"});
  ASSERT_TRUE(r1.IsBulk());
  EXPECT_EQ(r1.String(), "value");

  auto r2 = Client().Command({"GET", "persist2"});
  ASSERT_TRUE(r2.IsBulk());
  EXPECT_EQ(r2.String(), "value2");
}

TEST_F(DurabilityTestFixture, DataSurvivesKill) {
  constexpr int kKeys = 100;
  for (int i = 0; i < kKeys; ++i) {
    std::string key = "kill_" + std::to_string(i);
    ASSERT_TRUE(Client().Command({"SET", key, "v"}).IsOk());
  }

  KillAndRestartServer();

  int recovered = 0;
  for (int i = 0; i < kKeys; ++i) {
    std::string key = "kill_" + std::to_string(i);
    auto r = Client().Command({"GET", key});
    if (r.IsBulk() && r.String() == "v") ++recovered;
  }
  EXPECT_EQ(recovered, kKeys) << "all committed keys should survive SIGKILL";
}

TEST_F(DurabilityTestFixture, WalReplayPreservesLastWrite) {
  EXPECT_TRUE(Client().Command({"SET", "k", "v1"}).IsOk());
  EXPECT_TRUE(Client().Command({"SET", "k", "v2"}).IsOk());
  EXPECT_TRUE(Client().Command({"SET", "k", "v3"}).IsOk());

  KillAndRestartServer();

  auto r = Client().Command({"GET", "k"});
  ASSERT_TRUE(r.IsBulk());
  EXPECT_EQ(r.String(), "v3") << "last write should win after WAL replay";
}

}  // namespace
}  // namespace abyss::system_test
