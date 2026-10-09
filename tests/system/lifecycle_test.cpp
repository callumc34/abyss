#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "http_client.h"
#include "server_fixture.h"

namespace abyss::system_test {
namespace {

// ENGINE-1: the LOADING gate must lift in exactly one observable edge,
// after recovery and once the cold consumer pool is live. The moment
// the readiness pipe line is written (which Server::Run emits only
// after the kServing transition), a write must succeed: never time out
// or return -LOADING.
using LifecycleTest = IsolatedServerTest;
using LifecycleDataTest = IsolatedDataServerTest;

TEST_F(LifecycleTest, WritesSucceedImmediatelyAtReady) {
  // WaitForReady() (run in the fixture SetUp) returned the instant the
  // ready line was emitted. Fire a burst of pipelined writes with no
  // settle delay: none may race the end of startup.
  constexpr int kKeys = 200;
  std::vector<std::vector<std::string>> writes;
  writes.reserve(kKeys);
  for (int i = 0; i < kKeys; ++i) {
    writes.push_back({"SET", "k" + std::to_string(i), "v" + std::to_string(i)});
  }

  const auto replies = Client().Pipeline(writes);
  ASSERT_EQ(replies.size(), static_cast<size_t>(kKeys));
  for (int i = 0; i < kKeys; ++i) {
    EXPECT_TRUE(replies[i].IsOk())
        << "write at ready returned non-OK (LOADING/timeout window) at i=" << i << " reply=["
        << (replies[i].IsError() ? replies[i].String() : "non-error") << "]";
  }
}

TEST_F(LifecycleDataTest, WritesSucceedImmediatelyAcrossGracefulRestart) {
  // Repeat the at-ready write burst across a graceful (SIGTERM)
  // restart, which drives the recovery->serving edge with prior data in
  // the WAL. The gate must still lift only once recovery is done and
  // the cold consumer pool is live.
  ASSERT_TRUE(Client().Command({"SET", "seed", "v"}).IsOk());
  RestartServer();  // graceful: SIGTERM then re-Start, re-waits the ready line.

  const auto r = Client().Command({"SET", "after_restart", "v"});
  EXPECT_TRUE(r.IsOk())
      << "first write after a graceful restart should be served immediately, got ["
      << (r.IsError() ? r.String() : "non-error") << "]";
  EXPECT_EQ(Client().Command({"GET", "seed"}).String(), "v");
}

TEST_F(LifecycleTest, LifecycleGaugeReportsServing) {
  // The single lifecycle atomic is surfaced as a gauge; a serving server
  // reports state 2 (kServing). This is the observable backing the LOADING gate.
  const auto resp =
      abyss::testing::HttpTestClient::Send("127.0.0.1", Server().MetricsPort(), "GET", "/metrics");
  ASSERT_TRUE(resp.ok) << resp.error;
  ASSERT_EQ(resp.status, 200);
  EXPECT_NE(resp.body.find("abyss_server_lifecycle_state 2"), std::string::npos)
      << "lifecycle gauge should report kServing (2) while serving; body:\n"
      << resp.body;
}

// G6: a graceful SIGTERM shutdown drains the in-memory cold compaction buffer
// to durable storage before stopping, so the data survives a graceful restart
// (and the cold commit advanced, bounding replay). The fixture's RestartServer()
// delivers SIGTERM, which exercises the full drain path end-to-end.
TEST_F(LifecycleDataTest, ColdBufferSurvivesGracefulRestart) {
  constexpr int kKeys = 300;
  for (int i = 0; i < kKeys; ++i) {
    ASSERT_TRUE(
        Client().Command({"SET", "drain_" + std::to_string(i), "val_" + std::to_string(i)}).IsOk())
        << "pre-shutdown SET refused at i=" << i;
  }

  RestartServer();  // SIGTERM -> graceful drain -> restart.

  int recovered = 0;
  for (int i = 0; i < kKeys; ++i) {
    const auto r = Client().Command({"GET", "drain_" + std::to_string(i)});
    if (r.IsBulk() && r.String() == "val_" + std::to_string(i)) ++recovered;
  }
  EXPECT_EQ(recovered, kKeys) << "graceful drain + restart must preserve every write";
}

}  // namespace
}  // namespace abyss::system_test
