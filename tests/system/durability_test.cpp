#include <chrono>

#include "http_client.h"
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

TEST_F(DurabilityTestFixture, ExpiredAbsoluteTtlIsAbsentAfterRecovery) {
  // PXAT carries an absolute Unix-ms deadline that the parser preserves
  // verbatim across replay — distinct from PX/EX, which currently anchor to
  // each parser invocation's wall clock and so do not survive recovery (see
  // resp/request_pipeline open issue). Use PXAT so the deadline is fixed in
  // the past at recovery time, exercising abs-TTL skip / lazy-expiry.
  const auto now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                          std::chrono::system_clock::now().time_since_epoch())
                          .count();
  const auto past_ms = std::to_string(now_ms - 5000);
  EXPECT_TRUE(Client().Command({"SET", "ephemeral", "v", "PXAT", past_ms}).IsOk());
  KillAndRestartServer();

  auto r = Client().Command({"GET", "ephemeral"});
  EXPECT_TRUE(r.IsNil()) << "expired key should not be readable post-recovery, got: " << r;
}

TEST_F(DurabilityTestFixture, MsetnxAtomicOutcomeSurvivesCrash) {
  // MSETNX is conditional: succeeds only if all keys are absent. Run it,
  // crash-and-restart, and verify that whichever side the resolver decided is
  // the side observed by the client. (We don't assert success-vs-skip because
  // the crash may happen before or after the Resolved emission; the property
  // is determinism — the post-crash decision matches the pre-crash one had it
  // landed.)
  ASSERT_TRUE(Client().Command({"DEL", "a", "b"}).IsInteger());
  auto r = Client().Command({"MSETNX", "a", "1", "b", "2"});
  ASSERT_TRUE(r.IsInteger());
  const bool initial_apply = (r.Integer() == 1);

  KillAndRestartServer();

  auto a = Client().Command({"GET", "a"});
  auto b = Client().Command({"GET", "b"});
  if (initial_apply) {
    ASSERT_TRUE(a.IsBulk());
    ASSERT_TRUE(b.IsBulk());
    EXPECT_EQ(a.String(), "1");
    EXPECT_EQ(b.String(), "2");
  } else {
    EXPECT_TRUE(a.IsNil() || (a.IsBulk() && a.String() != "1"));
    EXPECT_TRUE(b.IsNil() || (b.IsBulk() && b.String() != "2"));
  }
}

TEST_F(DurabilityTestFixture, ReadyEndpointReports200OnceRecoveryComplete) {
  // After a kill+restart with prior writes in the WAL, the recovery
  // coordinator must drive /ready to 200. The fixture's RestartServer
  // already waits on the readiness pipe, but we additionally probe the
  // HTTP endpoint to assert the public observable.
  for (int i = 0; i < 50; ++i) {
    ASSERT_TRUE(Client().Command({"SET", "k" + std::to_string(i), "v"}).IsOk());
  }
  KillAndRestartServer();

  const auto resp =
      abyss::testing::HttpTestClient::Send("127.0.0.1", Server().AdminPort(), "GET", "/ready");
  ASSERT_TRUE(resp.ok) << resp.error;
  EXPECT_EQ(resp.status, 200);
  EXPECT_NE(resp.body.find("recovery_complete: true"), std::string::npos);
}

TEST_F(DurabilityTestFixture, StatusEndpointReportsCompleteRecoveryPhase) {
  // After recovery completes, /status reflects phase=complete with no
  // stuck progress markers. Confirms the StatusProvider wiring through
  // the coordinator's snapshot.
  EXPECT_TRUE(Client().Command({"SET", "k", "v"}).IsOk());
  KillAndRestartServer();

  const auto resp =
      abyss::testing::HttpTestClient::Send("127.0.0.1", Server().AdminPort(), "GET", "/status");
  ASSERT_TRUE(resp.ok) << resp.error;
  EXPECT_EQ(resp.status, 200);
  EXPECT_NE(resp.body.find("\"phase\":\"complete\""), std::string::npos)
      << "recovery phase should be complete, body: " << resp.body;
}

}  // namespace
}  // namespace abyss::system_test
