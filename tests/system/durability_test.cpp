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

TEST_F(DurabilityTestFixture, AcknowledgedWritesSurviveSigkill) {
  // Property: any write the client received +OK for must survive SIGKILL.
  // The pre-kill loop asserts every ack so the post-recovery check is
  // unambiguous — if a key is missing or wrong-valued, it's a durability
  // regression, not a precondition failure.
  constexpr int kKeys = 1000;

  for (int i = 0; i < kKeys; ++i) {
    const std::string key = "acked_" + std::to_string(i);
    const std::string value = "value_for_" + std::to_string(i);
    ASSERT_TRUE(Client().Command({"SET", key, value}).IsOk()) << "pre-kill SET refused at i=" << i;
  }

  KillAndRestartServer();

  int recovered = 0;
  int missing = 0;
  int mismatched = 0;
  std::vector<std::string> first_failures;
  for (int i = 0; i < kKeys; ++i) {
    const std::string key = "acked_" + std::to_string(i);
    const std::string expected = "value_for_" + std::to_string(i);
    const auto r = Client().Command({"GET", key});
    if (r.IsBulk() && r.String() == expected) {
      ++recovered;
    } else if (r.IsNil()) {
      ++missing;
      if (first_failures.size() < 5) first_failures.push_back(key + ":missing");
    } else {
      ++mismatched;
      if (first_failures.size() < 5) {
        first_failures.push_back(key + ":got=" + r.String());
      }
    }
  }

  std::string sample;
  for (const auto& f : first_failures) {
    if (!sample.empty()) sample += ", ";
    sample += f;
  }
  EXPECT_EQ(recovered, kKeys) << "missing=" << missing << " mismatched=" << mismatched << " first=["
                              << sample << "]";
}

TEST_F(DurabilityTestFixture, PipelinedAcksAllSurviveSigkill) {
  // Group commit batches concurrent in-flight writes into a single fsync;
  // a regression in batch boundary handling (ADP-009 §1.1 batch_last_seq)
  // could lose the closing entry of a batch while preserving prior ones.
  // The sequential test cannot surface that — this one ships every write
  // into the same window.
  constexpr int kKeys = 1000;

  std::vector<std::vector<std::string>> writes;
  writes.reserve(kKeys);
  for (int i = 0; i < kKeys; ++i) {
    writes.push_back({"SET", "pipe_" + std::to_string(i), "pv_" + std::to_string(i)});
  }

  const auto replies = Client().Pipeline(writes);
  ASSERT_EQ(replies.size(), static_cast<size_t>(kKeys));
  for (int i = 0; i < kKeys; ++i) {
    ASSERT_TRUE(replies[i].IsOk()) << "pipelined SET refused at i=" << i << " reply=" << replies[i];
  }

  KillAndRestartServer();

  int recovered = 0;
  std::vector<std::string> first_failures;
  for (int i = 0; i < kKeys; ++i) {
    const std::string key = "pipe_" + std::to_string(i);
    const std::string expected = "pv_" + std::to_string(i);
    const auto r = Client().Command({"GET", key});
    if (r.IsBulk() && r.String() == expected) {
      ++recovered;
    } else if (first_failures.size() < 5) {
      first_failures.push_back(key + (r.IsNil() ? ":missing" : ":got=" + r.String()));
    }
  }

  std::string sample;
  for (const auto& f : first_failures) {
    if (!sample.empty()) sample += ", ";
    sample += f;
  }
  EXPECT_EQ(recovered, kKeys) << "first=[" << sample << "]";
}

TEST_F(DurabilityTestFixture, CollectionWritesSurviveSigkill) {
  // Closes set + zset + collection-tombstone (DEL) durability. Strings and
  // hashes are covered by AcknowledgedWritesSurviveSigkill and
  // HmsetSurvivesCrashAndRestart.

  ASSERT_TRUE(Client().Command({"SADD", "set_a", "a", "b", "c"}).IsInteger());
  ASSERT_TRUE(Client().Command({"SADD", "set_a", "d"}).IsInteger());
  ASSERT_TRUE(Client().Command({"SREM", "set_a", "b"}).IsInteger());

  ASSERT_TRUE(Client().Command({"ZADD", "zset_a", "1", "x", "2", "y", "3", "z"}).IsInteger());
  ASSERT_TRUE(Client().Command({"ZADD", "zset_a", "5", "x"}).IsInteger());  // re-score
  ASSERT_TRUE(Client().Command({"ZREM", "zset_a", "y"}).IsInteger());

  ASSERT_TRUE(Client().Command({"SADD", "doomed_set", "ghost"}).IsInteger());
  ASSERT_EQ(Client().Command({"DEL", "doomed_set"}).Integer(), 1);

  KillAndRestartServer();

  EXPECT_EQ(Client().Command({"SCARD", "set_a"}).Integer(), 3);
  EXPECT_EQ(Client().Command({"SISMEMBER", "set_a", "a"}).Integer(), 1);
  EXPECT_EQ(Client().Command({"SISMEMBER", "set_a", "b"}).Integer(), 0)
      << "SREM must propagate through replay";
  EXPECT_EQ(Client().Command({"SISMEMBER", "set_a", "c"}).Integer(), 1);
  EXPECT_EQ(Client().Command({"SISMEMBER", "set_a", "d"}).Integer(), 1);

  EXPECT_EQ(Client().Command({"ZCARD", "zset_a"}).Integer(), 2);
  // ZSCORE+ZCARD are the only zset reads core::ops::ParseReadOp wires today
  // (ZRANGE et al. are registered at the RESP layer but the engine rejects
  // them — separate gap). std::stod absorbs whatever double format is emitted.
  const auto x_score = Client().Command({"ZSCORE", "zset_a", "x"});
  ASSERT_TRUE(x_score.IsBulk()) << "ZSCORE x: " << x_score;
  EXPECT_DOUBLE_EQ(std::stod(x_score.String()), 5.0);
  const auto z_score = Client().Command({"ZSCORE", "zset_a", "z"});
  ASSERT_TRUE(z_score.IsBulk()) << "ZSCORE z: " << z_score;
  EXPECT_DOUBLE_EQ(std::stod(z_score.String()), 3.0);
  EXPECT_TRUE(Client().Command({"ZSCORE", "zset_a", "y"}).IsNil())
      << "ZREM must propagate through replay";

  EXPECT_EQ(Client().Command({"EXISTS", "doomed_set"}).Integer(), 0)
      << "collection DEL must propagate through replay";
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

TEST_F(DurabilityTestFixture, HmsetSurvivesCrashAndRestart) {
  // HMSET is deprecated upstream but widely used; its persistence path must
  // match multi-field HSET. After a kill the recovered hash should match what
  // the client observed pre-crash.
  EXPECT_TRUE(Client().Command({"HMSET", "h", "a", "1", "b", "2", "c", "3"}).IsOk());
  KillAndRestartServer();

  auto a = Client().Command({"HGET", "h", "a"});
  ASSERT_TRUE(a.IsBulk());
  EXPECT_EQ(a.String(), "1");
  EXPECT_EQ(Client().Command({"HGET", "h", "b"}).String(), "2");
  EXPECT_EQ(Client().Command({"HGET", "h", "c"}).String(), "3");
  EXPECT_EQ(Client().Command({"HLEN", "h"}).Integer(), 3);
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

TEST_F(DurabilityTestFixture, FlushdbSurvivesKill) {
  for (int i = 0; i < 50; ++i) {
    ASSERT_TRUE(Client().Command({"SET", "pre_" + std::to_string(i), "x"}).IsOk());
  }
  ASSERT_TRUE(Client().Command({"FLUSHDB"}).IsStatus());
  ASSERT_TRUE(Client().Command({"SET", "post", "y"}).IsOk());

  KillAndRestartServer();

  for (int i = 0; i < 50; ++i) {
    auto r = Client().Command({"GET", "pre_" + std::to_string(i)});
    EXPECT_TRUE(r.IsNil()) << "pre-FLUSHDB key " << i << " should be gone after replay";
  }
  auto post = Client().Command({"GET", "post"});
  ASSERT_TRUE(post.IsBulk());
  EXPECT_EQ(post.String(), "y") << "post-FLUSHDB write should survive replay";
}

}  // namespace
}  // namespace abyss::system_test
