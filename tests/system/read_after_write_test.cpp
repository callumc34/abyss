#include <string>

#include "server_fixture.h"

namespace abyss::system_test {
namespace {

// Property: once a client receives +OK for a write, an immediately subsequent
// read returns that write's value. Never stale. The write handler blocks on
// both the queue fsync AND the hot consumer's apply before acking (ADP-006
// §Write Path, invariant 4) — apply happens-before the ack, so any read issued
// after OK observes the value. A single SET→GET almost always passes even if
// that ordering were broken, because the window is tiny; these tests stress the
// invariant where a regression would actually surface.
//
// Per-test isolated server (the durability-test pattern for write-heavy
// acceptance tests): a fresh empty server per test gives true isolation and
// keeps the shared fixture's FLUSHDB cleanup, which is orthogonal to
// read-after-write, out of these tests.
using ReadAfterWriteFixture = IsolatedDataServerTest;

TEST_F(ReadAfterWriteFixture, OverwriteSameKeyNeverStale) {
  // Rapidly overwrite one key and read it back each time. If the ack ever
  // preceded the apply being visible, GET would occasionally return the prior
  // value (v_{i-1}) instead of the one just acked.
  constexpr int kIterations = 2000;

  for (int i = 0; i < kIterations; ++i) {
    const std::string value = "v" + std::to_string(i);
    ASSERT_TRUE(Client().Command({"SET", "k", value}).IsOk()) << "SET refused at i=" << i;
    const auto r = Client().Command({"GET", "k"});
    ASSERT_TRUE(r.IsBulk()) << "GET k at i=" << i << " returned " << r;
    ASSERT_EQ(r.String(), value) << "stale read at i=" << i;
  }
}

TEST_F(ReadAfterWriteFixture, ReadAfterWriteAcrossManyKeysAndShards) {
  // Distinct keys spread across every shard (and thus every per-shard hot
  // consumer thread). Proves the apply→ack ordering holds on all shards, not
  // just the one shard 0 happens to land on.
  constexpr int kKeys = 1000;

  for (int i = 0; i < kKeys; ++i) {
    const std::string key = "raw_" + std::to_string(i);
    const std::string value = "val_" + std::to_string(i);
    ASSERT_TRUE(Client().Command({"SET", key, value}).IsOk()) << "SET refused at i=" << i;
    const auto r = Client().Command({"GET", key});
    ASSERT_TRUE(r.IsBulk()) << "GET " << key << " returned " << r;
    ASSERT_EQ(r.String(), value) << "stale read for " << key;
  }
}

TEST_F(ReadAfterWriteFixture, SecondConnectionSeesAcknowledgedWrite) {
  // Read-after-write is global, not per-connection: once the writer's SET is
  // acked, a different connection must observe the value too. Guards against a
  // future per-connection read cache regressing the guarantee.
  RedisClient reader;
  ASSERT_TRUE(reader.Connect("127.0.0.1", Server().Port()));

  ASSERT_TRUE(Client().Command({"SET", "shared_key", "shared_value"}).IsOk());

  const auto r = reader.Command({"GET", "shared_key"});
  ASSERT_TRUE(r.IsBulk()) << "second connection GET returned " << r;
  EXPECT_EQ(r.String(), "shared_value");
}

}  // namespace
}  // namespace abyss::system_test
