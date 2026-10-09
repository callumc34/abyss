// One test per bug the property and crash tests found, each reduced
// from the failing seed to the interleaving that matters.

#include <gtest/gtest.h>

#ifdef ABYSS_HAVE_ROCKSDB

#include <atomic>
#include <chrono>
#include <cstdint>
#include <string>
#include <vector>

#include "abyss/core/types.h"
#include "sequenced_engine_fixture.h"

namespace abyss::engine {
namespace {

class PropertyRegressionTest : public SequencedEngineTest {
 protected:
  void OpenWithClock(Options options = {}) {
    now_ms_ = std::chrono::duration_cast<std::chrono::milliseconds>(
                  core::WallClock::now().time_since_epoch())
                  .count();
    wall_ = [this] { return core::WallTime(std::chrono::milliseconds(now_ms_.load())); };
    hot_clocks_ = Clocks{.wall = wall_};
    Open(std::move(options));
  }
  void Advance(std::chrono::milliseconds by) { now_ms_ += by.count(); }
  // Drains `key` into cold and evicts it, leaving at most a stub.
  void ToCold(const std::string& key) {
    DrainToCold(ShardOf(key));
    EvictDrained();
    ASSERT_FALSE(Resident(key));
  }

  // NOLINTNEXTLINE(cppcoreguidelines-non-private-member-variables-in-classes)
  std::atomic<int64_t> now_ms_{0};
};

// A member or count read of a key cold holds as another type,
// past its TTL, replied WRONGTYPE: the loader judged the type before
// the TTL. An expired key is absent to every read.
TEST_F(PropertyRegressionTest, AnExpiredKeyOfAnotherTypeIsAbsentToAMemberRead) {
  OpenWithClock();
  ASSERT_EQ(Write({"SET", "k", "v", "PX", "100"}), "OK");
  ASSERT_EQ(Write({"SADD", "s", "a"}), ":1");
  ASSERT_EQ(Write({"PEXPIRE", "s", "100"}), ":1");
  ToCold("k");
  ToCold("s");
  Advance(std::chrono::milliseconds(150));

  EXPECT_EQ(Read({"HLEN", "k"}), ":0");
  EXPECT_EQ(Read({"SCARD", "k"}), ":0");
  EXPECT_EQ(Read({"ZCARD", "k"}), ":0");
  EXPECT_EQ(Read({"SISMEMBER", "k", "a"}), ":0");
  EXPECT_EQ(Read({"HGET", "k", "f"}), "nil");
  EXPECT_EQ(Read({"ZSCORE", "k", "m"}), "nil");
  EXPECT_EQ(Read({"HLEN", "s"}), ":0");
  EXPECT_EQ(Read({"HEXISTS", "s", "a"}), ":0");
}

// A string past its TTL that cold had already deleted left
// nothing for a later ZADD's load to find, so the ZADD was decided on
// an absent key with no DEL logged. Replay then applied it over the
// expired string and failed fatally. An effect applies with expiry
// judged at its own instant, as it was decided.
TEST_F(PropertyRegressionTest, ReplayAppliesAnEffectOverAnExpiryNoDecisionLogged) {
  OpenWithClock();
  ASSERT_EQ(Write({"SET", "k", "v", "PX", "5"}), "OK");
  Advance(std::chrono::milliseconds(10));
  // Cold's clock passes the TTL, so cold drops the string.
  ASSERT_EQ(Write({"SET", KeyOn(ShardOf("k"), 0, "other"), "x"}), "OK");
  ToCold("k");
  // Cold's TTL scanner deletes it, by cold's clock.
  ASSERT_TRUE(cold_->RunScannerTickForTesting().has_value());
  ASSERT_EQ(Write({"ZADD", "k", "1", "m"}), ":1");
  for (const auto& entry : Logged(ShardOf("k"))) {
    ASSERT_NE(ArgsOf(entry).front(), "DEL") << "an expiry was logged: the case needs none";
  }

  ASSERT_TRUE(Restart(Clocks{.wall = wall_}).has_value());
  EXPECT_EQ(Read({"ZSCORE", "k", "m"}), "1");
  EXPECT_EQ(Read({"TYPE", "k"}), "zset");
}

// A write met -OOM ("cold is behind") while cold had
// drained every key holding the shard's memory. Keys parked while
// undrained (due, but held for cold) were off the LRU lists, so the
// memory walk could not see them once cold drained them; only the
// next deadline pass, a worker tick away, released them.
TEST_F(PropertyRegressionTest, MemoryPressureEvictsParkedKeysColdHasDrained) {
  Options options;
  options.shards = 1;
  options.hot_memory_bytes = 4096;
  options.write_timeout = std::chrono::milliseconds(300);
  OpenWithClock(options);
  const std::string big(2000, 'x');
  ASSERT_EQ(Write({"SET", "k1", big}), "OK");
  ASSERT_EQ(Write({"SET", "k2", big}), "OK");
  // Due, but undrained: parked.
  EvictDrained();
  ASSERT_TRUE(Resident("k1"));
  ASSERT_TRUE(Resident("k2"));
  pool_->ConsumerFor(0).Drain();
  ASSERT_EQ(hot_->Drained(0), 2U);

  EXPECT_EQ(Write({"SET", "k3", big}), "OK");
  EXPECT_EQ(Write({"SET", "k4", big}), "OK");
  EXPECT_FALSE(Resident("k1"));
  EXPECT_EQ(Read({"GET", "k1"}), big);
}

// A write met -OOM with every key drained, the
// shard's memory held by entries no memory walk frees: keys loaded as
// absent (the negative cache) and drained tombstones awaiting GC.
TEST_F(PropertyRegressionTest, MemoryPressureFreesNegativesAndDrainedTombstones) {
  Options options;
  options.shards = 1;
  options.hot_memory_bytes = 4096;
  options.fill_doorkeeper = false;
  options.write_timeout = std::chrono::milliseconds(300);
  OpenWithClock(options);
  for (int i = 0; i < 80; ++i) ASSERT_EQ(Read({"GET", "absent" + std::to_string(i)}), "nil");
  for (int i = 0; i < 20; ++i) ASSERT_EQ(Write({"SET", "d" + std::to_string(i), "v"}), "OK");
  for (int i = 0; i < 20; ++i) ASSERT_EQ(Write({"DEL", "d" + std::to_string(i)}), ":1");
  // Before the fix this left the shard over its limit, all drained.
  pool_->ConsumerFor(0).Drain();
  EXPECT_EQ(Write({"SET", "k", "v"}), "OK");
  EXPECT_LE(hot_->Memory(0).used_bytes, hot_->Memory(0).limit_bytes);
}

// A write that needs a key's state met -OOM, every key drained, when
// that key's entry alone held its shard over the limit: the wait for
// memory evicted it, and the reload put the shard over again until
// the deadline. Only memory cold has not drained refuses a write.
TEST_F(PropertyRegressionTest, AWriteNeedingAnEntryOverItsShardsLimitIsNotRefused) {
  Options options;
  options.shards = 2;
  options.hot_memory_bytes = 8192;
  // An evicted key leaves no stub, so a TTL read loads the value.
  options.stub_memory_fraction = 0;
  options.write_timeout = std::chrono::milliseconds(300);
  OpenWithClock(options);
  const std::string big(6000, 'x');
  const std::string kept = KeyOn(1, 0, "k");
  const std::string from = KeyOn(0, 0, "k");
  const std::string to = KeyOn(1, 1, "k");
  ASSERT_EQ(Write({"SET", kept, big}), "OK");
  ASSERT_EQ(Write({"SET", from, big}), "OK");
  DrainToCold(0);
  DrainToCold(1);
  ASSERT_GT(hot_->Memory(0).used_bytes, hot_->Memory(0).limit_bytes);
  ASSERT_GT(hot_->Memory(1).used_bytes, hot_->Memory(1).limit_bytes);

  // Shrinks the entry: its old value is the reply.
  EXPECT_EQ(Write({"SET", kept, "v", "GET"}, Flags::kGet).size(), big.size());
  // Deletes it from shard 0, creating it on shard 1.
  EXPECT_EQ(Write({"RENAMENX", from, to}, Flags::kNx), ":1");
  EXPECT_EQ(Read({"GET", to}).size(), big.size());
  // Needs only the TTL, but a string's TTL load brings its value.
  DrainToCold(1);
  ASSERT_GT(hot_->Memory(1).used_bytes, hot_->Memory(1).limit_bytes);
  EXPECT_EQ(Write({"SET", to, "v", "KEEPTTL"}, Flags::kKeepTtl), "OK");
}

// A SET whose EXAT or PXAT is already past deletes the
// key and logs a DEL, as EXPIRE does, not a value already expired.
TEST_F(PropertyRegressionTest, ASetWithATtlAlreadyPastLogsADel) {
  OpenWithClock();
  const std::string now = std::to_string(now_ms_.load());
  ASSERT_EQ(Write({"SET", "k", "a"}), "OK");
  EXPECT_EQ(Write({"SET", "k", "b", "PXAT", now, "GET"}, Flags::kGet), "a");
  EXPECT_EQ(Write({"SET", "j", "b", "PXAT", now}), "OK");
  EXPECT_EQ(Write({"SET", "i", "b", "EXAT", "1"}), "OK");
  EXPECT_EQ(Write({"SET", "h", "b", "PXAT", now, "XX"}, Flags::kXx), "nil");
  for (const std::string key : {"k", "j", "i"}) {
    const auto logged = Logged(ShardOf(key));
    std::vector<std::string> last;
    for (const auto& entry : logged) {
      const auto args = ArgsOf(entry);
      if (args.size() > 1 && args[1] == key) last = args;
    }
    EXPECT_EQ(last, (std::vector<std::string>{"DEL", key})) << key;
    EXPECT_EQ(Read({"GET", key}), "nil") << key;
  }
  for (const auto& entry : Logged(ShardOf("h"))) {
    const auto args = ArgsOf(entry);
    EXPECT_FALSE(args.size() > 1 && args[1] == "h") << "a refused SET XX logged";
  }
  ASSERT_TRUE(Restart(Clocks{.wall = wall_}).has_value());
  EXPECT_EQ(Read({"GET", "k"}), "nil");
}

}  // namespace
}  // namespace abyss::engine

#endif  // ABYSS_HAVE_ROCKSDB
