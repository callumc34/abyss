#include "abyss/engine/tiering_engine.h"

#include <gtest/gtest.h>

#include <chrono>
#include <future>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "abyss/consumer/compaction_buffer.h"
#include "abyss/consumer/compaction_buffer_router.h"
#include "abyss/core/ops.h"
#include "abyss/core/shard_router.h"
#include "abyss/engine/loader.h"
#include "abyss/engine/sequencer.h"
#include "abyss/hot/sharded_hot_store.h"
#include "mock_cold_store.h"
#include "mock_queue.h"
#include "on_exit.h"

namespace abyss::engine {
namespace {

using ::testing::_;
using ::testing::Return;
using namespace std::chrono_literals;

class SingleBufferRouter : public consumer::CompactionBufferRouter {
 public:
  explicit SingleBufferRouter(consumer::CompactionBuffer& buffer) : buffer_(buffer) {}
  core::Result<core::RespValue> Exec(const core::ops::ReadOp& op,
                                     std::optional<core::Duration> /*deadline*/) override {
    return buffer_.Exec(op);
  }
  core::Result<core::RespValue> Read(std::string_view key) const override {
    return buffer_.Read(std::string(key));
  }
  consumer::BufferKeyPresence Probe(std::string_view key) const override {
    return buffer_.Probe(key);
  }
  consumer::HashOverlay HashOverlayFor(std::string_view key) const override {
    return buffer_.HashOverlayFor(key);
  }
  std::optional<consumer::CompactedState> Snapshot(core::ShardId /*shard*/,
                                                   std::string_view key) const override {
    return buffer_.Snapshot(key);
  }
  // Component-level engine tests run without a cold consumer thread, so the
  // wait is a structural no-op: there is no producer to advance the seq.
  // Returning true models "already caught up", which is the steady-state
  // expectation outside the slow-cold race we cover in integration tests.
  bool WaitForDrainedSeq(core::ShardId /*shard*/, core::SequenceId /*target_seq*/,
                         std::chrono::milliseconds /*timeout*/) override {
    return true;
  }

 private:
  consumer::CompactionBuffer& buffer_;
};

// A real hot store, buffer and loader over a mock cold store and an
// in-memory queue: what the engine dispatches, end to end.
class TieringEngineTest : public ::testing::Test {
 protected:
  static constexpr uint32_t kShardCount = 16;
  static constexpr std::chrono::milliseconds kWriteTimeout = 1s;

  // NOLINTBEGIN(cppcoreguidelines-non-private-member-variables-in-classes)
  ::testing::NiceMock<testing::MockQueue> queue_;
  hot::ShardedHotStore hot_{hot::ShardedHotStoreConfig{
      .max_memory_bytes = 64UL << 20,
      .shard_count = kShardCount,
  }};
  ::testing::NiceMock<testing::MockColdStore> cold_;
  consumer::CompactionBuffer buffer_;
  SingleBufferRouter router_{buffer_};
  Loader loader_{hot_, router_, cold_};
  Sequencer sequencer_{hot_, queue_, loader_, router_,
                       SequencerConfig{.write_timeout = kWriteTimeout}};
  // NOLINTEND(cppcoreguidelines-non-private-member-variables-in-classes)

  TieringEngine MakeEngine() {
    return {hot_, cold_, router_, sequencer_,
            TieringEngineConfig{.shard_count = kShardCount, .write_timeout = kWriteTimeout}};
  }

  core::RespCommand MakeCmd(std::initializer_list<std::string> args) {
    return core::RespCommand{.args = std::vector<std::string>(args)};
  }

  // Writes through the engine, so hot holds the key as decided.
  void Seed(TieringEngine& engine, std::initializer_list<std::string> args) {
    auto written = engine.DispatchWrite(*args.begin(), MakeCmd(args));
    ASSERT_TRUE(written.has_value()) << written.error().message();
    ASSERT_FALSE(written->IsError()) << written->AsString();
  }

  core::ShardId ShardOf(std::string_view key) const { return core::ComputeShard(key, kShardCount); }
};

// --- Read path ---

TEST_F(TieringEngineTest, ReadHotHitReturnsValue) {
  auto engine = MakeEngine();
  ASSERT_NO_FATAL_FAILURE(Seed(engine, {"SET", "key", "value"}));
  EXPECT_CALL(cold_, Exec(_, _)).Times(0);

  auto result = engine.DispatchRead("GET", MakeCmd({"GET", "key"}));
  ASSERT_TRUE(result.has_value());
  EXPECT_TRUE(result->IsBulkString());
  EXPECT_EQ(result->AsString(), "value");
}

TEST_F(TieringEngineTest, ReadHotMissBufferHitReturnsBufferValue) {
  auto engine = MakeEngine();

  buffer_.Absorb("key", core::ops::WriteOp{core::ops::StringSet{.key = "key", .value = "buffered"}},
                 core::EvictionTTL{86400}, 0, 0, 0);

  auto result = engine.DispatchRead("GET", MakeCmd({"GET", "key"}));
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->AsString(), "buffered");
}

TEST_F(TieringEngineTest, ReadHotMissBufferMissColdHitReturnsColdValue) {
  auto engine = MakeEngine();
  auto cold_value = core::RespValue::BulkString("cold_value");

  EXPECT_CALL(cold_, Exec(_, _)).WillOnce(Return(cold_value));

  auto result = engine.DispatchRead("GET", MakeCmd({"GET", "key"}));
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->AsString(), "cold_value");
}

TEST_F(TieringEngineTest, ReadAllTiersMissReturnsColdError) {
  auto engine = MakeEngine();

  EXPECT_CALL(cold_, Exec(_, _))
      .WillOnce(Return(std::unexpected(core::Error(core::ErrorCode::kNotFound, ""))));

  auto result = engine.DispatchRead("GET", MakeCmd({"GET", "missing"}));
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code(), core::ErrorCode::kNotFound);
}

TEST_F(TieringEngineTest, ReadHotErrorPropagates) {
  auto engine = MakeEngine();
  ASSERT_NO_FATAL_FAILURE(Seed(engine, {"SADD", "key", "m"}));

  auto result = engine.DispatchRead("GET", MakeCmd({"GET", "key"}));
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code(), core::ErrorCode::kWrongType);
}

TEST_F(TieringEngineTest, ReadBufferTombstoneReturnsNull) {
  auto engine = MakeEngine();

  buffer_.Absorb("key", core::ops::WriteOp{core::ops::StringSet{.key = "key", .value = "v"}},
                 core::EvictionTTL{86400}, 0, 0, 0);
  buffer_.Absorb("key", core::ops::WriteOp{core::ops::Del{.keys = {"key"}}},
                 core::EvictionTTL{86400}, 0, 0, 0);

  auto result = engine.DispatchRead("GET", MakeCmd({"GET", "key"}));
  ASSERT_TRUE(result.has_value());
  EXPECT_TRUE(result->IsNull());
}

// --- Hash multi-field read merge ---

namespace {

core::RespValue MakeHashGetAllResponse(
    const std::vector<std::pair<std::string, std::string>>& pairs) {
  std::vector<core::RespValue> elems;
  elems.reserve(pairs.size() * 2);
  for (const auto& [k, v] : pairs) {
    elems.push_back(core::RespValue::BulkString(k));
    elems.push_back(core::RespValue::BulkString(v));
  }
  return core::RespValue::Array(std::move(elems));
}

std::unordered_map<std::string, std::string> ArrayPairsToMap(const core::RespValue& v) {
  std::unordered_map<std::string, std::string> m;
  const auto& a = v.AsArray();
  for (size_t i = 0; i + 1 < a.size(); i += 2) {
    m.emplace(a[i].AsString(), a[i + 1].AsString());
  }
  return m;
}

}  // namespace

TEST_F(TieringEngineTest, HashGetAllMergesColdAndBufferOverlay) {
  auto engine = MakeEngine();
  // Buffer overlay: adds 'b' (new field) and 'a' (overrides cold's value);
  // removes 'c' (HDEL after HSET) — should disappear from the merged result.
  buffer_.Absorb(
      "h",
      core::ops::WriteOp{core::ops::HashSet{
          .key = "h", .fields = {{.field = "a", .value = "buf"}, {.field = "b", .value = "new"}}}},
      core::EvictionTTL{86400}, 0, 0, 0);
  buffer_.Absorb("h", core::ops::WriteOp{core::ops::HashDel{.key = "h", .fields = {"c"}}},
                 core::EvictionTTL{86400}, 0, 0, 0);

  // Cold has prior fields including 'c' (which the buffer has removed).
  EXPECT_CALL(cold_, Exec(_, _))
      .WillOnce(Return(MakeHashGetAllResponse({{"a", "cold"}, {"c", "stale"}, {"d", "kept"}})));

  auto result = engine.DispatchRead("HGETALL", MakeCmd({"HGETALL", "h"}));
  ASSERT_TRUE(result.has_value());
  ASSERT_TRUE(result->IsArray());
  const auto merged = ArrayPairsToMap(*result);
  EXPECT_EQ(merged.size(), 3U);
  EXPECT_EQ(merged.at("a"), "buf");    // buffer wins over cold
  EXPECT_EQ(merged.at("b"), "new");    // buffer-only
  EXPECT_EQ(merged.at("d"), "kept");   // cold-only, untouched
  EXPECT_FALSE(merged.contains("c"));  // removed in buffer
}

TEST_F(TieringEngineTest, HashGetAllTombstoneShortCircuitsCold) {
  auto engine = MakeEngine();
  buffer_.Absorb(
      "h",
      core::ops::WriteOp{core::ops::HashSet{.key = "h", .fields = {{.field = "a", .value = "v"}}}},
      core::EvictionTTL{86400}, 0, 0, 0);
  buffer_.Absorb("h", core::ops::WriteOp{core::ops::Del{.keys = {"h"}}}, core::EvictionTTL{86400},
                 0, 0, 0);

  // Tombstone must not consult cold.
  EXPECT_CALL(cold_, Exec(_, _)).Times(0);

  auto result = engine.DispatchRead("HGETALL", MakeCmd({"HGETALL", "h"}));
  ASSERT_TRUE(result.has_value());
  ASSERT_TRUE(result->IsArray());
  EXPECT_TRUE(result->AsArray().empty());
}

TEST_F(TieringEngineTest, HashGetAllWrongTypeOverlayReturnsError) {
  auto engine = MakeEngine();
  // Buffer was re-typed to a string after a prior hash.
  buffer_.Absorb("k", core::ops::WriteOp{core::ops::StringSet{.key = "k", .value = "now_a_string"}},
                 core::EvictionTTL{86400}, 0, 0, 0);

  EXPECT_CALL(cold_, Exec(_, _)).Times(0);

  auto result = engine.DispatchRead("HGETALL", MakeCmd({"HGETALL", "k"}));
  // WRONGTYPE is encoded as a kError RespValue (not a Result error).
  ASSERT_TRUE(result.has_value());
  EXPECT_TRUE(result->IsError());
}

TEST_F(TieringEngineTest, HashGetAllNotPresentDelegatesToCold) {
  auto engine = MakeEngine();
  EXPECT_CALL(cold_, Exec(_, _)).WillOnce(Return(MakeHashGetAllResponse({{"x", "1"}, {"y", "2"}})));

  auto result = engine.DispatchRead("HGETALL", MakeCmd({"HGETALL", "h"}));
  ASSERT_TRUE(result.has_value());
  const auto map = ArrayPairsToMap(*result);
  EXPECT_EQ(map.size(), 2U);
  EXPECT_EQ(map.at("x"), "1");
  EXPECT_EQ(map.at("y"), "2");
}

TEST_F(TieringEngineTest, HashLenMergedCardinality) {
  auto engine = MakeEngine();
  buffer_.Absorb(
      "h",
      core::ops::WriteOp{core::ops::HashSet{
          .key = "h", .fields = {{.field = "a", .value = "buf"}, {.field = "b", .value = "new"}}}},
      core::EvictionTTL{86400}, 0, 0, 0);
  buffer_.Absorb("h", core::ops::WriteOp{core::ops::HashDel{.key = "h", .fields = {"c"}}},
                 core::EvictionTTL{86400}, 0, 0, 0);

  EXPECT_CALL(cold_, Exec(_, _))
      .WillOnce(Return(MakeHashGetAllResponse({{"a", "cold"}, {"c", "stale"}, {"d", "kept"}})));

  // 3 distinct fields after merge: a (buf), b (buf), d (cold).
  auto result = engine.DispatchRead("HLEN", MakeCmd({"HLEN", "h"}));
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->AsInteger(), 3);
}

TEST_F(TieringEngineTest, HashKeysAndValsProjectMergedSet) {
  auto engine = MakeEngine();
  buffer_.Absorb("h",
                 core::ops::WriteOp{core::ops::HashSet{
                     .key = "h", .fields = {{.field = "buf_only", .value = "x"}}}},
                 core::EvictionTTL{86400}, 0, 0, 0);

  EXPECT_CALL(cold_, Exec(_, _))
      .Times(2)
      .WillRepeatedly(Return(MakeHashGetAllResponse({{"cold_only", "y"}})));

  auto keys = engine.DispatchRead("HKEYS", MakeCmd({"HKEYS", "h"}));
  ASSERT_TRUE(keys.has_value());
  std::unordered_set<std::string> key_set;
  for (const auto& e : keys->AsArray()) key_set.insert(e.AsString());
  EXPECT_EQ(key_set, (std::unordered_set<std::string>{"buf_only", "cold_only"}));

  auto vals = engine.DispatchRead("HVALS", MakeCmd({"HVALS", "h"}));
  ASSERT_TRUE(vals.has_value());
  std::unordered_set<std::string> val_set;
  for (const auto& e : vals->AsArray()) val_set.insert(e.AsString());
  EXPECT_EQ(val_set, (std::unordered_set<std::string>{"x", "y"}));
}

TEST_F(TieringEngineTest, HmgetMixedBufferAndColdFields) {
  auto engine = MakeEngine();
  buffer_.Absorb("h",
                 core::ops::WriteOp{core::ops::HashSet{
                     .key = "h", .fields = {{.field = "buf_known", .value = "from_buf"}}}},
                 core::EvictionTTL{86400}, 0, 0, 0);
  buffer_.Absorb("h", core::ops::WriteOp{core::ops::HashDel{.key = "h", .fields = {"buf_removed"}}},
                 core::EvictionTTL{86400}, 0, 0, 0);

  // Cold gets one per-field HGet for the unknown-from-buffer field.
  EXPECT_CALL(cold_, Exec(_, _)).WillOnce(Return(core::RespValue::BulkString("from_cold")));

  auto result = engine.DispatchRead(
      "HMGET", MakeCmd({"HMGET", "h", "buf_known", "buf_removed", "cold_only"}));
  ASSERT_TRUE(result.has_value());
  ASSERT_TRUE(result->IsArray());
  const auto& a = result->AsArray();
  ASSERT_EQ(a.size(), 3U);
  EXPECT_EQ(a[0].AsString(), "from_buf");
  EXPECT_TRUE(a[1].IsNull());
  EXPECT_EQ(a[2].AsString(), "from_cold");
}

TEST_F(TieringEngineTest, HexistsBufferKnownDoesNotCallCold) {
  auto engine = MakeEngine();
  buffer_.Absorb(
      "h",
      core::ops::WriteOp{core::ops::HashSet{.key = "h", .fields = {{.field = "f", .value = "v"}}}},
      core::EvictionTTL{86400}, 0, 0, 0);

  EXPECT_CALL(cold_, Exec(_, _)).Times(0);

  auto result = engine.DispatchRead("HEXISTS", MakeCmd({"HEXISTS", "h", "f"}));
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->AsInteger(), 1);
}

TEST_F(TieringEngineTest, HexistsBufferRemovedFieldReturnsZero) {
  auto engine = MakeEngine();
  buffer_.Absorb(
      "h",
      core::ops::WriteOp{core::ops::HashSet{.key = "h", .fields = {{.field = "f", .value = "v"}}}},
      core::EvictionTTL{86400}, 0, 0, 0);
  buffer_.Absorb("h", core::ops::WriteOp{core::ops::HashDel{.key = "h", .fields = {"f"}}},
                 core::EvictionTTL{86400}, 0, 0, 0);

  EXPECT_CALL(cold_, Exec(_, _)).Times(0);

  auto result = engine.DispatchRead("HEXISTS", MakeCmd({"HEXISTS", "h", "f"}));
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->AsInteger(), 0);
}

// --- ENGINE-2: uniform overlay for set/zset SCALAR reads --------------------

TEST_F(TieringEngineTest, ScardReadsBufferDeltaNotStaleCold) {
  auto engine = MakeEngine();
  // The set was flushed to cold with {a,b,c}; a partial SREM landed only in the
  // buffer. The buffer overlay must win: SCARD reflects 2, not cold's 3.
  buffer_.Absorb("s", core::ops::WriteOp{core::ops::SetAdd{.key = "s", .members = {"a", "b", "c"}}},
                 core::EvictionTTL{86400}, 0, 0, 0);
  buffer_.Absorb("s", core::ops::WriteOp{core::ops::SetRem{.key = "s", .members = {"b"}}},
                 core::EvictionTTL{86400}, 0, 0, 0);

  // Cold must NOT be consulted: the buffer answers authoritatively.
  EXPECT_CALL(cold_, Exec(_, _)).Times(0);

  auto result = engine.DispatchRead("SCARD", MakeCmd({"SCARD", "s"}));
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->AsInteger(), 2);
}

TEST_F(TieringEngineTest, SismemberReflectsBufferedRemoval) {
  auto engine = MakeEngine();
  buffer_.Absorb("s", core::ops::WriteOp{core::ops::SetAdd{.key = "s", .members = {"a", "b"}}},
                 core::EvictionTTL{86400}, 0, 0, 0);
  buffer_.Absorb("s", core::ops::WriteOp{core::ops::SetRem{.key = "s", .members = {"b"}}},
                 core::EvictionTTL{86400}, 0, 0, 0);

  EXPECT_CALL(cold_, Exec(_, _)).Times(0);

  auto present = engine.DispatchRead("SISMEMBER", MakeCmd({"SISMEMBER", "s", "a"}));
  ASSERT_TRUE(present.has_value());
  EXPECT_EQ(present->AsInteger(), 1);
  auto removed = engine.DispatchRead("SISMEMBER", MakeCmd({"SISMEMBER", "s", "b"}));
  ASSERT_TRUE(removed.has_value());
  EXPECT_EQ(removed->AsInteger(), 0);
}

TEST_F(TieringEngineTest, ZsetScalarBufferOverridesColdResidual) {
  auto engine = MakeEngine();
  // ZADD then ZREM only in the buffer: ZSCORE is nil and ZCARD is 0.
  buffer_.Absorb("z",
                 core::ops::WriteOp{
                     core::ops::ZsetAdd{.key = "z", .entries = {{.score = 1.0, .member = "m"}}}},
                 core::EvictionTTL{86400}, 0, 0, 0);
  buffer_.Absorb("z", core::ops::WriteOp{core::ops::ZsetRem{.key = "z", .members = {"m"}}},
                 core::EvictionTTL{86400}, 0, 0, 0);

  EXPECT_CALL(cold_, Exec(_, _)).Times(0);

  auto score = engine.DispatchRead("ZSCORE", MakeCmd({"ZSCORE", "z", "m"}));
  ASSERT_TRUE(score.has_value());
  EXPECT_TRUE(score->IsNull());
  auto card = engine.DispatchRead("ZCARD", MakeCmd({"ZCARD", "z"}));
  ASSERT_TRUE(card.has_value());
  EXPECT_EQ(card->AsInteger(), 0);
}

TEST_F(TieringEngineTest, CollectionScalarBufferMissFallsThroughToCold) {
  auto engine = MakeEngine();
  // No buffer entry for the key: each scalar shape falls through to cold.
  EXPECT_CALL(cold_, Exec(_, _))
      .WillOnce(Return(core::RespValue::Integer(3)))         // SCARD
      .WillOnce(Return(core::RespValue::Integer(1)))         // SISMEMBER
      .WillOnce(Return(core::RespValue::BulkString("2.5")))  // ZSCORE
      .WillOnce(Return(core::RespValue::Integer(5)));        // ZCARD

  EXPECT_EQ(engine.DispatchRead("SCARD", MakeCmd({"SCARD", "s"}))->AsInteger(), 3);
  EXPECT_EQ(engine.DispatchRead("SISMEMBER", MakeCmd({"SISMEMBER", "s", "x"}))->AsInteger(), 1);
  EXPECT_EQ(engine.DispatchRead("ZSCORE", MakeCmd({"ZSCORE", "z", "m"}))->AsString(), "2.5");
  EXPECT_EQ(engine.DispatchRead("ZCARD", MakeCmd({"ZCARD", "z"}))->AsInteger(), 5);
}

TEST_F(TieringEngineTest, CollectionScalarWrongTypeFromBufferShortCircuits) {
  auto engine = MakeEngine();
  // Buffer holds a string for "k"; SCARD must surface WRONGTYPE without cold.
  buffer_.Absorb("k", core::ops::WriteOp{core::ops::StringSet{.key = "k", .value = "v"}},
                 core::EvictionTTL{86400}, 0, 0, 0);

  EXPECT_CALL(cold_, Exec(_, _)).Times(0);

  auto result = engine.DispatchRead("SCARD", MakeCmd({"SCARD", "k"}));
  ASSERT_TRUE(result.has_value());
  EXPECT_TRUE(result->IsError());
}

// --- COLD-2 + ENGINE-2 coherence: cold scalar read carries the deadline -----

TEST_F(TieringEngineTest, CollectionScalarColdReadCarriesPointReadDeadline) {
  TieringEngineConfig cfg{.shard_count = kShardCount,
                          .write_timeout = kWriteTimeout,
                          .cold_read_deadline = 5ms,
                          .cold_scan_deadline = 50ms};
  TieringEngine engine(hot_, cold_, router_, sequencer_, cfg);

  // The scalar read routed to cold must carry the point-read deadline (COLD-2):
  // a deadline-bounded read, never an unbounded one.
  EXPECT_CALL(cold_, Exec(_, std::optional<core::Duration>(5ms)))
      .WillOnce(Return(core::RespValue::Integer(7)));

  auto result = engine.DispatchRead("SCARD", MakeCmd({"SCARD", "s"}));
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->AsInteger(), 7);
}

TEST_F(TieringEngineTest, CollectionScanColdReadCarriesScanDeadline) {
  TieringEngineConfig cfg{.shard_count = kShardCount,
                          .write_timeout = kWriteTimeout,
                          .cold_read_deadline = 5ms,
                          .cold_scan_deadline = 50ms};
  TieringEngine engine(hot_, cold_, router_, sequencer_, cfg);

  // A full-collection scan (SMEMBERS) gets the larger scan deadline.
  EXPECT_CALL(cold_, Exec(_, std::optional<core::Duration>(50ms)))
      .WillOnce(Return(core::RespValue::Array(
          {core::RespValue::BulkString("a"), core::RespValue::BulkString("b")})));

  auto result = engine.DispatchRead("SMEMBERS", MakeCmd({"SMEMBERS", "s"}));
  ASSERT_TRUE(result.has_value());
  ASSERT_TRUE(result->IsArray());
  EXPECT_EQ(result->AsArray().size(), 2U);
}

TEST_F(TieringEngineTest, ColdScanDeadlineExceededSurfacesErrorNotPartial) {
  TieringEngineConfig cfg{.shard_count = kShardCount,
                          .write_timeout = kWriteTimeout,
                          .cold_read_deadline = 5ms,
                          .cold_scan_deadline = 50ms};
  TieringEngine engine(hot_, cold_, router_, sequencer_, cfg);

  // Cold fails closed with a timeout on a large scan. The engine surfaces the
  // error to the client, never a silently truncated array (decision 4).
  EXPECT_CALL(cold_, Exec(_, std::optional<core::Duration>(50ms)))
      .WillOnce(Return(std::unexpected(core::Error(core::ErrorCode::kTimeout, "scan deadline"))));

  auto result = engine.DispatchRead("SMEMBERS", MakeCmd({"SMEMBERS", "big"}));
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code(), core::ErrorCode::kTimeout);
}

// --- Write path ---

TEST_F(TieringEngineTest, WriteGoesThroughTheSequencer) {
  auto engine = MakeEngine();
  auto result = engine.DispatchWrite("SET", MakeCmd({"SET", "key", "value"}));
  ASSERT_TRUE(result.has_value()) << result.error().message();
  EXPECT_EQ(result->AsString(), "OK");

  const auto published = queue_.Published(ShardOf("key"));
  ASSERT_EQ(published.size(), 1U);
  EXPECT_EQ(std::get<core::entry::Write>(published[0].payload).cmd.args,
            (std::vector<std::string>{"SET", "key", "value"}));
  EXPECT_CALL(cold_, Exec(_, _)).Times(0);
  auto read = engine.DispatchRead("GET", MakeCmd({"GET", "key"}));
  ASSERT_TRUE(read.has_value());
  EXPECT_EQ(read->AsString(), "value");
}

TEST_F(TieringEngineTest, WriteDecideErrorLogsNothing) {
  auto engine = MakeEngine();
  ASSERT_NO_FATAL_FAILURE(Seed(engine, {"SET", "key", "v"}));
  auto result = engine.DispatchWrite("SADD", MakeCmd({"SADD", "key", "m"}));
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code(), core::ErrorCode::kWrongType);
  EXPECT_EQ(queue_.Published(ShardOf("key")).size(), 1U);
}

TEST_F(TieringEngineTest, WriteReserveFailureAppliesNothing) {
  auto engine = MakeEngine();
  queue_.SetReserveFault([](std::span<const queue::ShardEntries>) -> std::optional<core::Error> {
    return core::Error{core::ErrorCode::kInternal, "pwrite: I/O error"};
  });
  auto result = engine.DispatchWrite("SET", MakeCmd({"SET", "key", "value"}));
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code(), core::ErrorCode::kInternal);
  EXPECT_TRUE(queue_.Published(ShardOf("key")).empty());
  auto read = hot_.Read(core::ops::ReadOp{core::ops::StringGet{.key = "key"}});
  ASSERT_FALSE(read.result.has_value());
  EXPECT_EQ(read.result.error().code(), core::ErrorCode::kNotFound);
}

TEST_F(TieringEngineTest, WriteDurableFailurePropagates) {
  auto engine = MakeEngine();
  queue_.FailDurable(core::Error{core::ErrorCode::kInternal, "fsync failed"});
  auto result = engine.DispatchWrite("SET", MakeCmd({"SET", "key", "value"}));
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code(), core::ErrorCode::kInternal);
}

TEST_F(TieringEngineTest, WriteDurableTimeoutRepliesWithTheTimeoutText) {
  Sequencer fast(hot_, queue_, loader_, router_, SequencerConfig{.write_timeout = 50ms});
  TieringEngine engine(hot_, cold_, router_, fast, TieringEngineConfig{.shard_count = kShardCount});
  queue_.HoldDurable();
  const testing::OnExit release([this] { queue_.ReleaseDurable(); });
  auto result = engine.DispatchWrite("SET", MakeCmd({"SET", "key", "value"}));
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code(), core::ErrorCode::kTimeout);
  EXPECT_TRUE(result.error().message().starts_with("write durable wait exceeded server timeout"))
      << result.error().message();
}

// --- Fan-out (MGET, EXISTS, MSET, DEL) ---
//
// Reads stay per key (#170); MSET and DEL are one decision and one
// reservation across their shards.

TEST_F(TieringEngineTest, MgetAggregatesAcrossTiersInPositionalOrder) {
  auto engine = MakeEngine();
  ASSERT_NO_FATAL_FAILURE(Seed(engine, {"SET", "a", "from_hot"}));
  buffer_.Absorb("b", core::ops::WriteOp{core::ops::StringSet{.key = "b", .value = "from_buf"}},
                 core::EvictionTTL{86400}, 0, 0, 0);

  // Only c and d fall through to cold (a hit hot, b hit buffer).
  EXPECT_CALL(cold_, Exec(_, _))
      .WillOnce(Return(core::RespValue::BulkString("from_cold")))                       // c
      .WillOnce(Return(std::unexpected(core::Error(core::ErrorCode::kNotFound, ""))));  // d

  auto result =
      engine.DispatchFanOut(core::MultiKeyKind::kMget, MakeCmd({"MGET", "a", "b", "c", "d"}));
  ASSERT_TRUE(result.has_value());
  ASSERT_TRUE(result->IsArray());
  const auto& arr = result->AsArray();
  ASSERT_EQ(arr.size(), 4U);
  EXPECT_EQ(arr[0].AsString(), "from_hot");
  EXPECT_EQ(arr[1].AsString(), "from_buf");
  EXPECT_EQ(arr[2].AsString(), "from_cold");
  EXPECT_TRUE(arr[3].IsNull());
}

TEST_F(TieringEngineTest, MgetWrongTypeInHotCollapsesToNil) {
  auto engine = MakeEngine();
  ASSERT_NO_FATAL_FAILURE(Seed(engine, {"SADD", "k", "m"}));

  auto result = engine.DispatchFanOut(core::MultiKeyKind::kMget, MakeCmd({"MGET", "k"}));
  ASSERT_TRUE(result.has_value());
  ASSERT_TRUE(result->IsArray());
  ASSERT_EQ(result->AsArray().size(), 1U);
  EXPECT_TRUE(result->AsArray()[0].IsNull());
}

TEST_F(TieringEngineTest, ExistsTombstoneInBufferOverridesColdResidual) {
  auto engine = MakeEngine();
  // Buffer holds a SET then a DEL — tombstone state for "k".
  buffer_.Absorb("k", core::ops::WriteOp{core::ops::StringSet{.key = "k", .value = "v"}},
                 core::EvictionTTL{86400}, 0, 0, 0);
  buffer_.Absorb("k", core::ops::WriteOp{core::ops::Del{.keys = {"k"}}}, core::EvictionTTL{86400},
                 0, 0, 0);

  // Hot has no record; cold is never consulted because the buffer probe is
  // authoritative on the tombstone.
  EXPECT_CALL(cold_, Exec(_, _)).Times(0);

  auto result = engine.DispatchFanOut(core::MultiKeyKind::kExists, MakeCmd({"EXISTS", "k"}));
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->AsInteger(), 0);
}

TEST_F(TieringEngineTest, ExistsCountsDuplicatesRedisStyle) {
  auto engine = MakeEngine();
  ASSERT_NO_FATAL_FAILURE(Seed(engine, {"SET", "k", "v"}));

  auto result = engine.DispatchFanOut(core::MultiKeyKind::kExists, MakeCmd({"EXISTS", "k", "k"}));
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->AsInteger(), 2);
}

TEST_F(TieringEngineTest, MsetReservesOneBatchAcrossItsShards) {
  auto engine = MakeEngine();
  int reserves = 0;
  queue_.SetReserveFault(
      [&reserves](std::span<const queue::ShardEntries>) -> std::optional<core::Error> {
        ++reserves;
        return std::nullopt;
      });

  auto result = engine.DispatchFanOut(core::MultiKeyKind::kMset,
                                      MakeCmd({"MSET", "a", "1", "b", "2", "c", "3"}));
  ASSERT_TRUE(result.has_value()) << result.error().message();
  EXPECT_EQ(result->AsString(), "OK");
  EXPECT_EQ(reserves, 1);
  for (const auto& [key, value] : {std::pair{"a", "1"}, std::pair{"b", "2"}, std::pair{"c", "3"}}) {
    bool found = false;
    for (const auto& entry : queue_.Published(ShardOf(key))) {
      const auto& args = std::get<core::entry::Write>(entry.payload).cmd.args;
      found = found || args == std::vector<std::string>{"SET", key, value};
    }
    EXPECT_TRUE(found) << "key '" << key << "' is not on its own shard";
  }
}

TEST_F(TieringEngineTest, DelCountsLiveKeysInOneDecision) {
  auto engine = MakeEngine();
  ASSERT_NO_FATAL_FAILURE(Seed(engine, {"SET", "a", "1"}));
  ASSERT_NO_FATAL_FAILURE(Seed(engine, {"SET", "c", "3"}));
  // b is not resident: its existence is probed, never loaded in full.
  EXPECT_CALL(cold_, ProbeKey(std::string_view{"b"}, _))
      .WillOnce(Return(core::Result<std::optional<core::KeyMeta>>(std::nullopt)));
  EXPECT_CALL(cold_, LoadKey(_, _)).Times(0);

  auto result = engine.DispatchFanOut(core::MultiKeyKind::kDelete, MakeCmd({"DEL", "a", "b", "c"}));
  ASSERT_TRUE(result.has_value()) << result.error().message();
  EXPECT_EQ(result->AsInteger(), 2);
}

TEST_F(TieringEngineTest, FlushAdmitsEveryShardWithinOneDeadline) {
  auto engine = MakeEngine();
  std::vector<core::SteadyTime> deadlines;
  EXPECT_CALL(queue_, Admit(_, _))
      .Times(kShardCount)
      .WillRepeatedly([&deadlines](core::ShardId, core::SteadyTime admit_by) {
        deadlines.push_back(admit_by);
        return core::Result<void>{};
      });

  auto result = engine.DispatchFlush(core::FlushTarget::kThisDb);
  ASSERT_TRUE(result.has_value()) << result.error().message();
  EXPECT_EQ(result->AsString(), "OK");
  ASSERT_EQ(deadlines.size(), kShardCount);
  EXPECT_TRUE(std::ranges::all_of(deadlines, [&](auto d) { return d == deadlines.front(); }));
  for (core::ShardId shard = 0; shard < kShardCount; ++shard) {
    const auto published = queue_.Published(shard);
    ASSERT_EQ(published.size(), 1U) << shard;
    EXPECT_TRUE(std::holds_alternative<core::entry::Flush>(published[0].payload));
  }
}

TEST_F(TieringEngineTest, FlushAdmissionFailureWipesNothing) {
  auto engine = MakeEngine();
  ASSERT_NO_FATAL_FAILURE(Seed(engine, {"SET", "k", "v"}));
  std::vector<core::SteadyTime> deadlines;
  ON_CALL(queue_, Admit(_, _))
      .WillByDefault([&deadlines](core::ShardId shard,
                                  core::SteadyTime admit_by) -> core::Result<void> {
        deadlines.push_back(admit_by);
        if (shard == kShardCount - 1) {
          return std::unexpected(core::Error{core::ErrorCode::kResourceExhausted, "window full"});
        }
        return {};
      });
  auto result = engine.DispatchFlush(core::FlushTarget::kThisDb);
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code(), core::ErrorCode::kResourceExhausted);
  ASSERT_EQ(deadlines.size(), kShardCount);
  EXPECT_TRUE(std::ranges::all_of(deadlines, [&](auto d) { return d == deadlines.front(); }));
  for (core::ShardId shard = 0; shard < kShardCount; ++shard) {
    EXPECT_EQ(queue_.Published(shard).size(), shard == ShardOf("k") ? 1U : 0U) << shard;
  }
  EXPECT_EQ(engine.DispatchRead("GET", MakeCmd({"GET", "k"}))->AsString(), "v");
}

TEST_F(TieringEngineTest, FlushReserveFailureWipesNothing) {
  auto engine = MakeEngine();
  ASSERT_NO_FATAL_FAILURE(Seed(engine, {"SET", "k", "v"}));
  queue_.SetReserveFault([](std::span<const queue::ShardEntries>) -> std::optional<core::Error> {
    return core::Error{core::ErrorCode::kInternal, "pwrite: I/O error"};
  });
  auto result = engine.DispatchFlush(core::FlushTarget::kThisDb);
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code(), core::ErrorCode::kInternal);
  EXPECT_EQ(queue_.Published(ShardOf("k")).size(), 1U);
  EXPECT_EQ(engine.DispatchRead("GET", MakeCmd({"GET", "k"}))->AsString(), "v");
}

TEST_F(TieringEngineTest, FlushDurableTimeoutRepliesWithTheTimeoutText) {
  Sequencer fast(hot_, queue_, loader_, router_, SequencerConfig{.write_timeout = 50ms});
  TieringEngine engine(hot_, cold_, router_, fast, TieringEngineConfig{.shard_count = kShardCount});
  queue_.HoldDurable();
  const testing::OnExit release([this] { queue_.ReleaseDurable(); });
  auto result = engine.DispatchFlush(core::FlushTarget::kThisDb);
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code(), core::ErrorCode::kTimeout);
  EXPECT_TRUE(result.error().message().starts_with("flush durable wait exceeded server timeout"))
      << result.error().message();
}

TEST_F(TieringEngineTest, FlushDurableFailurePropagates) {
  auto engine = MakeEngine();
  queue_.FailDurable(core::Error{core::ErrorCode::kInternal, "fsync failed"});
  auto result = engine.DispatchFlush(core::FlushTarget::kThisDb);
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code(), core::ErrorCode::kInternal);
}

// A multi-key DEL admits every shard against one deadline, the write's.
TEST_F(TieringEngineTest, DelAdmitsEveryShardWithinOneDeadline) {
  auto engine = MakeEngine();
  std::vector<core::SteadyTime> deadlines;
  ON_CALL(queue_, Admit(_, _))
      .WillByDefault([&deadlines](core::ShardId, core::SteadyTime admit_by) {
        deadlines.push_back(admit_by);
        return core::Result<void>{};
      });
  const auto start = core::SteadyClock::now();
  auto result =
      engine.DispatchFanOut(core::MultiKeyKind::kDelete, MakeCmd({"DEL", "a", "b", "c", "d", "e"}));
  ASSERT_TRUE(result.has_value()) << result.error().message();
  ASSERT_GE(deadlines.size(), 2U);
  EXPECT_TRUE(std::ranges::all_of(deadlines, [&](auto d) { return d == deadlines.front(); }));
  EXPECT_GE(deadlines.front(), start + kWriteTimeout);
  EXPECT_LT(deadlines.front(), start + kWriteTimeout + 1s);
}

// One reservation: a refused MSET applies none of its keys.
TEST_F(TieringEngineTest, MultiKeyReserveFailureAppliesNothing) {
  auto engine = MakeEngine();
  queue_.SetReserveFault([](std::span<const queue::ShardEntries>) -> std::optional<core::Error> {
    return core::Error{core::ErrorCode::kInternal, "pwrite: I/O error"};
  });

  auto result =
      engine.DispatchFanOut(core::MultiKeyKind::kMset, MakeCmd({"MSET", "a", "1", "b", "2"}));
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code(), core::ErrorCode::kInternal);
  for (const char* key : {"a", "b"}) {
    auto read = hot_.Read(core::ops::ReadOp{core::ops::StringGet{.key = key}});
    EXPECT_FALSE(read.result.has_value()) << key;
  }
}

}  // namespace
}  // namespace abyss::engine
