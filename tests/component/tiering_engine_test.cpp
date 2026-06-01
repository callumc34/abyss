#include "abyss/engine/tiering_engine.h"

#include <gtest/gtest.h>

#include <chrono>
#include <future>
#include <memory>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "abyss/consumer/compaction_buffer.h"
#include "abyss/consumer/compaction_buffer_router.h"
#include "abyss/consumer/hot_consumer_progress.h"
#include "abyss/core/consumer_rpc.h"
#include "abyss/core/ops.h"
#include "abyss/core/shard_router.h"
#include "mock_cold_store.h"
#include "mock_hot_store.h"
#include "mock_queue.h"

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

// Engine has no live HotConsumerPool in these tests; report 0 ("nothing
// settled yet") so DispatchHashRead skips the wait entirely.
class NullHotProgress : public consumer::HotConsumerProgress {
 public:
  core::SequenceId HighestSettledSeq(core::ShardId /*shard*/) const override { return 0; }
};

class TieringEngineTest : public ::testing::Test {
 protected:
  // NOLINTBEGIN(cppcoreguidelines-non-private-member-variables-in-classes)
  testing::MockQueue queue_;
  testing::MockHotStore hot_;
  testing::MockColdStore cold_;
  consumer::CompactionBuffer buffer_;
  SingleBufferRouter router_{buffer_};
  NullHotProgress hot_progress_;
  core::ConsumerRpc rpc_;
  // NOLINTEND(cppcoreguidelines-non-private-member-variables-in-classes)
  static constexpr uint32_t kShardCount = 16;
  static constexpr std::chrono::milliseconds kWriteTimeout = 1s;

  TieringEngine MakeEngine() {
    return {queue_,
            hot_,
            cold_,
            router_,
            hot_progress_,
            rpc_,
            TieringEngineConfig{.shard_count = kShardCount, .write_timeout = kWriteTimeout}};
  }

  core::RespCommand MakeCmd(std::initializer_list<std::string> args) {
    return core::RespCommand{.args = std::vector<std::string>(args)};
  }

  static queue::PendingAppend MakePending(core::SequenceId seq, bool fsync_ok) {
    std::promise<core::Result<void>> p;
    if (fsync_ok) {
      p.set_value(core::Result<void>{});
    } else {
      p.set_value(std::unexpected(core::Error{core::ErrorCode::kInternal, "fsync failed"}));
    }
    return queue::PendingAppend{seq, p.get_future(),
                                std::make_unique<testing::NoopAppendPublisher>()};
  }
};

// --- Read path ---

TEST_F(TieringEngineTest, ReadHotHitReturnsValue) {
  auto engine = MakeEngine();
  auto expected = core::RespValue::BulkString("value");

  EXPECT_CALL(hot_, Exec(_, _)).WillOnce(Return(expected));

  auto result = engine.DispatchRead("GET", MakeCmd({"GET", "key"}));
  ASSERT_TRUE(result.has_value());
  EXPECT_TRUE(result->IsBulkString());
  EXPECT_EQ(result->AsString(), "value");
}

TEST_F(TieringEngineTest, ReadHotMissBufferHitReturnsBufferValue) {
  auto engine = MakeEngine();

  buffer_.Absorb("key", core::ops::WriteOp{core::ops::StringSet{.key = "key", .value = "buffered"}},
                 core::EvictionTTL{86400});

  EXPECT_CALL(hot_, Exec(_, _))
      .WillOnce(Return(std::unexpected(core::Error(core::ErrorCode::kNotFound, ""))));

  auto result = engine.DispatchRead("GET", MakeCmd({"GET", "key"}));
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->AsString(), "buffered");
}

TEST_F(TieringEngineTest, ReadHotMissBufferMissColdHitReturnsColdValue) {
  auto engine = MakeEngine();
  auto cold_value = core::RespValue::BulkString("cold_value");

  EXPECT_CALL(hot_, Exec(_, _))
      .WillOnce(Return(std::unexpected(core::Error(core::ErrorCode::kNotFound, ""))));
  EXPECT_CALL(cold_, Exec(_, _)).WillOnce(Return(cold_value));

  auto result = engine.DispatchRead("GET", MakeCmd({"GET", "key"}));
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->AsString(), "cold_value");
}

TEST_F(TieringEngineTest, ReadAllTiersMissReturnsColdError) {
  auto engine = MakeEngine();

  EXPECT_CALL(hot_, Exec(_, _))
      .WillOnce(Return(std::unexpected(core::Error(core::ErrorCode::kNotFound, ""))));
  EXPECT_CALL(cold_, Exec(_, _))
      .WillOnce(Return(std::unexpected(core::Error(core::ErrorCode::kNotFound, ""))));

  auto result = engine.DispatchRead("GET", MakeCmd({"GET", "missing"}));
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code(), core::ErrorCode::kNotFound);
}

TEST_F(TieringEngineTest, ReadHotErrorPropagates) {
  auto engine = MakeEngine();

  EXPECT_CALL(hot_, Exec(_, _))
      .WillOnce(Return(std::unexpected(core::Error(core::ErrorCode::kWrongType, "wrong type"))));

  auto result = engine.DispatchRead("GET", MakeCmd({"GET", "key"}));
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code(), core::ErrorCode::kWrongType);
}

TEST_F(TieringEngineTest, ReadBufferTombstoneReturnsNull) {
  auto engine = MakeEngine();

  buffer_.Absorb("key", core::ops::WriteOp{core::ops::StringSet{.key = "key", .value = "v"}},
                 core::EvictionTTL{86400});
  buffer_.Absorb("key", core::ops::WriteOp{core::ops::Del{.keys = {"key"}}},
                 core::EvictionTTL{86400});

  EXPECT_CALL(hot_, Exec(_, _))
      .WillOnce(Return(std::unexpected(core::Error(core::ErrorCode::kNotFound, ""))));

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
      core::EvictionTTL{86400});
  buffer_.Absorb("h", core::ops::WriteOp{core::ops::HashDel{.key = "h", .fields = {"c"}}},
                 core::EvictionTTL{86400});

  EXPECT_CALL(hot_, Exec(_, _))
      .WillOnce(Return(std::unexpected(core::Error(core::ErrorCode::kNotFound, ""))));
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
      core::EvictionTTL{86400});
  buffer_.Absorb("h", core::ops::WriteOp{core::ops::Del{.keys = {"h"}}}, core::EvictionTTL{86400});

  EXPECT_CALL(hot_, Exec(_, _))
      .WillOnce(Return(std::unexpected(core::Error(core::ErrorCode::kNotFound, ""))));
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
                 core::EvictionTTL{86400});

  EXPECT_CALL(hot_, Exec(_, _))
      .WillOnce(Return(std::unexpected(core::Error(core::ErrorCode::kNotFound, ""))));
  EXPECT_CALL(cold_, Exec(_, _)).Times(0);

  auto result = engine.DispatchRead("HGETALL", MakeCmd({"HGETALL", "k"}));
  // WRONGTYPE is encoded as a kError RespValue (not a Result error).
  ASSERT_TRUE(result.has_value());
  EXPECT_TRUE(result->IsError());
}

TEST_F(TieringEngineTest, HashGetAllNotPresentDelegatesToCold) {
  auto engine = MakeEngine();
  EXPECT_CALL(hot_, Exec(_, _))
      .WillOnce(Return(std::unexpected(core::Error(core::ErrorCode::kNotFound, ""))));
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
      core::EvictionTTL{86400});
  buffer_.Absorb("h", core::ops::WriteOp{core::ops::HashDel{.key = "h", .fields = {"c"}}},
                 core::EvictionTTL{86400});

  EXPECT_CALL(hot_, Exec(_, _))
      .WillOnce(Return(std::unexpected(core::Error(core::ErrorCode::kNotFound, ""))));
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
                 core::EvictionTTL{86400});

  EXPECT_CALL(hot_, Exec(_, _))
      .Times(2)
      .WillRepeatedly(Return(std::unexpected(core::Error(core::ErrorCode::kNotFound, ""))));
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
                 core::EvictionTTL{86400});
  buffer_.Absorb("h", core::ops::WriteOp{core::ops::HashDel{.key = "h", .fields = {"buf_removed"}}},
                 core::EvictionTTL{86400});

  EXPECT_CALL(hot_, Exec(_, _))
      .WillOnce(Return(std::unexpected(core::Error(core::ErrorCode::kNotFound, ""))));
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
      core::EvictionTTL{86400});

  EXPECT_CALL(hot_, Exec(_, _))
      .WillOnce(Return(std::unexpected(core::Error(core::ErrorCode::kNotFound, ""))));
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
      core::EvictionTTL{86400});
  buffer_.Absorb("h", core::ops::WriteOp{core::ops::HashDel{.key = "h", .fields = {"f"}}},
                 core::EvictionTTL{86400});

  EXPECT_CALL(hot_, Exec(_, _))
      .WillOnce(Return(std::unexpected(core::Error(core::ErrorCode::kNotFound, ""))));
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
                 core::EvictionTTL{86400});
  buffer_.Absorb("s", core::ops::WriteOp{core::ops::SetRem{.key = "s", .members = {"b"}}},
                 core::EvictionTTL{86400});

  EXPECT_CALL(hot_, Exec(_, _))
      .WillOnce(Return(std::unexpected(core::Error(core::ErrorCode::kNotFound, ""))));
  // Cold must NOT be consulted: the buffer answers authoritatively.
  EXPECT_CALL(cold_, Exec(_, _)).Times(0);

  auto result = engine.DispatchRead("SCARD", MakeCmd({"SCARD", "s"}));
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->AsInteger(), 2);
}

TEST_F(TieringEngineTest, SismemberReflectsBufferedRemoval) {
  auto engine = MakeEngine();
  buffer_.Absorb("s", core::ops::WriteOp{core::ops::SetAdd{.key = "s", .members = {"a", "b"}}},
                 core::EvictionTTL{86400});
  buffer_.Absorb("s", core::ops::WriteOp{core::ops::SetRem{.key = "s", .members = {"b"}}},
                 core::EvictionTTL{86400});

  EXPECT_CALL(hot_, Exec(_, _))
      .Times(2)
      .WillRepeatedly(Return(std::unexpected(core::Error(core::ErrorCode::kNotFound, ""))));
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
                 core::EvictionTTL{86400});
  buffer_.Absorb("z", core::ops::WriteOp{core::ops::ZsetRem{.key = "z", .members = {"m"}}},
                 core::EvictionTTL{86400});

  EXPECT_CALL(hot_, Exec(_, _))
      .Times(2)
      .WillRepeatedly(Return(std::unexpected(core::Error(core::ErrorCode::kNotFound, ""))));
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
  EXPECT_CALL(hot_, Exec(_, _))
      .Times(4)
      .WillRepeatedly(Return(std::unexpected(core::Error(core::ErrorCode::kNotFound, ""))));
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
                 core::EvictionTTL{86400});

  EXPECT_CALL(hot_, Exec(_, _))
      .WillOnce(Return(std::unexpected(core::Error(core::ErrorCode::kNotFound, ""))));
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
  TieringEngine engine(queue_, hot_, cold_, router_, hot_progress_, rpc_, cfg);

  EXPECT_CALL(hot_, Exec(_, _))
      .WillOnce(Return(std::unexpected(core::Error(core::ErrorCode::kNotFound, ""))));
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
  TieringEngine engine(queue_, hot_, cold_, router_, hot_progress_, rpc_, cfg);

  EXPECT_CALL(hot_, Exec(_, _))
      .WillOnce(Return(std::unexpected(core::Error(core::ErrorCode::kNotFound, ""))));
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
  TieringEngine engine(queue_, hot_, cold_, router_, hot_progress_, rpc_, cfg);

  EXPECT_CALL(hot_, Exec(_, _))
      .WillOnce(Return(std::unexpected(core::Error(core::ErrorCode::kNotFound, ""))));
  // Cold fails closed with a timeout on a large scan. The engine surfaces the
  // error to the client, never a silently truncated array (decision 4).
  EXPECT_CALL(cold_, Exec(_, std::optional<core::Duration>(50ms)))
      .WillOnce(Return(std::unexpected(core::Error(core::ErrorCode::kTimeout, "scan deadline"))));

  auto result = engine.DispatchRead("SMEMBERS", MakeCmd({"SMEMBERS", "big"}));
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code(), core::ErrorCode::kTimeout);
}

// --- Write path ---

TEST_F(TieringEngineTest, WriteSuccessReturnsConsumerResult) {
  auto engine = MakeEngine();

  constexpr core::SequenceId kSeq = 42;
  const core::RpcId kRpcId = core::MakeRpcId(core::ComputeShard("key", kShardCount), kSeq);
  EXPECT_CALL(queue_, BeginAppend(_, _))
      // NOLINTNEXTLINE(performance-unnecessary-value-param)
      .WillOnce([](core::ShardId, core::QueueEntry) { return MakePending(kSeq, true); });

  std::thread fulfiller([this, kRpcId]() {
    while (rpc_.PendingCount() == 0) std::this_thread::yield();
    EXPECT_TRUE(rpc_.Fulfill(kRpcId, core::RespValue::SimpleString("OK")));
  });

  auto result = engine.DispatchWrite("SET", MakeCmd({"SET", "key", "value"}));
  fulfiller.join();

  ASSERT_TRUE(result.has_value());
  EXPECT_TRUE(result->IsSimpleString());
  EXPECT_EQ(result->AsString(), "OK");
  EXPECT_EQ(rpc_.PendingCount(), 0U);
}

TEST_F(TieringEngineTest, WriteConsumerErrorPropagates) {
  auto engine = MakeEngine();

  constexpr core::SequenceId kSeq = 43;
  const core::RpcId kRpcId = core::MakeRpcId(core::ComputeShard("key", kShardCount), kSeq);
  EXPECT_CALL(queue_, BeginAppend(_, _))
      // NOLINTNEXTLINE(performance-unnecessary-value-param)
      .WillOnce([](core::ShardId, core::QueueEntry) { return MakePending(kSeq, true); });

  std::thread fulfiller([this, kRpcId]() {
    while (rpc_.PendingCount() == 0) std::this_thread::yield();
    EXPECT_TRUE(rpc_.Fulfill(kRpcId, core::RespValue::Error(core::ErrorPrefix::kWrongType,
                                                            "operation against wrong type")));
  });

  auto result = engine.DispatchWrite("SADD", MakeCmd({"SADD", "key", "m"}));
  fulfiller.join();

  ASSERT_TRUE(result.has_value());
  EXPECT_TRUE(result->IsError());
  EXPECT_EQ(result->AsString(), "WRONGTYPE operation against wrong type");
  EXPECT_EQ(rpc_.PendingCount(), 0U);
}

TEST_F(TieringEngineTest, WriteQueueFailureReturnsError) {
  auto engine = MakeEngine();

  EXPECT_CALL(queue_, BeginAppend(_, _))
      .WillOnce(Return(std::unexpected(core::Error(core::ErrorCode::kResourceExhausted, "full"))));

  auto result = engine.DispatchWrite("SET", MakeCmd({"SET", "key", "value"}));
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code(), core::ErrorCode::kResourceExhausted);
  EXPECT_EQ(rpc_.PendingCount(), 0U);
}

TEST_F(TieringEngineTest, WriteFsyncFailureCancelsRpcAndPropagates) {
  auto engine = MakeEngine();

  EXPECT_CALL(queue_, BeginAppend(_, _))
      // NOLINTNEXTLINE(performance-unnecessary-value-param)
      .WillOnce([](core::ShardId, core::QueueEntry) { return MakePending(99, false); });

  auto result = engine.DispatchWrite("SET", MakeCmd({"SET", "key", "value"}));
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code(), core::ErrorCode::kInternal);
  EXPECT_EQ(rpc_.PendingCount(), 0U);
}

TEST_F(TieringEngineTest, WriteTimeoutReturnsErrorAndCancelsRpc) {
  TieringEngineConfig fast{.shard_count = kShardCount, .write_timeout = 50ms};
  TieringEngine engine(queue_, hot_, cold_, router_, hot_progress_, rpc_, fast);

  EXPECT_CALL(queue_, BeginAppend(_, _))
      // NOLINTNEXTLINE(performance-unnecessary-value-param)
      .WillOnce([](core::ShardId, core::QueueEntry) { return MakePending(100, true); });

  auto result = engine.DispatchWrite("SET", MakeCmd({"SET", "key", "value"}));
  ASSERT_TRUE(result.has_value());
  EXPECT_TRUE(result->IsError());
  EXPECT_TRUE(result->AsString().starts_with("ERR "));
  EXPECT_EQ(rpc_.PendingCount(), 0U);
}

// --- Fan-out (MGET, EXISTS, MSET, DEL) ---
//
// Coverage focuses on what unit tests can't reach: per-shard WAL placement on
// MSET/DEL, per-tier aggregation for MGET/EXISTS, and partial-failure surfacing
// when one sub-command's BeginAppend rejects.

TEST_F(TieringEngineTest, MgetAggregatesAcrossTiersInPositionalOrder) {
  auto engine = MakeEngine();
  buffer_.Absorb("b", core::ops::WriteOp{core::ops::StringSet{.key = "b", .value = "from_buf"}},
                 core::EvictionTTL{86400});

  EXPECT_CALL(hot_, Exec(_, _))
      .WillOnce(Return(core::RespValue::BulkString("from_hot")))                        // a
      .WillOnce(Return(std::unexpected(core::Error(core::ErrorCode::kNotFound, ""))))   // b
      .WillOnce(Return(std::unexpected(core::Error(core::ErrorCode::kNotFound, ""))))   // c
      .WillOnce(Return(std::unexpected(core::Error(core::ErrorCode::kNotFound, ""))));  // d
  // Only c falls through to cold (a hit hot, b hit buffer, d misses cold).
  EXPECT_CALL(cold_, Exec(_, _))
      .WillOnce(Return(core::RespValue::BulkString("from_cold")))                       // c
      .WillOnce(Return(std::unexpected(core::Error(core::ErrorCode::kNotFound, ""))));  // d

  // c's cold hit must promote through queue on c's shard, not on the MGET first-key shard.
  const core::ShardId c_shard = core::ComputeShard("c", kShardCount);
  EXPECT_CALL(cold_, GetPromotionCommand(std::string_view{"c"}))
      .WillOnce(Return(std::optional<core::RespCommand>{MakeCmd({"SET", "c", "from_cold"})}));
  EXPECT_CALL(queue_, Append(c_shard, _))
      .WillOnce([](core::ShardId, const core::QueueEntry&) -> core::Result<queue::AppendResult> {
        return queue::AppendResult{.seq = 1};
      });

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
  EXPECT_CALL(hot_, Exec(_, _))
      .WillOnce(Return(std::unexpected(core::Error(core::ErrorCode::kWrongType, "wrong type"))));

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
                 core::EvictionTTL{86400});
  buffer_.Absorb("k", core::ops::WriteOp{core::ops::Del{.keys = {"k"}}}, core::EvictionTTL{86400});

  // Hot has no record; cold is never consulted because the buffer probe is
  // authoritative on the tombstone.
  EXPECT_CALL(hot_, Probe(_)).WillOnce(Return(core::HotKeyPresence::kAbsent));
  EXPECT_CALL(cold_, Exec(_, _)).Times(0);

  auto result = engine.DispatchFanOut(core::MultiKeyKind::kExists, MakeCmd({"EXISTS", "k"}));
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->AsInteger(), 0);
}

TEST_F(TieringEngineTest, ExistsCountsDuplicatesRedisStyle) {
  auto engine = MakeEngine();
  EXPECT_CALL(hot_, Probe(_)).Times(2).WillRepeatedly(Return(core::HotKeyPresence::kPresent));

  auto result = engine.DispatchFanOut(core::MultiKeyKind::kExists, MakeCmd({"EXISTS", "k", "k"}));
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->AsInteger(), 2);
}

TEST_F(TieringEngineTest, MsetAppendsOneEntryPerKeyToOwningShard) {
  auto engine = MakeEngine();

  std::vector<core::ShardId> appended_shards;
  std::vector<std::string> appended_keys;
  std::vector<core::RpcId> rpc_ids;
  std::mutex mu;
  auto seq = std::make_shared<std::atomic<core::SequenceId>>(500);

  EXPECT_CALL(queue_, BeginAppend(_, _))
      .Times(3)
      .WillRepeatedly([&, seq](core::ShardId shard, core::QueueEntry entry) {
        const auto next = seq->fetch_add(1);
        std::string key;
        const auto* write = std::get_if<core::entry::Write>(&entry.payload);
        EXPECT_NE(write, nullptr);
        if (write != nullptr) {
          EXPECT_EQ(write->cmd.args[0], "SET");
          key = write->cmd.args[1];
        }
        {
          std::scoped_lock lock(mu);
          appended_shards.push_back(shard);
          appended_keys.push_back(key);
          rpc_ids.push_back(core::MakeRpcId(shard, next));
        }
        return MakePending(next, true);
      });

  // The mock records rpc_ids inside BeginAppend's lambda, before the engine
  // proceeds to rpc_.Register(). In production, Register strictly precedes
  // Publish — the consumer can't see the entry yet, so Fulfill can't race.
  // This fulfiller models that ordering by retrying when Fulfill reports the
  // id isn't yet registered.
  std::thread fulfiller([&]() {
    size_t fulfilled = 0;
    while (fulfilled < 3) {
      std::vector<core::RpcId> pending;
      {
        std::scoped_lock lock(mu);
        pending = rpc_ids;
      }
      bool made_progress = false;
      for (size_t i = fulfilled; i < pending.size(); ++i) {
        if (rpc_.Fulfill(pending[i], core::RespValue::SimpleString("OK"))) {
          ++fulfilled;
          made_progress = true;
        } else {
          break;
        }
      }
      if (!made_progress) std::this_thread::yield();
    }
  });

  auto result = engine.DispatchFanOut(core::MultiKeyKind::kMset,
                                      MakeCmd({"MSET", "a", "1", "b", "2", "c", "3"}));
  fulfiller.join();

  ASSERT_TRUE(result.has_value());
  EXPECT_TRUE(result->IsSimpleString());
  EXPECT_EQ(result->AsString(), "OK");

  // Each key's WAL entry lands on its own shard.
  ASSERT_EQ(appended_keys.size(), 3U);
  for (size_t i = 0; i < appended_keys.size(); ++i) {
    EXPECT_EQ(appended_shards[i], core::ComputeShard(appended_keys[i], kShardCount))
        << "key '" << appended_keys[i] << "' appended to shard " << appended_shards[i];
  }
  EXPECT_EQ(rpc_.PendingCount(), 0U);
}

TEST_F(TieringEngineTest, DelFanOutSumsPerKeyIntegerReplies) {
  auto engine = MakeEngine();
  std::vector<core::RpcId> rpc_ids;
  std::mutex mu;
  auto seq = std::make_shared<std::atomic<core::SequenceId>>(700);

  EXPECT_CALL(queue_, BeginAppend(_, _))
      .Times(3)
      .WillRepeatedly([&, seq](core::ShardId shard, const core::QueueEntry&) {
        const auto next = seq->fetch_add(1);
        {
          std::scoped_lock lock(mu);
          rpc_ids.push_back(core::MakeRpcId(shard, next));
        }
        return MakePending(next, true);
      });

  // Two keys existed, one didn't; the per-key hot consumer fulfils with 1/0.
  std::thread fulfiller([&]() {
    const std::vector<int64_t> replies{1, 0, 1};
    size_t fulfilled = 0;
    while (fulfilled < replies.size()) {
      std::vector<core::RpcId> pending;
      {
        std::scoped_lock lock(mu);
        pending = rpc_ids;
      }
      bool made_progress = false;
      for (size_t i = fulfilled; i < pending.size(); ++i) {
        if (rpc_.Fulfill(pending[i], core::RespValue::Integer(replies[i]))) {
          ++fulfilled;
          made_progress = true;
        } else {
          break;
        }
      }
      if (!made_progress) std::this_thread::yield();
    }
  });

  auto result = engine.DispatchFanOut(core::MultiKeyKind::kDelete, MakeCmd({"DEL", "a", "b", "c"}));
  fulfiller.join();

  ASSERT_TRUE(result.has_value());
  ASSERT_TRUE(result->IsInteger());
  EXPECT_EQ(result->AsInteger(), 2);
  EXPECT_EQ(rpc_.PendingCount(), 0U);
}

TEST_F(TieringEngineTest, FanOutPartialBeginAppendFailureSurfacesError) {
  auto engine = MakeEngine();
  // First sub succeeds (auto-publishes on scope exit), second fails. Per
  // ADP-005 the partially-published sub may still apply — we cancel its RPC
  // registration and return the error to the client.
  EXPECT_CALL(queue_, BeginAppend(_, _))
      // NOLINTNEXTLINE(performance-unnecessary-value-param)
      .WillOnce([](core::ShardId, core::QueueEntry) { return MakePending(900, true); })
      .WillOnce(Return(std::unexpected(core::Error(core::ErrorCode::kResourceExhausted, "full"))));

  auto result =
      engine.DispatchFanOut(core::MultiKeyKind::kMset, MakeCmd({"MSET", "a", "1", "b", "2"}));
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code(), core::ErrorCode::kResourceExhausted);
  EXPECT_EQ(rpc_.PendingCount(), 0U);
}

// C5: a slow durable wait must not leave the RPC wait starved. The guaranteed
// min RPC budget is write_timeout * min_rpc_wait_fraction. Without C5, a
// durable completion near the original deadline would give the RPC ~0 budget.
TEST_F(TieringEngineTest, SlowDurableDoesNotStarveRpcBudget) {
  // Real-time test of deadline math. Timings chosen so durable fires past the
  // T*(1-fraction) boundary where the floor actually extends the deadline,
  // and with CI jitter margin at each step:
  //   write_timeout=100ms, fraction=0.5
  //   durable@75ms   — 25ms below T, 25ms above T*(1-f)=50ms
  //   rpc@115ms      — 15ms past T, 10ms inside the extended floor 75+50=125ms
  TieringEngineConfig cfg{
      .shard_count = kShardCount,
      .write_timeout = 100ms,
      .min_rpc_wait_fraction = 0.5,
  };
  TieringEngine engine(queue_, hot_, cold_, router_, hot_progress_, rpc_, cfg);

  constexpr core::SequenceId kSeq = 201;
  const core::RpcId kRpcId = core::MakeRpcId(core::ComputeShard("key", kShardCount), kSeq);
  std::promise<core::Result<void>> durable_p;
  auto durable_fut = durable_p.get_future();
  EXPECT_CALL(queue_, BeginAppend(_, _))
      // NOLINTNEXTLINE(performance-unnecessary-value-param)
      .WillOnce([&durable_fut](core::ShardId, core::QueueEntry) {
        return queue::PendingAppend{kSeq, std::move(durable_fut),
                                    std::make_unique<testing::NoopAppendPublisher>()};
      });

  std::thread durable_releaser([&durable_p]() {
    std::this_thread::sleep_for(75ms);
    durable_p.set_value(core::Result<void>{});
  });
  std::thread rpc_releaser([this, kRpcId]() {
    std::this_thread::sleep_for(115ms);
    // Fulfill either succeeds (promise is still pending) or fails because the
    // RPC was cancelled on a timeout path — either way, no retry loop.
    (void)rpc_.Fulfill(kRpcId, core::RespValue::SimpleString("OK"));
  });

  auto result = engine.DispatchWrite("SET", MakeCmd({"SET", "key", "value"}));
  durable_releaser.join();
  rpc_releaser.join();

  ASSERT_TRUE(result.has_value());
  EXPECT_TRUE(result->IsSimpleString());
  EXPECT_EQ(result->AsString(), "OK");
  EXPECT_EQ(rpc_.PendingCount(), 0U);
}

}  // namespace
}  // namespace abyss::engine
