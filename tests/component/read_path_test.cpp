#include "abyss/engine/read_path.h"

#include <gtest/gtest.h>

#ifdef ABYSS_HAVE_ROCKSDB

#include <rocksdb/perf_context.h>
#include <rocksdb/perf_level.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <future>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "abyss/cold/backends/rocksdb_store.h"
#include "abyss/consumer/compacted_state.h"
#include "abyss/consumer/compaction_buffer.h"
#include "abyss/consumer/compaction_buffer_router.h"
#include "abyss/core/cold_store.h"
#include "abyss/core/ops.h"
#include "abyss/core/resp_types.h"
#include "abyss/core/result.h"
#include "abyss/core/shard_router.h"
#include "abyss/engine/loader.h"
#include "abyss/engine/sequencer.h"
#include "abyss/engine/tiering_engine.h"
#include "abyss/hot/sharded_hot_store.h"
#include "abyss/hot/single_shard_store.h"
#include "abyss/metrics/names.h"
#include "abyss/metrics/testing.h"
#include "latch.h"
#include "mock_queue.h"
#include "on_exit.h"
#include "temp_dir.h"
#include "test_clock.h"

namespace abyss::engine {
namespace {

using namespace std::chrono_literals;
namespace ops = core::ops;

constexpr uint32_t kShards = 4;
constexpr core::EvictionTTL kEviction{3600};

// Forwards to RocksDB, counting what each read asks of it.
class CountingColdStore : public core::ColdStore {
 public:
  explicit CountingColdStore(core::ColdStore& inner) : inner_(inner) {}

  core::Result<void> ApplyBatch(std::span<const ops::WriteOp> ops,
                                core::SequenceId highest_wal_seq) override {
    return inner_.ApplyBatch(ops, highest_wal_seq);
  }
  core::Result<void> Checkpoint(core::ShardId shard, core::SequenceId up_to) override {
    return inner_.Checkpoint(shard, up_to);
  }
  core::Result<void> Wipe(core::ShardId shard) override { return inner_.Wipe(shard); }
  core::Result<core::StorageStats> Stats() override { return inner_.Stats(); }
  core::Result<void> Compact() override { return inner_.Compact(); }
  core::Result<std::optional<core::ColdKeyState>> LoadKey(std::string_view key,
                                                          core::SteadyTime deadline) override {
    Note(deadline);
    ++loads;
    if (hold_full_loads_) {
      entered.Open();
      release.Wait();
    }
    return inner_.LoadKey(key, deadline);
  }
  core::Result<std::optional<core::LoadedAs>> LoadKeyAs(std::string_view key, core::KeyType type,
                                                        core::SteadyTime deadline) override {
    Note(deadline);
    ++loads;
    if (hold_loads_) {
      entered.Open();
      release.Wait();
    }
    return inner_.LoadKeyAs(key, type, deadline);
  }
  core::Result<std::optional<core::KeyMeta>> ProbeKey(std::string_view key,
                                                      core::SteadyTime deadline) override {
    Note(deadline);
    ++probes;
    return inner_.ProbeKey(key, deadline);
  }
  core::Result<std::vector<std::optional<core::MemberValue>>> LoadMembers(
      std::string_view key, core::KeyType type, std::span<const std::string_view> members,
      core::SteadyTime deadline) override {
    Note(deadline);
    ++member_batches;
    return inner_.LoadMembers(key, type, members, deadline);
  }

  int Reads() const { return loads + probes + member_batches; }
  void HoldLoads() { hold_loads_ = true; }
  // A write's: a read loads as its type.
  void HoldFullLoads() { hold_full_loads_ = true; }
  // The budget the last read was given.
  core::SteadyClock::duration LastBudget() const {
    const std::scoped_lock lock(mu_);
    return last_budget_;
  }

  // NOLINTBEGIN(cppcoreguidelines-non-private-member-variables-in-classes)
  std::atomic<int> loads{0};
  std::atomic<int> probes{0};
  std::atomic<int> member_batches{0};
  abyss::testing::Latch entered;
  abyss::testing::Latch release;
  // NOLINTEND(cppcoreguidelines-non-private-member-variables-in-classes)

 private:
  void Note(core::SteadyTime deadline) {
    const std::scoped_lock lock(mu_);
    last_budget_ = deadline - core::SteadyClock::now();
  }

  core::ColdStore& inner_;
  std::atomic<bool> hold_loads_{false};
  std::atomic<bool> hold_full_loads_{false};
  mutable std::mutex mu_;
  core::SteadyClock::duration last_budget_{};
};

// One buffer for every shard; no cold consumer runs, so nothing waits
// on a drain, and a test that asks for one fails.
class OneBufferRouter : public consumer::CompactionBufferRouter {
 public:
  explicit OneBufferRouter(consumer::CompactionBuffer& buffer) : buffer_(buffer) {}
  std::optional<consumer::CompactedState> Snapshot(core::ShardId /*shard*/,
                                                   std::string_view key) const override {
    return buffer_.Snapshot(key);
  }
  bool WaitForDrainedSeq(core::ShardId /*shard*/, core::SequenceId /*target_seq*/,
                         std::chrono::milliseconds /*timeout*/) override {
    ++drain_waits;
    return true;
  }

  // NOLINTNEXTLINE(cppcoreguidelines-non-private-member-variables-in-classes)
  std::atomic<int> drain_waits{0};

 private:
  consumer::CompactionBuffer& buffer_;
};

std::unique_ptr<cold::backends::RocksdbStore> OpenCold(const abyss::testing::TempDir& dir) {
  auto created = cold::backends::RocksdbStore::Create({.data_path = dir.String()});
  if (!created.has_value()) {
    ADD_FAILURE() << created.error().message();
    return nullptr;
  }
  return std::move(*created);
}

// A reply as text: "$v", ":1", "+set", "nil", "[...]" or "-CODE".
// `sorted` orders an array whose order the command leaves open.
std::string Describe(const core::Result<core::RespValue>& reply, bool sorted = false) {
  if (!reply.has_value()) {
    return reply.error().code() == core::ErrorCode::kWrongType ? "-WRONGTYPE"
                                                               : "-ERR " + reply.error().message();
  }
  const std::function<std::string(const core::RespValue&)> one =
      [&one, sorted](const core::RespValue& v) -> std::string {
    switch (v.type()) {
      case core::RespValue::Type::kNull:
        return "nil";
      case core::RespValue::Type::kInteger:
        return ":" + std::to_string(v.AsInteger());
      case core::RespValue::Type::kSimpleString:
        return "+" + v.AsString();
      case core::RespValue::Type::kBulkString:
        return "$" + v.AsString();
      case core::RespValue::Type::kError:
        return "-" + v.AsString();
      case core::RespValue::Type::kArray: {
        std::vector<std::string> items;
        for (const auto& item : v.AsArray()) items.push_back(one(item));
        if (sorted) std::ranges::sort(items);
        std::string out = "[";
        for (size_t i = 0; i < items.size(); ++i) out += (i == 0 ? "" : ",") + items[i];
        return out + "]";
      }
      default:
        return "?";
    }
  };
  return one(*reply);
}

// HGETALL's pairs, ordered.
std::string DescribePairs(const core::Result<core::RespValue>& reply) {
  if (!reply.has_value() || !reply->IsArray()) return Describe(reply);
  const auto& items = reply->AsArray();
  std::vector<std::string> pairs;
  for (size_t i = 0; i + 1 < items.size(); i += 2) {
    pairs.push_back(items[i].AsString() + "=" + items[i + 1].AsString());
  }
  std::ranges::sort(pairs);
  std::string out = "{";
  for (size_t i = 0; i < pairs.size(); ++i) out += (i == 0 ? "" : ",") + pairs[i];
  return out + "}";
}

// Generous, so a slow runner never times a read out, and distinct, so
// a test can tell which deadline a read used.
constexpr std::chrono::milliseconds kPointDeadline = 5s;
constexpr std::chrono::milliseconds kScanDeadline = 10s;

class ReadPathFixture : public ::testing::Test {
 protected:
  explicit ReadPathFixture(ReadPathConfig config = {.cold_read_deadline = kPointDeadline,
                                                    .cold_scan_deadline = kScanDeadline,
                                                    .fill_doorkeeper = false}) {
    config.wall_clock = clock_.WallFn();
    reads_ = std::make_unique<ReadPath>(hot_, loader_, sequencer_, config);
  }

  int64_t NowMs() const {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               clock_.WallNow().time_since_epoch())
        .count();
  }

  // Through the sequencer, so hot holds the key as decided.
  void Write(std::vector<std::string> args) {
    auto written =
        sequencer_.Execute(core::RespCommand{.args = std::move(args)}, core::PredicateFlags::kNone);
    ASSERT_TRUE(written.has_value()) << written.error().message();
    ASSERT_FALSE(written->IsError()) << written->AsString();
  }
  // As a flush leaves it in cold.
  void Cold(std::vector<ops::WriteOp> batch) {
    ASSERT_TRUE(rocks_->ApplyBatch(batch, ++seq_).has_value());
  }
  // Absorbed and unflushed.
  void Buffer(std::string_view key, const ops::WriteOp& op) {
    const core::SequenceId seq = ++seq_;
    buffer_.Absorb(std::string(key), op, kEviction, seq, static_cast<uint64_t>(NowMs()));
  }

  core::Result<core::RespValue> Read(std::vector<std::string> args) {
    const core::RespCommand cmd{.args = std::move(args)};
    TieringEngine engine(*reads_, sequencer_);
    const std::string name = cmd.args.front();
    if (name == "EXISTS") return engine.DispatchFanOut(core::MultiKeyKind::kExists, cmd);
    if (name == "MGET") return engine.DispatchFanOut(core::MultiKeyKind::kMget, cmd);
    return engine.DispatchRead(name, cmd);
  }
  // Hot holds an entry for `key`: a value read answers, if only with
  // WRONGTYPE. A stub answers meta reads alone.
  bool Resident(std::string_view key) {
    auto hit = hot_.Read(ops::ReadOp{ops::StringGet{.key = key}});
    return hit.result.has_value() || hit.result.error().code() != core::ErrorCode::kNotFound;
  }

  // NOLINTBEGIN(cppcoreguidelines-non-private-member-variables-in-classes)
  abyss::testing::TestClock clock_;
  std::atomic<core::SequenceId> drained_{hot::kAllDrained};
  core::SequenceId seq_ = 0;
  abyss::testing::TempDir dir_{"read_path"};
  std::unique_ptr<cold::backends::RocksdbStore> rocks_ = OpenCold(dir_);
  CountingColdStore cold_{*rocks_};
  consumer::CompactionBuffer buffer_{clock_.SteadyFn()};
  OneBufferRouter router_{buffer_};
  ::testing::NiceMock<abyss::testing::MockQueue> queue_;
  hot::ShardedHotStore hot_{hot::ShardedHotStoreConfig{
      .max_memory_bytes = size_t{64} << 20,
      .shard_count = kShards,
      .drained = [this](core::ShardId) { return drained_.load(); },
      .steady_clock = clock_.SteadyFn(),
      .wall_clock = clock_.WallFn(),
  }};
  Loader loader_{hot_, router_, cold_, clock_.WallFn()};
  Sequencer sequencer_{hot_, queue_, loader_, router_,
                       SequencerConfig{.write_timeout = 2s, .wall_clock = clock_.WallFn()}};
  std::unique_ptr<ReadPath> reads_;
  // NOLINTEND(cppcoreguidelines-non-private-member-variables-in-classes)
};

// --- Every read command across every place a key's state can be ---

enum class Where : uint8_t {
  kHotHit,
  kExpiredResident,
  kFlushFloor,
  kStub,
  kBufferOnly,
  kColdOnly,
  kDeltaOverCold,
  kAbsent,
  kWrongType,
};

std::string_view NameOf(Where where) {
  switch (where) {
    case Where::kHotHit:
      return "HotHit";
    case Where::kExpiredResident:
      return "ExpiredResident";
    case Where::kFlushFloor:
      return "FlushFloor";
    case Where::kStub:
      return "Stub";
    case Where::kBufferOnly:
      return "BufferOnly";
    case Where::kColdOnly:
      return "ColdOnly";
    case Where::kDeltaOverCold:
      return "DeltaOverCold";
    case Where::kAbsent:
      return "Absent";
    case Where::kWrongType:
      return "WrongType";
  }
  return "?";
}

struct Command {
  std::vector<std::string> args;
  // The reply for the canonical state, and for an absent key.
  std::string present;
  std::string absent;
  // The same command against a key of another type; empty for EXISTS,
  // TYPE and TTL, which read any type.
  std::vector<std::string> wrong_type;
  bool sorted = false;
  bool pairs = false;
};

// s = "v" expiring in 100 s, set = {a, b}, z = {m: 1, n: 2},
// h = {f: 1, g: 2}. Meta reads first, so a stub answers them before a
// fill replaces it.
std::vector<Command> Commands() {
  return {
      {.args = {"EXISTS", "s"}, .present = ":1", .absent = ":0"},
      {.args = {"TYPE", "set"}, .present = "+set", .absent = "+none"},
      {.args = {"TTL", "s"}, .present = ":100", .absent = ":-2"},
      {.args = {"PTTL", "s"}, .present = ":100000", .absent = ":-2"},
      {.args = {"TTL", "h"}, .present = ":-1", .absent = ":-2"},
      {.args = {"GET", "s"}, .present = "$v", .absent = "nil", .wrong_type = {"GET", "set"}},
      {.args = {"SISMEMBER", "set", "a"},
       .present = ":1",
       .absent = ":0",
       .wrong_type = {"SISMEMBER", "s", "a"}},
      {.args = {"SMEMBERS", "set"},
       .present = "[$a,$b]",
       .absent = "[]",
       .wrong_type = {"SMEMBERS", "h"},
       .sorted = true},
      {.args = {"SCARD", "set"}, .present = ":2", .absent = ":0", .wrong_type = {"SCARD", "z"}},
      {.args = {"ZSCORE", "z", "m"},
       .present = "$1",
       .absent = "nil",
       .wrong_type = {"ZSCORE", "set", "m"}},
      {.args = {"ZCARD", "z"}, .present = ":2", .absent = ":0", .wrong_type = {"ZCARD", "s"}},
      {.args = {"ZRANGE", "z", "0", "-1", "WITHSCORES"},
       .present = "[$m,$1,$n,$2]",
       .absent = "[]",
       .wrong_type = {"ZRANGE", "h", "0", "-1"}},
      {.args = {"HGET", "h", "f"},
       .present = "$1",
       .absent = "nil",
       .wrong_type = {"HGET", "z", "f"}},
      {.args = {"HGETALL", "h"},
       .present = "{f=1,g=2}",
       .absent = "{}",
       .wrong_type = {"HGETALL", "set"},
       .pairs = true},
      {.args = {"HMGET", "h", "f", "x", "g"},
       .present = "[$1,nil,$2]",
       .absent = "[nil,nil,nil]",
       .wrong_type = {"HMGET", "s", "f"}},
      {.args = {"HEXISTS", "h", "f"},
       .present = ":1",
       .absent = ":0",
       .wrong_type = {"HEXISTS", "z", "f"}},
      {.args = {"HKEYS", "h"},
       .present = "[$f,$g]",
       .absent = "[]",
       .wrong_type = {"HKEYS", "s"},
       .sorted = true},
      {.args = {"HVALS", "h"},
       .present = "[$1,$2]",
       .absent = "[]",
       .wrong_type = {"HVALS", "set"},
       .sorted = true},
      {.args = {"HLEN", "h"}, .present = ":2", .absent = ":0", .wrong_type = {"HLEN", "z"}},
  };
}

class ReadPathTableTest : public ReadPathFixture, public ::testing::WithParamInterface<Where> {
 protected:
  uint64_t Expiry() const { return static_cast<uint64_t>(NowMs() + 100'000); }

  // The canonical state as cold holds it once flushed.
  std::vector<ops::WriteOp> Canonical(const uint64_t& expiry) {
    return {ops::StringSet{.key = "s", .value = "v", .abs_ttl_ms = expiry},
            ops::SetAdd{.key = "set", .members = {"a", "b"}},
            ops::ZsetAdd{.key = "z",
                         .entries = {{.score = 1, .member = "m"}, {.score = 2, .member = "n"}}},
            ops::HashSet{.key = "h",
                         .fields = {{.field = "f", .value = "1"}, {.field = "g", .value = "2"}}}};
  }

  void Place(Where where) {
    const uint64_t expiry = Expiry();
    const std::string pxat = std::to_string(expiry);
    switch (where) {
      case Where::kHotHit:
        Write({"SET", "s", "v", "PXAT", pxat});
        Write({"SADD", "set", "a", "b"});
        Write({"ZADD", "z", "1", "m", "2", "n"});
        Write({"HSET", "h", "f", "1", "g", "2"});
        break;
      case Where::kExpiredResident: {
        // Cold's older values must never answer for an expired entry.
        Cold(Canonical(expiry));
        const std::string soon = std::to_string(NowMs() + 1000);
        Write({"SET", "s", "v", "PXAT", soon});
        for (const auto* key : {"set", "z", "h"}) Write({"PEXPIREAT", key, soon});
        clock_.Advance(2s);
        break;
      }
      case Where::kFlushFloor:
        Cold(Canonical(expiry));
        drained_ = 0;
        for (core::ShardId shard = 0; shard < kShards; ++shard) {
          ASSERT_TRUE(hot_.Wipe(shard, 10).has_value());
        }
        break;
      case Where::kStub:
        Cold(Canonical(expiry));
        Stub("s", hot::Entry::Type::kString, static_cast<int64_t>(expiry));
        Stub("set", hot::Entry::Type::kSet, 0);
        Stub("z", hot::Entry::Type::kZset, 0);
        Stub("h", hot::Entry::Type::kHash, 0);
        break;
      case Where::kBufferOnly:
        for (const auto& op : Canonical(expiry)) Buffer(ops::PrimaryKey(op), op);
        break;
      case Where::kColdOnly:
      case Where::kWrongType:
        Cold(Canonical(expiry));
        break;
      case Where::kDeltaOverCold:
        Cold(
            {ops::StringSet{.key = "s", .value = "old"},
             ops::SetAdd{.key = "set", .members = {"a", "x"}},
             ops::ZsetAdd{.key = "z",
                          .entries = {{.score = 5, .member = "m"}, {.score = 9, .member = "x"}}},
             ops::HashSet{.key = "h",
                          .fields = {{.field = "f", .value = "9"}, {.field = "x", .value = "0"}}}});
        Buffer("s", ops::StringSet{.key = "s", .value = "v", .abs_ttl_ms = expiry});
        Buffer("set", ops::SetRem{.key = "set", .members = {"x"}});
        Buffer("set", ops::SetAdd{.key = "set", .members = {"b"}});
        Buffer("z",
               ops::ZsetAdd{.key = "z",
                            .entries = {{.score = 1, .member = "m"}, {.score = 2, .member = "n"}}});
        Buffer("z", ops::ZsetRem{.key = "z", .members = {"x"}});
        Buffer("h", ops::HashSet{
                        .key = "h",
                        .fields = {{.field = "f", .value = "1"}, {.field = "g", .value = "2"}}});
        Buffer("h", ops::HashDel{.key = "h", .fields = {"x"}});
        break;
      case Where::kAbsent:
        break;
    }
  }

  void Stub(std::string_view key, hot::Entry::Type type, int64_t abs_ttl_ms) {
    const auto start = hot_.BeginLoad(key);
    ASSERT_TRUE(start.started());
    ASSERT_TRUE(hot_.CompleteLoad(key, start.token,
                                  hot::LoadedExists{.type = type, .abs_ttl_ms = abs_ttl_ms}));
  }
};

TEST_P(ReadPathTableTest, EveryReadCommandAnswersAsHotWould) {
  const Where where = GetParam();
  ASSERT_NO_FATAL_FAILURE(Place(where));
  const bool present = where == Where::kHotHit || where == Where::kStub ||
                       where == Where::kBufferOnly || where == Where::kColdOnly ||
                       where == Where::kDeltaOverCold;
  const int reads_before = cold_.Reads();
  for (const Command& command : Commands()) {
    const bool meta = command.wrong_type.empty();
    if (where == Where::kWrongType && meta) continue;
    const auto& args = where == Where::kWrongType ? command.wrong_type : command.args;
    const int before = cold_.Reads();
    const auto reply = Read(args);
    const std::string got = command.pairs ? DescribePairs(reply) : Describe(reply, command.sorted);
    std::string want = present ? command.present : command.absent;
    if (where == Where::kWrongType) want = "-WRONGTYPE";
    EXPECT_EQ(got, want) << NameOf(where) << ": " << args.front() << " " << args.at(1);
    if (where == Where::kStub && meta) {
      EXPECT_EQ(cold_.Reads(), before) << "a stub answers " << args.front();
    }
  }
  if (where == Where::kHotHit || where == Where::kExpiredResident || where == Where::kFlushFloor) {
    EXPECT_EQ(cold_.Reads(), reads_before) << NameOf(where) << " read cold";
  }
  EXPECT_EQ(router_.drain_waits.load(), 0) << "a read waited for cold to drain";
}

INSTANTIATE_TEST_SUITE_P(Places, ReadPathTableTest,
                         ::testing::Values(Where::kHotHit, Where::kExpiredResident,
                                           Where::kFlushFloor, Where::kStub, Where::kBufferOnly,
                                           Where::kColdOnly, Where::kDeltaOverCold, Where::kAbsent,
                                           Where::kWrongType),
                         [](const ::testing::TestParamInfo<Where>& info) {
                           return std::string(NameOf(info.param));
                         });

class ReadPathTest : public ReadPathFixture {};

// A delta's removals over a cold collection: the old buffer scalars
// answered from the delta alone.
TEST_F(ReadPathTest, MemberAndCardinalityReadsMergeTheDeltaOverCold) {
  Cold({ops::SetAdd{.key = "set", .members = {"a", "b"}},
        ops::ZsetAdd{.key = "z",
                     .entries = {{.score = 1, .member = "a"}, {.score = 2, .member = "b"}}},
        ops::HashSet{.key = "h",
                     .fields = {{.field = "a", .value = "1"}, {.field = "b", .value = "2"}}}});
  Buffer("set", ops::SetRem{.key = "set", .members = {"b"}});
  Buffer("set", ops::SetAdd{.key = "set", .members = {"c"}});
  Buffer("z", ops::ZsetRem{.key = "z", .members = {"b"}});
  Buffer("z", ops::ZsetAdd{.key = "z", .entries = {{.score = 3, .member = "c"}}});
  Buffer("h", ops::HashDel{.key = "h", .fields = {"b"}});
  Buffer("h", ops::HashSet{.key = "h", .fields = {{.field = "c", .value = "3"}}});

  EXPECT_EQ(Describe(Read({"SISMEMBER", "set", "b"})), ":0");
  EXPECT_EQ(Describe(Read({"SISMEMBER", "set", "a"})), ":1");
  EXPECT_EQ(Describe(Read({"SCARD", "set"})), ":2");
  EXPECT_EQ(Describe(Read({"SMEMBERS", "set"}), true), "[$a,$c]");
  EXPECT_EQ(Describe(Read({"ZSCORE", "z", "b"})), "nil");
  EXPECT_EQ(Describe(Read({"ZSCORE", "z", "a"})), "$1");
  EXPECT_EQ(Describe(Read({"ZCARD", "z"})), ":2");
  EXPECT_EQ(Describe(Read({"ZRANGE", "z", "0", "-1"})), "[$a,$c]");
  EXPECT_EQ(Describe(Read({"HEXISTS", "h", "b"})), ":0");
  EXPECT_EQ(Describe(Read({"HGET", "h", "a"})), "$1");
  EXPECT_EQ(Describe(Read({"HLEN", "h"})), ":2");
  EXPECT_EQ(DescribePairs(Read({"HGETALL", "h"})), "{a=1,c=3}");
}

// A string's TTL change waits in the buffer while cold holds the value.
TEST_F(ReadPathTest, ATtlOnlyDeltaOverColdAnswersEveryStringAndMetaRead) {
  const int64_t expiry = NowMs() + 1000;
  Cold({ops::StringSet{.key = "k", .value = "v"}});
  Buffer("k", ops::Expire{.key = "k", .abs_ttl_ms = static_cast<uint64_t>(expiry)});

  EXPECT_EQ(Describe(Read({"PTTL", "k"})), ":1000");
  EXPECT_EQ(Describe(Read({"TTL", "k"})), ":1");
  clock_.Advance(2s);
  EXPECT_EQ(Describe(Read({"GET", "k"})), "nil");
  EXPECT_EQ(Describe(Read({"TTL", "k"})), ":-2");
  EXPECT_EQ(Describe(Read({"PTTL", "k"})), ":-2");
  EXPECT_EQ(Describe(Read({"EXISTS", "k"})), ":0");
  EXPECT_EQ(Describe(Read({"TYPE", "k"})), "+none");
}

TEST_F(ReadPathTest, APersistOverAnExpiredColdValueKeepsIt) {
  Cold(
      {ops::StringSet{.key = "k", .value = "v", .abs_ttl_ms = static_cast<uint64_t>(NowMs() - 1)}});
  Buffer("k", ops::Persist{.key = "k"});

  EXPECT_EQ(Describe(Read({"GET", "k"})), "$v");
  EXPECT_EQ(Describe(Read({"TTL", "k"})), ":-1");
  EXPECT_EQ(Describe(Read({"PTTL", "k"})), ":-1");
  EXPECT_EQ(Describe(Read({"EXISTS", "k"})), ":1");
  EXPECT_EQ(Describe(Read({"TYPE", "k"})), "+string");
}

TEST_F(ReadPathTest, AMissNeverWaitsForColdToDrain) {
  // Nothing has drained: under the old path a miss waited here.
  drained_ = 0;
  Cold({ops::StringSet{.key = "k", .value = "v"}});
  const auto start = core::SteadyClock::now();
  EXPECT_EQ(Describe(Read({"GET", "k"})), "$v");
  const auto elapsed = core::SteadyClock::now() - start;
  EXPECT_EQ(router_.drain_waits.load(), 0);
  EXPECT_LE(cold_.LastBudget(), kPointDeadline) << "a point read's deadline";
  EXPECT_LT(elapsed, 1s);
}

double Fills(metrics::FillOutcome outcome) {
  return metrics::testing::GetCounterValue(metrics::names::kHotFillsTotal, outcome).value_or(0);
}

TEST_F(ReadPathTest, AColdHitFillsHotAndTheNextReadIsAHotHit) {
  const double installed = Fills(metrics::FillOutcome::kInstalled);
  Cold({ops::StringSet{.key = "k", .value = "v"}, ops::SetAdd{.key = "set", .members = {"a"}}});
  EXPECT_EQ(Describe(Read({"GET", "k"})), "$v");
  EXPECT_TRUE(Resident("k"));
  EXPECT_EQ(Describe(Read({"SMEMBERS", "set"})), "[$a]");
  EXPECT_TRUE(Resident("set"));

  const int reads = cold_.Reads();
  EXPECT_EQ(Describe(Read({"GET", "k"})), "$v");
  EXPECT_EQ(Describe(Read({"SCARD", "set"})), ":1");
  EXPECT_EQ(cold_.Reads(), reads) << "hot answered";
  EXPECT_EQ(Fills(metrics::FillOutcome::kInstalled), installed + 2);
}

// Waits, bounded, until `n` calls have joined another's load.
bool JoinedBy(const Loader& loader, uint64_t n) {
  const auto until = core::SteadyClock::now() + 10s;
  while (loader.JoinsForTesting() < n) {
    if (core::SteadyClock::now() >= until) return false;
    std::this_thread::sleep_for(1ms);
  }
  return true;
}

// The default fill policy, with deadlines that outlast a held load.
class ReadPathDefaultTest : public ReadPathFixture {
 protected:
  ReadPathDefaultTest()
      : ReadPathFixture(ReadPathConfig{.cold_read_deadline = 10s, .cold_scan_deadline = 10s}) {}
};

// The doorkeeper admits none of them on a first touch, or only some:
// every reader still shares the one load.
TEST_F(ReadPathDefaultTest, ConcurrentScansOfOneColdSetLoadItOnce) {
  constexpr int kReaders = 8;
  Cold({ops::SetAdd{.key = "set", .members = {"a", "b"}}});
  cold_.HoldLoads();
  std::vector<std::future<std::string>> scans;
  scans.reserve(kReaders);
  const abyss::testing::OnExit release([this] { cold_.release.Open(); });
  for (int i = 0; i < kReaders; ++i) {
    scans.push_back(std::async(std::launch::async,
                               [this] { return Describe(Read({"SMEMBERS", "set"}), true); }));
  }
  ASSERT_TRUE(cold_.entered.Wait());
  // Every other reader joins that load in flight.
  ASSERT_TRUE(JoinedBy(loader_, kReaders - 1)) << loader_.JoinsForTesting() << " joined";
  cold_.release.Open();
  for (auto& scan : scans) {
    ASSERT_EQ(scan.wait_for(10s), std::future_status::ready);
    EXPECT_EQ(scan.get(), "[$a,$b]");
  }
  EXPECT_EQ(cold_.loads.load(), 1);
}

// A miss after a write that drained and was evicted never joins a load
// that began before the write: it would fill hot with pre-write state.
TEST_F(ReadPathTest, AMissAfterADrainedWriteStartsItsOwnLoad) {
  drained_ = 0;
  Cold({ops::StringSet{.key = "k", .value = "v1"}});
  const core::ShardId shard = hot_.ShardOf("k");
  cold_.HoldLoads();
  const abyss::testing::OnExit release([this] { cold_.release.Open(); });
  auto stale = std::async(std::launch::async, [&] {
    return loader_.LoadAs(shard, "k", core::KeyType::kString, core::SteadyClock::now() + 10s);
  });
  // Held in its cold read, past its buffer snapshot.
  ASSERT_TRUE(cold_.entered.Wait());

  Write({"SET", "k", "v2"});
  Buffer("k", ops::StringSet{.key = "k", .value = "v2"});
  drained_ = hot::kAllDrained;
  clock_.Advance(48h);
  ASSERT_GE(hot_.EvictExpired(clock_.SteadyNow()).Total(), 1U);
  ASSERT_FALSE(Resident("k"));

  const uint64_t joins = loader_.JoinsForTesting();
  auto read = std::async(std::launch::async, [this] { return Describe(Read({"GET", "k"})); });
  ASSERT_EQ(read.wait_for(10s), std::future_status::ready) << "joined the stale load";
  EXPECT_EQ(read.get(), "$v2") << "through Install";
  auto loaded = loader_.LoadAs(shard, "k", core::KeyType::kString, core::SteadyClock::now() + 1s);
  ASSERT_TRUE(loaded.has_value());
  const auto* full = std::get_if<hot::LoadedFull>(loaded.value().get());
  ASSERT_NE(full, nullptr);
  EXPECT_EQ(std::get<std::string>(full->value), "v2") << "through LoadAs";
  EXPECT_EQ(loader_.JoinsForTesting(), joins);

  cold_.release.Open();
  ASSERT_EQ(stale.wait_for(10s), std::future_status::ready);
  EXPECT_TRUE(stale.get().has_value());
  const auto hot = hot_.Read(ops::ReadOp{ops::StringGet{.key = "k"}});
  ASSERT_TRUE(hot.result.has_value());
  EXPECT_EQ(Describe(hot.result), "$v2") << "pre-write state was made resident";
}

// A write's placeholder is a miss that buffer and cold answer: however
// long the write's full load takes, a read never waits on it, nor
// fills over it.
TEST_F(ReadPathTest, AReadNeverWaitsOnAWritesLoad) {
  Cold({ops::StringSet{.key = "k", .value = "v"},
        ops::HashSet{.key = "h",
                     .fields = {{.field = "f", .value = "1"}, {.field = "g", .value = "2"}}}});
  Buffer("h", ops::HashSet{.key = "h", .fields = {{.field = "f", .value = "9"}}});
  const auto fills = [] {
    using metrics::FillOutcome;
    double total = 0;
    for (const FillOutcome outcome :
         {FillOutcome::kInstalled, FillOutcome::kDiscarded, FillOutcome::kSkippedBackpressure,
          FillOutcome::kSkippedSize, FillOutcome::kSkippedEvictCap, FillOutcome::kFailed}) {
      total += Fills(outcome);
    }
    return total;
  };
  const double filled = fills();
  const auto write = [this](std::vector<std::string> args) {
    return std::async(std::launch::async, [this, args = std::move(args)]() mutable {
      return sequencer_.Execute(core::RespCommand{.args = std::move(args)},
                                core::PredicateFlags::kNone);
    });
  };
  cold_.HoldFullLoads();
  std::future<core::Result<core::RespValue>> persist;
  std::future<core::Result<core::RespValue>> hsetnx;
  const abyss::testing::OnExit release([this] { cold_.release.Open(); });
  persist = write({"PERSIST", "k"});
  hsetnx = write({"HSETNX", "h", "x", "1"});
  const auto until = core::SteadyClock::now() + 10s;
  while (cold_.loads.load() < 2) {
    ASSERT_LT(core::SteadyClock::now(), until) << "the writes never began their loads";
    std::this_thread::sleep_for(1ms);
  }
  ASSERT_TRUE(hot_.LoadPending("k") && hot_.LoadPending("h"));

  EXPECT_EQ(Describe(Read({"GET", "k"})), "$v");
  EXPECT_LE(cold_.LastBudget(), kPointDeadline) << "a point read's deadline";
  EXPECT_EQ(DescribePairs(Read({"HGETALL", "h"})), "{f=9,g=2}");
  EXPECT_EQ(fills(), filled);
  EXPECT_EQ(loader_.JoinsForTesting(), 0U);
  EXPECT_TRUE(hot_.LoadPending("k"));
  EXPECT_TRUE(hot_.LoadPending("h"));

  cold_.release.Open();
  ASSERT_EQ(persist.wait_for(10s), std::future_status::ready);
  ASSERT_EQ(hsetnx.wait_for(10s), std::future_status::ready);
  EXPECT_EQ(Describe(persist.get()), ":0");
  EXPECT_EQ(Describe(hsetnx.get()), ":1");
  EXPECT_EQ(DescribePairs(Read({"HGETALL", "h"})), "{f=9,g=2,x=1}");
}

TEST_F(ReadPathTest, NoFillPastTheBackpressureLimit) {
  const double skipped = Fills(metrics::FillOutcome::kSkippedBackpressure);
  Cold({ops::StringSet{.key = "k", .value = "v"}});
  // Undrained, so nothing on the shard can be evicted to make room.
  drained_ = 0;
  const core::ShardId shard = hot_.ShardOf("k");
  const std::string big(size_t{1} << 20, 'x');
  core::SequenceId seq = 0;
  for (int i = 0; hot_.EvictShardToTarget(shard); ++i) {
    ASSERT_LT(i, 100'000) << "the shard never went over its limit";
    const std::string key = "filler" + std::to_string(i);
    if (hot_.ShardOf(key) != shard) continue;
    const auto applied = hot_.Apply(ops::WriteOp{ops::StringSet{.key = key, .value = big}}, ++seq);
    EXPECT_TRUE(applied.has_value() ||
                applied.error().code() == core::ErrorCode::kResourceExhausted);
  }
  EXPECT_EQ(Describe(Read({"GET", "k"})), "$v");
  EXPECT_FALSE(Resident("k")) << "filled past the backpressure limit";
  EXPECT_FALSE(hot_.LoadPending("k"));
  EXPECT_EQ(Fills(metrics::FillOutcome::kSkippedBackpressure), skipped + 1);
}

// A shard holds 16 MiB here, so a fill may take 1 MiB.
TEST_F(ReadPathTest, AKeyOverItsShareOfTheBudgetIsNeverFilled) {
  std::vector<std::string> members;
  members.reserve(40'000);
  for (int i = 0; i < 40'000; ++i) members.push_back("member-" + std::to_string(i));
  Cold({ops::SetAdd{.key = "big", .members = {members.begin(), members.end()}}});
  const double skipped = Fills(metrics::FillOutcome::kSkippedSize);
  for (int i = 0; i < 2; ++i) {
    EXPECT_EQ(Describe(Read({"SCARD", "big"})), ":40000");
    const auto all = Read({"SMEMBERS", "big"});
    ASSERT_TRUE(all.has_value());
    EXPECT_EQ(all->AsArray().size(), 40'000U);
    EXPECT_FALSE(Resident("big"));
  }
  EXPECT_EQ(Fills(metrics::FillOutcome::kSkippedSize), skipped + 2);
}

// At 1M members, BM_ColdCardinality in loader_bench; writing them here
// would take most of a test tier's budget.
TEST_F(ReadPathTest, ACardinalityOfALargeColdSetReadsNoMember) {
  constexpr int kMembers = 100'000;
  constexpr int kBatch = 50'000;
  for (int start = 0; start < kMembers; start += kBatch) {
    std::vector<std::string> members;
    members.reserve(kBatch);
    for (int i = start; i < start + kBatch; ++i) members.push_back("m" + std::to_string(i));
    Cold({ops::SetAdd{.key = "big", .members = {members.begin(), members.end()}}});
  }
  rocksdb::SetPerfLevel(rocksdb::PerfLevel::kEnableCount);
  rocksdb::get_perf_context()->Reset();
  const auto card = Read({"SCARD", "big"});
  const uint64_t nexts = rocksdb::get_perf_context()->iter_next_count;
  const uint64_t seeks = rocksdb::get_perf_context()->iter_seek_count;
  rocksdb::SetPerfLevel(rocksdb::PerfLevel::kDisable);

  EXPECT_EQ(Describe(card), ":100000") << "within the point read's deadline";
  EXPECT_LE(cold_.LastBudget(), kPointDeadline);
  EXPECT_EQ(nexts, 0U) << "SCARD read the set's members";
  EXPECT_EQ(seeks, 0U);
  EXPECT_EQ(cold_.loads.load(), 0);
  EXPECT_FALSE(Resident("big"));
}

// Adds of members cold holds and removals of members it lacks change
// nothing; the rest change the count by one each.
TEST_F(ReadPathDefaultTest, ACardinalityCorrectsColdsCountByTheDelta) {
  Cold({ops::SetAdd{.key = "set", .members = {"a", "b", "c"}},
        ops::SetAdd{.key = "moved", .members = {"a", "b", "c"}},
        ops::ZsetAdd{.key = "z",
                     .entries = {{.score = 1, .member = "a"}, {.score = 2, .member = "b"}}},
        ops::HashSet{.key = "h",
                     .fields = {{.field = "f", .value = "1"}, {.field = "g", .value = "2"}}}});
  Buffer("set", ops::SetAdd{.key = "set", .members = {"a", "b"}});
  Buffer("set", ops::SetRem{.key = "set", .members = {"x", "y"}});
  Buffer("moved", ops::SetAdd{.key = "moved", .members = {"d", "e"}});
  Buffer("moved", ops::SetRem{.key = "moved", .members = {"a"}});
  Buffer("z", ops::ZsetAdd{.key = "z", .entries = {{.score = 5, .member = "a"}}});
  Buffer("z", ops::ZsetRem{.key = "z", .members = {"x"}});
  Buffer("h", ops::HashSet{.key = "h", .fields = {{.field = "f", .value = "9"}}});
  Buffer("h", ops::HashDel{.key = "h", .fields = {"x"}});

  EXPECT_EQ(Describe(Read({"SCARD", "set"})), ":3");
  EXPECT_EQ(Describe(Read({"SCARD", "moved"})), ":4");
  EXPECT_EQ(Describe(Read({"ZCARD", "z"})), ":2");
  EXPECT_EQ(Describe(Read({"HLEN", "h"})), ":2");
  EXPECT_EQ(cold_.member_batches.load(), 4) << "one batch of the delta's members each";
  EXPECT_EQ(cold_.loads.load(), 0) << "a full load counted, or a first touch filled";
}

TEST_F(ReadPathTest, AGetOfALargeColdSetIsWrongTypeAndReadsNoMembers) {
  std::vector<std::string> members;
  members.reserve(20'000);
  for (int i = 0; i < 20'000; ++i) members.push_back("member-" + std::to_string(i));
  Cold({ops::SetAdd{.key = "big", .members = {members.begin(), members.end()}}});

  rocksdb::SetPerfLevel(rocksdb::PerfLevel::kEnableCount);
  rocksdb::get_perf_context()->Reset();
  const auto reply = Read({"GET", "big"});
  const uint64_t nexts = rocksdb::get_perf_context()->iter_next_count;
  rocksdb::SetPerfLevel(rocksdb::PerfLevel::kDisable);

  EXPECT_EQ(Describe(reply), "-WRONGTYPE");
  EXPECT_EQ(nexts, 0U) << "the GET read the set's members";
  EXPECT_FALSE(Resident("big"));
}

TEST_F(ReadPathTest, AKeyWrittenAfterAFlushIsResidentToAFill) {
  drained_ = 0;
  Cold({ops::StringSet{.key = "k", .value = "pre-flush"}});
  ASSERT_TRUE(sequencer_.Flush().has_value());
  Write({"SET", "k", "post"});
  auto filled = loader_.Install("k", core::KeyType::kString, core::SteadyClock::now() + 1s);
  ASSERT_TRUE(filled.has_value());
  EXPECT_EQ(filled->fill, Loader::Fill::kResident);
  EXPECT_EQ(Describe(Read({"GET", "k"})), "$post");
}

TEST_F(ReadPathTest, HmgetReadsItsColdFieldsInOneBatch) {
  Cold({ops::HashSet{.key = "h",
                     .fields = {{.field = "a", .value = "1"},
                                {.field = "b", .value = "2"},
                                {.field = "c", .value = "3"}}}});
  Buffer("h", ops::HashSet{.key = "h", .fields = {{.field = "b", .value = "20"}}});
  EXPECT_EQ(Describe(Read({"HMGET", "h", "a", "b", "c", "d"})), "[$1,$20,$3,nil]");
  EXPECT_EQ(cold_.member_batches.load(), 1);
}

class ReadPathDoorkeeperTest : public ReadPathFixture {
 protected:
  ReadPathDoorkeeperTest()
      : ReadPathFixture(ReadPathConfig{.cold_read_deadline = kPointDeadline,
                                       .cold_scan_deadline = kScanDeadline}) {}
};

TEST_F(ReadPathDoorkeeperTest, AKeyFillsOnItsSecondMiss) {
  Cold({ops::StringSet{.key = "k", .value = "v"}});
  EXPECT_EQ(Describe(Read({"GET", "k"})), "$v");
  EXPECT_FALSE(Resident("k")) << "a one-hit wonder filled hot";
  EXPECT_EQ(Describe(Read({"GET", "k"})), "$v");
  EXPECT_TRUE(Resident("k"));
}

TEST_F(ReadPathDoorkeeperTest, AScanOfMissingKeysLeavesNoNegativeEntries) {
  for (int i = 0; i < 500; ++i) {
    EXPECT_EQ(Describe(Read({"GET", "missing" + std::to_string(i)})), "nil");
  }
  EXPECT_EQ(hot_.Stats()->negative_entries, 0U);
  EXPECT_EQ(Describe(Read({"GET", "missing7"})), "nil");
  EXPECT_EQ(hot_.Stats()->negative_entries, 1U) << "the second miss is cached";
}

TEST_F(ReadPathTest, NegativeEntriesAreBoundedByTheirOwnCap) {
  const size_t cap = hot::ShardedHotStoreConfig{}.negative_max_entries;
  for (size_t i = 0; i < cap + 1000; ++i) {
    EXPECT_EQ(Describe(Read({"GET", "missing" + std::to_string(i)})), "nil");
  }
  const auto stats = hot_.Stats();
  ASSERT_TRUE(stats.has_value());
  EXPECT_GT(stats->negative_entries, 0U);
  EXPECT_LE(stats->negative_entries, cap);
}

}  // namespace
}  // namespace abyss::engine

#endif  // ABYSS_HAVE_ROCKSDB
