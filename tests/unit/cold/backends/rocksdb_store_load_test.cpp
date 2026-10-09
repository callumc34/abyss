#include <gtest/gtest.h>
#include <rocksdb/perf_context.h>
#include <rocksdb/perf_level.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "abyss/cold/backends/rocksdb_store.h"
#include "abyss/core/cold_store.h"
#include "abyss/core/ops.h"
#include "abyss/core/result.h"
#include "abyss/core/types.h"
#include "temp_dir.h"
#include "test_clock.h"

namespace abyss::cold::backends {
namespace {

using namespace std::chrono_literals;
using core::KeyType;
namespace ops = core::ops;

// A wall clock past every TTL these tests set.
constexpr int64_t kTtlMs = 1'000'000;

class RocksdbLoadTest : public ::testing::Test {
 protected:
  RocksdbLoadTest() {
    clock_.SetWall(core::WallTime{std::chrono::milliseconds{kTtlMs * 2}});
    RocksdbConfig config{.data_path = dir_.String(),
                         .wall_clock = clock_.WallFn(),
                         .steady_clock = [this] { return Steady(); }};
    auto created = RocksdbStore::Create(std::move(config));
    EXPECT_TRUE(created.has_value()) << (created.has_value() ? "" : created.error().message());
    if (created.has_value()) store_ = std::move(*created);
  }

  core::SteadyTime Steady() {
    ++steady_reads_;
    return steady_reads_ > steady_jump_after_ ? clock_.SteadyNow() + 1h : clock_.SteadyNow();
  }
  core::SteadyTime Later() const { return clock_.SteadyNow() + 10s; }

  void Apply(std::vector<ops::WriteOp> batch) {
    ASSERT_TRUE(store_->ApplyBatch(batch, 0).has_value());
  }

  core::ColdKeyState MustLoad(std::string_view key) {
    auto loaded = store_->LoadKey(key, Later());
    EXPECT_TRUE(loaded.has_value() && loaded->has_value()) << key;
    if (!loaded.has_value() || !loaded->has_value()) return {};
    return **loaded;
  }

  // NOLINTBEGIN(cppcoreguidelines-non-private-member-variables-in-classes)
  abyss::testing::TempDir dir_{"rocksdb_load"};
  abyss::testing::TestClock clock_;
  std::atomic<uint64_t> steady_reads_{0};
  uint64_t steady_jump_after_ = UINT64_MAX;
  std::unique_ptr<RocksdbStore> store_;
  // NOLINTEND(cppcoreguidelines-non-private-member-variables-in-classes)
};

TEST_F(RocksdbLoadTest, LoadKeyRoundTripsEachTypeWithItsTtl) {
  Apply({ops::StringSet{.key = "str", .value = "v", .abs_ttl_ms = kTtlMs},
         ops::StringSet{.key = "plain", .value = ""},
         ops::SetAdd{.key = "set", .members = {"a", "b"}},
         ops::Expire{.key = "set", .abs_ttl_ms = kTtlMs + 1},
         ops::HashSet{.key = "hash", .fields = {{.field = "f", .value = "1"}}},
         ops::ZsetAdd{.key = "zset",
                      .entries = {{.score = 1.5, .member = "m"}, {.score = -2, .member = "n"}}},
         ops::Expire{.key = "zset", .abs_ttl_ms = kTtlMs + 2}});

  EXPECT_EQ(MustLoad("str"),
            (core::ColdKeyState{.type = KeyType::kString, .value = "v", .abs_ttl_ms = kTtlMs}));
  EXPECT_EQ(MustLoad("plain"), (core::ColdKeyState{.type = KeyType::kString, .value = ""}));
  EXPECT_EQ(MustLoad("set"), (core::ColdKeyState{.type = KeyType::kSet,
                                                 .value = std::unordered_set<std::string>{"a", "b"},
                                                 .abs_ttl_ms = kTtlMs + 1}));
  EXPECT_EQ(
      MustLoad("hash"),
      (core::ColdKeyState{.type = KeyType::kHash,
                          .value = std::unordered_map<std::string, std::string>{{"f", "1"}}}));
  EXPECT_EQ(
      MustLoad("zset"),
      (core::ColdKeyState{.type = KeyType::kZset,
                          .value = std::unordered_map<std::string, double>{{"m", 1.5}, {"n", -2}},
                          .abs_ttl_ms = kTtlMs + 2}));

  auto absent = store_->LoadKey("absent", Later());
  ASSERT_TRUE(absent.has_value());
  EXPECT_FALSE(absent->has_value());
}

TEST_F(RocksdbLoadTest, AnExpiredKeyLoadsAsItIsAndStays) {
  Apply({ops::SetAdd{.key = "set", .members = {"a"}},
         ops::Expire{.key = "set", .abs_ttl_ms = kTtlMs}});
  // The wall clock is past the TTL: the loads judge none of it.
  EXPECT_EQ(MustLoad("set").abs_ttl_ms, kTtlMs);
  auto probe = store_->ProbeKey("set", Later());
  ASSERT_TRUE(probe.has_value());
  EXPECT_EQ(probe->value_or(core::KeyMeta{}).abs_ttl_ms, kTtlMs) << "the load deleted nothing";
}

TEST_F(RocksdbLoadTest, ProbeKeyGivesTypeTtlAndCardinality) {
  Apply({ops::StringSet{.key = "str", .value = "v", .abs_ttl_ms = kTtlMs},
         ops::SetAdd{.key = "set", .members = {"a", "b", "c"}},
         ops::HashSet{.key = "hash", .fields = {{.field = "f", .value = "1"}}},
         ops::ZsetAdd{.key = "zset", .entries = {{.score = 1, .member = "m"}}},
         ops::Persist{.key = "zset"}});
  const auto probe = [&](std::string_view key) {
    auto meta = store_->ProbeKey(key, Later());
    EXPECT_TRUE(meta.has_value());
    return meta.has_value() ? *meta : std::nullopt;
  };
  EXPECT_EQ(probe("str"),
            (core::KeyMeta{.type = KeyType::kString, .abs_ttl_ms = kTtlMs, .cardinality = 1}));
  EXPECT_EQ(probe("set"), (core::KeyMeta{.type = KeyType::kSet, .cardinality = 3}));
  EXPECT_EQ(probe("hash"), (core::KeyMeta{.type = KeyType::kHash, .cardinality = 1}));
  EXPECT_EQ(probe("zset"), (core::KeyMeta{.type = KeyType::kZset, .cardinality = 1}));
  EXPECT_EQ(probe("absent"), std::nullopt);
}

TEST_F(RocksdbLoadTest, AProbeReadsNoMembers) {
  std::vector<std::string> members;
  members.reserve(2000);
  for (int i = 0; i < 2000; ++i) members.push_back("member-" + std::to_string(i));
  Apply({ops::SetAdd{.key = "big", .members = {members.begin(), members.end()}}});

  // RocksDB's per-thread counters see every record the store reads.
  rocksdb::SetPerfLevel(rocksdb::PerfLevel::kEnableCount);
  rocksdb::PerfContext& counters = *rocksdb::get_perf_context();
  counters.Reset();
  auto meta = store_->ProbeKey("big", Later());
  const uint64_t probe_seeks = counters.iter_seek_count;
  const uint64_t probe_nexts = counters.iter_next_count;
  const uint64_t probe_iter_bytes = counters.iter_read_bytes;
  counters.Reset();
  auto loaded = store_->LoadKey("big", Later());
  const uint64_t load_nexts = counters.iter_next_count;
  rocksdb::SetPerfLevel(rocksdb::PerfLevel::kDisable);

  ASSERT_TRUE(meta.has_value());
  EXPECT_EQ(meta->value_or(core::KeyMeta{}).cardinality, 2000U);
  EXPECT_EQ(probe_seeks, 0U);
  EXPECT_EQ(probe_nexts, 0U);
  EXPECT_EQ(probe_iter_bytes, 0U);
  ASSERT_TRUE(loaded.has_value());
  EXPECT_GE(load_nexts, 2000U) << "the counters see a full load's scan";
}

TEST_F(RocksdbLoadTest, LoadMemberReadsOneRecord) {
  Apply({ops::SetAdd{.key = "set", .members = {"a"}},
         ops::HashSet{.key = "hash", .fields = {{.field = "f", .value = "1"}}},
         ops::ZsetAdd{.key = "zset", .entries = {{.score = 2.5, .member = "m"}}},
         ops::StringSet{.key = "str", .value = "v"}});
  const auto member = [&](std::string_view key, KeyType type, std::string_view name) {
    auto value = store_->LoadMember(key, type, name, Later());
    EXPECT_TRUE(value.has_value());
    return value.has_value() ? *value : std::nullopt;
  };
  EXPECT_EQ(member("set", KeyType::kSet, "a"), core::MemberValue{});
  EXPECT_EQ(member("set", KeyType::kSet, "b"), std::nullopt);
  EXPECT_EQ(member("hash", KeyType::kHash, "f"), core::MemberValue{std::string("1")});
  EXPECT_EQ(member("zset", KeyType::kZset, "m"), core::MemberValue{2.5});
  EXPECT_EQ(member("set", KeyType::kZset, "a"), std::nullopt) << "another type's records";

  auto of_string = store_->LoadMember("str", KeyType::kString, "v", Later());
  ASSERT_FALSE(of_string.has_value());
  EXPECT_EQ(of_string.error().code(), core::ErrorCode::kInvalidArgument);
}

TEST_F(RocksdbLoadTest, APassedDeadlineIsATimeout) {
  Apply({ops::SetAdd{.key = "set", .members = {"a"}}});
  const auto past = clock_.SteadyNow() - 1ms;
  auto load = store_->LoadKey("set", past);
  auto probe = store_->ProbeKey("set", past);
  auto member = store_->LoadMember("set", KeyType::kSet, "a", past);
  ASSERT_FALSE(load.has_value());
  ASSERT_FALSE(probe.has_value());
  ASSERT_FALSE(member.has_value());
  EXPECT_EQ(load.error().code(), core::ErrorCode::kTimeout);
  EXPECT_EQ(probe.error().code(), core::ErrorCode::kTimeout);
  EXPECT_EQ(member.error().code(), core::ErrorCode::kTimeout);
}

TEST_F(RocksdbLoadTest, ADeadlinePassingMidScanIsATimeout) {
  std::vector<std::string> members;
  members.reserve(5000);
  for (int i = 0; i < 5000; ++i) members.push_back("m" + std::to_string(i));
  Apply({ops::SetAdd{.key = "big", .members = {members.begin(), members.end()}}});
  // The up-front check passes; every check during the scan is late.
  steady_reads_ = 0;
  steady_jump_after_ = 1;
  auto load = store_->LoadKey("big", Later());
  ASSERT_FALSE(load.has_value());
  EXPECT_EQ(load.error().code(), core::ErrorCode::kTimeout);
}

}  // namespace
}  // namespace abyss::cold::backends
