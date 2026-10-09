#include <gtest/gtest.h>
#include <rocksdb/db.h>
#include <rocksdb/options.h>

#ifdef _WIN32
#include <windows.h>
#else
#include <unistd.h>
#endif

#include <atomic>
#include <chrono>
#include <filesystem>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
#include <vector>

#include "abyss/cold/backends/rocksdb_store.h"
#include "abyss/cold/format/key_codec.h"
#include "abyss/core/cold_store.h"
#include "abyss/core/ops.h"
#include "abyss/core/resp_types.h"
#include "abyss/core/result.h"
#include "abyss/core/string_hash.h"
#include "abyss/core/types.h"
#include "abyss/metrics/names.h"
#include "abyss/metrics/testing.h"

namespace abyss::cold::backends {
namespace {

// Single shard: the fixture stores one logical shard, so key encoding and the
// raw meta injection below must agree on this slot count (ADP-010).
constexpr uint32_t kFixtureShardCount = 1;

// Test clock backed by a shared atomic so the fixture can advance time while
// the store holds a WallClockFn capturing the same atomic by reference.
struct TestClock {
  std::shared_ptr<std::atomic<uint64_t>> now_ms =
      std::make_shared<std::atomic<uint64_t>>(1'000'000);

  core::WallClockFn Fn() const {
    auto ptr = now_ms;
    return [ptr]() {
      return core::WallTime{std::chrono::milliseconds(ptr->load(std::memory_order_relaxed))};
    };
  }

  uint64_t Now() const { return now_ms->load(std::memory_order_relaxed); }
  void Advance(uint64_t delta_ms) const { now_ms->fetch_add(delta_ms, std::memory_order_relaxed); }
  void SetTo(uint64_t ms) const { now_ms->store(ms, std::memory_order_relaxed); }
};

class TtlFixture : public ::testing::Test {
 protected:
  void SetUp() override {
    static std::atomic<int> counter{0};
    auto base = std::filesystem::temp_directory_path();
#ifdef _WIN32
    path_ = base / ("abyss_cold_ttl_test_" + std::to_string(GetCurrentProcessId()) + "_" +
                    std::to_string(counter.fetch_add(1)));
#else
    path_ = base / ("abyss_cold_ttl_test_" + std::to_string(getpid()) + "_" +
                    std::to_string(counter.fetch_add(1)));
#endif
    std::filesystem::remove_all(path_);
    std::filesystem::create_directories(path_);
  }

  void TearDown() override {
    std::error_code ec;
    std::filesystem::remove_all(path_, ec);
  }

  std::unique_ptr<RocksdbStore> OpenStore() {
    RocksdbConfig config;
    config.data_path = path_.string();
    config.shard_count = kFixtureShardCount;
    config.wall_clock = clock_.Fn();
    config.log_clock = [clock = log_clock_ms_](core::ShardId) { return clock->load(); };
    auto store = RocksdbStore::Create(config);
    EXPECT_TRUE(store.has_value()) << (store.has_value() ? "" : store.error().message());
    return std::move(*store);
  }

  // `key`'s state, failing the test when cold does not hold it.
  static core::ColdKeyState MustLoad(RocksdbStore& store, std::string_view key) {
    auto loaded = store.LoadKey(key, core::SteadyClock::now() + std::chrono::seconds(5));
    EXPECT_TRUE(loaded.has_value() && loaded->has_value()) << key << " is missing";
    return loaded.value_or(std::nullopt).value_or(core::ColdKeyState{});
  }

  // NOLINTBEGIN(cppcoreguidelines-non-private-member-variables-in-classes)
  TestClock clock_;
  // Behind the wall clock, as the log's is while a shard is idle.
  std::shared_ptr<std::atomic<uint64_t>> log_clock_ms_ = std::make_shared<std::atomic<uint64_t>>(0);
  std::filesystem::path path_;
  // NOLINTEND(cppcoreguidelines-non-private-member-variables-in-classes)
};

// --- String TTL -------------------------------------------------------------

TEST_F(TtlFixture, StringWithFutureTtlReturnsValue) {
  auto store = OpenStore();
  std::string k = "k";
  std::string v = "v";
  core::ops::WriteOp op =
      core::ops::StringSet{.key = k, .value = v, .abs_ttl_ms = clock_.Now() + 60'000};
  ASSERT_TRUE(store->ApplyBatch(std::span{&op, 1}, 0).has_value());

  auto r = store->Exec(core::ops::StringGet{.key = k});
  ASSERT_TRUE(r.has_value());
  EXPECT_EQ(r->AsString(), "v");
}

TEST_F(TtlFixture, StringWithPastTtlReturnsNull) {
  auto store = OpenStore();
  std::string k = "k";
  std::string v = "v";
  core::ops::WriteOp op =
      core::ops::StringSet{.key = k, .value = v, .abs_ttl_ms = clock_.Now() - 1};
  ASSERT_TRUE(store->ApplyBatch(std::span{&op, 1}, 0).has_value());

  auto r = store->Exec(core::ops::StringGet{.key = k});
  ASSERT_TRUE(r.has_value());
  EXPECT_TRUE(r->IsNull());
}

TEST_F(TtlFixture, ZeroTtlNeverExpires) {
  auto store = OpenStore();
  std::string k = "k";
  std::string v = "v";
  // abs_ttl_ms=0 with flag-clear means "no TTL" regardless of clock.
  core::ops::WriteOp op = core::ops::StringSet{.key = k, .value = v, .abs_ttl_ms = 0};
  ASSERT_TRUE(store->ApplyBatch(std::span{&op, 1}, 0).has_value());

  // Avoid overflow issues
  clock_.SetTo(1'000'000'000'000ULL);
  auto r = store->Exec(core::ops::StringGet{.key = k});
  ASSERT_TRUE(r.has_value());
  EXPECT_EQ(r->AsString(), "v");
}

// Reads judge expiry by the wall clock but never write: only the TTL
// scanner, by the log clock, deletes.
TEST_F(TtlFixture, ReadOfAWallExpiredStringLeavesItsRecord) {
  abyss::metrics::testing::Reset();
  auto store = OpenStore();
  const auto baseline =
      metrics::testing::GetCounterValue(metrics::names::kTtlExpiredTotal, metrics::Tier::kCold)
          .value_or(0.0);

  core::ops::WriteOp op = core::ops::StringSet{
      .key = "k",
      .value = "v",
      .abs_ttl_ms = clock_.Now() + 100,
  };
  ASSERT_TRUE(store->ApplyBatch(std::span{&op, 1}, 0).has_value());
  const auto before = store->RecordsForTesting();
  ASSERT_EQ(before.size(), 2U) << "the format version and k";
  clock_.Advance(200);

  for (int i = 0; i < 2; ++i) {
    auto r = store->Exec(core::ops::StringGet{.key = "k"});
    ASSERT_TRUE(r.has_value());
    EXPECT_TRUE(r->IsNull());
  }
  core::ops::Exists exists;
  exists.keys = {"k"};
  EXPECT_EQ(store->Exec(exists)->AsInteger(), 0);

  EXPECT_EQ(store->RecordsForTesting(), before);
  EXPECT_EQ(
      metrics::testing::GetCounterValue(metrics::names::kTtlExpiredTotal, metrics::Tier::kCold)
          .value_or(0.0),
      baseline);
}

// The apply path judges no TTL: a PERSIST logged while the key was live
// lands even once the wall clock has passed the TTL.
TEST_F(TtlFixture, PersistOfAKeyPastItsTtlOnlyByTheWallClockClearsIt) {
  auto store = OpenStore();
  const uint64_t ttl = clock_.Now() + 100;
  core::ops::WriteOp set = core::ops::StringSet{.key = "k", .value = "v", .abs_ttl_ms = ttl};
  ASSERT_TRUE(store->ApplyBatch(std::span{&set, 1}, 0).has_value());
  log_clock_ms_->store(ttl - 1);
  clock_.Advance(10'000);
  EXPECT_TRUE(store->Exec(core::ops::StringGet{.key = "k"})->IsNull());

  core::ops::WriteOp persist = core::ops::Persist{.key = "k"};
  ASSERT_TRUE(store->ApplyBatch(std::span{&persist, 1}, 0).has_value());

  const auto k = MustLoad(*store, "k");
  EXPECT_EQ(k.value, core::ColdValue{std::string("v")});
  EXPECT_EQ(k.abs_ttl_ms, 0);
  EXPECT_EQ(store->Exec(core::ops::StringGet{.key = "k"})->AsString(), "v");
}

// --- Collection TTL ---------------------------------------------------------

// Rewrites the meta record with `kFlagHasTtl` and a timestamp long past
// under any clock, through the raw RocksDB handle while the store is shut.
void InjectExpiredMeta(const std::string& path, uint8_t inner_type, std::string_view key,
                       uint64_t cardinality) {
  rocksdb::Options opts;
  opts.create_if_missing = false;
  std::vector<std::string> cfs;
  ASSERT_TRUE(rocksdb::DB::ListColumnFamilies(opts, path, &cfs).ok());
  std::vector<rocksdb::ColumnFamilyDescriptor> descs;
  descs.reserve(cfs.size());
  for (const auto& name : cfs) {
    descs.emplace_back(name, rocksdb::ColumnFamilyOptions());
  }
  std::vector<rocksdb::ColumnFamilyHandle*> handles;
  std::unique_ptr<rocksdb::DB> db;
  rocksdb::DBOptions db_opts;
  ASSERT_TRUE(rocksdb::DB::Open(db_opts, path, descs, &handles, &db).ok());

  const auto meta_key = ::abyss::cold::format::EncodeMetaKey(inner_type, key, kFixtureShardCount);
  const auto meta_value = ::abyss::cold::format::EncodeMetaValue({
      .flags = ::abyss::cold::format::kFlagHasTtl,
      .abs_ttl_ms = 1,  // millisecond 1 after epoch — deeply expired under any clock
      .cardinality = cardinality,
  });
  ASSERT_TRUE(db->Put(rocksdb::WriteOptions(), handles[0], meta_key, meta_value).ok());

  for (auto* h : handles) {
    db->DestroyColumnFamilyHandle(h);
  }
}

TEST_F(TtlFixture, CollectionWithoutTtlSurvivesTimeAdvance) {
  auto store = OpenStore();
  std::vector<std::string> members = {"only"};
  std::vector<std::string_view> views(members.begin(), members.end());
  std::vector<core::ops::WriteOp> ops = {core::ops::SetAdd{.key = "s", .members = views}};
  ASSERT_TRUE(store->ApplyBatch(ops, 0).has_value());

  clock_.Advance(1'000'000'000);
  auto card = store->Exec(core::ops::SetCard{.key = "s"});
  EXPECT_EQ(card->AsInteger(), 1);
}

TEST_F(TtlFixture, ReadsOfAWallExpiredCollectionLeaveItsRecords) {
  abyss::metrics::testing::Reset();
  auto store = OpenStore();
  const auto baseline =
      metrics::testing::GetCounterValue(metrics::names::kTtlExpiredTotal, metrics::Tier::kCold)
          .value_or(0.0);

  std::vector<std::string_view> members = {"a", "b"};
  std::vector<core::ops::WriteOp> ops = {
      core::ops::SetAdd{.key = "s", .members = members},
      core::ops::Expire{.key = "s", .abs_ttl_ms = clock_.Now() + 100},
  };
  ASSERT_TRUE(store->ApplyBatch(ops, 0).has_value());
  const auto before = store->RecordsForTesting();
  ASSERT_EQ(before.size(), 4U) << "the format version, s's meta and two members";
  clock_.Advance(200);

  EXPECT_EQ(store->Exec(core::ops::SetCard{.key = "s"})->AsInteger(), 0);
  EXPECT_TRUE(store->Exec(core::ops::SetMembers{.key = "s"})->AsArray().empty());
  EXPECT_EQ(store->Exec(core::ops::SetIsMember{.key = "s", .member = "a"})->AsInteger(), 0);
  // A hash read of s checks s for another type, and finds it expired.
  EXPECT_TRUE(store->Exec(core::ops::HashGet{.key = "s", .field = "f"})->IsNull());
  core::ops::Exists exists;
  exists.keys = {"s"};
  EXPECT_EQ(store->Exec(exists)->AsInteger(), 0);

  EXPECT_EQ(store->RecordsForTesting(), before);
  EXPECT_EQ(
      metrics::testing::GetCounterValue(metrics::names::kTtlExpiredTotal, metrics::Tier::kCold)
          .value_or(0.0),
      baseline);
}

// An SADD logged while the set was live merges into it, even once the
// wall clock has passed the set's TTL.
TEST_F(TtlFixture, SaddToASetPastItsTtlOnlyByTheWallClockMerges) {
  auto store = OpenStore();
  const uint64_t ttl = clock_.Now() + 100;
  std::vector<std::string_view> old_members = {"a", "b"};
  std::vector<core::ops::WriteOp> seed = {
      core::ops::SetAdd{.key = "s", .members = old_members},
      core::ops::Expire{.key = "s", .abs_ttl_ms = ttl},
  };
  ASSERT_TRUE(store->ApplyBatch(seed, 0).has_value());
  log_clock_ms_->store(ttl - 1);
  clock_.Advance(10'000);

  std::vector<std::string_view> new_members = {"c"};
  core::ops::WriteOp add = core::ops::SetAdd{.key = "s", .members = new_members};
  ASSERT_TRUE(store->ApplyBatch(std::span{&add, 1}, 0).has_value());

  const auto s = MustLoad(*store, "s");
  EXPECT_EQ(s.value, (core::ColdValue{core::StringSet{"a", "b", "c"}}));
  EXPECT_EQ(s.abs_ttl_ms, static_cast<int64_t>(ttl));
}

TEST_F(TtlFixture, PersistOfASetPastItsTtlOnlyByTheWallClockClearsIt) {
  auto store = OpenStore();
  const uint64_t ttl = clock_.Now() + 100;
  std::vector<std::string_view> members = {"a"};
  std::vector<core::ops::WriteOp> seed = {
      core::ops::SetAdd{.key = "s", .members = members},
      core::ops::Expire{.key = "s", .abs_ttl_ms = ttl},
  };
  ASSERT_TRUE(store->ApplyBatch(seed, 0).has_value());
  clock_.Advance(10'000);

  core::ops::WriteOp persist = core::ops::Persist{.key = "s"};
  ASSERT_TRUE(store->ApplyBatch(std::span{&persist, 1}, 0).has_value());
  EXPECT_EQ(store->Exec(core::ops::SetCard{.key = "s"})->AsInteger(), 1);
}

TEST_F(TtlFixture, RemOnAWallExpiredZsetAppliesAsLogged) {
  auto store = OpenStore();
  std::vector<core::ops::ZsetAdd::Entry> entries = {{.score = 1.0, .member = "m"},
                                                    {.score = 2.0, .member = "n"}};
  std::vector<core::ops::WriteOp> seed = {
      core::ops::ZsetAdd{.key = "z", .entries = entries},
      core::ops::Expire{.key = "z", .abs_ttl_ms = clock_.Now() + 100},
  };
  ASSERT_TRUE(store->ApplyBatch(seed, 0).has_value());
  clock_.Advance(10'000);

  std::vector<std::string_view> rem = {"m"};
  core::ops::WriteOp rem_op = core::ops::ZsetRem{.key = "z", .members = rem};
  ASSERT_TRUE(store->ApplyBatch(std::span{&rem_op, 1}, 0).has_value());

  EXPECT_EQ(MustLoad(*store, "z").value, (core::ColdValue{core::StringMap<double>{{"n", 2.0}}}));
  // n's member record and score index entry, z's meta, the format version.
  EXPECT_EQ(store->RecordsForTesting().size(), 4U);
}

// The write path logs a DEL before an add that changes a key's type, so
// an add finding another type, live or expired, is a disagreement: it
// is counted, and the add still applies as logged.
TEST_F(TtlFixture, AnAddOverAnotherTypeIsReportedAndApplied) {
  abyss::metrics::testing::Reset();
  auto store = OpenStore();
  std::vector<core::ops::WriteOp> strings = {
      core::ops::StringSet{.key = "k", .value = "v", .abs_ttl_ms = clock_.Now() - 1},
      core::ops::StringSet{.key = "j", .value = "v"},
  };
  ASSERT_TRUE(store->ApplyBatch(strings, 0).has_value());
  const auto conflicts = [] {
    return metrics::testing::GetCounterValue(metrics::names::kColdApplyTypeConflictsTotal)
        .value_or(0.0);
  };

  std::vector<std::string_view> members = {"a"};
  std::vector<core::ops::WriteOp> retype_j = {
      core::ops::Del{.keys = {"j"}},
      core::ops::SetAdd{.key = "j", .members = members},
  };
  ASSERT_TRUE(store->ApplyBatch(retype_j, 0).has_value());
  EXPECT_EQ(conflicts(), 0.0);

  core::ops::WriteOp add = core::ops::SetAdd{.key = "k", .members = members};
  ASSERT_TRUE(store->ApplyBatch(std::span{&add, 1}, 0).has_value());
  EXPECT_EQ(conflicts(), 1.0);
  EXPECT_EQ(store->Exec(core::ops::SetCard{.key = "k"})->AsInteger(), 1);
  EXPECT_TRUE(store->Exec(core::ops::StringGet{.key = "k"})->IsNull());
}

TEST_F(TtlFixture, ExistsDoesNotCountExpiredCollection) {
  std::string key = "s";
  std::vector<std::string> members = {"a"};
  std::vector<std::string_view> views(members.begin(), members.end());
  std::vector<core::ops::WriteOp> ops = {core::ops::SetAdd{.key = key, .members = views}};
  {
    auto store = OpenStore();
    ASSERT_TRUE(store->ApplyBatch(ops, 0).has_value());
  }
  InjectExpiredMeta(path_.string(), format::kTypeSetMember, key, 1);

  auto store = OpenStore();
  core::ops::Exists op;
  op.keys = {key};
  EXPECT_EQ(store->Exec(op)->AsInteger(), 0);
}

// --- Concurrent reads ---------------------------------------------------

TEST_F(TtlFixture, ConcurrentReadsOfAnExpiredKeyNeverWrite) {
  auto store = OpenStore();
  std::string k = "k";
  std::string v = "v";
  core::ops::WriteOp op =
      core::ops::StringSet{.key = k, .value = v, .abs_ttl_ms = clock_.Now() + 10};
  ASSERT_TRUE(store->ApplyBatch(std::span{&op, 1}, 0).has_value());
  const auto before = store->RecordsForTesting();

  clock_.Advance(1'000);

  constexpr int kThreads = 8;
  std::vector<std::thread> threads;
  std::atomic<int> null_count{0};
  threads.reserve(kThreads);
  for (int i = 0; i < kThreads; ++i) {
    threads.emplace_back([&] {
      for (int j = 0; j < 64; ++j) {
        auto r = store->Exec(core::ops::StringGet{.key = k});
        if (r.has_value() && r->IsNull()) null_count.fetch_add(1, std::memory_order_relaxed);
      }
    });
  }
  for (auto& t : threads) t.join();
  EXPECT_EQ(null_count.load(), kThreads * 64);
  EXPECT_EQ(store->RecordsForTesting(), before);
}

}  // namespace
}  // namespace abyss::cold::backends
