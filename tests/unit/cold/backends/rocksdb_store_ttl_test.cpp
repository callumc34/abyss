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
#include <string>
#include <system_error>
#include <thread>
#include <vector>

#include "abyss/cold/backends/rocksdb_store.h"
#include "abyss/cold/format/key_codec.h"
#include "abyss/core/ops.h"
#include "abyss/core/resp_types.h"
#include "abyss/core/result.h"
#include "abyss/core/types.h"

namespace abyss::cold::backends {
namespace {

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
    config.wall_clock = clock_.Fn();
    auto store = RocksdbStore::Create(config);
    EXPECT_TRUE(store.has_value()) << (store.has_value() ? "" : store.error().message());
    return std::move(*store);
  }

  TestClock clock_;
  std::filesystem::path path_;
};

// --- String TTL -------------------------------------------------------------

TEST_F(TtlFixture, StringWithFutureTtlReturnsValue) {
  auto store = OpenStore();
  std::string k = "k";
  std::string v = "v";
  core::ops::WriteOp op =
      core::ops::StringSet{.key = k, .value = v, .abs_ttl_ms = clock_.Now() + 60'000};
  ASSERT_TRUE(store->ApplyBatch(std::span{&op, 1}).has_value());

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
  ASSERT_TRUE(store->ApplyBatch(std::span{&op, 1}).has_value());

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
  ASSERT_TRUE(store->ApplyBatch(std::span{&op, 1}).has_value());

  clock_.SetTo((std::numeric_limits<uint64_t>::max)() / 2);
  auto r = store->Exec(core::ops::StringGet{.key = k});
  ASSERT_TRUE(r.has_value());
  EXPECT_EQ(r->AsString(), "v");
}

TEST_F(TtlFixture, StringExpiryDeletesBackingRecord) {
  auto store = OpenStore();
  std::string k = "k";
  std::string v = "v";
  core::ops::WriteOp op =
      core::ops::StringSet{.key = k, .value = v, .abs_ttl_ms = clock_.Now() + 100};
  ASSERT_TRUE(store->ApplyBatch(std::span{&op, 1}).has_value());

  clock_.Advance(200);

  auto r1 = store->Exec(core::ops::StringGet{.key = k});
  EXPECT_TRUE(r1->IsNull());

  // Second read without an intervening write must also be Null and must not
  // re-surface the value — the first read should have deleted the record.
  auto r2 = store->Exec(core::ops::StringGet{.key = k});
  EXPECT_TRUE(r2->IsNull());

  // A DEL after lazy expiry must report 0 — the key is already gone.
  core::ops::Del del;
  del.keys = {"k"};
  auto dr = store->ExecDel(del);
  ASSERT_TRUE(dr.has_value());
  EXPECT_EQ(dr->AsInteger(), 0);
}

// --- Collection TTL ---------------------------------------------------------

// No EXPIRE-family op exists yet, so collection TTL is exercised by opening
// the raw RocksDB handle and rewriting the meta record with `kFlagHasTtl` and
// a past timestamp. This simulates what would happen if an expiry op landed
// and aged past `now`.
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

  const auto meta_key = ::abyss::cold::format::EncodeMetaKey(inner_type, key);
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
  ASSERT_TRUE(store->ApplyBatch(ops).has_value());

  clock_.Advance(1'000'000'000);
  auto card = store->Exec(core::ops::SetCard{.key = "s"});
  EXPECT_EQ(card->AsInteger(), 1);
}

TEST_F(TtlFixture, SetReadOnExpiredCollectionReturnsEmptyAndPurges) {
  std::string key = "s";
  std::vector<std::string> members = {"a", "b"};
  std::vector<std::string_view> views(members.begin(), members.end());
  std::vector<core::ops::WriteOp> ops = {core::ops::SetAdd{.key = key, .members = views}};
  {
    auto store = OpenStore();
    ASSERT_TRUE(store->ApplyBatch(ops).has_value());
  }

  InjectExpiredMeta(path_.string(), format::kTypeSetMember, key, members.size());

  auto store = OpenStore();
  EXPECT_EQ(store->Exec(core::ops::SetCard{.key = key})->AsInteger(), 0);
  EXPECT_TRUE(store->Exec(core::ops::SetMembers{.key = key})->AsArray().empty());
  EXPECT_EQ(store->Exec(core::ops::SetIsMember{.key = key, .member = "a"})->AsInteger(), 0);

  // After the first read lazily purged the collection, a follow-up SADD must
  // start from a clean slate rather than inherit the expired meta's TTL or
  // cardinality.
  std::vector<std::string> fresh = {"c"};
  std::vector<std::string_view> fresh_views(fresh.begin(), fresh.end());
  std::vector<core::ops::WriteOp> add = {core::ops::SetAdd{.key = key, .members = fresh_views}};
  ASSERT_TRUE(store->ApplyBatch(add).has_value());
  EXPECT_EQ(store->Exec(core::ops::SetCard{.key = key})->AsInteger(), 1);
  EXPECT_EQ(store->Exec(core::ops::SetIsMember{.key = key, .member = "c"})->AsInteger(), 1);
  EXPECT_EQ(store->Exec(core::ops::SetIsMember{.key = key, .member = "a"})->AsInteger(), 0);
}

TEST_F(TtlFixture, WriteOnExpiredCollectionPurgesOldMembersFirst) {
  std::string key = "h";
  std::vector<core::ops::HashSet::FieldValue> fvs = {{.field = "old", .value = "v"}};
  std::vector<core::ops::WriteOp> ops = {core::ops::HashSet{.key = key, .fields = fvs}};
  {
    auto store = OpenStore();
    ASSERT_TRUE(store->ApplyBatch(ops).has_value());
  }

  InjectExpiredMeta(path_.string(), format::kTypeHashField, key, 1);

  auto store = OpenStore();
  // Write into the expired key — the purge must happen atomically with the
  // new add, so the caller never observes a mixed state.
  std::vector<core::ops::HashSet::FieldValue> new_fvs = {{.field = "new", .value = "v"}};
  std::vector<core::ops::WriteOp> add = {core::ops::HashSet{.key = key, .fields = new_fvs}};
  ASSERT_TRUE(store->ApplyBatch(add).has_value());

  auto all = store->Exec(core::ops::HashGetAll{.key = key});
  ASSERT_EQ(all->AsArray().size(), 2U);
  EXPECT_EQ(all->AsArray()[0].AsString(), "new");
  EXPECT_TRUE(store->Exec(core::ops::HashGet{.key = key, .field = "old"})->IsNull());
}

TEST_F(TtlFixture, RemOnExpiredCollectionIsNoop) {
  std::string key = "z";
  std::vector<core::ops::ZsetAdd::Entry> entries = {{.score = 1.0, .member = "m"}};
  std::vector<core::ops::WriteOp> ops = {core::ops::ZsetAdd{.key = key, .entries = entries}};
  {
    auto store = OpenStore();
    ASSERT_TRUE(store->ApplyBatch(ops).has_value());
  }

  InjectExpiredMeta(path_.string(), format::kTypeZsetMember, key, 1);

  auto store = OpenStore();
  std::vector<std::string_view> rem = {"m"};
  std::vector<core::ops::WriteOp> rem_ops = {core::ops::ZsetRem{.key = key, .members = rem}};
  ASSERT_TRUE(store->ApplyBatch(rem_ops).has_value());

  // After the no-op REM, the collection must be fully purged — no score index
  // orphans, no stale member records.
  EXPECT_EQ(store->Exec(core::ops::ZsetCard{.key = key})->AsInteger(), 0);
  auto range =
      store->Exec(core::ops::ZsetRange{.key = key, .min = "-inf", .max = "+inf", .by_score = true});
  EXPECT_TRUE(range->AsArray().empty());
}

TEST_F(TtlFixture, ExistsDoesNotCountExpiredCollection) {
  std::string key = "s";
  std::vector<std::string> members = {"a"};
  std::vector<std::string_view> views(members.begin(), members.end());
  std::vector<core::ops::WriteOp> ops = {core::ops::SetAdd{.key = key, .members = views}};
  {
    auto store = OpenStore();
    ASSERT_TRUE(store->ApplyBatch(ops).has_value());
  }
  InjectExpiredMeta(path_.string(), format::kTypeSetMember, key, 1);

  auto store = OpenStore();
  core::ops::Exists op;
  op.keys = {key};
  EXPECT_EQ(store->Exec(op)->AsInteger(), 0);
}

// --- Concurrent lazy expiry -------------------------------------------------

TEST_F(TtlFixture, ConcurrentReadsOnExpiredKeyAreSafe) {
  auto store = OpenStore();
  std::string k = "k";
  std::string v = "v";
  core::ops::WriteOp op =
      core::ops::StringSet{.key = k, .value = v, .abs_ttl_ms = clock_.Now() + 10};
  ASSERT_TRUE(store->ApplyBatch(std::span{&op, 1}).has_value());

  clock_.Advance(1'000);

  // Fan several threads onto the same expired key. Each must return Null;
  // lazy-delete is idempotent so no corruption is possible.
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
}

}  // namespace
}  // namespace abyss::cold::backends
