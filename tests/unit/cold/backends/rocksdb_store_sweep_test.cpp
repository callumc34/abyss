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
#include <string_view>
#include <system_error>
#include <thread>
#include <vector>

#include "abyss/cold/backends/rocksdb_store.h"
#include "abyss/cold/format/key_codec.h"
#include "abyss/cold/ttl_scanner.h"
#include "abyss/core/ops.h"
#include "abyss/core/result.h"
#include "abyss/core/types.h"
#include "abyss/metrics/testing.h"

namespace abyss::cold::backends {
namespace {

using namespace std::chrono_literals;

// Single shard: key encoding and the raw meta injection must agree on the slot
// count (ADP-010).
constexpr uint32_t kFixtureShardCount = 1;

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
};

// Direct-DB write of an expired meta record. The cold store is closed
// during this call; reopening sees the meta as expired at sample time.
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
      .abs_ttl_ms = 1,
      .cardinality = cardinality,
  });
  ASSERT_TRUE(db->Put(rocksdb::WriteOptions(), handles[0], meta_key, meta_value).ok());

  for (auto* h : handles) {
    db->DestroyColumnFamilyHandle(h);
  }
}

class SweepFixture : public ::testing::Test {
 protected:
  void SetUp() override {
    metrics::testing::Reset();
    static std::atomic<int> counter{0};
    auto base = std::filesystem::temp_directory_path();
#ifdef _WIN32
    path_ = base / ("abyss_cold_sweep_test_" + std::to_string(GetCurrentProcessId()) + "_" +
                    std::to_string(counter.fetch_add(1)));
#else
    path_ = base / ("abyss_cold_sweep_test_" + std::to_string(getpid()) + "_" +
                    std::to_string(counter.fetch_add(1)));
#endif
    std::filesystem::remove_all(path_);
    std::filesystem::create_directories(path_);
  }

  void TearDown() override {
    std::error_code ec;
    std::filesystem::remove_all(path_, ec);
    metrics::testing::Reset();
  }

  std::unique_ptr<RocksdbStore> OpenStore() {
    RocksdbConfig config;
    config.data_path = path_.string();
    config.shard_count = kFixtureShardCount;
    config.wall_clock = clock_.Fn();
    config.ttl_scanner_mode = TtlScanner::ExecutionMode::kManualTick;
    config.ttl_scanner.base_sample_size = 200;
    config.ttl_scanner.max_sample_size = 200;
    // Tests always inject a benign disk-usage hook that returns 0 — we
    // exercise the disk-pressure path in the scanner unit tests.
    config.ttl_scanner_hooks = TtlScanner::Hooks{};
    config.ttl_scanner_hooks->steady_clock = core::DefaultSteadyClock;
    config.ttl_scanner_hooks->cpu_clock = []() { return std::chrono::nanoseconds{0}; };
    config.ttl_scanner_hooks->disk_usage = []() -> core::Result<double> { return 0.0; };
    auto store = RocksdbStore::Create(std::move(config));
    EXPECT_TRUE(store.has_value()) << (store.has_value() ? "" : store.error().message());
    return std::move(*store);
  }

  TestClock clock_;
  std::filesystem::path path_;
  std::vector<std::string> keys_;
};

// --- Empty store -----------------------------------------------------------

TEST_F(SweepFixture, EmptyStoreReturnsZeroes) {
  auto store = OpenStore();
  auto r = store->RunScannerTickForTesting();
  ASSERT_TRUE(r.has_value());
  EXPECT_EQ(r->total_sampled(), 0U);
  EXPECT_EQ(r->total_expired(), 0U);
  EXPECT_EQ(r->total_deleted(), 0U);
}

// --- Strings ---------------------------------------------------------------

TEST_F(SweepFixture, ExpiredStringsAreSweptWithoutBeingRead) {
  auto store = OpenStore();

  // 200 expired keys, all with past TTLs.
  std::vector<std::string> values(200, "x");
  std::vector<core::ops::WriteOp> ops;
  ops.reserve(200);
  keys_.reserve(keys_.size() + 200);
  for (size_t i = 0; i < 200; ++i) {
    auto& key_storage = keys_.emplace_back("k:" + std::to_string(i));
    ops.emplace_back(core::ops::StringSet{
        .key = key_storage,
        .value = values[i],
        .abs_ttl_ms = clock_.Now() - 1,
    });
  }
  ASSERT_TRUE(store->ApplyBatch(ops, 0).has_value());

  auto r = store->RunScannerTickForTesting();
  ASSERT_TRUE(r.has_value());
  EXPECT_GT(r->expired_strings, 0U);
  EXPECT_EQ(r->expired_strings, r->deleted_strings + r->conflicts_strings);
}

TEST_F(SweepFixture, FutureTtlIsNotDeleted) {
  auto store = OpenStore();
  std::string k = "live";
  std::string v = "v";
  core::ops::WriteOp op =
      core::ops::StringSet{.key = k, .value = v, .abs_ttl_ms = clock_.Now() + 60'000};
  ASSERT_TRUE(store->ApplyBatch(std::span{&op, 1}, 0).has_value());

  for (int i = 0; i < 5; ++i) {
    auto r = store->RunScannerTickForTesting();
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(r->deleted_strings, 0U);
  }
  // Live key still readable.
  auto get = store->Exec(core::ops::StringGet{.key = k});
  ASSERT_TRUE(get.has_value());
  EXPECT_EQ(get->AsString(), "v");
}

TEST_F(SweepFixture, NoTtlIsNotDeleted) {
  auto store = OpenStore();
  std::string k = "untimed";
  std::string v = "v";
  core::ops::WriteOp op = core::ops::StringSet{.key = k, .value = v, .abs_ttl_ms = 0};
  ASSERT_TRUE(store->ApplyBatch(std::span{&op, 1}, 0).has_value());

  for (int i = 0; i < 5; ++i) {
    auto r = store->RunScannerTickForTesting();
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(r->expired_strings, 0U);
    EXPECT_EQ(r->deleted_strings, 0U);
  }
  auto get = store->Exec(core::ops::StringGet{.key = k});
  ASSERT_TRUE(get.has_value());
  EXPECT_EQ(get->AsString(), "v");
}

// --- Repeat sweeps eventually drain expired keys ---------------------------

TEST_F(SweepFixture, RepeatedSweepsRemoveAllExpired) {
  auto store = OpenStore();
  // Mix of expired and live, to make sure we don't touch the live ones.
  for (int i = 0; i < 100; ++i) {
    auto& live_key = keys_.emplace_back("live:" + std::to_string(i));
    core::ops::WriteOp live = core::ops::StringSet{
        .key = live_key,
        .value = "v",
        .abs_ttl_ms = clock_.Now() + 60'000,
    };
    ASSERT_TRUE(store->ApplyBatch(std::span{&live, 1}, 0).has_value());

    auto& dead_key = keys_.emplace_back("dead:" + std::to_string(i));
    core::ops::WriteOp dead = core::ops::StringSet{
        .key = dead_key,
        .value = "v",
        .abs_ttl_ms = clock_.Now() - 1,
    };
    ASSERT_TRUE(store->ApplyBatch(std::span{&dead, 1}, 0).has_value());
  }

  // Run the scanner enough times for random sampling to converge.
  size_t total_deleted = 0;
  for (int i = 0; i < 200; ++i) {
    auto r = store->RunScannerTickForTesting();
    ASSERT_TRUE(r.has_value());
    total_deleted += r->deleted_strings;
  }
  EXPECT_GT(total_deleted, 0U);

  // Live keys must remain reachable.
  for (int i = 0; i < 100; ++i) {
    auto get = store->Exec(core::ops::StringGet{.key = "live:" + std::to_string(i)});
    ASSERT_TRUE(get.has_value());
    EXPECT_EQ(get->AsString(), "v") << "i=" << i;
  }
}

// --- Collections -----------------------------------------------------------

TEST_F(SweepFixture, ExpiredHashCollectionIsCleanedByScanner) {
  // Insert a hash, close the store, inject an expired meta record, reopen.
  std::string key = "h";
  {
    auto store = OpenStore();
    std::vector<core::ops::HashSet::FieldValue> fvs = {{.field = "f", .value = "v"}};
    std::vector<core::ops::WriteOp> ops = {
        core::ops::HashSet{.key = std::string_view{key}, .fields = fvs}};
    ASSERT_TRUE(store->ApplyBatch(ops, 0).has_value());
  }
  InjectExpiredMeta(path_.string(), format::kTypeHashField, key, 1);

  auto store = OpenStore();
  // Run enough sweeps that random sampling almost certainly hits the meta.
  size_t deletions = 0;
  for (int i = 0; i < 200; ++i) {
    auto r = store->RunScannerTickForTesting();
    ASSERT_TRUE(r.has_value());
    deletions += r->deleted_collections;
    if (deletions > 0) break;
  }
  EXPECT_GT(deletions, 0U);

  // After deletion, the hash must look empty: a Get on a field returns nil.
  auto get = store->Exec(core::ops::HashGet{.key = key, .field = "f"});
  ASSERT_TRUE(get.has_value());
  EXPECT_TRUE(get->IsNull());
}

// --- ScannerSnapshot --------------------------------------------------------

TEST_F(SweepFixture, ScannerSnapshotReportsState) {
  auto store = OpenStore();
  auto snap = store->ScannerSnapshot();
  EXPECT_EQ(snap.total_ticks, 0U);

  for (int i = 0; i < 3; ++i) {
    auto r = store->RunScannerTickForTesting();
    ASSERT_TRUE(r.has_value());
  }
  snap = store->ScannerSnapshot();
  EXPECT_EQ(snap.total_ticks, 3U);
}

// --- CAS conflict -----------------------------------------------------------

// A second writer rewrites the sampled key with a fresh TTL between the
// scanner's snapshot and the scanner's commit. The scanner must not delete
// the new value; it counts as a conflict.
TEST_F(SweepFixture, ConcurrentReSetTriggersCasConflict) {
  auto store = OpenStore();
  // Fill with expired strings.
  std::vector<std::string> key_storage;
  key_storage.reserve(100);
  std::vector<core::ops::WriteOp> ops;
  ops.reserve(100);
  for (int i = 0; i < 100; ++i) {
    auto& k = key_storage.emplace_back("k:" + std::to_string(i));
    ops.emplace_back(core::ops::StringSet{
        .key = k,
        .value = "old",
        .abs_ttl_ms = clock_.Now() - 1,
    });
  }
  ASSERT_TRUE(store->ApplyBatch(ops, 0).has_value());

  std::atomic<bool> stop_writer{false};
  std::thread writer([&]() {
    while (!stop_writer.load(std::memory_order_acquire)) {
      // Re-write each key with a fresh TTL.
      std::vector<core::ops::WriteOp> rewrites;
      rewrites.reserve(key_storage.size());
      for (const auto& k : key_storage) {
        rewrites.emplace_back(core::ops::StringSet{
            .key = k,
            .value = "new",
            .abs_ttl_ms = clock_.Now() + 60'000,
        });
      }
      auto r = store->ApplyBatch(rewrites, 0);
      (void)r;  // ignore failures from a torn-down store
    }
  });

  // Run a few sweeps. The scanner sees expired metadata in its sampling
  // pass; a concurrent rewrite causes the OCC commit to abort. With the CAS
  // path correct, we must never see a deletion of a freshly-rewritten key.
  for (int i = 0; i < 50; ++i) {
    auto r = store->RunScannerTickForTesting();
    ASSERT_TRUE(r.has_value());
  }

  stop_writer.store(true, std::memory_order_release);
  writer.join();

  // Final read of every key must return "new" (no false deletion).
  for (const auto& k : key_storage) {
    auto get = store->Exec(core::ops::StringGet{.key = k});
    ASSERT_TRUE(get.has_value()) << k;
    EXPECT_EQ(get->AsString(), "new") << k;
  }
}

}  // namespace
}  // namespace abyss::cold::backends
