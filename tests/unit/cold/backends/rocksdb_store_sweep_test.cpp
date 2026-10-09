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
#include <unordered_map>
#include <vector>

#include "abyss/cold/backends/rocksdb_store.h"
#include "abyss/cold/format/key_codec.h"
#include "abyss/cold/ttl_scanner.h"
#include "abyss/core/ops.h"
#include "abyss/core/result.h"
#include "abyss/core/shard_router.h"
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

  std::unique_ptr<RocksdbStore> OpenStore(uint32_t shard_count = kFixtureShardCount) {
    RocksdbConfig config;
    config.data_path = path_.string();
    config.shard_count = shard_count;
    config.wall_clock = clock_.Fn();
    config.log_clock = [this](core::ShardId shard) { return LogClockMs(shard); };
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

  uint64_t LogClockMs(core::ShardId shard) const {
    const auto it = shard_log_ms_.find(shard);
    return it == shard_log_ms_.end() ? log_ms_.load() : it->second;
  }

  // NOLINTBEGIN(cppcoreguidelines-non-private-member-variables-in-classes)
  TestClock clock_;
  // Every shard's log clock unless shard_log_ms_ names it. Level with the
  // wall clock's start, so keys written already past their TTL sweep.
  std::atomic<uint64_t> log_ms_{1'000'000};
  std::unordered_map<core::ShardId, uint64_t> shard_log_ms_;
  std::filesystem::path path_;
  std::vector<std::string> keys_;
  // NOLINTEND(cppcoreguidelines-non-private-member-variables-in-classes)
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

// Several shards, so samples must land in every one: a seek past the
// last shard would wrap to the first key and starve the rest.
TEST_F(SweepFixture, RepeatedSweepsRemoveAllExpired) {
  auto store = OpenStore(4);
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
  EXPECT_EQ(total_deleted, 100U);
  EXPECT_EQ(store->RecordsForTesting().size(), 101U) << "the live keys and the format version";

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

// --- The log clock ----------------------------------------------------------

// The wall clock runs far past the TTL first; only the log clock, once
// it reaches the TTL, lets the scanner delete.
TEST_F(SweepFixture, DeletesOnlyOnceItsShardsLogClockReachesTheTtl) {
  auto store = OpenStore();
  const uint64_t ttl = clock_.Now() + 100;
  core::ops::WriteOp op = core::ops::StringSet{.key = "k", .value = "v", .abs_ttl_ms = ttl};
  ASSERT_TRUE(store->ApplyBatch(std::span{&op, 1}, 0).has_value());
  log_ms_ = ttl - 1;
  clock_.Advance(3'600'000);

  for (int i = 0; i < 5; ++i) {
    auto r = store->RunScannerTickForTesting();
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(r->deleted_strings, 0U);
  }
  EXPECT_EQ(store->RecordsForTesting().size(), 2U) << "k was deleted early";

  log_ms_ = ttl;
  size_t deleted = 0;
  for (int i = 0; i < 5 && deleted == 0; ++i) {
    auto r = store->RunScannerTickForTesting();
    ASSERT_TRUE(r.has_value());
    deleted += r->deleted_strings;
  }
  EXPECT_EQ(deleted, 1U);
  EXPECT_EQ(store->RecordsForTesting().size(), 1U) << "only the format version is left";
}

// The first key whose shard lies within [lo, hi) of 65536.
std::string KeyOnShardIn(uint32_t lo, uint32_t hi, uint32_t shard_count) {
  for (int i = 0;; ++i) {
    std::string key = "key:" + std::to_string(i);
    const auto shard = core::ComputeShard(key, shard_count);
    if (shard >= lo && shard < hi) return key;
  }
}

// The scanner's random seeks spread over the shard prefix, so with
// every shard slot in use both keys are sampled many times a tick.
TEST_F(SweepFixture, AKeyOnAShardWithALowerLogClockStays) {
  constexpr uint32_t kShards = 65536;
  auto store = OpenStore(kShards);
  const std::string ahead = KeyOnShardIn(12'000, 20'000, kShards);
  const std::string behind = KeyOnShardIn(45'000, 53'000, kShards);
  const uint64_t ttl = clock_.Now() + 100;
  std::vector<core::ops::WriteOp> ops = {
      core::ops::StringSet{.key = ahead, .value = "v", .abs_ttl_ms = ttl},
      core::ops::StringSet{.key = behind, .value = "v", .abs_ttl_ms = ttl},
  };
  ASSERT_TRUE(store->ApplyBatch(ops, 0).has_value());
  log_ms_ = 0;
  shard_log_ms_[core::ComputeShard(ahead, kShards)] = ttl;
  shard_log_ms_[core::ComputeShard(behind, kShards)] = ttl - 1;
  clock_.Advance(3'600'000);

  size_t with_ttl = 0;
  size_t expired = 0;
  size_t deleted = 0;
  for (int i = 0; i < 20; ++i) {
    auto r = store->RunScannerTickForTesting();
    ASSERT_TRUE(r.has_value());
    with_ttl += r->with_ttl_strings;
    expired += r->expired_strings;
    deleted += r->deleted_strings;
  }
  EXPECT_EQ(deleted, 1U);
  EXPECT_EQ(expired, 1U) << "the key behind was judged expired";
  EXPECT_GT(with_ttl, 100U) << "the key behind was hardly sampled";
  auto loaded = store->LoadKey(behind, core::SteadyClock::now() + std::chrono::seconds(5));
  ASSERT_TRUE(loaded.has_value() && loaded->has_value()) << "the shard behind lost its key";
  EXPECT_EQ(store->RecordsForTesting().size(), 2U) << "the format version and the key behind";
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
