#pragma once

#ifndef ABYSS_HAVE_ROCKSDB
#error \
    "abyss/cold/backends/rocksdb_store.h requires ABYSS_HAVE_ROCKSDB — build with -DABYSS_WITH_ROCKSDB=ON"
#endif

#include <cstdint>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>

#include "abyss/cold/ttl_scanner.h"
#include "abyss/core/cold_store.h"
#include "abyss/core/types.h"

namespace abyss::cold::backends {

enum class CompactionStyle : std::uint8_t {
  kLevel,
  kUniversal,
};

struct RocksdbConfig {
  std::string data_path;
  // Must equal the hot/consumer shard count.
  uint32_t shard_count = 1;
  size_t write_buffer_size_bytes = 67108864;
  uint32_t max_write_buffer_number = 4;
  uint32_t bloom_filter_bits_per_key = 10;
  CompactionStyle compaction_style = CompactionStyle::kLevel;
  core::WallClockFn wall_clock = core::DefaultWallClock;
  TtlScanner::Config ttl_scanner;
  std::optional<TtlScanner::Hooks> ttl_scanner_hooks;
  std::optional<TtlScanner::ExecutionMode> ttl_scanner_mode;
};

// RocksDB-backed cold store.
class RocksdbStore : public core::ColdStore {
 public:
  static core::Result<std::unique_ptr<RocksdbStore>> Create(RocksdbConfig config);

  ~RocksdbStore() override;

  RocksdbStore(const RocksdbStore&) = delete;
  RocksdbStore& operator=(const RocksdbStore&) = delete;
  RocksdbStore(RocksdbStore&&) = delete;
  RocksdbStore& operator=(RocksdbStore&&) = delete;

  core::Result<core::RespValue> Exec(
      const core::ops::ReadOp& op, std::optional<core::Duration> deadline = std::nullopt) override;
  core::Result<void> ApplyBatch(std::span<const core::ops::WriteOp> ops) override;
  core::Result<void> Wipe(core::ShardId shard) override;
  core::Result<core::StorageStats> Stats() override;
  core::Result<void> Compact() override;
  core::Result<std::optional<core::RespCommand>> GetPromotionCommand(std::string_view key) override;

  // DEL returns the count of keys that existed before deletion.
  core::Result<core::RespValue> ExecDel(const core::ops::Del& op);

  core::Result<void> Start() override;
  core::Result<void> Stop() override;

  core::Result<SweepReport> RunScannerTickForTesting();
  TtlScanner::Snapshot ScannerSnapshot() const;

 private:
  struct Impl;
  explicit RocksdbStore(std::unique_ptr<Impl> impl);

  std::unique_ptr<Impl> impl_;
};

}  // namespace abyss::cold::backends
