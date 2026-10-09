#pragma once

#ifndef ABYSS_HAVE_ROCKSDB
#error \
    "abyss/cold/backends/rocksdb_store.h requires ABYSS_HAVE_ROCKSDB — build with -DABYSS_WITH_ROCKSDB=ON"
#endif

#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

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
  // Judges reads only: a key expired by it reads as absent.
  core::WallClockFn wall_clock = core::DefaultWallClock;
  // A shard's log clock in ms (ColdConsumerPool::LogClockMs). The TTL
  // scanner deletes a key only once its TTL is at or below its shard's
  // clock. Unset, nothing is deleted.
  std::function<uint64_t(core::ShardId)> log_clock;
  // Judges the loads' deadlines.
  core::SteadyClockFn steady_clock = core::DefaultSteadyClock;
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
  core::Result<void> ApplyBatch(std::span<const core::ops::WriteOp> ops,
                                core::SequenceId highest_wal_seq) override;
  core::Result<void> Checkpoint(core::ShardId shard, core::SequenceId up_to_wal_seq) override;
  core::Result<void> Wipe(core::ShardId shard) override;
  core::Result<core::StorageStats> Stats() override;
  core::Result<void> Compact() override;
  core::Result<std::optional<core::RespCommand>> GetPromotionCommand(std::string_view key) override;
  core::Result<std::optional<core::ColdKeyState>> LoadKey(std::string_view key,
                                                          core::SteadyTime deadline) override;
  core::Result<std::optional<core::KeyMeta>> ProbeKey(std::string_view key,
                                                      core::SteadyTime deadline) override;
  core::Result<std::optional<core::MemberValue>> LoadMember(std::string_view key,
                                                            core::KeyType type,
                                                            std::string_view member,
                                                            core::SteadyTime deadline) override;

  // DEL returns the count of keys that existed before deletion.
  core::Result<core::RespValue> ExecDel(const core::ops::Del& op);

  core::Result<void> Start() override;
  core::Result<void> Stop() override;

  core::Result<SweepReport> RunScannerTickForTesting();
  TtlScanner::Snapshot ScannerSnapshot() const;

  struct RawRecord {
    std::string column_family;
    std::string key;
    std::string value;

    bool operator==(const RawRecord&) const = default;
  };
  // Every record in both column families, in key order, from one
  // snapshot.
  std::vector<RawRecord> RecordsForTesting() const;

 private:
  struct Impl;
  explicit RocksdbStore(std::unique_ptr<Impl> impl);

  std::unique_ptr<Impl> impl_;
};

}  // namespace abyss::cold::backends
