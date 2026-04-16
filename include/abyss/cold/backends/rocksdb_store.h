#pragma once

#ifndef ABYSS_HAVE_ROCKSDB
#error \
    "abyss/cold/backends/rocksdb_store.h requires ABYSS_HAVE_ROCKSDB — build with -DABYSS_WITH_ROCKSDB=ON"
#endif

#include <cstdint>
#include <memory>
#include <span>
#include <string>

#include "abyss/core/cold_store.h"

namespace abyss::cold::backends {

enum class CompactionStyle : std::uint8_t {
  kLevel,
  kUniversal,
};

struct RocksdbConfig {
  std::string data_path;
  size_t write_buffer_size_bytes = 67108864;
  uint32_t max_write_buffer_number = 4;
  uint32_t bloom_filter_bits_per_key = 10;
  CompactionStyle compaction_style = CompactionStyle::kLevel;
};

class RocksdbStore : public core::ColdStore {
 public:
  static core::Result<std::unique_ptr<RocksdbStore>> Create(RocksdbConfig config);

  ~RocksdbStore() override;

  RocksdbStore(const RocksdbStore&) = delete;
  RocksdbStore& operator=(const RocksdbStore&) = delete;
  RocksdbStore(RocksdbStore&&) = delete;
  RocksdbStore& operator=(RocksdbStore&&) = delete;

  core::Result<core::RespValue> Exec(const core::RespCommand& cmd) override;
  core::Result<void> ApplyBatch(std::span<const core::RespCommand> cmds) override;
  core::Result<core::StorageStats> Stats() override;
  core::Result<void> Compact() override;

 private:
  struct Impl;
  explicit RocksdbStore(std::unique_ptr<Impl> impl);

  core::Result<core::RespValue> ExecGet(const core::RespCommand& cmd);
  core::Result<core::RespValue> ExecSet(const core::RespCommand& cmd);
  core::Result<core::RespValue> ExecDel(const core::RespCommand& cmd);

  std::unique_ptr<Impl> impl_;
};

}  // namespace abyss::cold::backends
