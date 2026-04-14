#pragma once

#include <string>

#include "abyss/core/cold_store.h"

namespace abyss::cold {

struct RocksdbConfig {
  std::string data_path;
  size_t write_buffer_size_bytes = 67108864;
};

class RocksdbStore : public core::ColdStore {
 public:
  explicit RocksdbStore(RocksdbConfig config);
  ~RocksdbStore() override;

  core::Result<core::RespValue> Exec(const core::RespCommand& cmd) override;
  core::Result<void> ApplyBatch(std::span<const core::RespCommand> cmds) override;
  core::Result<core::StorageStats> Stats() override;
  core::Result<void> Compact() override;

 private:
  RocksdbConfig config_;
};

}  // namespace abyss::cold
