#include "abyss/cold/rocksdb_store.h"

#include <utility>

namespace abyss::cold {

RocksdbStore::RocksdbStore(RocksdbConfig config) : config_(std::move(config)) {}
RocksdbStore::~RocksdbStore() = default;

core::Result<core::RespValue> RocksdbStore::Exec(const core::RespCommand& /*cmd*/) {
  return std::unexpected(
      core::Error(core::ErrorCode::kInternal, "RocksdbStore::Exec not implemented"));
}

core::Result<void> RocksdbStore::ApplyBatch(std::span<const core::RespCommand> /*cmds*/) {
  return std::unexpected(
      core::Error(core::ErrorCode::kInternal, "RocksdbStore::ApplyBatch not implemented"));
}

core::Result<core::StorageStats> RocksdbStore::Stats() {
  return std::unexpected(
      core::Error(core::ErrorCode::kInternal, "RocksdbStore::Stats not implemented"));
}

core::Result<void> RocksdbStore::Compact() {
  return std::unexpected(
      core::Error(core::ErrorCode::kInternal, "RocksdbStore::Compact not implemented"));
}

}  // namespace abyss::cold
