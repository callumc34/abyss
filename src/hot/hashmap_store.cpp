#include "abyss/hot/hashmap_store.h"

namespace abyss::hot {

HashmapStore::HashmapStore(HashmapConfig config) : config_(config) {}
HashmapStore::~HashmapStore() = default;

core::Result<core::RespValue> HashmapStore::Exec(const core::ops::ReadOp& /*op*/) {
  return std::unexpected(
      core::Error(core::ErrorCode::kInternal, "HashmapStore::Exec requires in-memory data store"));
}

core::Result<void> HashmapStore::Apply(const core::ops::WriteOp& /*op*/,
                                       core::EvictionTTL /*eviction*/) {
  return std::unexpected(
      core::Error(core::ErrorCode::kInternal, "HashmapStore::Apply requires in-memory data store"));
}

core::Result<void> HashmapStore::ApplyBatch(std::span<const core::ops::WriteOp> /*ops*/,
                                            core::EvictionTTL /*eviction*/) {
  return std::unexpected(core::Error(core::ErrorCode::kInternal,
                                     "HashmapStore::ApplyBatch requires in-memory data store"));
}

core::Result<core::MemoryStats> HashmapStore::Stats() {
  return std::unexpected(
      core::Error(core::ErrorCode::kInternal, "HashmapStore::Stats requires in-memory data store"));
}

core::Result<void> HashmapStore::Flush() {
  return std::unexpected(
      core::Error(core::ErrorCode::kInternal, "HashmapStore::Flush requires in-memory data store"));
}

}  // namespace abyss::hot
