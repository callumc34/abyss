#include "abyss/hot/hashmap_store.h"

namespace abyss::hot {

HashmapStore::HashmapStore(HashmapConfig config) : config_(config) {}
HashmapStore::~HashmapStore() = default;

core::Result<core::RespValue> HashmapStore::Exec(const core::RespCommand& /*cmd*/) {
  return std::unexpected(
      core::Error(core::ErrorCode::kInternal, "HashmapStore::Exec not implemented"));
}

core::Result<void> HashmapStore::Apply(const core::RespCommand& /*cmd*/,
                                       core::EvictionTTL /*eviction*/) {
  return std::unexpected(
      core::Error(core::ErrorCode::kInternal, "HashmapStore::Apply not implemented"));
}

core::Result<void> HashmapStore::ApplyBatch(std::span<const core::RespCommand> /*cmds*/,
                                            core::EvictionTTL /*eviction*/) {
  return std::unexpected(
      core::Error(core::ErrorCode::kInternal, "HashmapStore::ApplyBatch not implemented"));
}

core::Result<core::MemoryStats> HashmapStore::Stats() {
  return std::unexpected(
      core::Error(core::ErrorCode::kInternal, "HashmapStore::Stats not implemented"));
}

core::Result<void> HashmapStore::Flush() {
  return std::unexpected(
      core::Error(core::ErrorCode::kInternal, "HashmapStore::Flush not implemented"));
}

}  // namespace abyss::hot
