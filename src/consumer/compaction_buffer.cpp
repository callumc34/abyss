#include "abyss/consumer/compaction_buffer.h"

namespace abyss::consumer {

void CompactionBuffer::Absorb(const std::string& key, const core::ops::WriteOp& op) {
  const std::unique_lock lock(mutex_);
  auto& entry = entries_[key];
  if (entry.key.empty()) {
    entry.key = key;
    entry.first_seen = core::SteadyClock::now();
  }
  entry.state.Absorb(op);
  entry.last_modified = core::SteadyClock::now();
  ++entry.write_count;
}

core::Result<core::RespValue> CompactionBuffer::Read(const std::string& key) const {
  const std::shared_lock lock(mutex_);

  auto it = entries_.find(key);
  if (it == entries_.end()) {
    return std::unexpected(core::Error(core::ErrorCode::kNotFound, "buffer has no state for key"));
  }

  const auto& state = it->second.state;

  // Tombstones are authoritative — key was DEL'd, do not fall through to cold.
  if (state.IsTombstone()) {
    return core::RespValue::Null();
  }

  // Scalars: buffer can answer definitively.
  if (state.Type() == CompactedState::DataType::kString) {
    return core::RespValue::BulkString(state.StringValue());
  }

  // Collections: buffer has state but can't serve reads. Fall through to cold.
  return std::unexpected(
      core::Error(core::ErrorCode::kNotFound, "collection read bypasses buffer"));
}

std::vector<BufferEntry> CompactionBuffer::FlushReady(core::SteadyTime /*now*/) {
  const std::unique_lock lock(mutex_);
  return {};
}

size_t CompactionBuffer::Size() const {
  const std::shared_lock lock(mutex_);
  return entries_.size();
}

size_t CompactionBuffer::BytesEstimate() const {
  const std::shared_lock lock(mutex_);
  return 0;
}

}  // namespace abyss::consumer
