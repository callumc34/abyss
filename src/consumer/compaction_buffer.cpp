#include "abyss/consumer/compaction_buffer.h"

namespace abyss::consumer {

void CompactionBuffer::Absorb(const std::string& key, const core::RespCommand& cmd) {
  std::unique_lock lock(mutex_);
  auto& entry = entries_[key];
  if (entry.key.empty()) {
    entry.key = key;
    entry.first_seen = core::SteadyClock::now();
  }
  entry.state.Absorb(cmd);
  entry.last_modified = core::SteadyClock::now();
  ++entry.write_count;
}

core::Result<core::RespValue> CompactionBuffer::Read(const std::string& /*key*/) const {
  std::shared_lock lock(mutex_);
  return std::unexpected(core::Error(core::ErrorCode::kNotFound, "buffer read not implemented"));
}

std::vector<BufferEntry> CompactionBuffer::FlushReady(core::SteadyTime /*now*/) {
  std::unique_lock lock(mutex_);
  return {};
}

size_t CompactionBuffer::Size() const {
  std::shared_lock lock(mutex_);
  return entries_.size();
}

size_t CompactionBuffer::BytesEstimate() const {
  std::shared_lock lock(mutex_);
  return 0;
}

}  // namespace abyss::consumer
