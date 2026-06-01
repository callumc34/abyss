#pragma once

#include <cstdint>
#include <filesystem>
#include <string>

#include "abyss/core/result.h"

namespace abyss::core {

// Stable scheme identifiers (ADP-014). Persisted in the manifest so that any
// future change to the routing or placement scheme is detected as a mismatch
// rather than silently re-routing on-disk data. Bump the string (do not reuse
// it) when the corresponding scheme changes.
inline constexpr const char* kWireSlotScheme = "crc16-16384";
inline constexpr const char* kDataShardScheme = "slot-range-v1";

// The placement + wire topology the on-disk WAL and cold data were written
// under. `cold_format_epoch` mirrors cold::format::kFormatVersion.
struct TopologyDescriptor {
  uint32_t shard_count = 0;
  uint16_t cold_format_epoch = 0;
  std::string wire_slot_hash;
  std::string data_shard_hash;
};

// Persisted, refuse-to-start topology gate (ADP-014, finding G1). On the first
// start of a fresh data directory the descriptor is written atomically next to
// node.id (durable: tmp + fsync + rename + dir-fsync, via core::WriteFileAtomic).
// On every subsequent start the persisted manifest is read and validated against
// the effective descriptor; ANY field mismatch is a refuse-to-start kCorruption
// error so a changed --shard-count or cold-encoding epoch can never silently
// re-route/re-encode data. Run at the composition root BEFORE WAL/cold/consumers
// open.
class TopologyManifest {
 public:
  static Result<TopologyManifest> OpenOrValidate(const std::filesystem::path& data_dir,
                                                 const TopologyDescriptor& effective);

  const TopologyDescriptor& descriptor() const noexcept { return desc_; }

 private:
  explicit TopologyManifest(TopologyDescriptor desc) : desc_(std::move(desc)) {}

  TopologyDescriptor desc_;
};

}  // namespace abyss::core
