#pragma once

#include <gtest/gtest.h>

#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <map>
#include <string>

#include "abyss/queue/wal_queue.h"

namespace abyss::testing {

// A log directory's segment files by name, as their bytes stood.
using LogSnapshot = std::map<std::string, std::string>;

namespace power_loss_internal {

// Frames start after each segment's 4 KiB header block.
inline constexpr uint64_t kFramesAt = 4096;

// Calls `lose(path, offset)` for `extent`'s segment and for every later
// segment in its log directory, with the file offset where the bytes
// that were not durable start.
template <typename Lose>
void ForEachLostRange(const queue::DurableExtent& extent, const Lose& lose) {
  namespace fs = std::filesystem;
  const fs::path segment{extent.path};
  const std::string name = segment.filename().string();
  if (fs::exists(segment)) lose(segment, extent.offset);
  for (const auto& entry : fs::directory_iterator(segment.parent_path())) {
    const std::string other = entry.path().filename().string();
    // Ordinals are zero-padded to one width, so names sort as ordinals.
    if (entry.path().extension() == ".seg" && other.size() == name.size() && other > name) {
      lose(entry.path(), kFramesAt);
    }
  }
}

inline void Overwrite(const std::filesystem::path& path, uint64_t offset,
                      const std::string& bytes) {
  std::fstream file(path, std::ios::in | std::ios::out | std::ios::binary);
  ASSERT_TRUE(file.is_open()) << path;
  file.seekp(static_cast<std::streamoff>(offset));
  file.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
  ASSERT_TRUE(file.good()) << path;
}

}  // namespace power_loss_internal

// A power loss that kept only what was durable: zeroes `extent`'s
// segment from its offset to the end, and the frames of every later
// segment in the same log directory. Run it on a closed queue.
inline void SimulatePowerLoss(const queue::DurableExtent& extent) {
  power_loss_internal::ForEachLostRange(
      extent, [](const std::filesystem::path& path, uint64_t offset) {
        const uint64_t size = std::filesystem::file_size(path);
        if (offset >= size) return;
        power_loss_internal::Overwrite(path, offset, std::string(size - offset, '\0'));
      });
}

// Every segment file in a log directory, byte for byte.
inline LogSnapshot CaptureLog(const std::filesystem::path& dir) {
  LogSnapshot snapshot;
  for (const auto& entry : std::filesystem::directory_iterator(dir)) {
    if (entry.path().extension() != ".seg") continue;
    std::ifstream in(entry.path(), std::ios::binary);
    snapshot[entry.path().filename().string()] =
        std::string(std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>());
  }
  return snapshot;
}

// A power loss that kept only what was durable, on a device that still
// holds what the unflushed writes replaced: in a recycled segment, the
// frames of an earlier generation. Puts back every byte past `extent`
// from `snapshot`, taken while the durable end stood where it does now.
// A segment prepared since the snapshot had no frames durable, so its
// frames are zeroed; that includes a recycled spare renamed since, so
// only segments named in the snapshot carry stale frames. Run it on a
// closed queue.
inline void SimulatePowerLossRestoring(const queue::DurableExtent& extent,
                                       const LogSnapshot& snapshot) {
  power_loss_internal::ForEachLostRange(
      extent, [&snapshot](const std::filesystem::path& path, uint64_t offset) {
        const uint64_t size = std::filesystem::file_size(path);
        if (offset >= size) return;
        const auto it = snapshot.find(path.filename().string());
        if (it == snapshot.end()) {
          power_loss_internal::Overwrite(path, offset, std::string(size - offset, '\0'));
          return;
        }
        ASSERT_EQ(it->second.size(), size) << path;
        power_loss_internal::Overwrite(path, offset, it->second.substr(offset));
      });
}

}  // namespace abyss::testing
