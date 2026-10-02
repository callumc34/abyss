#pragma once

#include <gtest/gtest.h>

#include <cstdint>
#include <filesystem>
#include <fstream>
#include <string>

#include "abyss/queue/wal_queue.h"

namespace abyss::testing {

// A power loss that kept only what was durable: zeroes `extent`'s
// segment from its offset to the end, and the frames of every later
// segment in the same log directory. Run it on a closed queue.
inline void SimulatePowerLoss(const queue::DurableExtent& extent) {
  namespace fs = std::filesystem;
  // Frames start after each segment's 4 KiB header block.
  constexpr uint64_t kFramesAt = 4096;
  const auto zero_from = [](const fs::path& path, uint64_t offset) {
    const uint64_t size = fs::file_size(path);
    if (offset >= size) return;
    std::fstream file(path, std::ios::in | std::ios::out | std::ios::binary);
    ASSERT_TRUE(file.is_open()) << path;
    file.seekp(static_cast<std::streamoff>(offset));
    const std::string zeros(size - offset, '\0');
    file.write(zeros.data(), static_cast<std::streamsize>(zeros.size()));
    ASSERT_TRUE(file.good()) << path;
  };
  const fs::path segment{extent.path};
  const std::string name = segment.filename().string();
  if (fs::exists(segment)) zero_from(segment, extent.offset);
  for (const auto& entry : fs::directory_iterator(segment.parent_path())) {
    const std::string other = entry.path().filename().string();
    // Ordinals are zero-padded to one width, so names sort as ordinals.
    if (entry.path().extension() == ".seg" && other.size() == name.size() && other > name) {
      zero_from(entry.path(), kFramesAt);
    }
  }
}

}  // namespace abyss::testing
