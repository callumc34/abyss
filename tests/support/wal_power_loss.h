#pragma once

#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iterator>
#include <map>
#include <optional>
#include <random>
#include <string>
#include <vector>

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

// Whether a segment's first frame slot is empty: it took no frame.
inline bool Frameless(const std::filesystem::path& path) {
  std::ifstream in(path, std::ios::binary);
  in.seekg(static_cast<std::streamoff>(kFramesAt));
  std::string word(8, '\0');
  in.read(word.data(), static_cast<std::streamsize>(word.size()));
  return in.gcount() < 8 || word == std::string(8, '\0');
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

// Where log position `pos` of log `log` lies on disk, as a durable
// extent ending there.
inline queue::DurableExtent ExtentAt(const std::filesystem::path& wal, uint32_t log,
                                     uint64_t segment_size, uint64_t pos) {
  const uint64_t frame_space = segment_size - power_loss_internal::kFramesAt;
  const uint64_t ordinal = pos / frame_space;
  std::string log_dir = std::to_string(log);
  log_dir.insert(0, 4 - log_dir.size(), '0');
  std::string name = std::to_string(ordinal);
  name.insert(0, 20 - name.size(), '0');
  return {.path = (wal / ("log-" + log_dir) / (name + ".seg")).string(),
          .offset = power_loss_internal::kFramesAt + (pos - (ordinal * frame_space))};
}

// What a power loss may do beyond the durable extent.
struct PowerLossFaults {
  // Each dirty 4 KiB page reached the device or was lost (zeroes).
  double keep_page = 0.5;
  // Single bits flipped in pages that reached the device.
  int bit_flips = 1;
  // A later segment with no frame of its own left short, as a torn
  // preparation would leave it. A segment's name and size are synced
  // before it takes a frame, so one holding frames never is, and none
  // loses its directory entry (the log's directory tests cover the
  // changes it does not sync).
  double truncate_segment = 0.15;
};

// What a lost page held on the device before: for a recycled segment,
// an earlier generation's frames; nullopt for zeroes.
using OldBytes =
    std::function<std::optional<std::string>(const std::filesystem::path&, uint64_t, uint64_t)>;

// A power loss that kept what was durable and an arbitrary subset of
// what was not: past `extent`, each 4 KiB page survives or reverts to
// what the device held before (`old`, else zeroes), surviving ones may
// carry single-bit flips, and segments wholly past it that took no
// frame may be cut short. Never touches a byte below the extent. Run
// it on a closed queue.
inline void SimulatePowerLossDropping(const queue::DurableExtent& extent, uint64_t seed,
                                      const PowerLossFaults& faults = {},
                                      const OldBytes& old = {}) {
  namespace fs = std::filesystem;
  constexpr uint64_t kPage = 4096;
  std::mt19937_64 rng(seed);
  const auto chance = [&rng](double p) {
    return std::uniform_real_distribution<double>(0.0, 1.0)(rng) < p;
  };
  struct Kept {
    fs::path path;
    uint64_t from = 0;
    uint64_t to = 0;
  };
  std::vector<Kept> kept;
  const fs::path first{extent.path};
  power_loss_internal::ForEachLostRange(extent, [&](const fs::path& path, uint64_t offset) {
    const bool later = path != first;
    const uint64_t size = fs::file_size(path);
    // Short: a preparation the power loss tore. Too short to hold a
    // frame, as no segment holding one can be: each is sized and synced
    // before it is named.
    if (later && power_loss_internal::Frameless(path) && chance(faults.truncate_segment)) {
      const uint64_t most = std::min<uint64_t>(size, power_loss_internal::kFramesAt + 8) - 1;
      fs::resize_file(path, std::uniform_int_distribution<uint64_t>(0, most)(rng));
      return;
    }
    // The page holding the extent keeps its durable head.
    for (uint64_t page = offset - (offset % kPage); page < size; page += kPage) {
      const uint64_t from = std::max(page, offset);
      const uint64_t to = std::min(page + kPage, size);
      if (from >= to) continue;
      if (chance(faults.keep_page)) {
        kept.push_back({.path = path, .from = from, .to = to});
        continue;
      }
      std::optional<std::string> before;
      if (old) before = old(path, from, to);
      power_loss_internal::Overwrite(path, from,
                                     before.has_value() ? *before : std::string(to - from, '\0'));
    }
  });
  for (int i = 0; i < faults.bit_flips && !kept.empty(); ++i) {
    const Kept& page = kept[std::uniform_int_distribution<size_t>(0, kept.size() - 1)(rng)];
    const uint64_t at = std::uniform_int_distribution<uint64_t>(page.from, page.to - 1)(rng);
    std::fstream file(page.path, std::ios::in | std::ios::out | std::ios::binary);
    ASSERT_TRUE(file.is_open()) << page.path;
    file.seekg(static_cast<std::streamoff>(at));
    char byte = 0;
    file.get(byte);
    byte = static_cast<char>(byte ^ static_cast<char>(1U << (rng() % 8)));
    file.seekp(static_cast<std::streamoff>(at));
    file.put(byte);
    ASSERT_TRUE(file.good()) << page.path;
  }
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
