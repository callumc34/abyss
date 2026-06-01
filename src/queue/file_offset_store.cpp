#include "abyss/queue/file_offset_store.h"

#include <array>
#include <atomic>
#include <charconv>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <iomanip>
#include <optional>
#include <regex>
#include <span>
#include <sstream>
#include <string_view>
#include <vector>

#include "abyss/log/log.h"
#include "abyss/platform/fs.h"
#include "binary_io.h"
#include "crc32c.h"

ABYSS_LOG_COMPONENT("abyss.queue.offsets")

namespace abyss::queue {

namespace {

namespace pfs = abyss::platform::fs;

constexpr std::array<std::byte, 8> kOffsetMagic{
    std::byte{'A'}, std::byte{'B'}, std::byte{'Y'}, std::byte{'S'},
    std::byte{'S'}, std::byte{'O'}, std::byte{'F'}, std::byte{'F'},
};
constexpr uint8_t kOffsetFormatMajor = 2;
constexpr uint8_t kOffsetFormatMinor = 0;
// magic(8) + major(1) + minor(1) + reserved(2) + shard(4) + seq(8) + crc(4)
constexpr size_t kOffsetRecordSize = 28;
constexpr int kShardFileWidth = 20;

core::Result<std::vector<std::byte>> ReadAll(const std::string& path) {
  auto file =
      pfs::Open(std::filesystem::path(path), pfs::OpenOptions{.mode = pfs::OpenMode::kRead});
  if (!file.has_value()) {
    if (file.error().code() == core::ErrorCode::kNotFound) {
      return std::vector<std::byte>{};
    }
    return std::unexpected(file.error());
  }
  auto size = pfs::FileSize(*file);
  if (!size.has_value()) return std::unexpected(size.error());
  std::vector<std::byte> buf(static_cast<size_t>(*size));
  if (buf.empty()) return buf;
  auto nread = pfs::Pread(*file, buf.data(), buf.size(), 0);
  if (!nread.has_value()) return std::unexpected(nread.error());
  if (*nread < buf.size()) {
    return std::unexpected(core::Error{core::ErrorCode::kCorruption, "short read"});
  }
  return buf;
}

std::string FormatShardName(core::ShardId shard) {
  std::ostringstream oss;
  oss << std::setw(kShardFileWidth) << std::setfill('0') << shard << ".offset";
  return oss.str();
}

// Checked, non-throwing decimal parse into T (QUEUE-3): nullopt on overflow or
// any non-numeric trailing content. Replaces std::stoul/std::stoull, which
// throw std::out_of_range on 20-digit-plus names and abort recovery.
template <typename T>
std::optional<T> ParseUnsignedChecked(std::string_view text) {
  T value = 0;
  const char* begin = text.data();
  const char* end = begin + text.size();
  const auto [ptr, ec] = std::from_chars(begin, end, value);
  if (ec != std::errc{} || ptr != end) return std::nullopt;
  return value;
}

}  // namespace

core::Result<std::unique_ptr<FileOffsetStore>> FileOffsetStore::Open(FileOffsetStoreConfig config) {
  std::error_code ec;
  std::filesystem::create_directories(config.directory, ec);
  if (ec) {
    return std::unexpected(
        core::Error{core::ErrorCode::kInternal, "create offsets dir: " + ec.message()});
  }

  std::unique_ptr<FileOffsetStore> store(new FileOffsetStore(std::move(config)));

  auto load = store->LoadAll();
  if (!load.has_value()) return std::unexpected(load.error());

  size_t persisted = 0;
  {
    const std::scoped_lock lock(store->mu_);
    for (const auto& [_, shards] : store->offsets_) persisted += shards.size();
  }
  ABYSS_LOG_INFO("offset store opened", {"dir", std::string_view{store->config_.directory}},
                 {"persisted_entries", static_cast<uint64_t>(persisted)});

  return store;
}

FileOffsetStore::FileOffsetStore(FileOffsetStoreConfig config) : config_(std::move(config)) {}

std::optional<core::SequenceId> FileOffsetStore::Get(core::ConsumerId consumer,
                                                     core::ShardId shard) const {
  const std::scoped_lock lock(mu_);
  auto it = offsets_.find(consumer);
  if (it == offsets_.end()) return std::nullopt;
  auto sit = it->second.find(shard);
  if (sit == it->second.end()) return std::nullopt;
  return sit->second;
}

core::Result<void> FileOffsetStore::Set(core::ConsumerId consumer, core::ShardId shard,
                                        core::SequenceId seq) {
  // Write-through (XERR-2): persist durably FIRST, advance the in-memory cache
  // only on success. On a failed write the cache stays behind disk, so the
  // reaper (which reads the cache) never observes an offset not yet durable.
  auto result = WriteShardFile(consumer, shard, seq);
  if (!result.has_value()) {
    ABYSS_LOG_ERROR("offset persist failed", {"consumer", static_cast<uint64_t>(consumer)},
                    {"shard", static_cast<int64_t>(shard)}, {"seq", static_cast<uint64_t>(seq)},
                    {"err", std::string_view{result.error().message()}});
    return result;
  }
  // (consumer, shard) has one writer; only the shared cache needs locking.
  const std::scoped_lock lock(mu_);
  offsets_[consumer][shard] = seq;
  return {};
}

std::string FileOffsetStore::ConsumerDir(core::ConsumerId consumer) const {
  return config_.directory + "/" + std::to_string(consumer);
}

std::string FileOffsetStore::ShardFilePath(core::ConsumerId consumer, core::ShardId shard) const {
  return ConsumerDir(consumer) + "/" + FormatShardName(shard);
}

std::string FileOffsetStore::ShardTempPath(core::ConsumerId consumer, core::ShardId shard) const {
  static std::atomic<uint64_t> counter{0};
  const auto suffix = std::to_string(pfs::ProcessId()) + "." +
                      std::to_string(counter.fetch_add(1, std::memory_order_relaxed));
  return ShardFilePath(consumer, shard) + ".tmp." + suffix;
}

core::Result<void> FileOffsetStore::LoadAll() {
  std::error_code ec;
  const std::filesystem::directory_iterator root_it(config_.directory, ec);
  if (ec) return std::unexpected(core::Error{core::ErrorCode::kInternal, ec.message()});

  const std::regex consumer_pattern(R"(\d+)");
  const std::regex shard_pattern(R"((\d{20})\.offset)");

  for (const auto& consumer_entry : root_it) {
    if (!consumer_entry.is_directory()) continue;
    const auto consumer_name = consumer_entry.path().filename().string();
    if (!std::regex_match(consumer_name, consumer_pattern)) continue;

    auto consumer_id = ParseUnsignedChecked<core::ConsumerId>(consumer_name);
    if (!consumer_id.has_value()) {
      // An over-long / overflowing consumer dir name is not one we wrote; skip.
      ABYSS_LOG_WARN("skipping unparseable consumer dir",
                     {"name", std::string_view{consumer_name}});
      continue;
    }

    // Sweep orphan tmp files from a prior crash before scanning this dir
    // (QUEUE-7).
    SweepTempFiles(consumer_entry.path().string());

    const std::filesystem::directory_iterator shard_it(consumer_entry.path(), ec);
    if (ec) return std::unexpected(core::Error{core::ErrorCode::kInternal, ec.message()});

    for (const auto& shard_entry : shard_it) {
      if (!shard_entry.is_regular_file()) continue;
      const auto shard_name = shard_entry.path().filename().string();
      std::smatch match;
      if (!std::regex_match(shard_name, match, shard_pattern)) continue;

      auto parsed_shard = ParseUnsignedChecked<core::ShardId>(match[1].str());
      if (!parsed_shard.has_value()) {
        ABYSS_LOG_WARN("skipping unparseable offset file", {"name", std::string_view{shard_name}});
        continue;
      }
      const auto shard_id = *parsed_shard;
      auto record = LoadShardFile(shard_entry.path().string());
      if (!record.has_value()) return std::unexpected(record.error());

      if (record->shard != shard_id) {
        return std::unexpected(
            core::Error{core::ErrorCode::kCorruption,
                        "shard id mismatch in offset file: " + shard_entry.path().string()});
      }

      const std::scoped_lock lock(mu_);
      offsets_[*consumer_id][shard_id] = record->seq;
    }
  }
  return {};
}

void FileOffsetStore::SweepTempFiles(const std::string& dir) {
  std::error_code ec;
  std::filesystem::directory_iterator it(dir, ec);
  if (ec) return;  // Dir vanished or unreadable; nothing to sweep.
  for (const auto& entry : it) {
    if (!entry.is_regular_file()) continue;
    const auto name = entry.path().filename().string();
    if (!name.contains(".offset.tmp.")) continue;
    std::error_code rm_ec;
    std::filesystem::remove(entry.path(), rm_ec);
    if (rm_ec) {
      ABYSS_LOG_WARN("failed to sweep orphan offset tmp", {"path", entry.path().string()},
                     {"err", std::string_view{rm_ec.message()}});
    }
  }
}

core::Result<FileOffsetStore::Record> FileOffsetStore::LoadShardFile(const std::string& path) {
  auto bytes = ReadAll(path);
  if (!bytes.has_value()) return std::unexpected(bytes.error());
  if (bytes->size() != kOffsetRecordSize) {
    return std::unexpected(
        core::Error{core::ErrorCode::kCorruption, "offset file wrong size: " + path});
  }

  const auto* raw = reinterpret_cast<const uint8_t*>(bytes->data());
  if (std::memcmp(raw, kOffsetMagic.data(), kOffsetMagic.size()) != 0) {
    ABYSS_LOG_CRITICAL("offsets magic mismatch", {"path", std::string_view{path}});
    return std::unexpected(core::Error{core::ErrorCode::kCorruption, "offsets magic mismatch"});
  }

  const uint8_t major = raw[kOffsetMagic.size()];
  if (major != kOffsetFormatMajor) {
    ABYSS_LOG_CRITICAL("unsupported offsets format", {"path", std::string_view{path}},
                       {"found_major", static_cast<int64_t>(major)},
                       {"expected_major", static_cast<int64_t>(kOffsetFormatMajor)});
    return std::unexpected(
        core::Error{core::ErrorCode::kCorruption, "unsupported offsets format major"});
  }

  uint32_t shard = 0;
  uint64_t seq = 0;
  std::memcpy(&shard, raw + 12, sizeof(shard));
  std::memcpy(&seq, raw + 16, sizeof(seq));
  if constexpr (!binary::kNativeLittleEndian) {
    shard = std::byteswap(shard);
    seq = std::byteswap(seq);
  }

  uint32_t stored_crc = 0;
  std::memcpy(&stored_crc, raw + 24, sizeof(stored_crc));
  if constexpr (!binary::kNativeLittleEndian) stored_crc = std::byteswap(stored_crc);

  const uint32_t computed =
      Crc32c(std::span<const std::byte>(bytes->data(), kOffsetRecordSize - sizeof(uint32_t)));
  if (computed != stored_crc) {
    ABYSS_LOG_CRITICAL("offsets crc mismatch", {"path", std::string_view{path}},
                       {"stored_crc", static_cast<uint64_t>(stored_crc)},
                       {"computed_crc", static_cast<uint64_t>(computed)});
    return std::unexpected(core::Error{core::ErrorCode::kCorruption, "offsets crc mismatch"});
  }

  return Record{.shard = shard, .seq = seq};
}

core::Result<void> FileOffsetStore::WriteShardFile(core::ConsumerId consumer, core::ShardId shard,
                                                   core::SequenceId seq) const {
  const auto consumer_dir = ConsumerDir(consumer);
  std::error_code ec;
  // create_directories returns true iff it actually created the directory.
  const bool created_consumer_dir = std::filesystem::create_directories(consumer_dir, ec);
  if (ec) {
    return std::unexpected(
        core::Error{core::ErrorCode::kInternal, "create consumer dir: " + ec.message()});
  }

  std::vector<std::byte> buf;
  buf.reserve(kOffsetRecordSize);
  binary::AppendBytes(buf, kOffsetMagic.data(), kOffsetMagic.size());
  binary::WriteU8(buf, kOffsetFormatMajor);
  binary::WriteU8(buf, kOffsetFormatMinor);
  binary::WriteU16LE(buf, 0);
  binary::WriteU32LE(buf, shard);
  binary::WriteU64LE(buf, seq);

  const uint32_t crc = Crc32c(std::span<const std::byte>(buf));
  binary::WriteU32LE(buf, crc);

  const auto tmp = ShardTempPath(consumer, shard);
  auto file = pfs::Open(
      std::filesystem::path(tmp),
      pfs::OpenOptions{
          .mode = pfs::OpenMode::kWrite, .create = true, .exclusive = false, .truncate = true});
  if (!file.has_value()) return std::unexpected(file.error());

  if (auto r = pfs::WriteAll(*file, buf.data(), buf.size()); !r.has_value()) {
    file->Close();
    (void)pfs::Unlink(std::filesystem::path(tmp));  // NOLINT(bugprone-unused-return-value)
    return std::unexpected(r.error());
  }

  if (auto r = pfs::Fsync(*file); !r.has_value()) {
    file->Close();
    (void)pfs::Unlink(std::filesystem::path(tmp));  // NOLINT(bugprone-unused-return-value)
    return std::unexpected(r.error());
  }
  file->Close();

  const auto target = ShardFilePath(consumer, shard);
  if (auto r = pfs::Rename(std::filesystem::path(tmp), std::filesystem::path(target));
      !r.has_value()) {
    (void)pfs::Unlink(std::filesystem::path(tmp));  // NOLINT(bugprone-unused-return-value)
    return std::unexpected(r.error());
  }

  // The persisted ack must be durable; a volume that cannot fsync directories
  // cannot make the rename crash-durable, so a persisted offset could outrun
  // stable storage (invariants 2/5, G5).
  auto dir_sync = pfs::FsyncDir(std::filesystem::path(consumer_dir));
  if (!dir_sync.has_value()) return std::unexpected(dir_sync.error());
  if (*dir_sync == pfs::DirSyncOutcome::kUnsupported) {
    return std::unexpected(core::Error{core::ErrorCode::kFailedPrecondition,
                                       "offset directory durability unsupported on volume '" +
                                           consumer_dir +
                                           "'; a persisted ack could outrun durable storage"});
  }

  // The first ack for a new consumer creates a subdir under the offsets root;
  // that new subdir's directory entry must also be durably linked, or a crash
  // right after the rename can lose the whole subdir and the offset with it
  // (QUEUE-5).
  if (created_consumer_dir) {
    auto root_sync = pfs::FsyncDir(std::filesystem::path(config_.directory));
    if (!root_sync.has_value()) return std::unexpected(root_sync.error());
    if (*root_sync == pfs::DirSyncOutcome::kUnsupported) {
      return std::unexpected(core::Error{
          core::ErrorCode::kFailedPrecondition,
          "offsets-root directory durability unsupported on '" + config_.directory + "'"});
    }
  }
  return {};
}

}  // namespace abyss::queue
