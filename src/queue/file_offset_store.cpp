#include "abyss/queue/file_offset_store.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <array>
#include <atomic>
#include <cerrno>
#include <cstring>
#include <filesystem>
#include <iomanip>
#include <regex>
#include <span>
#include <sstream>
#include <vector>

#include "binary_io.h"
#include "crc32c.h"

namespace abyss::queue {

namespace {

constexpr std::array<std::byte, 8> kOffsetMagic{
    std::byte{'A'}, std::byte{'B'}, std::byte{'Y'}, std::byte{'S'},
    std::byte{'S'}, std::byte{'O'}, std::byte{'F'}, std::byte{'F'},
};
constexpr uint8_t kOffsetFormatMajor = 2;
constexpr uint8_t kOffsetFormatMinor = 0;
// magic(8) + major(1) + minor(1) + reserved(2) + shard(4) + seq(8) + crc(4)
constexpr size_t kOffsetRecordSize = 28;
constexpr int kShardFileWidth = 20;

core::Error IoError(const char* what) {
  return {core::ErrorCode::kInternal, std::string(what) + ": " + std::strerror(errno)};
}

core::Result<void> WriteFull(int fd, const void* buf, size_t count) {
  const auto* p = static_cast<const uint8_t*>(buf);
  size_t remaining = count;
  while (remaining > 0) {  // NOLINT(bugprone-infinite-loop)
    auto n = ::write(fd, p, remaining);
    if (n < 0) {
      if (errno == EINTR) continue;
      return std::unexpected(IoError("write"));
    }
    p += n;
    remaining -= static_cast<size_t>(n);
  }
  return {};
}

core::Result<std::vector<std::byte>> ReadAll(const std::string& path) {
  // NOLINTNEXTLINE(cppcoreguidelines-pro-type-vararg)
  const int fd = ::open(path.c_str(), O_RDONLY);
  if (fd < 0) {
    if (errno == ENOENT) {
      return std::vector<std::byte>{};
    }
    return std::unexpected(IoError("open"));
  }
  struct stat st{};
  if (::fstat(fd, &st) < 0) {
    ::close(fd);
    return std::unexpected(IoError("fstat"));
  }
  std::vector<std::byte> buf(static_cast<size_t>(st.st_size));
  auto* p = buf.data();
  size_t remaining = buf.size();
  while (remaining > 0) {
    auto n = ::read(fd, p, remaining);
    if (n < 0) {
      if (errno == EINTR) continue;
      ::close(fd);
      return std::unexpected(IoError("read"));
    }
    if (n == 0) {
      ::close(fd);
      return std::unexpected(core::Error{core::ErrorCode::kCorruption, "short read"});
    }
    p += n;
    remaining -= static_cast<size_t>(n);
  }
  ::close(fd);
  return buf;
}

core::Result<void> FsyncDir(const std::string& dir) {
  // NOLINTNEXTLINE(cppcoreguidelines-pro-type-vararg,cppcoreguidelines-init-variables)
  const int fd = ::open(dir.c_str(), O_RDONLY);
  if (fd < 0) return std::unexpected(IoError("open dir"));
  if (::fsync(fd) < 0) {
    ::close(fd);
    return std::unexpected(IoError("fsync dir"));
  }
  ::close(fd);
  return {};
}

std::string FormatShardName(core::ShardId shard) {
  std::ostringstream oss;
  oss << std::setw(kShardFileWidth) << std::setfill('0') << shard << ".offset";
  return oss.str();
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
  // (consumer, shard) has one writer; only the shared cache needs locking.
  {
    const std::scoped_lock lock(mu_);
    offsets_[consumer][shard] = seq;
  }
  return WriteShardFile(consumer, shard, seq);
}

std::string FileOffsetStore::ConsumerDir(core::ConsumerId consumer) const {
  return config_.directory + "/" + std::to_string(consumer);
}

std::string FileOffsetStore::ShardFilePath(core::ConsumerId consumer, core::ShardId shard) const {
  return ConsumerDir(consumer) + "/" + FormatShardName(shard);
}

std::string FileOffsetStore::ShardTempPath(core::ConsumerId consumer, core::ShardId shard) const {
  static std::atomic<uint64_t> counter{0};
  const auto suffix = std::to_string(::getpid()) + "." +
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

    const auto consumer_id = static_cast<core::ConsumerId>(std::stoul(consumer_name));

    const std::filesystem::directory_iterator shard_it(consumer_entry.path(), ec);
    if (ec) return std::unexpected(core::Error{core::ErrorCode::kInternal, ec.message()});

    for (const auto& shard_entry : shard_it) {
      if (!shard_entry.is_regular_file()) continue;
      const auto shard_name = shard_entry.path().filename().string();
      std::smatch match;
      if (!std::regex_match(shard_name, match, shard_pattern)) continue;

      const auto shard_id = static_cast<core::ShardId>(std::stoull(match[1].str()));
      auto record = LoadShardFile(shard_entry.path().string());
      if (!record.has_value()) return std::unexpected(record.error());

      if (record->shard != shard_id) {
        return std::unexpected(
            core::Error{core::ErrorCode::kCorruption,
                        "shard id mismatch in offset file: " + shard_entry.path().string()});
      }

      const std::scoped_lock lock(mu_);
      offsets_[consumer_id][shard_id] = record->seq;
    }
  }
  return {};
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
    return std::unexpected(core::Error{core::ErrorCode::kCorruption, "offsets magic mismatch"});
  }

  const uint8_t major = raw[kOffsetMagic.size()];
  if (major != kOffsetFormatMajor) {
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
    return std::unexpected(core::Error{core::ErrorCode::kCorruption, "offsets crc mismatch"});
  }

  return Record{.shard = shard, .seq = seq};
}

core::Result<void> FileOffsetStore::WriteShardFile(core::ConsumerId consumer, core::ShardId shard,
                                                   core::SequenceId seq) const {
  const auto consumer_dir = ConsumerDir(consumer);
  std::error_code ec;
  std::filesystem::create_directories(consumer_dir, ec);
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
  // NOLINTNEXTLINE(cppcoreguidelines-pro-type-vararg,cppcoreguidelines-init-variables)
  const int fd = ::open(tmp.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
  if (fd < 0) return std::unexpected(IoError("open tmp"));

  auto wr = WriteFull(fd, buf.data(), buf.size());
  if (!wr.has_value()) {
    ::close(fd);
    ::unlink(tmp.c_str());
    return std::unexpected(wr.error());
  }

  if (::fsync(fd) < 0) {
    ::close(fd);
    ::unlink(tmp.c_str());
    return std::unexpected(IoError("fsync tmp"));
  }
  ::close(fd);

  const auto target = ShardFilePath(consumer, shard);
  if (::rename(tmp.c_str(), target.c_str()) < 0) {
    ::unlink(tmp.c_str());
    return std::unexpected(IoError("rename"));
  }

  return FsyncDir(consumer_dir);
}

}  // namespace abyss::queue
