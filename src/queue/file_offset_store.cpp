#include "abyss/queue/file_offset_store.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstring>
#include <filesystem>
#include <regex>
#include <span>
#include <vector>

#include "binary_io.h"
#include "crc32c.h"

namespace abyss::queue {

namespace {

constexpr std::array<std::byte, 8> kOffsetMagic{
    std::byte{'A'}, std::byte{'B'}, std::byte{'Y'}, std::byte{'S'},
    std::byte{'S'}, std::byte{'O'}, std::byte{'F'}, std::byte{'F'},
};
constexpr uint8_t kOffsetFormatMajor = 1;
constexpr uint8_t kOffsetFormatMinor = 0;
constexpr size_t kOffsetHeaderSize = 16;  // magic(8) + major(1) + minor(1) + reserved(2) + count(4)
constexpr size_t kOffsetRecordSize = 12;  // shard(4) + seq(8)
constexpr size_t kOffsetCrcSize = 4;

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
  // NOLINTNEXTLINE(cppcoreguidelines-pro-type-vararg,cppcoreguidelines-init-variables)
  int fd = ::open(path.c_str(), O_RDONLY);
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
  int fd = ::open(dir.c_str(), O_RDONLY);
  if (fd < 0) return std::unexpected(IoError("open dir"));
  if (::fsync(fd) < 0) {
    ::close(fd);
    return std::unexpected(IoError("fsync dir"));
  }
  ::close(fd);
  return {};
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
  std::lock_guard lock(mu_);
  auto it = offsets_.find(consumer);
  if (it == offsets_.end()) return std::nullopt;
  auto sit = it->second.find(shard);
  if (sit == it->second.end()) return std::nullopt;
  return sit->second;
}

core::Result<void> FileOffsetStore::Set(core::ConsumerId consumer, core::ShardId shard,
                                        core::SequenceId seq) {
  ConsumerMap snapshot;
  {
    std::lock_guard lock(mu_);
    offsets_[consumer][shard] = seq;
    snapshot = offsets_[consumer];
  }
  return WriteConsumer(consumer, snapshot);
}

std::string FileOffsetStore::FilePath(core::ConsumerId consumer) const {
  return config_.directory + "/" + std::to_string(consumer) + ".offsets";
}

std::string FileOffsetStore::TempPath(core::ConsumerId consumer) const {
  return FilePath(consumer) + ".tmp";
}

core::Result<void> FileOffsetStore::LoadAll() {
  std::error_code ec;
  std::filesystem::directory_iterator it(config_.directory, ec);
  if (ec) return std::unexpected(core::Error{core::ErrorCode::kInternal, ec.message()});

  const std::regex name_pattern(R"((\d+)\.offsets)");
  for (const auto& entry : it) {
    if (!entry.is_regular_file()) continue;
    const auto name = entry.path().filename().string();
    std::smatch match;
    if (!std::regex_match(name, match, name_pattern)) continue;

    auto consumer_id = static_cast<core::ConsumerId>(std::stoul(match[1].str()));
    auto records = LoadConsumer(consumer_id);
    if (!records.has_value()) return std::unexpected(records.error());

    std::lock_guard lock(mu_);
    offsets_[consumer_id] = std::move(*records);
  }
  return {};
}

core::Result<FileOffsetStore::ConsumerMap> FileOffsetStore::LoadConsumer(
    core::ConsumerId consumer) const {
  auto bytes = ReadAll(FilePath(consumer));
  if (!bytes.has_value()) return std::unexpected(bytes.error());
  if (bytes->empty()) return ConsumerMap{};

  std::span<const std::byte> view(*bytes);
  if (view.size() < kOffsetHeaderSize + kOffsetCrcSize) {
    return std::unexpected(core::Error{core::ErrorCode::kCorruption, "offsets file truncated"});
  }

  if (std::memcmp(view.data(), kOffsetMagic.data(), kOffsetMagic.size()) != 0) {
    return std::unexpected(core::Error{core::ErrorCode::kCorruption, "offsets magic mismatch"});
  }

  const auto* raw = reinterpret_cast<const uint8_t*>(view.data());
  const uint8_t major = raw[kOffsetMagic.size()];
  if (major != kOffsetFormatMajor) {
    return std::unexpected(
        core::Error{core::ErrorCode::kCorruption, "unsupported offsets format major"});
  }

  uint32_t count = 0;
  std::memcpy(&count, raw + 12, sizeof(count));
  if constexpr (!binary::kNativeLittleEndian) count = std::byteswap(count);

  const size_t expected = kOffsetHeaderSize + (count * kOffsetRecordSize) + kOffsetCrcSize;
  if (view.size() != expected) {
    return std::unexpected(core::Error{core::ErrorCode::kCorruption, "offsets file size mismatch"});
  }

  uint32_t stored_crc = 0;
  std::memcpy(&stored_crc, raw + view.size() - kOffsetCrcSize, sizeof(stored_crc));
  if constexpr (!binary::kNativeLittleEndian) stored_crc = std::byteswap(stored_crc);

  const uint32_t computed =
      Crc32c(std::span<const std::byte>(view.data(), view.size() - kOffsetCrcSize));
  if (computed != stored_crc) {
    return std::unexpected(core::Error{core::ErrorCode::kCorruption, "offsets crc mismatch"});
  }

  ConsumerMap result;
  result.reserve(count);
  const uint8_t* cursor = raw + kOffsetHeaderSize;
  for (uint32_t i = 0; i < count; ++i) {
    uint32_t shard = 0;
    uint64_t seq = 0;
    std::memcpy(&shard, cursor, sizeof(shard));
    std::memcpy(&seq, cursor + sizeof(shard), sizeof(seq));
    if constexpr (!binary::kNativeLittleEndian) {
      shard = std::byteswap(shard);
      seq = std::byteswap(seq);
    }
    result[shard] = seq;
    cursor += kOffsetRecordSize;
  }
  return result;
}

core::Result<void> FileOffsetStore::WriteConsumer(core::ConsumerId consumer,
                                                  const ConsumerMap& entries) const {
  std::vector<std::byte> buf;
  buf.reserve(kOffsetHeaderSize + (entries.size() * kOffsetRecordSize) + kOffsetCrcSize);

  binary::AppendBytes(buf, kOffsetMagic.data(), kOffsetMagic.size());
  binary::WriteU8(buf, kOffsetFormatMajor);
  binary::WriteU8(buf, kOffsetFormatMinor);
  binary::WriteU16LE(buf, 0);
  binary::WriteU32LE(buf, static_cast<uint32_t>(entries.size()));

  std::vector<std::pair<core::ShardId, core::SequenceId>> sorted(entries.begin(), entries.end());
  std::ranges::sort(sorted);
  for (const auto& [shard, seq] : sorted) {
    binary::WriteU32LE(buf, shard);
    binary::WriteU64LE(buf, seq);
  }

  const uint32_t crc = Crc32c(std::span<const std::byte>(buf));
  binary::WriteU32LE(buf, crc);

  const auto tmp = TempPath(consumer);
  // NOLINTNEXTLINE(cppcoreguidelines-pro-type-vararg,cppcoreguidelines-init-variables)
  int fd = ::open(tmp.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
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

  const auto target = FilePath(consumer);
  if (::rename(tmp.c_str(), target.c_str()) < 0) {
    ::unlink(tmp.c_str());
    return std::unexpected(IoError("rename"));
  }

  return FsyncDir(config_.directory);
}

}  // namespace abyss::queue
