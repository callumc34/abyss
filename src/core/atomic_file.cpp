#include "abyss/core/atomic_file.h"

#include <atomic>
#include <cstdint>
#include <filesystem>
#include <string>
#include <system_error>
#include <utility>

#include "abyss/platform/fs.h"

namespace abyss::core {
namespace {

namespace fs = abyss::platform::fs;

Error FsError(std::string_view what, const std::filesystem::path& path, const std::error_code& ec) {
  std::string msg(what);
  msg.append(" '");
  msg.append(path.string());
  msg.append("': ");
  msg.append(ec.message());
  return {ErrorCode::kInternal, std::move(msg)};
}

std::filesystem::path TempSibling(const std::filesystem::path& path) {
  static std::atomic<uint64_t> counter{0};
  std::string suffix = ".tmp.";
  suffix += std::to_string(fs::ProcessId());
  suffix += '.';
  suffix += std::to_string(counter.fetch_add(1, std::memory_order_relaxed));
  auto out = path;
  out += suffix;
  return out;
}

Result<void> EnsureParent(const std::filesystem::path& path) {
  const auto parent = path.parent_path();
  if (parent.empty()) return {};
  std::error_code ec;
  std::filesystem::create_directories(parent, ec);
  if (ec) return std::unexpected(FsError("create parent dir", parent, ec));
  return {};
}

}  // namespace

Result<void> WriteFileAtomic(const std::filesystem::path& path, std::span<const std::byte> bytes) {
  if (auto r = EnsureParent(path); !r.has_value()) return r;
  const auto tmp = TempSibling(path);

  auto file = fs::Open(
      tmp, fs::OpenOptions{
               .mode = fs::OpenMode::kWrite, .create = true, .exclusive = false, .truncate = true});
  if (!file.has_value()) return std::unexpected(file.error());

  if (auto r = fs::WriteAll(*file, bytes.data(), bytes.size()); !r.has_value()) {
    file->Close();
    (void)fs::Unlink(tmp);  // NOLINT(bugprone-unused-return-value)
    return std::unexpected(r.error());
  }
  if (auto r = fs::Fsync(*file); !r.has_value()) {
    file->Close();
    (void)fs::Unlink(tmp);  // NOLINT(bugprone-unused-return-value)
    return std::unexpected(r.error());
  }
  file->Close();

  if (auto r = fs::Rename(tmp, path); !r.has_value()) {
    (void)fs::Unlink(tmp);  // NOLINT(bugprone-unused-return-value)
    return std::unexpected(r.error());
  }

  const auto parent = path.parent_path().empty() ? std::filesystem::path{"."} : path.parent_path();
  auto dir_sync = fs::FsyncDir(parent);
  if (!dir_sync.has_value()) return std::unexpected(dir_sync.error());
  if (*dir_sync == fs::DirSyncOutcome::kUnsupported) {
    return std::unexpected(Error{ErrorCode::kFailedPrecondition,
                                 "directory durability unsupported on volume for '" +
                                     parent.string() + "'; cannot atomically persist '" +
                                     path.string() + "'"});
  }
  return {};
}

Result<void> WriteFileAtomic(const std::filesystem::path& path, std::string_view text) {
  return WriteFileAtomic(path, std::span<const std::byte>(
                                   reinterpret_cast<const std::byte*>(text.data()), text.size()));
}

}  // namespace abyss::core
