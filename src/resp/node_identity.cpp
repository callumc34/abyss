#include "abyss/resp/node_identity.h"

#include <array>
#include <cstdint>
#include <fstream>
#include <random>
#include <string>
#include <system_error>

#include "abyss/core/atomic_file.h"

namespace abyss::resp {
namespace {

constexpr size_t kUuidStringLength = 36;
constexpr size_t kUuidByteLength = 16;

core::Error FsError(const std::filesystem::path& path, std::string_view what,
                    const std::error_code& ec) {
  std::string msg(what);
  msg.append(" '");
  msg.append(path.string());
  msg.append("': ");
  msg.append(ec.message());
  return {core::ErrorCode::kInternal, std::move(msg)};
}

std::string FormatUuidV4(const std::array<uint8_t, kUuidByteLength>& bytes) {
  static constexpr std::array<char, 16> kHex{
      '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f',
  };
  std::string out;
  out.reserve(kUuidStringLength);
  size_t i = 0;
  for (const auto byte : bytes) {
    if (i == 4 || i == 6 || i == 8 || i == 10) {
      out.push_back('-');
    }
    out.push_back(kHex.at((byte >> 4U) & 0x0FU));
    out.push_back(kHex.at(byte & 0x0FU));
    ++i;
  }
  return out;
}

std::string GenerateUuidV4() {
  std::array<uint8_t, kUuidByteLength> bytes{};
  std::random_device rd;
  for (auto& b : bytes) {
    b = static_cast<uint8_t>(rd() & 0xFFU);
  }
  bytes[6] = static_cast<uint8_t>((bytes[6] & 0x0FU) | 0x40U);  // version 4
  bytes[8] = static_cast<uint8_t>((bytes[8] & 0x3FU) | 0x80U);  // variant 10
  return FormatUuidV4(bytes);
}

bool IsHexChar(char c) {
  return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F');
}

bool IsValidUuid(std::string_view s) {
  if (s.size() != kUuidStringLength) return false;
  for (size_t i = 0; i < kUuidStringLength; ++i) {
    const bool is_dash_pos = (i == 8 || i == 13 || i == 18 || i == 23);
    if (is_dash_pos) {
      if (s[i] != '-') return false;
    } else if (!IsHexChar(s[i])) {
      return false;
    }
  }
  return true;
}

core::Result<std::string> ReadExisting(const std::filesystem::path& path) {
  std::ifstream in(path);
  if (!in.is_open()) {
    return std::unexpected(
        core::Error(core::ErrorCode::kInternal, "node.id open failed: " + path.string()));
  }
  std::string content;
  std::getline(in, content);
  while (!content.empty() && (content.back() == '\r' || content.back() == '\n')) {
    content.pop_back();
  }
  if (!IsValidUuid(content)) {
    return std::unexpected(
        core::Error(core::ErrorCode::kCorruption, "node.id is not a valid UUID: " + path.string()));
  }
  return content;
}

}  // namespace

core::Result<NodeIdentity> NodeIdentity::Open(const std::filesystem::path& data_dir) {
  std::error_code ec;
  std::filesystem::create_directories(data_dir, ec);
  if (ec) {
    return std::unexpected(FsError(data_dir, "node.id data dir", ec));
  }

  const auto path = data_dir / "node.id";
  if (std::filesystem::exists(path, ec)) {
    auto existing = ReadExisting(path);
    if (!existing.has_value()) {
      return std::unexpected(existing.error());
    }
    return NodeIdentity(std::move(*existing));
  }

  auto fresh = GenerateUuidV4();
  std::string buf = fresh;
  buf.push_back('\n');
  if (auto r = core::WriteFileAtomic(path, buf); !r.has_value()) {
    return std::unexpected(r.error());
  }
  return NodeIdentity(std::move(fresh));
}

}  // namespace abyss::resp
