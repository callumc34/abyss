#include "yaml_decode.h"

#include <string>
#include <unordered_set>

namespace abyss::config::internal {

namespace {

// Safely format a node's source location.
std::string FormatMarkOf(const YAML::Node& node) {
  if (!node.IsDefined()) return {};
  try {
    const auto mark = node.Mark();
    if (mark.line < 0 || mark.column < 0) return {};
    std::string out = " (at line ";
    out += std::to_string(mark.line + 1);
    out += ':';
    out += std::to_string(mark.column + 1);
    out += ')';
    return out;
  } catch (const YAML::Exception&) {
    return {};
  }
}

std::string JoinPath(std::string_view base, std::string_view key) {
  if (base.empty()) return std::string(key);
  std::string out(base);
  out += '.';
  out += key;
  return out;
}

}  // namespace

YamlCursor YamlCursor::Child(std::string_view key) const {
  return {node_[std::string(key)], JoinPath(path_, key)};
}

YamlCursor YamlCursor::Index(size_t index) const {
  std::string sub = path_;
  sub += '[';
  sub += std::to_string(index);
  sub += ']';
  return {node_[index], std::move(sub)};
}

core::Result<void> YamlCursor::RequireMap() const {
  if (!node_.IsDefined() || node_.IsNull()) {
    return std::unexpected(MakeError("expected a mapping, got null or missing value"));
  }
  if (!node_.IsMap()) {
    return std::unexpected(MakeError("expected a mapping"));
  }
  return {};
}

core::Result<void> YamlCursor::RejectUnknownKeys(std::span<const std::string_view> known) const {
  if (!node_.IsMap()) return {};
  const std::unordered_set<std::string_view> allowed(known.begin(), known.end());
  for (const auto& kv : node_) {
    if (!kv.first.IsScalar()) {
      return std::unexpected(MakeError("map key is not a scalar"));
    }
    auto key = kv.first.as<std::string>();
    if (!allowed.contains(key)) {
      auto child = Child(key);
      return std::unexpected(child.MakeError("unknown field; typo or unsupported option"));
    }
  }
  return {};
}

core::Result<void> YamlCursor::RejectUnknownKeys(
    std::initializer_list<std::string_view> known) const {
  return RejectUnknownKeys(std::span<const std::string_view>(known.begin(), known.size()));
}

core::Error YamlCursor::MakeError(std::string_view message) const {
  std::string msg;
  if (!path_.empty()) {
    msg += path_;
    msg += ": ";
  }
  msg += message;
  msg += FormatMarkOf(node_);
  return {core::ErrorCode::kInvalidArgument, std::move(msg)};
}

core::Result<std::string> DecodeString(const YamlCursor& cur) {
  if (!cur.node().IsScalar()) {
    return std::unexpected(cur.MakeError("expected a string scalar"));
  }
  return cur.node().as<std::string>();
}

core::Result<bool> DecodeBool(const YamlCursor& cur) {
  if (!cur.node().IsScalar()) {
    return std::unexpected(cur.MakeError("expected a boolean"));
  }
  try {
    return cur.node().as<bool>();
  } catch (const YAML::Exception&) {
    return std::unexpected(cur.MakeError("expected a boolean"));
  }
}

core::Result<double> DecodeDouble(const YamlCursor& cur) {
  if (!cur.node().IsScalar()) {
    return std::unexpected(cur.MakeError("expected a number"));
  }
  try {
    return cur.node().as<double>();
  } catch (const YAML::Exception&) {
    return std::unexpected(cur.MakeError("expected a number"));
  }
}

core::Result<std::chrono::seconds> DecodeSeconds(const YamlCursor& cur) {
  auto raw = DecodeUnsigned<uint64_t>(cur);
  if (!raw.has_value()) return std::unexpected(raw.error());
  return std::chrono::seconds{*raw};
}

core::Result<std::chrono::milliseconds> DecodeMilliseconds(const YamlCursor& cur) {
  auto raw = DecodeUnsigned<uint64_t>(cur);
  if (!raw.has_value()) return std::unexpected(raw.error());
  return std::chrono::milliseconds{*raw};
}

core::Result<std::chrono::microseconds> DecodeMicroseconds(const YamlCursor& cur) {
  auto raw = DecodeUnsigned<uint64_t>(cur);
  if (!raw.has_value()) return std::unexpected(raw.error());
  return std::chrono::microseconds{*raw};
}

core::Result<core::Durability> DecodeDurability(const YamlCursor& cur) {
  auto name = DecodeString(cur);
  if (!name.has_value()) return std::unexpected(name.error());
  const auto durability = core::ParseDurability(*name);
  if (!durability.has_value()) {
    return std::unexpected(cur.MakeError("must be one of: process_crash, power_loss"));
  }
  return *durability;
}

SectionDecoder::SectionDecoder(YamlCursor cur) : cur_(std::move(cur)) {
  if (auto r = cur_.RequireMap(); !r.has_value()) {
    state_ = std::unexpected(r.error());
  }
}

SectionDecoder& SectionDecoder::Removed(std::string_view key, std::string_view hint) {
  Register(key);
  if (!state_.has_value()) return *this;
  if (auto child = cur_.Child(key); child.node().IsDefined()) {
    state_ = std::unexpected(child.MakeError(hint));
  }
  return *this;
}

core::Result<void> SectionDecoder::Finish() {
  if (!state_.has_value()) return state_;
  return cur_.RejectUnknownKeys(std::span<const std::string_view>(known_));
}

}  // namespace abyss::config::internal
