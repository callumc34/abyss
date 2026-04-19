#pragma once

#include <yaml-cpp/yaml.h>

#include <chrono>
#include <concepts>
#include <initializer_list>
#include <limits>
#include <span>
#include <string>
#include <string_view>
#include <type_traits>
#include <vector>

#include "abyss/core/result.h"

namespace abyss::config::internal {

class YamlCursor {
 public:
  explicit YamlCursor(const YAML::Node& root) : node_(root) {}
  YamlCursor(const YAML::Node& node, std::string path) : node_(node), path_(std::move(path)) {}

  const YAML::Node& node() const { return node_; }
  const std::string& path() const { return path_; }

  YamlCursor Child(std::string_view key) const;
  YamlCursor Index(size_t index) const;

  core::Result<void> RequireMap() const;
  core::Result<void> RejectUnknownKeys(std::initializer_list<std::string_view> known) const;
  core::Result<void> RejectUnknownKeys(std::span<const std::string_view> known) const;

  core::Error MakeError(std::string_view message) const;

 private:
  YAML::Node node_;
  std::string path_;
};

core::Result<std::string> DecodeString(const YamlCursor& cur);
core::Result<bool> DecodeBool(const YamlCursor& cur);
core::Result<double> DecodeDouble(const YamlCursor& cur);

// Template decoders are defined inline so any T (including platform-specific
// typedefs like size_t) instantiates on demand without per-type explicit
// instantiations elsewhere.
template <std::unsigned_integral T>
  requires(!std::is_same_v<T, bool>)
core::Result<T> DecodeUnsigned(const YamlCursor& cur) {
  if (!cur.node().IsScalar()) {
    return std::unexpected(cur.MakeError("expected an unsigned integer"));
  }
  uint64_t raw = 0;
  try {
    const auto& text = cur.node().Scalar();
    if (!text.empty() && text.front() == '-') {
      return std::unexpected(cur.MakeError("expected a non-negative integer"));
    }
    raw = cur.node().template as<uint64_t>();
  } catch (const YAML::Exception&) {
    return std::unexpected(cur.MakeError("expected an unsigned integer"));
  }
  if (raw > std::numeric_limits<T>::max()) {
    return std::unexpected(cur.MakeError("value out of range for field"));
  }
  return static_cast<T>(raw);
}

template <std::signed_integral T>
  requires(!std::is_same_v<T, bool>)
core::Result<T> DecodeSigned(const YamlCursor& cur) {
  if (!cur.node().IsScalar()) {
    return std::unexpected(cur.MakeError("expected a signed integer"));
  }
  int64_t raw = 0;
  try {
    raw = cur.node().template as<int64_t>();
  } catch (const YAML::Exception&) {
    return std::unexpected(cur.MakeError("expected a signed integer"));
  }
  if (raw < std::numeric_limits<T>::min() || raw > std::numeric_limits<T>::max()) {
    return std::unexpected(cur.MakeError("value out of range for field"));
  }
  return static_cast<T>(raw);
}

core::Result<std::chrono::seconds> DecodeSeconds(const YamlCursor& cur);
core::Result<std::chrono::milliseconds> DecodeMilliseconds(const YamlCursor& cur);
core::Result<std::chrono::microseconds> DecodeMicroseconds(const YamlCursor& cur);

// DecoderFor<T> maps a destination field type to the decoder that produces
// it. The primary template is intentionally undefined so unsupported T
// produces a compile error at the call site. Supported types are added via
// specialisation or a constrained partial specialisation.
template <typename T>
struct DecoderFor;

template <>
struct DecoderFor<std::string> {
  static core::Result<std::string> Decode(const YamlCursor& c) { return DecodeString(c); }
};
template <>
struct DecoderFor<bool> {
  static core::Result<bool> Decode(const YamlCursor& c) { return DecodeBool(c); }
};
template <>
struct DecoderFor<double> {
  static core::Result<double> Decode(const YamlCursor& c) { return DecodeDouble(c); }
};

// All non-bool unsigned integer types route through DecodeUnsigned<T>. This
// covers uint16_t/uint32_t/uint64_t *and* size_t on platforms where size_t
// is a distinct typedef (macOS arm64: unsigned long vs unsigned long long).
template <std::unsigned_integral T>
  requires(!std::is_same_v<T, bool>)
struct DecoderFor<T> {
  static core::Result<T> Decode(const YamlCursor& c) { return DecodeUnsigned<T>(c); }
};

template <>
struct DecoderFor<std::chrono::seconds> {
  static core::Result<std::chrono::seconds> Decode(const YamlCursor& c) { return DecodeSeconds(c); }
};
template <>
struct DecoderFor<std::chrono::milliseconds> {
  static core::Result<std::chrono::milliseconds> Decode(const YamlCursor& c) {
    return DecodeMilliseconds(c);
  }
};
template <>
struct DecoderFor<std::chrono::microseconds> {
  static core::Result<std::chrono::microseconds> Decode(const YamlCursor& c) {
    return DecodeMicroseconds(c);
  }
};

// Fluent builder for parsing a YAML map into a typed destination struct.
// First error sticks; unknown keys fail the parse in Finish().
class SectionDecoder {
 public:
  explicit SectionDecoder(YamlCursor cur);

  template <typename T>
  SectionDecoder& Optional(std::string_view key, T& out) {
    Register(key);
    if (!state_.has_value()) return *this;
    auto child = cur_.Child(key);
    if (!child.node().IsDefined() || child.node().IsNull()) return *this;
    auto decoded = DecoderFor<T>::Decode(child);
    if (!decoded.has_value()) {
      state_ = std::unexpected(decoded.error());
      return *this;
    }
    out = std::move(*decoded);
    return *this;
  }

  template <typename T>
  SectionDecoder& Required(std::string_view key, T& out) {
    Register(key);
    if (!state_.has_value()) return *this;
    auto child = cur_.Child(key);
    if (!child.node().IsDefined() || child.node().IsNull()) {
      state_ = std::unexpected(child.MakeError("required field is missing"));
      return *this;
    }
    auto decoded = DecoderFor<T>::Decode(child);
    if (!decoded.has_value()) {
      state_ = std::unexpected(decoded.error());
      return *this;
    }
    out = std::move(*decoded);
    return *this;
  }

  // Parse a YAML sequence of T using a caller-provided per-item parser.
  // Absent key → out is left untouched. Missing sequence → error.
  template <typename T, typename ItemParser>
  SectionDecoder& OptionalSequence(std::string_view key, std::vector<T>& out, ItemParser parse) {
    Register(key);
    if (!state_.has_value()) return *this;
    auto seq = cur_.Child(key);
    if (!seq.node().IsDefined() || seq.node().IsNull()) return *this;
    if (!seq.node().IsSequence()) {
      state_ = std::unexpected(seq.MakeError("expected a sequence"));
      return *this;
    }
    out.clear();
    for (size_t i = 0; i < seq.node().size(); ++i) {
      auto item = seq.Index(i);
      T entry;
      if (auto r = parse(item, entry); !r.has_value()) {
        state_ = std::unexpected(r.error());
        return *this;
      }
      out.push_back(std::move(entry));
    }
    return *this;
  }

  core::Result<void> Finish();

 private:
  void Register(std::string_view key) { known_.push_back(key); }

  YamlCursor cur_;
  core::Result<void> state_;
  std::vector<std::string_view> known_;
};

}  // namespace abyss::config::internal
