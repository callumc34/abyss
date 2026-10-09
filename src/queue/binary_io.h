#pragma once

#include <bit>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <span>
#include <vector>

namespace abyss::queue::binary {

inline constexpr bool kNativeLittleEndian = std::endian::native == std::endian::little;

inline void AppendBytes(std::vector<std::byte>& out, const void* src, size_t n) {
  const auto* p = static_cast<const std::byte*>(src);
  out.insert(out.end(), p, p + n);
}

inline void WriteU8(std::vector<std::byte>& out, uint8_t v) {
  out.push_back(static_cast<std::byte>(v));
}

inline void WriteU16LE(std::vector<std::byte>& out, uint16_t v) {
  if constexpr (!kNativeLittleEndian) {
    v = std::byteswap(v);
  }
  AppendBytes(out, &v, sizeof(v));
}

inline void WriteU32LE(std::vector<std::byte>& out, uint32_t v) {
  if constexpr (!kNativeLittleEndian) {
    v = std::byteswap(v);
  }
  AppendBytes(out, &v, sizeof(v));
}

inline void WriteU64LE(std::vector<std::byte>& out, uint64_t v) {
  if constexpr (!kNativeLittleEndian) {
    v = std::byteswap(v);
  }
  AppendBytes(out, &v, sizeof(v));
}

inline void WriteI64LE(std::vector<std::byte>& out, int64_t v) {
  WriteU64LE(out, std::bit_cast<uint64_t>(v));
}

inline void PatchU32LE(std::vector<std::byte>& out, size_t offset, uint32_t v) {
  if constexpr (!kNativeLittleEndian) {
    v = std::byteswap(v);
  }
  std::memcpy(out.data() + offset, &v, sizeof(v));
}

// Writes into a fixed span. A write past its end is dropped and marks
// the writer overflowed, so a caller checks once at the end.
class SpanWriter {
 public:
  explicit SpanWriter(std::span<std::byte> out) noexcept : out_(out) {}

  void Bytes(const void* src, size_t n) noexcept {
    if (n > out_.size() - at_) {
      overflowed_ = true;
      return;
    }
    if (n > 0) std::memcpy(out_.data() + at_, src, n);
    at_ += n;
  }
  template <typename T>
  void LE(T v) noexcept {
    if constexpr (!kNativeLittleEndian) v = std::byteswap(v);
    Bytes(&v, sizeof(v));
  }

  size_t written() const noexcept { return at_; }
  bool overflowed() const noexcept { return overflowed_; }

 private:
  std::span<std::byte> out_;
  size_t at_ = 0;
  bool overflowed_ = false;
};

template <typename T>
T LoadLE(const std::byte* p) noexcept {
  T v{};
  std::memcpy(&v, p, sizeof(v));
  if constexpr (!kNativeLittleEndian) v = std::byteswap(v);
  return v;
}

template <typename T>
void StoreLE(std::byte* p, T v) noexcept {
  if constexpr (!kNativeLittleEndian) v = std::byteswap(v);
  std::memcpy(p, &v, sizeof(v));
}

inline bool ReadU8(std::span<const std::byte>& bytes, uint8_t& out) {
  if (bytes.empty()) {
    return false;
  }
  out = static_cast<uint8_t>(bytes.front());
  bytes = bytes.subspan(1);
  return true;
}

inline bool ReadU16LE(std::span<const std::byte>& bytes, uint16_t& out) {
  if (bytes.size() < sizeof(uint16_t)) {
    return false;
  }
  std::memcpy(&out, bytes.data(), sizeof(out));
  if constexpr (!kNativeLittleEndian) {
    out = std::byteswap(out);
  }
  bytes = bytes.subspan(sizeof(out));
  return true;
}

inline bool ReadU32LE(std::span<const std::byte>& bytes, uint32_t& out) {
  if (bytes.size() < sizeof(uint32_t)) {
    return false;
  }
  std::memcpy(&out, bytes.data(), sizeof(out));
  if constexpr (!kNativeLittleEndian) {
    out = std::byteswap(out);
  }
  bytes = bytes.subspan(sizeof(out));
  return true;
}

inline bool ReadU64LE(std::span<const std::byte>& bytes, uint64_t& out) {
  if (bytes.size() < sizeof(uint64_t)) {
    return false;
  }
  std::memcpy(&out, bytes.data(), sizeof(out));
  if constexpr (!kNativeLittleEndian) {
    out = std::byteswap(out);
  }
  bytes = bytes.subspan(sizeof(out));
  return true;
}

inline bool ReadI64LE(std::span<const std::byte>& bytes, int64_t& out) {
  uint64_t u = 0;
  if (!ReadU64LE(bytes, u)) {
    return false;
  }
  out = std::bit_cast<int64_t>(u);
  return true;
}

}  // namespace abyss::queue::binary
