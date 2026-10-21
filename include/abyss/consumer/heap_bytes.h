#pragma once

#include <algorithm>
#include <cstddef>
#include <string>

namespace abyss::consumer {

// A glibc chunk: the request and a size word, in 16-byte steps, and
// never under its minimum chunk.
constexpr size_t AllocBytes(size_t n) {
  constexpr size_t kMinChunk = 4 * sizeof(void*);
  return std::max((n + sizeof(void*) + 15) & ~size_t{15}, kMinChunk);
}

inline size_t StringHeapBytes(size_t size) {
  constexpr size_t kInline = std::string().capacity();
  return size > kInline ? AllocBytes(size + 1) : 0;
}

inline constexpr size_t kHashNodeLinks = 2 * sizeof(void*);

}  // namespace abyss::consumer
