#pragma once

#include <cstddef>
#include <functional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>

namespace abyss::core {

// Lets string-keyed containers find a std::string_view without
// building a std::string.
struct StringHash {
  using is_transparent = void;

  size_t operator()(std::string_view s) const noexcept { return std::hash<std::string_view>{}(s); }
};

template <class V>
using StringMap = std::unordered_map<std::string, V, StringHash, std::equal_to<>>;

using StringSet = std::unordered_set<std::string, StringHash, std::equal_to<>>;

}  // namespace abyss::core
