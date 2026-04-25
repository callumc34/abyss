#pragma once

#include <filesystem>
#include <span>
#include <string_view>

#include "abyss/core/result.h"

namespace abyss::core {

// Crash-safe rename: tmp + write + fsync + rename + fsync(parent dir). On
// crash either the prior file survives or the new contents are durable.
// Creates parent directories if missing.
Result<void> WriteFileAtomic(const std::filesystem::path& path, std::span<const std::byte> bytes);

Result<void> WriteFileAtomic(const std::filesystem::path& path, std::string_view text);

}  // namespace abyss::core
