#pragma once

#include <filesystem>
#include <string>
#include <string_view>

#include "abyss/core/result.h"

namespace abyss::resp {

// Stable node UUID persisted at <data_dir>/node.id across restarts.
// Read-only after Open.
class NodeIdentity {
 public:
  static core::Result<NodeIdentity> Open(const std::filesystem::path& data_dir);

  explicit NodeIdentity(std::string id) : id_(std::move(id)) {}

  std::string_view Id() const noexcept { return id_; }

 private:
  std::string id_;
};

}  // namespace abyss::resp
