#include "abyss/config/config.h"

namespace abyss::config {

Config Config::LoadFromFile(const std::string& /*path*/) {
  // Will use yaml-cpp once added as dependency
  return Defaults();
}

Config Config::Defaults() { return {}; }

}  // namespace abyss::config
