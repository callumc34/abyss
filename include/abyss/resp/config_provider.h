#pragma once

#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace abyss::resp {

class ConfigProvider {
 public:
  using Entry = std::pair<std::string_view, std::string>;

  ConfigProvider() = default;
  virtual ~ConfigProvider() = default;
  ConfigProvider(const ConfigProvider&) = delete;
  ConfigProvider& operator=(const ConfigProvider&) = delete;
  ConfigProvider(ConfigProvider&&) = delete;
  ConfigProvider& operator=(ConfigProvider&&) = delete;

  virtual std::vector<Entry> Entries() const = 0;
};

}  // namespace abyss::resp
