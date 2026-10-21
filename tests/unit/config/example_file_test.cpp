#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>
#include <istream>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "abyss/config/config.h"

#ifndef ABYSS_EXAMPLE_CONFIG_PATH
#error "ABYSS_EXAMPLE_CONFIG_PATH must be defined at compile time"
#endif
#ifndef ABYSS_DEPLOYMENT_DOC_PATH
#error "ABYSS_DEPLOYMENT_DOC_PATH must be defined at compile time"
#endif

namespace abyss::config {
namespace {

// The yaml blocks of the guide's Configuration Reference section.
std::vector<std::string> ReferenceBlocks(std::istream& doc) {
  std::vector<std::string> blocks;
  std::optional<std::string> block;
  bool in_section = false;
  for (std::string line; std::getline(doc, line);) {
    if (block.has_value()) {
      if (line.starts_with("```")) {
        blocks.push_back(*std::move(block));
        block.reset();
      } else {
        *block += line + "\n";
      }
    } else if (line.starts_with("## ")) {
      in_section = line == "## Configuration Reference";
    } else if (in_section && line == "```yaml") {
      block.emplace();
    }
  }
  return blocks;
}

TEST(ConfigExampleFile, ShippedExampleLoadsCleanly) {
  const std::filesystem::path path{ABYSS_EXAMPLE_CONFIG_PATH};
  ASSERT_TRUE(std::filesystem::exists(path)) << path;

  auto cfg = Config::LoadFromFile(path);
  ASSERT_TRUE(cfg.has_value()) << cfg.error().message();
  EXPECT_EQ(cfg->profile, "embedded");
}

// The deployment guide's config is what operators copy: the strict
// parser must take it as written.
TEST(ConfigExampleFile, DeploymentGuideConfigParses) {
  std::ifstream doc{std::filesystem::path{ABYSS_DEPLOYMENT_DOC_PATH}};
  ASSERT_TRUE(doc.is_open()) << ABYSS_DEPLOYMENT_DOC_PATH;
  const auto blocks = ReferenceBlocks(doc);
  ASSERT_FALSE(blocks.empty()) << "no yaml block under ## Configuration Reference";
  for (const std::string& yaml : blocks) {
    const auto cfg = Config::ParseFromYaml(yaml);
    EXPECT_TRUE(cfg.has_value()) << cfg.error().message() << "\n" << yaml;
  }
}

TEST(ConfigExampleFile, MissingFileReportsPath) {
  auto cfg = Config::LoadFromFile("/nonexistent/abyss-does-not-exist.yaml");
  ASSERT_FALSE(cfg.has_value());
  EXPECT_NE(cfg.error().message().find("/nonexistent/abyss-does-not-exist.yaml"),
            std::string::npos);
}

}  // namespace
}  // namespace abyss::config
