#include "abyss/resp/node_identity.h"

#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>
#include <string>

#include "temp_dir.h"

namespace abyss::resp {
namespace {

TEST(NodeIdentityTest, GeneratesAndPersists) {
  testing::TempDir dir("node_identity");
  auto first = NodeIdentity::Open(dir.Path());
  ASSERT_TRUE(first.has_value()) << first.error().message();
  EXPECT_EQ(first->Id().size(), 36U);
  EXPECT_TRUE(std::filesystem::exists(dir.Sub("node.id")));
}

TEST(NodeIdentityTest, ReusesExistingId) {
  testing::TempDir dir("node_identity_reuse");
  auto first = NodeIdentity::Open(dir.Path());
  ASSERT_TRUE(first.has_value());
  const std::string id{first->Id()};

  auto second = NodeIdentity::Open(dir.Path());
  ASSERT_TRUE(second.has_value());
  EXPECT_EQ(std::string(second->Id()), id);
}

TEST(NodeIdentityTest, TolerateTrailingNewline) {
  testing::TempDir dir("node_identity_trailing");
  const std::string id = "01234567-89ab-4cde-8f01-234567890abc";
  {
    std::ofstream out(dir.Sub("node.id"));
    out << id << '\n';
  }
  auto loaded = NodeIdentity::Open(dir.Path());
  ASSERT_TRUE(loaded.has_value());
  EXPECT_EQ(std::string(loaded->Id()), id);
}

TEST(NodeIdentityTest, RejectsMalformedId) {
  testing::TempDir dir("node_identity_bad");
  {
    std::ofstream out(dir.Sub("node.id"));
    out << "definitely-not-a-uuid";
  }
  auto loaded = NodeIdentity::Open(dir.Path());
  ASSERT_FALSE(loaded.has_value());
  EXPECT_EQ(loaded.error().code(), core::ErrorCode::kCorruption);
}

TEST(NodeIdentityTest, CreatesDataDirIfMissing) {
  testing::TempDir dir("node_identity_missing");
  const auto nested = dir.Sub("subdir/deeper");
  auto loaded = NodeIdentity::Open(nested);
  ASSERT_TRUE(loaded.has_value());
  EXPECT_TRUE(std::filesystem::exists(nested / "node.id"));
}

}  // namespace
}  // namespace abyss::resp
