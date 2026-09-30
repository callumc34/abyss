#include "abyss/core/topology_manifest.h"

#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>
#include <string>

#include "abyss/core/result.h"
#include "temp_dir.h"

namespace abyss::core {
namespace {

TopologyDescriptor MakeDescriptor() {
  return TopologyDescriptor{
      .shard_count = 64,
      .cold_format_epoch = 3,
      .wire_slot_hash = kWireSlotScheme,
      .data_shard_hash = kDataShardScheme,
  };
}

TEST(TopologyManifestTest, FreshDirWritesThenReadsBack) {
  testing::TempDir dir("topology");
  const auto effective = MakeDescriptor();

  auto first = TopologyManifest::OpenOrValidate(dir.Path(), effective);
  ASSERT_TRUE(first.has_value()) << first.error().message();
  EXPECT_EQ(first->descriptor().shard_count, effective.shard_count);
  EXPECT_EQ(first->descriptor().cold_format_epoch, effective.cold_format_epoch);
  EXPECT_EQ(first->descriptor().wire_slot_hash, effective.wire_slot_hash);
  EXPECT_EQ(first->descriptor().data_shard_hash, effective.data_shard_hash);

  EXPECT_TRUE(std::filesystem::exists(dir.Path() / "topology.manifest"));

  // A second open with the identical descriptor validates and reads back.
  auto second = TopologyManifest::OpenOrValidate(dir.Path(), effective);
  ASSERT_TRUE(second.has_value()) << second.error().message();
  EXPECT_EQ(second->descriptor().shard_count, effective.shard_count);
}

TEST(TopologyManifestTest, WriteIsDurableAtomicFile) {
  // WriteFileAtomic leaves no temp sibling and produces a complete file.
  testing::TempDir dir("topology");
  ASSERT_TRUE(TopologyManifest::OpenOrValidate(dir.Path(), MakeDescriptor()).has_value());

  int regular = 0;
  for (const auto& e : std::filesystem::directory_iterator(dir.Path())) {
    EXPECT_EQ(e.path().filename().string(), "topology.manifest")
        << "unexpected leftover file: " << e.path();
    ++regular;
  }
  EXPECT_EQ(regular, 1);
}

TEST(TopologyManifestTest, ChangedShardCountRefusesToStart) {
  testing::TempDir dir("topology");
  auto effective = MakeDescriptor();
  ASSERT_TRUE(TopologyManifest::OpenOrValidate(dir.Path(), effective).has_value());

  effective.shard_count = 32;
  auto reopened = TopologyManifest::OpenOrValidate(dir.Path(), effective);
  ASSERT_FALSE(reopened.has_value());
  EXPECT_EQ(reopened.error().code(), ErrorCode::kCorruption);
  const std::string msg = reopened.error().message();
  EXPECT_NE(msg.find("shard_count"), std::string::npos) << msg;
  EXPECT_NE(msg.find("64"), std::string::npos) << msg;
  EXPECT_NE(msg.find("32"), std::string::npos) << msg;
}

TEST(TopologyManifestTest, ChangedColdFormatEpochRefusesToStart) {
  testing::TempDir dir("topology");
  auto effective = MakeDescriptor();
  ASSERT_TRUE(TopologyManifest::OpenOrValidate(dir.Path(), effective).has_value());

  effective.cold_format_epoch = 2;
  auto reopened = TopologyManifest::OpenOrValidate(dir.Path(), effective);
  ASSERT_FALSE(reopened.has_value());
  EXPECT_EQ(reopened.error().code(), ErrorCode::kCorruption);
  EXPECT_NE(reopened.error().message().find("cold_format_epoch"), std::string::npos);
}

TEST(TopologyManifestTest, ChangedWireHashRefusesToStart) {
  testing::TempDir dir("topology");
  auto effective = MakeDescriptor();
  ASSERT_TRUE(TopologyManifest::OpenOrValidate(dir.Path(), effective).has_value());

  effective.wire_slot_hash = "crc16-some-other";
  auto reopened = TopologyManifest::OpenOrValidate(dir.Path(), effective);
  ASSERT_FALSE(reopened.has_value());
  EXPECT_EQ(reopened.error().code(), ErrorCode::kCorruption);
  EXPECT_NE(reopened.error().message().find("wire_slot_hash"), std::string::npos);
}

TEST(TopologyManifestTest, ChangedDataShardHashRefusesToStart) {
  testing::TempDir dir("topology");
  auto effective = MakeDescriptor();
  ASSERT_TRUE(TopologyManifest::OpenOrValidate(dir.Path(), effective).has_value());

  effective.data_shard_hash = "xxh3-64";  // the rejected legacy scheme
  auto reopened = TopologyManifest::OpenOrValidate(dir.Path(), effective);
  ASSERT_FALSE(reopened.has_value());
  EXPECT_EQ(reopened.error().code(), ErrorCode::kCorruption);
  EXPECT_NE(reopened.error().message().find("data_shard_hash"), std::string::npos);
}

TEST(TopologyManifestTest, NewerManifestVersionRefused) {
  testing::TempDir dir("topology");
  const auto path = dir.Path() / "topology.manifest";
  {
    std::ofstream out(path, std::ios::binary);
    out << "manifest_version=999\n"
        << "shard_count=64\n"
        << "cold_format_epoch=3\n"
        << "wire_slot_hash=" << kWireSlotScheme << "\n"
        << "data_shard_hash=" << kDataShardScheme << "\n";
  }
  auto opened = TopologyManifest::OpenOrValidate(dir.Path(), MakeDescriptor());
  ASSERT_FALSE(opened.has_value());
  EXPECT_EQ(opened.error().code(), ErrorCode::kCorruption);
  EXPECT_NE(opened.error().message().find("newer"), std::string::npos);
}

TEST(TopologyManifestTest, MalformedManifestRefused) {
  testing::TempDir dir("topology");
  const auto path = dir.Path() / "topology.manifest";
  {
    std::ofstream out(path, std::ios::binary);
    out << "this is not a manifest\n";
  }
  auto opened = TopologyManifest::OpenOrValidate(dir.Path(), MakeDescriptor());
  ASSERT_FALSE(opened.has_value());
  EXPECT_EQ(opened.error().code(), ErrorCode::kCorruption);
}

}  // namespace
}  // namespace abyss::core
