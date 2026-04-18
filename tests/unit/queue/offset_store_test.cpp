#include "abyss/queue/offset_store.h"

#include <gtest/gtest.h>

#include <filesystem>
#include <memory>
#include <string>

#include "abyss/queue/file_offset_store.h"
#include "abyss/queue/memory_offset_store.h"

namespace abyss::queue {
namespace {

class OffsetStoreTest : public ::testing::TestWithParam<std::string> {
 protected:
  void SetUp() override {
    auto tmpl = std::filesystem::temp_directory_path() / "abyss_offset_XXXXXX";
    std::string s = tmpl.string();
    ASSERT_NE(::mkdtemp(s.data()), nullptr);
    tmp_dir_ = s;
  }

  void TearDown() override {
    store_.reset();
    if (!tmp_dir_.empty()) {
      std::error_code ec;
      std::filesystem::remove_all(tmp_dir_, ec);
    }
  }

  std::unique_ptr<OffsetStore> MakeStore() {
    if (GetParam() == "memory") {
      return std::make_unique<MemoryOffsetStore>();
    }
    auto result = FileOffsetStore::Open({.directory = tmp_dir_});
    EXPECT_TRUE(result.has_value());
    return std::move(*result);
  }

  // NOLINTBEGIN(cppcoreguidelines-non-private-member-variables-in-classes)
  std::string tmp_dir_;
  std::unique_ptr<OffsetStore> store_;
  // NOLINTEND(cppcoreguidelines-non-private-member-variables-in-classes)
};

TEST_P(OffsetStoreTest, GetBeforeSetReturnsNullopt) {
  store_ = MakeStore();
  EXPECT_FALSE(store_->Get(0, 0).has_value());
}

TEST_P(OffsetStoreTest, SetGetRoundTrip) {
  store_ = MakeStore();
  ASSERT_TRUE(store_->Set(0, 3, 42).has_value());
  auto got = store_->Get(0, 3);
  ASSERT_TRUE(got.has_value());
  EXPECT_EQ(*got, 42U);
}

TEST_P(OffsetStoreTest, MultipleConsumersIndependent) {
  store_ = MakeStore();
  ASSERT_TRUE(store_->Set(0, 0, 100).has_value());
  ASSERT_TRUE(store_->Set(1, 0, 200).has_value());

  EXPECT_EQ(*store_->Get(0, 0), 100U);
  EXPECT_EQ(*store_->Get(1, 0), 200U);
}

TEST_P(OffsetStoreTest, MultipleShardsIndependent) {
  store_ = MakeStore();
  ASSERT_TRUE(store_->Set(0, 0, 10).has_value());
  ASSERT_TRUE(store_->Set(0, 1, 20).has_value());
  ASSERT_TRUE(store_->Set(0, 2, 30).has_value());

  EXPECT_EQ(*store_->Get(0, 0), 10U);
  EXPECT_EQ(*store_->Get(0, 1), 20U);
  EXPECT_EQ(*store_->Get(0, 2), 30U);
}

TEST_P(OffsetStoreTest, SetOverwritesPrevious) {
  store_ = MakeStore();
  ASSERT_TRUE(store_->Set(0, 0, 100).has_value());
  ASSERT_TRUE(store_->Set(0, 0, 200).has_value());
  EXPECT_EQ(*store_->Get(0, 0), 200U);
}

INSTANTIATE_TEST_SUITE_P(Impls, OffsetStoreTest, ::testing::Values("memory", "file"));

// File-specific tests.

class FileOffsetStoreTest : public ::testing::Test {
 protected:
  void SetUp() override {
    auto tmpl = std::filesystem::temp_directory_path() / "abyss_file_offsets_XXXXXX";
    std::string s = tmpl.string();
    ASSERT_NE(::mkdtemp(s.data()), nullptr);
    tmp_dir_ = s;
  }

  void TearDown() override {
    if (!tmp_dir_.empty()) {
      std::error_code ec;
      std::filesystem::remove_all(tmp_dir_, ec);
    }
  }

  std::string tmp_dir_;  // NOLINT(cppcoreguidelines-non-private-member-variables-in-classes)
};

TEST_F(FileOffsetStoreTest, SetPersistsSynchronously) {
  auto store = FileOffsetStore::Open({.directory = tmp_dir_});
  ASSERT_TRUE(store.has_value());
  ASSERT_TRUE((*store)->Set(0, 0, 7).has_value());

  const auto path = std::filesystem::path(tmp_dir_) / "0.offsets";
  EXPECT_TRUE(std::filesystem::exists(path));
}

TEST_F(FileOffsetStoreTest, PersistsAcrossReopen) {
  {
    auto store = FileOffsetStore::Open({.directory = tmp_dir_});
    ASSERT_TRUE(store.has_value());
    ASSERT_TRUE((*store)->Set(0, 5, 123).has_value());
    ASSERT_TRUE((*store)->Set(1, 2, 456).has_value());
  }

  auto store = FileOffsetStore::Open({.directory = tmp_dir_});
  ASSERT_TRUE(store.has_value());
  EXPECT_EQ(*(*store)->Get(0, 5), 123U);
  EXPECT_EQ(*(*store)->Get(1, 2), 456U);
}

TEST_F(FileOffsetStoreTest, CorruptedFileDetected) {
  {
    auto store = FileOffsetStore::Open({.directory = tmp_dir_});
    ASSERT_TRUE(store.has_value());
    ASSERT_TRUE((*store)->Set(0, 0, 100).has_value());
  }

  const auto path = std::filesystem::path(tmp_dir_) / "0.offsets";
  ASSERT_TRUE(std::filesystem::exists(path));
  std::filesystem::resize_file(path, 8);

  auto store = FileOffsetStore::Open({.directory = tmp_dir_});
  ASSERT_FALSE(store.has_value());
  EXPECT_EQ(store.error().code(), core::ErrorCode::kCorruption);
}

}  // namespace
}  // namespace abyss::queue
