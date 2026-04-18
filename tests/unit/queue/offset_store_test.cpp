#include "abyss/queue/offset_store.h"

#include <fcntl.h>
#include <gtest/gtest.h>
#include <unistd.h>

#include <cstdlib>
#include <filesystem>
#include <iomanip>
#include <memory>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

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

  std::string ShardFilePath(core::ConsumerId consumer, core::ShardId shard) const {
    std::ostringstream oss;
    oss << std::setw(20) << std::setfill('0') << shard << ".offset";
    return (std::filesystem::path(tmp_dir_) / std::to_string(consumer) / oss.str()).string();
  }

  std::string tmp_dir_;  // NOLINT(cppcoreguidelines-non-private-member-variables-in-classes)
};

TEST_F(FileOffsetStoreTest, SetPersistsSynchronouslyToPerShardFile) {
  auto store = FileOffsetStore::Open({.directory = tmp_dir_});
  ASSERT_TRUE(store.has_value());
  ASSERT_TRUE((*store)->Set(0, 0, 7).has_value());

  EXPECT_TRUE(std::filesystem::exists(ShardFilePath(0, 0)));
}

TEST_F(FileOffsetStoreTest, SetsDifferentShardsCreateSeparateFiles) {
  auto store = FileOffsetStore::Open({.directory = tmp_dir_});
  ASSERT_TRUE(store.has_value());
  ASSERT_TRUE((*store)->Set(0, 0, 1).has_value());
  ASSERT_TRUE((*store)->Set(0, 1, 2).has_value());
  ASSERT_TRUE((*store)->Set(0, 7, 3).has_value());

  EXPECT_TRUE(std::filesystem::exists(ShardFilePath(0, 0)));
  EXPECT_TRUE(std::filesystem::exists(ShardFilePath(0, 1)));
  EXPECT_TRUE(std::filesystem::exists(ShardFilePath(0, 7)));
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

TEST_F(FileOffsetStoreTest, CorruptedRecordDetected) {
  {
    auto store = FileOffsetStore::Open({.directory = tmp_dir_});
    ASSERT_TRUE(store.has_value());
    ASSERT_TRUE((*store)->Set(0, 0, 100).has_value());
  }

  std::filesystem::resize_file(ShardFilePath(0, 0), 8);

  auto store = FileOffsetStore::Open({.directory = tmp_dir_});
  ASSERT_FALSE(store.has_value());
  EXPECT_EQ(store.error().code(), core::ErrorCode::kCorruption);
}

TEST_F(FileOffsetStoreTest, CrcMismatchDetected) {
  {
    auto store = FileOffsetStore::Open({.directory = tmp_dir_});
    ASSERT_TRUE(store.has_value());
    ASSERT_TRUE((*store)->Set(0, 0, 100).has_value());
  }

  // Flip a byte in the seq field (offset 16).
  const auto path = ShardFilePath(0, 0);
  // NOLINTNEXTLINE(cppcoreguidelines-pro-type-vararg)
  const int fd = ::open(path.c_str(), O_RDWR);
  ASSERT_GE(fd, 0);
  uint8_t byte = 0;
  ASSERT_EQ(::pread(fd, &byte, 1, 16), 1);
  byte ^= 0x01;
  ASSERT_EQ(::pwrite(fd, &byte, 1, 16), 1);
  ::close(fd);

  auto store = FileOffsetStore::Open({.directory = tmp_dir_});
  ASSERT_FALSE(store.has_value());
  EXPECT_EQ(store.error().code(), core::ErrorCode::kCorruption);
}

TEST_F(FileOffsetStoreTest, ConcurrentSetsAcrossShardsSameConsumerAllPersist) {
  // Models shard-per-core: N threads, each handling one (fixed) shard of the
  // same consumer, concurrently acking. Every write must land in its own file
  // with no cross-shard interference.
  constexpr int kShards = 16;
  constexpr int kSetsPerShard = 100;

  auto store = FileOffsetStore::Open({.directory = tmp_dir_});
  ASSERT_TRUE(store.has_value());

  std::vector<std::thread> threads;
  threads.reserve(kShards);
  for (int s = 0; s < kShards; ++s) {
    threads.emplace_back([&store, s] {
      for (int i = 0; i < kSetsPerShard; ++i) {
        ASSERT_TRUE((*store)->Set(0, s, i + 1).has_value());
      }
    });
  }
  for (auto& t : threads) t.join();

  for (int s = 0; s < kShards; ++s) {
    auto got = (*store)->Get(0, s);
    ASSERT_TRUE(got.has_value()) << "shard " << s;
    EXPECT_EQ(*got, static_cast<core::SequenceId>(kSetsPerShard)) << "shard " << s;
  }

  // And the per-shard values must survive reopen.
  store->reset();
  auto reopened = FileOffsetStore::Open({.directory = tmp_dir_});
  ASSERT_TRUE(reopened.has_value());
  for (int s = 0; s < kShards; ++s) {
    auto got = (*reopened)->Get(0, s);
    ASSERT_TRUE(got.has_value()) << "shard " << s;
    EXPECT_EQ(*got, static_cast<core::SequenceId>(kSetsPerShard)) << "shard " << s;
  }
}

TEST_F(FileOffsetStoreTest, ConcurrentSetsAcrossConsumersSameShardAllPersist) {
  constexpr int kConsumers = 8;
  constexpr int kSetsPerConsumer = 100;

  auto store = FileOffsetStore::Open({.directory = tmp_dir_});
  ASSERT_TRUE(store.has_value());

  std::vector<std::thread> threads;
  threads.reserve(kConsumers);
  for (int c = 0; c < kConsumers; ++c) {
    threads.emplace_back([&store, c] {
      for (int i = 0; i < kSetsPerConsumer; ++i) {
        ASSERT_TRUE((*store)->Set(c, 0, i + 1).has_value());
      }
    });
  }
  for (auto& t : threads) t.join();

  store->reset();
  auto reopened = FileOffsetStore::Open({.directory = tmp_dir_});
  ASSERT_TRUE(reopened.has_value());
  for (int c = 0; c < kConsumers; ++c) {
    auto got = (*reopened)->Get(c, 0);
    ASSERT_TRUE(got.has_value()) << "consumer " << c;
    EXPECT_EQ(*got, static_cast<core::SequenceId>(kSetsPerConsumer)) << "consumer " << c;
  }
}

}  // namespace
}  // namespace abyss::queue
