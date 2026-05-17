#include <thread>

#include "server_fixture.h"

namespace abyss::system_test {
namespace {

class TieringTest : public DataCommandTest {};

TEST_F(TieringTest, HotStoreServesRead) {
  EXPECT_TRUE(Client().Command({"SET", "k", "v"}).IsOk());
  auto r = Client().Command({"GET", "k"});
  ASSERT_TRUE(r.IsBulk());
  EXPECT_EQ(r.String(), "v");
}

TEST_F(TieringTest, ConsistencyAfterBulkWrites) {
  constexpr int kKeys = 1000;
  for (int i = 0; i < kKeys; ++i) {
    std::string key = "bulk_" + std::to_string(i);
    std::string val = "val_" + std::to_string(i);
    ASSERT_TRUE(Client().Command({"SET", key, val}).IsOk()) << "failed at key " << i;
  }
  for (int i = 0; i < kKeys; ++i) {
    std::string key = "bulk_" + std::to_string(i);
    std::string expected = "val_" + std::to_string(i);
    auto r = Client().Command({"GET", key});
    ASSERT_TRUE(r.IsBulk()) << "key " << key << " not found";
    EXPECT_EQ(r.String(), expected);
  }
}

TEST_F(TieringTest, EvictionUnderMemoryPressure) {
  std::string big_val(64UL * 1024UL, 'x');
  constexpr int kKeys = 200;

  for (int i = 0; i < kKeys; ++i) {
    std::string key = "evict_" + std::to_string(i);
    auto r = Client().Command({"SET", key, big_val});
    ASSERT_TRUE(r.IsOk()) << "failed at key " << i;
  }

  int found = 0;
  for (int i = 0; i < kKeys; ++i) {
    std::string key = "evict_" + std::to_string(i);
    auto r = Client().Command({"GET", key});
    if (r.IsBulk()) ++found;
  }
  EXPECT_EQ(found, kKeys) << "all keys should be readable (from hot or cold)";
}

TEST_F(TieringTest, TtlExpiryRemovesKey) {
  // TTL sized to outlast the SET→GET round trip under TSAN/ASAN, where the
  // round trip alone can exceed a tight TTL and lazy expiry trips the first
  // GET. The 2 s / 2.5 s pair is the smallest stable budget; the assertion
  // is about expiry semantics, not timing precision.
  EXPECT_TRUE(Client().Command({"SET", "k", "v", "PX", "2000"}).IsOk());

  auto before = Client().Command({"GET", "k"});
  EXPECT_TRUE(before.IsBulk());

  std::this_thread::sleep_for(std::chrono::milliseconds{2500});

  auto after = Client().Command({"GET", "k"});
  EXPECT_TRUE(after.IsNil()) << "key should have expired";
}

TEST_F(TieringTest, OverwriteAfterEviction) {
  EXPECT_TRUE(Client().Command({"SET", "k", "original"}).IsOk());
  EXPECT_TRUE(Client().Command({"SET", "k", "updated"}).IsOk());
  auto r = Client().Command({"GET", "k"});
  ASSERT_TRUE(r.IsBulk());
  EXPECT_EQ(r.String(), "updated");
}

}  // namespace
}  // namespace abyss::system_test
