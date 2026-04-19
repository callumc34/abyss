#include "abyss/core/consumer_rpc.h"

#include <gtest/gtest.h>

#include <atomic>
#include <future>
#include <thread>
#include <vector>

namespace abyss::core {
namespace {

using namespace std::chrono_literals;

TEST(ConsumerRpcTest, RegisterAndFulfill) {
  ConsumerRpc rpc;
  auto fut = rpc.Register(42);
  EXPECT_EQ(rpc.PendingCount(), 1U);

  EXPECT_TRUE(rpc.Fulfill(42, RespValue::SimpleString("OK")));
  EXPECT_EQ(rpc.PendingCount(), 0U);

  auto value = fut.get();
  EXPECT_TRUE(value.IsSimpleString());
  EXPECT_EQ(value.AsString(), "OK");
}

TEST(ConsumerRpcTest, FulfillUnknownIdReturnsFalse) {
  ConsumerRpc rpc;
  EXPECT_FALSE(rpc.Fulfill(99, RespValue::Integer(1)));
}

TEST(ConsumerRpcTest, CancelBreaksFuture) {
  ConsumerRpc rpc;
  auto fut = rpc.Register(7);
  EXPECT_TRUE(rpc.Cancel(7));
  EXPECT_EQ(rpc.PendingCount(), 0U);
  EXPECT_THROW((void)fut.get(), std::future_error);
}

// Guards against a regression to destructor-based cancel: the broken_promise
// exception must be set explicitly so it's observable the instant Cancel
// returns, not when the registry eventually destructs.
TEST(ConsumerRpcTest, CancelDeliversExceptionImmediately) {
  ConsumerRpc rpc;
  auto fut = rpc.Register(77);
  ASSERT_TRUE(rpc.Cancel(77));

  EXPECT_EQ(fut.wait_for(0ms), std::future_status::ready);
  EXPECT_THROW((void)fut.get(), std::future_error);
}

TEST(ConsumerRpcTest, CancelUnknownIdReturnsFalse) {
  ConsumerRpc rpc;
  EXPECT_FALSE(rpc.Cancel(123));
}

TEST(ConsumerRpcTest, DuplicateRegisterReturnsBrokenFutureAndDoesNotClobberPending) {
  ConsumerRpc rpc;
  auto first = rpc.Register(1);
  auto duplicate = rpc.Register(1);

  EXPECT_THROW((void)duplicate.get(), std::future_error);
  EXPECT_EQ(rpc.PendingCount(), 1U);

  EXPECT_TRUE(rpc.Fulfill(1, RespValue::Integer(5)));
  EXPECT_EQ(first.get().AsInteger(), 5);
}

TEST(ConsumerRpcTest, FulfillCarriesErrorRespValue) {
  ConsumerRpc rpc;
  auto fut = rpc.Register(10);
  EXPECT_TRUE(rpc.Fulfill(10, RespValue::Error(ErrorPrefix::kWrongType, "bad type")));

  auto value = fut.get();
  EXPECT_TRUE(value.IsError());
  EXPECT_EQ(value.AsString(), "WRONGTYPE bad type");
}

TEST(ConsumerRpcTest, ConcurrentRegisterAndFulfill) {
  ConsumerRpc rpc;
  constexpr int kCount = 200;

  std::vector<std::future<RespValue>> futures;
  futures.reserve(kCount);
  for (int i = 0; i < kCount; ++i) {
    futures.push_back(rpc.Register(static_cast<RpcId>(i)));
  }
  EXPECT_EQ(rpc.PendingCount(), static_cast<size_t>(kCount));

  std::thread fulfiller([&rpc]() {
    for (int i = 0; i < kCount; ++i) {
      rpc.Fulfill(static_cast<RpcId>(i), RespValue::Integer(i));
    }
  });

  for (int i = 0; i < kCount; ++i) {
    auto value = futures[i].get();
    EXPECT_EQ(value.AsInteger(), i);
  }
  fulfiller.join();
  EXPECT_EQ(rpc.PendingCount(), 0U);
}

TEST(ConsumerRpcTest, FutureCanBeAwaitedWithTimeout) {
  ConsumerRpc rpc;
  auto fut = rpc.Register(1);

  auto status = fut.wait_for(10ms);
  EXPECT_EQ(status, std::future_status::timeout);

  rpc.Fulfill(1, RespValue::SimpleString("OK"));
  status = fut.wait_for(1s);
  EXPECT_EQ(status, std::future_status::ready);
}

TEST(ConsumerRpcTest, DefaultShardCountMatchesConfig) {
  ConsumerRpc rpc;
  EXPECT_EQ(rpc.registry_shard_count(), 16U);
}

TEST(ConsumerRpcTest, ConfigurableShardCount) {
  ConsumerRpc rpc({.registry_shard_count = 4});
  EXPECT_EQ(rpc.registry_shard_count(), 4U);
}

TEST(ConsumerRpcTest, ZeroShardCountClampsToOne) {
  ConsumerRpc rpc({.registry_shard_count = 0});
  EXPECT_EQ(rpc.registry_shard_count(), 1U);

  auto fut = rpc.Register(17);
  EXPECT_TRUE(rpc.Fulfill(17, RespValue::Integer(17)));
  EXPECT_EQ(fut.get().AsInteger(), 17);
}

TEST(ConsumerRpcTest, IdsMappingToSameShardStayIndependent) {
  // With 4 shards, ids that share `id % 4` live in the same bucket.
  // Registration and fulfilment must stay per-id, not per-shard.
  ConsumerRpc rpc({.registry_shard_count = 4});

  auto fut_a = rpc.Register(8);   // shard 0
  auto fut_b = rpc.Register(12);  // shard 0

  EXPECT_EQ(rpc.PendingCount(), 2U);
  EXPECT_TRUE(rpc.Fulfill(12, RespValue::Integer(12)));
  EXPECT_EQ(fut_b.get().AsInteger(), 12);
  EXPECT_EQ(rpc.PendingCount(), 1U);

  EXPECT_TRUE(rpc.Fulfill(8, RespValue::Integer(8)));
  EXPECT_EQ(fut_a.get().AsInteger(), 8);
  EXPECT_EQ(rpc.PendingCount(), 0U);
}

// Many producers and fulfillers hammering the striped registry concurrently.
// Deadlocks, missed fulfilments, or cross-id corruption would show up as
// hangs, wrong values, or stale pending entries.
TEST(ConsumerRpcTest, StripedContentionStress) {
  ConsumerRpc rpc({.registry_shard_count = 8});

  constexpr int kProducers = 8;
  constexpr int kIdsPerProducer = 500;
  constexpr int kTotal = kProducers * kIdsPerProducer;

  std::atomic<int> observed_ok{0};
  std::vector<std::thread> threads;
  threads.reserve(static_cast<size_t>(kProducers) * 2);

  for (int p = 0; p < kProducers; ++p) {
    threads.emplace_back([&, p]() {
      std::vector<std::future<RespValue>> futures;
      futures.reserve(kIdsPerProducer);
      for (int i = 0; i < kIdsPerProducer; ++i) {
        const auto id =
            static_cast<RpcId>((static_cast<uint64_t>(p) << 32) | static_cast<uint32_t>(i));
        futures.push_back(rpc.Register(id));
      }
      for (int i = 0; i < kIdsPerProducer; ++i) {
        const int expected = (p * 1000) + i;
        if (futures[i].get().AsInteger() == expected) {
          observed_ok.fetch_add(1, std::memory_order_relaxed);
        }
      }
    });
    threads.emplace_back([&, p]() {
      for (int i = 0; i < kIdsPerProducer; ++i) {
        const auto id =
            static_cast<RpcId>((static_cast<uint64_t>(p) << 32) | static_cast<uint32_t>(i));
        const int value = (p * 1000) + i;
        // Spin until the producer has registered.
        while (!rpc.Fulfill(id, RespValue::Integer(value))) {
          std::this_thread::yield();
        }
      }
    });
  }

  for (auto& t : threads) t.join();

  EXPECT_EQ(observed_ok.load(), kTotal);
  EXPECT_EQ(rpc.PendingCount(), 0U);
}

TEST(ConsumerRpcTest, DefaultTimeoutAccessible) {
  ConsumerRpc rpc({.default_timeout = 250ms});
  EXPECT_EQ(rpc.default_timeout(), 250ms);
}

}  // namespace
}  // namespace abyss::core
