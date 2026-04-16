#include "abyss/core/consumer_rpc.h"

#include <gtest/gtest.h>

#include <chrono>
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
  EXPECT_THROW(fut.get(), std::future_error);
}

TEST(ConsumerRpcTest, CancelUnknownIdReturnsFalse) {
  ConsumerRpc rpc;
  EXPECT_FALSE(rpc.Cancel(123));
}

TEST(ConsumerRpcTest, DuplicateRegisterReturnsBrokenFutureAndDoesNotClobberPending) {
  ConsumerRpc rpc;
  auto first = rpc.Register(1);
  auto duplicate = rpc.Register(1);

  EXPECT_THROW(duplicate.get(), std::future_error);
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

  // Fulfil concurrently from a worker thread.
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

}  // namespace
}  // namespace abyss::core
