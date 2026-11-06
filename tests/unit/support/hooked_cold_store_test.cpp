#include "hooked_cold_store.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <future>
#include <string_view>
#include <thread>

#include "abyss/core/cold_store.h"
#include "abyss/core/types.h"
#include "latch.h"
#include "mock_cold_store.h"
#include "on_exit.h"

namespace abyss::testing {
namespace {

using std::chrono_literals::operator""ms;
using std::chrono_literals::operator""s;

// A hook cleared while a load runs it: the clear returns only once
// that call has, and no later load calls it.
TEST(HookedColdStoreTest, ClearingAHookWaitsOutItsCallInFlight) {
  ::testing::NiceMock<MockColdStore> cold;
  HookedColdStore hooked([&cold]() -> core::ColdStore& { return cold; });
  Latch entered;
  Latch release;
  std::atomic<int> calls{0};
  std::atomic<bool> returned{false};
  hooked.SetHook([&](std::string_view) {
    calls.fetch_add(1);
    entered.Open();
    (void)release.Wait();
    returned = true;
  });
  std::thread loader(
      [&] { EXPECT_TRUE(hooked.ProbeKey("k", core::SteadyClock::now() + 10s).has_value()); });
  std::promise<bool> cleared;
  std::future<bool> cleared_after_call = cleared.get_future();
  std::thread clearer;
  const OnExit join([&] {
    release.Open();
    if (clearer.joinable()) clearer.join();
    loader.join();
  });
  ASSERT_TRUE(entered.Wait());
  clearer = std::thread([&] {
    hooked.SetHook(nullptr);
    cleared.set_value(returned.load());
  });
  EXPECT_EQ(cleared_after_call.wait_for(50ms), std::future_status::timeout)
      << "the clear returned with the hook still running";
  release.Open();
  EXPECT_TRUE(cleared_after_call.get()) << "the clear returned before the call did";
  EXPECT_TRUE(hooked.ProbeKey("k", core::SteadyClock::now() + 10s).has_value());
  EXPECT_EQ(calls.load(), 1);
}

}  // namespace
}  // namespace abyss::testing
