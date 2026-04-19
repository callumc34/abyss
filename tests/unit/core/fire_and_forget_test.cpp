#include "abyss/core/fire_and_forget.h"

#include <gtest/gtest.h>

#include <atomic>

namespace abyss::core {
namespace {

TEST(FireAndForget, SuccessDoesNotIncrementCounter) {
  std::atomic<uint64_t> counter{0};
  FireAndForget(Result<int>{42}, counter);
  EXPECT_EQ(counter.load(), 0U);
}

TEST(FireAndForget, FailureIncrementsCounter) {
  std::atomic<uint64_t> counter{0};
  FireAndForget(Result<int>{std::unexpected(Error{ErrorCode::kInternal, "boom"})}, counter);
  EXPECT_EQ(counter.load(), 1U);
}

TEST(FireAndForget, AcceptsVoidResult) {
  std::atomic<uint64_t> counter{0};
  FireAndForget(Result<void>{}, counter);
  EXPECT_EQ(counter.load(), 0U);

  FireAndForget(Result<void>{std::unexpected(Error{ErrorCode::kUnavailable, "x"})}, counter);
  EXPECT_EQ(counter.load(), 1U);
}

}  // namespace
}  // namespace abyss::core
