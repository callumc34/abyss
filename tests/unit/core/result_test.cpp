#include "abyss/core/result.h"

#include <gtest/gtest.h>

namespace abyss::core {
namespace {

TEST(ResultTest, SuccessValue) {
  Result<int> result = 42;
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(*result, 42);
}

TEST(ResultTest, ErrorValue) {
  Result<int> result = std::unexpected(Error(ErrorCode::kNotFound, "not found"));
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code(), ErrorCode::kNotFound);
  EXPECT_EQ(result.error().message(), "not found");
}

TEST(ResultTest, VoidSuccess) {
  Result<void> result;
  ASSERT_TRUE(result.has_value());
}

TEST(ResultTest, VoidError) {
  Result<void> result = std::unexpected(Error(ErrorCode::kInternal, "fail"));
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code(), ErrorCode::kInternal);
}

}  // namespace
}  // namespace abyss::core
