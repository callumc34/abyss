#include "abyss/core/resp_types.h"

#include <gtest/gtest.h>

namespace abyss::core {
namespace {

TEST(RespValueTest, Null) {
  auto v = RespValue::Null();
  EXPECT_TRUE(v.IsNull());
  EXPECT_EQ(v.type(), RespValue::Type::kNull);
}

TEST(RespValueTest, String) {
  auto v = RespValue::String("hello");
  EXPECT_TRUE(v.IsString());
  EXPECT_EQ(v.AsString(), "hello");
}

TEST(RespValueTest, Integer) {
  auto v = RespValue::Integer(42);
  EXPECT_TRUE(v.IsInteger());
  EXPECT_EQ(v.AsInteger(), 42);
}

TEST(RespValueTest, Error) {
  auto v = RespValue::Error("ERR something wrong");
  EXPECT_TRUE(v.IsError());
  EXPECT_EQ(v.AsString(), "ERR something wrong");
}

TEST(RespValueTest, Array) {
  auto v = RespValue::Array({
      RespValue::String("one"),
      RespValue::Integer(2),
  });
  EXPECT_TRUE(v.IsArray());
  EXPECT_EQ(v.AsArray().size(), 2);
}

TEST(RespCommandTest, NameAndArgCount) {
  RespCommand cmd{{"SET", "key", "value"}};
  EXPECT_EQ(cmd.Name(), "SET");
  EXPECT_EQ(cmd.ArgCount(), 3);
}

}  // namespace
}  // namespace abyss::core
