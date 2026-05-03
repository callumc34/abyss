#include "json_writer.h"

#include <gtest/gtest.h>

namespace abyss::admin::internal {
namespace {

TEST(JsonWriterTest, EmptyObject) {
  JsonWriter w;
  w.BeginObject();
  w.EndObject();
  EXPECT_EQ(w.Finish(), "{}");
}

TEST(JsonWriterTest, FlatObject) {
  JsonWriter w;
  w.BeginObject();
  w.Key("a");
  w.UInt(1);
  w.Key("b");
  w.String("hello");
  w.Key("c");
  w.Bool(true);
  w.EndObject();
  EXPECT_EQ(w.Finish(), R"({"a":1,"b":"hello","c":true})");
}

TEST(JsonWriterTest, NestedObject) {
  JsonWriter w;
  w.BeginObject();
  w.Key("outer");
  w.BeginObject();
  w.Key("inner");
  w.UInt(42);
  w.EndObject();
  w.EndObject();
  EXPECT_EQ(w.Finish(), R"({"outer":{"inner":42}})");
}

TEST(JsonWriterTest, Array) {
  JsonWriter w;
  w.BeginArray();
  w.UInt(1);
  w.UInt(2);
  w.UInt(3);
  w.EndArray();
  EXPECT_EQ(w.Finish(), "[1,2,3]");
}

TEST(JsonWriterTest, EscapesNamedControlChars) {
  JsonWriter w;
  w.String("\b\f\n\r\t");
  EXPECT_EQ(w.Finish(), "\"\\b\\f\\n\\r\\t\"");
}

TEST(JsonWriterTest, EscapesGenericControlCharsAsUnicode) {
  JsonWriter w;
  w.String("\x01\x1f");
  EXPECT_EQ(w.Finish(), "\"\\u0001\\u001f\"");
}

TEST(JsonWriterTest, EscapesQuotesAndBackslash) {
  JsonWriter w;
  w.String(R"(say "hi" \ go)");
  EXPECT_EQ(w.Finish(), R"("say \"hi\" \\ go")");
}

TEST(JsonWriterTest, NullAndBool) {
  JsonWriter w;
  w.BeginObject();
  w.Key("n");
  w.Null();
  w.Key("t");
  w.Bool(true);
  w.Key("f");
  w.Bool(false);
  w.EndObject();
  EXPECT_EQ(w.Finish(), R"({"n":null,"t":true,"f":false})");
}

TEST(JsonWriterTest, IntSigned) {
  JsonWriter w;
  w.Int(-42);
  EXPECT_EQ(w.Finish(), "-42");
}

TEST(JsonWriterTest, ResetsAfterFinish) {
  JsonWriter w;
  w.BeginObject();
  w.Key("a");
  w.UInt(1);
  w.EndObject();
  EXPECT_EQ(w.Finish(), R"({"a":1})");

  w.BeginObject();
  w.Key("b");
  w.UInt(2);
  w.EndObject();
  EXPECT_EQ(w.Finish(), R"({"b":2})");
}

}  // namespace
}  // namespace abyss::admin::internal
