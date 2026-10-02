#include "entry_payload.h"

#include <gtest/gtest.h>

#include <cstddef>
#include <span>
#include <string>
#include <string_view>
#include <variant>
#include <vector>

#include "abyss/core/queue_entry.h"
#include "binary_io.h"

namespace abyss::queue::entry_payload {
namespace {

core::Result<Payload> RoundTrip(const core::QueueEntry& entry) {
  std::vector<std::byte> bytes;
  Encode(entry, bytes);
  std::span<const std::byte> cursor(bytes);
  auto decoded = Decode(TypeOf(entry), cursor);
  EXPECT_TRUE(cursor.empty()) << cursor.size() << " bytes left after the payload";
  return decoded;
}

std::vector<std::string> WriteArgs(const core::Result<Payload>& payload) {
  EXPECT_TRUE(payload.has_value());
  if (!payload.has_value()) return {};
  const auto* write = std::get_if<core::entry::Write>(&*payload);
  EXPECT_NE(write, nullptr);
  return write != nullptr ? write->cmd.args : std::vector<std::string>{};
}

// A Resolved payload with no ops whose return value is `resp` verbatim.
std::vector<std::byte> ResolvedWithReturn(std::string_view resp) {
  std::vector<std::byte> out;
  binary::WriteU64LE(out, 0);
  binary::WriteU8(out, 0);
  binary::WriteU32LE(out, 0);
  binary::WriteU32LE(out, static_cast<uint32_t>(resp.size()));
  binary::AppendBytes(out, resp.data(), resp.size());
  return out;
}

TEST(EntryPayloadTest, RoundTripsWritesOfManyLargeAndEmptyArgs) {
  std::vector<std::string> many{"MSET"};
  for (int i = 0; i < 200; ++i) many.push_back("k" + std::to_string(i));
  const std::vector<std::vector<std::string>> cases{
      {"PING"}, many, {"SET", "k", std::string(std::size_t{1} << 20, 'x')}, {"SET", "", ""}};
  for (const auto& args : cases) {
    const core::QueueEntry entry{.payload = core::entry::Write{.cmd = core::RespCommand{args}}};
    EXPECT_EQ(WriteArgs(RoundTrip(entry)), args);
  }
}

TEST(EntryPayloadTest, RoundTripsResolvedSkipsAndMultipleOps) {
  const core::QueueEntry skip{.payload = core::entry::Resolved{
                                  .ref = 10,
                                  .decision = core::Decision::kSkip,
                                  .return_value = core::RespValue::Null(),
                              }};
  auto skipped = RoundTrip(skip);
  ASSERT_TRUE(skipped.has_value()) << skipped.error().message();
  const auto& s = std::get<core::entry::Resolved>(*skipped);
  EXPECT_EQ(s.decision, core::Decision::kSkip);
  EXPECT_TRUE(s.materialised_ops.empty());
  EXPECT_TRUE(s.return_value.IsNull());

  const core::QueueEntry apply{
      .payload = core::entry::Resolved{
          .ref = 25,
          .decision = core::Decision::kApply,
          .materialised_ops = {core::RespCommand{{"DEL", "src"}},
                               core::RespCommand{{"SET", "dst", "v"}},
                               core::RespCommand{{"PEXPIREAT", "dst", "1700000000000"}}},
          .return_value = core::RespValue::Integer(1),
      }};
  auto applied = RoundTrip(apply);
  ASSERT_TRUE(applied.has_value()) << applied.error().message();
  const auto& a = std::get<core::entry::Resolved>(*applied);
  EXPECT_EQ(a.ref, 25U);
  ASSERT_EQ(a.materialised_ops.size(), 3U);
  EXPECT_EQ(a.materialised_ops[2].args,
            (std::vector<std::string>{"PEXPIREAT", "dst", "1700000000000"}));
  EXPECT_EQ(a.return_value.AsInteger(), 1);
}

TEST(EntryPayloadTest, AnUnknownTypeIsCorruption) {
  const std::vector<std::byte> bytes;
  std::span<const std::byte> cursor(bytes);
  // NOLINTNEXTLINE(clang-analyzer-optin.core.EnumCastOutOfRange): the point.
  auto decoded = Decode(static_cast<EntryType>(0xFF), cursor);
  ASSERT_FALSE(decoded.has_value());
  EXPECT_EQ(decoded.error().code(), core::ErrorCode::kCorruption);
}

TEST(EntryPayloadTest, ATruncatedPayloadIsCorruption) {
  const core::QueueEntry entry{
      .payload = core::entry::Write{.cmd = core::RespCommand{{"SET", "key", "value"}}}};
  std::vector<std::byte> bytes;
  Encode(entry, bytes);
  for (std::size_t keep = 0; keep < bytes.size(); ++keep) {
    std::span<const std::byte> cursor(bytes.data(), keep);
    auto decoded = Decode(EntryType::kWrite, cursor);
    ASSERT_FALSE(decoded.has_value()) << "kept " << keep;
    EXPECT_EQ(decoded.error().code(), core::ErrorCode::kCorruption);
  }
}

// A return value the RESP parser cannot frame is corruption, never a
// silent nil.
TEST(EntryPayloadTest, AnUnparseableReturnValueIsCorruption) {
  const auto bytes = ResolvedWithReturn("+unterminated");
  std::span<const std::byte> cursor(bytes);
  auto decoded = Decode(EntryType::kResolved, cursor);
  ASSERT_FALSE(decoded.has_value());
  EXPECT_EQ(decoded.error().code(), core::ErrorCode::kCorruption);
  EXPECT_NE(decoded.error().message().find("return_value"), std::string::npos);
}

// The bounded parser refuses a huge declared count before allocating.
TEST(EntryPayloadTest, AHugeInnerArrayCountIsCorruptionNotACrash) {
  const auto bytes = ResolvedWithReturn("*2000000000\r\n");
  std::span<const std::byte> cursor(bytes);
  auto decoded = Decode(EntryType::kResolved, cursor);
  ASSERT_FALSE(decoded.has_value());
  EXPECT_EQ(decoded.error().code(), core::ErrorCode::kCorruption);
}

// A count the payload cannot hold is refused before it sizes an
// allocation: reserving it throws bad_alloc on Windows.
TEST(EntryPayloadTest, ACountBeyondThePayloadIsCorruptionNotABadAlloc) {
  std::vector<std::byte> args;
  binary::WriteU32LE(args, 0xFFFF'FFFF);
  std::span<const std::byte> cursor(args);
  auto write = Decode(EntryType::kWrite, cursor);
  ASSERT_FALSE(write.has_value());
  EXPECT_EQ(write.error().code(), core::ErrorCode::kCorruption);

  std::vector<std::byte> ops;
  binary::WriteU64LE(ops, 0);
  binary::WriteU8(ops, 0);
  binary::WriteU32LE(ops, 0xFFFF'FFFF);
  cursor = std::span<const std::byte>(ops);
  auto resolved = Decode(EntryType::kResolved, cursor);
  ASSERT_FALSE(resolved.has_value());
  EXPECT_EQ(resolved.error().code(), core::ErrorCode::kCorruption);
}

TEST(EntryPayloadTest, AValidReturnValueDecodes) {
  const auto bytes = ResolvedWithReturn("+OK\r\n");
  std::span<const std::byte> cursor(bytes);
  auto decoded = Decode(EntryType::kResolved, cursor);
  ASSERT_TRUE(decoded.has_value()) << decoded.error().message();
  EXPECT_EQ(std::get<core::entry::Resolved>(*decoded).return_value.AsString(), "OK");
}

}  // namespace
}  // namespace abyss::queue::entry_payload
