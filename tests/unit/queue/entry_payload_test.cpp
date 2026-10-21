#include "entry_payload.h"

#include <gtest/gtest.h>

#include <cstddef>
#include <cstdint>
#include <span>
#include <string>
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

TEST(EntryPayloadTest, AFlushHasNoPayload) {
  const core::QueueEntry flush{.payload = core::entry::Flush{}};
  EXPECT_EQ(TypeOf(flush), EntryType::kFlush);
  EXPECT_EQ(EncodedSize(flush), 0U);
  auto decoded = RoundTrip(flush);
  ASSERT_TRUE(decoded.has_value()) << decoded.error().message();
  EXPECT_TRUE(std::holds_alternative<core::entry::Flush>(*decoded));
}

// 0x01 and 0x02 were the conditional and resolved types; they are
// reserved, and like any type byte the codec does not know, corruption.
TEST(EntryPayloadTest, AReservedOrUnknownTypeIsCorruption) {
  const core::QueueEntry write{.payload =
                                   core::entry::Write{.cmd = core::RespCommand{{"SET", "k", "v"}}}};
  std::vector<std::byte> bytes;
  Encode(write, bytes);
  for (const uint8_t type : {uint8_t{0x01}, uint8_t{0x02}, uint8_t{0x04}, uint8_t{0xFF}}) {
    std::span<const std::byte> cursor(bytes);
    // NOLINTNEXTLINE(clang-analyzer-optin.core.EnumCastOutOfRange): the point.
    auto decoded = Decode(static_cast<EntryType>(type), cursor);
    ASSERT_FALSE(decoded.has_value()) << "type " << static_cast<int>(type);
    EXPECT_EQ(decoded.error().code(), core::ErrorCode::kCorruption);
    EXPECT_NE(decoded.error().message().find("unknown entry type"), std::string::npos);
  }
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

// A count the payload cannot hold is refused before it sizes an
// allocation: reserving it throws bad_alloc on Windows.
TEST(EntryPayloadTest, ACountBeyondThePayloadIsCorruptionNotABadAlloc) {
  std::vector<std::byte> args;
  binary::WriteU32LE(args, 0xFFFF'FFFF);
  std::span<const std::byte> cursor(args);
  auto write = Decode(EntryType::kWrite, cursor);
  ASSERT_FALSE(write.has_value());
  EXPECT_EQ(write.error().code(), core::ErrorCode::kCorruption);
}

}  // namespace
}  // namespace abyss::queue::entry_payload
