#include "entry_payload.h"

#include <cstdint>
#include <string>
#include <type_traits>
#include <utility>
#include <variant>

#include "abyss/resp/parser.h"
#include "abyss/resp/serializer.h"
#include "binary_io.h"

namespace abyss::queue::entry_payload {

namespace {

core::Error Corrupted(const char* what) {
  return {core::ErrorCode::kCorruption, std::string("WAL entry corruption: ") + what};
}

void WriteRespCommand(std::vector<std::byte>& out, const core::RespCommand& cmd) {
  binary::WriteU32LE(out, static_cast<uint32_t>(cmd.args.size()));
  for (const auto& arg : cmd.args) {
    binary::WriteU32LE(out, static_cast<uint32_t>(arg.size()));
    binary::AppendBytes(out, arg.data(), arg.size());
  }
}

core::Result<core::RespCommand> ReadRespCommand(std::span<const std::byte>& cursor) {
  uint32_t arg_count = 0;
  if (!binary::ReadU32LE(cursor, arg_count)) {
    return std::unexpected(Corrupted("missing arg_count"));
  }
  core::RespCommand cmd;
  cmd.args.reserve(arg_count);
  for (uint32_t i = 0; i < arg_count; ++i) {
    uint32_t arg_len = 0;
    if (!binary::ReadU32LE(cursor, arg_len)) {
      return std::unexpected(Corrupted("missing arg length"));
    }
    if (cursor.size() < arg_len) {
      return std::unexpected(Corrupted("missing arg bytes"));
    }
    const auto* data = reinterpret_cast<const char*>(cursor.data());
    cmd.args.emplace_back(data, arg_len);
    cursor = cursor.subspan(arg_len);
  }
  return cmd;
}

void WriteRespValue(std::vector<std::byte>& out, const core::RespValue& val) {
  auto serialized = resp::Serializer::Serialize(val);
  binary::WriteU32LE(out, static_cast<uint32_t>(serialized.size()));
  binary::AppendBytes(out, reinterpret_cast<const char*>(serialized.data()), serialized.size());
}

core::Result<Payload> DecodeResolved(std::span<const std::byte>& cursor) {
  using namespace binary;
  uint64_t ref = 0;
  if (!ReadU64LE(cursor, ref)) {
    return std::unexpected(Corrupted("missing ref"));
  }
  uint8_t decision = 0;
  if (!ReadU8(cursor, decision)) {
    return std::unexpected(Corrupted("missing decision"));
  }
  uint32_t op_count = 0;
  if (!ReadU32LE(cursor, op_count)) {
    return std::unexpected(Corrupted("missing materialised_ops count"));
  }
  std::vector<core::RespCommand> mat_ops;
  mat_ops.reserve(op_count);
  for (uint32_t i = 0; i < op_count; ++i) {
    auto cmd = ReadRespCommand(cursor);
    if (!cmd.has_value()) return std::unexpected(cmd.error());
    mat_ops.push_back(std::move(*cmd));
  }
  uint32_t resp_len = 0;
  if (!ReadU32LE(cursor, resp_len)) {
    return std::unexpected(Corrupted("missing return_value length"));
  }
  if (cursor.size() < resp_len) {
    return std::unexpected(Corrupted("truncated return_value"));
  }
  auto resp_bytes = cursor.first(resp_len);
  // Bound the parse with the replay envelope: a crafted inner array/bulk
  // count cannot abort recovery via length_error/bad_alloc (RESP-1). A
  // parse failure on a CRC-valid frame is genuine corruption — surface it
  // instead of silently substituting nil, so recovery fails-stop and the
  // corruption counter is bumped (QUEUE-6). It is never left as a
  // default-constructed kNull.
  auto parsed_resp =
      resp::Parser::Parse({reinterpret_cast<const uint8_t*>(resp_bytes.data()), resp_len},
                          resp::ParserLimits::ForWalReplay());
  if (!parsed_resp.has_value()) {
    return std::unexpected(Corrupted("return_value parse failed"));
  }
  cursor = cursor.subspan(resp_len);

  return core::entry::Resolved{
      .ref = ref,
      .decision = static_cast<core::Decision>(decision),
      .materialised_ops = std::move(mat_ops),
      .return_value = std::move(parsed_resp->value),
  };
}

}  // namespace

WalEntryType TypeOf(const core::QueueEntry& entry) {
  return std::visit(
      [](const auto& p) -> WalEntryType {
        using T = std::decay_t<decltype(p)>;
        if constexpr (std::is_same_v<T, core::entry::Write>) {
          return WalEntryType::kWrite;
        } else if constexpr (std::is_same_v<T, core::entry::Conditional>) {
          return WalEntryType::kConditional;
        } else if constexpr (std::is_same_v<T, core::entry::Resolved>) {
          return WalEntryType::kResolved;
        } else {
          return WalEntryType::kFlush;
        }
      },
      entry.payload);
}

void Encode(const core::QueueEntry& entry, std::vector<std::byte>& out) {
  std::visit(
      [&out](const auto& p) {
        using T = std::decay_t<decltype(p)>;
        if constexpr (std::is_same_v<T, core::entry::Write>) {
          WriteRespCommand(out, p.cmd);
        } else if constexpr (std::is_same_v<T, core::entry::Conditional>) {
          binary::WriteU16LE(out, static_cast<uint16_t>(p.flags));
          WriteRespCommand(out, p.cmd);
        } else if constexpr (std::is_same_v<T, core::entry::Resolved>) {
          binary::WriteU64LE(out, p.ref);
          binary::WriteU8(out, static_cast<uint8_t>(p.decision));
          binary::WriteU32LE(out, static_cast<uint32_t>(p.materialised_ops.size()));
          for (const auto& op : p.materialised_ops) {
            WriteRespCommand(out, op);
          }
          WriteRespValue(out, p.return_value);
        } else if constexpr (std::is_same_v<T, core::entry::Flush>) {
          // No payload.
        }
      },
      entry.payload);
}

core::Result<Payload> Decode(WalEntryType type, std::span<const std::byte>& cursor) {
  switch (type) {
    case WalEntryType::kWrite: {
      auto cmd = ReadRespCommand(cursor);
      if (!cmd.has_value()) return std::unexpected(cmd.error());
      return core::entry::Write{.cmd = std::move(*cmd)};
    }
    case WalEntryType::kConditional: {
      uint16_t flags = 0;
      if (!binary::ReadU16LE(cursor, flags)) {
        return std::unexpected(Corrupted("missing predicate flags"));
      }
      auto cmd = ReadRespCommand(cursor);
      if (!cmd.has_value()) return std::unexpected(cmd.error());
      return core::entry::Conditional{
          .cmd = std::move(*cmd),
          .flags = static_cast<core::PredicateFlags>(flags),
      };
    }
    case WalEntryType::kResolved:
      return DecodeResolved(cursor);
    case WalEntryType::kFlush:
      return core::entry::Flush{};
  }
  return std::unexpected(Corrupted("unknown entry type"));
}

}  // namespace abyss::queue::entry_payload
