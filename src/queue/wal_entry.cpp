#include "abyss/queue/wal_entry.h"

#include <chrono>
#include <string>
#include <variant>

#include "abyss/resp/parser.h"
#include "abyss/resp/serializer.h"
#include "binary_io.h"
#include "crc32c.h"

namespace abyss::queue {

namespace {

core::Error Truncated(const char* what) {
  return {core::ErrorCode::kCorruption, std::string("WAL entry truncated: ") + what};
}

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

WalEntryType EntryType(const core::QueueEntry& entry) {
  return std::visit(
      [](const auto& p) -> WalEntryType {
        using T = std::decay_t<decltype(p)>;
        if constexpr (std::is_same_v<T, core::entry::Write>) {
          return WalEntryType::kWrite;
        } else if constexpr (std::is_same_v<T, core::entry::Conditional>) {
          return WalEntryType::kConditional;
        } else {
          return WalEntryType::kResolved;
        }
      },
      entry.payload);
}

}  // namespace

size_t EncodeWalEntry(const core::QueueEntry& entry, std::vector<std::byte>& out) {
  using namespace binary;
  const size_t start = out.size();

  WriteU32LE(out, 0);
  const size_t body_start = out.size();

  WriteU8(out, static_cast<uint8_t>(EntryType(entry)));
  WriteU64LE(out, entry.seq);

  const auto appended_us =
      std::chrono::duration_cast<std::chrono::microseconds>(entry.appended_at.time_since_epoch())
          .count();
  WriteI64LE(out, appended_us);

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
          if (p.materialised_op.has_value()) {
            binary::WriteU8(out, 1);
            WriteRespCommand(out, *p.materialised_op);
          } else {
            binary::WriteU8(out, 0);
          }
          WriteRespValue(out, p.return_value);
        }
      },
      entry.payload);

  const size_t body_len = out.size() - body_start;
  PatchU32LE(out, start, static_cast<uint32_t>(body_len));

  const std::span<const std::byte> body(out.data() + body_start, body_len);
  WriteU32LE(out, Crc32c(body));

  return out.size() - start;
}

core::Result<DecodedWalEntry> DecodeWalEntry(std::span<const std::byte> bytes) {
  using namespace binary;
  const size_t initial_size = bytes.size();

  uint32_t body_len = 0;
  if (!ReadU32LE(bytes, body_len)) {
    return std::unexpected(Truncated("length prefix"));
  }
  if (bytes.size() < static_cast<size_t>(body_len) + sizeof(uint32_t)) {
    return std::unexpected(Truncated("body or crc"));
  }

  const std::span<const std::byte> body = bytes.first(body_len);
  bytes = bytes.subspan(body_len);

  uint32_t stored_crc = 0;
  if (!ReadU32LE(bytes, stored_crc)) {
    return std::unexpected(Truncated("crc"));
  }

  if (Crc32c(body) != stored_crc) {
    return std::unexpected(Corrupted("body CRC mismatch"));
  }

  std::span<const std::byte> cursor = body;

  uint8_t type_byte = 0;
  if (!ReadU8(cursor, type_byte)) {
    return std::unexpected(Corrupted("missing type"));
  }

  core::QueueEntry qe;

  uint64_t seq = 0;
  if (!ReadU64LE(cursor, seq)) {
    return std::unexpected(Corrupted("missing seq"));
  }
  qe.seq = seq;

  int64_t appended_us = 0;
  if (!ReadI64LE(cursor, appended_us)) {
    return std::unexpected(Corrupted("missing appended_us"));
  }
  qe.appended_at = core::WallTime(std::chrono::microseconds(appended_us));

  switch (static_cast<WalEntryType>(type_byte)) {
    case WalEntryType::kWrite: {
      auto cmd = ReadRespCommand(cursor);
      if (!cmd.has_value()) return std::unexpected(cmd.error());
      qe.payload = core::entry::Write{.cmd = std::move(*cmd)};
      break;
    }
    case WalEntryType::kConditional: {
      uint16_t flags = 0;
      if (!ReadU16LE(cursor, flags)) {
        return std::unexpected(Corrupted("missing predicate flags"));
      }
      auto cmd = ReadRespCommand(cursor);
      if (!cmd.has_value()) return std::unexpected(cmd.error());
      qe.payload = core::entry::Conditional{
          .cmd = std::move(*cmd),
          .flags = static_cast<core::PredicateFlags>(flags),
      };
      break;
    }
    case WalEntryType::kResolved: {
      uint64_t ref = 0;
      if (!ReadU64LE(cursor, ref)) {
        return std::unexpected(Corrupted("missing ref"));
      }
      uint8_t decision = 0;
      if (!ReadU8(cursor, decision)) {
        return std::unexpected(Corrupted("missing decision"));
      }
      uint8_t has_op = 0;
      if (!ReadU8(cursor, has_op)) {
        return std::unexpected(Corrupted("missing has_op"));
      }
      std::optional<core::RespCommand> mat_op;
      if (has_op != 0) {
        auto cmd = ReadRespCommand(cursor);
        if (!cmd.has_value()) return std::unexpected(cmd.error());
        mat_op = std::move(*cmd);
      }
      // Read inline RESP-encoded return value.
      uint32_t resp_len = 0;
      if (!ReadU32LE(cursor, resp_len)) {
        return std::unexpected(Corrupted("missing return_value length"));
      }
      if (cursor.size() < resp_len) {
        return std::unexpected(Corrupted("truncated return_value"));
      }
      auto resp_bytes = cursor.first(resp_len);
      auto parsed_resp =
          resp::Parser::Parse({reinterpret_cast<const uint8_t*>(resp_bytes.data()), resp_len});
      core::RespValue return_value;
      if (parsed_resp.has_value()) {
        return_value = std::move(parsed_resp->value);
      }
      cursor = cursor.subspan(resp_len);

      qe.payload = core::entry::Resolved{
          .ref = ref,
          .decision = static_cast<core::Decision>(decision),
          .materialised_op = std::move(mat_op),
          .return_value = std::move(return_value),
      };
      break;
    }
    default:
      return std::unexpected(Corrupted("unknown entry type"));
  }

  return DecodedWalEntry{.entry = std::move(qe), .bytes_consumed = initial_size - bytes.size()};
}

}  // namespace abyss::queue
