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

void WriteRespCommand(binary::SpanWriter& out, const core::RespCommand& cmd) {
  out.LE(static_cast<uint32_t>(cmd.args.size()));
  for (const auto& arg : cmd.args) {
    out.LE(static_cast<uint32_t>(arg.size()));
    out.Bytes(arg.data(), arg.size());
  }
}

std::size_t RespCommandSize(const core::RespCommand& cmd) {
  std::size_t size = sizeof(uint32_t);
  for (const auto& arg : cmd.args) size += sizeof(uint32_t) + arg.size();
  return size;
}

core::Result<core::RespCommand> ReadRespCommand(std::span<const std::byte>& cursor) {
  uint32_t arg_count = 0;
  if (!binary::ReadU32LE(cursor, arg_count)) {
    return std::unexpected(Corrupted("missing arg_count"));
  }
  // Each arg carries at least its length, so a count the bytes cannot
  // hold is corruption, refused before it sizes an allocation.
  if (arg_count > cursor.size() / sizeof(uint32_t)) {
    return std::unexpected(Corrupted("arg_count exceeds the payload"));
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

void WriteRespValue(binary::SpanWriter& out, const core::RespValue& val) {
  auto serialized = resp::Serializer::Serialize(val);
  out.LE(static_cast<uint32_t>(serialized.size()));
  out.Bytes(serialized.data(), serialized.size());
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
  if (op_count > cursor.size() / sizeof(uint32_t)) {
    return std::unexpected(Corrupted("materialised_ops count exceeds the payload"));
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

EntryType TypeOf(const core::QueueEntry& entry) {
  return std::visit(
      [](const auto& p) -> EntryType {
        using T = std::decay_t<decltype(p)>;
        if constexpr (std::is_same_v<T, core::entry::Write>) {
          return EntryType::kWrite;
        } else if constexpr (std::is_same_v<T, core::entry::Conditional>) {
          return EntryType::kConditional;
        } else if constexpr (std::is_same_v<T, core::entry::Resolved>) {
          return EntryType::kResolved;
        } else {
          return EntryType::kFlush;
        }
      },
      entry.payload);
}

void Encode(const core::QueueEntry& entry, binary::SpanWriter& out) {
  std::visit(
      [&out](const auto& p) {
        using T = std::decay_t<decltype(p)>;
        if constexpr (std::is_same_v<T, core::entry::Write>) {
          WriteRespCommand(out, p.cmd);
        } else if constexpr (std::is_same_v<T, core::entry::Conditional>) {
          out.LE(static_cast<uint16_t>(p.flags));
          WriteRespCommand(out, p.cmd);
        } else if constexpr (std::is_same_v<T, core::entry::Resolved>) {
          out.LE(p.ref);
          out.LE(static_cast<uint8_t>(p.decision));
          out.LE(static_cast<uint32_t>(p.materialised_ops.size()));
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

void Encode(const core::QueueEntry& entry, std::vector<std::byte>& out) {
  const std::size_t start = out.size();
  out.resize(start + EncodedSize(entry));
  binary::SpanWriter writer(std::span(out).subspan(start));
  Encode(entry, writer);
}

std::size_t EncodedSize(const core::QueueEntry& entry) {
  return std::visit(
      [](const auto& p) -> std::size_t {
        using T = std::decay_t<decltype(p)>;
        if constexpr (std::is_same_v<T, core::entry::Write>) {
          return RespCommandSize(p.cmd);
        } else if constexpr (std::is_same_v<T, core::entry::Conditional>) {
          return sizeof(uint16_t) + RespCommandSize(p.cmd);
        } else if constexpr (std::is_same_v<T, core::entry::Resolved>) {
          std::size_t size = sizeof(uint64_t) + sizeof(uint8_t) + sizeof(uint32_t);
          for (const auto& op : p.materialised_ops) size += RespCommandSize(op);
          return size + sizeof(uint32_t) + resp::Serializer::Serialize(p.return_value).size();
        } else {
          return 0;
        }
      },
      entry.payload);
}

core::Result<Payload> Decode(EntryType type, std::span<const std::byte>& cursor) {
  switch (type) {
    case EntryType::kWrite: {
      auto cmd = ReadRespCommand(cursor);
      if (!cmd.has_value()) return std::unexpected(cmd.error());
      return core::entry::Write{.cmd = std::move(*cmd)};
    }
    case EntryType::kConditional: {
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
    case EntryType::kResolved:
      return DecodeResolved(cursor);
    case EntryType::kFlush:
      return core::entry::Flush{};
  }
  return std::unexpected(Corrupted("unknown entry type"));
}

}  // namespace abyss::queue::entry_payload
