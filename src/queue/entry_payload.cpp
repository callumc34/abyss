#include "entry_payload.h"

#include <cstdint>
#include <string>
#include <string_view>
#include <utility>
#include <variant>

#include "binary_io.h"

namespace abyss::queue::entry_payload {

namespace {

core::Error Corrupted(std::string_view what) {
  return {core::ErrorCode::kCorruption, "WAL entry corruption: " + std::string(what)};
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

}  // namespace

EntryType TypeOf(const core::QueueEntry& entry) {
  return std::holds_alternative<core::entry::Write>(entry.payload) ? EntryType::kWrite
                                                                   : EntryType::kFlush;
}

void Encode(const core::QueueEntry& entry, binary::SpanWriter& out) {
  // A Flush has no payload.
  if (const auto* write = std::get_if<core::entry::Write>(&entry.payload)) {
    WriteRespCommand(out, write->cmd);
  }
}

void Encode(const core::QueueEntry& entry, std::vector<std::byte>& out) {
  const std::size_t start = out.size();
  out.resize(start + EncodedSize(entry));
  binary::SpanWriter writer(std::span(out).subspan(start));
  Encode(entry, writer);
}

std::size_t EncodedSize(const core::QueueEntry& entry) {
  const auto* write = std::get_if<core::entry::Write>(&entry.payload);
  return write != nullptr ? RespCommandSize(write->cmd) : 0;
}

core::Result<Payload> Decode(EntryType type, std::span<const std::byte>& cursor) {
  switch (type) {
    case EntryType::kWrite: {
      auto cmd = ReadRespCommand(cursor);
      if (!cmd.has_value()) return std::unexpected(cmd.error());
      return core::entry::Write{.cmd = std::move(*cmd)};
    }
    case EntryType::kFlush:
      return core::entry::Flush{};
  }
  return std::unexpected(Corrupted("unknown entry type " + std::to_string(static_cast<int>(type))));
}

}  // namespace abyss::queue::entry_payload
