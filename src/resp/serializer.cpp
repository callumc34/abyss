#include "abyss/resp/serializer.h"

#include <array>
#include <charconv>
#include <cstddef>
#include <cstdint>

#include "abyss/core/byte_cast.h"

namespace abyss::resp {
namespace {

using core::AsBytes;
using core::RespCommand;
using core::RespValue;

void AppendStr(std::vector<uint8_t>& out, std::string_view s) {
  out.insert(out.end(), s.begin(), s.end());
}

void AppendCrlf(std::vector<uint8_t>& out) {
  out.push_back('\r');
  out.push_back('\n');
}

void AppendInt(std::vector<uint8_t>& out, int64_t value) {
  std::array<char, 32> buf{};
  auto [ptr, ec] = std::to_chars(buf.data(), buf.data() + buf.size(), value);
  // std::to_chars on int64_t into 32 chars cannot fail.
  out.insert(out.end(), AsBytes(buf.data()), AsBytes(ptr));
}

void SerializeInto(const RespValue& value, std::vector<uint8_t>& out) {
  switch (value.type()) {
    case RespValue::Type::kNull:
      AppendStr(out, "$-1\r\n");
      return;
    case RespValue::Type::kNullArray:
      AppendStr(out, "*-1\r\n");
      return;
    case RespValue::Type::kSimpleString:
      out.push_back('+');
      AppendStr(out, value.AsString());
      AppendCrlf(out);
      return;
    case RespValue::Type::kBulkString: {
      const auto& s = value.AsString();
      out.push_back('$');
      AppendInt(out, static_cast<int64_t>(s.size()));
      AppendCrlf(out);
      AppendStr(out, s);
      AppendCrlf(out);
      return;
    }
    case RespValue::Type::kInteger:
      out.push_back(':');
      AppendInt(out, value.AsInteger());
      AppendCrlf(out);
      return;
    case RespValue::Type::kError:
      out.push_back('-');
      AppendStr(out, value.AsString());
      AppendCrlf(out);
      return;
    case RespValue::Type::kArray: {
      const auto& elements = value.AsArray();
      out.push_back('*');
      AppendInt(out, static_cast<int64_t>(elements.size()));
      AppendCrlf(out);
      for (const auto& el : elements) {
        SerializeInto(el, out);
      }
      return;
    }
  }
}

size_t DigitsOf(int64_t value) {
  std::array<char, 32> buf{};
  auto [ptr, ec] = std::to_chars(buf.data(), buf.data() + buf.size(), value);
  return static_cast<size_t>(ptr - buf.data());
}

// The prefix byte, the body and the CRLF.
size_t LineSize(size_t body) { return 1 + body + 2; }

size_t SizeOf(const RespValue& value) {
  switch (value.type()) {
    case RespValue::Type::kNull:
    case RespValue::Type::kNullArray:
      return 5;
    case RespValue::Type::kSimpleString:
    case RespValue::Type::kError:
      return LineSize(value.AsString().size());
    case RespValue::Type::kBulkString: {
      const size_t len = value.AsString().size();
      return LineSize(DigitsOf(static_cast<int64_t>(len))) + len + 2;
    }
    case RespValue::Type::kInteger:
      return LineSize(DigitsOf(value.AsInteger()));
    case RespValue::Type::kArray: {
      const auto& elements = value.AsArray();
      size_t size = LineSize(DigitsOf(static_cast<int64_t>(elements.size())));
      for (const auto& el : elements) size += SizeOf(el);
      return size;
    }
  }
  return 0;
}

}  // namespace

void Serializer::Append(const RespValue& value, std::vector<uint8_t>& out) {
  SerializeInto(value, out);
}

size_t Serializer::Size(const RespValue& value) { return SizeOf(value); }

std::vector<uint8_t> Serializer::Serialize(const RespValue& value) {
  std::vector<uint8_t> out;
  SerializeInto(value, out);
  return out;
}

std::vector<uint8_t> Serializer::SerializeCommand(const RespCommand& cmd) {
  std::vector<uint8_t> out;
  out.push_back('*');
  AppendInt(out, static_cast<int64_t>(cmd.args.size()));
  AppendCrlf(out);
  for (const auto& arg : cmd.args) {
    out.push_back('$');
    AppendInt(out, static_cast<int64_t>(arg.size()));
    AppendCrlf(out);
    AppendStr(out, arg);
    AppendCrlf(out);
  }
  return out;
}

}  // namespace abyss::resp
