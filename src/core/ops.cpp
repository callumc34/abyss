#include "abyss/core/ops.h"

#include <cctype>
#include <charconv>
#include <chrono>
#include <string>
#include <unordered_map>

#include "abyss/core/types.h"

namespace abyss::core::ops {

namespace {

Result<double> ParseDouble(std::string_view s) {
  double value = 0.0;
  const auto* begin = s.data();
  const auto* end = s.data() + s.size();
  const auto [ptr, ec] = std::from_chars(begin, end, value);
  if (ec != std::errc{} || ptr != end) {
    return std::unexpected(
        Error(ErrorCode::kInvalidArgument, "not a valid float: '" + std::string(s) + "'"));
  }
  // NOLINTNEXTLINE(bugprone-narrowing-conversions,cppcoreguidelines-narrowing-conversions)
  return value;
}

Result<uint64_t> ParseUint64(std::string_view s) {
  uint64_t value = 0;
  const auto* begin = s.data();
  const auto* end = s.data() + s.size();
  const auto [ptr, ec] = std::from_chars(begin, end, value);
  if (ec != std::errc{} || ptr != end) {
    return std::unexpected(
        Error(ErrorCode::kInvalidArgument, "not a valid integer: '" + std::string(s) + "'"));
  }
  return value;
}

std::string AsciiUpper(std::string_view s) {
  std::string out(s);
  for (char& c : out) {
    c = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
  }
  return out;
}

uint64_t NowWallMs() {
  return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(
                                   core::WallClock::now().time_since_epoch())
                                   .count());
}

Error SyntaxError(std::string_view detail) {
  return {ErrorCode::kInvalidArgument, std::string(detail)};
}

// Collect args[start..] as string_views.
std::vector<std::string_view> CollectArgs(const RespCommand& cmd, size_t start) {
  std::vector<std::string_view> out;
  out.reserve(cmd.args.size() - start);
  for (size_t i = start; i < cmd.args.size(); ++i) {
    out.emplace_back(cmd.args[i]);
  }
  return out;
}

// --- Read parsers -----------------------------------------------------------

using ReadParserFn = Result<ReadOp> (*)(const RespCommand&);

Result<ReadOp> ParseGet(const RespCommand& cmd) { return ReadOp{StringGet{.key = cmd.args[1]}}; }

Result<ReadOp> ParseMget(const RespCommand& cmd) {
  return ReadOp{MultiStringGet{.keys = CollectArgs(cmd, 1)}};
}

Result<ReadOp> ParseSismember(const RespCommand& cmd) {
  return ReadOp{SetIsMember{.key = cmd.args[1], .member = cmd.args[2]}};
}

Result<ReadOp> ParseSmembers(const RespCommand& cmd) {
  return ReadOp{SetMembers{.key = cmd.args[1]}};
}

Result<ReadOp> ParseScard(const RespCommand& cmd) { return ReadOp{SetCard{.key = cmd.args[1]}}; }

Result<ReadOp> ParseZscore(const RespCommand& cmd) {
  return ReadOp{ZsetScore{.key = cmd.args[1], .member = cmd.args[2]}};
}

Result<ReadOp> ParseZcard(const RespCommand& cmd) { return ReadOp{ZsetCard{.key = cmd.args[1]}}; }

Result<ReadOp> ParseHget(const RespCommand& cmd) {
  return ReadOp{HashGet{.key = cmd.args[1], .field = cmd.args[2]}};
}

Result<ReadOp> ParseHgetall(const RespCommand& cmd) {
  return ReadOp{HashGetAll{.key = cmd.args[1]}};
}

Result<ReadOp> ParseHmget(const RespCommand& cmd) {
  return ReadOp{HashMultiGet{.key = cmd.args[1], .fields = CollectArgs(cmd, 2)}};
}

Result<ReadOp> ParseHexists(const RespCommand& cmd) {
  return ReadOp{HashFieldExists{.key = cmd.args[1], .field = cmd.args[2]}};
}

Result<ReadOp> ParseHkeys(const RespCommand& cmd) { return ReadOp{HashKeys{.key = cmd.args[1]}}; }

Result<ReadOp> ParseHvals(const RespCommand& cmd) { return ReadOp{HashVals{.key = cmd.args[1]}}; }

Result<ReadOp> ParseHlen(const RespCommand& cmd) { return ReadOp{HashLen{.key = cmd.args[1]}}; }

Result<ReadOp> ParseExists(const RespCommand& cmd) {
  return ReadOp{Exists{.keys = CollectArgs(cmd, 1)}};
}

const std::unordered_map<std::string_view, ReadParserFn>& ReadParsers() {
  static const std::unordered_map<std::string_view, ReadParserFn> table{
      {"GET", ParseGet},           {"MGET", ParseMget},       {"SISMEMBER", ParseSismember},
      {"SMEMBERS", ParseSmembers}, {"SCARD", ParseScard},     {"ZSCORE", ParseZscore},
      {"ZCARD", ParseZcard},       {"HGET", ParseHget},       {"HGETALL", ParseHgetall},
      {"HMGET", ParseHmget},       {"HEXISTS", ParseHexists}, {"HKEYS", ParseHkeys},
      {"HVALS", ParseHvals},       {"HLEN", ParseHlen},       {"EXISTS", ParseExists},
  };
  return table;
}

// --- Write parsers ----------------------------------------------------------

using WriteParserFn = Result<WriteOp> (*)(const RespCommand&);

Result<WriteOp> ParseSet(const RespCommand& cmd) {
  uint64_t abs_ttl_ms = 0;
  for (size_t i = 3; i < cmd.args.size(); ++i) {
    const auto opt = AsciiUpper(cmd.args[i]);
    if (opt == "EX" || opt == "PX" || opt == "EXAT" || opt == "PXAT") {
      if (i + 1 >= cmd.args.size()) {
        return std::unexpected(SyntaxError("syntax error — expected value after '" + opt + "'"));
      }
      auto ttl_arg = ParseUint64(cmd.args[++i]);
      if (!ttl_arg.has_value()) return std::unexpected(ttl_arg.error());
      if (opt == "EX") {
        abs_ttl_ms = NowWallMs() + (*ttl_arg * 1000);
      } else if (opt == "PX") {
        abs_ttl_ms = NowWallMs() + *ttl_arg;
      } else if (opt == "EXAT") {
        abs_ttl_ms = *ttl_arg * 1000;
      } else {
        abs_ttl_ms = *ttl_arg;
      }
    }
  }
  return WriteOp{StringSet{.key = cmd.args[1], .value = cmd.args[2], .abs_ttl_ms = abs_ttl_ms}};
}

Result<WriteOp> ParseSetex(const RespCommand& cmd) {
  auto ttl = ParseUint64(cmd.args[2]);
  if (!ttl.has_value()) return std::unexpected(ttl.error());
  return WriteOp{StringSet{
      .key = cmd.args[1], .value = cmd.args[3], .abs_ttl_ms = NowWallMs() + (*ttl * 1000)}};
}

Result<WriteOp> ParsePsetex(const RespCommand& cmd) {
  auto ttl = ParseUint64(cmd.args[2]);
  if (!ttl.has_value()) return std::unexpected(ttl.error());
  return WriteOp{
      StringSet{.key = cmd.args[1], .value = cmd.args[3], .abs_ttl_ms = NowWallMs() + *ttl}};
}

Result<WriteOp> ParseMset(const RespCommand& cmd) {
  if (cmd.args.size() % 2 == 0) {
    return std::unexpected(
        SyntaxError("MSET requires an odd number of arguments (key-value pairs)"));
  }
  std::vector<MultiStringSet::Entry> entries;
  entries.reserve((cmd.args.size() - 1) / 2);
  for (size_t i = 1; i < cmd.args.size(); i += 2) {
    entries.push_back({.key = cmd.args[i], .value = cmd.args[i + 1]});
  }
  return WriteOp{MultiStringSet{.entries = std::move(entries)}};
}

Result<WriteOp> ParseDel(const RespCommand& cmd) {
  return WriteOp{Del{.keys = CollectArgs(cmd, 1)}};
}

Result<WriteOp> ParseSadd(const RespCommand& cmd) {
  return WriteOp{SetAdd{.key = cmd.args[1], .members = CollectArgs(cmd, 2)}};
}

Result<WriteOp> ParseSrem(const RespCommand& cmd) {
  return WriteOp{SetRem{.key = cmd.args[1], .members = CollectArgs(cmd, 2)}};
}

Result<WriteOp> ParseZadd(const RespCommand& cmd) {
  size_t i = 2;
  while (i < cmd.args.size()) {
    const auto opt = AsciiUpper(cmd.args[i]);
    if (opt == "NX" || opt == "XX" || opt == "GT" || opt == "LT" || opt == "CH") {
      ++i;
      continue;
    }
    break;
  }
  if ((cmd.args.size() - i) < 2 || (cmd.args.size() - i) % 2 != 0) {
    return std::unexpected(SyntaxError("ZADD requires score-member pairs after flags"));
  }
  std::vector<ZsetAdd::Entry> entries;
  entries.reserve((cmd.args.size() - i) / 2);
  for (; i < cmd.args.size(); i += 2) {
    auto score = ParseDouble(cmd.args[i]);
    if (!score.has_value()) return std::unexpected(score.error());
    entries.push_back({.score = *score, .member = cmd.args[i + 1]});
  }
  return WriteOp{ZsetAdd{.key = cmd.args[1], .entries = std::move(entries)}};
}

Result<WriteOp> ParseZrem(const RespCommand& cmd) {
  return WriteOp{ZsetRem{.key = cmd.args[1], .members = CollectArgs(cmd, 2)}};
}

Result<WriteOp> ParseHset(const RespCommand& cmd) {
  if (cmd.args.size() % 2 != 0) {
    return std::unexpected(SyntaxError("HSET requires field-value pairs"));
  }
  std::vector<HashSet::FieldValue> fields;
  fields.reserve((cmd.args.size() - 2) / 2);
  for (size_t i = 2; i < cmd.args.size(); i += 2) {
    fields.push_back({.field = cmd.args[i], .value = cmd.args[i + 1]});
  }
  return WriteOp{HashSet{.key = cmd.args[1], .fields = std::move(fields)}};
}

Result<WriteOp> ParseHmset(const RespCommand& cmd) {
  if (cmd.args.size() % 2 != 0) {
    return std::unexpected(SyntaxError("HMSET requires field-value pairs"));
  }
  std::vector<HashSet::FieldValue> fields;
  fields.reserve((cmd.args.size() - 2) / 2);
  for (size_t i = 2; i < cmd.args.size(); i += 2) {
    fields.push_back({.field = cmd.args[i], .value = cmd.args[i + 1]});
  }
  return WriteOp{HashMSet{.key = cmd.args[1], .fields = std::move(fields)}};
}

Result<WriteOp> ParseHdel(const RespCommand& cmd) {
  return WriteOp{HashDel{.key = cmd.args[1], .fields = CollectArgs(cmd, 2)}};
}

// All four EXPIRE forms collapse to abs_ttl_ms; NX/XX/GT/LT are stripped
// by the predicate extractor before this runs.
Result<WriteOp> ParseExpireSeconds(const RespCommand& cmd) {
  auto secs = ParseUint64(cmd.args[2]);
  if (!secs.has_value()) return std::unexpected(secs.error());
  return WriteOp{Expire{.key = cmd.args[1], .abs_ttl_ms = NowWallMs() + (*secs * 1000)}};
}

Result<WriteOp> ParseExpireMs(const RespCommand& cmd) {
  auto ms = ParseUint64(cmd.args[2]);
  if (!ms.has_value()) return std::unexpected(ms.error());
  return WriteOp{Expire{.key = cmd.args[1], .abs_ttl_ms = NowWallMs() + *ms}};
}

Result<WriteOp> ParseExpireAt(const RespCommand& cmd) {
  auto ts = ParseUint64(cmd.args[2]);
  if (!ts.has_value()) return std::unexpected(ts.error());
  return WriteOp{Expire{.key = cmd.args[1], .abs_ttl_ms = *ts * 1000}};
}

Result<WriteOp> ParsePexpireAt(const RespCommand& cmd) {
  auto ts = ParseUint64(cmd.args[2]);
  if (!ts.has_value()) return std::unexpected(ts.error());
  return WriteOp{Expire{.key = cmd.args[1], .abs_ttl_ms = *ts}};
}

Result<WriteOp> ParsePersist(const RespCommand& cmd) {
  return WriteOp{Persist{.key = cmd.args[1]}};
}

const std::unordered_map<std::string_view, WriteParserFn>& WriteParsers() {
  static const std::unordered_map<std::string_view, WriteParserFn> table{
      {"SET", ParseSet},
      {"SETEX", ParseSetex},
      {"PSETEX", ParsePsetex},
      {"MSET", ParseMset},
      {"DEL", ParseDel},
      {"UNLINK", ParseDel},
      {"SADD", ParseSadd},
      {"SREM", ParseSrem},
      {"ZADD", ParseZadd},
      {"ZREM", ParseZrem},
      {"HSET", ParseHset},
      {"HMSET", ParseHmset},
      {"HDEL", ParseHdel},
      {"EXPIRE", ParseExpireSeconds},
      {"PEXPIRE", ParseExpireMs},
      {"EXPIREAT", ParseExpireAt},
      {"PEXPIREAT", ParsePexpireAt},
      {"PERSIST", ParsePersist},
  };
  return table;
}

}  // namespace

Result<ReadOp> ParseReadOp(std::string_view name, const RespCommand& cmd) {
  const auto& table = ReadParsers();
  const auto it = table.find(name);
  if (it == table.end()) {
    return std::unexpected(
        Error(ErrorCode::kInvalidArgument, "unknown read command '" + std::string(name) + "'"));
  }
  return it->second(cmd);
}

Result<WriteOp> ParseWriteOp(std::string_view name, const RespCommand& cmd) {
  const auto& table = WriteParsers();
  const auto it = table.find(name);
  if (it == table.end()) {
    return std::unexpected(
        Error(ErrorCode::kInvalidArgument, "unknown write command '" + std::string(name) + "'"));
  }
  return it->second(cmd);
}

std::string_view PrimaryKey(const ReadOp& op) {
  return std::visit(
      [](const auto& o) -> std::string_view {
        using T = std::decay_t<decltype(o)>;
        if constexpr (std::is_same_v<T, MultiStringGet> || std::is_same_v<T, Exists>) {
          return o.keys.empty() ? std::string_view{} : o.keys[0];
        } else {
          return o.key;
        }
      },
      op);
}

std::string_view PrimaryKey(const WriteOp& op) {
  return std::visit(
      [](const auto& o) -> std::string_view {
        using T = std::decay_t<decltype(o)>;
        if constexpr (std::is_same_v<T, Del>) {
          return o.keys.empty() ? std::string_view{} : o.keys[0];
        } else if constexpr (std::is_same_v<T, MultiStringSet>) {
          return o.entries.empty() ? std::string_view{} : o.entries[0].key;
        } else {
          return o.key;
        }
      },
      op);
}

}  // namespace abyss::core::ops
