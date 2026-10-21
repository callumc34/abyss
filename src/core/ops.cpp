#include "abyss/core/ops.h"

#include <array>
#include <cctype>
#include <charconv>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <limits>
#include <string>
#include <type_traits>
#include <unordered_map>

#include "abyss/core/types.h"

namespace abyss::core::ops {

namespace {

// NaN is rejected: it has no place in a zset's score order.
Result<double> ParseDouble(std::string_view s) {
  // from_chars takes no leading '+'; Redis (strtod) takes one.
  if (s.size() > 1 && s[0] == '+' && s[1] != '+' && s[1] != '-') s.remove_prefix(1);
  double value = 0.0;
  const auto* begin = s.data();
  const auto* end = s.data() + s.size();
  const auto [ptr, ec] = std::from_chars(begin, end, value);
  if (ec != std::errc{} || ptr != end || std::isnan(value)) {
    return std::unexpected(Error(ErrorCode::kInvalidArgument, "value is not a valid float"));
  }
  // NOLINTNEXTLINE(bugprone-narrowing-conversions,cppcoreguidelines-narrowing-conversions)
  return value;
}

Result<int64_t> ParseInt64(std::string_view s) {
  int64_t value = 0;
  const auto* begin = s.data();
  const auto* end = s.data() + s.size();
  const auto [ptr, ec] = std::from_chars(begin, end, value);
  if (ec != std::errc{} || ptr != end) {
    return std::unexpected(
        Error(ErrorCode::kInvalidArgument, "value is not an integer or out of range"));
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
Result<ReadOp> ParseTtl(const RespCommand& cmd) { return ReadOp{Ttl{.key = cmd.args[1]}}; }
Result<ReadOp> ParsePttl(const RespCommand& cmd) {
  return ReadOp{Ttl{.key = cmd.args[1], .millis = true}};
}
Result<ReadOp> ParseType(const RespCommand& cmd) { return ReadOp{Type{.key = cmd.args[1]}}; }

// Parses the trailing `LIMIT offset count` clause starting at args[i]. On
// success advances `i` past the clause and writes offset/count into `op`.
Result<void> ParseLimitClause(const RespCommand& cmd, size_t& i, ZsetRange& op) {
  if (i + 2 >= cmd.args.size()) {
    return std::unexpected(SyntaxError("syntax error"));
  }
  auto offset = ParseInt64(cmd.args[i + 1]);
  if (!offset.has_value()) return std::unexpected(offset.error());
  auto count = ParseInt64(cmd.args[i + 2]);
  if (!count.has_value()) return std::unexpected(count.error());
  op.offset = *offset;
  op.count = *count;
  i += 3;
  return {};
}

// ZRANGE key start stop [BYSCORE|BYLEX] [REV] [LIMIT offset count] [WITHSCORES].
// Index, score, and lex modes share one variant; the by_score/by_lex flags pick
// the bound interpretation downstream (hot ExecZsetRange and cold Handle).
Result<ReadOp> ParseZrange(const RespCommand& cmd) {
  ZsetRange op{.key = cmd.args[1], .min = cmd.args[2], .max = cmd.args[3]};
  for (size_t i = 4; i < cmd.args.size();) {
    const auto opt = AsciiUpper(cmd.args[i]);
    if (opt == "BYSCORE") {
      op.by_score = true;
      ++i;
    } else if (opt == "BYLEX") {
      op.by_lex = true;
      ++i;
    } else if (opt == "REV") {
      op.rev = true;
      ++i;
    } else if (opt == "WITHSCORES") {
      op.with_scores = true;
      ++i;
    } else if (opt == "LIMIT") {
      auto limit = ParseLimitClause(cmd, i, op);
      if (!limit.has_value()) return std::unexpected(limit.error());
    } else {
      return std::unexpected(SyntaxError("syntax error"));
    }
  }
  if (op.by_score && op.by_lex) {
    return std::unexpected(SyntaxError("syntax error"));
  }
  return ReadOp{op};
}

// ZRANGEBYSCORE key min max [WITHSCORES] [LIMIT offset count].
Result<ReadOp> ParseZrangeByScore(const RespCommand& cmd) {
  ZsetRange op{.key = cmd.args[1], .min = cmd.args[2], .max = cmd.args[3], .by_score = true};
  for (size_t i = 4; i < cmd.args.size();) {
    const auto opt = AsciiUpper(cmd.args[i]);
    if (opt == "WITHSCORES") {
      op.with_scores = true;
      ++i;
    } else if (opt == "LIMIT") {
      auto limit = ParseLimitClause(cmd, i, op);
      if (!limit.has_value()) return std::unexpected(limit.error());
    } else {
      return std::unexpected(SyntaxError("syntax error"));
    }
  }
  return ReadOp{op};
}

// ZRANGEBYLEX key min max [LIMIT offset count]. WITHSCORES is not valid for lex.
Result<ReadOp> ParseZrangeByLex(const RespCommand& cmd) {
  ZsetRange op{.key = cmd.args[1], .min = cmd.args[2], .max = cmd.args[3], .by_lex = true};
  for (size_t i = 4; i < cmd.args.size();) {
    const auto opt = AsciiUpper(cmd.args[i]);
    if (opt == "LIMIT") {
      auto limit = ParseLimitClause(cmd, i, op);
      if (!limit.has_value()) return std::unexpected(limit.error());
    } else {
      return std::unexpected(SyntaxError("syntax error"));
    }
  }
  return ReadOp{op};
}

const std::unordered_map<std::string_view, ReadParserFn>& ReadParsers() {
  // MGET / EXISTS are intentionally absent: the engine decomposes them in
  // DispatchFanOut and never round-trips through ParseReadOp.
  static const std::unordered_map<std::string_view, ReadParserFn> table{
      {"GET", ParseGet},
      {"SISMEMBER", ParseSismember},
      {"SMEMBERS", ParseSmembers},
      {"SCARD", ParseScard},
      {"ZSCORE", ParseZscore},
      {"ZCARD", ParseZcard},
      {"HGET", ParseHget},
      {"HGETALL", ParseHgetall},
      {"HMGET", ParseHmget},
      {"HEXISTS", ParseHexists},
      {"HKEYS", ParseHkeys},
      {"HVALS", ParseHvals},
      {"HLEN", ParseHlen},
      {"TTL", ParseTtl},
      {"PTTL", ParsePttl},
      {"TYPE", ParseType},
      {"ZRANGE", ParseZrange},
      {"ZRANGEBYSCORE", ParseZrangeByScore},
      {"ZRANGEBYLEX", ParseZrangeByLex},
  };
  return table;
}

// --- Write parsers ----------------------------------------------------------

using WriteParserFn = Result<WriteOp> (*)(const RespCommand&, uint64_t);

enum class TtlBase : uint8_t { kRelative, kAbsolute };

struct TtlSpec {
  int64_t unit_ms;
  TtlBase base;
  // SET's family rejects a time of zero or less; EXPIRE's deletes.
  bool positive = false;
  std::string_view command;
};

// Redis's expiry checks. The result is never 0, which means no TTL:
// a time already past is 1, the earliest one.
Result<uint64_t> ParseTtl(std::string_view arg, const TtlSpec& spec, uint64_t now_ms) {
  auto value = ParseInt64(arg);
  if (!value.has_value()) return std::unexpected(value.error());
  const auto invalid = [&spec] {
    return std::unexpected(
        SyntaxError("invalid expire time in '" + std::string(spec.command) + "' command"));
  };
  constexpr int64_t kMax = std::numeric_limits<int64_t>::max();
  constexpr int64_t kMin = std::numeric_limits<int64_t>::min();
  if (spec.positive && *value <= 0) return invalid();
  if (*value > kMax / spec.unit_ms || *value < kMin / spec.unit_ms) return invalid();
  int64_t ms = *value * spec.unit_ms;
  if (spec.base == TtlBase::kRelative) {
    const auto now = static_cast<int64_t>(now_ms);
    if (ms > kMax - now) return invalid();
    ms += now;
  }
  return static_cast<uint64_t>(std::max<int64_t>(ms, 1));
}

// NX, XX and GET are the engine's predicates; they are legal here so
// every SET parse agrees on the accepted tokens.
Result<WriteOp> ParseSet(const RespCommand& cmd, uint64_t wall_now_ms) {
  uint64_t abs_ttl_ms = 0;
  bool expiry = false;
  bool keep_ttl = false;
  bool nx = false;
  bool xx = false;
  for (size_t i = 3; i < cmd.args.size(); ++i) {
    const auto opt = AsciiUpper(cmd.args[i]);
    const bool relative = opt == "EX" || opt == "PX";
    if (relative || opt == "EXAT" || opt == "PXAT") {
      if (expiry || keep_ttl || i + 1 >= cmd.args.size()) {
        return std::unexpected(SyntaxError("syntax error"));
      }
      const TtlSpec spec{
          .unit_ms = opt == "EX" || opt == "EXAT" ? 1000 : 1,
          .base = relative ? TtlBase::kRelative : TtlBase::kAbsolute,
          .positive = true,
          .command = "set",
      };
      auto ttl = ParseTtl(cmd.args[++i], spec, wall_now_ms);
      if (!ttl.has_value()) return std::unexpected(ttl.error());
      abs_ttl_ms = *ttl;
      expiry = true;
    } else if (opt == "KEEPTTL") {
      if (expiry) return std::unexpected(SyntaxError("syntax error"));
      keep_ttl = true;
    } else if (opt == "NX" || opt == "XX") {
      (opt == "NX" ? nx : xx) = true;
      if (nx && xx) return std::unexpected(SyntaxError("syntax error"));
    } else if (opt != "GET") {
      return std::unexpected(SyntaxError("syntax error"));
    }
  }
  return WriteOp{StringSet{.key = cmd.args[1], .value = cmd.args[2], .abs_ttl_ms = abs_ttl_ms}};
}

Result<WriteOp> ParseSetex(const RespCommand& cmd, uint64_t wall_now_ms) {
  const TtlSpec spec{
      .unit_ms = 1000, .base = TtlBase::kRelative, .positive = true, .command = "setex"};
  auto ttl = ParseTtl(cmd.args[2], spec, wall_now_ms);
  if (!ttl.has_value()) return std::unexpected(ttl.error());
  return WriteOp{StringSet{.key = cmd.args[1], .value = cmd.args[3], .abs_ttl_ms = *ttl}};
}

Result<WriteOp> ParsePsetex(const RespCommand& cmd, uint64_t wall_now_ms) {
  const TtlSpec spec{
      .unit_ms = 1, .base = TtlBase::kRelative, .positive = true, .command = "psetex"};
  auto ttl = ParseTtl(cmd.args[2], spec, wall_now_ms);
  if (!ttl.has_value()) return std::unexpected(ttl.error());
  return WriteOp{StringSet{.key = cmd.args[1], .value = cmd.args[3], .abs_ttl_ms = *ttl}};
}

Result<WriteOp> ParseDel(const RespCommand& cmd, uint64_t /*wall_now_ms*/) {
  return WriteOp{Del{.keys = CollectArgs(cmd, 1)}};
}

Result<WriteOp> ParseSadd(const RespCommand& cmd, uint64_t /*wall_now_ms*/) {
  return WriteOp{SetAdd{.key = cmd.args[1], .members = CollectArgs(cmd, 2)}};
}

Result<WriteOp> ParseSrem(const RespCommand& cmd, uint64_t /*wall_now_ms*/) {
  return WriteOp{SetRem{.key = cmd.args[1], .members = CollectArgs(cmd, 2)}};
}

Result<WriteOp> ParseZadd(const RespCommand& cmd, uint64_t /*wall_now_ms*/) {
  size_t i = 2;
  while (i < cmd.args.size()) {
    const auto opt = AsciiUpper(cmd.args[i]);
    if (opt == "NX" || opt == "XX" || opt == "GT" || opt == "LT" || opt == "CH") {
      ++i;
      continue;
    }
    if (opt == "INCR") return std::unexpected(SyntaxError("ZADD INCR is not supported"));
    break;
  }
  if ((cmd.args.size() - i) < 2 || (cmd.args.size() - i) % 2 != 0) {
    return std::unexpected(SyntaxError("syntax error"));
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

Result<WriteOp> ParseZrem(const RespCommand& cmd, uint64_t /*wall_now_ms*/) {
  return WriteOp{ZsetRem{.key = cmd.args[1], .members = CollectArgs(cmd, 2)}};
}

Result<WriteOp> ParseHset(const RespCommand& cmd, uint64_t /*wall_now_ms*/) {
  if (cmd.args.size() % 2 != 0) {
    return std::unexpected(SyntaxError("wrong number of arguments for 'hset' command"));
  }
  std::vector<HashSet::FieldValue> fields;
  fields.reserve((cmd.args.size() - 2) / 2);
  for (size_t i = 2; i < cmd.args.size(); i += 2) {
    fields.push_back({.field = cmd.args[i], .value = cmd.args[i + 1]});
  }
  return WriteOp{HashSet{.key = cmd.args[1], .fields = std::move(fields)}};
}

Result<WriteOp> ParseHmset(const RespCommand& cmd, uint64_t /*wall_now_ms*/) {
  if (cmd.args.size() % 2 != 0) {
    return std::unexpected(SyntaxError("wrong number of arguments for 'hmset' command"));
  }
  std::vector<HashSet::FieldValue> fields;
  fields.reserve((cmd.args.size() - 2) / 2);
  for (size_t i = 2; i < cmd.args.size(); i += 2) {
    fields.push_back({.field = cmd.args[i], .value = cmd.args[i + 1]});
  }
  return WriteOp{HashMSet{.key = cmd.args[1], .fields = std::move(fields)}};
}

Result<WriteOp> ParseHdel(const RespCommand& cmd, uint64_t /*wall_now_ms*/) {
  return WriteOp{HashDel{.key = cmd.args[1], .fields = CollectArgs(cmd, 2)}};
}

// All four EXPIRE forms collapse to abs_ttl_ms. NX/XX/GT/LT are the
// engine's predicates, so they are only checked for legality here.
Result<WriteOp> ParseExpire(const RespCommand& cmd, uint64_t wall_now_ms, const TtlSpec& spec) {
  for (size_t i = 3; i < cmd.args.size(); ++i) {
    const auto opt = AsciiUpper(cmd.args[i]);
    if (opt != "NX" && opt != "XX" && opt != "GT" && opt != "LT") {
      return std::unexpected(SyntaxError("Unsupported option " + std::string(cmd.args[i])));
    }
  }
  auto ttl = ParseTtl(cmd.args[2], spec, wall_now_ms);
  if (!ttl.has_value()) return std::unexpected(ttl.error());
  return WriteOp{Expire{.key = cmd.args[1], .abs_ttl_ms = *ttl}};
}

Result<WriteOp> ParseExpireSeconds(const RespCommand& cmd, uint64_t wall_now_ms) {
  return ParseExpire(cmd, wall_now_ms,
                     {.unit_ms = 1000, .base = TtlBase::kRelative, .command = "expire"});
}

Result<WriteOp> ParseExpireMs(const RespCommand& cmd, uint64_t wall_now_ms) {
  return ParseExpire(cmd, wall_now_ms,
                     {.unit_ms = 1, .base = TtlBase::kRelative, .command = "pexpire"});
}

Result<WriteOp> ParseExpireAt(const RespCommand& cmd, uint64_t wall_now_ms) {
  return ParseExpire(cmd, wall_now_ms,
                     {.unit_ms = 1000, .base = TtlBase::kAbsolute, .command = "expireat"});
}

Result<WriteOp> ParsePexpireAt(const RespCommand& cmd, uint64_t wall_now_ms) {
  return ParseExpire(cmd, wall_now_ms,
                     {.unit_ms = 1, .base = TtlBase::kAbsolute, .command = "pexpireat"});
}

Result<WriteOp> ParsePersist(const RespCommand& cmd, uint64_t /*wall_now_ms*/) {
  return WriteOp{Persist{.key = cmd.args[1]}};
}

const std::unordered_map<std::string_view, WriteParserFn>& WriteParsers() {
  // MSET is intentionally absent: the engine decomposes it into per-key SETs
  // before queueing, so the WAL never carries an MSET payload.
  static const std::unordered_map<std::string_view, WriteParserFn> table{
      {"SET", ParseSet},
      {"SETEX", ParseSetex},
      {"PSETEX", ParsePsetex},
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

uint64_t WallNowMs() { return NowWallMs(); }

Result<WriteOp> ParseWriteOp(std::string_view name, const RespCommand& cmd, uint64_t wall_now_ms) {
  const auto& table = WriteParsers();
  const auto it = table.find(name);
  if (it == table.end()) {
    return std::unexpected(
        Error(ErrorCode::kInvalidArgument, "unknown write command '" + std::string(name) + "'"));
  }
  return it->second(cmd, wall_now_ms);
}

bool HasWriteParser(std::string_view name) { return WriteParsers().contains(name); }

// std::to_string would round to 6 decimals and silently change scores on
// the way to the WAL.
std::string ScoreToString(double score) {
  std::array<char, 40> buf{};
  const auto [ptr, ec] = std::to_chars(buf.data(), buf.data() + buf.size(), score);
  if (ec != std::errc{}) return "0";
  return {buf.data(), ptr};
}

namespace {

// Sized up front and filled by emplace: an initializer_list would copy every
// argument (its elements are const, so they cannot be moved from) and an
// unreserved vector would then realloc its way up. Both matter here -- this
// runs once per write, on the client's thread.
RespCommand Begin(std::string_view name, size_t arg_count) {
  RespCommand cmd;
  cmd.args.reserve(arg_count);
  cmd.args.emplace_back(name);
  return cmd;
}

void PushAll(RespCommand& cmd, const std::vector<std::string_view>& values) {
  for (const auto& v : values) cmd.args.emplace_back(v);
}

}  // namespace

RespCommand CanonicalCommand(const WriteOp& op) {
  return std::visit(
      [](const auto& o) -> RespCommand {
        using T = std::decay_t<decltype(o)>;
        if constexpr (std::is_same_v<T, StringSet>) {
          auto cmd = Begin("SET", o.abs_ttl_ms > 0 ? 5 : 3);
          cmd.args.emplace_back(o.key);
          cmd.args.emplace_back(o.value);
          if (o.abs_ttl_ms > 0) {
            cmd.args.emplace_back("PXAT");
            cmd.args.emplace_back(std::to_string(o.abs_ttl_ms));
          }
          return cmd;
        } else if constexpr (std::is_same_v<T, Del>) {
          auto cmd = Begin("DEL", 1 + o.keys.size());
          PushAll(cmd, o.keys);
          return cmd;
        } else if constexpr (std::is_same_v<T, SetAdd>) {
          auto cmd = Begin("SADD", 2 + o.members.size());
          cmd.args.emplace_back(o.key);
          PushAll(cmd, o.members);
          return cmd;
        } else if constexpr (std::is_same_v<T, SetRem>) {
          auto cmd = Begin("SREM", 2 + o.members.size());
          cmd.args.emplace_back(o.key);
          PushAll(cmd, o.members);
          return cmd;
        } else if constexpr (std::is_same_v<T, ZsetAdd>) {
          auto cmd = Begin("ZADD", 2 + (2 * o.entries.size()));
          cmd.args.emplace_back(o.key);
          for (const auto& e : o.entries) {
            cmd.args.push_back(ScoreToString(e.score));
            cmd.args.emplace_back(e.member);
          }
          return cmd;
        } else if constexpr (std::is_same_v<T, ZsetRem>) {
          auto cmd = Begin("ZREM", 2 + o.members.size());
          cmd.args.emplace_back(o.key);
          PushAll(cmd, o.members);
          return cmd;
        } else if constexpr (std::is_same_v<T, HashSet> || std::is_same_v<T, HashMSet>) {
          // HMSET acknowledges with OK where HSET returns a count, so they are
          // distinct ops and must stay distinct commands.
          constexpr std::string_view kName = std::is_same_v<T, HashSet> ? "HSET" : "HMSET";
          auto cmd = Begin(kName, 2 + (2 * o.fields.size()));
          cmd.args.emplace_back(o.key);
          for (const auto& f : o.fields) {
            cmd.args.emplace_back(f.field);
            cmd.args.emplace_back(f.value);
          }
          return cmd;
        } else if constexpr (std::is_same_v<T, HashDel>) {
          auto cmd = Begin("HDEL", 2 + o.fields.size());
          cmd.args.emplace_back(o.key);
          PushAll(cmd, o.fields);
          return cmd;
        } else if constexpr (std::is_same_v<T, Expire>) {
          auto cmd = Begin("PEXPIREAT", 3);
          cmd.args.emplace_back(o.key);
          cmd.args.push_back(std::to_string(o.abs_ttl_ms));
          return cmd;
        } else {
          static_assert(std::is_same_v<T, Persist>);
          auto cmd = Begin("PERSIST", 2);
          cmd.args.emplace_back(o.key);
          return cmd;
        }
      },
      op);
}

std::string_view PrimaryKey(const ReadOp& op) {
  return std::visit(
      [](const auto& o) -> std::string_view {
        using T = std::decay_t<decltype(o)>;
        if constexpr (std::is_same_v<T, Exists>) {
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
        } else {
          return o.key;
        }
      },
      op);
}

}  // namespace abyss::core::ops
