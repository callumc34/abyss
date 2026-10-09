#include "abyss/engine/decide.h"

#include <algorithm>
#include <array>
#include <charconv>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <variant>
#include <vector>

#include "abyss/core/ascii.h"
#include "abyss/core/fatal.h"
#include "abyss/core/ops.h"

namespace abyss::engine {

namespace {

namespace ops = core::ops;
using Presence = hot::KeyView::Presence;
using Type = hot::Entry::Type;
using core::PredicateFlags;
using core::RespValue;

core::Error Syntax(std::string message) {
  return {core::ErrorCode::kInvalidArgument, std::move(message)};
}

core::Error WrongType() {
  return {core::ErrorCode::kWrongType, "Operation against a key holding the wrong kind of value"};
}

Decision Fail(core::Error error) { return Decision{.error = std::move(error)}; }

enum class Kind : uint8_t {
  kSet,
  kSetNx,
  kMSet,
  kMSetNx,
  kDel,
  kSAdd,
  kSRem,
  kZAdd,
  kZRem,
  kHSet,
  kHDel,
  kHSetNx,
  kExpire,
  kPersist,
  kRenameNx,
  kCopy,
};

// Where a command's keys are.
enum class Keys : uint8_t {
  kFirst,
  // Arguments 1 and 2.
  kFirstTwo,
  // Every argument after the name.
  kAll,
  // Every other argument from 1: key-value pairs.
  kPairs,
};

struct Command {
  std::string_view name;
  // As the registry's: exact if positive, a minimum if negative.
  int arity;
  Kind kind;
  Keys keys = Keys::kFirst;
  bool grows = false;
};

constexpr auto kCommands = std::to_array<Command>({
    {.name = "SET", .arity = -3, .kind = Kind::kSet, .grows = true},
    {.name = "SETEX", .arity = 4, .kind = Kind::kSet, .grows = true},
    {.name = "PSETEX", .arity = 4, .kind = Kind::kSet, .grows = true},
    {.name = "SETNX", .arity = 3, .kind = Kind::kSetNx, .grows = true},
    {.name = "MSET", .arity = -3, .kind = Kind::kMSet, .keys = Keys::kPairs, .grows = true},
    {.name = "MSETNX", .arity = -3, .kind = Kind::kMSetNx, .keys = Keys::kPairs, .grows = true},
    {.name = "DEL", .arity = -2, .kind = Kind::kDel, .keys = Keys::kAll},
    {.name = "UNLINK", .arity = -2, .kind = Kind::kDel, .keys = Keys::kAll},
    {.name = "SADD", .arity = -3, .kind = Kind::kSAdd, .grows = true},
    {.name = "SREM", .arity = -3, .kind = Kind::kSRem},
    {.name = "ZADD", .arity = -4, .kind = Kind::kZAdd, .grows = true},
    {.name = "ZREM", .arity = -3, .kind = Kind::kZRem},
    {.name = "HSET", .arity = -4, .kind = Kind::kHSet, .grows = true},
    {.name = "HMSET", .arity = -4, .kind = Kind::kHSet, .grows = true},
    {.name = "HDEL", .arity = -3, .kind = Kind::kHDel},
    {.name = "HSETNX", .arity = 4, .kind = Kind::kHSetNx, .grows = true},
    {.name = "EXPIRE", .arity = -3, .kind = Kind::kExpire},
    {.name = "PEXPIRE", .arity = -3, .kind = Kind::kExpire},
    {.name = "EXPIREAT", .arity = -3, .kind = Kind::kExpire},
    {.name = "PEXPIREAT", .arity = -3, .kind = Kind::kExpire},
    {.name = "PERSIST", .arity = 2, .kind = Kind::kPersist},
    {.name = "RENAMENX",
     .arity = 3,
     .kind = Kind::kRenameNx,
     .keys = Keys::kFirstTwo,
     .grows = true},
    {.name = "COPY", .arity = -3, .kind = Kind::kCopy, .keys = Keys::kFirstTwo, .grows = true},
});

const Command* FindCommand(std::string_view name) {
  for (const Command& command : kCommands) {
    if (command.name == name) return &command;
  }
  return nullptr;
}

bool ArityHolds(const Command& command, size_t argc) {
  const auto n = static_cast<int>(argc);
  if (command.arity >= 0 ? n != command.arity : n < -command.arity) return false;
  // MSET and MSETNX take key-value pairs.
  return (command.kind != Kind::kMSet && command.kind != Kind::kMSetNx) || n % 2 == 1;
}

// Whether a collection write changes `view`, a present key of its type.
// HSET always does: Redis counts every pair it sets.
bool Changes(const ops::WriteOp& op, const hot::KeyView& view) {
  return std::visit(
      [&view](const auto& o) {
        using T = std::decay_t<decltype(o)>;
        if constexpr (std::is_same_v<T, ops::SetAdd>) {
          return std::ranges::any_of(o.members, [&view](auto m) { return !view.set_has(m); });
        } else if constexpr (std::is_same_v<T, ops::SetRem>) {
          return std::ranges::any_of(o.members, [&view](auto m) { return view.set_has(m); });
        } else if constexpr (std::is_same_v<T, ops::ZsetRem>) {
          return std::ranges::any_of(o.members,
                                     [&view](auto m) { return view.zset_score(m).has_value(); });
        } else if constexpr (std::is_same_v<T, ops::HashDel>) {
          return std::ranges::any_of(o.fields, [&view](auto f) { return view.hash_has(f); });
        } else {
          return true;
        }
      },
      op);
}

struct KeyRead {
  enum class State : uint8_t { kPresent, kAbsent, kUnknown };
  State state = State::kUnknown;
  hot::KeyView view;

  bool present() const { return state == State::kPresent; }
  bool unknown() const { return state == State::kUnknown; }
};

class Decider {
 public:
  Decider(core::RespCommand& cmd, PredicateFlags flags, uint64_t now_ms, const KeyLookup& lookup)
      : cmd_(cmd), flags_(flags), now_ms_(now_ms), lookup_(lookup) {}

  Decision Set(std::string_view name);
  Decision SetNx();
  Decision MSet();
  Decision MSetNx();
  Decision Del();
  Decision Collection(std::string_view name, Type type, bool removes);
  Decision ZAdd();
  Decision Expire(std::string_view name);
  Decision Persist();
  Decision HSetNx();
  Decision RenameNx();
  Decision Copy();

 private:
  bool Has(PredicateFlags flag) const { return core::HasFlag(flags_, flag); }
  std::string_view Arg(size_t i) const { return cmd_.args[i]; }

  KeyRead Read(std::string_view key, Need need);
  void Observe(core::ShardId shard, core::SequenceId seq);
  // A small effect, built in canonical form from `op`.
  void Emit(const ops::WriteOp& op, bool replaces_state);
  // Starts an effect for Append and Take to fill.
  void Open(std::string_view name, std::string_view key, size_t argc, bool replaces_state,
            bool reply_old_value = false);
  void Append(std::string arg) { decision_.effects.back().cmd.args.push_back(std::move(arg)); }
  // Moves request argument `i` into the open effect.
  void Take(size_t i);
  // SET <key> <value> [PXAT ms]: the key copied, the value moved.
  void EmitSet(size_t key, size_t value, uint64_t abs_ttl_ms, bool reply_old_value = false);
  // The request itself, its name canonical.
  void EmitRequest(std::string_view name, bool replaces_state);
  // ZADD of `zadd`'s `entries`: scores canonical, members moved.
  void EmitZAdd(const ops::ZsetAdd& zadd, const std::vector<size_t>& entries, bool replaces_state);
  // Emits `key`'s DEL once, returning whether it did; later reads see
  // it absent.
  bool Delete(std::string_view key);
  // The effects that rebuild `src`'s value and TTL at `dst`.
  void Recreate(std::string_view dst, const hot::KeyView& src);
  void Reply(RespValue reply) { decision_.reply = std::move(reply); }
  // WRONGTYPE for a key read: it reveals that key's state, so the reply
  // keeps what was observed for the fence.
  Decision WrongTypeOf();
  Decision Finish();

  core::RespCommand& cmd_;
  PredicateFlags flags_;
  uint64_t now_ms_;
  const KeyLookup& lookup_;
  Decision decision_;
  // Views of cmd_'s own arguments.
  std::unordered_set<std::string_view> deleted_;
};

KeyRead Decider::Read(std::string_view key, Need need) {
  KeyRead read{.view = lookup_(key)};
  switch (read.view.presence) {
    case Presence::kLive:
      if (read.view.value == nullptr) core::Fatal("a live key's view has no value");
      read.state = KeyRead::State::kPresent;
      break;
    case Presence::kTombstoned:
      read.state = KeyRead::State::kAbsent;
      break;
    case Presence::kExpired:
      // Logged, so cold and replay see this expiry whatever their clock.
      if (Delete(key)) decision_.effects.back().observed_expiry = true;
      read.state = KeyRead::State::kAbsent;
      break;
    case Presence::kStub:
      read.state = need == Need::kExistence ? KeyRead::State::kPresent : KeyRead::State::kUnknown;
      break;
    case Presence::kNonResident:
      break;
  }
  if (read.unknown()) {
    const auto it = std::ranges::find(decision_.needs_load, key, &KeyLoad::key);
    if (it == decision_.needs_load.end()) {
      decision_.needs_load.push_back({.key = std::string(key), .need = need});
    } else {
      it->need = std::max(it->need, need);
    }
    return read;
  }
  Observe(read.view.shard, read.view.latest_seq);
  if (deleted_.contains(key)) read.state = KeyRead::State::kAbsent;
  return read;
}

void Decider::Observe(core::ShardId shard, core::SequenceId seq) {
  const auto it = std::ranges::find(decision_.observed, shard, &ShardSeq::shard);
  if (it == decision_.observed.end()) {
    decision_.observed.push_back({.shard = shard, .seq = seq});
  } else {
    it->seq = std::max(it->seq, seq);
  }
}

void Decider::Emit(const ops::WriteOp& op, bool replaces_state) {
  decision_.effects.push_back(core::Effect{
      .key = std::string(ops::PrimaryKey(op)),
      .cmd = ops::CanonicalCommand(op),
      .replaces_state = replaces_state,
  });
}

void Decider::Open(std::string_view name, std::string_view key, size_t argc, bool replaces_state,
                   bool reply_old_value) {
  core::Effect effect{.key = std::string(key),
                      .replaces_state = replaces_state,
                      .reply_old_value = reply_old_value};
  effect.cmd.args.reserve(argc);
  effect.cmd.args.emplace_back(name);
  decision_.effects.push_back(std::move(effect));
}

void Decider::Take(size_t i) {
  auto& args = decision_.effects.back().cmd.args;
  decision_.moved.push_back({.effect = static_cast<uint32_t>(decision_.effects.size() - 1),
                             .arg = static_cast<uint32_t>(args.size()),
                             .request_arg = static_cast<uint32_t>(i)});
  args.push_back(std::move(cmd_.args[i]));
}

void Decider::EmitSet(size_t key, size_t value, uint64_t abs_ttl_ms, bool reply_old_value) {
  Open("SET", Arg(key), abs_ttl_ms > 0 ? 5 : 3, /*replaces_state=*/true, reply_old_value);
  Append(std::string(Arg(key)));
  Take(value);
  if (abs_ttl_ms > 0) {
    Append("PXAT");
    Append(std::to_string(abs_ttl_ms));
  }
}

void Decider::EmitRequest(std::string_view name, bool replaces_state) {
  const size_t argc = cmd_.args.size();
  Open(name, Arg(1), argc, replaces_state);
  for (size_t i = 1; i < argc; ++i) Take(i);
}

void Decider::EmitZAdd(const ops::ZsetAdd& zadd, const std::vector<size_t>& entries,
                       bool replaces_state) {
  // The score-member pairs end the request.
  const size_t first = cmd_.args.size() - (2 * zadd.entries.size());
  Open("ZADD", zadd.key, 2 + (2 * entries.size()), replaces_state);
  Append(std::string(zadd.key));
  for (const size_t entry : entries) {
    Append(ops::ScoreToString(zadd.entries[entry].score));
    Take(first + (2 * entry) + 1);
  }
}

bool Decider::Delete(std::string_view key) {
  if (!deleted_.insert(key).second) return false;
  Emit(ops::Del{.keys = {key}}, /*replaces_state=*/true);
  return true;
}

// The value is hot's, so it is copied: there is nothing to move.
void Decider::Recreate(std::string_view dst, const hot::KeyView& src) {
  const uint64_t ttl = src.abs_ttl_ms > 0 ? static_cast<uint64_t>(src.abs_ttl_ms) : 0;
  std::visit(
      [this, dst, ttl](const auto& value) {
        using T = std::decay_t<decltype(value)>;
        if constexpr (std::is_same_v<T, std::string>) {
          Emit(ops::StringSet{.key = dst, .value = value, .abs_ttl_ms = ttl},
               /*replaces_state=*/true);
        } else if constexpr (std::is_same_v<T, hot::SetValue>) {
          Emit(ops::SetAdd{.key = dst,
                           .members = std::vector<std::string_view>(value.members.begin(),
                                                                    value.members.end())},
               /*replaces_state=*/true);
        } else if constexpr (std::is_same_v<T, hot::ZsetValue>) {
          std::vector<ops::ZsetAdd::Entry> entries;
          entries.reserve(value.member_scores.size());
          for (const auto& [score, members] : value.score_members) {
            for (const auto& member : members) {
              entries.push_back({.score = score, .member = member});
            }
          }
          Emit(ops::ZsetAdd{.key = dst, .entries = std::move(entries)}, /*replaces_state=*/true);
        } else {
          static_assert(std::is_same_v<T, hot::HashValue>);
          std::vector<ops::HashSet::FieldValue> fields;
          fields.reserve(value.fields.size());
          for (const auto& [field, field_value] : value.fields) {
            fields.push_back({.field = field, .value = field_value});
          }
          Emit(ops::HashSet{.key = dst, .fields = std::move(fields)}, /*replaces_state=*/true);
        }
      },
      *src.value);
  // A string carries its TTL in its SET. The EXPIRE alone does not
  // determine the key, so it is not flagged.
  if (ttl > 0 && src.type != Type::kString) {
    Emit(ops::Expire{.key = dst, .abs_ttl_ms = ttl}, /*replaces_state=*/false);
  }
}

Decision Decider::WrongTypeOf() {
  ABYSS_DCHECK(decision_.effects.empty() && decision_.moved.empty(),
               "WRONGTYPE after an effect was emitted");
  return Decision{.observed = std::move(decision_.observed), .error = WrongType()};
}

Decision Decider::Finish() {
  ABYSS_DCHECK(decision_.needs_load.empty() || decision_.moved.empty(),
               "a decision that needs a load moved request arguments");
  if (decision_.needs_load.empty()) return std::move(decision_);
  return Decision{.needs_load = std::move(decision_.needs_load)};
}

Decision Decider::Set(std::string_view name) {
  auto op = ops::ParseWriteOp(name, cmd_, now_ms_);
  if (!op.has_value()) return Fail(op.error());
  const auto& set = std::get<ops::StringSet>(*op);
  // SETEX and PSETEX put the TTL before the value.
  const size_t value = name == "SET" ? 2 : 3;
  const bool nx = Has(PredicateFlags::kNx);
  const bool xx = Has(PredicateFlags::kXx);
  const bool get = Has(PredicateFlags::kGet);
  const bool keep_ttl = Has(PredicateFlags::kKeepTtl);
  if (nx && xx) return Fail(Syntax("syntax error"));
  // A TTL already past deletes the key, logged as a DEL as EXPIRE's is.
  const auto past = [this](uint64_t ttl) { return ttl != 0 && ttl <= now_ms_; };
  if (!nx && !xx && !get && !keep_ttl) {
    if (past(set.abs_ttl_ms)) {
      Delete(Arg(1));
      Reply(RespValue::SimpleString("OK"));
    } else {
      EmitSet(1, value, set.abs_ttl_ms);
    }
    return Finish();
  }

  // Only GET needs the value; a stub's type and TTL answer the rest.
  const KeyRead key = Read(set.key, get ? Need::kState : Need::kExistence);
  if (key.unknown()) return Finish();
  if (get && key.present() && key.view.type != Type::kString) return WrongTypeOf();
  if (nx && key.present()) {
    Reply(get ? RespValue::BulkString(std::string(key.view.string_value())) : RespValue::Null());
    return Finish();
  }
  if (xx && !key.present()) {
    Reply(RespValue::Null());
    return Finish();
  }
  uint64_t ttl = set.abs_ttl_ms;
  if (keep_ttl && key.present() && key.view.abs_ttl_ms > 0) {
    ttl = static_cast<uint64_t>(key.view.abs_ttl_ms);
  }
  if (past(ttl)) {
    if (!get) {
      Reply(RespValue::SimpleString("OK"));
    } else if (key.present()) {
      Reply(RespValue::BulkString(std::string(key.view.string_value())));
    } else {
      Reply(RespValue::Null());
    }
    if (key.present()) Delete(Arg(1));
    return Finish();
  }
  EmitSet(1, value, ttl, /*reply_old_value=*/get && key.present());
  if (get && !key.present()) Reply(RespValue::Null());
  return Finish();
}

Decision Decider::SetNx() {
  const KeyRead key = Read(Arg(1), Need::kExistence);
  if (key.unknown()) return Finish();
  if (key.present()) {
    Reply(RespValue::Integer(0));
    return Finish();
  }
  EmitSet(1, 2, 0);
  Reply(RespValue::Integer(1));
  return Finish();
}

Decision Decider::MSet() {
  for (size_t i = 1; i < cmd_.args.size(); i += 2) EmitSet(i, i + 1, 0);
  Reply(RespValue::SimpleString("OK"));
  return Finish();
}

Decision Decider::MSetNx() {
  bool any_present = false;
  for (size_t i = 1; i < cmd_.args.size(); i += 2) {
    any_present = Read(Arg(i), Need::kExistence).present() || any_present;
  }
  if (!decision_.needs_load.empty()) return Finish();
  if (any_present) {
    Reply(RespValue::Integer(0));
    return Finish();
  }
  for (size_t i = 1; i < cmd_.args.size(); i += 2) EmitSet(i, i + 1, 0);
  Reply(RespValue::Integer(1));
  return Finish();
}

Decision Decider::Del() {
  int64_t deleted = 0;
  for (size_t i = 1; i < cmd_.args.size(); ++i) {
    if (!Read(Arg(i), Need::kExistence).present()) continue;
    Delete(Arg(i));
    ++deleted;
  }
  Reply(RespValue::Integer(deleted));
  return Finish();
}

Decision Decider::Collection(std::string_view name, Type type, bool removes) {
  auto op = ops::ParseWriteOp(name, cmd_, now_ms_);
  if (!op.has_value()) return Fail(op.error());
  const KeyRead key = Read(ops::PrimaryKey(*op), Need::kState);
  if (key.unknown()) return Finish();
  if (key.present() && key.view.type != type) return WrongTypeOf();
  // As Redis propagates nothing for a write that changes nothing,
  // nothing is logged; the reply fences on what was read.
  if (!key.present() ? removes : !Changes(*op, key.view)) {
    Reply(RespValue::Integer(0));
    return Finish();
  }
  EmitRequest(name, /*replaces_state=*/!key.present());
  return Finish();
}

Decision Decider::ZAdd() {
  auto op = ops::ParseWriteOp("ZADD", cmd_, now_ms_);
  if (!op.has_value()) return Fail(op.error());
  const auto& zadd = std::get<ops::ZsetAdd>(*op);
  const bool nx = Has(PredicateFlags::kNx);
  const bool xx = Has(PredicateFlags::kXx);
  const bool gt = Has(PredicateFlags::kZAddGt);
  const bool lt = Has(PredicateFlags::kZAddLt);
  const bool ch = Has(PredicateFlags::kZAddCh);
  if (nx && xx) return Fail(Syntax("XX and NX options at the same time are not compatible"));
  if ((gt && lt) || (nx && (gt || lt))) {
    return Fail(Syntax("GT, LT, and/or NX options at the same time are not compatible"));
  }

  const KeyRead key = Read(zadd.key, Need::kState);
  if (key.unknown()) return Finish();
  if (key.present() && key.view.type != Type::kZset) return WrongTypeOf();

  // Pair by pair, as Redis does, so a repeated member sees its own
  // earlier pair. GT and LT only gate updates; new members are added.
  std::unordered_map<std::string_view, double> scores;
  std::vector<size_t> kept;
  int64_t added = 0;
  int64_t changed = 0;
  for (size_t i = 0; i < zadd.entries.size(); ++i) {
    const auto& entry = zadd.entries[i];
    std::optional<double> current;
    if (const auto it = scores.find(entry.member); it != scores.end()) {
      current = it->second;
    } else if (key.present()) {
      current = key.view.zset_score(entry.member);
    }
    if (nx && current.has_value()) continue;
    if (xx && !current.has_value()) continue;
    if (current.has_value() && gt && !(entry.score > *current)) continue;
    if (current.has_value() && lt && !(entry.score < *current)) continue;
    kept.push_back(i);
    if (!current.has_value()) {
      ++added;
    } else if (entry.score != *current) {
      ++changed;
    }
    scores[entry.member] = entry.score;
  }
  // Redis logs a ZADD only when it adds or updates a score.
  if (added + changed == 0) {
    Reply(RespValue::Integer(0));
    return Finish();
  }
  EmitZAdd(zadd, kept, /*replaces_state=*/!key.present());
  Reply(RespValue::Integer(ch ? added + changed : added));
  return Finish();
}

Decision Decider::Expire(std::string_view name) {
  auto op = ops::ParseWriteOp(name, cmd_, now_ms_);
  if (!op.has_value()) return Fail(op.error());
  const auto& expire = std::get<ops::Expire>(*op);
  const bool nx = Has(PredicateFlags::kNx);
  const bool xx = Has(PredicateFlags::kXx);
  const bool gt = Has(PredicateFlags::kExpireGt);
  const bool lt = Has(PredicateFlags::kExpireLt);
  if (nx && (xx || gt || lt)) {
    return Fail(Syntax("NX and XX, GT or LT options at the same time are not compatible"));
  }
  if (gt && lt) return Fail(Syntax("GT and LT options at the same time are not compatible"));

  const KeyRead key = Read(expire.key, Need::kState);
  if (key.unknown()) return Finish();
  // No TTL counts as an infinite one for GT and LT.
  const auto current = static_cast<uint64_t>(key.view.abs_ttl_ms);
  const bool has_ttl = current > 0;
  const bool refused = !key.present() || (nx && has_ttl) || (xx && !has_ttl) ||
                       (gt && (!has_ttl || expire.abs_ttl_ms <= current)) ||
                       (lt && has_ttl && expire.abs_ttl_ms >= current);
  if (refused) {
    Reply(RespValue::Integer(0));
    return Finish();
  }
  // A time already past deletes the key, logged as a DEL as Redis does.
  if (expire.abs_ttl_ms <= now_ms_) {
    Delete(expire.key);
    Reply(RespValue::Integer(1));
    return Finish();
  }
  Emit(*op, /*replaces_state=*/false);
  return Finish();
}

Decision Decider::Persist() {
  const KeyRead key = Read(Arg(1), Need::kState);
  if (key.unknown()) return Finish();
  if (!key.present() || key.view.abs_ttl_ms == 0) {
    Reply(RespValue::Integer(0));
    return Finish();
  }
  Emit(ops::Persist{.key = Arg(1)}, /*replaces_state=*/false);
  return Finish();
}

Decision Decider::HSetNx() {
  const KeyRead key = Read(Arg(1), Need::kState);
  if (key.unknown()) return Finish();
  if (key.present() && key.view.type != Type::kHash) return WrongTypeOf();
  if (key.present() && key.view.hash_has(Arg(2))) {
    Reply(RespValue::Integer(0));
    return Finish();
  }
  Open("HSET", Arg(1), 4, /*replaces_state=*/!key.present());
  Append(std::string(Arg(1)));
  Take(2);
  Take(3);
  Reply(RespValue::Integer(1));
  return Finish();
}

Decision Decider::RenameNx() {
  const KeyRead src = Read(Arg(1), Need::kState);
  const KeyRead dst = Read(Arg(2), Need::kExistence);
  if (src.unknown() || dst.unknown()) return Finish();
  if (!src.present()) {
    Reply(RespValue::Error(core::ErrorPrefix::kErr, "no such key"));
    return Finish();
  }
  if (dst.present()) {
    Reply(RespValue::Integer(0));
    return Finish();
  }
  Delete(Arg(1));
  Recreate(Arg(2), src.view);
  Reply(RespValue::Integer(1));
  return Finish();
}

Decision Decider::Copy() {
  bool replace = false;
  for (size_t i = 3; i < cmd_.args.size(); ++i) {
    const std::string option = core::AsciiUpper(Arg(i));
    if (option == "REPLACE") {
      replace = true;
    } else if (option == "DB") {
      if (i + 1 >= cmd_.args.size()) return Fail(Syntax("syntax error"));
      const std::string_view db = Arg(++i);
      int64_t index = 0;
      const auto [end, ec] = std::from_chars(db.data(), db.data() + db.size(), index);
      if (ec != std::errc{} || end != db.data() + db.size()) {
        return Fail(Syntax("value is not an integer or out of range"));
      }
      // Only database 0 exists.
      if (index != 0) return Fail(Syntax("DB index is out of range"));
    } else {
      return Fail(Syntax("syntax error"));
    }
  }
  if (Arg(1) == Arg(2)) return Fail(Syntax("source and destination objects are the same"));

  const KeyRead src = Read(Arg(1), Need::kState);
  const KeyRead dst = Read(Arg(2), Need::kExistence);
  if (src.unknown() || dst.unknown()) return Finish();
  if (!src.present() || (dst.present() && !replace)) {
    Reply(RespValue::Integer(0));
    return Finish();
  }
  if (dst.present()) Delete(Arg(2));
  Recreate(Arg(2), src.view);
  Reply(RespValue::Integer(1));
  return Finish();
}

core::Result<const Command*> Recognise(const core::RespCommand& cmd) {
  if (cmd.args.empty()) return std::unexpected(Syntax("empty command"));
  const std::string name = core::AsciiUpper(cmd.args[0]);
  const Command* command = FindCommand(name);
  if (command == nullptr) {
    return std::unexpected(Syntax("unsupported write command '" + name + "'"));
  }
  if (!ArityHolds(*command, cmd.args.size())) {
    return std::unexpected(
        Syntax("wrong number of arguments for '" + core::AsciiLower(name) + "' command"));
  }
  return command;
}

}  // namespace

core::Result<std::vector<std::string_view>> WriteKeys(const core::RespCommand& cmd) {
  auto command = Recognise(cmd);
  if (!command.has_value()) return std::unexpected(command.error());
  std::vector<std::string_view> keys;
  switch ((*command)->keys) {
    case Keys::kFirst:
      keys.emplace_back(cmd.args[1]);
      break;
    case Keys::kFirstTwo:
      keys.emplace_back(cmd.args[1]);
      keys.emplace_back(cmd.args[2]);
      break;
    case Keys::kAll:
      keys.assign(cmd.args.begin() + 1, cmd.args.end());
      break;
    case Keys::kPairs:
      for (size_t i = 1; i < cmd.args.size(); i += 2) keys.emplace_back(cmd.args[i]);
      break;
  }
  return keys;
}

bool GrowsMemory(const core::RespCommand& cmd) {
  auto command = Recognise(cmd);
  return command.has_value() && (*command)->grows;
}

Decision Decide(core::RespCommand& cmd, PredicateFlags flags, uint64_t now_ms,
                const KeyLookup& lookup) {
  auto recognised = Recognise(cmd);
  if (!recognised.has_value()) return Fail(recognised.error());
  const Command* command = *recognised;
  const std::string name(command->name);

  Decider decider(cmd, flags, now_ms, lookup);
  switch (command->kind) {
    case Kind::kSet:
      return decider.Set(name);
    case Kind::kSetNx:
      return decider.SetNx();
    case Kind::kMSet:
      return decider.MSet();
    case Kind::kMSetNx:
      return decider.MSetNx();
    case Kind::kDel:
      return decider.Del();
    case Kind::kSAdd:
      return decider.Collection(name, Type::kSet, /*removes=*/false);
    case Kind::kSRem:
      return decider.Collection(name, Type::kSet, /*removes=*/true);
    case Kind::kZAdd:
      return decider.ZAdd();
    case Kind::kZRem:
      return decider.Collection(name, Type::kZset, /*removes=*/true);
    case Kind::kHSet:
      return decider.Collection(name, Type::kHash, /*removes=*/false);
    case Kind::kHDel:
      return decider.Collection(name, Type::kHash, /*removes=*/true);
    case Kind::kHSetNx:
      return decider.HSetNx();
    case Kind::kExpire:
      return decider.Expire(name);
    case Kind::kPersist:
      return decider.Persist();
    case Kind::kRenameNx:
      return decider.RenameNx();
    case Kind::kCopy:
      return decider.Copy();
  }
  return Fail(Syntax("unsupported write command '" + name + "'"));
}

void Restore(Decision&& decision, core::RespCommand& cmd) {
  Decision taken = std::move(decision);
  ABYSS_DCHECK(
      [&taken] {
        std::vector<uint32_t> args;
        args.reserve(taken.moved.size());
        for (const Moved& moved : taken.moved) args.push_back(moved.request_arg);
        std::ranges::sort(args);
        return std::ranges::adjacent_find(args) == args.end();
      }(),
      "a request argument was moved twice");
  for (const Moved& moved : taken.moved) {
    cmd.args[moved.request_arg] = std::move(taken.effects[moved.effect].cmd.args[moved.arg]);
  }
}

}  // namespace abyss::engine
