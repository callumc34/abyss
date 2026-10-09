#pragma once

// A reference model of the Redis semantics Abyss serves: strings, sets,
// zsets and hashes with TTLs, every command judged at one instant. It
// knows nothing of tiers, logs or locks, which is what makes it a
// reference for them.

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <charconv>
#include <cstdint>
#include <functional>
#include <map>
#include <optional>
#include <set>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "abyss/core/ascii.h"
#include "abyss/core/resp_format.h"
#include "abyss/core/resp_types.h"

namespace abyss::testing {

class RedisModel {
 public:
  enum class Type : uint8_t { kString, kSet, kZset, kHash };

  struct Value {
    Type type = Type::kString;
    std::string str;
    std::set<std::string> set;
    std::map<std::string, double> zset;
    std::map<std::string, std::string> hash;
    // Absolute ms; 0 is none. Absent once now reaches it.
    int64_t abs_ttl_ms = 0;

    bool operator==(const Value&) const = default;
  };

  // Whether a multi-key write over these keys is refused CROSSSLOT.
  using CrossSlot = std::function<bool(std::span<const std::string> keys)>;

  RedisModel() = default;
  explicit RedisModel(CrossSlot cross_slot) : cross_slot_(std::move(cross_slot)) {}

  static bool Expired(const Value& value, int64_t now_ms) {
    return value.abs_ttl_ms != 0 && now_ms >= value.abs_ttl_ms;
  }

  // The key's state at `now_ms`, or null when absent.
  const Value* Find(std::string_view key, int64_t now_ms) const {
    const auto it = data_.find(std::string(key));
    if (it == data_.end() || Expired(it->second, now_ms)) return nullptr;
    return &it->second;
  }

  // Every key held, expired ones included.
  const std::map<std::string, Value>& data() const { return data_; }
  void Clear() { data_.clear(); }
  void Put(const std::string& key, Value value) { data_[key] = std::move(value); }
  void Erase(const std::string& key) { data_.erase(key); }

  // `args` as Redis runs them at `now_ms`. Assumes well-formed input:
  // the generator only makes commands the server parses.
  core::RespValue Execute(std::span<const std::string> args, int64_t now_ms) {
    now_ = now_ms;
    const std::string name = core::AsciiUpper(args[0]);
    if (!Modelled(name)) {
      ADD_FAILURE() << "the model does not model " << name;
      return core::RespValue::Error(core::ErrorPrefix::kErr, "unmodelled command");
    }
    if (name == "FLUSHDB") {
      data_.clear();
      return Ok();
    }
    if (IsMultiKeyWrite(name) && cross_slot_ && cross_slot_(WriteKeys(name, args))) {
      return core::RespValue::Error(core::ErrorPrefix::kCrossSlot,
                                    "Keys in request don't hash to the same slot");
    }
    if (name == "SET") return Set(args);
    if (name == "SETEX" || name == "PSETEX") {
      const int64_t unit = name == "SETEX" ? 1000 : 1;
      Store(args[1], StringOf(args[3], now_ + (Int(args[2]) * unit)));
      return Ok();
    }
    if (name == "SETNX") {
      if (Live(args[1]) != nullptr) return Int64(0);
      Store(args[1], StringOf(args[2], 0));
      return Int64(1);
    }
    if (name == "MSET") {
      for (size_t i = 1; i + 1 < args.size(); i += 2) Store(args[i], StringOf(args[i + 1], 0));
      return Ok();
    }
    if (name == "MSETNX") {
      for (size_t i = 1; i + 1 < args.size(); i += 2) {
        if (Live(args[i]) != nullptr) return Int64(0);
      }
      for (size_t i = 1; i + 1 < args.size(); i += 2) Store(args[i], StringOf(args[i + 1], 0));
      return Int64(1);
    }
    if (name == "DEL" || name == "UNLINK") {
      int64_t deleted = 0;
      for (size_t i = 1; i < args.size(); ++i) {
        if (Live(args[i]) == nullptr) continue;
        data_.erase(args[i]);
        ++deleted;
      }
      return Int64(deleted);
    }
    if (name == "SADD" || name == "SREM") return SetWrite(name == "SADD", args);
    if (name == "ZADD") return ZAdd(args);
    if (name == "ZREM") return ZRem(args);
    if (name == "HSET" || name == "HMSET") return HSet(name == "HMSET", args);
    if (name == "HDEL") return HDel(args);
    if (name == "HSETNX") return HSetNx(args);
    if (name == "EXPIRE" || name == "PEXPIRE" || name == "EXPIREAT" || name == "PEXPIREAT") {
      return Expire(name, args);
    }
    if (name == "PERSIST") {
      Value* value = Live(args[1]);
      if (value == nullptr || value->abs_ttl_ms == 0) return Int64(0);
      value->abs_ttl_ms = 0;
      return Int64(1);
    }
    if (name == "RENAMENX") return RenameNx(args);
    if (name == "COPY") return Copy(args);
    return Read(name, args);
  }

  static bool Modelled(std::string_view name) {
    static constexpr auto kNames = std::to_array<std::string_view>(
        {"FLUSHDB", "SET",      "SETEX",    "PSETEX",    "SETNX",   "MSET",     "MSETNX",
         "DEL",     "UNLINK",   "SADD",     "SREM",      "ZADD",    "ZREM",     "HSET",
         "HMSET",   "HDEL",     "HSETNX",   "EXPIRE",    "PEXPIRE", "EXPIREAT", "PEXPIREAT",
         "PERSIST", "RENAMENX", "COPY",     "GET",       "MGET",    "EXISTS",   "TYPE",
         "TTL",     "PTTL",     "SMEMBERS", "SISMEMBER", "SCARD",   "ZSCORE",   "ZCARD",
         "ZRANGE",  "HGET",     "HGETALL",  "HMGET",     "HEXISTS", "HLEN",     "HKEYS",
         "HVALS"});
    return std::ranges::find(kNames, name) != kNames.end();
  }

  // Multi-key writes, whose keys must share a log.
  static bool IsMultiKeyWrite(std::string_view name) {
    return name == "MSET" || name == "MSETNX" || name == "DEL" || name == "UNLINK" ||
           name == "RENAMENX" || name == "COPY";
  }

  // The keys a write names, as the server routes it.
  static std::vector<std::string> WriteKeys(std::string_view name,
                                            std::span<const std::string> args) {
    std::vector<std::string> keys;
    if (name == "MSET" || name == "MSETNX") {
      for (size_t i = 1; i < args.size(); i += 2) keys.push_back(args[i]);
    } else if (name == "DEL" || name == "UNLINK") {
      keys.assign(args.begin() + 1, args.end());
    } else if (name == "RENAMENX" || name == "COPY") {
      keys = {args[1], args[2]};
    } else if (args.size() > 1) {
      keys = {args[1]};
    }
    return keys;
  }

  // A canonical text of a reply: arrays from unordered reads must be
  // sorted by the caller first (Canonical does it by command name).
  static std::string Render(const core::RespValue& value) {
    if (value.IsNull()) return "nil";
    if (value.IsNullArray()) return "nil*";
    if (value.IsInteger()) return ":" + std::to_string(value.AsInteger());
    if (value.IsSimpleString()) return "+" + value.AsString();
    if (value.IsBulkString()) return "$" + value.AsString();
    // Errors in full: the server's texts are Redis's.
    if (value.IsError()) return "-" + value.AsString();
    std::string out = "[";
    for (size_t i = 0; i < value.AsArray().size(); ++i) {
      if (i > 0) out += ",";
      out += Render(value.AsArray()[i]);
    }
    return out + "]";
  }

  // Render, with the replies whose order Redis leaves open sorted.
  static std::string Canonical(std::string_view command, const core::RespValue& value) {
    const std::string name = core::AsciiUpper(command);
    if (!value.IsArray()) return Render(value);
    std::vector<std::string> parts;
    const auto& elements = value.AsArray();
    if (name == "HGETALL") {
      for (size_t i = 0; i + 1 < elements.size(); i += 2) {
        parts.push_back(Render(elements[i]) + "=" + Render(elements[i + 1]));
      }
    } else if (name == "SMEMBERS" || name == "HKEYS" || name == "HVALS") {
      for (const auto& element : elements) parts.push_back(Render(element));
    } else {
      return Render(value);
    }
    std::ranges::sort(parts);
    std::string out = "[";
    for (size_t i = 0; i < parts.size(); ++i) out += (i > 0 ? "," : "") + parts[i];
    return out + "]";
  }

  // `keys`' live state at `now_ms` as text, one-to-one, for memoised
  // searches.
  std::string Digest(std::span<const std::string> keys, int64_t now_ms) const {
    std::string out;
    const auto put = [&out](std::string_view field) {
      out += std::to_string(field.size());
      out += ':';
      out += field;
    };
    for (const auto& key : keys) {
      const Value* value = Find(key, now_ms);
      if (value == nullptr) {
        put("-");
        continue;
      }
      put(std::to_string(static_cast<int>(value->type)));
      put(std::to_string(value->abs_ttl_ms));
      put(value->str);
      put(std::to_string(value->set.size()));
      for (const auto& member : value->set) put(member);
      put(std::to_string(value->zset.size()));
      for (const auto& [member, score] : value->zset) {
        put(member);
        put(core::FormatRespDouble(score));
      }
      put(std::to_string(value->hash.size()));
      for (const auto& [field, v] : value->hash) {
        put(field);
        put(v);
      }
    }
    return out;
  }

  // A value as text, for failure messages.
  static std::string Describe(const Value& value) {
    std::string out;
    switch (value.type) {
      case Type::kString:
        out = "\"" + value.str + "\"";
        break;
      case Type::kSet:
        out = "set{";
        for (const auto& member : value.set) out += member + ",";
        out += "}";
        break;
      case Type::kZset:
        out = "zset{";
        for (const auto& [member, score] : value.zset) {
          out += member + "=" + core::FormatRespDouble(score) + ",";
        }
        out += "}";
        break;
      case Type::kHash:
        out = "hash{";
        for (const auto& [field, v] : value.hash)
          out.append(field).append("=").append(v).append(",");
        out += "}";
        break;
    }
    if (value.abs_ttl_ms != 0) out += " ttl " + std::to_string(value.abs_ttl_ms);
    return out;
  }

 private:
  static core::RespValue Ok() { return core::RespValue::SimpleString("OK"); }
  static core::RespValue Int64(int64_t n) { return core::RespValue::Integer(n); }
  static core::RespValue WrongType() {
    return core::RespValue::Error(core::ErrorPrefix::kWrongType,
                                  "Operation against a key holding the wrong kind of value");
  }

  static int64_t Int(std::string_view text) {
    int64_t out = 0;
    std::from_chars(text.data(), text.data() + text.size(), out);
    return out;
  }
  static double Score(const std::string& text) { return std::stod(text); }

  static Value StringOf(std::string str, int64_t abs_ttl_ms) {
    return Value{.type = Type::kString, .str = std::move(str), .abs_ttl_ms = abs_ttl_ms};
  }

  // A live key, an expired one dropped first.
  Value* Live(const std::string& key) {
    const auto it = data_.find(key);
    if (it == data_.end()) return nullptr;
    if (Expired(it->second, now_)) {
      data_.erase(it);
      return nullptr;
    }
    return &it->second;
  }

  // A stored TTL already past leaves the key absent.
  void Store(const std::string& key, Value value) {
    if (Expired(value, now_)) {
      data_.erase(key);
      return;
    }
    data_[key] = std::move(value);
  }

  // Drops a collection its write emptied.
  void DropIfEmpty(const std::string& key) {
    const auto it = data_.find(key);
    if (it == data_.end()) return;
    const Value& value = it->second;
    if ((value.type == Type::kSet && value.set.empty()) ||
        (value.type == Type::kZset && value.zset.empty()) ||
        (value.type == Type::kHash && value.hash.empty())) {
      data_.erase(it);
    }
  }

  core::RespValue Set(std::span<const std::string> args) {
    bool nx = false;
    bool xx = false;
    bool get = false;
    bool keep_ttl = false;
    int64_t ttl = 0;
    for (size_t i = 3; i < args.size(); ++i) {
      const std::string opt = core::AsciiUpper(args[i]);
      if (opt == "NX") {
        nx = true;
      } else if (opt == "XX") {
        xx = true;
      } else if (opt == "GET") {
        get = true;
      } else if (opt == "KEEPTTL") {
        keep_ttl = true;
      } else if (opt == "EX") {
        ttl = now_ + (Int(args[++i]) * 1000);
      } else if (opt == "PX") {
        ttl = now_ + Int(args[++i]);
      } else if (opt == "EXAT") {
        ttl = std::max<int64_t>(Int(args[++i]) * 1000, 1);
      } else if (opt == "PXAT") {
        ttl = std::max<int64_t>(Int(args[++i]), 1);
      }
    }
    Value* current = Live(args[1]);
    if (get && current != nullptr && current->type != Type::kString) return WrongType();
    core::RespValue reply = Ok();
    if (get) {
      reply =
          current != nullptr ? core::RespValue::BulkString(current->str) : core::RespValue::Null();
    }
    if ((nx && current != nullptr) || (xx && current == nullptr)) {
      return get ? reply : core::RespValue::Null();
    }
    if (keep_ttl && current != nullptr) ttl = current->abs_ttl_ms;
    Store(args[1], StringOf(args[2], ttl));
    return reply;
  }

  core::RespValue SetWrite(bool add, std::span<const std::string> args) {
    Value* value = Live(args[1]);
    if (value != nullptr && value->type != Type::kSet) return WrongType();
    if (value == nullptr) {
      if (!add) return Int64(0);
      value = &data_[args[1]];
      *value = Value{.type = Type::kSet};
    }
    int64_t changed = 0;
    for (size_t i = 2; i < args.size(); ++i) {
      if (add) {
        changed += value->set.insert(args[i]).second ? 1 : 0;
      } else {
        changed += static_cast<int64_t>(value->set.erase(args[i]));
      }
    }
    DropIfEmpty(args[1]);
    return Int64(changed);
  }

  core::RespValue ZAdd(std::span<const std::string> args) {
    bool nx = false;
    bool xx = false;
    bool gt = false;
    bool lt = false;
    bool ch = false;
    size_t i = 2;
    for (; i < args.size(); ++i) {
      const std::string opt = core::AsciiUpper(args[i]);
      if (opt == "NX") {
        nx = true;
      } else if (opt == "XX") {
        xx = true;
      } else if (opt == "GT") {
        gt = true;
      } else if (opt == "LT") {
        lt = true;
      } else if (opt == "CH") {
        ch = true;
      } else {
        break;
      }
    }
    Value* value = Live(args[1]);
    if (value != nullptr && value->type != Type::kZset) return WrongType();
    std::map<std::string, double> scores;
    if (value != nullptr) scores = value->zset;
    int64_t added = 0;
    int64_t changed = 0;
    for (; i + 1 < args.size(); i += 2) {
      const double score = Score(args[i]);
      const std::string& member = args[i + 1];
      const auto it = scores.find(member);
      if (nx && it != scores.end()) continue;
      if (xx && it == scores.end()) continue;
      if (it != scores.end() && gt && !(score > it->second)) continue;
      if (it != scores.end() && lt && !(score < it->second)) continue;
      if (it == scores.end()) {
        ++added;
        scores.emplace(member, score);
      } else if (score != it->second) {
        // An equal score keeps the stored one, as Redis does.
        ++changed;
        it->second = score;
      }
    }
    if (added + changed > 0) {
      if (value == nullptr) {
        value = &data_[args[1]];
        *value = Value{.type = Type::kZset};
      }
      value->zset = std::move(scores);
    }
    return Int64(ch ? added + changed : added);
  }

  core::RespValue ZRem(std::span<const std::string> args) {
    Value* value = Live(args[1]);
    if (value == nullptr) return Int64(0);
    if (value->type != Type::kZset) return WrongType();
    int64_t removed = 0;
    for (size_t i = 2; i < args.size(); ++i) {
      removed += static_cast<int64_t>(value->zset.erase(args[i]));
    }
    DropIfEmpty(args[1]);
    return Int64(removed);
  }

  core::RespValue HSet(bool hmset, std::span<const std::string> args) {
    Value* value = Live(args[1]);
    if (value != nullptr && value->type != Type::kHash) return WrongType();
    if (value == nullptr) {
      value = &data_[args[1]];
      *value = Value{.type = Type::kHash};
    }
    int64_t added = 0;
    for (size_t i = 2; i + 1 < args.size(); i += 2) {
      added += value->hash.insert_or_assign(args[i], args[i + 1]).second ? 1 : 0;
    }
    return hmset ? Ok() : Int64(added);
  }

  core::RespValue HDel(std::span<const std::string> args) {
    Value* value = Live(args[1]);
    if (value == nullptr) return Int64(0);
    if (value->type != Type::kHash) return WrongType();
    int64_t removed = 0;
    for (size_t i = 2; i < args.size(); ++i) {
      removed += static_cast<int64_t>(value->hash.erase(args[i]));
    }
    DropIfEmpty(args[1]);
    return Int64(removed);
  }

  core::RespValue HSetNx(std::span<const std::string> args) {
    Value* value = Live(args[1]);
    if (value != nullptr && value->type != Type::kHash) return WrongType();
    if (value != nullptr && value->hash.contains(args[2])) return Int64(0);
    if (value == nullptr) {
      value = &data_[args[1]];
      *value = Value{.type = Type::kHash};
    }
    value->hash[args[2]] = args[3];
    return Int64(1);
  }

  core::RespValue Expire(const std::string& name, std::span<const std::string> args) {
    const bool seconds = name == "EXPIRE" || name == "EXPIREAT";
    const bool relative = name == "EXPIRE" || name == "PEXPIRE";
    int64_t at = Int(args[2]) * (seconds ? 1000 : 1);
    if (relative) at += now_;
    at = std::max<int64_t>(at, 1);
    bool nx = false;
    bool xx = false;
    bool gt = false;
    bool lt = false;
    for (size_t i = 3; i < args.size(); ++i) {
      const std::string opt = core::AsciiUpper(args[i]);
      nx = nx || opt == "NX";
      xx = xx || opt == "XX";
      gt = gt || opt == "GT";
      lt = lt || opt == "LT";
    }
    Value* value = Live(args[1]);
    if (value == nullptr) return Int64(0);
    const int64_t current = value->abs_ttl_ms;
    const bool has_ttl = current != 0;
    // No TTL counts as an infinite one for GT and LT.
    if ((nx && has_ttl) || (xx && !has_ttl) || (gt && (!has_ttl || at <= current)) ||
        (lt && has_ttl && at >= current)) {
      return Int64(0);
    }
    if (at <= now_) {
      data_.erase(args[1]);
      return Int64(1);
    }
    value->abs_ttl_ms = at;
    return Int64(1);
  }

  core::RespValue RenameNx(std::span<const std::string> args) {
    Value* src = Live(args[1]);
    Value* dst = Live(args[2]);
    if (src == nullptr) return core::RespValue::Error(core::ErrorPrefix::kErr, "no such key");
    if (dst != nullptr) return Int64(0);
    Value moved = *src;
    data_.erase(args[1]);
    data_[args[2]] = std::move(moved);
    return Int64(1);
  }

  core::RespValue Copy(std::span<const std::string> args) {
    bool replace = false;
    for (size_t i = 3; i < args.size(); ++i) {
      if (core::AsciiUpper(args[i]) == "REPLACE") replace = true;
    }
    if (args[1] == args[2]) {
      return core::RespValue::Error(core::ErrorPrefix::kErr,
                                    "source and destination objects are the same");
    }
    Value* src = Live(args[1]);
    Value* dst = Live(args[2]);
    if (src == nullptr || (dst != nullptr && !replace)) return Int64(0);
    Value copied = *src;
    data_[args[2]] = std::move(copied);
    return Int64(1);
  }

  core::RespValue TypedRead(const std::string& name, std::span<const std::string> args) {
    const Value* value = Live(args[1]);
    const auto want = [&name] {
      if (name == "GET") return Type::kString;
      if (name.starts_with('S')) return Type::kSet;
      if (name.starts_with('Z')) return Type::kZset;
      return Type::kHash;
    }();
    if (value != nullptr && value->type != want) return WrongType();
    const auto array = [](std::vector<core::RespValue> elements) {
      return core::RespValue::Array(std::move(elements));
    };
    if (name == "GET") {
      return value != nullptr ? core::RespValue::BulkString(value->str) : core::RespValue::Null();
    }
    if (name == "SMEMBERS") {
      std::vector<core::RespValue> out;
      if (value != nullptr) {
        for (const auto& m : value->set) out.push_back(core::RespValue::BulkString(m));
      }
      return array(std::move(out));
    }
    if (name == "SISMEMBER") return Int64(value != nullptr && value->set.contains(args[2]) ? 1 : 0);
    if (name == "SCARD") return Int64(value != nullptr ? std::ssize(value->set) : 0);
    if (name == "ZCARD") return Int64(value != nullptr ? std::ssize(value->zset) : 0);
    if (name == "ZSCORE") {
      if (value == nullptr) return core::RespValue::Null();
      const auto it = value->zset.find(args[2]);
      return it == value->zset.end()
                 ? core::RespValue::Null()
                 : core::RespValue::BulkString(core::FormatRespDouble(it->second));
    }
    if (name == "ZRANGE") return ZRange(value, args);
    if (name == "HGET") {
      if (value == nullptr) return core::RespValue::Null();
      const auto it = value->hash.find(args[2]);
      return it == value->hash.end() ? core::RespValue::Null()
                                     : core::RespValue::BulkString(it->second);
    }
    if (name == "HEXISTS") return Int64(value != nullptr && value->hash.contains(args[2]) ? 1 : 0);
    if (name == "HLEN") return Int64(value != nullptr ? std::ssize(value->hash) : 0);
    if (name == "HMGET") {
      std::vector<core::RespValue> out;
      for (size_t i = 2; i < args.size(); ++i) {
        if (value == nullptr || !value->hash.contains(args[i])) {
          out.push_back(core::RespValue::Null());
        } else {
          out.push_back(core::RespValue::BulkString(value->hash.at(args[i])));
        }
      }
      return array(std::move(out));
    }
    std::vector<core::RespValue> out;
    if (value != nullptr) {
      for (const auto& [field, v] : value->hash) {
        if (name != "HVALS") out.push_back(core::RespValue::BulkString(field));
        if (name != "HKEYS") out.push_back(core::RespValue::BulkString(v));
      }
    }
    return array(std::move(out));
  }

  static core::RespValue ZRange(const Value* value, std::span<const std::string> args) {
    std::vector<std::pair<double, std::string>> ordered;
    if (value != nullptr) {
      for (const auto& [member, score] : value->zset) ordered.emplace_back(score, member);
    }
    std::ranges::sort(ordered);
    const auto size = static_cast<int64_t>(ordered.size());
    int64_t start = Int(args[2]);
    int64_t stop = Int(args[3]);
    if (start < 0) start += size;
    if (stop < 0) stop += size;
    start = std::max<int64_t>(start, 0);
    stop = std::min(stop, size - 1);
    const bool with_scores = args.size() > 4 && core::AsciiUpper(args[4]) == "WITHSCORES";
    std::vector<core::RespValue> out;
    for (int64_t i = start; i <= stop; ++i) {
      const auto& [score, member] = ordered[static_cast<size_t>(i)];
      out.push_back(core::RespValue::BulkString(member));
      if (with_scores) out.push_back(core::RespValue::BulkString(core::FormatRespDouble(score)));
    }
    return core::RespValue::Array(std::move(out));
  }

  core::RespValue Read(const std::string& name, std::span<const std::string> args) {
    if (name == "EXISTS") {
      int64_t count = 0;
      for (size_t i = 1; i < args.size(); ++i) count += Live(args[i]) != nullptr ? 1 : 0;
      return Int64(count);
    }
    if (name == "MGET") {
      std::vector<core::RespValue> out;
      for (size_t i = 1; i < args.size(); ++i) {
        const Value* value = Live(args[i]);
        out.push_back(value != nullptr && value->type == Type::kString
                          ? core::RespValue::BulkString(value->str)
                          : core::RespValue::Null());
      }
      return core::RespValue::Array(std::move(out));
    }
    if (name == "TYPE") {
      const Value* value = Live(args[1]);
      static constexpr auto kNames =
          std::to_array<std::string_view>({"string", "set", "zset", "hash"});
      return core::RespValue::SimpleString(
          value == nullptr ? "none" : std::string(kNames.at(static_cast<size_t>(value->type))));
    }
    if (name == "TTL" || name == "PTTL") {
      const Value* value = Live(args[1]);
      if (value == nullptr) return Int64(-2);
      if (value->abs_ttl_ms == 0) return Int64(-1);
      const int64_t left = std::max<int64_t>(value->abs_ttl_ms - now_, 0);
      return Int64(name == "PTTL" ? left : (left + 500) / 1000);
    }
    return TypedRead(name, args);
  }

  CrossSlot cross_slot_;
  std::map<std::string, Value> data_;
  int64_t now_ = 0;
};

}  // namespace abyss::testing
