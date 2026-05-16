#pragma once

#include <cstdint>
#include <string_view>
#include <variant>
#include <vector>

#include "abyss/core/resp_types.h"
#include "abyss/core/result.h"

namespace abyss::core::ops {

// --- Reads ---

struct StringGet {
  std::string_view key;
};

struct SetIsMember {
  std::string_view key;
  std::string_view member;
};

struct SetMembers {
  std::string_view key;
};

struct SetCard {
  std::string_view key;
};

struct ZsetScore {
  std::string_view key;
  std::string_view member;
};

struct ZsetCard {
  std::string_view key;
};

struct ZsetRange {
  std::string_view key;
  std::string_view min;
  std::string_view max;
  bool by_score = false;
  bool by_lex = false;
  bool rev = false;
  int64_t offset = 0;
  int64_t count = -1;
  bool with_scores = false;
};

struct HashGet {
  std::string_view key;
  std::string_view field;
};

struct HashGetAll {
  std::string_view key;
};

struct HashMultiGet {
  std::string_view key;
  std::vector<std::string_view> fields;
};

struct HashFieldExists {
  std::string_view key;
  std::string_view field;
};

struct HashKeys {
  std::string_view key;
};

struct HashVals {
  std::string_view key;
};

struct HashLen {
  std::string_view key;
};

struct Exists {
  std::vector<std::string_view> keys;
};

struct MultiStringGet {
  std::vector<std::string_view> keys;
};

using ReadOp = std::variant<StringGet, SetIsMember, SetMembers, SetCard, ZsetScore, ZsetCard,
                            ZsetRange, HashGet, HashGetAll, HashMultiGet, HashFieldExists, HashKeys,
                            HashVals, HashLen, Exists, MultiStringGet>;

// --- Writes ---

struct StringSet {
  std::string_view key;
  std::string_view value;
  uint64_t abs_ttl_ms = 0;
};

struct Del {
  std::vector<std::string_view> keys;
};

struct SetAdd {
  std::string_view key;
  std::vector<std::string_view> members;
};

struct SetRem {
  std::string_view key;
  std::vector<std::string_view> members;
};

struct ZsetAdd {
  struct Entry {
    double score = 0.0;
    std::string_view member;
  };
  std::string_view key;
  std::vector<Entry> entries;
};

struct ZsetRem {
  std::string_view key;
  std::vector<std::string_view> members;
};

struct HashSet {
  struct FieldValue {
    std::string_view field;
    std::string_view value;
  };
  std::string_view key;
  std::vector<FieldValue> fields;
};

// Identical payload to HashSet; distinct variant carries the +OK reply
// semantic so the hot store's Apply produces SimpleString("OK") instead of
// the per-field new-count integer that HSET requires.
struct HashMSet {
  std::string_view key;
  std::vector<HashSet::FieldValue> fields;
};

struct HashDel {
  std::string_view key;
  std::vector<std::string_view> fields;
};

struct MultiStringSet {
  struct Entry {
    std::string_view key;
    std::string_view value;
  };
  std::vector<Entry> entries;
};

// abs_ttl_ms is computed at parse time from EXPIRE/PEXPIRE/EXPIREAT/PEXPIREAT
// so replay sees a single unambiguous timestamp.
struct Expire {
  std::string_view key;
  uint64_t abs_ttl_ms = 0;
};

// Distinct from Expire{abs_ttl_ms=0} so the WAL reader tells "clear TTL"
// apart from "expire immediately".
struct Persist {
  std::string_view key;
};

using WriteOp = std::variant<StringSet, Del, SetAdd, SetRem, ZsetAdd, ZsetRem, HashSet, HashMSet,
                             HashDel, MultiStringSet, Expire, Persist>;

Result<ReadOp> ParseReadOp(std::string_view name, const RespCommand& cmd);
Result<WriteOp> ParseWriteOp(std::string_view name, const RespCommand& cmd);

std::string_view PrimaryKey(const ReadOp& op);
std::string_view PrimaryKey(const WriteOp& op);

}  // namespace abyss::core::ops
