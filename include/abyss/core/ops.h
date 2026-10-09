#pragma once

#include <cstdint>
#include <string>
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

using ReadOp = std::variant<StringGet, SetIsMember, SetMembers, SetCard, ZsetScore, ZsetCard,
                            ZsetRange, HashGet, HashGetAll, HashMultiGet, HashFieldExists, HashKeys,
                            HashVals, HashLen, Exists>;

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
                             HashDel, Expire, Persist>;

Result<ReadOp> ParseReadOp(std::string_view name, const RespCommand& cmd);
// `wall_now_ms` is the reference for materialising relative TTLs (PX/EX/EXPIRE)
// to absolute. Pass the entry's `appended_at` so hot and cold agree across replay.
uint64_t WallNowMs();
Result<WriteOp> ParseWriteOp(std::string_view name, const RespCommand& cmd,
                             uint64_t wall_now_ms = WallNowMs());
// False means no tier in this build can materialise the command, which is a
// capability gap; a parser that exists and then rejects is malformed input.
// The two failures need opposite handling and ParseWriteOp alone cannot
// distinguish them -- both surface as an error.
bool HasWriteParser(std::string_view name);

// The one form of `op` that gets written to the WAL: aliases collapsed
// (SETEX/PSETEX -> SET), relative TTLs already absolute (EX/PX -> PXAT). Its
// contract is a round trip -- ParseWriteOp on the result reproduces `op` --
// which is what keeps replies and tier behaviour identical to the client's
// original spelling. HMSET therefore stays HMSET: it is a distinct WriteOp
// carrying a distinct reply, not a spelling of HSET.
RespCommand CanonicalCommand(const WriteOp& op);

// A score's canonical form: the shortest that parses back bit-for-bit.
std::string ScoreToString(double score);

std::string_view PrimaryKey(const ReadOp& op);
std::string_view PrimaryKey(const WriteOp& op);

}  // namespace abyss::core::ops
