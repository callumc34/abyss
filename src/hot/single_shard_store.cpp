#include "abyss/hot/single_shard_store.h"

#include <algorithm>
#include <charconv>
#include <chrono>
#include <limits>
#include <optional>
#include <ranges>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

#include "abyss/core/resp_format.h"

namespace abyss::hot {

namespace {

int64_t WallMs(const core::WallClockFn& clock) {
  return std::chrono::duration_cast<std::chrono::milliseconds>(clock().time_since_epoch()).count();
}

bool IsExpiredByTtl(const Entry& entry, const core::WallClockFn& clock) {
  if (entry.abs_ttl_ms == 0) return false;
  return WallMs(clock) >= entry.abs_ttl_ms;
}

// Total, non-throwing parse of a ZRANGEBYSCORE bound into a double. A score
// bound is a remote-supplied string read under a shared lock, so a throwing
// conversion (std::stod) could unwind through the reactor; std::from_chars
// never throws and surfaces malformed input as a clean error Result instead.
// `empty_default`/the -inf/+inf sentinels stand in for an unbounded edge, since
// from_chars(double) does not itself accept "inf". C8's ParseLexBound should
// follow this same from_chars/Result discipline for lex bounds.
core::Result<double> ParseScoreBound(std::string_view s, double empty_default) {
  if (s.empty()) return empty_default;
  if (s == "-inf") return -std::numeric_limits<double>::infinity();
  if (s == "+inf" || s == "inf") return std::numeric_limits<double>::infinity();
  double value = 0.0;
  const auto* begin = s.data();
  const auto* end = s.data() + s.size();
  const auto [ptr, ec] = std::from_chars(begin, end, value);
  if (ec != std::errc{} || ptr != end) {
    return std::unexpected(core::Error(core::ErrorCode::kInvalidArgument,
                                       "not a valid score: '" + std::string(s) + "'"));
  }
  return value;
}

// Total, non-throwing parse of a ZRANGE index bound (a signed integer) into an
// int64. Empty stands in for the supplied default edge index. Replaces a
// throwing std::stoll on remote input read under the shared lock.
core::Result<int64_t> ParseIndexBound(std::string_view s, int64_t empty_default) {
  if (s.empty()) return empty_default;
  int64_t value = 0;
  const auto* begin = s.data();
  const auto* end = s.data() + s.size();
  const auto [ptr, ec] = std::from_chars(begin, end, value);
  if (ec != std::errc{} || ptr != end) {
    return std::unexpected(core::Error(core::ErrorCode::kInvalidArgument,
                                       "not a valid index: '" + std::string(s) + "'"));
  }
  return value;
}

// A parsed ZRANGEBYLEX bound. Redis lex syntax: `[value` (inclusive), `(value`
// (exclusive), `-` (negative infinity), `+` (positive infinity). A bare value
// with no prefix is a syntax error. Total and non-throwing — it never inspects
// a numeric conversion, so it follows C11's from_chars/Result discipline by
// surfacing malformed input as a clean error rather than unwinding.
struct LexBound {
  std::string value;
  bool exclusive = false;
  bool neg_inf = false;
  bool pos_inf = false;
};

core::Result<LexBound> ParseLexBound(std::string_view s) {
  if (s == "-") return LexBound{.neg_inf = true};
  if (s == "+") return LexBound{.pos_inf = true};
  if (!s.empty() && s.front() == '[') {
    return LexBound{.value = std::string(s.substr(1)), .exclusive = false};
  }
  if (!s.empty() && s.front() == '(') {
    return LexBound{.value = std::string(s.substr(1)), .exclusive = true};
  }
  return std::unexpected(core::Error(core::ErrorCode::kInvalidArgument,
                                     "not a valid lex range bound: '" + std::string(s) + "'"));
}

// True iff `member` is at or above the lex lower bound `min`.
bool LexAtOrAboveMin(std::string_view member, const LexBound& min) {
  if (min.neg_inf) return true;
  if (min.pos_inf) return false;
  return min.exclusive ? member > min.value : member >= min.value;
}

// True iff `member` is at or below the lex upper bound `max`.
bool LexAtOrBelowMax(std::string_view member, const LexBound& max) {
  if (max.pos_inf) return true;
  if (max.neg_inf) return false;
  return max.exclusive ? member < max.value : member <= max.value;
}

// The single key a read op targets, or nullopt for the multi-key Exists probe
// (which resolves presence per key rather than producing one shaped reply).
std::optional<std::string_view> SingleKeyOf(const core::ops::ReadOp& op) {
  return std::visit(
      [](const auto& o) -> std::optional<std::string_view> {
        using T = std::decay_t<decltype(o)>;
        if constexpr (std::is_same_v<T, core::ops::Exists>) {
          return std::nullopt;
        } else {
          return o.key;
        }
      },
      op);
}

// The empty/nil reply a read op yields for a missing key, shaped per op — as a
// success, so a tombstone hit answers authoritatively rather than falling through.
core::RespValue EmptyReadResponse(const core::ops::ReadOp& op) {
  return std::visit(
      [](const auto& o) -> core::RespValue {
        using T = std::decay_t<decltype(o)>;
        if constexpr (std::is_same_v<T, core::ops::StringGet> ||
                      std::is_same_v<T, core::ops::ZsetScore> ||
                      std::is_same_v<T, core::ops::HashGet>) {
          return core::RespValue::Null();
        } else if constexpr (std::is_same_v<T, core::ops::SetIsMember> ||
                             std::is_same_v<T, core::ops::SetCard> ||
                             std::is_same_v<T, core::ops::ZsetCard> ||
                             std::is_same_v<T, core::ops::HashFieldExists> ||
                             std::is_same_v<T, core::ops::HashLen>) {
          return core::RespValue::Integer(0);
        } else if constexpr (std::is_same_v<T, core::ops::HashMultiGet>) {
          return core::RespValue::Array(
              std::vector<core::RespValue>(o.fields.size(), core::RespValue::Null()));
        } else {
          // SetMembers, ZsetRange, HashGetAll, HashKeys, HashVals.
          return core::RespValue::Array({});
        }
      },
      op);
}

}  // namespace

size_t Entry::ApproximateBytes() const {
  size_t bytes = sizeof(Entry);
  std::visit(
      [&bytes](const auto& v) {
        using T = std::decay_t<decltype(v)>;
        if constexpr (std::is_same_v<T, std::string>) {
          bytes += v.capacity();
        } else if constexpr (std::is_same_v<T, SetValue>) {
          for (const auto& m : v.members) {
            bytes += sizeof(m) + m.capacity();
          }
        } else if constexpr (std::is_same_v<T, HashValue>) {
          for (const auto& [k, val] : v.fields) {
            bytes += sizeof(k) + k.capacity() + sizeof(val) + val.capacity();
          }
        } else if constexpr (std::is_same_v<T, ZsetValue>) {
          for (const auto& [m, s] : v.member_scores) {
            bytes += sizeof(m) + m.capacity() + sizeof(s);
          }
          // The score-ordered index duplicates every member string and adds
          // map/set node overhead. Counting only member_scores understated a
          // zset by ~half its real heap; include score_members so used_bytes_
          // is the single, correct hot-memory oracle.
          for (const auto& [score, members] : v.score_members) {
            bytes += sizeof(score) + sizeof(members);
            for (const auto& m : members) {
              bytes += sizeof(m) + m.capacity();
            }
          }
        }
      },
      value);
  return bytes;
}

SingleShardStore::SingleShardStore(SingleShardConfig config)
    : config_(std::move(config)), governor_(config_.max_memory_bytes) {}

// --- Read operations (const) ---

core::Result<core::RespValue> SingleShardStore::Exec(const core::ops::ReadOp& op) const {
  // A tombstone is authoritative: the key was deleted.
  if (const auto key = SingleKeyOf(op);
      key.has_value() && Probe(*key) == core::HotKeyPresence::kTombstoned) {
    return EmptyReadResponse(op);
  }
  return std::visit(
      [this](const auto& o) -> core::Result<core::RespValue> {
        using T = std::decay_t<decltype(o)>;
        if constexpr (std::is_same_v<T, core::ops::StringGet>) {
          return ExecStringGet(o);
        } else if constexpr (std::is_same_v<T, core::ops::SetIsMember>) {
          return ExecSetIsMember(o);
        } else if constexpr (std::is_same_v<T, core::ops::SetMembers>) {
          return ExecSetMembers(o);
        } else if constexpr (std::is_same_v<T, core::ops::SetCard>) {
          return ExecSetCard(o);
        } else if constexpr (std::is_same_v<T, core::ops::ZsetScore>) {
          return ExecZsetScore(o);
        } else if constexpr (std::is_same_v<T, core::ops::ZsetCard>) {
          return ExecZsetCard(o);
        } else if constexpr (std::is_same_v<T, core::ops::ZsetRange>) {
          return ExecZsetRange(o);
        } else if constexpr (std::is_same_v<T, core::ops::HashGet>) {
          return ExecHashGet(o);
        } else if constexpr (std::is_same_v<T, core::ops::HashGetAll>) {
          return ExecHashGetAll(o);
        } else if constexpr (std::is_same_v<T, core::ops::HashMultiGet>) {
          return ExecHashMultiGet(o);
        } else if constexpr (std::is_same_v<T, core::ops::HashFieldExists>) {
          return ExecHashFieldExists(o);
        } else if constexpr (std::is_same_v<T, core::ops::HashKeys>) {
          return ExecHashKeys(o);
        } else if constexpr (std::is_same_v<T, core::ops::HashVals>) {
          return ExecHashVals(o);
        } else if constexpr (std::is_same_v<T, core::ops::HashLen>) {
          return ExecHashLen(o);
        } else if constexpr (std::is_same_v<T, core::ops::Exists>) {
          return ExecExists(o);
        } else {
          return std::unexpected(
              core::Error(core::ErrorCode::kInternal, "unsupported read operation"));
        }
      },
      op);
}

core::Result<core::RespValue> SingleShardStore::ExecStringGet(
    const core::ops::StringGet& op) const {
  auto result = FindTypedEntry(op.key, Entry::Type::kString);
  if (!result.has_value()) return std::unexpected(result.error());
  if (*result == nullptr) {
    return std::unexpected(core::Error(core::ErrorCode::kNotFound, ""));
  }
  return core::RespValue::BulkString(std::get<std::string>((*result)->value));
}

core::Result<core::RespValue> SingleShardStore::ExecSetIsMember(
    const core::ops::SetIsMember& op) const {
  auto result = FindTypedEntry(op.key, Entry::Type::kSet);
  if (!result.has_value()) return std::unexpected(result.error());
  if (*result == nullptr) {
    return std::unexpected(core::Error(core::ErrorCode::kNotFound, ""));
  }
  const auto& members = std::get<SetValue>((*result)->value).members;
  return core::RespValue::Integer(members.contains(std::string(op.member)) ? 1 : 0);
}

core::Result<core::RespValue> SingleShardStore::ExecSetMembers(
    const core::ops::SetMembers& op) const {
  auto result = FindTypedEntry(op.key, Entry::Type::kSet);
  if (!result.has_value()) return std::unexpected(result.error());
  if (*result == nullptr) {
    return std::unexpected(core::Error(core::ErrorCode::kNotFound, ""));
  }
  const auto& members = std::get<SetValue>((*result)->value).members;
  std::vector<core::RespValue> elements;
  elements.reserve(members.size());
  for (const auto& m : members) {
    elements.push_back(core::RespValue::BulkString(m));
  }
  return core::RespValue::Array(std::move(elements));
}

core::Result<core::RespValue> SingleShardStore::ExecSetCard(const core::ops::SetCard& op) const {
  auto result = FindTypedEntry(op.key, Entry::Type::kSet);
  if (!result.has_value()) return std::unexpected(result.error());
  if (*result == nullptr) {
    return std::unexpected(core::Error(core::ErrorCode::kNotFound, ""));
  }
  const auto& members = std::get<SetValue>((*result)->value).members;
  return core::RespValue::Integer(static_cast<int64_t>(members.size()));
}

core::Result<core::RespValue> SingleShardStore::ExecZsetScore(
    const core::ops::ZsetScore& op) const {
  auto result = FindTypedEntry(op.key, Entry::Type::kZset);
  if (!result.has_value()) return std::unexpected(result.error());
  if (*result == nullptr) {
    return std::unexpected(core::Error(core::ErrorCode::kNotFound, ""));
  }
  const auto& zset = std::get<ZsetValue>((*result)->value);
  auto it = zset.member_scores.find(std::string(op.member));
  if (it == zset.member_scores.end()) {
    return core::RespValue::Null();
  }
  return core::RespValue::BulkString(core::FormatRespDouble(it->second));
}

core::Result<core::RespValue> SingleShardStore::ExecZsetCard(const core::ops::ZsetCard& op) const {
  auto result = FindTypedEntry(op.key, Entry::Type::kZset);
  if (!result.has_value()) return std::unexpected(result.error());
  if (*result == nullptr) {
    return std::unexpected(core::Error(core::ErrorCode::kNotFound, ""));
  }
  const auto& zset = std::get<ZsetValue>((*result)->value);
  return core::RespValue::Integer(static_cast<int64_t>(zset.member_scores.size()));
}

core::Result<core::RespValue> SingleShardStore::ExecZsetRange(
    const core::ops::ZsetRange& op) const {
  auto result = FindTypedEntry(op.key, Entry::Type::kZset);
  if (!result.has_value()) return std::unexpected(result.error());
  if (*result == nullptr) {
    return std::unexpected(core::Error(core::ErrorCode::kNotFound, ""));
  }
  const auto& zset = std::get<ZsetValue>((*result)->value);
  std::vector<core::RespValue> elements;

  if (op.by_lex) {
    auto min_parsed = ParseLexBound(op.min);
    if (!min_parsed.has_value()) return std::unexpected(min_parsed.error());
    auto max_parsed = ParseLexBound(op.max);
    if (!max_parsed.has_value()) return std::unexpected(max_parsed.error());

    // Lex range is over member names in pure lexicographic order (Redis assumes
    // equal scores). member_scores keys are the members; collect and sort so
    // the order matches cold's member-indexed CF scan byte-for-byte.
    std::vector<std::string_view> members;
    members.reserve(zset.member_scores.size());
    for (const auto& [member, score] : zset.member_scores) {
      if (LexAtOrAboveMin(member, *min_parsed) && LexAtOrBelowMax(member, *max_parsed)) {
        members.emplace_back(member);
      }
    }
    std::ranges::sort(members);
    if (op.rev) std::ranges::reverse(members);

    int64_t start = std::max<int64_t>(op.offset, 0);
    int64_t count = op.count < 0 ? static_cast<int64_t>(members.size()) : op.count;
    if (std::cmp_less(start, members.size())) {
      auto end = std::min(start + count, static_cast<int64_t>(members.size()));
      for (int64_t i = start; i < end; ++i) {
        elements.push_back(
            core::RespValue::BulkString(std::string(members[static_cast<size_t>(i)])));
      }
    }
  } else if (op.by_score) {
    auto min_parsed = ParseScoreBound(op.min, -std::numeric_limits<double>::infinity());
    if (!min_parsed.has_value()) return std::unexpected(min_parsed.error());
    auto max_parsed = ParseScoreBound(op.max, std::numeric_limits<double>::infinity());
    if (!max_parsed.has_value()) return std::unexpected(max_parsed.error());
    double min_score = *min_parsed;
    double max_score = *max_parsed;

    if (op.rev) std::swap(min_score, max_score);

    auto lo = zset.score_members.lower_bound(std::min(min_score, max_score));
    auto hi = zset.score_members.upper_bound(std::max(min_score, max_score));

    std::vector<std::pair<std::string, double>> collected;
    for (auto it = lo; it != hi; ++it) {
      for (const auto& member : it->second) {
        collected.emplace_back(member, it->first);
      }
    }

    if (op.rev) {
      std::ranges::reverse(collected);
    }

    int64_t start = op.offset;
    int64_t count = op.count < 0 ? static_cast<int64_t>(collected.size()) : op.count;
    if (std::cmp_less(start, collected.size())) {
      auto end = std::min(start + count, static_cast<int64_t>(collected.size()));
      for (int64_t i = start; i < end; ++i) {
        elements.push_back(core::RespValue::BulkString(collected[static_cast<size_t>(i)].first));
        if (op.with_scores) {
          elements.push_back(core::RespValue::BulkString(
              core::FormatRespDouble(collected[static_cast<size_t>(i)].second)));
        }
      }
    }
  } else {
    // Index-based range — convert to score iteration
    const auto& sm = zset.score_members;
    std::vector<std::pair<std::string, double>> all;
    for (const auto& [score, members] : sm) {
      for (const auto& member : members) {
        all.emplace_back(member, score);
      }
    }
    if (op.rev) {
      std::ranges::reverse(all);
    }

    auto min_parsed = ParseIndexBound(op.min, 0);
    if (!min_parsed.has_value()) return std::unexpected(min_parsed.error());
    auto max_parsed = ParseIndexBound(op.max, static_cast<int64_t>(all.size()) - 1);
    if (!max_parsed.has_value()) return std::unexpected(max_parsed.error());
    int64_t min_idx = *min_parsed;
    int64_t max_idx = *max_parsed;

    if (min_idx < 0) min_idx += static_cast<int64_t>(all.size());
    if (max_idx < 0) max_idx += static_cast<int64_t>(all.size());
    min_idx = std::max(min_idx, int64_t{0});
    max_idx = std::min(max_idx, static_cast<int64_t>(all.size()) - 1);

    // NOLINTNEXTLINE(bugprone-infinite-loop)
    for (int64_t i = min_idx; i <= max_idx; ++i) {
      elements.push_back(core::RespValue::BulkString(all[static_cast<size_t>(i)].first));
      if (op.with_scores) {
        elements.push_back(core::RespValue::BulkString(
            core::FormatRespDouble(all[static_cast<size_t>(i)].second)));
      }
    }
  }

  return core::RespValue::Array(std::move(elements));
}

core::Result<core::RespValue> SingleShardStore::ExecHashGet(const core::ops::HashGet& op) const {
  auto result = FindTypedEntry(op.key, Entry::Type::kHash);
  if (!result.has_value()) return std::unexpected(result.error());
  if (*result == nullptr) {
    return std::unexpected(core::Error(core::ErrorCode::kNotFound, ""));
  }
  const auto& fields = std::get<HashValue>((*result)->value).fields;
  auto it = fields.find(std::string(op.field));
  if (it == fields.end()) {
    return core::RespValue::Null();
  }
  return core::RespValue::BulkString(it->second);
}

core::Result<core::RespValue> SingleShardStore::ExecHashGetAll(
    const core::ops::HashGetAll& op) const {
  auto result = FindTypedEntry(op.key, Entry::Type::kHash);
  if (!result.has_value()) return std::unexpected(result.error());
  if (*result == nullptr) {
    return std::unexpected(core::Error(core::ErrorCode::kNotFound, ""));
  }
  const auto& fields = std::get<HashValue>((*result)->value).fields;
  std::vector<core::RespValue> elements;
  elements.reserve(fields.size() * 2);
  for (const auto& [k, v] : fields) {
    elements.push_back(core::RespValue::BulkString(k));
    elements.push_back(core::RespValue::BulkString(v));
  }
  return core::RespValue::Array(std::move(elements));
}

core::Result<core::RespValue> SingleShardStore::ExecHashMultiGet(
    const core::ops::HashMultiGet& op) const {
  auto result = FindTypedEntry(op.key, Entry::Type::kHash);
  if (!result.has_value()) return std::unexpected(result.error());
  if (*result == nullptr) {
    return std::unexpected(core::Error(core::ErrorCode::kNotFound, ""));
  }
  const auto& fields = std::get<HashValue>((*result)->value).fields;
  std::vector<core::RespValue> elements;
  elements.reserve(op.fields.size());
  for (auto field : op.fields) {
    auto it = fields.find(std::string(field));
    if (it == fields.end()) {
      elements.push_back(core::RespValue::Null());
    } else {
      elements.push_back(core::RespValue::BulkString(it->second));
    }
  }
  return core::RespValue::Array(std::move(elements));
}

core::Result<core::RespValue> SingleShardStore::ExecHashFieldExists(
    const core::ops::HashFieldExists& op) const {
  auto result = FindTypedEntry(op.key, Entry::Type::kHash);
  if (!result.has_value()) return std::unexpected(result.error());
  if (*result == nullptr) {
    return std::unexpected(core::Error(core::ErrorCode::kNotFound, ""));
  }
  const auto& fields = std::get<HashValue>((*result)->value).fields;
  return core::RespValue::Integer(fields.contains(std::string(op.field)) ? 1 : 0);
}

core::Result<core::RespValue> SingleShardStore::ExecHashKeys(const core::ops::HashKeys& op) const {
  auto result = FindTypedEntry(op.key, Entry::Type::kHash);
  if (!result.has_value()) return std::unexpected(result.error());
  if (*result == nullptr) {
    return std::unexpected(core::Error(core::ErrorCode::kNotFound, ""));
  }
  const auto& fields = std::get<HashValue>((*result)->value).fields;
  std::vector<core::RespValue> elements;
  elements.reserve(fields.size());
  for (const auto& [k, _] : fields) {
    elements.push_back(core::RespValue::BulkString(k));
  }
  return core::RespValue::Array(std::move(elements));
}

core::Result<core::RespValue> SingleShardStore::ExecHashVals(const core::ops::HashVals& op) const {
  auto result = FindTypedEntry(op.key, Entry::Type::kHash);
  if (!result.has_value()) return std::unexpected(result.error());
  if (*result == nullptr) {
    return std::unexpected(core::Error(core::ErrorCode::kNotFound, ""));
  }
  const auto& fields = std::get<HashValue>((*result)->value).fields;
  std::vector<core::RespValue> elements;
  elements.reserve(fields.size());
  for (const auto& [_, v] : fields) {
    elements.push_back(core::RespValue::BulkString(v));
  }
  return core::RespValue::Array(std::move(elements));
}

core::Result<core::RespValue> SingleShardStore::ExecHashLen(const core::ops::HashLen& op) const {
  auto result = FindTypedEntry(op.key, Entry::Type::kHash);
  if (!result.has_value()) return std::unexpected(result.error());
  if (*result == nullptr) {
    return std::unexpected(core::Error(core::ErrorCode::kNotFound, ""));
  }
  const auto& fields = std::get<HashValue>((*result)->value).fields;
  return core::RespValue::Integer(static_cast<int64_t>(fields.size()));
}

core::Result<core::RespValue> SingleShardStore::ExecExists(const core::ops::Exists& op) const {
  int64_t count = 0;
  for (auto key : op.keys) {
    if (FindLiveEntry(key) != nullptr) ++count;
  }
  return core::RespValue::Integer(count);
}

// --- Write operations ---

core::Result<core::RespValue> SingleShardStore::Apply(const core::ops::WriteOp& op,
                                                      core::EvictionTTL eviction,
                                                      core::SequenceId seq) {
  // Only create/replace ops grow memory; delete/expire ops only shrink it, so
  // the budget post-check runs solely on growth ops.
  const bool grows = std::holds_alternative<core::ops::StringSet>(op) ||
                     std::holds_alternative<core::ops::SetAdd>(op) ||
                     std::holds_alternative<core::ops::ZsetAdd>(op) ||
                     std::holds_alternative<core::ops::HashSet>(op) ||
                     std::holds_alternative<core::ops::HashMSet>(op);

  auto result = std::visit(
      [this, eviction, seq](const auto& o) -> core::Result<core::RespValue> {
        using T = std::decay_t<decltype(o)>;
        if constexpr (std::is_same_v<T, core::ops::StringSet>) {
          return ApplyStringSet(o, eviction);
        } else if constexpr (std::is_same_v<T, core::ops::Del>) {
          return ApplyDel(o, seq);
        } else if constexpr (std::is_same_v<T, core::ops::SetAdd>) {
          return ApplySetAdd(o, eviction);
        } else if constexpr (std::is_same_v<T, core::ops::SetRem>) {
          return ApplySetRem(o, seq);
        } else if constexpr (std::is_same_v<T, core::ops::ZsetAdd>) {
          return ApplyZsetAdd(o, eviction);
        } else if constexpr (std::is_same_v<T, core::ops::ZsetRem>) {
          return ApplyZsetRem(o, seq);
        } else if constexpr (std::is_same_v<T, core::ops::HashSet>) {
          return ApplyHashSet(o, eviction);
        } else if constexpr (std::is_same_v<T, core::ops::HashMSet>) {
          return ApplyHashMSet(o, eviction);
        } else if constexpr (std::is_same_v<T, core::ops::HashDel>) {
          return ApplyHashDel(o, seq);
        } else if constexpr (std::is_same_v<T, core::ops::Expire>) {
          return ApplyExpire(o);
        } else if constexpr (std::is_same_v<T, core::ops::Persist>) {
          return ApplyPersist(o);
        } else {
          return std::unexpected(
              core::Error(core::ErrorCode::kInternal, "unsupported write operation"));
        }
      },
      op);

  // The write has already applied (and is durable in the queue). Make room by
  // evicting OTHER LRU victims down to the budget, protecting the just-written
  // key. If even then the entry cannot fit (a single value larger than the
  // whole budget, no other victims), surface kResourceExhausted as an admission
  // signal — the entry is NOT lost, it stays durable in the queue/cold
  // (invariant 2). Suppressed during replay.
  if (grows && result.has_value() && !EnsureCapacityFor(core::ops::PrimaryKey(op))) {
    return std::unexpected(
        core::Error(core::ErrorCode::kResourceExhausted, "hot store memory budget exhausted"));
  }
  return result;
}

core::Result<void> SingleShardStore::ApplyBatch(std::span<const core::ops::WriteOp> ops,
                                                core::EvictionTTL eviction, core::SequenceId seq) {
  for (const auto& op : ops) {
    auto result = Apply(op, eviction, seq);
    if (!result.has_value()) return std::unexpected(result.error());
  }
  return {};
}

core::Result<core::RespValue> SingleShardStore::ApplyStringSet(const core::ops::StringSet& op,
                                                               core::EvictionTTL eviction) {
  auto it = entries_.find(std::string(op.key));
  // A tombstone is treated as absent: fall to the create path so the key is
  // resurrected as a live string (with the key_count increment that implies).
  if (it != entries_.end() && !it->second.tombstoned) {
    if (it->second.type != Entry::Type::kString) {
      TrackRemove(it->second, op.key);
      it->second.type = Entry::Type::kString;
      it->second.value = std::string(op.value);
      it->second.eviction = eviction;
      it->second.eviction_deadline = config_.steady_clock() + eviction;
      it->second.last_access = config_.steady_clock();
      it->second.abs_ttl_ms = static_cast<int64_t>(op.abs_ttl_ms);
      TrackInsert(it->second, op.key);
      return core::RespValue::SimpleString("OK");
    }
    TrackRemove(it->second, op.key);
    std::get<std::string>(it->second.value) = std::string(op.value);
    it->second.eviction = eviction;
    it->second.eviction_deadline = config_.steady_clock() + eviction;
    it->second.last_access = config_.steady_clock();
    it->second.abs_ttl_ms = static_cast<int64_t>(op.abs_ttl_ms);
    TrackInsert(it->second, op.key);
    return core::RespValue::SimpleString("OK");
  }

  // GetOrCreateEntry now tracks an empty-entry baseline; remove it before
  // re-adding the populated footprint so the create path stays balanced.
  auto& entry = GetOrCreateEntry(op.key, Entry::Type::kString, eviction);
  TrackRemove(entry, op.key);
  entry.value = std::string(op.value);
  entry.abs_ttl_ms = static_cast<int64_t>(op.abs_ttl_ms);
  TrackInsert(entry, op.key);
  return core::RespValue::SimpleString("OK");
}

core::Result<core::RespValue> SingleShardStore::ApplyDel(const core::ops::Del& op,
                                                         core::SequenceId seq) {
  int64_t removed = 0;
  for (auto key : op.keys) {
    auto it = entries_.find(std::string(key));
    if (it == entries_.end() || it->second.tombstoned) continue;
    if (IsExpiredByTtl(it->second, config_.wall_clock)) {
      // Already TTL-dead: cold applies the same expiry deterministically, so no
      // tombstone is needed and the key does not count as removed (Redis parity).
      RemoveEntry(std::string(key));
      continue;
    }
    TombstoneEntry(it->second, key, seq);
    ++removed;
  }
  return core::RespValue::Integer(removed);
}

core::Result<core::RespValue> SingleShardStore::ApplySetAdd(const core::ops::SetAdd& op,
                                                            core::EvictionTTL eviction) {
  auto it = entries_.find(std::string(op.key));
  if (it != entries_.end() && !it->second.tombstoned &&
      !IsExpiredByTtl(it->second, config_.wall_clock) && it->second.type != Entry::Type::kSet) {
    return std::unexpected(core::Error(core::ErrorCode::kWrongType,
                                       "Operation against a key holding the wrong kind of value"));
  }
  if (it != entries_.end() && !it->second.tombstoned &&
      IsExpiredByTtl(it->second, config_.wall_clock)) {
    RemoveEntry(std::string(op.key));
  }

  // GetOrCreateEntry establishes a tracked empty-entry baseline on create or
  // tombstone-resurrect, so this leading TrackRemove always has a matching
  // prior TrackInsert (HOT-2). The pattern is then balanced for every case:
  // remove the old (empty or populated) footprint, mutate, re-add the new one.
  auto& entry = GetOrCreateEntry(op.key, Entry::Type::kSet, eviction);
  TrackRemove(entry, op.key);
  auto& members = std::get<SetValue>(entry.value).members;
  int64_t added = 0;
  for (auto member : op.members) {
    if (members.insert(std::string(member)).second) ++added;
  }
  entry.eviction = eviction;
  entry.eviction_deadline = config_.steady_clock() + eviction;
  entry.last_access = config_.steady_clock();
  TrackInsert(entry, op.key);
  return core::RespValue::Integer(added);
}

core::Result<core::RespValue> SingleShardStore::ApplySetRem(const core::ops::SetRem& op,
                                                            core::SequenceId seq) {
  auto it = entries_.find(std::string(op.key));
  if (it == entries_.end() || it->second.tombstoned ||
      IsExpiredByTtl(it->second, config_.wall_clock)) {
    return core::RespValue::Integer(0);
  }
  if (it->second.type != Entry::Type::kSet) {
    return std::unexpected(core::Error(core::ErrorCode::kWrongType,
                                       "Operation against a key holding the wrong kind of value"));
  }
  TrackRemove(it->second, op.key);
  auto& members = std::get<SetValue>(it->second.value).members;
  int64_t removed = 0;
  for (auto member : op.members) {
    removed += static_cast<int64_t>(members.erase(std::string(member)));
  }
  TrackInsert(it->second, op.key);
  // Emptying a collection deletes the key (Redis semantics); leave a tombstone
  // so the delete is authoritative against a lagging overlay.
  if (members.empty()) {
    TombstoneEntry(it->second, op.key, seq);
  }
  return core::RespValue::Integer(removed);
}

core::Result<core::RespValue> SingleShardStore::ApplyZsetAdd(const core::ops::ZsetAdd& op,
                                                             core::EvictionTTL eviction) {
  auto it = entries_.find(std::string(op.key));
  if (it != entries_.end() && !it->second.tombstoned &&
      !IsExpiredByTtl(it->second, config_.wall_clock) && it->second.type != Entry::Type::kZset) {
    return std::unexpected(core::Error(core::ErrorCode::kWrongType,
                                       "Operation against a key holding the wrong kind of value"));
  }
  if (it != entries_.end() && !it->second.tombstoned &&
      IsExpiredByTtl(it->second, config_.wall_clock)) {
    RemoveEntry(std::string(op.key));
  }

  auto& entry = GetOrCreateEntry(op.key, Entry::Type::kZset, eviction);
  TrackRemove(entry, op.key);
  auto& zset = std::get<ZsetValue>(entry.value);

  int64_t added = 0;
  for (const auto& e : op.entries) {
    std::string member(e.member);
    auto existing = zset.member_scores.find(member);
    if (existing != zset.member_scores.end()) {
      double old_score = existing->second;
      zset.score_members[old_score].erase(member);
      if (zset.score_members[old_score].empty()) {
        zset.score_members.erase(old_score);
      }
    } else {
      ++added;
    }
    zset.member_scores[member] = e.score;
    zset.score_members[e.score].insert(std::move(member));
  }

  entry.eviction = eviction;
  entry.eviction_deadline = config_.steady_clock() + eviction;
  entry.last_access = config_.steady_clock();
  TrackInsert(entry, op.key);
  return core::RespValue::Integer(added);
}

core::Result<core::RespValue> SingleShardStore::ApplyZsetRem(const core::ops::ZsetRem& op,
                                                             core::SequenceId seq) {
  auto it = entries_.find(std::string(op.key));
  if (it == entries_.end() || it->second.tombstoned ||
      IsExpiredByTtl(it->second, config_.wall_clock)) {
    return core::RespValue::Integer(0);
  }
  if (it->second.type != Entry::Type::kZset) {
    return std::unexpected(core::Error(core::ErrorCode::kWrongType,
                                       "Operation against a key holding the wrong kind of value"));
  }
  TrackRemove(it->second, op.key);
  auto& zset = std::get<ZsetValue>(it->second.value);

  int64_t removed = 0;
  for (auto member : op.members) {
    std::string m(member);
    auto score_it = zset.member_scores.find(m);
    if (score_it != zset.member_scores.end()) {
      double score = score_it->second;
      zset.score_members[score].erase(m);
      if (zset.score_members[score].empty()) {
        zset.score_members.erase(score);
      }
      zset.member_scores.erase(score_it);
      ++removed;
    }
  }

  TrackInsert(it->second, op.key);
  if (zset.member_scores.empty()) {
    TombstoneEntry(it->second, op.key, seq);
  }
  return core::RespValue::Integer(removed);
}

core::Result<core::RespValue> SingleShardStore::ApplyHashSet(const core::ops::HashSet& op,
                                                             core::EvictionTTL eviction) {
  auto it = entries_.find(std::string(op.key));
  if (it != entries_.end() && !it->second.tombstoned &&
      !IsExpiredByTtl(it->second, config_.wall_clock) && it->second.type != Entry::Type::kHash) {
    return std::unexpected(core::Error(core::ErrorCode::kWrongType,
                                       "Operation against a key holding the wrong kind of value"));
  }
  if (it != entries_.end() && !it->second.tombstoned &&
      IsExpiredByTtl(it->second, config_.wall_clock)) {
    RemoveEntry(std::string(op.key));
  }

  auto& entry = GetOrCreateEntry(op.key, Entry::Type::kHash, eviction);
  TrackRemove(entry, op.key);
  auto& fields = std::get<HashValue>(entry.value).fields;
  int64_t new_fields = 0;
  for (const auto& fv : op.fields) {
    auto [field_it, inserted] = fields.try_emplace(std::string(fv.field), std::string(fv.value));
    if (inserted) {
      ++new_fields;
    } else {
      field_it->second = std::string(fv.value);
    }
  }
  entry.eviction = eviction;
  entry.eviction_deadline = config_.steady_clock() + eviction;
  entry.last_access = config_.steady_clock();
  TrackInsert(entry, op.key);
  return core::RespValue::Integer(new_fields);
}

core::Result<core::RespValue> SingleShardStore::ApplyHashMSet(const core::ops::HashMSet& op,
                                                              core::EvictionTTL eviction) {
  // HMSET shares HSET's apply path; the only difference is the reply (+OK
  // vs new-field count). Constructing a transient HashSet preserves type
  // safety for any future hot-store apply changes.
  const core::ops::HashSet hset{.key = op.key, .fields = op.fields};
  auto r = ApplyHashSet(hset, eviction);
  if (!r.has_value()) return r;
  return core::RespValue::SimpleString("OK");
}

core::Result<core::RespValue> SingleShardStore::ApplyHashDel(const core::ops::HashDel& op,
                                                             core::SequenceId seq) {
  auto it = entries_.find(std::string(op.key));
  if (it == entries_.end() || it->second.tombstoned ||
      IsExpiredByTtl(it->second, config_.wall_clock)) {
    return core::RespValue::Integer(0);
  }
  if (it->second.type != Entry::Type::kHash) {
    return std::unexpected(core::Error(core::ErrorCode::kWrongType,
                                       "Operation against a key holding the wrong kind of value"));
  }
  TrackRemove(it->second, op.key);
  auto& fields = std::get<HashValue>(it->second.value).fields;
  int64_t removed = 0;
  for (auto field : op.fields) {
    removed += static_cast<int64_t>(fields.erase(std::string(field)));
  }
  TrackInsert(it->second, op.key);
  if (fields.empty()) {
    TombstoneEntry(it->second, op.key, seq);
  }
  return core::RespValue::Integer(removed);
}

core::Result<core::RespValue> SingleShardStore::ApplyExpire(const core::ops::Expire& op) {
  auto it = entries_.find(std::string(op.key));
  if (it == entries_.end()) return core::RespValue::Integer(0);
  // A tombstone is logically absent (mirrors FindLiveEntry/Probe). EXPIRE on a
  // deleted-but-not-yet-GC'd key returns 0 and must not resurrect or mutate the
  // tombstone — the tombstone is reclaimed by GcTombstones, not by a TTL op.
  if (it->second.tombstoned) return core::RespValue::Integer(0);
  if (IsExpiredByTtl(it->second, config_.wall_clock)) {
    RemoveEntry(std::string(op.key));
    return core::RespValue::Integer(0);
  }
  TrackRemove(it->second, op.key);
  it->second.abs_ttl_ms = static_cast<int64_t>(op.abs_ttl_ms);
  TrackInsert(it->second, op.key);
  return core::RespValue::Integer(1);
}

core::Result<core::RespValue> SingleShardStore::ApplyPersist(const core::ops::Persist& op) {
  auto it = entries_.find(std::string(op.key));
  if (it == entries_.end()) return core::RespValue::Integer(0);
  // A tombstone is logically absent: PERSIST returns 0 and leaves it untouched.
  if (it->second.tombstoned) return core::RespValue::Integer(0);
  if (IsExpiredByTtl(it->second, config_.wall_clock)) {
    RemoveEntry(std::string(op.key));
    return core::RespValue::Integer(0);
  }
  if (it->second.abs_ttl_ms == 0) {
    return core::RespValue::Integer(0);
  }
  TrackRemove(it->second, op.key);
  it->second.abs_ttl_ms = 0;
  TrackInsert(it->second, op.key);
  return core::RespValue::Integer(1);
}

// --- Maintenance ---

void SingleShardStore::RefreshAccess(std::string_view key, core::SteadyTime now) {
  auto it = entries_.find(std::string(key));
  if (it == entries_.end()) return;
  it->second.last_access = now;
  it->second.eviction_deadline = now + it->second.eviction;
}

SingleShardStore::EvictExpiredReport SingleShardStore::EvictExpired(core::SteadyTime now) {
  EvictExpiredReport report;
  for (auto it = entries_.begin(); it != entries_.end();) {
    // Tombstones are reclaimed by GcTombstones once cold catches up, never by
    // the eviction deadline — removing one early could expose a stale overlay.
    if (it->second.tombstoned) {
      ++it;
      continue;
    }
    // NOLINTNEXTLINE(cppcoreguidelines-init-variables)
    const bool ttl_expired = IsExpiredByTtl(it->second, config_.wall_clock);
    const bool deadline_elapsed = it->second.eviction_deadline <= now;
    if (ttl_expired || deadline_elapsed) {
      TrackRemove(it->second, it->first);
      key_count_--;
      it = entries_.erase(it);
      // Bucket by the entry's actual disposition, not raw iteration: TTL is a
      // deletion (expired_count_), deadline is a tier transition
      // (eviction_count_). TTL wins when both fire (the semantic outcome is
      // "deleted entirely"). Tombstones are already skipped above, so C8's
      // HOT-4 tombstone-handling change cannot mis-bucket a tombstoned entry
      // here — only live entries reach this branch.
      if (ttl_expired) {
        expired_count_++;
        ++report.by_ttl;
      } else {
        eviction_count_++;
        ++report.by_deadline;
      }
    } else {
      ++it;
    }
  }
  return report;
}

size_t SingleShardStore::EvictLru(size_t target_bytes) {
  return EvictLru(target_bytes, std::string_view{});
}

size_t SingleShardStore::EvictLru(size_t target_bytes, std::string_view protect_key) {
  if (used_bytes_ <= target_bytes) return 0;

  std::vector<std::pair<core::SteadyTime, std::string>> candidates;
  candidates.reserve(entries_.size());
  for (const auto& [key, entry] : entries_) {
    if (entry.tombstoned) continue;
    if (!protect_key.empty() && key == protect_key) continue;
    candidates.emplace_back(entry.last_access, key);
  }

  std::ranges::sort(candidates);

  size_t evicted = 0;
  for (const auto& [access_time, key] : candidates) {
    if (used_bytes_ <= target_bytes) break;
    RemoveEntry(key);
    eviction_count_++;
    ++evicted;
  }
  return evicted;
}

bool SingleShardStore::EnsureCapacityFor(std::string_view protect_key) {
  // Called post-write: the just-written entry (protect_key) is already counted
  // in used_bytes_ and must survive, so make room by evicting OTHER LRU keys
  // down to the budget. Suppressed during replay: evicting mid-replay would
  // make the rebuilt hot view depend on memory timing, breaking deterministic
  // queue replay (invariant 4). The eviction worker reconverges after replay.
  if (replay_mode_ || !governor_.Enabled()) return true;
  if (!governor_.WouldExceed(used_bytes_, 0)) return true;
  EvictLru(governor_.Target(0), protect_key);
  // After evicting every other eligible key, the protected entry may still not
  // fit (a single value larger than the whole budget). It is already durable in
  // the queue, so the caller surfaces kResourceExhausted as an admission signal
  // — not a lost write (invariant 2).
  return !governor_.WouldExceed(used_bytes_, 0);
}

core::MemoryStats SingleShardStore::Stats() const {
  return {.used_bytes = used_bytes_,
          .key_count = key_count_,
          .eviction_count = eviction_count_,
          .expired_count = expired_count_,
          .max_bytes = governor_.max_bytes()};
}

void SingleShardStore::Wipe() {
  entries_.clear();
  used_bytes_ = 0;
  key_count_ = 0;
}

// --- Internal helpers ---

const Entry* SingleShardStore::FindEntry(std::string_view key) const {
  auto it = entries_.find(std::string(key));
  if (it == entries_.end()) return nullptr;
  return &it->second;
}

const Entry* SingleShardStore::FindLiveEntry(std::string_view key) const {
  const auto* entry = FindEntry(key);
  if (entry == nullptr) return nullptr;
  if (entry->tombstoned) return nullptr;
  if (IsExpiredByTtl(*entry, config_.wall_clock)) return nullptr;
  return entry;
}

Entry& SingleShardStore::GetOrCreateEntry(std::string_view key, Entry::Type type,
                                          core::EvictionTTL eviction) {
  auto [it, inserted] = entries_.try_emplace(std::string(key));
  // A tombstone is reborn as a fresh live key. Untrack its footprint first so
  // the slot is in the same state as a freshly inserted entry (the caller's
  // TrackInsert then accounts for the new value).
  if (!inserted && it->second.tombstoned) {
    TrackRemove(it->second, key);
  }
  if (inserted || it->second.tombstoned) {
    it->second.type = type;
    it->second.tombstoned = false;
    it->second.tombstone_seq = 0;
    it->second.abs_ttl_ms = 0;
    it->second.eviction = eviction;
    it->second.eviction_deadline = config_.steady_clock() + eviction;
    it->second.last_access = config_.steady_clock();
    switch (type) {
      case Entry::Type::kString:
        it->second.value = std::string{};
        break;
      case Entry::Type::kSet:
        it->second.value = SetValue{};
        break;
      case Entry::Type::kHash:
        it->second.value = HashValue{};
        break;
      case Entry::Type::kZset:
        it->second.value = ZsetValue{};
        break;
    }
    key_count_++;
    // Track the empty-entry baseline so the collection apply paths' balanced
    // TrackRemove/mutate/TrackInsert pattern has a matching prior insert. The
    // string-create path (ApplyStringSet) does its own TrackInsert after
    // setting the value, so it does not go through this baseline.
    TrackInsert(it->second, key);
  }
  return it->second;
}

core::Result<const Entry*> SingleShardStore::FindTypedEntry(std::string_view key,
                                                            Entry::Type expected) const {
  const auto* entry = FindLiveEntry(key);
  if (entry == nullptr) return nullptr;
  if (entry->type != expected) {
    return std::unexpected(core::Error(core::ErrorCode::kWrongType,
                                       "Operation against a key holding the wrong kind of value"));
  }
  return entry;
}

void SingleShardStore::RemoveEntry(const std::string& key) {
  auto it = entries_.find(key);
  if (it == entries_.end()) return;
  TrackRemove(it->second, key);
  key_count_--;
  entries_.erase(it);
}

void SingleShardStore::TombstoneEntry(Entry& entry, std::string_view key, core::SequenceId seq) {
  if (entry.tombstoned) {
    entry.tombstone_seq = seq;
    return;
  }
  TrackRemove(entry, key);
  entry.value = std::string{};
  entry.abs_ttl_ms = 0;
  entry.tombstoned = true;
  entry.tombstone_seq = seq;
  key_count_--;
  TrackInsert(entry, key);
}

core::HotKeyPresence SingleShardStore::Probe(std::string_view key) const {
  const auto* entry = FindEntry(key);
  if (entry == nullptr) return core::HotKeyPresence::kAbsent;
  if (entry->tombstoned) return core::HotKeyPresence::kTombstoned;
  if (IsExpiredByTtl(*entry, config_.wall_clock)) return core::HotKeyPresence::kAbsent;
  return core::HotKeyPresence::kPresent;
}

size_t SingleShardStore::GcTombstones(core::SequenceId horizon) {
  size_t reclaimed = 0;
  for (auto it = entries_.begin(); it != entries_.end();) {
    if (it->second.tombstoned && it->second.tombstone_seq <= horizon) {
      TrackRemove(it->second, it->first);
      it = entries_.erase(it);
      ++reclaimed;
    } else {
      ++it;
    }
  }
  return reclaimed;
}

void SingleShardStore::TrackInsert(const Entry& entry, std::string_view key) {
  used_bytes_ += entry.ApproximateBytes() + sizeof(std::string) + key.size();
}

void SingleShardStore::TrackRemove(const Entry& entry, std::string_view key) {
  auto bytes = entry.ApproximateBytes() + sizeof(std::string) + key.size();
  used_bytes_ = (used_bytes_ >= bytes) ? used_bytes_ - bytes : 0;
}

}  // namespace abyss::hot
