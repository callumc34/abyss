#include "abyss/engine/loader.h"

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <span>
#include <string>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <variant>

#include "abyss/core/shard_router.h"

namespace abyss::engine {

namespace {

using consumer::CompactedState;
using core::KeyType;
using DataType = CompactedState::DataType;
using TtlIntent = CompactedState::TtlIntent;

using Members = std::unordered_set<std::string>;
using Fields = std::unordered_map<std::string, std::string>;
using Scores = std::unordered_map<std::string, double>;

core::Error WrongType() {
  return {core::ErrorCode::kWrongType, "Operation against a key holding the wrong kind of value"};
}

std::optional<KeyType> TypeOf(const CompactedState& delta) {
  switch (delta.Type()) {
    case DataType::kString:
      return KeyType::kString;
    case DataType::kSet:
      return KeyType::kSet;
    case DataType::kHash:
      return KeyType::kHash;
    case DataType::kZset:
      return KeyType::kZset;
    case DataType::kNone:
      break;
  }
  return std::nullopt;
}

hot::Entry::Type HotType(KeyType type) {
  switch (type) {
    case KeyType::kSet:
      return hot::Entry::Type::kSet;
    case KeyType::kHash:
      return hot::Entry::Type::kHash;
    case KeyType::kZset:
      return hot::Entry::Type::kZset;
    case KeyType::kString:
      break;
  }
  return hot::Entry::Type::kString;
}

bool Invalidated(const CompactedState& delta) {
  return delta.Invalidation() == CompactedState::BaseInvalidation::kDeleteAll;
}

// A string's value, or a collection's added members.
bool HasAdds(const CompactedState& delta) {
  switch (delta.Type()) {
    case DataType::kString:
      return true;
    case DataType::kSet:
      return !delta.SetMembers().empty();
    case DataType::kHash:
      return !delta.HashFields().empty();
    case DataType::kZset:
      return !delta.ZsetMembers().empty();
    case DataType::kNone:
      break;
  }
  return false;
}

size_t Removals(const CompactedState& delta) {
  switch (delta.Type()) {
    case DataType::kSet:
      return delta.SetRemovedMembers().size();
    case DataType::kHash:
      return delta.HashRemovedFields().size();
    case DataType::kZset:
      return delta.ZsetRemovedMembers().size();
    case DataType::kString:
    case DataType::kNone:
      break;
  }
  return 0;
}

// The TTL after `delta`, over a base whose TTL is `base_ttl`. A string
// carries its own. A TTL set to 0 becomes 1, as 0 means none.
int64_t TtlAfter(const CompactedState& delta, int64_t base_ttl) {
  const auto delta_ttl = static_cast<int64_t>(delta.AbsTtlMs());
  switch (delta.Ttl()) {
    case TtlIntent::kSetTo:
      return std::max<int64_t>(delta_ttl, 1);
    case TtlIntent::kCleared:
      return 0;
    case TtlIntent::kUnchanged:
      break;
  }
  return delta.Type() == DataType::kString ? delta_ttl : base_ttl;
}

// Whether the key's type and TTL after `delta` depend on cold.
bool NeedsColdMeta(const CompactedState& delta) {
  if (delta.IsTombstone() || Invalidated(delta)) return false;
  return !HasAdds(delta) ||
         (delta.Type() != DataType::kString && delta.Ttl() == TtlIntent::kUnchanged);
}

// The key's type and TTL after `delta`, over cold's `base`; nullopt
// when absent. Cardinality is cold's, so removals may have emptied it.
std::optional<core::KeyMeta> KeyAfter(std::optional<core::KeyMeta> base,
                                      const CompactedState* delta) {
  if (delta == nullptr) return base;
  if (delta->IsTombstone()) return std::nullopt;
  if (Invalidated(*delta)) base.reset();
  if (const auto type = TypeOf(*delta); type.has_value() && HasAdds(*delta)) {
    const bool same = base.has_value() && base->type == *type;
    return core::KeyMeta{.type = *type,
                         .abs_ttl_ms = TtlAfter(*delta, same ? base->abs_ttl_ms : 0)};
  }
  // TTL changes, or removals, of a key that must already exist.
  if (!base.has_value()) return std::nullopt;
  base->abs_ttl_ms = TtlAfter(*delta, base->abs_ttl_ms);
  return base;
}

// Whether `delta`'s removals may have emptied `key`, which only a full
// load can tell. That load is bounded: cardinality <= removals.
bool MayBeEmptied(const core::KeyMeta& key, const CompactedState* delta) {
  return delta != nullptr && !HasAdds(*delta) && TypeOf(*delta) == key.type &&
         key.cardinality <= Removals(*delta);
}

// Existence after `delta`; nullopt when its removals may have emptied
// the key.
std::optional<hot::LoadResult> ExistenceAfter(std::optional<core::KeyMeta> base,
                                              const CompactedState* delta) {
  const auto key = KeyAfter(base, delta);
  if (!key.has_value()) return hot::LoadedAbsent{};
  if (MayBeEmptied(*key, delta)) return std::nullopt;
  return hot::LoadedExists{.type = HotType(key->type), .abs_ttl_ms = key->abs_ttl_ms};
}

core::ColdValue EmptyValue(KeyType type) {
  switch (type) {
    case KeyType::kSet:
      return Members{};
    case KeyType::kHash:
      return Fields{};
    case KeyType::kZset:
      return Scores{};
    case KeyType::kString:
      break;
  }
  return std::string{};
}

// `delta` applied to `base` as cold's flush applies it: adds of a new
// type clear the old type; removals of a type the key does not hold
// change nothing.
std::optional<core::ColdKeyState> MergeState(std::optional<core::ColdKeyState> base,
                                             const CompactedState& delta) {
  if (delta.IsTombstone()) return std::nullopt;
  if (Invalidated(delta)) base.reset();
  const auto type = TypeOf(delta);
  if (base.has_value() && (!type.has_value() || (base->type != *type && !HasAdds(delta)))) {
    base->abs_ttl_ms = TtlAfter(delta, base->abs_ttl_ms);
    return base;
  }
  if (!type.has_value()) return std::nullopt;
  const bool same = base.has_value() && base->type == *type;
  core::ColdKeyState merged{.type = *type,
                            .value = same ? std::move(base->value) : EmptyValue(*type),
                            .abs_ttl_ms = TtlAfter(delta, same ? base->abs_ttl_ms : 0)};
  switch (*type) {
    case KeyType::kString:
      merged.value = delta.StringValue();
      break;
    case KeyType::kSet: {
      auto& members = std::get<Members>(merged.value);
      members.insert(delta.SetMembers().begin(), delta.SetMembers().end());
      for (const auto& member : delta.SetRemovedMembers()) members.erase(member);
      break;
    }
    case KeyType::kHash: {
      auto& fields = std::get<Fields>(merged.value);
      for (const auto& [field, value] : delta.HashFields()) fields.insert_or_assign(field, value);
      for (const auto& field : delta.HashRemovedFields()) fields.erase(field);
      break;
    }
    case KeyType::kZset: {
      auto& scores = std::get<Scores>(merged.value);
      for (const auto& [member, score] : delta.ZsetMembers()) {
        scores.insert_or_assign(member, score);
      }
      for (const auto& member : delta.ZsetRemovedMembers()) scores.erase(member);
      break;
    }
  }
  return merged;
}

hot::Value HotValue(core::ColdValue value) {
  return std::visit(
      [](auto&& v) -> hot::Value {
        using T = std::decay_t<decltype(v)>;
        if constexpr (std::is_same_v<T, Members>) {
          return hot::SetValue{.members = std::forward<decltype(v)>(v)};
        } else if constexpr (std::is_same_v<T, Fields>) {
          return hot::HashValue{.fields = std::forward<decltype(v)>(v)};
        } else if constexpr (std::is_same_v<T, Scores>) {
          hot::ZsetValue zset;
          for (const auto& [member, score] : v) zset.score_members[score].insert(member);
          zset.member_scores = std::forward<decltype(v)>(v);
          return zset;
        } else {
          return std::forward<decltype(v)>(v);
        }
      },
      std::move(value));
}

bool EmptyCollection(const core::ColdValue& value) {
  return std::visit(
      [](const auto& v) {
        if constexpr (std::is_same_v<std::decay_t<decltype(v)>, std::string>) {
          return false;
        } else {
          return v.empty();
        }
      },
      value);
}

hot::LoadResult Loaded(std::optional<core::ColdKeyState> state) {
  if (!state.has_value() || EmptyCollection(state->value)) return hot::LoadedAbsent{};
  return hot::MakeLoadedFull(HotValue(std::move(state->value)), state->abs_ttl_ms);
}

std::optional<core::MemberValue> DeltaMember(const CompactedState& delta, KeyType type,
                                             const std::string& member) {
  switch (type) {
    case KeyType::kSet:
      if (delta.SetMembers().contains(member)) return std::monostate{};
      break;
    case KeyType::kHash:
      if (const auto it = delta.HashFields().find(member); it != delta.HashFields().end()) {
        return it->second;
      }
      break;
    case KeyType::kZset:
      if (const auto it = delta.ZsetMembers().find(member); it != delta.ZsetMembers().end()) {
        return it->second;
      }
      break;
    case KeyType::kString:
      break;
  }
  return std::nullopt;
}

bool DeltaRemoved(const CompactedState& delta, KeyType type, const std::string& member) {
  switch (type) {
    case KeyType::kSet:
      return delta.SetRemovedMembers().contains(member);
    case KeyType::kHash:
      return delta.HashRemovedFields().contains(member);
    case KeyType::kZset:
      return delta.ZsetRemovedMembers().contains(member);
    case KeyType::kString:
      break;
  }
  return false;
}

}  // namespace

hot::LoadResult MergeLoad(std::optional<core::ColdKeyState> base, const CompactedState* delta) {
  if (delta == nullptr) return Loaded(std::move(base));
  return Loaded(MergeState(std::move(base), *delta));
}

Loader::Loader(hot::ShardedHotStore& hot, const consumer::CompactionBufferRouter& buffers,
               core::ColdStore& cold, core::WallClockFn wall_clock)
    : hot_(hot), buffers_(buffers), cold_(cold), wall_clock_(std::move(wall_clock)) {}

core::Result<hot::LoadResult> Loader::Load(core::ShardId shard, std::string_view key, Need need,
                                           core::SteadyTime deadline) const {
  const auto delta = buffers_.Snapshot(shard, key);
  const CompactedState* changes = delta.has_value() ? &*delta : nullptr;
  if (changes != nullptr && changes->IsTombstone()) return hot::LoadedAbsent{};

  if (need == Need::kExistence && hot_.RetainsStubs()) {
    std::optional<core::KeyMeta> base;
    if (changes == nullptr || NeedsColdMeta(*changes)) {
      auto probed = cold_.ProbeKey(key, deadline);
      if (!probed.has_value()) return std::unexpected(probed.error());
      base = *probed;
    }
    if (auto exists = ExistenceAfter(base, changes); exists.has_value()) return *std::move(exists);
  }

  std::optional<core::ColdKeyState> base;
  if (changes == nullptr || !Invalidated(*changes)) {
    auto loaded = cold_.LoadKey(key, deadline);
    if (!loaded.has_value()) return std::unexpected(loaded.error());
    base = *std::move(loaded);
  }
  return MergeLoad(std::move(base), changes);
}

core::Result<Loader::Filled> Loader::Install(std::string_view key, core::SteadyTime deadline) {
  const auto shard = core::ComputeShard(key, hot_.shard_count());
  using Status = hot::LoadStart::Status;
  for (;;) {
    const hot::LoadStart start = hot_.BeginLoad(key);
    switch (start.status) {
      case Status::kResident:
        return Filled{.fill = Fill::kResident};
      case Status::kFlushed:
        return Filled{.fill = Fill::kFlushed};
      case Status::kPending:
        if (!hot_.AwaitLoad(key, deadline)) {
          return std::unexpected(
              core::Error{core::ErrorCode::kTimeout, "timed out awaiting another load of the key"});
        }
        continue;
      case Status::kStarted:
        break;
    }
    auto loaded = Load(shard, key, Need::kState, deadline);
    if (!loaded.has_value()) {
      hot_.AbortLoad(key, start.token);
      return std::unexpected(loaded.error());
    }
    hot::LoadCompletion completion{
        .key = std::string(key), .token = start.token, .result = *std::move(loaded)};
    if (hot_.CompleteLoads(shard, std::span(&completion, 1)) == 1) {
      return Filled{.fill = Fill::kInstalled};
    }
    return Filled{.fill = Fill::kDiscarded, .result = std::move(completion.result)};
  }
}

core::Result<std::optional<core::MemberValue>> Loader::Member(core::ShardId shard,
                                                              std::string_view key, KeyType type,
                                                              std::string_view member,
                                                              core::SteadyTime deadline) const {
  const auto delta = buffers_.Snapshot(shard, key);
  const CompactedState* changes = delta.has_value() ? &*delta : nullptr;
  if (changes != nullptr && changes->IsTombstone()) return std::nullopt;

  std::optional<core::KeyMeta> base;
  if (changes == nullptr || NeedsColdMeta(*changes)) {
    auto probed = cold_.ProbeKey(key, deadline);
    if (!probed.has_value()) return std::unexpected(probed.error());
    base = *probed;
  }
  const auto meta = KeyAfter(base, changes);
  if (!meta.has_value()) return std::nullopt;
  if (meta->type != type) {
    // A key of another type that removals emptied is absent, not
    // WRONGTYPE.
    if (MayBeEmptied(*meta, changes)) {
      auto loaded = Load(shard, key, Need::kState, deadline);
      if (!loaded.has_value()) return std::unexpected(loaded.error());
      if (std::holds_alternative<hot::LoadedAbsent>(*loaded)) return std::nullopt;
    }
    return std::unexpected(WrongType());
  }
  const auto now_ms =
      std::chrono::duration_cast<std::chrono::milliseconds>(wall_clock_().time_since_epoch())
          .count();
  if (meta->abs_ttl_ms != 0 && now_ms >= meta->abs_ttl_ms) return std::nullopt;

  if (changes != nullptr && TypeOf(*changes) == type) {
    const std::string owned(member);
    if (auto value = DeltaMember(*changes, type, owned); value.has_value()) return value;
    if (DeltaRemoved(*changes, type, owned) || Invalidated(*changes)) return std::nullopt;
  }
  return cold_.LoadMember(key, type, member, deadline);
}

core::Result<bool> Loader::IsMember(core::ShardId shard, std::string_view key,
                                    std::string_view member, core::SteadyTime deadline) const {
  auto value = Member(shard, key, KeyType::kSet, member, deadline);
  if (!value.has_value()) return std::unexpected(value.error());
  return value->has_value();
}

core::Result<std::optional<double>> Loader::Score(core::ShardId shard, std::string_view key,
                                                  std::string_view member,
                                                  core::SteadyTime deadline) const {
  auto value = Member(shard, key, KeyType::kZset, member, deadline);
  if (!value.has_value()) return std::unexpected(value.error());
  if (!value->has_value()) return std::nullopt;
  return std::get<double>(**value);
}

core::Result<std::optional<std::string>> Loader::HashField(core::ShardId shard,
                                                           std::string_view key,
                                                           std::string_view field,
                                                           core::SteadyTime deadline) const {
  auto value = Member(shard, key, KeyType::kHash, field, deadline);
  if (!value.has_value()) return std::unexpected(value.error());
  if (!value->has_value()) return std::nullopt;
  return std::get<std::string>(std::move(**value));
}

}  // namespace abyss::engine
