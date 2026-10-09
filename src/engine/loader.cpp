#include "abyss/engine/loader.h"

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <span>
#include <string>
#include <type_traits>
#include <utility>
#include <variant>

#include "abyss/core/shard_router.h"
#include "abyss/core/string_hash.h"

namespace abyss::engine {

namespace {

using consumer::CompactedState;
using core::KeyType;
using DataType = CompactedState::DataType;
using TtlIntent = CompactedState::TtlIntent;

using Members = core::StringSet;
using Fields = core::StringMap<std::string>;
using Scores = core::StringMap<double>;

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

// Members or fields `delta` adds; 1 for a string.
size_t Adds(const CompactedState& delta) {
  switch (delta.Type()) {
    case DataType::kSet:
      return delta.SetMembers().size();
    case DataType::kHash:
      return delta.HashFields().size();
    case DataType::kZset:
      return delta.ZsetMembers().size();
    case DataType::kString:
      return 1;
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
    : hot_(hot), buffers_(buffers), cold_(cold), wall_clock_(std::move(wall_clock)) {
  flights_.reserve(hot_.shard_count());
  for (uint32_t i = 0; i < hot_.shard_count(); ++i) flights_.push_back(std::make_unique<Flights>());
}

core::Result<hot::LoadResult> Loader::Load(core::ShardId shard, std::string_view key, Need need,
                                           core::SteadyTime deadline) const {
  if (need == Need::kExistence && hot_.RetainsStubs()) return Probe(shard, key, deadline);
  const auto delta = buffers_.Snapshot(shard, key);
  const CompactedState* changes = delta.has_value() ? &*delta : nullptr;
  if (changes != nullptr && changes->IsTombstone()) return hot::LoadedAbsent{};

  std::optional<core::ColdKeyState> base;
  if (changes == nullptr || !Invalidated(*changes)) {
    auto loaded = cold_.LoadKey(key, deadline);
    if (!loaded.has_value()) return std::unexpected(loaded.error());
    base = *std::move(loaded);
  }
  return MergeLoad(std::move(base), changes);
}

core::Result<Loader::Shared> Loader::LoadAs(core::ShardId shard, std::string_view key, KeyType type,
                                            core::SteadyTime deadline, Source* source) const {
  return Fly(shard, key, type, deadline, source, nullptr);
}

core::Result<Loader::Shared> Loader::Fly(core::ShardId shard, std::string_view key, KeyType type,
                                         core::SteadyTime deadline, Source* source,
                                         std::optional<hot::LoadResult>* owned) const {
  Flights& flights = *flights_.at(shard);
  // A flight is joined only at the drain horizon its leader read before
  // its buffer snapshot. A write after that snapshot can leave hot only
  // once drained, and its seq is above that horizon, so a caller that
  // finds the key non-resident after such a write sees a later horizon
  // and loads afresh: no flight hands out state older than a write
  // that was resident when the caller missed.
  const std::tuple<std::string, KeyType, core::SequenceId> id{std::string(key), type,
                                                              hot_.Drained(shard)};
  std::promise<Flight> leading;
  std::shared_future<Flight> flight;
  bool leader = false;
  {
    const std::scoped_lock lock(flights.mu);
    auto [it, started] = flights.loading.try_emplace(id);
    if (started) {
      it->second.done = leading.get_future().share();
      leader = true;
    } else {
      ++it->second.waiters;
      ++joins_;
    }
    flight = it->second.done;
  }
  if (!leader) {
    if (flight.wait_until(deadline) != std::future_status::ready) {
      return std::unexpected(
          core::Error{core::ErrorCode::kTimeout, "timed out awaiting another load of the key"});
    }
    if (source != nullptr) *source = flight.get().source;
    return flight.get().result;
  }

  Flight done;
  auto loaded = LoadTyped(shard, key, type, deadline, &done.source);
  size_t waiters = 0;
  {
    // Erased with its waiters counted, so none joins after.
    const std::scoped_lock lock(flights.mu);
    const auto it = flights.loading.find(id);
    waiters = it->second.waiters;
    flights.loading.erase(it);
  }
  if (!loaded.has_value()) {
    done.result = std::unexpected(loaded.error());
  } else if (owned == nullptr) {
    done.result = std::make_shared<const hot::LoadResult>(*std::move(loaded));
  } else {
    // The caller keeps its own; waiters, if any, share a copy.
    if (waiters > 0) done.result = std::make_shared<const hot::LoadResult>(*loaded);
    *owned = *std::move(loaded);
  }
  leading.set_value(done);
  if (source != nullptr) *source = done.source;
  return std::move(done.result);
}

core::Result<hot::LoadResult> Loader::LoadTyped(core::ShardId shard, std::string_view key,
                                                KeyType type, core::SteadyTime deadline,
                                                Source* source) const {
  Source unused = Source::kBuffer;
  Source& from = source != nullptr ? *source : unused;
  from = Source::kBuffer;
  const auto delta = buffers_.Snapshot(shard, key);
  const CompactedState* changes = delta.has_value() ? &*delta : nullptr;
  if (changes != nullptr && changes->IsTombstone()) return hot::LoadedAbsent{};
  if (changes != nullptr && Invalidated(*changes)) return MergeLoad(std::nullopt, changes);

  const std::optional<KeyType> added =
      changes != nullptr && HasAdds(*changes) ? TypeOf(*changes) : std::nullopt;
  if (added.has_value() && *added != type) {
    // The adds make it another type; only its TTL may be cold's.
    std::optional<core::KeyMeta> base;
    if (NeedsColdMeta(*changes)) {
      from = Source::kCold;
      auto probed = cold_.ProbeKey(key, deadline);
      if (!probed.has_value()) return std::unexpected(probed.error());
      base = *probed;
    }
    const auto meta = KeyAfter(base, changes);
    if (!meta.has_value()) return hot::LoadedAbsent{};
    return hot::LoadedExists{.type = HotType(meta->type), .abs_ttl_ms = meta->abs_ttl_ms};
  }

  // A string's delta is its whole state, TTL included.
  if (added == KeyType::kString) return MergeLoad(std::nullopt, changes);
  from = Source::kCold;
  auto loaded = cold_.LoadKeyAs(key, type, deadline);
  if (!loaded.has_value()) return std::unexpected(loaded.error());
  if (!loaded->has_value()) return MergeLoad(std::nullopt, changes);
  if (auto* state = std::get_if<core::ColdKeyState>(&**loaded)) {
    return MergeLoad(std::move(*state), changes);
  }
  // Cold holds another type, which adds of `type` replace.
  if (added.has_value()) return MergeLoad(std::nullopt, changes);
  const auto meta = KeyAfter(std::get<core::KeyMeta>(**loaded), changes);
  if (!meta.has_value()) return hot::LoadedAbsent{};
  if (MayBeEmptied(*meta, changes)) return Load(shard, key, Need::kState, deadline);
  return hot::LoadedExists{.type = HotType(meta->type), .abs_ttl_ms = meta->abs_ttl_ms};
}

core::Result<hot::LoadResult> Loader::Probe(core::ShardId shard, std::string_view key,
                                            core::SteadyTime deadline, Source* source) const {
  Source unused = Source::kBuffer;
  Source& from = source != nullptr ? *source : unused;
  from = Source::kBuffer;
  const auto delta = buffers_.Snapshot(shard, key);
  const CompactedState* changes = delta.has_value() ? &*delta : nullptr;
  if (changes != nullptr && changes->IsTombstone()) return hot::LoadedAbsent{};

  std::optional<core::KeyMeta> base;
  if (changes == nullptr || NeedsColdMeta(*changes)) {
    from = Source::kCold;
    auto probed = cold_.ProbeKey(key, deadline);
    if (!probed.has_value()) return std::unexpected(probed.error());
    base = *probed;
  }
  if (auto exists = ExistenceAfter(base, changes); exists.has_value()) return *std::move(exists);
  // Bounded: the key has no more members than the delta removes.
  std::optional<core::ColdKeyState> full;
  if (!Invalidated(*changes)) {
    from = Source::kCold;
    auto loaded = cold_.LoadKey(key, deadline);
    if (!loaded.has_value()) return std::unexpected(loaded.error());
    full = *std::move(loaded);
  }
  return MergeLoad(std::move(full), changes);
}

core::Result<Loader::Filled> Loader::Install(std::string_view key, KeyType type,
                                             core::SteadyTime deadline) {
  const auto shard = core::ComputeShard(key, hot_.shard_count());
  using Status = hot::LoadStart::Status;
  const hot::LoadStart start = hot_.BeginLoad(key);
  switch (start.status) {
    case Status::kResident:
      return Filled{.fill = Fill::kResident};
    case Status::kFlushed:
      return Filled{.fill = Fill::kFlushed};
    case Status::kPending:
      // A write's full load can take far longer than a read's deadline.
      return Filled{.fill = Fill::kPending};
    case Status::kStarted:
      break;
  }
  Source source = Source::kBuffer;
  std::optional<hot::LoadResult> owned;
  auto flown = Fly(shard, key, type, deadline, &source, &owned);
  if (!flown.has_value()) {
    hot_.AbortLoad(key, start.token);
    return std::unexpected(flown.error());
  }
  // A read loading it already lent its result: a copy to install.
  std::optional<hot::LoadResult> loaded = std::move(owned);
  if (!loaded.has_value()) loaded = **flown;
  // Small enough to keep a copy of, so the caller need not re-read.
  std::optional<hot::LoadResult> kept;
  if (!std::holds_alternative<hot::LoadedFull>(*loaded)) kept = *loaded;
  using Result = hot::ShardedHotStore::FillResult;
  const Result filled = hot_.Fill(key, start.token, *std::move(loaded));
  if (filled == Result::kInstalled) {
    return Filled{.fill = Fill::kInstalled, .result = std::move(kept), .source = source};
  }
  Fill fill = Fill::kDiscarded;
  if (filled == Result::kOverBackpressure) fill = Fill::kSkippedBackpressure;
  if (filled == Result::kTooLarge) fill = Fill::kSkippedSize;
  if (filled == Result::kNoRoom) fill = Fill::kSkippedEvictCap;
  // NOLINTNEXTLINE(bugprone-use-after-move): moved only when installed.
  return Filled{.fill = fill, .result = std::move(loaded), .source = source};
}

core::Result<Loader::Typed> Loader::MetaAs(core::ShardId shard, std::string_view key, KeyType type,
                                           const CompactedState* changes,
                                           core::SteadyTime deadline) const {
  Typed out;
  if (changes != nullptr && changes->IsTombstone()) return out;
  if (changes == nullptr || NeedsColdMeta(*changes)) {
    out.source = Source::kCold;
    auto probed = cold_.ProbeKey(key, deadline);
    if (!probed.has_value()) return std::unexpected(probed.error());
    out.base = *probed;
  }
  const auto meta = KeyAfter(out.base, changes);
  if (!meta.has_value()) return out;
  if (meta->type != type) {
    // A key of another type that removals emptied is absent, not
    // WRONGTYPE.
    if (MayBeEmptied(*meta, changes)) {
      auto loaded = Load(shard, key, Need::kState, deadline);
      if (!loaded.has_value()) return std::unexpected(loaded.error());
      if (std::holds_alternative<hot::LoadedAbsent>(*loaded)) return out;
    }
    return std::unexpected(WrongType());
  }
  const auto now_ms =
      std::chrono::duration_cast<std::chrono::milliseconds>(wall_clock_().time_since_epoch())
          .count();
  if (meta->abs_ttl_ms != 0 && now_ms >= meta->abs_ttl_ms) return out;
  out.meta = meta;
  return out;
}

core::Result<Loader::Members> Loader::ReadMembers(core::ShardId shard, std::string_view key,
                                                  KeyType type,
                                                  std::span<const std::string_view> members,
                                                  core::SteadyTime deadline) const {
  Members out{.values = std::vector<std::optional<core::MemberValue>>(members.size())};
  const auto delta = buffers_.Snapshot(shard, key);
  const CompactedState* changes = delta.has_value() ? &*delta : nullptr;
  auto typed = MetaAs(shard, key, type, changes, deadline);
  if (!typed.has_value()) return std::unexpected(typed.error());
  out.source = typed->source;
  if (!typed->meta.has_value()) return out;
  const auto& base = typed->base;

  const bool same = changes != nullptr && TypeOf(*changes) == type;
  out.cardinality = (base.has_value() && base->type == type ? base->cardinality : 0) +
                    (same ? Adds(*changes) : 0);
  std::vector<std::string_view> cold_asks;
  std::vector<size_t> cold_slots;
  for (size_t i = 0; i < members.size(); ++i) {
    if (same) {
      const std::string owned(members[i]);
      if (auto value = DeltaMember(*changes, type, owned); value.has_value()) {
        out.values[i] = std::move(value);
        continue;
      }
      if (DeltaRemoved(*changes, type, owned) || Invalidated(*changes)) continue;
    }
    cold_asks.push_back(members[i]);
    cold_slots.push_back(i);
  }
  if (cold_asks.empty()) return out;
  out.source = Source::kCold;
  auto cold = cold_.LoadMembers(key, type, cold_asks, deadline);
  if (!cold.has_value()) return std::unexpected(cold.error());
  if (cold->size() != cold_asks.size()) {
    return std::unexpected(
        core::Error{core::ErrorCode::kInternal, "cold answered a different member count"});
  }
  for (size_t i = 0; i < cold_slots.size(); ++i) out.values[cold_slots[i]] = (*cold)[i];
  return out;
}

core::Result<Loader::Count> Loader::Cardinality(core::ShardId shard, std::string_view key,
                                                KeyType type, core::SteadyTime deadline) const {
  const auto delta = buffers_.Snapshot(shard, key);
  const CompactedState* changes = delta.has_value() ? &*delta : nullptr;
  auto typed = MetaAs(shard, key, type, changes, deadline);
  if (!typed.has_value()) return std::unexpected(typed.error());
  Count out{.source = typed->source};
  if (!typed->meta.has_value()) return out;
  const bool counted = typed->base.has_value() && typed->base->type == type;
  const uint64_t base = counted ? typed->base->cardinality : 0;
  // TTL changes, or removals of another type, leave cold's count.
  if (changes == nullptr || TypeOf(*changes) != type) {
    out.members = base;
    return out;
  }
  if (Invalidated(*changes) || !counted) {
    out.members = Adds(*changes);
    return out;
  }
  // Cold's count, plus the adds it lacks, less the removals it holds.
  std::vector<std::string_view> asks;
  const auto ask = [&asks](const auto& members) {
    for (const auto& member : members) {
      if constexpr (std::is_same_v<std::decay_t<decltype(member)>, std::string>) {
        asks.emplace_back(member);
      } else {
        asks.emplace_back(member.first);
      }
    }
  };
  switch (type) {
    case KeyType::kSet:
      ask(changes->SetMembers());
      break;
    case KeyType::kHash:
      ask(changes->HashFields());
      break;
    case KeyType::kZset:
      ask(changes->ZsetMembers());
      break;
    case KeyType::kString:
      break;
  }
  const size_t adds = asks.size();
  switch (type) {
    case KeyType::kSet:
      ask(changes->SetRemovedMembers());
      break;
    case KeyType::kHash:
      ask(changes->HashRemovedFields());
      break;
    case KeyType::kZset:
      ask(changes->ZsetRemovedMembers());
      break;
    case KeyType::kString:
      break;
  }
  out.members = base;
  if (asks.empty()) return out;
  out.source = Source::kCold;
  auto held = cold_.LoadMembers(key, type, asks, deadline);
  if (!held.has_value()) return std::unexpected(held.error());
  if (held->size() != asks.size()) {
    return std::unexpected(
        core::Error{core::ErrorCode::kInternal, "cold answered a different member count"});
  }
  for (size_t i = 0; i < asks.size(); ++i) {
    const bool in_cold = (*held)[i].has_value();
    if (i < adds && !in_cold) ++out.members;
    if (i >= adds && in_cold && out.members > 0) --out.members;
  }
  return out;
}

core::Result<std::optional<core::MemberValue>> Loader::Member(core::ShardId shard,
                                                              std::string_view key, KeyType type,
                                                              std::string_view member,
                                                              core::SteadyTime deadline) const {
  auto read = ReadMembers(shard, key, type, std::span(&member, 1), deadline);
  if (!read.has_value()) return std::unexpected(read.error());
  return std::move(read->values.front());
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
