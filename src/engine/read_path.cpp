#include "abyss/engine/read_path.h"

#include <chrono>
#include <cstdint>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

#include "abyss/core/resp_format.h"
#include "abyss/hot/single_shard_store.h"
#include "abyss/metrics/names.h"

namespace abyss::engine {

namespace {

namespace ops = core::ops;

// A read that loads its whole key to answer.
bool LoadsWholeKey(const ops::ReadOp& op) {
  return std::holds_alternative<ops::SetMembers>(op) ||
         std::holds_alternative<ops::ZsetRange>(op) ||
         std::holds_alternative<ops::HashGetAll>(op) || std::holds_alternative<ops::HashKeys>(op) ||
         std::holds_alternative<ops::HashVals>(op);
}

bool Expired(int64_t abs_ttl_ms, int64_t now_ms) { return abs_ttl_ms != 0 && now_ms >= abs_ttl_ms; }

core::RespValue BulkOrNull(const std::optional<core::MemberValue>& value) {
  if (!value.has_value()) return core::RespValue::Null();
  if (const auto* score = std::get_if<double>(&*value)) {
    return core::RespValue::BulkString(core::FormatRespDouble(*score));
  }
  if (const auto* field = std::get_if<std::string>(&*value)) {
    return core::RespValue::BulkString(*field);
  }
  return core::RespValue::Integer(1);
}

}  // namespace

ReadPath::ReadPath(hot::ShardedHotStore& hot, Loader& loader, Sequencer& sequencer,
                   ReadPathConfig config)
    : hot_(hot), loader_(loader), sequencer_(sequencer), config_(std::move(config)) {
  if (config_.fill_doorkeeper) {
    doorkeeper_ = std::make_unique<Doorkeeper>(config_.doorkeeper_window);
  }
  auto& reg = metrics::Registry::Instance();
  hits_hot_ = reg.Counter(metrics::names::kHitsTotal, metrics::Tier::kHot);
  hits_buffer_ = reg.Counter(metrics::names::kHitsTotal, metrics::Tier::kBuffer);
  hits_cold_ = reg.Counter(metrics::names::kHitsTotal, metrics::Tier::kCold);
  misses_ = reg.Counter(metrics::names::kMissesTotal);
  cold_scan_deadline_exceeded_ = reg.Counter(metrics::names::kColdScanDeadlineExceededTotal);
  using metrics::FillOutcome;
  for (const FillOutcome outcome :
       {FillOutcome::kInstalled, FillOutcome::kDiscarded, FillOutcome::kSkippedBackpressure,
        FillOutcome::kSkippedSize, FillOutcome::kSkippedEvictCap, FillOutcome::kFailed}) {
    fills_.at(static_cast<size_t>(outcome)) = reg.Counter(metrics::names::kHotFillsTotal, outcome);
  }
}

core::Result<core::RespValue> ReadPath::Read(const ops::ReadOp& op) {
  if (auto hot = FromHot(op); hot.has_value()) return *std::move(hot);
  return Miss(op);
}

core::Result<core::RespValue> ReadPath::Mget(std::span<const std::string> keys) {
  std::vector<core::RespValue> out;
  out.reserve(keys.size());
  for (const auto& key : keys) {
    auto result = Read(ops::ReadOp{ops::StringGet{.key = key}});
    if (!result.has_value()) {
      // Redis MGET answers nil for a key of another type.
      if (result.error().code() != core::ErrorCode::kWrongType) {
        return std::unexpected(result.error());
      }
      out.push_back(core::RespValue::Null());
      continue;
    }
    out.push_back(std::move(*result));
  }
  return core::RespValue::Array(std::move(out));
}

core::Result<core::RespValue> ReadPath::Exists(std::span<const std::string> keys) {
  // EXISTS k k counts twice (Redis behaviour): no dedup.
  int64_t count = 0;
  for (const auto& key : keys) {
    auto present = Read(ops::ReadOp{ops::Exists{.keys = {key}}});
    if (!present.has_value()) return std::unexpected(present.error());
    count += present->AsInteger();
  }
  return core::RespValue::Integer(count);
}

std::optional<core::Result<core::RespValue>> ReadPath::FromHot(
    const ops::ReadOp& op, std::optional<Loader::Source> filled_from) {
  auto read = hot_.Read(op);
  if (!read.result.has_value() && read.result.error().code() == core::ErrorCode::kNotFound) {
    return std::nullopt;
  }
  if (read.result.has_value()) {
    if (filled_from.has_value()) {
      CountHit(*filled_from);
    } else {
      hits_hot_.Increment();
    }
  }
  if (read.fence.has_value()) {
    const ShardSeq fence{.shard = hot_.ShardOf(ops::PrimaryKey(op)), .seq = *read.fence};
    // Hot shows a write once it is applied, which precedes its publish.
    if (auto fenced = sequencer_.Fence(std::span(&fence, 1), After(config_.write_timeout));
        !fenced) {
      return std::unexpected(fenced.error());
    }
  }
  return std::move(read.result);
}

core::Result<core::RespValue> ReadPath::Miss(const ops::ReadOp& op) {
  return std::visit(
      [&](const auto& o) -> core::Result<core::RespValue> {
        using T = std::decay_t<decltype(o)>;
        using core::KeyType;
        if constexpr (std::is_same_v<T, ops::Exists> || std::is_same_v<T, ops::Ttl> ||
                      std::is_same_v<T, ops::Type>) {
          const auto key = ops::PrimaryKey(op);
          Loader::Source source = Loader::Source::kBuffer;
          auto probed =
              loader_.Probe(hot_.ShardOf(key), key, After(config_.cold_read_deadline), &source);
          if (!probed.has_value()) return Failed(op, probed.error());
          return Answer(op, *probed, source);
        } else if constexpr (std::is_same_v<T, ops::StringGet>) {
          return Fill(op, KeyType::kString, config_.cold_read_deadline);
        } else if constexpr (std::is_same_v<T, ops::SetIsMember>) {
          return PointRead(op, KeyType::kSet);
        } else if constexpr (std::is_same_v<T, ops::ZsetScore>) {
          return PointRead(op, KeyType::kZset);
        } else if constexpr (std::is_same_v<T, ops::HashGet> ||
                             std::is_same_v<T, ops::HashFieldExists> ||
                             std::is_same_v<T, ops::HashMultiGet>) {
          return PointRead(op, KeyType::kHash);
        } else if constexpr (std::is_same_v<T, ops::SetCard>) {
          return CountRead(op, KeyType::kSet);
        } else if constexpr (std::is_same_v<T, ops::ZsetCard>) {
          return CountRead(op, KeyType::kZset);
        } else if constexpr (std::is_same_v<T, ops::HashLen>) {
          return CountRead(op, KeyType::kHash);
        } else if constexpr (std::is_same_v<T, ops::SetMembers>) {
          return Fill(op, KeyType::kSet, config_.cold_scan_deadline);
        } else if constexpr (std::is_same_v<T, ops::ZsetRange>) {
          return Fill(op, KeyType::kZset, config_.cold_scan_deadline);
        } else {
          return Fill(op, KeyType::kHash, config_.cold_scan_deadline);
        }
      },
      op);
}

core::Result<core::RespValue> ReadPath::Fill(const ops::ReadOp& op, core::KeyType type,
                                             std::chrono::milliseconds budget) {
  const auto key = ops::PrimaryKey(op);
  const auto shard = hot_.ShardOf(key);
  const auto deadline = After(budget);
  if (Admit(key)) {
    auto filled = loader_.Install(key, type, deadline);
    CountFill(filled);
    if (!filled.has_value()) return Failed(op, filled.error());
    if (filled->result.has_value()) return Answer(op, *filled->result, filled->source);
    const bool loaded_here = filled->fill == Loader::Fill::kInstalled;
    if (auto hot = FromHot(op, loaded_here ? std::optional(filled->source) : std::nullopt);
        hot.has_value()) {
      return *std::move(hot);
    }
    // Evicted since, so drained: buffer and cold hold it, and that hot
    // read found no flush floor.
  }
  Loader::Source source = Loader::Source::kBuffer;
  auto loaded = loader_.LoadAs(shard, key, type, deadline, &source);
  if (!loaded.has_value()) return Failed(op, loaded.error());
  return Answer(op, **loaded, source);
}

core::Result<core::RespValue> ReadPath::CountRead(const ops::ReadOp& op, core::KeyType type) {
  const auto key = ops::PrimaryKey(op);
  auto count = loader_.Cardinality(hot_.ShardOf(key), key, type, After(config_.cold_read_deadline));
  if (!count.has_value()) return std::unexpected(count.error());
  if (count->members == 0) {
    misses_.Increment();
  } else {
    CountHit(count->source);
  }
  FillSmall(key, type, count->members);
  return core::RespValue::Integer(static_cast<int64_t>(count->members));
}

void ReadPath::FillSmall(std::string_view key, core::KeyType type, uint64_t members) {
  if (members == 0 || members >= config_.fill_max_members || !Admit(key)) return;
  // A failed fill costs only the next read's cold read.
  CountFill(loader_.Install(key, type, After(config_.cold_scan_deadline)));
}

void ReadPath::CountFill(const core::Result<Loader::Filled>& filled) {
  using metrics::FillOutcome;
  if (!filled.has_value()) {
    fills_.at(static_cast<size_t>(FillOutcome::kFailed)).Increment();
    return;
  }
  switch (filled->fill) {
    case Loader::Fill::kInstalled:
      fills_.at(static_cast<size_t>(FillOutcome::kInstalled)).Increment();
      break;
    case Loader::Fill::kDiscarded:
      fills_.at(static_cast<size_t>(FillOutcome::kDiscarded)).Increment();
      break;
    case Loader::Fill::kSkippedBackpressure:
      fills_.at(static_cast<size_t>(FillOutcome::kSkippedBackpressure)).Increment();
      break;
    case Loader::Fill::kSkippedSize:
      fills_.at(static_cast<size_t>(FillOutcome::kSkippedSize)).Increment();
      break;
    case Loader::Fill::kSkippedEvictCap:
      fills_.at(static_cast<size_t>(FillOutcome::kSkippedEvictCap)).Increment();
      break;
    case Loader::Fill::kResident:
    case Loader::Fill::kFlushed:
      break;
  }
}

core::Result<core::RespValue> ReadPath::PointRead(const ops::ReadOp& op, core::KeyType type) {
  const auto key = ops::PrimaryKey(op);
  const auto shard = hot_.ShardOf(key);
  std::vector<std::string_view> asks;
  std::visit(
      [&asks](const auto& o) {
        using T = std::decay_t<decltype(o)>;
        if constexpr (std::is_same_v<T, ops::SetIsMember> || std::is_same_v<T, ops::ZsetScore>) {
          asks.push_back(o.member);
        } else if constexpr (std::is_same_v<T, ops::HashGet> ||
                             std::is_same_v<T, ops::HashFieldExists>) {
          asks.push_back(o.field);
        } else if constexpr (std::is_same_v<T, ops::HashMultiGet>) {
          asks = o.fields;
        }
      },
      op);
  auto read = loader_.ReadMembers(shard, key, type, asks, After(config_.cold_read_deadline));
  if (!read.has_value()) return std::unexpected(read.error());
  if (read->cardinality == 0) {
    misses_.Increment();
  } else {
    CountHit(read->source);
  }
  FillSmall(key, type, read->cardinality);

  const auto& values = read->values;
  if (std::holds_alternative<ops::HashMultiGet>(op)) {
    std::vector<core::RespValue> out;
    out.reserve(values.size());
    for (const auto& value : values) out.push_back(BulkOrNull(value));
    return core::RespValue::Array(std::move(out));
  }
  if (std::holds_alternative<ops::SetIsMember>(op) ||
      std::holds_alternative<ops::HashFieldExists>(op)) {
    return core::RespValue::Integer(values.front().has_value() ? 1 : 0);
  }
  return BulkOrNull(values.front());
}

core::Result<core::RespValue> ReadPath::Failed(const ops::ReadOp& op, const core::Error& error) {
  if (error.code() == core::ErrorCode::kTimeout && LoadsWholeKey(op)) {
    // Never a truncated answer: the error, and a metric (decision 4).
    cold_scan_deadline_exceeded_.Increment();
  }
  return std::unexpected(error);
}

core::Result<core::RespValue> ReadPath::Answer(const ops::ReadOp& op, const hot::LoadResult& loaded,
                                               Loader::Source source) {
  const int64_t now_ms =
      std::chrono::duration_cast<std::chrono::milliseconds>(config_.wall_clock().time_since_epoch())
          .count();
  return std::visit(
      [&](const auto& state) -> core::Result<core::RespValue> {
        using T = std::decay_t<decltype(state)>;
        if constexpr (std::is_same_v<T, hot::LoadedAbsent>) {
          misses_.Increment();
          return hot::EmptyReadResponse(op);
        } else {
          if (Expired(state.abs_ttl_ms, now_ms)) {
            misses_.Increment();
            return hot::EmptyReadResponse(op);
          }
          CountHit(source);
          if constexpr (std::is_same_v<T, hot::LoadedExists>) {
            return hot::AnswerRead(op, state.type, nullptr, state.abs_ttl_ms, now_ms);
          } else {
            return hot::AnswerRead(op, state.type(), &state.value, state.abs_ttl_ms, now_ms);
          }
        }
      },
      loaded);
}

void ReadPath::CountHit(Loader::Source source) {
  if (source == Loader::Source::kCold) {
    hits_cold_.Increment();
  } else {
    hits_buffer_.Increment();
  }
}

bool ReadPath::Admit(std::string_view key) {
  return doorkeeper_ == nullptr || doorkeeper_->Admit(key);
}

core::SteadyTime ReadPath::After(std::chrono::milliseconds budget) const {
  return core::SteadyClock::now() + budget;
}

}  // namespace abyss::engine
