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

// A collection's fill after its point or count read, on the pool's
// background queue. It holds no command and posts nothing. Install
// takes the key's placeholder before it reads, so however late it
// runs it cannot install what a write has since replaced.
class ReadPath::FillJob final : public IoJob {
 public:
  FillJob(ReadPath& reads, std::string_view key, core::KeyType type, core::SteadyTime deadline)
      : reads_(reads), key_(key), type_(type), deadline_(deadline) {}

  // NOLINTNEXTLINE(bugprone-exception-escape): an OOM may terminate.
  void Run() noexcept override { reads_.CountFill(reads_.loader_.Install(key_, type_, deadline_)); }
  // NOLINTNEXTLINE(bugprone-exception-escape): its outcome is in range.
  void Fail(core::Error error) noexcept override {
    reads_.CountFill(error.code() == core::ErrorCode::kTimeout ? metrics::FillOutcome::kExpired
                                                               : metrics::FillOutcome::kDropped);
  }

 private:
  ReadPath& reads_;
  std::string key_;
  core::KeyType type_;
  core::SteadyTime deadline_;
};

void FanOut::Reset(core::MultiKeyKind fan_kind, std::span<const std::string> fan_keys) {
  kind = fan_kind;
  keys = fan_keys;
  next = 0;
  values.clear();
  count = 0;
  fences.clear();
  error.reset();
}

core::Result<core::RespValue> FanOut::Reply() {
  if (error.has_value()) return std::unexpected(*error);
  if (kind == core::MultiKeyKind::kExists) return core::RespValue::Integer(count);
  return core::RespValue::Array(std::move(values));
}

ReadPath::ReadPath(hot::ShardedHotStore& hot, Loader& loader, IoPool& pool, ReadPathConfig config)
    : hot_(hot), loader_(loader), pool_(pool), config_(std::move(config)) {
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
        FillOutcome::kSkippedSize, FillOutcome::kSkippedEvictCap, FillOutcome::kFailed,
        FillOutcome::kDropped, FillOutcome::kExpired}) {
    fills_.at(static_cast<size_t>(outcome)) = reg.Counter(metrics::names::kHotFillsTotal, outcome);
  }
}

std::optional<ReadAnswer> ReadPath::TryHot(const ops::ReadOp& op) { return FromHot(op); }

metrics::IoKind ReadPath::KindOf(const ops::ReadOp& op) {
  return LoadsWholeKey(op) ? metrics::IoKind::kScan : metrics::IoKind::kPoint;
}

void ReadPath::FanOutHot(FanOut& fan) {
  while (!fan.done()) {
    const std::string& key = fan.keys[fan.next];
    std::optional<ReadAnswer> hit;
    if (fan.kind == core::MultiKeyKind::kExists) {
      hit = TryHot(ops::ReadOp{ops::Exists{.keys = {key}}});
    } else {
      hit = TryHot(ops::ReadOp{ops::StringGet{.key = key}});
    }
    if (!hit.has_value()) return;
    Fold(fan, *std::move(hit));
  }
}

void ReadPath::FanOutMiss(FanOut& fan, core::SteadyTime deadline) {
  bool more = !fan.done();
  while (more) {
    const std::string& key = fan.keys[fan.next];
    ops::ReadOp op;
    if (fan.kind == core::MultiKeyKind::kExists) {
      op = ops::Exists{.keys = {key}};
    } else {
      op = ops::StringGet{.key = key};
    }
    std::optional<ReadAnswer> answer = TryHot(op);
    if (!answer.has_value()) answer = Miss(op, deadline);
    more = Fold(fan, *std::move(answer)) && !fan.done();
  }
}

bool ReadPath::Fold(FanOut& fan, ReadAnswer answer) {
  if (answer.fence.has_value()) fan.fences.push_back(*answer.fence);
  ++fan.next;
  if (!answer.reply.has_value()) {
    // Redis MGET answers nil for a key of another type.
    if (fan.kind == core::MultiKeyKind::kMget &&
        answer.reply.error().code() == core::ErrorCode::kWrongType) {
      fan.values.push_back(core::RespValue::Null());
      return true;
    }
    fan.error = answer.reply.error();
    return false;
  }
  if (fan.kind == core::MultiKeyKind::kExists) {
    // EXISTS k k counts twice (Redis behaviour): no dedup.
    fan.count += answer.reply->AsInteger();
  } else {
    fan.values.push_back(*std::move(answer.reply));
  }
  return true;
}

std::optional<ReadAnswer> ReadPath::FromHot(const ops::ReadOp& op,
                                            std::optional<Loader::Source> filled_from) {
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
  return ReadAnswer{.reply = std::move(read.result), .fence = FenceOf(op, read.fence)};
}

// Hot shows a write once it is applied, which precedes its publish.
std::optional<ShardSeq> ReadPath::FenceOf(const ops::ReadOp& op,
                                          std::optional<core::SequenceId> seq) const {
  if (!seq.has_value()) return std::nullopt;
  return ShardSeq{.shard = hot_.ShardOf(ops::PrimaryKey(op)), .seq = *seq};
}

ReadAnswer ReadPath::Miss(const ops::ReadOp& op, core::SteadyTime deadline) {
  return std::visit(
      [&](const auto& o) -> ReadAnswer {
        using T = std::decay_t<decltype(o)>;
        using core::KeyType;
        if constexpr (std::is_same_v<T, ops::Exists> || std::is_same_v<T, ops::Ttl> ||
                      std::is_same_v<T, ops::Type>) {
          const auto key = ops::PrimaryKey(op);
          Loader::Source source = Loader::Source::kBuffer;
          auto probed = loader_.Probe(hot_.ShardOf(key), key, deadline, &source);
          if (!probed.has_value()) return {.reply = Failed(op, probed.error())};
          return {.reply = Answer(op, *probed, source)};
        } else if constexpr (std::is_same_v<T, ops::StringGet>) {
          return Fill(op, KeyType::kString, deadline);
        } else if constexpr (std::is_same_v<T, ops::SetIsMember>) {
          return {.reply = PointRead(op, KeyType::kSet, deadline)};
        } else if constexpr (std::is_same_v<T, ops::ZsetScore>) {
          return {.reply = PointRead(op, KeyType::kZset, deadline)};
        } else if constexpr (std::is_same_v<T, ops::HashGet> ||
                             std::is_same_v<T, ops::HashFieldExists> ||
                             std::is_same_v<T, ops::HashMultiGet>) {
          return {.reply = PointRead(op, KeyType::kHash, deadline)};
        } else if constexpr (std::is_same_v<T, ops::SetCard>) {
          return {.reply = CountRead(op, KeyType::kSet, deadline)};
        } else if constexpr (std::is_same_v<T, ops::ZsetCard>) {
          return {.reply = CountRead(op, KeyType::kZset, deadline)};
        } else if constexpr (std::is_same_v<T, ops::HashLen>) {
          return {.reply = CountRead(op, KeyType::kHash, deadline)};
        } else if constexpr (std::is_same_v<T, ops::SetMembers>) {
          return Fill(op, KeyType::kSet, deadline);
        } else if constexpr (std::is_same_v<T, ops::ZsetRange>) {
          return Fill(op, KeyType::kZset, deadline);
        } else {
          return Fill(op, KeyType::kHash, deadline);
        }
      },
      op);
}

ReadAnswer ReadPath::Fill(const ops::ReadOp& op, core::KeyType type, core::SteadyTime deadline) {
  const auto key = ops::PrimaryKey(op);
  const auto shard = hot_.ShardOf(key);
  if (Admit(key)) {
    auto filled = loader_.Install(key, type, deadline);
    CountFill(filled);
    if (!filled.has_value()) return {.reply = Failed(op, filled.error())};
    if (filled->result.has_value()) {
      return {.reply = Answer(op, *filled->result, filled->source)};
    }
    if (filled->fill != Loader::Fill::kPending) {
      const bool loaded_here = filled->fill == Loader::Fill::kInstalled;
      if (auto hot = FromHot(op, loaded_here ? std::optional(filled->source) : std::nullopt);
          hot.has_value()) {
        return *std::move(hot);
      }
    }
    // Pending, or evicted since and so drained: buffer and cold hold
    // it, and no hot read found a flush floor.
  }
  Loader::Source source = Loader::Source::kBuffer;
  auto loaded = loader_.LoadAs(shard, key, type, deadline, &source);
  if (!loaded.has_value()) return {.reply = Failed(op, loaded.error())};
  return {.reply = Answer(op, **loaded, source)};
}

core::Result<core::RespValue> ReadPath::CountRead(const ops::ReadOp& op, core::KeyType type,
                                                  core::SteadyTime deadline) {
  const auto key = ops::PrimaryKey(op);
  auto count = loader_.Cardinality(hot_.ShardOf(key), key, type, deadline);
  if (!count.has_value()) return std::unexpected(count.error());
  if (count->members == 0) {
    misses_.Increment();
  } else {
    CountHit(count->source);
  }
  FillSmall(key, type, count->members, deadline);
  return core::RespValue::Integer(static_cast<int64_t>(count->members));
}

void ReadPath::FillSmall(std::string_view key, core::KeyType type, uint64_t members,
                         core::SteadyTime deadline) {
  if (members == 0 || members >= config_.fill_max_members || !Admit(key)) return;
  // A fill that never runs costs only the next read's cold read.
  pool_.SubmitBackground(std::make_unique<FillJob>(*this, key, type, deadline), deadline);
}

void ReadPath::CountFill(const core::Result<Loader::Filled>& filled) {
  using metrics::FillOutcome;
  if (!filled.has_value()) {
    CountFill(FillOutcome::kFailed);
    return;
  }
  switch (filled->fill) {
    case Loader::Fill::kInstalled:
      CountFill(FillOutcome::kInstalled);
      break;
    case Loader::Fill::kDiscarded:
      CountFill(FillOutcome::kDiscarded);
      break;
    case Loader::Fill::kSkippedBackpressure:
      CountFill(FillOutcome::kSkippedBackpressure);
      break;
    case Loader::Fill::kSkippedSize:
      CountFill(FillOutcome::kSkippedSize);
      break;
    case Loader::Fill::kSkippedEvictCap:
      CountFill(FillOutcome::kSkippedEvictCap);
      break;
    case Loader::Fill::kResident:
    case Loader::Fill::kFlushed:
    case Loader::Fill::kPending:
      break;
  }
}

void ReadPath::CountFill(metrics::FillOutcome outcome) {
  fills_.at(static_cast<size_t>(outcome)).Increment();
}

core::Result<core::RespValue> ReadPath::PointRead(const ops::ReadOp& op, core::KeyType type,
                                                  core::SteadyTime deadline) {
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
  auto read = loader_.ReadMembers(shard, key, type, asks, deadline);
  if (!read.has_value()) return std::unexpected(read.error());
  if (read->cardinality == 0) {
    misses_.Increment();
  } else {
    CountHit(read->source);
  }
  FillSmall(key, type, read->cardinality, deadline);

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

}  // namespace abyss::engine
