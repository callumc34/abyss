#include "abyss/core/durable_wait.h"

#include <algorithm>
#include <memory>
#include <string>
#include <string_view>
#include <utility>

#include "abyss/core/fatal.h"
#include "abyss/core/queue.h"
#include "abyss/log/log.h"

ABYSS_LOG_COMPONENT("abyss.core.durable_wait")

namespace abyss::core {

namespace {

constexpr std::string_view kWriteDurableTimeout =
    "write durable wait exceeded server timeout; the write is applied and may yet become durable";
constexpr std::string_view kFlushDurableTimeout =
    "flush durable wait exceeded server timeout; retry to complete the wipe";
constexpr std::string_view kFenceTimeout =
    "durable wait exceeded server timeout; the reply would show a write not yet durable";

}  // namespace

DurableWait::~DurableWait() {
  ABYSS_DCHECK(Quiescent(), "a durable wait freed with a node linked or a post on its way");
}

void DurableWait::Begin(Queue& queue, Kind kind, Completion completion, size_t streams,
                        Result<RespValue> reply, Timer* timer) {
  const bool idle = Quiescent() && !waiting_;
  ABYSS_DCHECK(idle, "a durable wait begun while another is open");
  // The last wait's nodes are all unlinked by now.
  for (size_t i = 0; i < added_; ++i) At(i).node.reset();
  // Nodes for a FLUSHDB over many shards are not all kept after it.
  constexpr size_t kKeptStreams = 64;
  const size_t keep = std::max(streams, kKeptStreams) - 1;
  if (more_.size() > keep) more_.resize(keep);
  while (1 + more_.size() < streams) more_.push_back(std::make_unique<Stream>());
  queue_ = &queue;
  ack_ = queue.AckDurability();
  kind_ = kind;
  completion_ = completion;
  timer_ = timer;
  if (timer_ != nullptr) began_ = std::chrono::steady_clock::now();
  reply_ = std::move(reply);
  error_.reset();
  waiting_ = true;
  added_ = 0;
}

void DurableWait::Add(ShardId shard, SequenceId seq) {
  const bool room = waiting_ && added_ < 1 + more_.size();
  ABYSS_DCHECK(room, "a durable wait added to past its streams");
  if (error_.has_value()) return;
  Stream& stream = At(added_++);
  stream.shard = shard;
  SeqWaiterNode& node = stream.node.emplace(seq, completion_);
  const std::optional<Result<void>> now = queue_->WhenDurable(shard, ack_, node);
  if (!now.has_value()) {
    ++outstanding_;
    return;
  }
  if (!now->has_value()) error_ = now->error();
}

std::optional<Result<RespValue>> DurableWait::Ready() {
  ABYSS_DCHECK(waiting_, "a durable wait asked for its reply twice");
  if (error_.has_value()) {
    Error error = *error_;
    CancelRest();
    return Failed(std::move(error));
  }
  if (outstanding_ > 0) return std::nullopt;
  return Release();
}

std::optional<Result<RespValue>> DurableWait::Arrived(Result<void> result) {
  ABYSS_DCHECK(outstanding_ > 0, "a kDurable for a wait with nothing registered");
  --outstanding_;
  if (!waiting_) return std::nullopt;
  if (!result.has_value()) {
    CancelRest();
    return Failed(result.error());
  }
  if (outstanding_ > 0) return std::nullopt;
  return Release();
}

Result<RespValue> DurableWait::Expire() {
  ABYSS_DCHECK(waiting_, "a durable wait expired while not waiting");
  waiting_ = false;
  reply_ = {};
  const Stream& first = At(0);
  const SequenceId seq = first.node.has_value() ? first.node->seq() : 0;
  CancelRest();
  if (kind_ == Kind::kFence) {
    ABYSS_LOG_WARN("fence durable wait timeout", {"shard", static_cast<int64_t>(first.shard)},
                   {"seq", static_cast<uint64_t>(seq)});
    return std::unexpected(Error{ErrorCode::kTimeout, std::string(kFenceTimeout)});
  }
  const bool flush = kind_ == Kind::kFlush;
  ABYSS_LOG_WARN(flush ? "flush durable wait timeout" : "write durable wait timeout",
                 {"shard", static_cast<int64_t>(first.shard)}, {"seq", static_cast<uint64_t>(seq)},
                 {"streams", static_cast<uint64_t>(added_)});
  return std::unexpected(
      Error{ErrorCode::kTimeout, std::string(flush ? kFlushDurableTimeout : kWriteDurableTimeout)});
}

void DurableWait::Abandon() {
  ABYSS_DCHECK(waiting_, "a durable wait abandoned while not waiting");
  waiting_ = false;
  reply_ = {};
  CancelRest();
}

void DurableWait::CancelRest() {
  for (size_t i = 0; i < added_; ++i) {
    Stream& stream = At(i);
    if (!stream.node.has_value()) continue;
    if (queue_->CancelDurable(stream.shard, ack_, *stream.node)) {
      --outstanding_;
      stream.node.reset();
    }
  }
}

Result<RespValue> DurableWait::Release() {
  waiting_ = false;
  if (timer_ != nullptr) timer_->Waited(std::chrono::steady_clock::now() - began_);
  return std::move(reply_);
}

Result<RespValue> DurableWait::Failed(Error error) {
  waiting_ = false;
  reply_ = {};
  if (kind_ != Kind::kFence) {
    const Stream& first = At(0);
    ABYSS_LOG_ERROR(kind_ == Kind::kFlush ? "flush durable failed" : "write durable failed",
                    {"shard", static_cast<int64_t>(first.shard)},
                    {"seq", static_cast<uint64_t>(first.node.has_value() ? first.node->seq() : 0)},
                    {"err", std::string_view{error.message()}});
  }
  return std::unexpected(std::move(error));
}

}  // namespace abyss::core
