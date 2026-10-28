#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "abyss/core/command_dispatcher.h"
#include "abyss/core/completion.h"
#include "abyss/core/resp_types.h"
#include "abyss/core/timer_wheel.h"
#include "abyss/core/types.h"
#include "abyss/metrics/names.h"
#include "abyss/resp/command_registry.h"

namespace abyss::resp {

class ConfigProvider;
class LoadingStateProvider;
class NodeIdentity;
class RespMetrics;
class ServerStatsProvider;

struct ConnectionState {
  uint64_t client_id = 0;
  std::string client_name;
  uint8_t protocol_version = 2;
};

// Non-owning. A null required dependency surfaces as ERR internal server error
// (logged at ERROR); see RequestPipeline::InternalServerError.
struct PipelineDependencies {
  core::CommandDispatcher* dispatcher = nullptr;
  const LoadingStateProvider* loading = nullptr;
  const ServerStatsProvider* stats = nullptr;
  const ConfigProvider* config = nullptr;
  const NodeIdentity* identity = nullptr;
  RespMetrics* metrics = nullptr;
};

// The reactor's, which its pipelines share: where the engine's events
// for them go, and the wheel their deadlines are kept on.
struct PipelineHost {
  core::CompletionSink* sink = nullptr;
  core::TimerWheel* timers = nullptr;
};

struct PipelineLimits {
  // Commands in flight, dispatched and not yet written.
  size_t max_inflight = 1024;
  // Dispatch stops while the connection's unwritten bytes, with the
  // replies held here, reach this.
  size_t reply_bytes = 4194304;
};

// A pending command's task that its deadline abandoned while something
// of it was still out, with its command's slot: kept until its last
// event has landed.
struct OrphanTask {
  core::Slot slot;
  std::unique_ptr<core::Task> task;
};

// One connection's ordered dispatcher; not thread-safe, and every call
// is on the connection's reactor. Commands dispatch in order, each once
// every earlier one has reached its linearisation point: none while one
// is pending. Later ones run while earlier ones wait to be durable, and
// replies leave in command order.
class RequestPipeline {
 public:
  using Dependencies = PipelineDependencies;

  enum class CloseRequest : uint8_t { kNone, kClientQuit, kProtocolError };

  struct FeedResult {
    size_t consumed = 0;
    // Stopped with input left that it may not dispatch yet.
    bool blocked = false;
  };

  RequestPipeline(const CommandRegistry& registry, ConnectionState state,
                  PipelineDependencies deps = {}, PipelineHost host = {},
                  PipelineLimits limits = {});
  ~RequestPipeline();

  RequestPipeline(const RequestPipeline&) = delete;
  RequestPipeline& operator=(const RequestPipeline&) = delete;
  RequestPipeline(RequestPipeline&&) = delete;
  RequestPipeline& operator=(RequestPipeline&&) = delete;

  // Parses and dispatches commands from `input` while none is pending,
  // fewer than the cap are in flight, and `unwritten` with the replies
  // held here is under the reply-byte limit.
  FeedResult Feed(std::span<const uint8_t> input, size_t unwritten);
  // Appends the replies ready at the head, in command order.
  void Drain(std::vector<uint8_t>& output);
  // An engine event: true if it was for this pipeline's task, or for a
  // slot still owed it.
  bool OnEvent(const core::CompletionEvent& event);
  // Ends each command waiting past its deadline at `now`. A pending
  // one's task still busy once abandoned is handed back, to be kept
  // until it settles; a closed pipeline keeps it itself.
  std::unique_ptr<OrphanTask> Expire(core::SteadyTime now);
  // The deadline of the oldest command still waiting on the engine.
  std::optional<core::SteadyTime> NextDeadline() const;
  // Dispatches nothing more: the server is stopping.
  void StopDispatch();
  // The connection has gone. A pending command runs on to its outcome
  // or its deadline, and every reply is dropped; only events land.
  void Close();

  // No task busy and no slot owed a post: safe to free.
  bool Quiescent() const;
  // Dispatched and not yet written.
  size_t InFlight() const { return tail_ - drain_; }
  // Slots in use: in flight, or written and still owed a post.
  size_t Slots() const { return tail_ - head_; }
  // Replies held here, ready or waiting to be durable, as encoded.
  size_t ReplyBytes() const { return reply_bytes_; }
  CloseRequest close_request() const { return close_request_; }
  bool closed() const { return closed_; }
  uint64_t client_id() const { return state_.client_id; }
  const ConnectionState& state() const { return state_; }
  size_t RingCapacityForTesting() const { return ring_.size(); }
  // What one slot holds idle, its pointer in the ring included.
  static size_t EntryBytesForTesting();

 private:
  struct Entry;
  struct Immediate {
    core::RespValue response;
    metrics::RequestStatus status;
    std::string_view cmd_name;
  };

  bool CanDispatch(size_t unwritten) const;
  Entry& Acquire();
  void Grow();
  void Reclaim();
  void SyncTimer();
  Entry& At(uint64_t position) const { return *ring_[position % ring_.size()]; }

  void DispatchCommand(core::RespCommand cmd);
  // nullopt once handed to the engine; otherwise the reply now.
  std::optional<Immediate> DispatchResolved(Entry& entry, const ResolvedCommand& resolved,
                                            core::RespCommand cmd);
  void Submit(Entry& entry, core::Request request, std::string_view what);
  void Settle(Entry& entry, core::Outcome outcome);
  // An engine reply for `entry`.
  void Reply(Entry& entry, core::Result<core::RespValue> result);
  void Finish(Entry& entry, core::RespValue response, metrics::RequestStatus status,
              std::string_view cmd_name);
  void Store(Entry& entry, core::RespValue response);
  // Written, or dropped: its reply goes, and its slot waits only on
  // what is still to post to it.
  void Release(Entry& entry);
  // A closed pipeline's reply, which no one hears.
  void Drop(Entry& entry);
  bool TaskBusy() const { return task_ != nullptr && !task_->Quiescent(); }

  Immediate HandleAdminStateless(const ResolvedCommand& resolved, const core::RespCommand& cmd);
  core::RespValue HandlePing(const core::RespCommand& cmd);
  core::RespValue HandleEcho(const core::RespCommand& cmd);
  Immediate HandleHello(const CommandSpec& spec, const core::RespCommand& cmd);
  core::RespValue HandleQuit();
  core::RespValue HandleTime();
  core::RespValue HandleClient(std::string_view subcommand, const core::RespCommand& cmd);
  core::RespValue HandleReset();

  // Logs `context` at ERROR; returns ERR internal server error to the wire so
  // dependency identity isn't leaked to clients.
  core::RespValue InternalServerError(std::string_view context, std::string_view cmd_name);

  static core::RespValue MakeLoading();
  static core::RespValue MakeUnknownCommand(std::string_view name, std::string_view first_arg);
  static core::RespValue MakeArityError(const ResolvedCommand& resolved);
  static core::RespValue MakeUnknownSubcommand(const CommandSpec& parent, std::string_view raw_sub);

  const CommandRegistry& registry_;
  Dependencies deps_;
  PipelineHost host_;
  PipelineLimits limits_;
  ConnectionState state_;
  CloseRequest close_request_ = CloseRequest::kNone;
  bool stopped_ = false;
  bool closed_ = false;

  // Declared in the order they must outlive one another.
  core::TimerNode timer_;
  // Heap entries that never move; the ring holds them by pointer. A
  // command's seq is its position + 1. [head_, drain_) are written and
  // owed a post; [drain_, tail_) are in flight.
  std::vector<std::unique_ptr<Entry>> ring_;
  uint64_t head_ = 0;
  uint64_t drain_ = 0;
  uint64_t tail_ = 0;
  size_t reply_bytes_ = 0;
  // The command the task works for while it is busy; 0 when idle.
  uint64_t task_seq_ = 0;
  std::unique_ptr<core::Task> task_;
};

}  // namespace abyss::resp
