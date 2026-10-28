#pragma once

#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "abyss/core/completion.h"
#include "abyss/core/resp_types.h"
#include "abyss/core/types.h"
#include "abyss/resp/command_registry.h"
#include "abyss/resp/orphans.h"
#include "abyss/resp/parser.h"
#include "abyss/resp/request_pipeline.h"
#include "abyss/resp/serializer.h"
#include "event_inbox.h"

namespace abyss::testing {

// One connection's pipeline, driven on the calling thread as its
// reactor would drive it: its events, deadlines and orphans are here.
// Dispatch runs one command to its reply, then on until nothing of it
// is owed a post. One thread at a time.
class SyncPipeline {
 public:
  SyncPipeline(const resp::CommandRegistry& registry, resp::ConnectionState state,
               resp::PipelineDependencies deps, resp::PipelineLimits limits = {})
      : pipeline_(std::make_unique<resp::RequestPipeline>(
            registry, std::move(state), deps, resp::PipelineHost{.sink = &inbox_}, limits)) {}

  SyncPipeline(const SyncPipeline&) = delete;
  SyncPipeline& operator=(const SyncPipeline&) = delete;
  SyncPipeline(SyncPipeline&&) = delete;
  SyncPipeline& operator=(SyncPipeline&&) = delete;
  ~SyncPipeline() = default;

  core::RespValue Dispatch(const core::RespCommand& cmd) {
    const std::vector<uint8_t> bytes = resp::Serializer::SerializeCommand(cmd);
    const auto fed = pipeline_->Feed(bytes, 0);
    if (fed.consumed != bytes.size()) {
      return core::RespValue::Error(core::ErrorPrefix::kErr, "the pipeline took no command");
    }
    std::vector<uint8_t> out;
    pipeline_->Drain(out);
    while (out.empty() || !pipeline_->Quiescent() || !orphans_.empty()) {
      const bool replied = !out.empty();
      std::optional<core::CompletionEvent> event =
          inbox_.Take(replied ? std::nullopt : pipeline_->NextDeadline());
      if (!event.has_value()) {
        if (auto orphan = pipeline_->Expire(core::SteadyClock::now())) {
          orphans_.Adopt(std::move(orphan));
        }
      } else {
        (void)orphans_.Route(*event, pipeline_.get());
      }
      pipeline_->Drain(out);
    }
    auto parsed = resp::Parser::Parse(out);
    if (!parsed.has_value()) {
      return core::RespValue::Error(core::ErrorPrefix::kErr,
                                    "unparsable reply: " + std::string(parsed.error().message()));
    }
    return std::move(parsed->value);
  }

  resp::RequestPipeline& pipeline() { return *pipeline_; }
  EventInbox& inbox() { return inbox_; }

 private:
  // Declared in the order they must outlive one another.
  EventInbox inbox_;
  resp::Orphans orphans_;
  std::unique_ptr<resp::RequestPipeline> pipeline_;
};

}  // namespace abyss::testing
