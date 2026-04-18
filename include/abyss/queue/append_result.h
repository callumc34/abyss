#pragma once

#include <future>

#include "abyss/core/result.h"
#include "abyss/core/types.h"

namespace abyss::queue {

using DurabilityFuture = std::future<core::Result<void>>;

struct AppendResult {
  core::SequenceId seq = 0;
  DurabilityFuture durable;
};

struct AppendBatchResult {
  core::SequenceId first_seq = 0;
  core::SequenceId last_seq = 0;
  DurabilityFuture durable;
};

}  // namespace abyss::queue
