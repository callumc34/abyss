#pragma once

#include <future>
#include <mutex>
#include <unordered_map>

#include "abyss/core/result.h"
#include "abyss/core/thread_annotations.h"
#include "abyss/core/types.h"

namespace abyss::engine {

class WritePromiseMap {
 public:
  std::future<core::Result<void>> Register(core::SequenceId seq) ABYSS_EXCLUDES(mutex_);
  void Fulfill(core::SequenceId seq, core::Result<void> result) ABYSS_EXCLUDES(mutex_);

 private:
  std::mutex mutex_;
  std::unordered_map<core::SequenceId, std::promise<core::Result<void>>> pending_
      ABYSS_GUARDED_BY(mutex_);
};

}  // namespace abyss::engine
