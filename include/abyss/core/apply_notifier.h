#pragma once

#include <cstddef>
#include <cstdint>
#include <future>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <vector>

#include "abyss/core/thread_annotations.h"
#include "abyss/core/types.h"

namespace abyss::core {

struct ApplyNotifierConfig {
  uint32_t registry_shard_count = 16;
};

// Hot signals "applied at seq"; the resolver awaits before fulfilling the
// client RPC. Required for read-your-write on conditional writes.
class ApplyNotifier {
 public:
  explicit ApplyNotifier(ApplyNotifierConfig config = {});
  ~ApplyNotifier() = default;

  ApplyNotifier(const ApplyNotifier&) = delete;
  ApplyNotifier& operator=(const ApplyNotifier&) = delete;
  ApplyNotifier(ApplyNotifier&&) = delete;
  ApplyNotifier& operator=(ApplyNotifier&&) = delete;

  // Late Await (after Notify) returns ready within a bounded recent-seq window;
  // older seqs return a broken future.
  std::future<void> AwaitApplied(SequenceId seq);
  void NotifyApplied(SequenceId seq);
  bool Cancel(SequenceId seq);

  size_t PendingCount() const;

 private:
  struct Shard {
    mutable std::mutex mu;
    std::unordered_map<SequenceId, std::promise<void>> pending ABYSS_GUARDED_BY(mu);
    std::vector<SequenceId> recent_fired ABYSS_GUARDED_BY(mu);
    size_t recent_cursor ABYSS_GUARDED_BY(mu) = 0;
  };

  Shard& ShardFor(SequenceId seq) const;

  std::vector<std::unique_ptr<Shard>> shards_;
  size_t late_window_size_per_shard_ = 64;
};

}  // namespace abyss::core
