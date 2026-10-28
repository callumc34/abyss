#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <thread>
#include <unordered_map>

#include "abyss/core/completion.h"
#include "abyss/core/types.h"
#include "abyss/metrics/metrics.h"
#include "abyss/resp/request_pipeline.h"

namespace abyss::resp {

// What a reactor's connections leave owed a post: a task whose
// deadline abandoned it while a job or a wake of it was still out,
// keyed by its client and slot; and a closed connection's pipeline,
// keyed by its client. Each stays until it is quiescent, so nothing
// the engine still refers to is freed. One thread, the reactor's.
class Orphans {
 public:
  explicit Orphans(metrics::GaugeHandle gauge = {});
  ~Orphans();

  Orphans(const Orphans&) = delete;
  Orphans& operator=(const Orphans&) = delete;
  Orphans(Orphans&&) = delete;
  Orphans& operator=(Orphans&&) = delete;

  void Adopt(std::unique_ptr<OrphanTask> task);
  // A closed pipeline.
  void Adopt(std::unique_ptr<RequestPipeline> pipeline);

  // An engine event, to the first that is owed it: `live`, the client's
  // open pipeline if any (its task, if the seq is the task's, or the
  // slot still owed it); the task orphaned at its client and slot; the
  // client's orphaned pipeline. False: nothing is owed it.
  bool Route(const core::CompletionEvent& event, RequestPipeline* live);
  // An orphaned pipeline's deadline at `now`.
  void Expire(uint64_t client_id, core::SteadyTime now);

  size_t size() const { return tasks_.size() + pipelines_.size(); }
  bool empty() const { return tasks_.empty() && pipelines_.empty(); }

 private:
  struct Key {
    uint64_t client_id = 0;
    uint64_t slot_seq = 0;
    bool operator==(const Key&) const = default;
  };
  struct KeyHash {
    size_t operator()(const Key& key) const noexcept;
  };

  void OnOwner();

  std::unordered_map<Key, std::unique_ptr<OrphanTask>, KeyHash> tasks_;
  std::unordered_map<uint64_t, std::unique_ptr<RequestPipeline>> pipelines_;
  metrics::GaugeHandle gauge_;
  std::thread::id owner_;
};

}  // namespace abyss::resp
