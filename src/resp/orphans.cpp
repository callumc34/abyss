#include "abyss/resp/orphans.h"

#include <functional>
#include <utility>

#include "abyss/core/fatal.h"

namespace abyss::resp {

size_t Orphans::KeyHash::operator()(const Key& key) const noexcept {
  const size_t client = std::hash<uint64_t>{}(key.client_id);
  return client ^ (std::hash<uint64_t>{}(key.slot_seq) + 0x9e3779b97f4a7c15ULL + (client << 6U) +
                   (client >> 2U));
}

Orphans::Orphans(metrics::GaugeHandle gauge) : gauge_(gauge) {}

Orphans::~Orphans() { ABYSS_DCHECK(empty(), "orphans freed while still owed a post"); }

void Orphans::Adopt(std::unique_ptr<OrphanTask> task) {
  OnOwner();
  if (task->task->Quiescent()) return;
  const Key key{.client_id = task->slot.completion.client_id,
                .slot_seq = task->slot.completion.slot_seq};
  const bool adopted = tasks_.try_emplace(key, std::move(task)).second;
  ABYSS_DCHECK(adopted, "a slot orphaned twice");
  gauge_.Increment();
}

void Orphans::Adopt(std::unique_ptr<RequestPipeline> pipeline) {
  OnOwner();
  ABYSS_DCHECK(pipeline->closed(), "an open pipeline orphaned");
  if (pipeline->Quiescent()) return;
  const uint64_t client_id = pipeline->client_id();
  const bool adopted = pipelines_.try_emplace(client_id, std::move(pipeline)).second;
  ABYSS_DCHECK(adopted, "a client's pipeline orphaned twice");
  gauge_.Increment();
}

bool Orphans::Route(const core::CompletionEvent& event, RequestPipeline* live) {
  OnOwner();
  if (live != nullptr && live->OnEvent(event)) return true;
  if (event.kind == core::CompletionKind::kResume) {
    const auto task = tasks_.find(Key{.client_id = event.client_id, .slot_seq = event.slot_seq});
    if (task != tasks_.end()) {
      OrphanTask& orphan = *task->second;
      // Abandoned: it only cleans up, and its outcome is no one's.
      (void)orphan.task->Run(orphan.slot, event.result);
      if (orphan.task->Quiescent()) {
        tasks_.erase(task);
        gauge_.Decrement();
      }
      return true;
    }
  }
  const auto pipeline = pipelines_.find(event.client_id);
  if (pipeline == pipelines_.end() || !pipeline->second->OnEvent(event)) return false;
  if (pipeline->second->Quiescent()) {
    pipelines_.erase(pipeline);
    gauge_.Decrement();
  }
  return true;
}

void Orphans::Expire(uint64_t client_id, core::SteadyTime now) {
  OnOwner();
  const auto pipeline = pipelines_.find(client_id);
  if (pipeline == pipelines_.end()) return;
  (void)pipeline->second->Expire(now);
  if (pipeline->second->Quiescent()) {
    pipelines_.erase(pipeline);
    gauge_.Decrement();
  }
}

void Orphans::OnOwner() {
  const std::thread::id self = std::this_thread::get_id();
  if (owner_ == std::thread::id{}) owner_ = self;
  ABYSS_DCHECK(owner_ == self, "an orphan touched off its reactor's thread");
}

}  // namespace abyss::resp
