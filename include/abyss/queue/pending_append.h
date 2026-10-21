#pragma once

#include <cstddef>
#include <memory>

#include "abyss/core/types.h"
#include "abyss/queue/append_result.h"

namespace abyss::queue {

class AppendPublisher {
 public:
  AppendPublisher() = default;
  virtual ~AppendPublisher() = default;

  AppendPublisher(const AppendPublisher&) = delete;
  AppendPublisher& operator=(const AppendPublisher&) = delete;
  AppendPublisher(AppendPublisher&&) = delete;
  AppendPublisher& operator=(AppendPublisher&&) = delete;

  virtual void Publish() noexcept = 0;
};

class PendingAppend {
 public:
  PendingAppend(core::SequenceId seq, DurabilityFuture durable,
                std::unique_ptr<AppendPublisher> publisher);

  ~PendingAppend();

  PendingAppend(PendingAppend&&) noexcept = default;
  PendingAppend& operator=(PendingAppend&&) noexcept = default;
  PendingAppend(const PendingAppend&) = delete;
  PendingAppend& operator=(const PendingAppend&) = delete;

  core::SequenceId seq() const noexcept { return seq_; }
  // Resolves once the entry is durable at the queue's AckDurability().
  DurabilityFuture& durable() noexcept { return durable_; }

  // Idempotent after the first call; destructor auto-calls if still pending.
  void Publish() noexcept;

 private:
  core::SequenceId seq_ = 0;
  DurabilityFuture durable_;
  std::unique_ptr<AppendPublisher> publisher_;  // null after Publish()
};

class PendingBatchAppend {
 public:
  PendingBatchAppend(core::SequenceId first_seq, core::SequenceId last_seq,
                     DurabilityFuture durable, std::unique_ptr<AppendPublisher> publisher);

  ~PendingBatchAppend();

  PendingBatchAppend(PendingBatchAppend&&) noexcept = default;
  PendingBatchAppend& operator=(PendingBatchAppend&&) noexcept = default;
  PendingBatchAppend(const PendingBatchAppend&) = delete;
  PendingBatchAppend& operator=(const PendingBatchAppend&) = delete;

  core::SequenceId first_seq() const noexcept { return first_seq_; }
  core::SequenceId last_seq() const noexcept { return last_seq_; }
  size_t size() const noexcept { return static_cast<size_t>(last_seq_ - first_seq_) + 1U; }
  core::SequenceId seq_at(size_t index) const noexcept {
    return first_seq_ + static_cast<core::SequenceId>(index);
  }

  DurabilityFuture& durable() noexcept { return durable_; }

  void Publish() noexcept;

 private:
  core::SequenceId first_seq_ = 0;
  core::SequenceId last_seq_ = 0;
  DurabilityFuture durable_;
  std::unique_ptr<AppendPublisher> publisher_;
};

}  // namespace abyss::queue
