#include "abyss/queue/pending_append.h"

#include <utility>

namespace abyss::queue {

PendingAppend::PendingAppend(core::SequenceId seq, DurabilityFuture durable,
                             std::unique_ptr<AppendPublisher> publisher)
    : seq_(seq), durable_(std::move(durable)), publisher_(std::move(publisher)) {}

PendingAppend::~PendingAppend() { Publish(); }

void PendingAppend::Publish() noexcept {
  if (publisher_) {
    publisher_->Publish();
    publisher_.reset();
  }
}

PendingBatchAppend::PendingBatchAppend(core::SequenceId first_seq, core::SequenceId last_seq,
                                       DurabilityFuture durable,
                                       std::unique_ptr<AppendPublisher> publisher)
    : first_seq_(first_seq),
      last_seq_(last_seq),
      durable_(std::move(durable)),
      publisher_(std::move(publisher)) {}

PendingBatchAppend::~PendingBatchAppend() { Publish(); }

void PendingBatchAppend::Publish() noexcept {
  if (publisher_) {
    publisher_->Publish();
    publisher_.reset();
  }
}

}  // namespace abyss::queue
