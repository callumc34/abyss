#include "abyss/queue/reservation.h"

#include <string>
#include <utility>

#include "abyss/core/fatal.h"

namespace abyss::queue {

namespace {

thread_local uint32_t t_held = 0;

[[noreturn]] void Dropped(const std::vector<ReservedRange>& ranges) {
  --t_held;
  std::string what;
  for (const ReservedRange& range : ranges) {
    what += " shard " + std::to_string(range.shard) + " seqs " + std::to_string(range.first) + "-" +
            std::to_string(range.last);
  }
  core::Fatal("WAL reservation dropped without Complete; it stalls its log:" + what);
}

}  // namespace

Reservation::Reservation(std::vector<ReservedRange> ranges, DurableFutures durable,
                         std::unique_ptr<ReservationFiller> filler)
    : ranges_(std::move(ranges)),
      durable_(std::move(durable)),
      filler_(std::move(filler)),
      owner_(std::this_thread::get_id()) {
  if (filler_) ++t_held;
}

// Only allocation can throw here, and the process is ending anyway.
// NOLINTNEXTLINE(bugprone-exception-escape)
Reservation::~Reservation() {
  if (filler_) Dropped(ranges_);
}

// NOLINTNEXTLINE(bugprone-exception-escape): as the destructor.
Reservation& Reservation::operator=(Reservation&& other) noexcept {
  if (this == &other) return *this;
  if (filler_) Dropped(ranges_);
  ranges_ = std::move(other.ranges_);
  durable_ = std::move(other.durable_);
  filler_ = std::move(other.filler_);
  owner_ = other.owner_;
  return *this;
}

DurableFutures Reservation::Finish() noexcept {
  if (filler_) {
    ABYSS_DCHECK(std::this_thread::get_id() == owner_,
                 "a WAL reservation was completed off the thread that reserved it");
    filler_->Fill();
    filler_.reset();
    --t_held;
  }
  return std::move(durable_);
}

uint32_t ReservationsHeld() noexcept { return t_held; }

}  // namespace abyss::queue
