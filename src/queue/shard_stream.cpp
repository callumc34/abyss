#include "shard_stream.h"

#include <algorithm>
#include <chrono>
#include <limits>
#include <string>
#include <thread>
#include <tuple>
#include <utility>

#include "abyss/core/fatal.h"
#include "abyss/core/queue.h"
#include "abyss/metrics/names.h"
#include "cpu_relax.h"

namespace abyss::queue {

namespace {

constexpr uint64_t kWriting = uint64_t{1} << 63;
// No seq has the writing bit, so a slot never written never validates.
constexpr uint64_t kNever = ~uint64_t{0};
constexpr uint64_t kIndexStride = uint64_t{64} << 10;
constexpr std::size_t kReadReserve = 256;
// Keeps a thread's encode buffer from pinning a large value's memory.
constexpr auto kSpinFor = std::chrono::microseconds(2);
// Past this a publish wait parks rather than yields.
constexpr auto kYieldFor = std::chrono::microseconds(100);

thread_local std::vector<uint32_t> t_sizes;

core::Error Stopping() { return {core::ErrorCode::kUnavailable, "queue shutting down"}; }

DurabilityFuture ReadyFuture(core::Result<void> value) {
  std::promise<core::Result<void>> promise;
  promise.set_value(std::move(value));
  return promise.get_future();
}

// An append's power_loss future: its own node and sink, freed by the
// one post its node gets.
class PromiseWaiter final : public core::CompletionSink {
 public:
  explicit PromiseWaiter(core::SequenceId seq)
      : node_(seq, core::Completion{.sink = this, .client_id = 0, .slot_seq = seq}) {}

  core::SeqWaiterNode& node() noexcept { return node_; }
  DurabilityFuture Future() { return promise_.get_future(); }
  void Resolve(core::Result<void> result) { promise_.set_value(std::move(result)); }

  // A promise throws only on a second value, which one post rules out.
  // NOLINTNEXTLINE(bugprone-exception-escape)
  void Post(std::span<const core::CompletionEvent> events) noexcept override {
    promise_.set_value(events.front().result);
    delete this;  // NOLINT(cppcoreguidelines-owning-memory): released by its registrant.
  }

 private:
  core::SeqWaiterNode node_;
  std::promise<core::Result<void>> promise_;
};

// Encodes `entry` straight into its reserved slice, then commits it.
void CommitEncoded(Log& log, const core::QueueEntry& entry, core::ShardId shard,
                   const Log::Reservation& slice, uint64_t batch_rest) {
  log.CommitInPlace(slice,
                    frame::EncodeEntryInto(entry, shard, {slice.dst, slice.size}, batch_rest));
}

}  // namespace

class ShardStream::Publisher final : public AppendPublisher {
 public:
  Publisher(ShardStream& stream, std::unique_lock<std::mutex> lock, core::SequenceId first,
            core::SequenceId end, LogPosition end_pos) noexcept
      : stream_(stream), lock_(std::move(lock)), first_(first), end_(end), end_pos_(end_pos) {}

  // Only allocation can throw here, and an OOM while publishing may
  // terminate.
  // NOLINTNEXTLINE(bugprone-exception-escape)
  void Publish() noexcept override {
    if (!lock_.owns_lock()) return;
    // A reservation before this append may not have completed.
    stream_.AwaitPublished(first_);
    ABYSS_DCHECK(first_ >= core::kFirstSeq, "a publish from seq 0");
    stream_.published_end_.store(end_, std::memory_order_seq_cst);
    lock_.unlock();
    // A flush may have covered these frames before they were published.
    if (stream_.RaisePowerEnd()) stream_.PowerAdvanced();
    stream_.WakePublished(end_);
    stream_.config_.unit->committer->Published(end_pos_);
  }

 private:
  ShardStream& stream_;
  std::unique_lock<std::mutex> lock_;
  core::SequenceId first_;
  core::SequenceId end_;
  LogPosition end_pos_;
};

// A reservation's remaining work. Deadlock-free by log position: each
// stream reserves log space inside its append lock, so a shard's
// reservation order is position order, across shards too. AwaitFilled
// and each in-order publish wait only on frames at earlier positions,
// whose owners hold no lock until they complete, so no wait closes a
// cycle.
class ShardStream::Filler final : public ReservationFiller {
 public:
  struct Part {
    ShardStream* stream = nullptr;
    core::SequenceId first = 0;
    core::SequenceId end = 0;
  };
  struct Deferred {
    core::QueueEntry entry;
    core::ShardId shard = 0;
    Log::Reservation slice;
    uint64_t batch_rest = 0;
  };

  Filler(LogUnit& unit, std::vector<Part> parts, LogPosition end, std::vector<Deferred> deferred,
         std::function<void(std::size_t)> hook, std::size_t committed, std::size_t frames)
      : unit_(unit),
        parts_(std::move(parts)),
        end_(end),
        deferred_(std::move(deferred)),
        hook_(std::move(hook)),
        committed_(committed),
        frames_(frames) {}

  // Only allocation can throw here, and an OOM while filling may
  // terminate.
  // NOLINTNEXTLINE(bugprone-exception-escape)
  void Fill() noexcept override {
    Log& log = *unit_.log;
    for (Deferred& deferred : deferred_) {
      CommitEncoded(log, deferred.entry, deferred.shard, deferred.slice, deferred.batch_rest);
      if (hook_ && ++committed_ < frames_) hook_(committed_);
    }
    deferred_.clear();
    log.AwaitFilled(end_);
    for (const Part& part : parts_) part.stream->PublishInOrder(part.first, part.end);
    unit_.committer->Published(end_);
    unit_.ready_to_complete.fetch_sub(1, std::memory_order_acq_rel);
  }

 private:
  LogUnit& unit_;
  std::vector<Part> parts_;
  LogPosition end_;
  std::vector<Deferred> deferred_;
  std::function<void(std::size_t)> hook_;
  std::size_t committed_;
  std::size_t frames_;
};

ShardStream::ShardStream(ShardStreamConfig config)
    : config_(config),
      appended_(metrics::Registry::Instance().Counter(metrics::names::kQueueAppendedTotal)),
      publish_wait_(
          metrics::Registry::Instance().Histogram(metrics::names::kWalPublishWaitSeconds)),
      index_gauge_(metrics::Registry::Instance().Gauge(metrics::names::kWalIndexBytes)),
      ring_gauge_(metrics::Registry::Instance().Gauge(metrics::names::kWalRingBytes)),
      read_out_of_range_(
          metrics::Registry::Instance().Counter(metrics::names::kQueueReadOutOfRangeTotal)),
      ring_mask_(config_.ring_entries - 1),
      ring_(config_.ring_entries) {
  for (Slot& slot : ring_) slot.stamp.store(kNever, std::memory_order_relaxed);
  ring_gauge_.Increment(static_cast<double>(RingBytes(ring_.size())));
  for (Hint& hint : hints_) hint.seq.store(kNever, std::memory_order_relaxed);
}

ShardStream::~ShardStream() {
  ring_gauge_.Decrement(static_cast<double>(RingBytes(ring_.size())));
  const std::scoped_lock lock(index_mu_);
  index_gauge_.Decrement(static_cast<double>(index_.size() * sizeof(IndexPoint)));
}

core::Result<void> ShardStream::Recover(const RecoveredFrame& frame) {
  const core::SequenceId seq = frame.header.seq;
  if (seq < core::kFirstSeq) {
    // Only builds of the unreleased format before seqs started at 1
    // wrote one, so wiping is the remedy, not a migration.
    return std::unexpected(core::Error{
        core::ErrorCode::kCorruption,
        "WAL log " + std::to_string(config_.unit->id) + ": shard " + std::to_string(config_.shard) +
            " has a frame with seq 0 at position " + std::to_string(frame.pos) +
            ", written by an earlier build of an unreleased WAL format: delete the data "
            "directory (queue.wal_path and cold.data_path) and start empty"});
  }
  if (recovered_next_.has_value() && seq != *recovered_next_) {
    return std::unexpected(core::Error{
        core::ErrorCode::kCorruption,
        "WAL log " + std::to_string(config_.unit->id) + ": shard " + std::to_string(config_.shard) +
            " seq " + std::to_string(seq) + " follows " + std::to_string(*recovered_next_ - 1) +
            " at position " + std::to_string(frame.pos)});
  }
  if (!recovered_first_.has_value()) recovered_first_ = seq;
  recovered_next_ = seq + 1;
  const std::scoped_lock lock(append_mu_);
  Record(seq, frame.pos, frame.size);
  return {};
}

void ShardStream::FinishRecovery(core::SequenceId next) {
  ABYSS_DCHECK(next >= core::kFirstSeq, "a shard's next seq is 0");
  const std::scoped_lock lock(append_mu_);
  next_seq_ = next;
  first_seq_.store(recovered_first_.value_or(next), std::memory_order_release);
  published_end_.store(next, std::memory_order_release);
  flushed_end_.store(next, std::memory_order_release);
  power_end_.store(next, std::memory_order_release);
}

// Seqlock writer (single, under the append lock). The release store
// of `pos` carries the writing stamp: a reader that sees the new pos
// sees the stamp too. No fence, which TSan cannot model.
void ShardStream::Record(core::SequenceId seq, LogPosition pos, uint32_t size) {
  Slot& slot = ring_[seq & ring_mask_];
  slot.stamp.store(seq | kWriting, std::memory_order_relaxed);
  slot.pos.store(pos, std::memory_order_release);
  slot.stamp.store(seq, std::memory_order_release);

  if (!has_point_ || since_point_ >= kIndexStride) {
    {
      const std::scoped_lock lock(index_mu_);
      index_.push_back(IndexPoint{.seq = seq, .pos = pos});
    }
    index_gauge_.Increment(static_cast<double>(sizeof(IndexPoint)));
    has_point_ = true;
    since_point_ = 0;
  }
  since_point_ += size;
}

core::Result<PendingAppend> ShardStream::BeginAppend(core::QueueEntry entry,
                                                     core::SteadyTime admit_by) {
  auto begun = Begin({&entry, 1}, admit_by);
  if (!begun.has_value()) return std::unexpected(begun.error());
  return PendingAppend{begun->first, std::move(begun->durable), std::move(begun->publisher)};
}

core::Result<PendingBatchAppend> ShardStream::BeginAppendBatch(
    std::span<const core::QueueEntry> entries, core::SteadyTime admit_by) {
  if (entries.empty()) {
    return std::unexpected(core::Error{core::ErrorCode::kInvalidArgument, "empty batch"});
  }
  std::vector<core::QueueEntry> owned(entries.begin(), entries.end());
  auto begun = Begin(owned, admit_by);
  if (!begun.has_value()) return std::unexpected(begun.error());
  return PendingBatchAppend{begun->first, begun->last, std::move(begun->durable),
                            std::move(begun->publisher)};
}

core::Result<ShardStream::Begun> ShardStream::Begin(std::span<core::QueueEntry> entries,
                                                    core::SteadyTime admit_by) {
  const bool batch = entries.size() > 1;
  if (config_.window != nullptr) {
    if (auto admitted = config_.window->Admit(config_.unit->age, admit_by); !admitted) {
      return std::unexpected(admitted.error());
    }
  }
  std::vector<uint32_t>& sizes = t_sizes;
  sizes.clear();
  uint64_t total = 0;
  for (const core::QueueEntry& entry : entries) {
    const std::size_t size = frame::EntryFrameSize(entry);
    if (size > config_.max_value_size_bytes) {
      return std::unexpected(core::Error{core::ErrorCode::kValueTooLarge,
                                         std::string(batch ? "batch entry of " : "entry of ") +
                                             std::to_string(size) +
                                             " bytes exceeds queue.max_value_size_bytes (" +
                                             std::to_string(config_.max_value_size_bytes) + ")"});
    }
    sizes.push_back(static_cast<uint32_t>(size));
    total += size;
  }
  if (total > std::numeric_limits<uint32_t>::max()) {
    return std::unexpected(
        core::Error{core::ErrorCode::kResourceExhausted, "batch exceeds segment capacity"});
  }
  for (;;) {
    std::unique_lock lock(append_mu_);
    if (stopping_.load(std::memory_order_acquire)) return std::unexpected(Stopping());
    auto reserved = log().Reserve(static_cast<uint32_t>(total));
    if (!reserved.has_value()) {
      const core::ErrorCode code = reserved.error().code();
      if (code == core::ErrorCode::kUnavailable) {
        lock.unlock();
        if (log().WaitForSpare(admit_by)) continue;
        if (stopping_.load(std::memory_order_acquire)) return std::unexpected(Stopping());
        return std::unexpected(core::Error{
            core::ErrorCode::kResourceExhausted,
            "no spare WAL segment ready: the disk is full or the segment preparer is behind"});
      }
      if (code == core::ErrorCode::kResourceExhausted && batch) {
        return std::unexpected(
            core::Error{code, "batch exceeds segment capacity: " + reserved.error().message()});
      }
      return std::unexpected(reserved.error());
    }

    const Log::Reservation& at = *reserved;
    if (config_.window != nullptr) config_.window->Add(total);
    config_.unit->age.Start(DurabilityWindow::Clock::now());
    const core::SequenceId first = next_seq_;
    ABYSS_DCHECK(first >= core::kFirstSeq, "a frame assigned seq 0");
    uint64_t off = 0;
    for (std::size_t i = 0; i < entries.size(); ++i) {
      entries[i].seq = first + i;
      const Log::Reservation slice{.pos = at.pos + off,
                                   .size = sizes[i],
                                   .gen = at.gen,
                                   .salt = at.salt,
                                   .dst = at.dst + off};
      CommitEncoded(log(), entries[i], config_.shard, slice, total - off);
      Record(first + i, slice.pos, sizes[i]);
      off += sizes[i];
      if (batch_commit_hook_ && i + 1 < entries.size()) batch_commit_hook_(i + 1);
    }
    const LogPosition end_pos = at.pos + total;
    log().AwaitFilled(end_pos);

    const core::SequenceId last = first + entries.size() - 1;
    next_seq_ = last + 1;
    appended_.Increment(static_cast<double>(entries.size()));
    DurabilityFuture durable = config_.ack_durability == core::Durability::kPowerLoss
                                   ? PowerDurableFuture(last)
                                   : ReadyFuture({});
    return Begun{
        .first = first,
        .last = last,
        .durable = std::move(durable),
        .publisher = std::make_unique<Publisher>(*this, std::move(lock), first, last + 1, end_pos)};
  }
}

// A reservation over several logs: each log's filler, in log order.
class ShardStream::MultiFiller final : public ReservationFiller {
 public:
  explicit MultiFiller(std::vector<std::unique_ptr<ReservationFiller>> fillers)
      : fillers_(std::move(fillers)) {}

  void Fill() noexcept override {
    for (const auto& filler : fillers_) filler->Fill();
  }

 private:
  std::vector<std::unique_ptr<ReservationFiller>> fillers_;
};

core::Result<Reservation> ShardStream::Reserve(std::span<const LogParts> logs) {
  // Kept per thread, so a reservation allocates nothing under the
  // caller's locks that it does not hand back. Cleared on every return,
  // which releases the appends.
  thread_local std::vector<std::unique_lock<std::mutex>> t_locks;
  thread_local std::vector<uint64_t> t_totals;
  // Checked before the guard: a re-entry would clear the outer's locks.
  ABYSS_DCHECK(t_locks.empty() && t_totals.empty(), "a reservation re-entered on its thread");
  struct Clear {
    Clear(const Clear&) = delete;
    Clear& operator=(const Clear&) = delete;
    Clear(Clear&&) = delete;
    Clear& operator=(Clear&&) = delete;
    Clear() = default;
    ~Clear() {
      t_locks.clear();
      t_totals.clear();
    }
  } const clear;
  std::vector<std::unique_lock<std::mutex>>& locks = t_locks;
  for (const LogParts& group : logs) {
    for (ShardStream* stream : group.streams) {
      locks.emplace_back(stream->append_mu_);
      if (stream->stopping_.load(std::memory_order_acquire)) return std::unexpected(Stopping());
    }
  }
  std::vector<uint64_t>& totals = t_totals;
  for (const LogParts& group : logs) {
    uint64_t total = 0;
    for (const uint32_t size : group.sizes) total += size;
    totals.push_back(total);
  }
  // With every stream of each log locked nothing else reserves there,
  // so a check of all first leaves no log reserved when one refuses.
  if (logs.size() > 1) {
    for (std::size_t g = 0; g < logs.size(); ++g) {
      if (!logs[g].streams.front()->log().CanReserve(static_cast<uint32_t>(totals[g]))) {
        return std::unexpected(core::Error{
            core::ErrorCode::kUnavailable,
            "no spare WAL segment ready: the disk is full or the segment preparer is behind"});
      }
    }
  }

  std::vector<ReservedRange> ranges;
  // Bytes encoded under the caller's locks, across the reservation.
  std::size_t locked = 0;
  if (logs.size() == 1) {
    auto filler = ReserveLocked(logs.front(), totals.front(), locked, ranges);
    if (!filler.has_value()) return std::unexpected(filler.error());
    return Reservation(std::move(ranges), *std::move(filler));
  }
  std::vector<std::unique_ptr<ReservationFiller>> fillers;
  for (std::size_t g = 0; g < logs.size(); ++g) {
    auto filler = ReserveLocked(logs[g], totals[g], locked, ranges);
    if (!filler.has_value()) {
      if (g > 0) core::Fatal("a WAL log refused space it was checked to have");
      return std::unexpected(filler.error());
    }
    fillers.push_back(*std::move(filler));
  }
  return Reservation(std::move(ranges), std::make_unique<MultiFiller>(std::move(fillers)));
}

core::Result<std::unique_ptr<ReservationFiller>> ShardStream::ReserveLocked(
    const LogParts& group, uint64_t total, std::size_t& locked,
    std::vector<ReservedRange>& ranges) {
  ShardStream& lead = *group.streams.front();
  LogUnit& unit = *lead.config_.unit;
  Log& log = *unit.log;
  // Sizes do not depend on seqs, so space comes first and a refusal
  // leaves nothing to undo.
  auto at = log.Reserve(static_cast<uint32_t>(total));
  if (!at.has_value()) {
    const core::ErrorCode code = at.error().code();
    if (code == core::ErrorCode::kUnavailable) {
      return std::unexpected(core::Error{
          code, "no spare WAL segment ready: the disk is full or the segment preparer is behind"});
    }
    if (code == core::ErrorCode::kResourceExhausted) {
      return std::unexpected(
          core::Error{core::ErrorCode::kValueTooLarge,
                      "batch exceeds segment capacity: " + at.error().message()});
    }
    return std::unexpected(at.error());
  }

  if (lead.config_.window != nullptr) lead.config_.window->Add(total);
  unit.age.Start(DurabilityWindow::Clock::now());
  const std::function<void(std::size_t)>& hook = lead.batch_commit_hook_;
  std::vector<Filler::Part> publish;
  publish.reserve(group.parts.size());
  std::vector<Filler::Deferred> deferred;
  std::size_t committed = 0;
  std::size_t k = 0;
  uint64_t off = 0;
  for (std::size_t p = 0; p < group.parts.size(); ++p) {
    ShardStream& stream = *group.streams[p];
    const ShardEntries& part = group.parts[p];
    const core::SequenceId first = stream.next_seq_;
    ABYSS_DCHECK(first >= core::kFirstSeq, "a frame assigned seq 0");
    for (std::size_t i = 0; i < part.entries.size(); ++i, ++k) {
      core::QueueEntry& entry = part.entries[i];
      const core::SequenceId seq = first + i;
      entry.seq = seq;
      const Log::Reservation slice{.pos = at->pos + off,
                                   .size = group.sizes[k],
                                   .gen = at->gen,
                                   .salt = at->salt,
                                   .dst = at->dst + off};
      // Encoded under the locks only while the reservation's total stays
      // within the budget; every later frame is left to Complete.
      if (deferred.empty() && locked + slice.size <= core::kLockHoldFrameBytes) {
        locked += slice.size;
        CommitEncoded(log, entry, part.shard, slice, total - off);
        if (hook && ++committed < group.sizes.size()) hook(committed);
      } else {
        deferred.push_back(Filler::Deferred{.entry = std::move(entry),
                                            .shard = part.shard,
                                            .slice = slice,
                                            .batch_rest = total - off});
      }
      stream.Record(seq, slice.pos, slice.size);
      off += slice.size;
    }
    const core::SequenceId last = first + part.entries.size() - 1;
    stream.next_seq_ = last + 1;
    stream.appended_.Increment(static_cast<double>(part.entries.size()));
    ranges.push_back(ReservedRange{.shard = part.shard, .first = first, .last = last});
    publish.push_back(Filler::Part{.stream = &stream, .first = first, .end = last + 1});
  }
  std::function<void(std::size_t)> fill_hook;
  if (hook && !deferred.empty()) fill_hook = hook;
  unit.ready_to_complete.fetch_add(1, std::memory_order_acq_rel);
  return std::make_unique<Filler>(unit, std::move(publish), at->pos + total, std::move(deferred),
                                  std::move(fill_hook), committed, group.sizes.size());
}

DurabilityFuture ShardStream::PowerDurableFuture(core::SequenceId last) {
  auto waiter = std::make_unique<PromiseWaiter>(last);
  DurabilityFuture future = waiter->Future();
  if (auto ready = power_waiters_.Register(waiter->node(), power_end_)) {
    waiter->Resolve(*std::move(ready));
  } else {
    // Linked: the post that resolves it frees it.
    std::ignore = waiter.release();
  }
  return future;
}

// Spins, yields, then parks: the wait is a predecessor's fill and
// publish. The acquire pairs with that publish's store, so its frames
// are filled before this publisher's store too.
//
// Parking saves CPU; it is not the latency fix. The wait is head-of-
// line behind a large fill on the same log, which only the blob lane
// (#162) removes, and on an asynchronous reactor parking or yielding
// here would stall the reactor all the same. A parked owner of a
// reservation keeps the Reserve-to-Complete rule: this is Complete's
// own wait, on an earlier seq whose publisher never waits on it. Only
// a test's park hook can throw here.
// NOLINTNEXTLINE(bugprone-exception-escape)
void ShardStream::AwaitPublished(core::SequenceId first) noexcept {
  if (published_end_.load(std::memory_order_acquire) < first) {
    const auto start = std::chrono::steady_clock::now();
    bool spinning = true;
    while (published_end_.load(std::memory_order_acquire) < first) {
      const auto waited = std::chrono::steady_clock::now() - start;
      if (waited < kSpinFor) {
        CpuRelax();
        continue;
      }
      spinning = false;
      if (waited < kYieldFor) {
        std::this_thread::yield();
        continue;
      }
      if (park_hook_) park_hook_();
      // Counted, then re-read, against WakePublished's store, then
      // count: one of them sees the other, and wait() compares the
      // value it was given, so no wake falls before the park.
      publish_parked_.fetch_add(1, std::memory_order_seq_cst);
      for (core::SequenceId seen = published_end_.load(std::memory_order_seq_cst); seen < first;
           seen = published_end_.load(std::memory_order_seq_cst)) {
        published_end_.wait(seen, std::memory_order_seq_cst);
      }
      publish_parked_.fetch_sub(1, std::memory_order_seq_cst);
    }
    if (!spinning) {
      publish_wait_.Observe(
          std::chrono::duration<double>(std::chrono::steady_clock::now() - start).count());
    }
  }
  // Only the publisher of `first` stores, so anything else is a bug
  // that the store after this would hide by moving the end backwards.
  ABYSS_DCHECK(published_end_.load(std::memory_order_acquire) == first,
               "a shard's published end passed a seq not yet published");
}

// As the Publisher's, only allocation can throw here.
// NOLINTNEXTLINE(bugprone-exception-escape)
void ShardStream::PublishInOrder(core::SequenceId first, core::SequenceId end) noexcept {
  ABYSS_DCHECK(first >= core::kFirstSeq, "a publish from seq 0");
  AwaitPublished(first);
  // Only the publisher of `first` stores now, so the end never regresses.
  published_end_.store(end, std::memory_order_seq_cst);
  if (RaisePowerEnd()) PowerAdvanced();
  WakePublished(end);
}

// The publisher stored published_end_ seq_cst; each waiter count is
// then loaded seq_cst, so the empty case costs a load apiece.
void ShardStream::WakePublished(core::SequenceId end) noexcept {
  if (publish_parked_.load(std::memory_order_seq_cst) > 0) published_end_.notify_all();
  published_waiters_.Advance(end);
}

core::SequenceId ShardStream::next_seq() const {
  const std::scoped_lock lock(append_mu_);
  return next_seq_;
}

core::SequenceId ShardStream::DurableEnd(core::Durability durability) const noexcept {
  return EndOf(durability).load(std::memory_order_acquire);
}

std::optional<core::Result<void>> ShardStream::WhenDurable(core::Durability durability,
                                                           core::SeqWaiterNode& node) {
  if (DurableEnd(durability) > node.seq()) return core::Result<void>{};
  return Waiters(durability).Register(node, EndOf(durability));
}

bool ShardStream::CancelDurable(core::Durability durability, core::SeqWaiterNode& node) noexcept {
  return Waiters(durability).Cancel(node);
}

core::Result<bool> ShardStream::AwaitDurable(core::SequenceId seq, core::Durability durability,
                                             core::Duration timeout) {
  if (DurableEnd(durability) > seq) return true;
  if (durable_wait_hook_) durable_wait_hook_();
  auto waited = Waiters(durability).AwaitEnd(EndOf(durability), seq, timeout);
  if (waited.has_value()) return true;
  if (waited.error().code() == core::ErrorCode::kTimeout) return false;
  return std::unexpected(waited.error());
}

bool ShardStream::Durable(core::SequenceId end) noexcept {
  ABYSS_DCHECK(end > core::kFirstSeq, "a durable batch ends at seq 0");
  flushed_end_.store(end, std::memory_order_seq_cst);
  return RaisePowerEnd();
}

// The power end is the flushed end, but never past the published one:
// a frame is filled, and may be flushed, before its PendingAppend is
// published, and nothing may see it until then. The flusher stores the
// flushed end and the publisher the published end, each then reading
// the other, all seq_cst, so the later of the two raises it. The raise
// is seq_cst too, for PowerAdvanced's waiter count.
bool ShardStream::RaisePowerEnd() noexcept {
  const core::SequenceId target = std::min(flushed_end_.load(std::memory_order_seq_cst),
                                           published_end_.load(std::memory_order_seq_cst));
  core::SequenceId current = power_end_.load(std::memory_order_relaxed);
  while (target > current) {
    if (power_end_.compare_exchange_weak(current, target, std::memory_order_seq_cst,
                                         std::memory_order_relaxed)) {
      return true;
    }
  }
  return false;
}

void ShardStream::PowerAdvanced() noexcept {
  power_waiters_.Advance(power_end_.load(std::memory_order_seq_cst));
}

void ShardStream::Reclaimed(std::optional<core::SequenceId> max_seq, LogPosition retained_from) {
  std::size_t dropped = 0;
  bool floored = false;
  {
    const std::scoped_lock lock(index_mu_);
    core::SequenceId first = first_seq_.load(std::memory_order_relaxed);
    if (max_seq.has_value()) first = std::max(first, *max_seq + 1);
    while (!index_.empty() && index_.front().pos < retained_from) {
      index_.pop_front();
      ++dropped;
    }
    // Frames from `first` up to the next point have none of their own.
    if (index_.empty() || index_.front().seq > first) {
      index_.push_front(IndexPoint{.seq = first, .pos = retained_from});
      floored = true;
    }
    first_seq_.store(first, std::memory_order_release);
  }
  if (floored) index_gauge_.Increment(static_cast<double>(sizeof(IndexPoint)));
  if (dropped > 0) index_gauge_.Decrement(static_cast<double>(dropped * sizeof(IndexPoint)));
}

std::size_t ShardStream::index_points() const {
  const std::scoped_lock lock(index_mu_);
  return index_.size();
}

// Seqlock reader: a pos from a rewrite acquires that rewrite's writing
// stamp, so the re-read sees it and validation fails.
std::optional<LogPosition> ShardStream::RingAt(core::SequenceId seq) const noexcept {
  const Slot& slot = ring_[seq & ring_mask_];
  if (slot.stamp.load(std::memory_order_acquire) != seq) return std::nullopt;
  const LogPosition pos = slot.pos.load(std::memory_order_acquire);
  if (slot.stamp.load(std::memory_order_relaxed) != seq) return std::nullopt;
  return pos;
}

std::optional<ShardStream::IndexPoint> ShardStream::Floor(Log::Cursor& cursor,
                                                          core::SequenceId seq) const {
  std::optional<IndexPoint> best;
  {
    const std::scoped_lock lock(index_mu_);
    const auto it = std::ranges::upper_bound(index_, seq, {}, &IndexPoint::seq);
    if (it != index_.begin()) best = *std::prev(it);
  }
  // A hint's two fields may come from different updates, so the frame
  // at its position must carry its seq.
  for (const Hint& hint : hints_) {
    const core::SequenceId hinted = hint.seq.load(std::memory_order_relaxed);
    const LogPosition pos = hint.pos.load(std::memory_order_relaxed);
    if (hinted > seq || (best.has_value() && hinted <= best->seq)) continue;
    auto view = cursor.Peek(pos);
    if (!view.has_value() || view->header.kind != frame::Kind::kEntry ||
        view->header.shard != config_.shard || view->header.seq != hinted) {
      continue;
    }
    best = IndexPoint{.seq = hinted, .pos = pos};
  }
  return best;
}

core::Result<LogPosition> ShardStream::SkipTo(Log::Cursor& cursor, LogPosition pos,
                                              core::SequenceId seq) const {
  uint64_t stepped = 0;
  auto found = [&]() -> core::Result<LogPosition> {
    for (LogPosition at = pos;; ++stepped) {
      if (at >= log().FilledPrefix()) {
        return std::unexpected(
            core::Error{core::ErrorCode::kInternal, "WAL shard " + std::to_string(config_.shard) +
                                                        " has no frame for published seq " +
                                                        std::to_string(seq)});
      }
      auto view = cursor.Peek(at);
      if (!view.has_value()) return std::unexpected(view.error());
      const frame::Header& header = view->header;
      if (header.kind == frame::Kind::kEntry && header.shard == config_.shard) {
        if (header.seq == seq) return at;
        if (header.seq > seq) {
          return std::unexpected(
              core::Error{core::ErrorCode::kInternal, "WAL shard " + std::to_string(config_.shard) +
                                                          " skips from seq " + std::to_string(seq) +
                                                          " to " + std::to_string(header.seq)});
        }
      }
      // Padding ends at its segment's end, where the next frame starts.
      at += view->size;
    }
  }();
  skipped_.fetch_add(stepped, std::memory_order_relaxed);
  return found;
}

core::Result<LogPosition> ShardStream::LocateRetained(Log::Cursor& cursor,
                                                      core::SequenceId seq) const {
  if (auto pos = RingAt(seq)) return *pos;
  const auto floor = Floor(cursor, seq);
  if (!floor.has_value()) {
    return std::unexpected(core::Error{core::ErrorCode::kOutOfRange,
                                       "WAL shard " + std::to_string(config_.shard) + " seq " +
                                           std::to_string(seq) + " has been reclaimed"});
  }
  return SkipTo(cursor, floor->pos, seq);
}

// A reclaim takes segments from the table before it moves the floors,
// so a segment found gone waits for that to finish, then looks again.
// Only a reclaim during the attempt explains a miss.
core::Result<LogPosition> ShardStream::Locate(Log::Cursor& cursor, core::SequenceId seq) const {
  LogUnit& unit = *config_.unit;
  for (;;) {
    const uint64_t reclaims = unit.reclaims.load(std::memory_order_acquire);
    auto pos = LocateRetained(cursor, seq);
    if (pos.has_value() || pos.error().code() != core::ErrorCode::kOutOfRange) return pos;
    {
      const std::scoped_lock settled(unit.reclaim_mu);
    }
    if (seq < first_seq()) return pos;
    if (unit.reclaims.load(std::memory_order_acquire) == reclaims) {
      return std::unexpected(core::Error{
          core::ErrorCode::kInternal, "WAL shard " + std::to_string(config_.shard) + " seq " +
                                          std::to_string(seq) +
                                          " is retained but not found: " + pos.error().message()});
    }
  }
}

core::Result<void> ShardStream::CheckReadable(core::SequenceId from) const {
  if (stopping_.load(std::memory_order_acquire)) return std::unexpected(Stopping());
  if (const core::SequenceId first = first_seq(); from < first) {
    read_out_of_range_.Increment();
    return std::unexpected(core::Error{core::ErrorCode::kOutOfRange,
                                       "read from seq " + std::to_string(from) +
                                           " below first retained seq " + std::to_string(first) +
                                           " on shard " + std::to_string(config_.shard)});
  }
  return {};
}

core::Error ShardStream::ReadFailed(const core::Error& error, core::SequenceId seq) const {
  if (error.code() == core::ErrorCode::kCorruption) {
    core::Fatal("WAL read of shard " + std::to_string(config_.shard) + " seq " +
                std::to_string(seq) + " failed: " + error.message());
  }
  if (error.code() == core::ErrorCode::kOutOfRange) {
    read_out_of_range_.Increment();
  }
  return error;
}

void ShardStream::Remember(core::SequenceId seq, LogPosition pos) const noexcept {
  const std::size_t slot = next_hint_.fetch_add(1, std::memory_order_relaxed) % hints_.size();
  // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-constant-array-index): reduced mod the size.
  Hint& hint = hints_[slot];
  hint.seq.store(seq, std::memory_order_relaxed);
  hint.pos.store(pos, std::memory_order_relaxed);
}

core::Result<std::vector<core::QueueEntry>> ShardStream::Read(core::SequenceId from,
                                                              std::size_t max_count,
                                                              core::Duration timeout,
                                                              core::Durability visible) {
  if (auto readable = CheckReadable(from); !readable) return std::unexpected(readable.error());
  if (DurableEnd(visible) <= from && timeout > core::Duration::zero()) {
    // Ended, timed out or failed, the end decides what is read.
    std::ignore = Waiters(visible).AwaitEnd(EndOf(visible), from, timeout);
    if (auto readable = CheckReadable(from); !readable) return std::unexpected(readable.error());
  }
  const core::SequenceId limit = DurableEnd(visible);
  std::vector<core::QueueEntry> out;
  if (limit <= from || max_count == 0) return out;
  const core::SequenceId end = from + std::min<uint64_t>(max_count, limit - from);
  out.reserve(std::min<uint64_t>(end - from, kReadReserve));

  Log::Cursor cursor(log());
  auto located = Locate(cursor, from);
  if (!located.has_value()) return std::unexpected(ReadFailed(located.error(), from));
  LogPosition pos = *located;
  std::size_t size = 0;
  for (core::SequenceId seq = from; seq < end; ++seq) {
    if (seq != from) {
      if (auto hit = RingAt(seq)) {
        pos = *hit;
      } else {
        auto next = SkipTo(cursor, pos + size, seq);
        if (!next.has_value()) return std::unexpected(ReadFailed(next.error(), seq));
        pos = *next;
      }
    }
    auto view = cursor.Read(pos);
    if (!view.has_value()) return std::unexpected(ReadFailed(view.error(), seq));
    if (view->header.kind != frame::Kind::kEntry || view->header.shard != config_.shard ||
        view->header.seq != seq) {
      return std::unexpected(core::Error{core::ErrorCode::kInternal,
                                         "WAL shard " + std::to_string(config_.shard) + " seq " +
                                             std::to_string(seq) + " is not at position " +
                                             std::to_string(pos)});
    }
    auto entry = frame::DecodeEntry(*view);
    if (!entry.has_value()) return std::unexpected(ReadFailed(entry.error(), seq));
    out.push_back(std::move(*entry));
    size = view->size;
  }
  Remember(end - 1, pos);
  return out;
}

void ShardStream::SetBatchCommitHookForTesting(std::function<void(std::size_t committed)> hook) {
  const std::scoped_lock lock(append_mu_);
  batch_commit_hook_ = std::move(hook);
}

void ShardStream::Shutdown() {
  {
    const std::scoped_lock lock(append_mu_);
    if (stopping_.load(std::memory_order_relaxed)) return;
    stopping_.store(true, std::memory_order_seq_cst);
  }
  published_waiters_.FailAll(Stopping());
}

void ShardStream::CommitterStopped() {
  power_waiters_.FailAll(core::Error{core::ErrorCode::kUnavailable, "WAL group committer stopped"});
}

}  // namespace abyss::queue
