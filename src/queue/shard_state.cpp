#include "shard_state.h"

#include <algorithm>
#include <charconv>
#include <chrono>
#include <filesystem>
#include <iomanip>
#include <optional>
#include <regex>
#include <sstream>
#include <string_view>
#include <utility>

#include "abyss/core/fatal.h"
#include "abyss/log/log.h"
#include "abyss/metrics/metrics.h"
#include "abyss/metrics/names.h"
#include "abyss/platform/fs.h"
#include "abyss/queue/segment_header.h"
#include "abyss/queue/wal_entry.h"

ABYSS_LOG_COMPONENT("abyss.queue.shard")

namespace abyss::queue {

namespace {

namespace pfs = abyss::platform::fs;

constexpr int kSegmentNameWidth = 20;

std::string FormatSegmentName(core::SequenceId base_seq) {
  std::ostringstream oss;
  oss << std::setw(kSegmentNameWidth) << std::setfill('0') << base_seq << ".log";
  return oss.str();
}

// Checked, non-throwing decimal parse. Returns nullopt on overflow or any
// non-numeric trailing content so a damaged directory entry is skipped rather
// than aborting recovery with an uncaught std::out_of_range (QUEUE-3).
std::optional<core::SequenceId> ParseSeqChecked(std::string_view text) {
  core::SequenceId value = 0;
  const char* begin = text.data();
  const char* end = begin + text.size();
  const auto [ptr, ec] = std::from_chars(begin, end, value);
  if (ec != std::errc{} || ptr != end) return std::nullopt;
  return value;
}

core::Result<std::vector<core::SequenceId>> EnumerateSegmentBaseSeqs(const std::string& dir) {
  std::error_code ec;
  std::filesystem::directory_iterator it(dir, ec);
  if (ec) {
    return std::unexpected(
        core::Error{core::ErrorCode::kInternal, "list segment dir: " + ec.message()});
  }

  std::vector<core::SequenceId> result;
  const std::regex pattern(R"((\d{20})\.log)");
  for (const auto& entry : it) {
    if (!entry.is_regular_file()) continue;
    const auto name = entry.path().filename().string();
    std::smatch match;
    if (!std::regex_match(name, match, pattern)) continue;
    auto parsed = ParseSeqChecked(match[1].str());
    if (!parsed.has_value()) {
      // 20 digits can exceed UINT64_MAX (e.g. 99999999999999999999). A name
      // that cannot be a real base_seq is not a segment we wrote; skip it.
      ABYSS_LOG_WARN("skipping unparseable segment name", {"dir", std::string_view{dir}},
                     {"name", std::string_view{name}});
      continue;
    }
    result.push_back(*parsed);
  }
  std::ranges::sort(result);
  return result;
}

// fsync the shard directory so a freshly created/rotated segment's name->inode
// link is on stable media before the segment can ack any write (QUEUE-1/NET-6).
// FsyncDir "unsupported" is a hard error here (invariant 5): we must not ack a
// write whose segment's directory entry is not durable.
core::Result<void> FsyncShardDir(const std::string& dir) {
  auto out = pfs::FsyncDir(std::filesystem::path(dir));
  if (!out.has_value()) return std::unexpected(out.error());
  if (*out == pfs::DirSyncOutcome::kUnsupported) {
    return std::unexpected(
        core::Error{core::ErrorCode::kFailedPrecondition,
                    "shard directory durability unsupported on volume '" + dir +
                        "'; a segment's directory entry cannot be made durable"});
  }
  return {};
}

DurabilityFuture ReadyFuture() {
  std::promise<core::Result<void>> p;
  p.set_value({});
  return p.get_future();
}

class ShardStatePublisher final : public AppendPublisher {
 public:
  ShardStatePublisher(std::condition_variable& cv, std::unique_lock<std::mutex> lock,
                      GroupCommitter& committer, core::SequenceId end,
                      std::function<void()> on_rotate, bool rotated) noexcept
      : cv_(cv),
        lock_(std::move(lock)),
        committer_(committer),
        end_(end),
        on_rotate_(std::move(on_rotate)),
        rotated_(rotated) {}

  void Publish() noexcept override {
    if (!lock_.owns_lock()) return;
    lock_.unlock();
    cv_.notify_all();
    committer_.Published(end_);
    if (rotated_ && on_rotate_) {
      // Swallow: on_rotate is contractually noexcept; don't terminate from
      // the handle's destructor path.
      try {
        on_rotate_();
      } catch (...) {  // NOLINT(bugprone-empty-catch)
      }
    }
  }

 private:
  std::condition_variable& cv_;
  std::unique_lock<std::mutex> lock_;
  GroupCommitter& committer_;
  core::SequenceId end_;
  std::function<void()> on_rotate_;
  bool rotated_ = false;
};

}  // namespace

core::Result<std::unique_ptr<ShardState>> ShardState::Open(ShardStateConfig config) {
  std::error_code ec;
  std::filesystem::create_directories(config.directory, ec);
  if (ec) {
    return std::unexpected(
        core::Error{core::ErrorCode::kInternal, "create shard dir: " + ec.message()});
  }

  std::unique_ptr<ShardState> state(new ShardState(std::move(config)));
  auto init = state->Initialize();
  if (!init.has_value()) return std::unexpected(init.error());

  const size_t segment_count = state->sealed_.size() + 1;
  // A pristine shard has exactly one segment with seq 0. Pre-existing state
  // means this process is picking up from prior writes; call that out.
  if (segment_count > 1 || state->head_seq() > 0) {
    ABYSS_LOG_INFO("shard recovered", {"shard", static_cast<int64_t>(state->config_.shard)},
                   {"segments", static_cast<int64_t>(segment_count)},
                   {"head_seq", static_cast<uint64_t>(state->head_seq())},
                   {"first_seq", static_cast<uint64_t>(state->first_seq())});
  } else {
    ABYSS_LOG_DEBUG("shard opened", {"shard", static_cast<int64_t>(state->config_.shard)});
  }
  return state;
}

ShardState::ShardState(ShardStateConfig config)
    : config_(std::move(config)),
      appended_(metrics::Registry::Instance().Counter(metrics::names::kQueueAppendedTotal)) {}

ShardState::~ShardState() { Shutdown(); }

core::Result<void> ShardState::Initialize() {
  const std::scoped_lock lock(append_mu_);

  auto existing = EnumerateSegmentBaseSeqs(config_.directory);
  if (!existing.has_value()) return std::unexpected(existing.error());

  if (existing->empty()) {
    auto created = CreateActiveSegment(0);
    if (!created.has_value()) return std::unexpected(created.error());
  } else {
    auto opened = OpenExistingSegments();
    if (!opened.has_value()) return std::unexpected(opened.error());
  }

  // Everything recovered was synced above; a new segment's header was
  // synced at creation.
  flushed_base_ = active_->base_seq();
  flushed_offset_ = active_->write_offset();
  committer_ = std::make_unique<GroupCommitter>(
      GroupCommitter::Extent{.end = next_seq_, .bytes = 0}, [this] { return Flush(); },
      [this](GroupCommitter::Extent previous, GroupCommitter::Extent flushed) {
        Flushed(previous, flushed);
      });
  return {};
}

core::Result<void> ShardState::CreateActiveSegment(core::SequenceId base_seq) {
  const SegmentHeader header{
      .format_major = kWalFormatMajor,
      .format_minor = kWalFormatMinor,
      .flags = 0,
      .shard_id = config_.shard,
      .base_seq = base_seq,
      .created_at = core::WallClock::now(),
  };

  const auto path = std::filesystem::path(config_.directory) / FormatSegmentName(base_seq);
  auto seg = Segment::Create(path.string(), header, config_.segment_size_bytes);
  if (!seg.has_value()) return std::unexpected(seg.error());

  // Durably link the new segment's name before it can ack a write.
  if (auto sync = FsyncShardDir(config_.directory); !sync.has_value()) {
    std::error_code ec;
    std::filesystem::remove(path, ec);
    return std::unexpected(sync.error());
  }

  active_ = std::make_shared<Segment>(std::move(*seg));
  next_seq_ = active_->next_seq();
  return {};
}

core::Result<void> ShardState::OpenExistingSegments() {
  auto existing = EnumerateSegmentBaseSeqs(config_.directory);
  if (!existing.has_value()) return std::unexpected(existing.error());

  sealed_.clear();
  sealed_.reserve(!existing->empty() ? existing->size() - 1 : 0);

  // Enforces segment sequencing.
  std::optional<core::SequenceId> expected_base_seq;
  for (size_t i = 0; i < existing->size(); ++i) {
    const auto path = std::filesystem::path(config_.directory) / FormatSegmentName((*existing)[i]);
    auto opened = Segment::Open(path.string(), config_.segment_size_bytes);
    if (!opened.has_value()) {
      // A crash between creating the newest segment and syncing its header
      // leaves at most a header's bytes and no entries: redo the creation.
      std::error_code size_ec;
      const auto size = std::filesystem::file_size(path, size_ec);
      if (i + 1 == existing->size() && !size_ec && size <= kSegmentHeaderSize) {
        ABYSS_LOG_WARN("discarding a segment whose creation did not complete",
                       {"path", path.string()}, {"shard", static_cast<int64_t>(config_.shard)},
                       {"bytes", static_cast<uint64_t>(size)});
        std::error_code rm_ec;
        std::filesystem::remove(path, rm_ec);
        if (rm_ec) {
          return std::unexpected(core::Error{core::ErrorCode::kInternal,
                                             "remove aborted segment: " + rm_ec.message()});
        }
        if (auto sync = FsyncShardDir(config_.directory); !sync.has_value()) {
          return std::unexpected(sync.error());
        }
        // Recreate where the previous segment's recovered tail ends. A
        // power loss can drop that unsynced tail while the stub's name
        // survives; those seqs were never durable, so reusing them is safe.
        // With no previous segment, the name is the only record of the base.
        const core::SequenceId named = (*existing)[i];
        const core::SequenceId base = expected_base_seq.value_or(named);
        if (base > named) {
          return std::unexpected(
              core::Error{core::ErrorCode::kCorruption,
                          "aborted segment named below the recovered tail at " + path.string()});
        }
        if (base < named) {
          ABYSS_LOG_WARN("unsynced WAL tail lost before an aborted segment creation",
                         {"shard", static_cast<int64_t>(config_.shard)},
                         {"lost_from_seq", static_cast<uint64_t>(base)},
                         {"lost_to_seq", static_cast<uint64_t>(named)});
        }
        if (!sealed_.empty() && sealed_.back()->base_seq() == base) {
          // The previous segment recovered empty, so it already starts at
          // base: reopen it as the active segment instead of creating one.
          const std::string previous = sealed_.back()->path();
          sealed_.pop_back();
          auto reopened = Segment::Open(previous, config_.segment_size_bytes);
          if (!reopened.has_value()) return std::unexpected(reopened.error());
          active_ = std::make_shared<Segment>(std::move(*reopened));
        } else if (auto created = CreateActiveSegment(base); !created) {
          return std::unexpected(created.error());
        }
        break;
      }
      return std::unexpected(opened.error());
    }

    if (opened->header().shard_id != config_.shard) {
      ABYSS_LOG_CRITICAL("segment shard_id mismatch", {"path", path.string()},
                         {"expected_shard", static_cast<int64_t>(config_.shard)},
                         {"found_shard", static_cast<int64_t>(opened->header().shard_id)});
      return std::unexpected(core::Error{core::ErrorCode::kCorruption,
                                         "segment shard_id mismatch in " + path.string()});
    }

    if (expected_base_seq.has_value() && opened->header().base_seq != *expected_base_seq) {
      ABYSS_LOG_CRITICAL("segment base_seq gap", {"path", path.string()},
                         {"shard", static_cast<int64_t>(config_.shard)},
                         {"expected_base_seq", static_cast<uint64_t>(*expected_base_seq)},
                         {"found_base_seq", static_cast<uint64_t>(opened->header().base_seq)});
      return std::unexpected(core::Error{
          core::ErrorCode::kCorruption,
          "segment base_seq gap: expected " + std::to_string(*expected_base_seq) + ", got " +
              std::to_string(opened->header().base_seq) + " at " + path.string()});
    }
    expected_base_seq = opened->next_seq();

    auto segment = std::make_shared<Segment>(std::move(*opened));
    if (i + 1 == existing->size()) {
      active_ = std::move(segment);
    } else {
      if (auto sealed = segment->Seal(); !sealed.has_value()) {
        return std::unexpected(sealed.error());
      }
      sealed_.push_back(std::move(segment));
    }
  }

  // A crashed process may have left the recovered tail only in the page
  // cache. Sync it so every recovered entry is durable before replay.
  const auto active_base = active_->base_seq();
  const auto active_next = active_->next_seq();
  if (active_next > active_base) {
    const auto start = std::chrono::steady_clock::now();
    if (auto sync = active_->Fsync(); !sync.has_value()) return std::unexpected(sync.error());
    const auto elapsed = std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::steady_clock::now() - start);
    ABYSS_LOG_INFO("recovered WAL tail synced", {"shard", static_cast<int64_t>(config_.shard)},
                   {"tail_seq", static_cast<uint64_t>(active_next - 1)},
                   {"duration_us", static_cast<int64_t>(elapsed.count())});
  }

  next_seq_ = active_->next_seq();
  return {};
}

core::Result<void> ShardState::Rotate() {
  // Creating the next segment is the last step that may fail cleanly: the
  // shard is left in its pre-rotate state with no orphan file on disk.
  const SegmentHeader header{
      .format_major = kWalFormatMajor,
      .format_minor = kWalFormatMinor,
      .flags = 0,
      .shard_id = config_.shard,
      .base_seq = next_seq_,
      .created_at = core::WallClock::now(),
  };

  const auto new_path = std::filesystem::path(config_.directory) / FormatSegmentName(next_seq_);
  auto new_seg = Segment::Create(new_path.string(), header, config_.segment_size_bytes);
  if (!new_seg.has_value()) return std::unexpected(new_seg.error());

  // Durably link the new segment's name into the shard directory before it
  // becomes the active append target (QUEUE-1/NET-6).
  if (auto sync = FsyncShardDir(config_.directory); !sync.has_value()) {
    std::error_code ec;
    std::filesystem::remove(new_path, ec);
    return std::unexpected(sync.error());
  }

  // The old segment holds published entries, so a failed seal may have
  // lost acknowledged data.
  auto sealed = RunFlushHook();
  if (sealed.has_value()) sealed = active_->Seal();
  if (!sealed.has_value()) {
    core::Fatal("WAL segment seal failed on shard " + std::to_string(config_.shard) + ": " +
                sealed.error().message());
  }

  const auto sealed_base = active_->base_seq();
  const auto sealed_bytes = active_->write_offset();
  const auto sealed_entries = active_->entry_count();
  sealed_.push_back(std::move(active_));
  active_ = std::make_shared<Segment>(std::move(*new_seg));

  ABYSS_LOG_DEBUG("segment rotated", {"shard", static_cast<int64_t>(config_.shard)},
                  {"sealed_base_seq", static_cast<uint64_t>(sealed_base)},
                  {"sealed_bytes", static_cast<uint64_t>(sealed_bytes)},
                  {"sealed_entries", static_cast<uint64_t>(sealed_entries)},
                  {"new_base_seq", static_cast<uint64_t>(next_seq_)});
  return {};
}

core::Result<void> ShardState::Admit(core::SteadyTime admit_by) {
  if (config_.window == nullptr) return {};
  return config_.window->Admit(unflushed_age_, admit_by);
}

DurabilityFuture ShardState::Written(core::SequenceId last_seq, uint64_t entries, uint64_t bytes) {
  published_bytes_ += bytes;
  appended_.Increment(static_cast<double>(entries));
  if (config_.window != nullptr) config_.window->Add(bytes);
  unflushed_age_.Start(DurabilityWindow::Clock::now());
  if (config_.ack_durability == core::Durability::kPowerLoss) {
    return committer_->WhenDurable(last_seq);
  }
  return ReadyFuture();
}

core::Result<PendingAppend> ShardState::BeginAppend(core::QueueEntry entry,
                                                    core::SteadyTime admit_by) {
  if (auto admitted = Admit(admit_by); !admitted.has_value()) {
    return std::unexpected(admitted.error());
  }

  std::unique_lock lock(append_mu_);
  if (shutting_down_) {
    return std::unexpected(core::Error{core::ErrorCode::kUnavailable, "queue shutting down"});
  }

  entry.seq = next_seq_;

  std::vector<std::byte> buf;
  EncodeWalEntry(entry, entry.seq, buf);

  // Cap on the entry size, not the segment size: a value within
  // max_value_size_bytes is always acceptable. The validator guarantees a
  // fixed-size segment can hold one max-size entry, so a value at the ceiling
  // fits after at most one rotation (no jumbo segments — Decision 2).
  if (buf.size() > config_.max_value_size_bytes) {
    return std::unexpected(core::Error{core::ErrorCode::kValueTooLarge,
                                       "entry of " + std::to_string(buf.size()) +
                                           " bytes exceeds queue.max_value_size_bytes (" +
                                           std::to_string(config_.max_value_size_bytes) + ")"});
  }

  bool rotated = false;
  if (active_->SpaceRemaining() < buf.size()) {
    auto r = Rotate();
    if (!r.has_value()) return std::unexpected(r.error());
    rotated = true;
  }

  auto written = active_->AppendEncoded(buf, entry.seq);
  if (!written.has_value()) return std::unexpected(written.error());
  next_seq_ = entry.seq + 1;

  const core::SequenceId seq = entry.seq;
  DurabilityFuture future = Written(seq, 1, *written);

  auto publisher = std::make_unique<ShardStatePublisher>(read_cv_, std::move(lock), *committer_,
                                                         next_seq_, config_.on_rotate, rotated);
  return PendingAppend{seq, std::move(future), std::move(publisher)};
}

core::Result<PendingBatchAppend> ShardState::BeginAppendBatch(
    std::span<const core::QueueEntry> entries, core::SteadyTime admit_by) {
  if (entries.empty()) {
    return std::unexpected(core::Error{core::ErrorCode::kInvalidArgument, "empty batch"});
  }
  if (auto admitted = Admit(admit_by); !admitted.has_value()) {
    return std::unexpected(admitted.error());
  }

  std::unique_lock lock(append_mu_);
  if (shutting_down_) {
    return std::unexpected(core::Error{core::ErrorCode::kUnavailable, "queue shutting down"});
  }

  std::vector<core::QueueEntry> owned(entries.begin(), entries.end());
  for (size_t i = 0; i < owned.size(); ++i) {
    owned[i].seq = next_seq_ + i;
  }
  const core::SequenceId first_seq = owned.front().seq;
  const core::SequenceId last_seq = owned.back().seq;

  std::vector<std::byte> encoded;
  std::vector<size_t> sizes;
  sizes.reserve(owned.size());
  for (const auto& entry : owned) {
    const size_t size = EncodeWalEntry(entry, last_seq, encoded);
    if (size > config_.max_value_size_bytes) {
      return std::unexpected(core::Error{core::ErrorCode::kValueTooLarge,
                                         "batch entry of " + std::to_string(size) +
                                             " bytes exceeds queue.max_value_size_bytes (" +
                                             std::to_string(config_.max_value_size_bytes) + ")"});
    }
    sizes.push_back(size);
  }
  const size_t total_bytes = encoded.size();

  // A batch is appended atomically to one segment after at most one rotation,
  // so it must fit a single fixed-size segment. Phrase the check as an addition
  // to avoid the unsigned underflow a small segment_size_bytes used to trigger
  // (QUEUE-4); the validator floors segment_size_bytes above the header.
  if (total_bytes + kSegmentHeaderSize > config_.segment_size_bytes) {
    return std::unexpected(
        core::Error{core::ErrorCode::kResourceExhausted, "batch exceeds segment capacity"});
  }

  bool rotated = false;
  if (active_->SpaceRemaining() < total_bytes) {
    auto r = Rotate();
    if (!r.has_value()) return std::unexpected(r.error());
    rotated = true;
  }

  auto written = active_->AppendEncodedBatch(encoded, sizes, first_seq);
  if (!written.has_value()) return std::unexpected(written.error());
  next_seq_ = last_seq + 1;

  DurabilityFuture future = Written(last_seq, sizes.size(), *written);

  auto publisher = std::make_unique<ShardStatePublisher>(read_cv_, std::move(lock), *committer_,
                                                         next_seq_, config_.on_rotate, rotated);
  return PendingBatchAppend{first_seq, last_seq, std::move(future), std::move(publisher)};
}

core::Result<AppendResult> ShardState::Append(core::QueueEntry entry, core::SteadyTime admit_by) {
  auto pending = BeginAppend(std::move(entry), admit_by);
  if (!pending.has_value()) return std::unexpected(pending.error());
  const core::SequenceId seq = pending->seq();
  DurabilityFuture durable = std::move(pending->durable());
  pending->Publish();
  return AppendResult{.seq = seq, .durable = std::move(durable)};
}

core::Result<AppendBatchResult> ShardState::AppendBatch(std::span<const core::QueueEntry> entries,
                                                        core::SteadyTime admit_by) {
  auto pending = BeginAppendBatch(entries, admit_by);
  if (!pending.has_value()) return std::unexpected(pending.error());
  const core::SequenceId first = pending->first_seq();
  const core::SequenceId last = pending->last_seq();
  DurabilityFuture durable = std::move(pending->durable());
  pending->Publish();
  return AppendBatchResult{.first_seq = first, .last_seq = last, .durable = std::move(durable)};
}

core::Result<void> ShardState::CheckReadable(core::SequenceId from_seq) const {
  if (shutting_down_) {
    return std::unexpected(core::Error{core::ErrorCode::kUnavailable, "queue shutting down"});
  }
  if (const core::SequenceId first = FirstSeqLocked(); from_seq < first) {
    metrics::Registry::Instance().Counter(metrics::names::kQueueReadOutOfRangeTotal).Increment();
    return std::unexpected(core::Error{core::ErrorCode::kOutOfRange,
                                       "read from seq " + std::to_string(from_seq) +
                                           " below first retained seq " + std::to_string(first) +
                                           " on shard " + std::to_string(config_.shard)});
  }
  return {};
}

core::Result<std::vector<core::QueueEntry>> ShardState::Read(core::SequenceId from_seq,
                                                             size_t max_count,
                                                             core::Duration timeout,
                                                             core::Durability visible) {
  const bool power_loss = visible == core::Durability::kPowerLoss;
  if (power_loss) {
    {
      const std::scoped_lock lock(append_mu_);
      if (auto readable = CheckReadable(from_seq); !readable) {
        return std::unexpected(readable.error());
      }
    }
    // The commit thread wakes this on every advance of the durable end.
    committer_->AwaitDurable(from_seq, timeout);
  }

  std::vector<std::shared_ptr<Segment>> snapshot;
  core::SequenceId visible_end = 0;
  {
    std::unique_lock lock(append_mu_);
    if (auto readable = CheckReadable(from_seq); !readable)
      return std::unexpected(readable.error());
    if (!power_loss) {
      read_cv_.wait_for(lock, timeout, [this, from_seq] ABYSS_REQUIRES(append_mu_) {
        return shutting_down_ || next_seq_ > from_seq;
      });
      if (auto readable = CheckReadable(from_seq); !readable) {
        return std::unexpected(readable.error());
      }
    }

    visible_end = power_loss ? std::min(next_seq_, committer_->DurableEnd()) : next_seq_;
    if (visible_end <= from_seq) {
      return std::vector<core::QueueEntry>{};
    }

    snapshot.reserve(sealed_.size() + 1);
    for (const auto& seg : sealed_) {
      if (seg->next_seq() > from_seq) {
        snapshot.push_back(seg);
      }
    }
    snapshot.push_back(active_);
  }

  max_count = std::min(max_count, static_cast<size_t>(visible_end - from_seq));

  std::vector<core::QueueEntry> result;
  result.reserve(std::min<size_t>(max_count, 256));
  for (const auto& seg : snapshot) {
    if (result.size() >= max_count) break;
    if (seg->next_seq() <= from_seq) continue;

    auto read = seg->ReadEntriesFrom(from_seq, max_count - result.size());
    if (!read.has_value()) return std::unexpected(read.error());
    for (auto& entry : read->entries) {
      result.push_back(std::move(entry));
    }
  }
  return result;
}

core::SequenceId ShardState::head_seq() const {
  const std::scoped_lock lock(append_mu_);
  return next_seq_;
}

core::SequenceId ShardState::first_seq() const {
  const std::scoped_lock lock(append_mu_);
  return FirstSeqLocked();
}

core::SequenceId ShardState::FirstSeqLocked() const {
  if (!sealed_.empty()) {
    return sealed_.front()->base_seq();
  }
  return active_ ? active_->base_seq() : 0;
}

core::SequenceId ShardState::DurableEnd(core::Durability durability) const {
  if (durability == core::Durability::kPowerLoss) return committer_->DurableEnd();
  return head_seq();
}

bool ShardState::AwaitDurable(core::SequenceId seq, core::Durability durability,
                              core::Duration timeout) const {
  if (durability == core::Durability::kPowerLoss) return committer_->AwaitDurable(seq, timeout);
  std::unique_lock lock(append_mu_);
  read_cv_.wait_for(lock, timeout, [this, seq] ABYSS_REQUIRES(append_mu_) {
    return shutting_down_ || next_seq_ > seq;
  });
  return next_seq_ > seq;
}

core::Duration ShardState::DurabilityLag() const {
  return std::chrono::duration_cast<core::Duration>(
      unflushed_age_.Age(DurabilityWindow::Clock::now()));
}

core::Result<void> ShardState::RunFlushHook() const {
  FlushHook hook;
  {
    const std::scoped_lock lock(hook_mu_);
    hook = flush_hook_;
  }
  if (!hook) return {};
  return hook(config_.shard);
}

core::Result<GroupCommitter::Extent> ShardState::Flush() {
  std::shared_ptr<Segment> segment;
  GroupCommitter::Extent extent;
  {
    const std::scoped_lock lock(append_mu_);
    segment = active_;
    extent = {.end = next_seq_, .bytes = published_bytes_};
    snapshot_base_ = segment->base_seq();
    snapshot_offset_ = segment->write_offset();
    snapshot_time_ = DurabilityWindow::Clock::now();
  }
  // Entries below the snapshot are in `segment` or in a segment sealed
  // (and synced) before it became inactive.
  if (auto hook = RunFlushHook(); !hook.has_value()) return std::unexpected(hook.error());
  if (auto sync = segment->Fsync(); !sync.has_value()) return std::unexpected(sync.error());
  return extent;
}

void ShardState::Flushed(GroupCommitter::Extent previous, GroupCommitter::Extent flushed) {
  {
    const std::scoped_lock lock(append_mu_);
    if (next_seq_ > flushed.end) {
      unflushed_age_.Set(snapshot_time_);
    } else {
      unflushed_age_.Clear();
    }
    flushed_base_ = snapshot_base_;
    flushed_offset_ = snapshot_offset_;
  }
  if (config_.window != nullptr) config_.window->Release(flushed.bytes - previous.bytes);
}

size_t ShardState::total_entries() const {
  const std::scoped_lock lock(append_mu_);
  size_t total = 0;
  for (const auto& seg : sealed_) total += seg->entry_count();
  if (active_) total += active_->entry_count();
  return total;
}

size_t ShardState::total_bytes() const {
  const std::scoped_lock lock(append_mu_);
  size_t total = 0;
  for (const auto& seg : sealed_) total += seg->write_offset();
  if (active_) total += active_->write_offset();
  return total;
}

std::vector<SegmentRegistry::SealedSegmentInfo> ShardState::ListSealedSegments() const {
  const std::scoped_lock lock(append_mu_);
  std::vector<SegmentRegistry::SealedSegmentInfo> result;
  result.reserve(sealed_.size());
  for (const auto& seg : sealed_) {
    result.push_back({
        .path = seg->path(),
        .shard = config_.shard,
        .base_seq = seg->base_seq(),
        .last_seq = seg->next_seq() == 0 ? 0 : seg->next_seq() - 1,
        .created_at = seg->header().created_at,
    });
  }
  return result;
}

core::Result<void> ShardState::RemoveSegment(core::SequenceId base_seq) {
  std::shared_ptr<Segment> to_remove;
  std::string path;
  {
    const std::scoped_lock lock(append_mu_);
    auto it = std::ranges::find_if(
        sealed_, [&](const std::shared_ptr<Segment>& s) { return s->base_seq() == base_seq; });
    if (it == sealed_.end()) {
      return {};
    }
    to_remove = *it;
    path = to_remove->path();
  }

  // Unlink BEFORE deregistering. Dropping the segment first and failing to
  // unlink would leave the file on disk with nothing tracking it: no later
  // sweep would retry it, it would vanish from the retention stats, and the
  // stuck-reclamation age would read as healthy while the bytes remain.
  std::error_code ec;
  std::filesystem::remove(path, ec);
  if (ec) {
    return std::unexpected(
        core::Error{core::ErrorCode::kInternal, "unlink segment: " + ec.message()});
  }

  const std::scoped_lock lock(append_mu_);
  auto it = std::ranges::find_if(
      sealed_, [&](const std::shared_ptr<Segment>& s) { return s->base_seq() == base_seq; });
  if (it != sealed_.end()) {
    sealed_.erase(it);
  }
  return {};
}

void ShardState::Shutdown() {
  core::SequenceId end = 0;
  {
    const std::scoped_lock lock(append_mu_);
    if (shutting_down_) return;
    shutting_down_ = true;
    end = next_seq_;
  }
  read_cv_.notify_all();

  if (committer_) {
    const bool final_flush = !skip_final_flush_.load(std::memory_order_acquire);
    // A publisher may not have reported its end yet; the final flush
    // must still cover it.
    if (final_flush) committer_->Published(end);
    committer_->Stop(final_flush);
  }
}

void ShardState::SetFlushHookForTesting(FlushHook hook) {
  const std::scoped_lock lock(hook_mu_);
  flush_hook_ = std::move(hook);
}

FlushedExtent ShardState::FlushedExtentForTesting() const {
  const std::scoped_lock lock(append_mu_);
  // A segment created since the last flush has only its synced header.
  const size_t offset = active_->base_seq() == flushed_base_ ? flushed_offset_ : kSegmentHeaderSize;
  return FlushedExtent{.path = active_->path(), .offset = offset};
}

void ShardState::SkipFinalFlushForTesting() {
  skip_final_flush_.store(true, std::memory_order_release);
}

}  // namespace abyss::queue
