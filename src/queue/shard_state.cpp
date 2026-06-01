#include "shard_state.h"

#include <algorithm>
#include <charconv>
#include <filesystem>
#include <iomanip>
#include <optional>
#include <regex>
#include <sstream>
#include <string_view>
#include <utility>

#include "abyss/log/log.h"
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

class ShardStatePublisher final : public AppendPublisher {
 public:
  ShardStatePublisher(std::condition_variable& cv, std::unique_lock<std::mutex> lock,
                      std::function<void()> on_rotate, bool rotated) noexcept
      : cv_(cv), lock_(std::move(lock)), on_rotate_(std::move(on_rotate)), rotated_(rotated) {}

  void Publish() noexcept override {
    if (!lock_.owns_lock()) return;
    cv_.notify_all();
    lock_.unlock();
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
                   {"tail_seq", static_cast<uint64_t>(state->tail_seq())});
  } else {
    ABYSS_LOG_DEBUG("shard opened", {"shard", static_cast<int64_t>(state->config_.shard)});
  }
  return state;
}

ShardState::ShardState(ShardStateConfig config) : config_(std::move(config)) {}

ShardState::~ShardState() { Shutdown(); }

core::Result<void> ShardState::Initialize() {
  const std::scoped_lock lock(append_mu_);

  auto existing = EnumerateSegmentBaseSeqs(config_.directory);
  if (!existing.has_value()) return std::unexpected(existing.error());

  if (existing->empty()) {
    auto created = CreateInitialSegment();
    if (!created.has_value()) return std::unexpected(created.error());
  } else {
    auto opened = OpenExistingSegments();
    if (!opened.has_value()) return std::unexpected(opened.error());
  }

  auto make_fsync_fn = [](std::shared_ptr<Segment> seg) -> GroupCommitter::FsyncFn {
    return [captured = std::move(seg)] { return captured->Fsync(); };
  };

  committer_ = std::make_shared<GroupCommitter>(config_.commit, make_fsync_fn(active_));
  return {};
}

core::Result<void> ShardState::CreateInitialSegment() {
  const SegmentHeader header{
      .format_major = kWalFormatMajor,
      .format_minor = kWalFormatMinor,
      .flags = 0,
      .shard_id = config_.shard,
      .base_seq = 0,
      .created_at = core::WallClock::now(),
  };

  const auto path = std::filesystem::path(config_.directory) / FormatSegmentName(0);
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
    if (!opened.has_value()) return std::unexpected(opened.error());

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
      const auto sealed_base = segment->base_seq();
      const auto sealed_next = segment->next_seq();
      if (sealed_next > sealed_base) {
        // Segment holds at least one committed entry; it is fully fsynced.
        highest_sealed_seq_ = std::max(highest_sealed_seq_, sealed_next - 1);
        has_sealed_durable_ = true;
      }
      sealed_.push_back(std::move(segment));
    }
  }

  next_seq_ = active_->next_seq();
  return {};
}

core::Result<void> ShardState::Rotate() {
  // All fallible steps run before any state mutation. If any step fails, the
  // shard is left in its pre-rotate state: active_ still points at the old
  // segment, committer_ still bound to it, no orphan files on disk.
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

  // If a later fallible step fails after we've created the new file on disk,
  // unlink it so a later Open doesn't see an orphan that would be misread as
  // the active segment.
  const auto unlink_orphan = [&new_path] {
    std::error_code ec;
    std::filesystem::remove(new_path, ec);
  };

  // Durably link the new segment's name into the shard directory before it
  // becomes the active append target (QUEUE-1/NET-6). FsyncDir failure leaves
  // us in the pre-rotate state with the orphan removed.
  if (auto sync = FsyncShardDir(config_.directory); !sync.has_value()) {
    unlink_orphan();
    return std::unexpected(sync.error());
  }

  auto drain = committer_->Drain();
  if (!drain.has_value()) {
    unlink_orphan();
    return std::unexpected(drain.error());
  }

  if (auto sealed = active_->Seal(); !sealed.has_value()) {
    unlink_orphan();
    return std::unexpected(sealed.error());
  }

  // Every fallible step has succeeded. We can commit. The sealed segment is
  // fully fsynced, so its highest seq is durable even after the committer swap.
  const auto sealed_base = active_->base_seq();
  const auto sealed_bytes = active_->write_offset();
  const auto sealed_entries = active_->entry_count();
  const auto sealed_next = active_->next_seq();
  if (sealed_next > sealed_base) {
    highest_sealed_seq_ = std::max(highest_sealed_seq_, sealed_next - 1);
    has_sealed_durable_ = true;
  }
  sealed_.push_back(std::move(active_));
  committer_.reset();
  active_ = std::make_shared<Segment>(std::move(*new_seg));
  committer_ = std::make_shared<GroupCommitter>(config_.commit,
                                                [captured = active_] { return captured->Fsync(); });

  ABYSS_LOG_DEBUG("segment rotated", {"shard", static_cast<int64_t>(config_.shard)},
                  {"sealed_base_seq", static_cast<uint64_t>(sealed_base)},
                  {"sealed_bytes", static_cast<uint64_t>(sealed_bytes)},
                  {"sealed_entries", static_cast<uint64_t>(sealed_entries)},
                  {"new_base_seq", static_cast<uint64_t>(next_seq_)});
  return {};
}

// NOLINTNEXTLINE(readability-make-member-function-const)
core::Result<PendingAppend> ShardState::BeginAppend(core::QueueEntry entry) {
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
  DurabilityFuture future = committer_->Submit(*written, seq);

  auto publisher =
      std::make_unique<ShardStatePublisher>(read_cv_, std::move(lock), config_.on_rotate, rotated);
  return PendingAppend{seq, std::move(future), std::move(publisher)};
}

core::Result<PendingBatchAppend> ShardState::BeginAppendBatch(
    std::span<const core::QueueEntry> entries) {
  if (entries.empty()) {
    return std::unexpected(core::Error{core::ErrorCode::kInvalidArgument, "empty batch"});
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

  std::vector<std::vector<std::byte>> encoded;
  encoded.reserve(owned.size());
  size_t total_bytes = 0;
  for (const auto& entry : owned) {
    std::vector<std::byte> buf;
    EncodeWalEntry(entry, last_seq, buf);
    if (buf.size() > config_.max_value_size_bytes) {
      return std::unexpected(core::Error{core::ErrorCode::kValueTooLarge,
                                         "batch entry of " + std::to_string(buf.size()) +
                                             " bytes exceeds queue.max_value_size_bytes (" +
                                             std::to_string(config_.max_value_size_bytes) + ")"});
    }
    total_bytes += buf.size();
    encoded.push_back(std::move(buf));
  }

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

  size_t written_bytes = 0;
  for (size_t i = 0; i < owned.size(); ++i) {
    auto w = active_->AppendEncoded(encoded[i], owned[i].seq);
    if (!w.has_value()) return std::unexpected(w.error());
    written_bytes += *w;
    next_seq_ = owned[i].seq + 1;
  }

  DurabilityFuture future = committer_->Submit(written_bytes, last_seq);

  auto publisher =
      std::make_unique<ShardStatePublisher>(read_cv_, std::move(lock), config_.on_rotate, rotated);
  return PendingBatchAppend{first_seq, last_seq, std::move(future), std::move(publisher)};
}

core::Result<AppendResult> ShardState::Append(core::QueueEntry entry) {
  auto pending = BeginAppend(std::move(entry));
  if (!pending.has_value()) return std::unexpected(pending.error());
  const core::SequenceId seq = pending->seq();
  DurabilityFuture durable = std::move(pending->durable());
  pending->Publish();
  return AppendResult{.seq = seq, .durable = std::move(durable)};
}

core::Result<AppendBatchResult> ShardState::AppendBatch(std::span<const core::QueueEntry> entries) {
  auto pending = BeginAppendBatch(entries);
  if (!pending.has_value()) return std::unexpected(pending.error());
  const core::SequenceId first = pending->first_seq();
  const core::SequenceId last = pending->last_seq();
  DurabilityFuture durable = std::move(pending->durable());
  pending->Publish();
  return AppendBatchResult{.first_seq = first, .last_seq = last, .durable = std::move(durable)};
}

core::Result<std::vector<core::QueueEntry>> ShardState::Read(core::SequenceId from_seq,
                                                             size_t max_count,
                                                             core::Duration timeout) {
  std::vector<std::shared_ptr<Segment>> snapshot;
  core::SequenceId publish_cliff = 0;
  {
    std::unique_lock lock(append_mu_);
    read_cv_.wait_for(lock, timeout,
                      [this, from_seq] { return shutting_down_ || next_seq_ > from_seq; });

    if (shutting_down_) {
      return std::unexpected(core::Error{core::ErrorCode::kUnavailable, "queue shutting down"});
    }
    if (next_seq_ <= from_seq) {
      return std::vector<core::QueueEntry>{};
    }

    publish_cliff = next_seq_;

    snapshot.reserve(sealed_.size() + 1);
    for (const auto& seg : sealed_) {
      if (seg->next_seq() > from_seq) {
        snapshot.push_back(seg);
      }
    }
    snapshot.push_back(active_);
  }

  max_count = std::min(max_count, static_cast<size_t>(publish_cliff - from_seq));

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

core::SequenceId ShardState::tail_seq() const {
  const std::scoped_lock lock(append_mu_);
  if (!sealed_.empty()) {
    return sealed_.front()->base_seq();
  }
  return active_ ? active_->base_seq() : 0;
}

core::SequenceId ShardState::DurableSeq() const {
  const std::scoped_lock lock(append_mu_);
  core::SequenceId durable = highest_sealed_seq_;
  if (committer_) {
    durable = std::max(durable, committer_->DurableSeq());
  }
  return durable;
}

bool ShardState::HasDurable() const {
  const std::scoped_lock lock(append_mu_);
  if (has_sealed_durable_) return true;
  return committer_ && committer_->HasDurable();
}

bool ShardState::AwaitDurable(core::SequenceId seq, core::Duration timeout) const {
  // Snapshot the active committer under the lock (a shared_ptr keeps it alive
  // across a concurrent Rotate), then wait without the lock so the commit
  // thread can advance the watermark and appends can proceed.
  std::shared_ptr<GroupCommitter> committer;
  {
    const std::scoped_lock lock(append_mu_);
    // A sealed segment covering seq is already durable. Guard the seq-0 case
    // with has_sealed_durable_ so a 0 watermark with no sealed data does not
    // falsely satisfy the wait.
    if (has_sealed_durable_ && highest_sealed_seq_ >= seq) return true;
    committer = committer_;
  }
  if (!committer) return false;
  return committer->AwaitDurable(seq, timeout);
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
    to_remove = std::move(*it);
    sealed_.erase(it);
    path = to_remove->path();
  }

  std::error_code ec;
  std::filesystem::remove(path, ec);
  if (ec) {
    return std::unexpected(
        core::Error{core::ErrorCode::kInternal, "unlink segment: " + ec.message()});
  }
  return {};
}

void ShardState::Shutdown() {
  {
    const std::scoped_lock lock(append_mu_);
    if (shutting_down_) return;
    shutting_down_ = true;
  }
  read_cv_.notify_all();

  if (committer_) {
    committer_->Stop();
  }
}

}  // namespace abyss::queue
