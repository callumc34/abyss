#include "shard_state.h"

#include <algorithm>
#include <filesystem>
#include <iomanip>
#include <regex>
#include <sstream>
#include <utility>

#include "abyss/queue/segment_header.h"
#include "abyss/queue/wal_entry.h"

namespace abyss::queue {

namespace {

constexpr int kSegmentNameWidth = 20;

std::string FormatSegmentName(core::SequenceId base_seq) {
  std::ostringstream oss;
  oss << std::setw(kSegmentNameWidth) << std::setfill('0') << base_seq << ".log";
  return oss.str();
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
    result.push_back(static_cast<core::SequenceId>(std::stoull(match[1].str())));
  }
  std::ranges::sort(result);
  return result;
}

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
  return state;
}

ShardState::ShardState(ShardStateConfig config) : config_(std::move(config)) {}

ShardState::~ShardState() { Shutdown(); }

core::Result<void> ShardState::Initialize() {
  std::lock_guard lock(append_mu_);

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

  committer_ = std::make_unique<GroupCommitter>(config_.commit, make_fsync_fn(active_));
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

  active_ = std::make_shared<Segment>(std::move(*seg));
  next_seq_ = active_->next_seq();
  return {};
}

core::Result<void> ShardState::OpenExistingSegments() {
  auto existing = EnumerateSegmentBaseSeqs(config_.directory);
  if (!existing.has_value()) return std::unexpected(existing.error());

  sealed_.clear();
  sealed_.reserve(existing->size() > 0 ? existing->size() - 1 : 0);

  for (size_t i = 0; i < existing->size(); ++i) {
    const auto path = std::filesystem::path(config_.directory) / FormatSegmentName((*existing)[i]);
    auto opened = Segment::Open(path.string(), config_.segment_size_bytes);
    if (!opened.has_value()) return std::unexpected(opened.error());

    if (opened->header().shard_id != config_.shard) {
      return std::unexpected(core::Error{core::ErrorCode::kCorruption,
                                         "segment shard_id mismatch in " + path.string()});
    }

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

  next_seq_ = active_->next_seq();
  return {};
}

core::Result<void> ShardState::Rotate() {
  auto drain = committer_->Drain();
  if (!drain.has_value()) return std::unexpected(drain.error());

  if (auto sealed = active_->Seal(); !sealed.has_value()) {
    return std::unexpected(sealed.error());
  }
  sealed_.push_back(std::move(active_));

  const SegmentHeader header{
      .format_major = kWalFormatMajor,
      .format_minor = kWalFormatMinor,
      .flags = 0,
      .shard_id = config_.shard,
      .base_seq = next_seq_,
      .created_at = core::WallClock::now(),
  };

  const auto path = std::filesystem::path(config_.directory) / FormatSegmentName(next_seq_);
  auto seg = Segment::Create(path.string(), header, config_.segment_size_bytes);
  if (!seg.has_value()) return std::unexpected(seg.error());

  active_ = std::make_shared<Segment>(std::move(*seg));
  committer_->SetFsyncFn([captured = active_] { return captured->Fsync(); });
  return {};
}

core::Result<size_t> ShardState::AppendUnlocked(const core::QueueEntry& entry,
                                                core::SequenceId batch_last_seq) {
  auto written = active_->Append(entry, batch_last_seq);
  if (!written.has_value()) return std::unexpected(written.error());
  next_seq_ = entry.seq + 1;
  return written;
}

// NOLINTNEXTLINE(readability-make-member-function-const)
core::Result<AppendResult> ShardState::Append(core::QueueEntry entry) {
  bool rotated = false;
  core::SequenceId seq = 0;
  DurabilityFuture future;
  {
    std::unique_lock lock(append_mu_);
    if (shutting_down_) {
      return std::unexpected(core::Error{core::ErrorCode::kUnavailable, "queue shutting down"});
    }

    entry.seq = next_seq_;

    std::vector<std::byte> tmp;
    const size_t encoded = EncodeWalEntry(entry, entry.seq, tmp);

    if (encoded > config_.segment_size_bytes - kSegmentHeaderSize) {
      return std::unexpected(
          core::Error{core::ErrorCode::kInvalidArgument, "entry exceeds segment capacity"});
    }

    if (active_->SpaceRemaining() < encoded) {
      auto r = Rotate();
      if (!r.has_value()) return std::unexpected(r.error());
      rotated = true;
    }

    auto written = AppendUnlocked(entry, entry.seq);
    if (!written.has_value()) return std::unexpected(written.error());

    seq = entry.seq;
    read_cv_.notify_all();
    future = committer_->Submit(*written);
  }

  if (rotated && config_.on_rotate) {
    config_.on_rotate();
  }
  return AppendResult{.seq = seq, .durable = std::move(future)};
}

core::Result<AppendBatchResult> ShardState::AppendBatch(std::span<const core::QueueEntry> entries) {
  if (entries.empty()) {
    return std::unexpected(core::Error{core::ErrorCode::kInvalidArgument, "empty batch"});
  }

  bool rotated = false;
  core::SequenceId first_seq = 0;
  core::SequenceId last_seq = 0;
  DurabilityFuture future;
  {
    std::unique_lock lock(append_mu_);
    if (shutting_down_) {
      return std::unexpected(core::Error{core::ErrorCode::kUnavailable, "queue shutting down"});
    }

    std::vector<core::QueueEntry> owned(entries.begin(), entries.end());
    for (size_t i = 0; i < owned.size(); ++i) {
      owned[i].seq = next_seq_ + i;
    }
    first_seq = owned.front().seq;
    last_seq = owned.back().seq;

    size_t total_bytes = 0;
    for (const auto& entry : owned) {
      std::vector<std::byte> tmp;
      total_bytes += EncodeWalEntry(entry, last_seq, tmp);
    }

    const size_t capacity = config_.segment_size_bytes - kSegmentHeaderSize;
    if (total_bytes > capacity) {
      return std::unexpected(
          core::Error{core::ErrorCode::kInvalidArgument, "batch exceeds segment capacity"});
    }

    if (active_->SpaceRemaining() < total_bytes) {
      auto r = Rotate();
      if (!r.has_value()) return std::unexpected(r.error());
      rotated = true;
    }

    size_t written_bytes = 0;
    for (const auto& entry : owned) {
      auto w = AppendUnlocked(entry, last_seq);
      if (!w.has_value()) return std::unexpected(w.error());
      written_bytes += *w;
    }

    read_cv_.notify_all();
    future = committer_->Submit(written_bytes);
  }

  if (rotated && config_.on_rotate) {
    config_.on_rotate();
  }
  return AppendBatchResult{
      .first_seq = first_seq, .last_seq = last_seq, .durable = std::move(future)};
}

core::Result<std::vector<core::QueueEntry>> ShardState::Read(core::SequenceId from_seq,
                                                             size_t max_count,
                                                             core::Duration timeout) {
  std::vector<std::shared_ptr<Segment>> snapshot;
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

    snapshot.reserve(sealed_.size() + 1);
    for (const auto& seg : sealed_) {
      if (seg->next_seq() > from_seq) {
        snapshot.push_back(seg);
      }
    }
    snapshot.push_back(active_);
  }

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
  std::lock_guard lock(append_mu_);
  return next_seq_;
}

core::SequenceId ShardState::tail_seq() const {
  std::lock_guard lock(append_mu_);
  if (!sealed_.empty()) {
    return sealed_.front()->base_seq();
  }
  return active_ ? active_->base_seq() : 0;
}

size_t ShardState::total_entries() const {
  std::lock_guard lock(append_mu_);
  size_t total = 0;
  for (const auto& seg : sealed_) total += seg->entry_count();
  if (active_) total += active_->entry_count();
  return total;
}

size_t ShardState::total_bytes() const {
  std::lock_guard lock(append_mu_);
  size_t total = 0;
  for (const auto& seg : sealed_) total += seg->write_offset();
  if (active_) total += active_->write_offset();
  return total;
}

std::vector<SegmentRegistry::SealedSegmentInfo> ShardState::ListSealedSegments() const {
  std::lock_guard lock(append_mu_);
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
    std::lock_guard lock(append_mu_);
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
    std::lock_guard lock(append_mu_);
    if (shutting_down_) return;
    shutting_down_ = true;
  }
  read_cv_.notify_all();

  if (committer_) {
    committer_->Stop();
  }
}

}  // namespace abyss::queue
