#include "abyss/queue/group_commit.h"

#include <utility>

namespace abyss::queue {

namespace {

DurabilityFuture MakeReadyFuture(core::Result<void> value) {
  std::promise<core::Result<void>> p;
  p.set_value(std::move(value));
  return p.get_future();
}

}  // namespace

GroupCommitter::GroupCommitter(GroupCommitConfig config, FsyncFn fsync_fn)
    : config_(config), fsync_fn_(std::move(fsync_fn)) {
  if (config_.policy == FsyncPolicy::kGroupCommit) {
    thread_ = std::thread([this] { Run(); });
  }
}

GroupCommitter::~GroupCommitter() { Stop(); }

DurabilityFuture GroupCommitter::Submit(size_t bytes) {
  switch (config_.policy) {
    case FsyncPolicy::kNone:
      return MakeReadyFuture({});

    case FsyncPolicy::kPerWrite: {
      auto result = fsync_fn_();
      return MakeReadyFuture(std::move(result));
    }

    case FsyncPolicy::kGroupCommit: {
      std::promise<core::Result<void>> promise;
      auto future = promise.get_future();
      {
        std::lock_guard lock(mu_);
        if (stopped_) {
          return MakeReadyFuture(std::unexpected(
              core::Error{core::ErrorCode::kUnavailable, "group committer stopped"}));
        }
        pending_.push_back({.promise = std::move(promise), .bytes = bytes});
        pending_bytes_ += bytes;
        if (pending_bytes_ >= config_.max_bytes) {
          flush_requested_ = true;
        }
      }
      cv_.notify_one();
      return future;
    }
  }
  return MakeReadyFuture(
      std::unexpected(core::Error{core::ErrorCode::kInternal, "unknown fsync policy"}));
}

core::Result<void> GroupCommitter::Drain() {
  if (config_.policy == FsyncPolicy::kNone) {
    return {};
  }
  if (config_.policy == FsyncPolicy::kPerWrite) {
    return fsync_fn_();
  }

  std::promise<core::Result<void>> promise;
  auto future = promise.get_future();
  {
    std::lock_guard lock(mu_);
    if (stopped_) {
      return std::unexpected(core::Error{core::ErrorCode::kUnavailable, "group committer stopped"});
    }
    pending_.push_back({.promise = std::move(promise), .bytes = 0});
    flush_requested_ = true;
  }
  cv_.notify_one();
  return future.get();
}

void GroupCommitter::Stop() {
  {
    std::lock_guard lock(mu_);
    if (stopped_) return;
    stopped_ = true;
    flush_requested_ = true;
  }
  cv_.notify_all();
  if (thread_.joinable()) {
    thread_.join();
  }
}

void GroupCommitter::Run() {
  std::unique_lock lock(mu_);
  while (true) {
    cv_.wait(lock, [this] { return !pending_.empty() || stopped_; });

    if (pending_.empty()) {
      // stopped_ must be true here.
      return;
    }

    // NOLINTNEXTLINE(cppcoreguidelines-init-variables)
    const bool should_coalesce =
        !stopped_ && config_.interval.count() > 0 && pending_bytes_ < config_.max_bytes;
    if (should_coalesce) {
      cv_.wait_for(lock, config_.interval, [this] {
        return stopped_ || flush_requested_ || pending_bytes_ >= config_.max_bytes;
      });
    }

    auto batch = std::exchange(pending_, {});
    pending_bytes_ = 0;
    flush_requested_ = false;
    auto fsync_fn = fsync_fn_;
    lock.unlock();

    core::Result<void> result = fsync_fn();

    for (auto& entry : batch) {
      entry.promise.set_value(result);
    }

    lock.lock();
  }
}

}  // namespace abyss::queue
