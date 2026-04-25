#pragma once

namespace abyss::resp {

// Implementations must be thread-safe.
class LoadingStateProvider {
 public:
  LoadingStateProvider() = default;
  virtual ~LoadingStateProvider() = default;
  LoadingStateProvider(const LoadingStateProvider&) = delete;
  LoadingStateProvider& operator=(const LoadingStateProvider&) = delete;
  LoadingStateProvider(LoadingStateProvider&&) = delete;
  LoadingStateProvider& operator=(LoadingStateProvider&&) = delete;

  virtual bool IsLoading() const = 0;
};

}  // namespace abyss::resp
