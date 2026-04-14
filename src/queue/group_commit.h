#pragma once

#include <chrono>
#include <cstddef>

namespace abyss::queue {

struct GroupCommitConfig {
  std::chrono::microseconds interval{1000};
  size_t max_bytes = 1048576;
};

class GroupCommitter {
 public:
  explicit GroupCommitter(GroupCommitConfig config);

 private:
  GroupCommitConfig config_;
};

}  // namespace abyss::queue
