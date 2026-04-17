#pragma once

#include <cstddef>
#include <string>

#include "abyss/core/types.h"

namespace abyss::queue {

class Segment {
 public:
  Segment(std::string path, core::SequenceId base_offset, size_t max_size);

 private:
  [[maybe_unused]] std::string path_;
  [[maybe_unused]] core::SequenceId base_offset_;
  [[maybe_unused]] size_t max_size_;
};

}  // namespace abyss::queue
