#pragma once

#include <cstddef>
#include <string>

#include "abyss/core/types.h"

namespace abyss::queue {

class Segment {
 public:
  Segment(std::string path, core::SequenceId base_offset, size_t max_size);

 private:
  std::string path_;
  core::SequenceId base_offset_;
  size_t max_size_;
};

}  // namespace abyss::queue
