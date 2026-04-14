#include "segment.h"

#include <utility>

namespace abyss::queue {

Segment::Segment(std::string path, core::SequenceId base_offset, size_t max_size)
    : path_(std::move(path)), base_offset_(base_offset), max_size_(max_size) {}

}  // namespace abyss::queue
