#pragma once

#include <cstddef>

#ifdef __APPLE__
#include <mach/mach.h>
#elifdef __linux__
#include <unistd.h>

#include <fstream>
#endif

namespace abyss::bench {

// The process's resident memory in bytes, 0 where unknown: macOS's
// physical footprint, Linux's resident set.
inline size_t Footprint() {
#ifdef __APPLE__
  task_vm_info_data_t info{};
  mach_msg_type_number_t count = TASK_VM_INFO_COUNT;
  // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast): the Mach API's shape.
  if (task_info(mach_task_self(), TASK_VM_INFO, reinterpret_cast<task_info_t>(&info), &count) !=
      KERN_SUCCESS) {
    return 0;
  }
  return static_cast<size_t>(info.phys_footprint);
#elifdef __linux__
  std::ifstream statm("/proc/self/statm");
  size_t pages = 0;
  size_t resident = 0;
  if (!(statm >> pages >> resident)) return 0;
  return resident * static_cast<size_t>(sysconf(_SC_PAGESIZE));
#else
  return 0;
#endif
}

}  // namespace abyss::bench
