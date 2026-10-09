#pragma once

#if defined(_M_X64) || defined(_M_IX86)
#include <intrin.h>
#endif

namespace abyss::queue {

inline void CpuRelax() noexcept {
#if defined(_M_X64) || defined(_M_IX86)
  _mm_pause();
#elif defined(__x86_64__) || defined(__i386__)
  __builtin_ia32_pause();
#elifdef __aarch64__
  __asm__ __volatile__("yield");
#endif
}

}  // namespace abyss::queue
