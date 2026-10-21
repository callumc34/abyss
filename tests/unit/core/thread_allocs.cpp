#include "thread_allocs.h"

#include <cstddef>
#include <cstdlib>
#include <new>

namespace abyss::testing {
namespace {

thread_local std::size_t t_allocs = 0;

}  // namespace

std::size_t ThreadAllocs() { return t_allocs; }

}  // namespace abyss::testing

// The replaceable global allocation functions, counting. A TU of their
// own, so no caller here inlines them into a new/free mismatch.
// NOLINTBEGIN(cppcoreguidelines-no-malloc,cppcoreguidelines-owning-memory,readability-inconsistent-declaration-parameter-name)
void* operator new(std::size_t size) {
  void* ptr = std::malloc(size > 0 ? size : 1);
  if (ptr == nullptr) throw std::bad_alloc();
  ++abyss::testing::t_allocs;
  return ptr;
}
void* operator new[](std::size_t size) { return operator new(size); }
void operator delete(void* ptr) noexcept { std::free(ptr); }
void operator delete[](void* ptr) noexcept { operator delete(ptr); }
void operator delete(void* ptr, std::size_t /*size*/) noexcept { operator delete(ptr); }
void operator delete[](void* ptr, std::size_t /*size*/) noexcept { operator delete(ptr); }
// NOLINTEND(cppcoreguidelines-no-malloc,cppcoreguidelines-owning-memory,readability-inconsistent-declaration-parameter-name)
