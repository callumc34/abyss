#include "alloc_counter.h"

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdlib>
#include <new>

namespace abyss::testing {
namespace {

std::atomic<std::size_t> g_big_allocs{0};
std::atomic<std::size_t> g_big_frees{0};
std::atomic<std::size_t> g_frees{0};
// Live big allocations, so a free can tell it is one.
std::array<std::atomic<void*>, 256> g_big_live{};

void NoteAlloc(void* ptr, std::size_t size) {
  if (size < kBigAllocBytes) return;
  g_big_allocs.fetch_add(1, std::memory_order_relaxed);
  for (auto& slot : g_big_live) {
    void* empty = nullptr;
    if (slot.compare_exchange_strong(empty, ptr)) return;
  }
}

void NoteFree(void* ptr) {
  g_frees.fetch_add(1, std::memory_order_relaxed);
  for (auto& slot : g_big_live) {
    void* held = ptr;
    if (slot.compare_exchange_strong(held, nullptr)) {
      g_big_frees.fetch_add(1, std::memory_order_relaxed);
      return;
    }
  }
}

}  // namespace

std::size_t BigAllocs() { return g_big_allocs.load(); }
std::size_t BigFrees() { return g_big_frees.load(); }
std::size_t Frees() { return g_frees.load(); }

}  // namespace abyss::testing

// The replaceable global allocation functions, counting.
// NOLINTBEGIN(cppcoreguidelines-no-malloc,cppcoreguidelines-owning-memory,readability-inconsistent-declaration-parameter-name)
void* operator new(std::size_t size) {
  void* ptr = std::malloc(size > 0 ? size : 1);
  if (ptr == nullptr) throw std::bad_alloc();
  abyss::testing::NoteAlloc(ptr, size);
  return ptr;
}
void* operator new[](std::size_t size) { return operator new(size); }
void operator delete(void* ptr) noexcept {
  if (ptr == nullptr) return;
  abyss::testing::NoteFree(ptr);
  std::free(ptr);
}
void operator delete[](void* ptr) noexcept { operator delete(ptr); }
void operator delete(void* ptr, std::size_t /*size*/) noexcept { operator delete(ptr); }
void operator delete[](void* ptr, std::size_t /*size*/) noexcept { operator delete(ptr); }
// NOLINTEND(cppcoreguidelines-no-malloc,cppcoreguidelines-owning-memory,readability-inconsistent-declaration-parameter-name)
