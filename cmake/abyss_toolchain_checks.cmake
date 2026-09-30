include(CheckCXXSourceCompiles)

# Numeric parsing uses floating-point std::from_chars (locale-free and
# non-throwing). Some standard libraries ship only the integral overloads.
check_cxx_source_compiles([=[
#include <charconv>
#include <system_error>
int main() {
  constexpr char kText[] = "1.5";
  double value = 0.0;
  const auto result = std::from_chars(kText, kText + 3, value);
  return result.ec == std::errc{} ? 0 : 1;
}
]=] ABYSS_HAVE_FLOAT_FROM_CHARS)

if(NOT ABYSS_HAVE_FLOAT_FROM_CHARS)
  message(FATAL_ERROR
    "The C++ standard library does not provide floating-point std::from_chars, "
    "which Abyss requires. See docs/development/building.md for supported "
    "toolchains.")
endif()
