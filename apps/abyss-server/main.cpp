#include <cstdlib>
#include <iostream>

#include "abyss/version.h"

int main() {
  std::cout << "abyss v" << abyss::kVersion << "\n";
  return EXIT_SUCCESS;
}
