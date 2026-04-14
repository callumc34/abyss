#include "abyss/metrics/metrics.h"

namespace abyss::metrics {

Registry& Registry::Instance() {
  static Registry instance;
  return instance;
}

}  // namespace abyss::metrics
