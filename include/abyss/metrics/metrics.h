#pragma once

namespace abyss::metrics {

// Metrics design is deferred until prometheus-cpp integration.
// Components will register their own counters/histograms/gauges
// through a shared registry rather than a central enumeration.

class Registry {
 public:
  static Registry& Instance();

 private:
  Registry() = default;
};

}  // namespace abyss::metrics
