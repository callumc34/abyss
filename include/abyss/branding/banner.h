#pragma once

#include <cstdio>
#include <string>
#include <string_view>

namespace abyss::branding {

struct BannerOptions {
  bool suppressed = false;
  std::string_view profile;
};

// Writes the startup banner to |stream|. No-op when opts.suppressed is true;
// the suppressed path performs no allocation and no writes.
void PrintBanner(std::FILE* stream, const BannerOptions& opts);

// One-line version string: "v<ver>-<short-sha> | <build-type>", with
// " | <profile> profile" appended when |profile| is non-empty. Reused by
// --version output (no profile) and the banner subtitle (with profile).
std::string VersionLine(std::string_view profile);

// True when ABYSS_NO_BANNER is set to "1", "true", "yes", or "on"
// (case-insensitive). Empty or unset means false.
bool SuppressedByEnv();

}  // namespace abyss::branding
