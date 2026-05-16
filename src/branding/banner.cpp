#include "abyss/branding/banner.h"

#include <cctype>
#include <cstdio>
#include <cstdlib>
#include <string>
#include <string_view>

#include "abyss/version.h"

namespace abyss::branding {

namespace {

constexpr std::string_view kBannerArt =
    R"(       _
      | |
  __ _| |__  _   _ ___ ___
 / _` | '_ \| | | / __/ __|
| (_| | |_) | |_| \__ \__ \
 \__,_|_.__/ \__, |___/___/
              __/ |
             |___/
)";

constexpr std::string_view kTagline =
    "A Redis-compatible KV store with transparent hot-cold tiering.";

constexpr std::size_t kShortShaLen = 7;

std::string_view ShortSha() {
  const std::string_view full{kBuildCommit};
  if (full == "unknown") return full;
  if (full.size() > kShortShaLen) return full.substr(0, kShortShaLen);
  return full;
}

bool EqualsIgnoreCase(std::string_view a, std::string_view b) {
  if (a.size() != b.size()) return false;
  for (std::size_t i = 0; i < a.size(); ++i) {
    const auto ca = static_cast<char>(std::tolower(static_cast<unsigned char>(a[i])));
    if (ca != b[i]) return false;
  }
  return true;
}

}  // namespace

std::string VersionLine(std::string_view profile) {
  std::string out;
  out.reserve(64);
  out += 'v';
  out += kVersion;
  const std::string_view sha = ShortSha();
  if (sha != "unknown") {
    out += '-';
    out.append(sha);
  }
  out += " | ";
  out += kBuildType;
  if (!profile.empty()) {
    out += " | ";
    out.append(profile);
    out += " profile";
  }
  return out;
}

// NOLINTNEXTLINE(readability-non-const-parameter) — stream is written to.
void PrintBanner(std::FILE* stream, const BannerOptions& opts) {
  if (opts.suppressed) return;
  std::string out;
  out.reserve(kBannerArt.size() + kTagline.size() + 96);
  out.append(kBannerArt);
  out.append("   ");
  out.append(kTagline);
  out.push_back('\n');
  out.append("   ");
  out.append(VersionLine(opts.profile));
  out.append("\n\n");
  std::fwrite(out.data(), 1, out.size(), stream);
  std::fflush(stream);
}

bool SuppressedByEnv() {
  const char* v = nullptr;
  v = std::getenv("ABYSS_NO_BANNER");
  if (v == nullptr || *v == '\0') return false;
  const std::string_view sv{v};
  return EqualsIgnoreCase(sv, "1") || EqualsIgnoreCase(sv, "true") || EqualsIgnoreCase(sv, "yes") ||
         EqualsIgnoreCase(sv, "on");
}

}  // namespace abyss::branding
