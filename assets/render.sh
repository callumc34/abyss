#!/usr/bin/env bash
# Regenerate PNG exports from the SVG sources under assets/.
# Requires rsvg-convert:
#   macOS:  brew install librsvg
#   Linux:  apt-get install librsvg2-bin   (or your distro equivalent)

set -euo pipefail
cd "$(dirname "$0")"

if ! command -v rsvg-convert >/dev/null 2>&1; then
  echo "rsvg-convert not found. Install with:"
  echo "  macOS:  brew install librsvg"
  echo "  Linux:  apt-get install librsvg2-bin"
  exit 1
fi

# Logo variants — 1x and 2x using the SVG's native viewBox aspect.
for svg in logo/logo.svg logo/logo-dark.svg logo/logo-light.svg logo/logo-mark.svg; do
  base="${svg%.svg}"
  echo "  rendering ${base}.png  ${base}@2x.png"
  rsvg-convert --zoom=1 "${svg}" -o "${base}.png"
  rsvg-convert --zoom=2 "${svg}" -o "${base}@2x.png"
done

# Social preview — fixed 1280×640 (GitHub requirement).
echo "  rendering social/social-preview.png"
rsvg-convert --width=1280 --height=640 social/social-preview.svg -o social/social-preview.png

# Architecture diagram — fixed 1200×720.
echo "  rendering diagrams/architecture.png"
rsvg-convert --width=1200 --height=720 diagrams/architecture.svg -o diagrams/architecture.png

echo "done."
