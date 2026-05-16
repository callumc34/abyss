# Abyss brand

Single-page reference for the visual identity. Source assets live under
[`assets/`](../assets/). The startup banner is rendered by
[`src/branding/banner.cpp`](../src/branding/banner.cpp).

## Tagline

> A Redis-compatible KV store with transparent hot–cold tiering.

One sentence. Reused verbatim in the README hero and the banner subtitle
(banner uses a plain hyphen since it is ASCII-only).

## Palette

The palette is a subset of Tailwind CSS's default colour tokens (MIT-licensed,
public design tokens — not lifted from any one project's brand).

| Role            | Hex       | Token                 | Notes                                            |
|-----------------|-----------|-----------------------|--------------------------------------------------|
| Primary         | `#7C3AED` | `tailwind purple-600` | Mark fill, primary accent                        |
| Accent          | `#A855F7` | `tailwind purple-500` | Gradient highlight, secondary accent             |
| Tint            | `#E9D5FF` | `tailwind purple-200` | Light strokes, captions on dark backgrounds      |
| Ink             | `#0A0A0F` | near-black neutral    | Body text on light, gradient terminus            |
| Panel           | `#1A1320` | near-black w/ violet  | Dark-mode panels, social preview background      |

Do not introduce additional hues. If a UI element needs differentiation, use
opacity on the existing palette rather than reaching for a new colour.

## Typography

| Surface             | Family                                                         | Weight  |
|---------------------|----------------------------------------------------------------|---------|
| Headings / wordmark | `system-ui, -apple-system, Inter, "Helvetica Neue", sans-serif` | 700–800 |
| Body                | Same stack                                                     | 400–500 |
| Code, monospace     | `ui-monospace, "JetBrains Mono", "Fira Code", monospace`        | 400–500 |

The wordmark in `assets/logo/*.svg` uses the system font stack via `<text>`.
That keeps the SVG small but renders inconsistently across platforms. When a
final typeface is committed to, convert the wordmark to outline paths
(Inkscape: *Path → Object to Path*) and replace the `<text>` element. Until
then, the variation across platforms is acceptable.

## Logo usage

### Mark

The mark is a rounded purple square containing an inverted triangular "well"
that fades from purple to near-black — the visual hook is descent into the
abyss. Use the mark alone where the wordmark is too wide (favicons, app
icons, channel avatars). Never recolour the gradient stops; never replace the
purple frame with another hue.

### Wordmark + mark

The combined logo is the primary asset for README headers, presentation
title slides, and social previews. Use `logo-light.svg` over light
backgrounds and `logo-dark.svg` over dark backgrounds. The README hero uses
both via a `<picture>` element so the wordmark adapts to the viewer's GitHub
theme.

### Clear space

Leave at least one mark-width of clear space on all sides of the logo. Do
not crop the mark or place text inside its frame.

### Don't

- Don't recolour outside the palette.
- Don't apply drop shadows, glows, or strokes to the logo or wordmark.
- Don't stretch or skew. Scale uniformly.
- Don't rotate the mark — the descent must point down.
- Don't place the wordmark on a busy photograph or over the mark itself.

## Startup banner

The banner is printed to stderr at server startup, immediately after config
validation and before the logging system initialises. The art is plain ASCII
so it survives piping and terminals without UTF-8 support.

Suppression:

- `--no-banner` CLI flag (added in [`apps/abyss-server/main.cpp`](../apps/abyss-server/main.cpp))
- `ABYSS_NO_BANNER=1` environment variable (also accepts `true`, `yes`, `on`,
  case-insensitive)

The suppressed path performs no allocations and no writes. The `--version`
flag prints the same identity line as the banner subtitle, minus the profile
(which isn't known until config has loaded).

## Voice

Precise. Operator-facing. No marketing inflation, no emoji in product output.
Error messages name what failed and what to do; they do not apologise.
README and docs may be lightly opinionated but stop short of hype. If a claim
is in the README, it has to be a claim the code can stand behind.

## Asset workflow

1. Edit the SVG source under `assets/`.
2. Run `assets/render.sh` to regenerate PNG exports.
3. Commit the SVG and the regenerated PNGs together — they are paired.

PNG exports are regenerated locally; CI does not currently re-render them.
That is a deliberate trade-off (avoiding a `librsvg` dependency in CI) and
will be revisited if the visual identity is iterated on frequently.
