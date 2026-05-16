# Abyss brand assets

SVG sources for the wordmark, mark, social preview, and architecture diagram.
The brand spec lives at [`docs/branding.md`](../docs/branding.md).

## Layout

```
assets/
├── render.sh                 # Regenerate PNG exports from SVGs
├── logo/
│   ├── logo.svg              # Primary wordmark + mark
│   ├── logo-light.svg        # Wordmark + mark, dark text — light backgrounds
│   ├── logo-dark.svg         # Wordmark + mark, light text — dark backgrounds
│   └── logo-mark.svg         # Icon only, square — favicons, compact contexts
├── social/
│   └── social-preview.svg    # 1280 × 640 GitHub social preview
└── diagrams/
    └── architecture.svg      # README-facing architecture diagram
```

The authoritative architecture diagram is the ASCII version in
[`docs/design/architecture.md`](../docs/design/architecture.md); the SVG here
is a convenience for README rendering and should be kept in sync.

## Regenerating PNGs

```sh
./render.sh
```

Requires `rsvg-convert`:

- macOS: `brew install librsvg`
- Debian/Ubuntu: `apt-get install librsvg2-bin`

PNGs are produced at 1x and 2x for each logo SVG; the social preview is
rendered at the fixed 1280 × 640 that GitHub expects.

## Notes

The wordmark currently renders the letterforms via a system font stack
(`system-ui`, Inter, Helvetica, …). For consistent rendering across platforms
the letterforms should be converted to outline paths once a final typeface is
chosen — see [`docs/branding.md`](../docs/branding.md#typography).
