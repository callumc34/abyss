# Building

## Language and Toolchain

- **Language:** C++23
- **Build system:** CMake 3.25+ with presets, vcpkg for dependency management
- **Compiler targets:** GCC 13+, Clang 17+, Apple Clang 17+
- **Container target:** Distroless

## Prerequisites

vcpkg must be installed and `VCPKG_ROOT` set in the environment before configuring the build:

```bash
git clone --depth 1 https://github.com/microsoft/vcpkg.git ~/.local/share/vcpkg
~/.local/share/vcpkg/bootstrap-vcpkg.sh -disableMetrics

# fish
set -Ux VCPKG_ROOT $HOME/.local/share/vcpkg
fish_add_path $VCPKG_ROOT

# bash / zsh
echo 'export VCPKG_ROOT=$HOME/.local/share/vcpkg' >> ~/.profile
echo 'export PATH="$VCPKG_ROOT:$PATH"' >> ~/.profile
```

The CMake presets pick up the vcpkg toolchain from `$VCPKG_ROOT`. Dependencies are declared in `vcpkg.json` and resolved automatically on configure.

## Dependencies

| Dependency | Purpose | License |
|------------|---------|---------|
| crc32c | WAL entry and segment integrity checksums | BSD-3-Clause |
| RocksDB | Built-in cold store | Apache 2.0 / GPL 2.0 |
| xxHash | Key hashing / shard routing | BSD |
| hiredis | RESP parsing, external Redis client | BSD |
| liburing | io_uring async I/O (Linux) | LGPL / MIT |
| spdlog | Structured logging | MIT |
| prometheus-cpp | Metrics export | MIT |
| yaml-cpp | Configuration | MIT |
| googletest | Testing | BSD |
| benchmark | Microbenchmarks (optional, `benchmarks` feature) | Apache 2.0 |

Optional (external profile):

| Dependency | Purpose | License |
|------------|---------|---------|
| librdkafka | Kafka queue backend | BSD |
| nats.c | NATS JetStream queue backend | Apache 2.0 |

## Quick Start

```bash
# Configure (debug build with tests)
cmake --preset default

# Build
cmake --build build/default

# Run tests
ctest --preset default

# Run the server
./build/default/apps/abyss-server/abyss-server
```

## Build Presets

| Preset | Type | Tests | Description |
|--------|------|-------|-------------|
| `default` | Debug | On | Development build |
| `release` | Release | Off | Optimised build |
| `asan` | Debug | On | Address sanitizer + undefined behaviour sanitizer (default UBSan check group) |
| `tsan` | Debug | On | Thread sanitizer |
| `ubsan` | Debug, `-O1` | On | Standalone undefined behaviour sanitizer with extended check set. Clang-only |
| `bench` | Release | Off | Benchmarks enabled (pulls in `benchmark` via vcpkg) |
| `container` | Release | Off | Static-linked binary for container images (Linux only) |

```bash
# Address sanitizer build (combined with default UBSan group)
cmake --preset asan
cmake --build build/asan

# Thread sanitizer build
cmake --preset tsan
cmake --build build/tsan

# Standalone UndefinedBehaviorSanitizer (extended check set, requires Clang)
CC=clang CXX=clang++ cmake --preset ubsan
cmake --build build/ubsan
```

## Sanitizers

Sanitizer wiring is centralised in `cmake/abyss_sanitizers.cmake` and selected via `ABYSS_SANITIZER`:

| `ABYSS_SANITIZER` | Compiler | Checks |
|-------------------|----------|--------|
| `none` (default) | any | — |
| `address` | GCC or Clang | AddressSanitizer only |
| `thread` | GCC or Clang | ThreadSanitizer |
| `address+undefined` | GCC or Clang | AddressSanitizer + default UBSan check group, used by the `asan` preset |
| `undefined` | Clang only | Extended UBSan check set, used by the `ubsan` preset |

The `undefined` mode enables a wider check set than the default `-fsanitize=undefined` group: `nullability`, `local-bounds`, `float-divide-by-zero`, `integer` (including unsigned overflow), and `implicit-conversion`. Two noisy implicit-conversion sub-checks (`implicit-integer-sign-change`, `implicit-signed-integer-truncation`) are turned off because they fire on idiomatic `int → size_t` indexing and `uint8_t = literal` patterns.

The `ubsan` preset pins to Clang because the extended checks (`nullability`, `local-bounds`) and the modern `-fsanitize-ignorelist` semantics are clang-first; GCC's UBSan has parity gaps. It builds at `-O1` rather than unoptimised Debug — UBSan's value comes from finding UB the optimiser is about to exploit, and `-O1` engages range analysis and DCE without the inlining transforms that ruin stack traces. Assertions are kept enabled (`Debug` build type, no `NDEBUG`).

`-fno-sanitize-recover=all` is set on the `ubsan` preset and `-fno-sanitize-recover=undefined` on the `asan` preset, so a UBSan finding aborts the test process — required for CI to fail on UB.

Third-party headers pulled into our translation units (RocksDB, hiredis, libc++/libstdc++, gtest) are scoped out via `cmake/ubsan_ignorelist.txt`. Runtime suppressions for `vptr`/`function` checks (which can't be ignorelisted at compile time) live in `cmake/ubsan_suppressions.txt`.

## Build Options

| Option | Default | Description |
|--------|---------|-------------|
| `ABYSS_BUILD_TESTS` | `ON` | Build test targets |
| `ABYSS_BUILD_BENCHMARKS` | `OFF` | Build benchmark targets |
| `ABYSS_WERROR` | `OFF` | Treat warnings as errors |
| `ABYSS_STRICT_WARNINGS` | `OFF` | Enable additional warning flags beyond -Wall -Wextra -Wpedantic |
| `ABYSS_SANITIZER` | `none` | Sanitizer to enable (`none`, `address`, `thread`, `undefined`, `address+undefined`) |

## Container Build

The `container` preset produces a statically-linked release binary for container images. It is only available on Linux.

Uses a custom vcpkg triplet (`cmake/triplets/x64-linux-static-release.cmake`) that builds all dependencies as static libraries. The binary links libstdc++ and libgcc statically, leaving only glibc as a dynamic dependency.

```bash
# Build the binary directly (Linux only)
cmake --preset container -G Ninja
cmake --build build/container

# Build the Docker image (on Apple Silicon, add --platform linux/amd64)
docker build -f docker/Dockerfile -t abyss:local .
docker run --rm abyss:local
```

The Docker image uses a multi-stage build with `gcc:14` as the builder and `gcr.io/distroless/cc-debian12:nonroot` as the runtime base. Target image size is under 80 MB.

## Project Structure

```
abyss/
├── .github/workflows/   # CI pipeline
├── include/abyss/       # Public headers
│   ├── core/            # Types, interfaces, Result<T>
│   ├── resp/            # RESP protocol
│   ├── queue/           # Queue implementations
│   ├── hot/             # Hot store implementations
│   ├── cold/            # Cold store implementations
│   ├── consumer/        # Consumers, compaction buffer
│   ├── engine/          # Tiering engine, write promise
│   ├── config/          # Configuration
│   └── metrics/         # Metrics registry
├── src/                 # Source files + per-library CMakeLists
├── apps/abyss-server/   # Main binary
├── cmake/
│   ├── triplets/        # Custom vcpkg triplets
│   └── *.cmake          # Build system utilities
├── tests/
│   ├── support/         # Mock implementations for testing
│   ├── unit/            # Unit tests
│   ├── integration/     # Integration tests
│   └── benchmark/       # Performance benchmarks
├── config/              # Example configuration
├── docker/              # Dockerfiles
└── deploy/helm/         # Helm charts
```

## Code Style

Google C++ Style Guide with the following project-specific choices:

- File extension: `.cpp` (not `.cc`)
- Header guards: `#pragma once`
- Column limit: 100
- Formatting enforced via `.clang-format` (run `clang-format -i`)
- Static analysis via `.clang-tidy`
- Thread safety annotations on all mutex-guarded members
