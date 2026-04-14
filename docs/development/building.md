# Building

## Language and Toolchain

- **Language:** C++23
- **Build system:** CMake 3.25+ with presets, vcpkg for dependency management
- **Compiler targets:** GCC 13+, Clang 17+, Apple Clang 17+
- **Container target:** Distroless or Alpine, target < 50 MB

## Dependencies

| Dependency | Purpose | License |
|------------|---------|---------|
| RocksDB | Built-in cold store | Apache 2.0 / GPL 2.0 |
| xxHash | Key hashing / shard routing | BSD |
| hiredis | RESP parsing, external Redis client | BSD |
| liburing | io_uring async I/O (Linux) | LGPL / MIT |
| spdlog | Structured logging | MIT |
| prometheus-cpp | Metrics export | MIT |
| protobuf | Queue WAL entry serialisation | BSD |
| yaml-cpp | Configuration | MIT |
| googletest | Testing | BSD |
| benchmark | Microbenchmarks | Apache 2.0 |

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
| `asan` | Debug | On | Address sanitizer + undefined behaviour sanitizer |
| `tsan` | Debug | On | Thread sanitizer |

```bash
# Address sanitizer build
cmake --preset asan
cmake --build build/asan

# Thread sanitizer build
cmake --preset tsan
cmake --build build/tsan
```

## Build Options

| Option | Default | Description |
|--------|---------|-------------|
| `ABYSS_BUILD_TESTS` | `ON` | Build test targets |
| `ABYSS_BUILD_BENCHMARKS` | `OFF` | Build benchmark targets |
| `ABYSS_WERROR` | `OFF` | Treat warnings as errors |
| `ABYSS_STRICT_WARNINGS` | `OFF` | Enable additional warning flags beyond -Wall -Wextra -Wpedantic |

## Project Structure

```
abyss/
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
├── tests/
│   ├── support/         # Mock implementations for testing
│   ├── unit/            # Unit tests
│   ├── integration/     # Integration tests
│   └── benchmark/       # Performance benchmarks
├── config/              # Example configuration
├── deploy/helm/         # Helm charts
└── docker/              # Dockerfiles
```

## Code Style

Google C++ Style Guide with the following project-specific choices:

- File extension: `.cpp` (not `.cc`)
- Header guards: `#pragma once`
- Column limit: 100
- Formatting enforced via `.clang-format` (run `clang-format -i`)
- Static analysis via `.clang-tidy`
- Thread safety annotations on all mutex-guarded members
