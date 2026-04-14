# Abyss

A Redis-compatible key-value store with transparent hot-cold tiering.

Abyss exposes a standard Redis protocol interface backed by a Kappa architecture: an append-only queue is the single source of truth, and two independent consumers materialise state into a fast in-memory hot tier and a durable on-disk cold tier. Data moves between tiers automatically. Clients connect with any Redis client library.

## Features

- **Redis compatible** — RESP2 protocol. Any Redis client works out of the box.
- **Two-tier storage** — in-memory hot tier for sub-millisecond reads, on-disk cold tier for durability. Transparent promotion and eviction between tiers.
- **Durable writes** — writes are not acknowledged until committed to the append-only queue. A crash never loses an acknowledged write.
- **Smart compaction** — the cold consumer's compaction buffer collapses intermediate writes. A key updated 1000 times results in a single disk write.
- **Pluggable backends** — embedded (zero dependencies) or external (Kafka, DragonflyDB, KVRocks). Mix and match via deployment profiles.
- **Kubernetes-native** — StatefulSet deployment with PVCs. Redis Cluster protocol for horizontal scaling.

## Quick Start

```bash
# Configure and build
cmake --preset default
cmake --build build/default

# Run tests
ctest --preset default

# Run the server
./build/default/apps/abyss-server/abyss-server
```

### Requirements

- CMake 3.25+
- C++23 compiler (GCC 13+, Clang 17+, Apple Clang 17+)

## Architecture

```
                    ┌───────────────┐
  Redis Client ────▶│  RESP Frontend │
                    └──────┬────────┘
                           │ write
                           ▼
                    ┌───────────────┐
                    │    Queue      │  ◄── Single source of truth
                    │  (append-only │
                    │     log)      │
                    └──┬─────────┬──┘
                       │         │
            ┌──────────▼──┐  ┌──▼───────────┐
            │ Hot Consumer │  │ Cold Consumer │
            │  (eager,     │  │  (smart,      │
            │   real-time) │  │   compacting) │
            └──────┬───────┘  └──────┬───────┘
                   │                 │
            ┌──────▼───────┐  ┌──────▼───────┐
            │  Hot Store   │  │  Cold Store  │
            │ (in-memory)  │  │  (on-disk)   │
            └──────────────┘  └──────────────┘
```

See the [architecture docs](docs/design/architecture.md) for the full design.

## Documentation

| Document | Description |
|----------|-------------|
| [Architecture](docs/design/architecture.md) | System design, component model, deployment profiles |
| [Requirements](docs/design/requirements.md) | Performance targets, durability guarantees, milestones |
| [Design Proposals](docs/design/proposals/) | Detailed designs for each subsystem (ADP-001 through ADP-008) |
| [Deployment](docs/operations/deployment.md) | Kubernetes, Helm, configuration reference |
| [Observability](docs/operations/observability.md) | Metrics, health endpoints, logging |
| [Failure Modes](docs/operations/failure-modes.md) | Backpressure, failure scenarios, recovery |
| [Building](docs/development/building.md) | Build from source, presets, dependencies |
| [Testing](docs/development/testing.md) | Test strategy, running tests |

## Project Status

Abyss is in early development. The project structure and interfaces are defined. Implementation of Phase 1 (embedded profile, single pod) is in progress.

See [Requirements — Milestones](docs/design/requirements.md#milestones) for the full roadmap.

## Contributing

### Building from Source

```bash
git clone https://github.com/callumc34/abyss.git
cd abyss
cmake --preset default
cmake --build build/default
ctest --preset default
```

### Code Style

- Google C++ Style Guide
- `.cpp` file extension, `#pragma once` header guards
- 100-column limit, enforced via `.clang-format`
- Thread safety annotations on all mutex-guarded members
- Minimal comments — the code should be readable without them

### Running Checks

```bash
# Format
find include src tests -name '*.cpp' -o -name '*.h' | xargs clang-format -i

# Static analysis
cmake --build build/default --target abyss_unit_tests
clang-tidy -p build/default src/**/*.cpp

# Sanitizers
cmake --preset asan && cmake --build build/asan && ctest --test-dir build/asan
cmake --preset tsan && cmake --build build/tsan && ctest --test-dir build/tsan
```

### Pull Requests

- One focused change per PR.
- All tests must pass. New functionality needs tests.
- Run clang-format before submitting.
- Design changes should reference the relevant ADP (Abyss Design Proposal).

## License

TBD
