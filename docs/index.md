# Abyss

Abyss is a Redis-compatible key-value store that transparently manages two storage tiers: a fast in-memory hot tier and a durable on-disk cold tier.

## The Problem

Many services need a fast key-value store with persistence guarantees that outlive process restarts, but without the operational overhead of a full database. In-memory stores like Redis offer excellent latency but lose data on restart and become expensive as data volumes grow. Disk-backed stores offer persistence but can't match in-memory read latency. Running both independently creates consistency and synchronisation headaches.

## What Abyss Does

Abyss presents a single Redis-compatible interface. Clients connect with any standard Redis client library, there's no custom SDKs, no protocol changes. Behind that interface, Abyss manages data across two tiers:

- **Hot tier** — in-memory, sub-millisecond reads. Data lives here while it's being actively accessed.
- **Cold tier** — on-disk, durable. Data migrates here automatically when it hasn't been accessed recently, and is promoted back to hot on the next read.

The transition between tiers is transparent. Callers don't need to know which tier holds their data.

## How It Works

Abyss uses a Kappa architecture: an append-only queue is the single source of truth. Every write goes to the queue first. Two independent consumers read from the queue and materialise state into the hot and cold stores. Recovery is a simple queue replay, no complex replication protocols, no dual-write consistency problems.

The cold consumer is the most interesting component. Rather than writing every update to disk immediately, it maintains an in-memory compaction buffer that absorbs writes and waits for an optimal time to flush. If a key is updated a thousand times in a minute, only the final state hits disk.

## Key Properties

- **Redis compatible** — RESP2 protocol. Any Redis client works.
- **Durable** — writes are not acknowledged until they reach the configured durability class: surviving a process crash by default, or a power loss when configured. A failure within that class never loses an acknowledged write.
- **Efficient** — the compaction buffer collapses intermediate writes. The cold store sees a fraction of the total write volume.
- **Recoverable** — pod restart rebuilds state from the queue. No external coordination needed.
- **Pluggable** — each component (queue, hot store, cold store) is behind an abstract interface. The embedded profile runs everything in-process with zero dependencies. The external profile delegates to Kafka, Redis, KVRocks, or similar systems.
- **Kubernetes-native** — designed for StatefulSet deployment with PVCs.

## Documentation

### Design

- [Architecture](design/architecture.md) — system design, component model, deployment profiles, TTL model
- [Requirements](design/requirements.md) — performance targets, durability guarantees, design principles, milestones
- [ADP-013: Performance Harness](design/proposals/013-performance-harness.md) — how Abyss measures itself against its targets
- [ADP-015: Write Path, Durability Classes and Execution Model](design/proposals/015-write-path-and-durability.md) — how a write is sequenced, made durable and acknowledged

### Operations

- [Deployment](operations/deployment.md) — Kubernetes, Helm, configuration reference
- [Observability](operations/observability.md) — metrics, health endpoints, logging
- [Failure Modes](operations/failure-modes.md) — backpressure, failure scenarios, recovery procedures

### Development

- [Building](development/building.md) — build from source, presets, dependencies
- [Testing](development/testing.md) — test strategy, running tests
- [Performance](development/performance.md) — performance harness operating guide (probes, load gen, output schema, local-run caveats)
