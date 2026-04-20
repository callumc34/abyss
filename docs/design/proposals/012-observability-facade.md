# ADP-012: Observability Facade

## Status

Proposed.

## Context

Every subsystem that lands after this point — the WAL queue, hot store, cold store, consumers, RESP frontend, tiering engine — is expected to emit structured logs and Prometheus metrics. Without a single foundational abstraction in place before those subsystems arrive, each will either pick its own pattern or defer instrumentation until a later refactor. Both outcomes are incompatible with the operational targets in the PRD.

This ADP defines a logging facade over spdlog and a metrics facade over prometheus-cpp. The facade is intentionally thin — it does not re-implement structured logging or metric collection — but it is the single abstraction boundary between subsystems and their telemetry backends, and it codifies the discipline that makes operational data trustworthy at scale.

## Goals

- Single source of truth for metric names and their label schemas. Ad-hoc names are not possible.
- Bounded metric cardinality enforced at compile time where feasible and at the registration boundary otherwise.
- Zero allocation and no locks on the metric observation path.
- Zero argument evaluation on log calls gated below the configured level.
- Safe under heavy concurrency and safe under static-initialisation ordering.
- Test-hermetic: registry and sinks are resettable between tests.
- Swappable backends: spdlog and prometheus-cpp types do not leak across the facade boundary.

## Non-goals

- The `/metrics` HTTP scrape endpoint is out of scope (tracked separately).
- Dynamic runtime log-level reconfiguration.
- Log rotation, remote sinks, push-gateway, OpenTelemetry bridges.
- Per-subsystem instrumentation. Each subsystem ships its instrumentation in the PR that lands it.
- Asynchronous logging queues. Synchronous emission only.

## Logging

### Lifecycle

`abyss::log::Init` is called as the first action in `main` before any Abyss component is constructed. It installs the configured sinks, the global default level, and the per-component level overrides. Re-calling `Init` replaces the configuration atomically; this is exposed for tests, not for application use.

A stderr fallback sink is constructed at first use via a function-local static. Any logger lookup before `Init` returns a logger backed by this fallback at INFO. This exists to prevent crashes from accidental log calls during third-party static initialisation; it is not a supported pattern for application code and application code should not rely on it.

### Emission model

Loggers are acquired by component name. Lookup results are cached and reused. Each logger carries an atomic effective level — the global default overridden by the per-component configuration — that can be read lock-free on the hot path.

Emission is performed through level-named macros. The macros expand to an atomic level check and early return before the argument list is evaluated. This is load-bearing: a DEBUG call on a hot path must not evaluate its arguments when DEBUG is disabled.

All emission is structured. A log record comprises:

- A wall-clock timestamp in ISO 8601 UTC.
- A severity level.
- The component (logger name).
- A short, static, human-readable `msg`.
- Optional field pairs supplied at the call site.

Dynamic values belong in fields, never in `msg`. `msg` is a low-cardinality anchor for alerting and log queries. This is the same convention adopted by zap, slog, and tracing-rs.

### No raw keys

Abyss is a key-value store; keys are frequently PII or secret material. Log records must not contain raw Redis keys or values at any level. The facade exposes a key-hash helper so the rule is trivial to follow at call sites; without the helper the rule would remain aspirational.

### Sinks and formats

The default sink writes JSON Lines to standard output. An alternative human-readable coloured sink is selectable via configuration for local development. All sinks are thread-safe.

### Test ergonomics

The testing surface exposes a capturing sink that records structured records (not formatted lines) so tests can assert on the record shape directly. A reset helper restores the pre-test sink configuration and clears captured state.

## Metrics

### Registry

The registry is a process-wide singleton. It lazy-initialises on first access; no explicit init call is required. Components register their metrics in their constructors, receive lightweight handles, and cache those handles as members. Further observation is a method call on the cached handle and performs an atomic update with no allocation.

### Catalogue and naming

Every metric is declared as a `constexpr` descriptor in the names catalogue. Each descriptor carries a name, help string, the types of its label values, and — for histograms — its bucket boundaries. The registry API accepts only descriptors; there is no overload that takes a free-form name string. This is the enforcement of single-source-of-truth: a new metric requires a change to the catalogue, which forces a deliberate review of name, help, labels, and cardinality.

### Label contract

The permitted label keys are `tier`, `shard`, `cmd`, `reason`, `status`, `op`. This is encoded as a closed enum; descriptor compilation rejects any key outside the enum.

Label values are typed. Where the space is small and fixed — tier, status, reason — a typed enumeration with a compile-time string conversion represents each possible value. Where the space is the server's own command registry — cmd — values are passed through a strong wrapper type backed by a view into that registry. Where the space is the shard space — shard — values are passed as a typed wrapper around a shard identifier, formatted to string once at registration.

Label values must not be raw Redis keys, client-supplied strings, IP addresses, or any other unbounded cardinality source. The facade cannot syntactically prevent this for `cmd` — the command registry is not a fixed set at compile time — so the discipline is enforced by convention, code review, and the requirement that `cmd` values flow through the dedicated wrapper type.

### Registration semantics

Registration with an identical descriptor is idempotent and returns the previously registered handle. Registration with a conflicting descriptor — same name, different signature — is a programmer error: the facade writes a diagnostic to standard error and aborts the process. This can only arise from two declarations of the same metric name in the codebase, which would otherwise produce nondeterministic metrics depending on registration order. Failing loudly at startup is the only safe outcome.

### Observation semantics

Handle methods are thin wrappers over prometheus-cpp atomics. They perform no allocation and take no locks. The hot-path cost is a single atomic read-modify-write.

### Disabled state

When metrics are disabled by configuration, `Scrape` returns an empty payload. Observation continues to update internal state so that enabling metrics later — whether at startup via a different configuration or at runtime via a future admin interface — does not require components to check an enabled flag on hot paths.

### Test ergonomics

The testing surface exposes a registry reset helper that replaces the internal prometheus-cpp registry with a fresh instance, plus accessors for the current value of a counter, gauge, or histogram. Tests install the reset helper in their `SetUp`/`TearDown` fixtures.

## Configuration

### LogConfig

A global default level (INFO in release builds, DEBUG in debug builds); a sink format (JSON or text); a sink destination (stdout or stderr); and a map of per-component level overrides. Environment overlays for the first three via `ABYSS_LOG_LEVEL`, `ABYSS_LOG_FORMAT`, `ABYSS_LOG_SINK`.

### MetricsConfig

An enabled flag (default true); a bind address; a port (default 9090). Environment overlays via `ABYSS_METRICS_ENABLED`, `ABYSS_METRICS_BIND`, `ABYSS_METRICS_PORT`. The bind and port fields are consumed by the scrape endpoint, not by the facade itself; they live on `MetricsConfig` for configuration locality.

## Thread-safety

- Logger lookup, level changes, and emission are thread-safe. Emission is lock-free except for the final sink write, which is serialised by the sink's own mutex.
- Metric registration is serialised by the registry. Observation is lock-free.
- The testing reset helpers are not safe to call concurrently with emission or observation. Tests serialise this themselves via fixture ordering.

## Static-init safety

- The logging fallback sink is obtained via a function-local static and is therefore constructed on first use, independent of translation-unit initialisation order.
- The metrics registry is likewise a function-local static.
- Neither facade performs non-trivial work from a global constructor.

## Migration

No existing subsystem is migrated by this ADP. Migration happens in the landing PR of each subsystem.

## Risks and mitigations

- **Backend swap risk.** The PIMPL boundary in the facade is the single point to rewrite if spdlog or prometheus-cpp is replaced. The catalogue and the macros do not change.
- **Cardinality leakage via `cmd`.** Enforced by convention rather than the facade. Mitigated by the dedicated wrapper type, by code review, and by a future linter rule that flags suspect constructions at the call site.
- **Test interference from non-hermetic registries.** Mitigated by the reset helpers and by a test that asserts reset restores a pristine state (registry empty, default sink installed).

## Alternatives considered

- **Free-form registry API with a style rule against ad-hoc names.** Rejected: relies on discipline rather than enforcement.
- **Per-subsystem registry instead of a process-wide singleton.** Rejected: requires threading a registry through every component constructor for no operational benefit.
- **OpenTelemetry as the single backend.** Rejected for the current phase: heavier dependency surface; prometheus-cpp and spdlog are best-of-breed for their respective concerns and a later swap to OpenTelemetry remains achievable through the PIMPL boundary.
- **Asynchronous logging queue.** Rejected for the current phase: shifts the observability failure mode to dropped messages under back-pressure and requires a deliberate capacity and drop-policy decision that is not worth making on day one.

## Acceptance

This ADP is accepted when the public headers — `log.h`, `metrics.h`, `names.h`, and the two `testing.h` counterparts — plus the `LogConfig` and `MetricsConfig` additions to `config.h` have been reviewed and merged. Implementation bodies and tests follow in a separate set of commits on the same branch.
