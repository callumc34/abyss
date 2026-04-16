# ADP-011: Conditional Writes and Consumer RPC

**Status:** Accepted
**Created:** 2026-04-15

## Context

The RESP frontend must support Redis commands with conditional semantics — `SET NX`, `SET XX`, `SETNX`, `MSETNX`, `ZADD NX|XX|GT|LT`, `SET KEEPTTL`, `SET GET`, `RENAMENX`, `COPY`, `EXPIRE NX|XX|GT|LT`, and future transactional constructs (`MULTI/EXEC`, `WATCH`). A conditional op's outcome depends on the current state of one or more keys, which raises a problem unique to Kappa architectures: **the queue is the single source of truth, but the decision depends on materialised state, and different consumers hold different views**.

Hot and cold consumers disagree on "exists" because hot evicts values while cold retains them. A naïve per-consumer evaluation produces split-brain outcomes (see Trade-offs for a worked example). Pre-resolving at the frontend via tiered reads is feasible in single-pod Phase 1 but puts cold reads on the write hot path and does not extend to multi-op transactions.

This ADP defines a Kappa-preserving design across three mechanisms:

1. **Consumer RPC** — a generalisation of the write-promise pattern ([ADP-005](005-resp-frontend.md), [ADP-006](006-read-write-paths.md)) that lets any consumer return a typed `RespValue` to the frontend.
2. **Queue entry taxonomy** — three variants: `Write`, `Conditional`, `Resolved`. See [ADP-001](001-queue-wal.md) for the `LogEntry` interface update.
3. **The Resolver** — a new in-process consumer that turns `Conditional` intents into `Resolved` decisions recorded in the queue.

Consumer block-and-scan semantics, recovery ordering, and latency characteristics follow from these three.

## Design

### Consumer RPC

The existing write-ack uses `std::promise<void>` keyed by `SequenceId`. Consumer RPC generalises this to `std::promise<RespValue>` keyed by `RpcId`, supporting any consumer that needs to return a value to the frontend — not only writes.

```cpp
namespace abyss::core {

using RpcId = uint64_t;

class ConsumerRpc {
 public:
  virtual ~ConsumerRpc() = default;

  // Frontend registers a pending RPC. Returns the future to await.
  virtual std::future<RespValue> Register(RpcId id) = 0;

  // Consumer fulfils an RPC with a value (includes error RespValues).
  virtual void Fulfill(RpcId id, RespValue value) = 0;

  // Cancel a pending RPC (used on timeout).
  virtual void Cancel(RpcId id) = 0;
};

}  // namespace abyss::core
```

The write-ack path becomes the degenerate case: `RpcId = SequenceId` of the write's queue entry, fulfilled with `RespValue::SimpleString("OK")` or an error. No regression in the unconditional write path.

**ID allocation.** Writes and conditional writes use the queue `SequenceId` of the `Conditional` or `Write` entry they produced. Non-write admin RPCs (e.g. `DBSIZE`, `INFO`) use a separate monotonic counter with a high-bit tag to prevent collision with sequence IDs. The frontend owns this counter; it is connection-lifetime, not persisted.

**Registry.** A single `std::mutex`-protected `std::unordered_map<RpcId, std::promise<RespValue>>`. Critical section is one insert (register), one lookup + erase (fulfil), or one lookup + erase (cancel). Short — contention is minimal.

**Timeout.** Configurable per-RPC. Default 5s for writes (matches existing write-promise timeout). On timeout, the frontend cancels the RPC and returns an error to the client. The underlying queue entry is still durable and will be applied when the consumer recovers; the client simply did not observe the outcome.

### Queue Entry Taxonomy

Every queue entry carries a type tag. Phase 1 defines three variants:

| Type | Semantics | Written by | Read by |
|------|-----------|------------|---------|
| `Write` | Unconditional op: SET, DEL, SADD, ZADD(non-conditional), ... | Frontend | Hot, Cold |
| `Conditional` | Op + predicate: SET NX, ZADD GT, MSETNX, ... | Frontend | Resolver |
| `Resolved` | ref = conditional seq, decision, materialised op, return value | Resolver | Hot, Cold |

Every `Conditional` at seq `X` is followed by exactly one `Resolved` at some seq `Y > X` with `Resolved.ref = X`. The queue remains totally ordered and append-only.

Logical layout (on-disk byte format in [ADP-009](009-wal-format.md); type byte values align across ADP-001, ADP-009, and this ADP):

```
Write      { type = 0x00, cmd: RespCommand }
Conditional{ type = 0x01, cmd: RespCommand, predicate: Predicate }
Resolved   { type = 0x02,
             ref: SequenceId,
             decision: Apply | Skip,
             materialised_op: RespCommand?,   // present iff decision = Apply
             return_value: RespValue }        // carries bulk for SET GET,
                                              // integer for SETNX, etc.
```

`Predicate` is an enum over the conditional forms: `kNx`, `kXx`, `kKeepTtl`, `kSetGet`, `kZAddNx`, `kZAddXx`, `kZAddGt`, `kZAddLt`, `kMsetNx`, `kExpireNx/Xx/Gt/Lt`, `kRenameNx`, `kCopyNoReplace`, and any flags they combine with. This is an explicit typed enum, not a free-form predicate DSL — Phase 1 has a closed set and we enforce it at the type level.

### The Resolver

A new in-process consumer thread, peer to hot and cold:

```cpp
class Resolver {
 public:
  virtual ~Resolver() = default;

  virtual Result<void> Start() = 0;
  virtual Result<void> Stop() = 0;
};
```

**Existence lookup.** For each `Conditional`, the Resolver must determine whether affected keys exist (or their current value, for `SET GET`; their current score, for `ZADD GT|LT`). It queries a tiered view:

1. **Local recent-writes cache** — an in-memory `unordered_map<std::string, KeyMeta>` populated as the Resolver processes queue entries. `KeyMeta` holds the latest seq, absolute TTL, and (for scalars) a small-value cache. Bounded by eviction duration (configurable, default equal to hot store's `default_eviction_seconds`). Microsecond lookup.
2. **Compaction buffer** — shared read lock against the cold consumer's buffer (same primitive as the read path, [ADP-006](006-read-write-paths.md)). Microsecond lookup.
3. **Cold store** — `ColdStore::Exec(...)`. Millisecond lookup. Bounded by cold read SLA (<5ms p99).

The tiered lookup terminates on the first definitive answer. Miss through all three means the key does not exist.

**Per-key serialisation.** The Resolver acquires the shard-striped lock (same scheme as the hot store, [ADP-002](002-hot-store.md)) for the affected key(s) before evaluating. This guarantees that concurrent `Conditional` entries targeting the same key resolve in queue order, preserving total ordering.

**Emission.** After deciding, the Resolver appends `Resolved` to the queue with ref, decision, materialised op (if apply), and return value, then fulfils the Consumer RPC promise keyed by the `Conditional`'s seq.

### Parallel Execution with Fsync

The Resolver fits the existing parallel-append pattern — consumers read from the in-memory queue buffer concurrently with the fsync in flight. The critical path for conditional writes with Resolver cache hit collapses to the same ~50us p99 as unconditional writes:

```
t=0     Frontend appends Conditional → in-memory queue buffer
t=+1us  Resolver reads Conditional from buffer (fsync not yet issued)
t=+5us  Resolver decision (cache hit)
t=+5us  Resolver appends Resolved → same in-memory buffer (same fsync batch)
t=+5us  Hot consumer reads Resolved from buffer
t=+50us fsync completes (both Conditional and Resolved durable together)
t=+50us Hot apply completes (parallel to fsync)
t=+50us Resolver fulfils Consumer RPC → client response
```

Latency profile:

| Scenario | p99 | Notes |
|----------|-----|-------|
| Unconditional write | ~50us | Baseline ([requirements](../requirements.md)) |
| Conditional write, Resolver cache hit | ~50us | Batched into the same fsync window |
| Conditional write, Resolver falls to cold | ~5ms | Bounded by cold read p99 |
| Conditional write on truly fresh key | ~5ms | Must verify non-existence in cold; unavoidable |

In steady state, the Resolver runs ahead of hot and cold (same position in the queue as the hot consumer, usually), so `Resolved` entries are already buffered by the time hot/cold reach the corresponding `Conditional`. The cold-fallback case is the slow path, and it matches the SLA already accepted for cold reads.

### Consumer Block-and-Scan

Hot and cold consumers process queue entries in seq order. On encountering a `Conditional` at seq `X`:

1. Block the replay head at `X`.
2. Scan ahead in the in-memory buffer (and, if necessary, on-disk segments) for `Resolved(ref=X, ...)`.
3. When found, apply `materialised_op` if `decision = Apply`; skip if `decision = Skip`. The effect is as if the op occurred at seq position `X`.
4. Advance the replay head past both `X` and the matching `Resolved`'s seq.

The `Resolved` entry is marked as "consumed by block-and-scan" when encountered at its own position — hot and cold do not reapply it a second time.

Ordering invariant: the effective application sequence preserves queue order. The materialised op occupies the `Conditional`'s slot, not the `Resolved`'s slot. Reads from either store reflect this ordering.

Under Resolver stalls (e.g. cold-lookup backlog), hot/cold block. The hot consumer's lag budget ([ADP-002](002-hot-store.md)) degrades gracefully — write latency rises as block-and-scan waits, providing natural backpressure.

### Recovery Ordering

[ADP-007](007-recovery.md) defines recovery as `cold replay → hot replay`. This extends to **`resolver replay → cold replay → hot replay`**:

1. **Resolver replay** — rebuilds the recent-writes cache from queue entries since the resolver's ack point. Does not emit new `Resolved` entries during replay; the log already contains matching `Resolved` entries for every `Conditional` from the original run.
2. **Cold replay** — unchanged from ADP-007. Cold consumer sees `Write` and `Resolved` entries; block-and-scan handles `Conditional` transparently.
3. **Hot replay** — unchanged from ADP-007.

Resolver replay is fast: existence cache only, no cold lookups needed during replay (the log already carries the decisions). Adds negligible recovery time relative to cold and hot.

### Configuration

```yaml
resolver:
  existence_cache_seconds: 86400        # Matches hot's default_eviction_seconds
  existence_cache_max_entries: 10000000
  cold_lookup_timeout_ms: 100

consumer_rpc:
  default_timeout_ms: 5000
  registry_shard_count: 16              # Lock striping for the RPC map
```

## Invariants

1. Every conditional operation produces exactly one `Conditional` entry and exactly one `Resolved` entry. The `Resolved` references the `Conditional` by seq id.
2. The Resolver is the sole producer of `Resolved` entries.
3. Hot and cold consumers never evaluate conditional predicates independently — they defer to the `Resolved` decision.
4. The Resolver serialises decisions per key via the shard-striped lock; concurrent conditionals on the same key resolve in queue order.
5. On recovery, the resolver replays before cold, which replays before hot.
6. Consumer RPC IDs are globally unique: writes and conditionals use the queue `SequenceId`; non-write RPCs use a separate monotonic counter with a distinguishing tag.
7. `Resolved` entries applied by hot and cold take effect at the `Conditional`'s seq position, not the `Resolved`'s seq position. Queue order is preserved.
8. The write/conditional-write client path is not acknowledged until the queue fsync completes AND the responsible consumer fulfils the Consumer RPC.

## Trade-offs

**Split-brain worked example — why this design is needed.**

Consider naïve per-consumer resolution (no Resolver, each consumer evaluates NX locally):

1. `SET K v1 EX 300` (eviction=60s, ttl=300s) — both hot and cold apply.
2. 70s pass with no reads. Hot evicts the value (tier transition, not delete). Cold still has K.
3. `SET K v2 NX` arrives.
4. Hot consumer: "K absent from hot" → NX succeeds → writes v2.
5. Cold consumer: "K present in cold" → NX fails → no-op.
6. Client observes OK. Hot now has v2, cold still has v1. Next cold-hit read after v2's eviction returns v1.

The split-brain is a direct consequence of hot and cold having differential retention. Routing decisions through the Resolver eliminates this by construction: both consumers see the same `Resolved` outcome.

**Why a separate Resolver thread, not inline frontend resolution under a striped lock?**

Inline frontend resolution (sync tiered read from the write handler) is Kappa-adjacent but has three flaws:

1. **Critical path coupling.** Puts cold reads on the conditional write path. The Resolver isolates cold latency in a dedicated component that can batch, prefetch, and optimise.
2. **No transaction extension.** `MULTI/EXEC` needs deferred multi-op resolution with rollback semantics. Inline cannot produce a durable record of "transaction aborted because WATCH'd key changed" — that record belongs in the queue, and the Resolver is the component that produces it.
3. **Multi-pod awkwardness.** In Phase 2, multi-key conditionals across pods would need synchronous cross-pod reads from the frontend. A Resolver per shard keeps resolution local to the owning pod.

The Resolver is the strategic choice. Inline would be expedient now at the cost of known repayment later.

**Why queue both `Conditional` and `Resolved` instead of only `Resolved`?**

Durability symmetry. The client's command must be durable before the Resolver computes the decision — otherwise a crash between "frontend receives command" and "Resolver runs" loses the intent. Recording the `Conditional` first guarantees the command's durability; the `Resolved` entry is the Resolver's attestation.

Alternative considered: frontend synchronously waits for the Resolver before appending anything. That collapses to inline resolution above, with the same drawbacks.

**Why `Resolved` carries `return_value` instead of recomputing at hot apply time?**

`SET GET` returns the prior value. `ZADD CH` returns the count of changed members. These values are computed by the Resolver from the pre-decision state. Recomputing at hot apply time would require hot to re-read tiered state, defeating the purpose of the Resolver.

**Why hot and cold skip `Conditional` instead of applying the intent as a best-effort?**

Without the `Resolved` decision, a consumer does not know whether to apply or skip. Applying unconditionally produces incorrect semantics (NX becomes always-apply). Skipping unconditionally loses writes. The only safe behaviour is to wait for the authoritative decision. Block-and-scan makes this explicit.

**Truly-fresh-key NX latency.**

`SET K v NX` on a K never written pays ~5ms (Resolver cache miss → cold lookup → confirms not-found → NX succeeds). This is unavoidable without a global all-time-written Bloom filter (memory-expensive and probabilistic). 5ms is within the cold read SLA and is acceptable for the strong-semantics NX operation. Clients using NX for distributed locking typically accept this latency.

Future optimisation: a negative-result cache in the Resolver ("K was checked at seq X and didn't exist", short TTL) would fast-path repeated NX attempts on the same fresh key (distributed lock retry loops). Not in Phase 1 scope.

**Recovery cost.**

Resolver replay is a pure queue scan rebuilding the existence cache — no cold lookups, no disk I/O beyond reading queue segments. Adds measurably less time than the cold replay it precedes.

**Generalising Consumer RPC is non-trivial scope.**

The current write-promise is `std::promise<void>` keyed by `SequenceId`. Generalising to `std::promise<RespValue>` with a separate ID space is a small refactor but load-bearing across #1, #2, #3, #34. Treating it as a first-class primitive (rather than a bespoke channel per admin command) avoids proliferation of parallel ad-hoc sync mechanisms.

**Future-forward fit.**

Transactions (`MULTI/EXEC`) are multi-op conditionals. `WATCH` adds a "seq-since" predicate. CAS primitives (if ever added) are single-key conditionals. All fit the Resolver pattern with no new architectural machinery — only new predicate variants in the enum.
