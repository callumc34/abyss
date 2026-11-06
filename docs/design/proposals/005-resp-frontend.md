# ADP-005: RESP Frontend

**Status:** Accepted
**Created:** 2026-04-09
**Updated:** 2026-11-06

> **Amended by [ADP-015](015-write-path-and-durability.md).** Conditional writes are ordinary sequenced writes that log their decided effects; §Command Classification and Dispatch, §Multi-Key Commands and §Write Acknowledgement describe that. Dispatch is asynchronous (#161): a reactor never waits on durability, cold reads or a capacity wait, each command reaches its linearisation point before the next on its connection is dispatched, and replies keep command order. §Write Acknowledgement, §Connection Handling, §Configuration and §Invariants describe it.

> **§CLUSTER Commands refined by [ADP-014](014-slot-routing-and-topology.md).** `CLUSTER KEYSLOT` still returns the `CRC16` wire slot unchanged. `CLUSTER SLOTS`/`SHARDS` now advertise slot ranges grouped by their owning shard (resolved through slot-to-shard mapping) rather than a single full-range stub; in single-pod the ranges still cover the whole slot space, partitioned disjointly by shard. The `MOVED` path (gated behind multi-pod) computes its target from slot ownership, so a redirect always names the pod whose shard owns the key. Invariant 8's "only the slot-to-pod mapping differs between phases" is upgraded: that mapping is now explicit, slot-derived, and tested, not coincidental.

## Context

Abyss exposes a TCP listener implementing the Redis wire protocol (RESP2). Any standard Redis client library connects without modification — no custom SDKs, no protocol extensions. The frontend is responsible for parsing commands, classifying them, and routing them to the appropriate subsystem.

This ADP defines:

- The wire protocol and its version lock (§Protocol Version Lock, §Inline Protocol Support).
- The typed response contract (§RespValue Type Contract).
- The error prefix vocabulary (§Error Prefix Table).
- The complete Phase 1 command surface (§Command Registry).
- Per-command dispatch classes.
- Admin command handling: `HELLO`, `CLUSTER`, `INFO`, `DBSIZE`, `CLIENT`.
- Unknown-command and arity-mismatch behaviour.

Conditional writes (`SET NX`, `ZADD GT`, etc.) are decided by the per-shard sequencer against the key's complete state and logged as the effects they decided (decide-then-log), as specified in [ADP-015](015-write-path-and-durability.md) §Sequenced write path. That replaced [ADP-011](011-conditional-writes-and-consumer-rpc.md)'s resolver and Consumer RPC. This ADP references that design but does not duplicate it.

## Design

### Protocol Version Lock

RESP2 (Redis Serialisation Protocol version 2). This is the protocol spoken by every Redis client library in every language. Abyss does not implement RESP3 in Phase 1 — this is a hard invariant, not an aspiration.

- `HELLO 2` is accepted and responds with server info.
- `HELLO 3` is rejected with `NOPROTO unsupported protocol version`.
- `HELLO` with no args returns current handshake state.

Parser and serializer are RESP2-only. RESP3-specific types (maps, sets, doubles, big numbers, verbatim strings, push) are not emitted on the wire. When RESP3 lands in a future phase, it will be additive — existing RESP2 clients will not break.

### RespValue Type Contract

The RESP value type has six variants, and keeps simple and bulk strings separate: null, simple string, bulk string, integer, error and array.

Wire serialisation:

| Variant | Wire format | Use |
|---------|-------------|-----|
| Null | `$-1\r\n` (bulk null) or `*-1\r\n` (array null) | Missing key, NX-fail, array absence |
| Simple string | `+<value>\r\n` | Protocol constants: `OK`, `PONG`, `QUEUED` |
| Bulk string | `$<len>\r\n<bytes>\r\n` | User values, binary-safe |
| Integer | `:<n>\r\n` | Counts, bool as 0/1 |
| Error | `-<prefix> <message>\r\n` | See §Error Prefix Table |
| Array | `*<len>\r\n<elems>` | Multi-value responses |

Parser emits the correct variant based on type byte (`+`/`-`/`:`/`$`/`*`). Serializer is deterministic — no guessing between simple and bulk.

Every error is built by one helper that takes a prefix from the fixed set in §Error Prefix Table and a message, so the prefix vocabulary is enforced by the type system. A `WRONGTYPE` error, for example, is that helper given the wrong-type prefix and Redis's message "Operation against a key holding the wrong kind of value". Hand-formatted error strings (`"-ERR ..."`) are disallowed.

### Error Prefix Table

| Prefix | Semantics | Owner | Emitted when |
|--------|-----------|-------|--------------|
| `ERR` | Generic: syntax, bad args, unknown command | Frontend | Unknown command, arity mismatch, malformed args, parse error |
| `WRONGTYPE` | Key exists with a different type | Sequencer / read path | Type mismatch found when a write is decided (nothing is logged) or when a read meets the key |
| `LOADING` | Server is loading state from the queue | Frontend | Recovery gate open ([ADP-007](007-recovery.md)) |
| `MOVED <slot> <ip>:<port>` | Key belongs to a different shard | Frontend | Phase 2+ cluster routing ([ADP-008](008-horizontal-scaling.md)); emission path exists in Phase 1 but never triggered |
| `CROSSSLOT` | Multi-key write spans what one atomic batch can cover | Sequencer | Keys span WAL logs when `queue.log_count` > 1 (#169); Phase 2+ multi-key across pods |
| `OOM` | Write rejected due to memory limit | Sequencer | The write's hot shard over `hot.max_memory_bytes` ÷ `hot.shard_count` × `hot.backpressure_ratio` with cold behind, still at the write's deadline; nothing is applied or logged. Per shard, so under skew one shard can reject writes while hot's total is under its limit |
| `NOSCRIPT` | Script not loaded / not supported | Frontend | Any script-related command in Phase 1 (`EVAL`, `EVALSHA`, `SCRIPT`) |
| `NOPROTO` | Unsupported protocol version | Frontend | `HELLO 3` |
| `READONLY` | (Reserved) | — | Not emitted in Phase 1. Reserved for future replica mode. |
| `NOAUTH` | (Reserved) | — | Not emitted in Phase 1. Reserved for future AUTH support. |

Error message text follows Redis's canonical formatting where a standard message exists. Clients that parse error text (some do) then behave consistently against Abyss.

### Inline Protocol Support

RESP accepts two input formats:

1. **RESP arrays** — first byte `*`. Standard client format.
2. **Inline** — whitespace-separated tokens ending in `\r\n` (or `\n`). Used for telnet-based debugging.

The parser detects at the first non-whitespace byte: `*`/`+`/`-`/`:`/`$` → RESP; otherwise → inline. Inline tokenisation supports `"double-quoted"` and `'single-quoted'` strings with C-style escapes (`\n \t \r \" \\ \xHH`) per Redis convention.

Both paths produce the same `RespCommand` and feed the same registry lookup. Inline is not a separate command space. Pipelined inline commands work through the same incremental-parse mechanism as RESP pipelining.

### Command Classification and Dispatch

Every command is classified along two axes.

**Class** — what the command does to state:

| Class | Examples |
|-------|----------|
| Read | `GET`, `SMEMBERS`, `ZRANGEBYSCORE`, `EXISTS`, `TTL`, `TYPE`, `MGET` |
| Write | `SET`, `DEL`, `SADD`, `ZADD`, `EXPIRE`, `MSET` |
| Admin | `PING`, `INFO`, `DBSIZE`, `CLUSTER SLOTS`, `HELLO`, `CLIENT` |

**Dispatch** — how the frontend executes it:

| Dispatch | Handling | Ack via |
|----------|----------|---------|
| `kStateless` | Frontend computes locally; no engine interaction | Direct return |
| `kTieredRead` | Read path: hot, then on a miss a stub, the flush floor, or buffer plus cold, with no wait ([ADP-006](006-read-write-paths.md)) | Direct return, after the read fence |
| `kWritePath` | Every write, conditional or not: the sequencer decides it against the key's complete state, reserves, applies to hot and publishes ([ADP-015](015-write-path-and-durability.md) §Sequenced write path). The command's predicate extractor supplies its predicate flags (`NX`, `XX`, `GT`, `LT`, `GET`, ...) per call | The decided reply, once its frames reach the durability class |
| `kAdmin` | Answered from server state, not the data path (`DBSIZE`, `OBJECT IDLETIME`) | Direct return |
| `kFlush` | Broadcast wipe (`FLUSHDB`/`FLUSHALL`): one `Flush` frame per shard, reserved as one batch under every shard lock ([ADP-006](006-read-write-paths.md) §Broadcast Write Path) | `+OK`, once the Flush frames reach the durability class |

The request pipeline (issue #34) is a data-driven five-way dispatch against the command registry, not a growing switch.

### Container Commands and Subcommands

`CLUSTER`, `CLIENT`, `CONFIG`, `COMMAND`, and `OBJECT` are container commands: their first argument selects a subcommand whose semantics may differ from the parent (different arity, dispatch class, or recovery-time visibility). The registry models these explicitly. Each container command's `CommandSpec` carries a list of `SubcommandSpec` entries, and the pipeline resolves the subcommand against that list before applying arity, dispatch, and `loading_safe` rules.

This means:

- The set of accepted subcommands is the registry's truth. Unknown subcommands surface `ERR Unknown <PARENT> subcommand` at the frontend and never reach the queue or any handler.
- Per-subcommand `loading_safe` is authoritative when a subcommand resolves. The narrow recovery-time allowlist (`CLUSTER SLOTS`, `CLUSTER INFO`, `CLUSTER MYID`, `COMMAND` / `COMMAND COUNT|INFO|DOCS`, `PING`, `INFO`, `HELLO`, `QUIT`) lives on the spec, not in handler-side `if` chains.
- `OBJECT ENCODING` (read) and `OBJECT IDLETIME` (admin) carry distinct `Dispatch` values on their respective subspecs; the parent's dispatch is unused when a subcommand resolves.
- Arity on `SubcommandSpec` is the total RESP arg count including the parent token, so `CLUSTER KEYSLOT key` is arity 3 and `COMMAND INFO [name ...]` is arity -2.

When a container is invoked with no subcommand argument (e.g. `COMMAND` alone), parent dispatch applies. Containers whose parent arity rules out the no-subcommand form (e.g. `CONFIG` with arity -3) cannot reach this fallback.

### Command Registry (Phase 1)

Phase 1 restricts the surface to **direct key access and modification**, plus the `FLUSHDB`/`FLUSHALL` global wipe (routed through the queue as a broadcast write — [ADP-006](006-read-write-paths.md) §Broadcast Write Path) — no enumeration (`KEYS`, `SCAN`, `RANDOMKEY`), no other global operations (`SWAPDB`, `MOVE`, `SELECT`), no pub/sub, no scripting, no transactions (not yet implemented; they will execute on the sequencer, ADP-015), no streams.

Arity follows Redis's `COMMAND INFO` convention: positive = exact; negative = "at least |n|".

**Admin — stateless:**

| Command | Arity | Response |
|---------|-------|----------|
| `PING [msg]` | -1 | `+PONG` or bulk echo |
| `ECHO msg` | 2 | Bulk string |
| `QUIT` | 1 | `+OK`, then close connection |
| `HELLO [ver [AUTH u p] [SETNAME n]]` | -1 | Array; see §HELLO and Handshake State |
| `CLIENT ID` | 2 | Integer (monotonic per-connection id) |
| `CLIENT GETNAME` | 2 | Bulk or nil |
| `CLIENT SETNAME name` | 3 | `+OK` |
| `CLIENT NO-EVICT on\|off` | 3 | `+OK` (recorded; no-op in Phase 1) |
| `RESET` | 1 | `+RESET` (clears per-connection state) |
| `TIME` | 1 | Array[unix_seconds, microseconds] |
| `COMMAND` | -1 | Array of all commands' metadata |
| `COMMAND INFO [cmd ...]` | -2 | Array of specific command metadata |
| `COMMAND COUNT` | 2 | Integer |
| `COMMAND DOCS [cmd ...]` | -2 | Map-as-array of docs |
| `CONFIG GET param` | 3 | Array[key, value] pairs for matching params |

**Admin — server state:**

| Command | Arity | Response | Reason |
|---------|-------|----------|--------|
| `DBSIZE` | 1 | Integer | Needs hot + cold key count |
| `INFO [section]` | -1 | Bulk string | Needs live stats from every tier |

Both are served from a synchronous `ServerStatsProvider` snapshot, the same stats that feed the Prometheus gauges, so neither touches the data path. `DBSIZE` has the `kAdmin` dispatch class; `INFO` is `kStateless`.

**Admin — cluster (Phase 1 single-shard view; see §CLUSTER Commands):**

| Command | Arity | Response |
|---------|-------|----------|
| `CLUSTER SLOTS` | 2 | Array of slot ranges |
| `CLUSTER SHARDS` | 2 | Array of shard descriptors |
| `CLUSTER NODES` | 2 | Bulk string (node list) |
| `CLUSTER INFO` | 2 | Bulk string (cluster state) |
| `CLUSTER MYID` | 2 | Bulk string (node UUID) |
| `CLUSTER KEYSLOT key` | 3 | Integer (`CRC16(key) mod 16384`) |
| `CLUSTER COUNTKEYSINSLOT slot` | 3 | Integer |

**Admin — broadcast write (global wipe):**

| Command | Arity | Class | Dispatch | Response |
|---------|-------|-------|----------|----------|
| `FLUSHDB [ASYNC\|SYNC]` | -1 | Write | Flush | `+OK` |
| `FLUSHALL [ASYNC\|SYNC]` | -1 | Write | Flush | `+OK` |

Both take every shard lock, log one `Flush` frame per shard in a single reservation, and wipe hot before the locks are released. The reply waits only for the Flush frames to reach the durability class; until cold's wipe lands, hot's flush floor answers a miss on each shard as absent ([ADP-006](006-read-write-paths.md) §Broadcast Write Path). With `queue.log_count` above 1, each log's Flushes are a batch of their own, so a crash can leave the wipe applied to some shards only; a retry completes it (#169). The `ASYNC`/`SYNC` modifier is accepted (hence arity -1) but ignored — the wipe is always synchronous. Single-pod Phase 1 has only DB 0, so `FLUSHALL` and `FLUSHDB` are equivalent. Both are `loading_safe = false`: during recovery they return `LOADING`.

**Strings (direct key only):**

| Command | Arity | Class | Dispatch | Response |
|---------|-------|-------|----------|----------|
| `GET key` | 2 | Read | TieredRead | Bulk or nil |
| `SET key value [options...]` | -3 | Write | Write (§SET) | `+OK` / nil / bulk |
| `SETNX key value` | 3 | Write | Write (conditional) | Integer 0/1 |
| `SETEX key seconds value` | 4 | Write | Write | `+OK` |
| `PSETEX key ms value` | 4 | Write | Write | `+OK` |
| `STRLEN key` | 2 | Read | TieredRead | Integer |
| `MGET key [key ...]` | -2 | Read | TieredRead (fan-out) | Array of bulk/nil |
| `MSET k v [k v ...]` | -3 | Write | Write (fan-out, atomic) | `+OK` |
| `MSETNX k v [k v ...]` | -3 | Write | Write (conditional, atomic) | Integer 0/1 |

**Sets (direct key only):**

| Command | Arity | Class | Dispatch | Response |
|---------|-------|-------|----------|----------|
| `SADD key m [m ...]` | -3 | Write | Write | Integer (count added) |
| `SREM key m [m ...]` | -3 | Write | Write | Integer (count removed) |
| `SMEMBERS key` | 2 | Read | TieredRead | Array |
| `SISMEMBER key m` | 3 | Read | TieredRead | Integer 0/1 |
| `SMISMEMBER key m [m ...]` | -3 | Read | TieredRead | Array of 0/1 |
| `SCARD key` | 2 | Read | TieredRead | Integer |
| `SRANDMEMBER key [count]` | -2 | Read | TieredRead | Bulk / array / nil |

**Sorted sets (direct key only):**

| Command | Arity | Class | Dispatch | Response |
|---------|-------|-------|----------|----------|
| `ZADD key [NX\|XX\|GT\|LT] [CH] [INCR] score m ...` | -4 | Write | Write | Integer / bulk |
| `ZREM key m [m ...]` | -3 | Write | Write | Integer |
| `ZSCORE key m` | 3 | Read | TieredRead | Bulk or nil |
| `ZMSCORE key m [m ...]` | -3 | Read | TieredRead | Array |
| `ZCARD key` | 2 | Read | TieredRead | Integer |
| `ZRANK key m [WITHSCORE]` | -3 | Read | TieredRead | Integer/array/nil |
| `ZREVRANK key m [WITHSCORE]` | -3 | Read | TieredRead | Integer/array/nil |
| `ZRANGE key start stop [BYSCORE\|BYLEX] [REV] [LIMIT o c] [WITHSCORES]` | -4 | Read | TieredRead | Array |
| `ZRANGEBYSCORE key min max [WITHSCORES] [LIMIT o c]` | -4 | Read | TieredRead | Array |
| `ZRANGEBYLEX key min max [LIMIT o c]` | -4 | Read | TieredRead | Array |
| `ZCOUNT key min max` | 4 | Read | TieredRead | Integer |
| `ZLEXCOUNT key min max` | 4 | Read | TieredRead | Integer |

**Hashes (direct key only):**

| Command | Arity | Class | Dispatch | Response |
|---------|-------|-------|----------|----------|
| `HSET key field value [field value ...]` | -4 | Write | Write | Integer (count of new fields) |
| `HMSET key field value [field value ...]` | -4 | Write | Write | `+OK` (deprecated upstream; carried for client compatibility) |
| `HDEL key field [field ...]` | -3 | Write | Write | Integer (count deleted) |
| `HGET key field` | 3 | Read | TieredRead | Bulk or nil |
| `HMGET key field [field ...]` | -3 | Read | TieredRead | Array (nil per missing field) |
| `HEXISTS key field` | 3 | Read | TieredRead | Integer 0/1 |
| `HGETALL key` | 2 | Read | TieredRead | Array |
| `HKEYS key` | 2 | Read | TieredRead | Array of field names |
| `HVALS key` | 2 | Read | TieredRead | Array of values |
| `HLEN key` | 2 | Read | TieredRead | Integer (field count) |

`HSETNX` is registered separately, as an always-conditional write. The remaining hash commands (`HINCRBY`, `HINCRBYFLOAT`, `HRANDFIELD`, `HSCAN`, `HSTRLEN`, `HEXPIRE` family) are deferred until the corresponding store ops are exposed.

When the hot store does not hold the key — typical after eviction — multi-field hash reads (`HGETALL`, `HKEYS`, `HVALS`, `HLEN`, `HMGET`, `HEXISTS`) are answered by merging the compaction buffer's overlay with cold's persisted state in the tiering engine. The buffer alone never holds the complete picture for these reads: its hash-field map captures only net writes since the last flush. The merge enforces buffer-removed fields and buffer-overridden values without dropping cold-resident fields.

**Generic / key management (direct key only):**

| Command | Arity | Class | Dispatch | Response |
|---------|-------|-------|----------|----------|
| `DEL key [key ...]` | -2 | Write | Write (fan-out, atomic) | Integer (count) |
| `UNLINK key [key ...]` | -2 | Write | Write (fan-out, atomic) | Integer (Phase 1: alias for DEL) |
| `EXISTS key [key ...]` | -2 | Read | TieredRead | Integer |
| `EXPIRE key s [NX\|XX\|GT\|LT]` | -3 | Write | Write | Integer 0/1 |
| `PEXPIRE key ms [NX\|XX\|GT\|LT]` | -3 | Write | Write | Integer 0/1 |
| `EXPIREAT key ts [NX\|XX\|GT\|LT]` | -3 | Write | Write | Integer 0/1 |
| `PEXPIREAT key ts-ms [NX\|XX\|GT\|LT]` | -3 | Write | Write | Integer 0/1 |
| `PERSIST key` | 2 | Write | Write | Integer 0/1 |
| `TTL key` | 2 | Read | TieredRead | Integer |
| `PTTL key` | 2 | Read | TieredRead | Integer |
| `EXPIRETIME key` | 2 | Read | TieredRead | Integer |
| `PEXPIRETIME key` | 2 | Read | TieredRead | Integer |
| `TYPE key` | 2 | Read | TieredRead | Simple string |
| `RENAMENX src dst` | 3 | Write | Write (conditional, atomic) | Integer 0/1 |
| `COPY src dst [DB n] [REPLACE]` | -3 | Write | Write (conditional, atomic) | Integer 0/1 |
| `OBJECT ENCODING key` | 3 | Read | TieredRead | Bulk |
| `OBJECT IDLETIME key` | 3 | Read | Admin | `ERR not supported in this build` (abyss#97) |

**Excluded in Phase 1** (reject with `ERR unknown command`):

- Enumeration — `KEYS`, `SCAN`, `HSCAN`, `SSCAN`, `ZSCAN`, `RANDOMKEY`
- Global — `SWAPDB`, `MOVE`, `SELECT` (non-zero DB). `FLUSHDB`/`FLUSHALL` are **supported** — see §Command Registry above and [ADP-006](006-read-write-paths.md) §Broadcast Write Path.
- Transactions — `MULTI`, `EXEC`, `DISCARD`, `WATCH`, `UNWATCH` (not yet implemented; they will execute on the sequencer as one batch, [ADP-015](015-write-path-and-durability.md) §Sequenced write path)
- Pub/sub — `SUBSCRIBE`, `UNSUBSCRIBE`, `PSUBSCRIBE`, `PUNSUBSCRIBE`, `PUBLISH`, `PUBSUB`
- Scripting — `EVAL`, `EVALSHA`, `SCRIPT`, `FUNCTION` (`NOSCRIPT` where Redis semantics demand it)
- Streams — `XADD`, `XREAD`, `XRANGE`, `XREVRANGE`, `XLEN`, `XDEL`, `XGROUP`, `XACK`, ...
- Debug — `DEBUG`, `MONITOR`, `LATENCY`, `SLOWLOG`
- Dump / restore — `DUMP`, `RESTORE`, `MIGRATE`
- Deferred types — lists (`LPUSH` et al.), bitmaps, hyperloglog, geo. Rejected with `ERR unknown command` in Phase 1; added in Phase 2 when the hot store supports them. Hashes are partially supported (see "Hashes" above); the remaining hash commands listed there are deferred.
- Read-modify-write — `APPEND`, `DECR`, `DECRBY`, `GETDEL`, `GETSET`, `INCR`, `INCRBY`, `INCRBYFLOAT`, `SPOP`, `ZINCRBY`. See below.
- Multi-key rename — `RENAME`. `RENAMENX` is supported; the unconditional form is excluded with the read-modify-write group: like them it computes its effect from the source's current state, so it must be decided by the sequencer as `RENAMENX` is, and that has not been added yet.

#### Why read-modify-write commands cannot be plain writes

These were briefly advertised as unconditional writes with no typed op behind them, so the frontend accepted them, logged them, and only then failed at apply. The fix is not to add parsers — it is that the unconditional write path is the wrong home for them.

Every one of these commands computes its result from the key's current value. The compaction buffer that feeds the cold tier absorbs and emits *typed ops*, never raw commands, and it holds only what it has seen in the current window — the key may be cold-resident, in which case the buffer cannot know the value at all. A statement-form increment therefore cannot be absorbed: N increments cannot collapse into one op without evaluating them, and cold would have to perform its own read-modify-write to apply what it was given. Two tiers independently re-deriving a value from a statement is precisely the divergence invariant 3 exists to prevent.

This is why Redis can record `INCR` verbatim in its AOF and Abyss cannot: Redis has a single authoritative copy and no compacting secondary view. Systems that do have one resolve atomic operations to concrete values before the log records them.

Abyss already has the mechanism — decide-then-log: the sequencer evaluates every write against the key's complete state in hot, under the shard lock, and logs the concrete effect it decided together with its reply ([ADP-015](015-write-path-and-durability.md) §Sequenced write path). The correct implementation decides these commands the same way (#142), so the log records the computed result rather than the intent to compute. Hot and cold then apply an ordinary concrete write, compaction collapses it normally, and replay is deterministic because the decided value is in the log. Until that lands they are excluded rather than advertised, so clients get an honest, feature-detectable `ERR unknown command` instead of an accepted write that fails after a round trip.

### SET Command Full Specification

`SET key value [EX s | PX ms | EXAT ts | PXAT ts-ms | KEEPTTL] [NX | XX] [GET]`

Supported options:

| Option | Semantics | Interaction with two-TTL model |
|--------|-----------|--------------------------------|
| `EX seconds` | Set absolute TTL to `seconds` from now | Sets `ttl`. Does NOT set `eviction` — eviction remains global/per-prefix config. |
| `PX ms` | Set absolute TTL to `ms` from now | Same, ms precision. |
| `EXAT unix-seconds` | Set absolute TTL to a Unix timestamp | Same, absolute. |
| `PXAT unix-ms` | Set absolute TTL to a Unix ms timestamp | Same, absolute ms. |
| `KEEPTTL` | Preserve existing TTL on the key | Decided: the sequencer reads the existing TTL from hot and logs the `SET` with it as an absolute expiry ([ADP-015](015-write-path-and-durability.md)). |
| `NX` | Only set if key does not exist | Decided; a key that exists gets nil, and nothing is logged. |
| `XX` | Only set if key exists | Decided; an absent key gets nil, and nothing is logged. |
| `GET` | Return prior value, set new | Decided; the prior value is the reply, sent once the write is durable at the class. |

Excluded options:

| Option | Reason |
|--------|--------|
| `IDLE seconds` | Redis-specific LRU knob; does not fit Abyss's eviction model (eviction is duration-based, not LRU-based except under memory pressure). |

Dispatch rule: every form is `kWritePath`. The predicate extractor reports `NX`, `XX`, `KEEPTTL` and `GET` as predicate flags, and the sequencer decides the `SET` against hot with them; with none, the decision is a plain overwrite.

TTL option conflict detection (multiple TTL forms in one command) produces `ERR syntax error` at the frontend.

An `EXAT` or `PXAT` at or before now deletes the key, as `EXPIRE` does: the sequencer logs a `DEL`, and the reply is unchanged (`OK`, or the prior value with `GET`).

**Expiry boundary (deliberate deviation).** Every tier treats a key as expired once now ≥ its TTL, in milliseconds. Redis does so when it sets a TTL (`checkAlreadyExpired`), but on lookup it treats a key whose TTL equals now as live (`keyIsExpired`). Abyss uses one rule everywhere, so hot, buffer, cold and replay agree on the instant a key expires.

### Unknown Command Behaviour

Two distinct layers of "unknown":

**1. Unknown command name** — name not in the registry.

Frontend immediately responds `ERR unknown command '<NAME>', with args beginning with: <first-arg>` (Redis-canonical format) and does not touch the queue. This includes the Phase-1-deferred types (hashes, lists, streams, geo) — rejected at the frontend, not queued for rejection at apply time.

This supersedes the prior ambiguous "classify unknown as write" language.

**2. Known command name, malformed arguments** — the command has a typed-operation parser and that parser rejects these arguments.

Arity is a weaker check than the parser: `SET k v BOGUS` and `HSET k f v f` both satisfy their registry arity and are still malformed. The frontend validates such a command against its canonical parser before routing and rejects it with `ERR` (the prefix table's "malformed args" case). Nothing unparseable reaches the queue. Recording a command in the log that no tier can materialise would durably preserve an intent that can never be applied, and the queue is the single source of truth — it should not accumulate entries that are meaningless to every materialised view.

**3. Known command name, no typed-operation parser exists** — a log entry names a command no tier in this build can materialise.

This is no longer reachable from the wire. The registry advertises a write only when a typed-operation parser backs it, and a registry test asserts that agreement rather than leaving it to be maintained by hand. Fan-out writes are exempt because the sequencer logs them as per-key effects that do parse; the always-conditional writes (`SETNX`, `MSETNX`, `RENAMENX`, `COPY`, `HSETNX`) are exempt because the sequencer decides them whole and logs effects that do parse.

The case survives for replay: a log written by a build with a wider command surface can still contain such an entry. The cold consumer must distinguish it from genuine decoder skew. An entry no parser can decode cannot be materialised by this build, so the cold consumer skips it, counting the occurrence. Recovery's hot replayer does not skip it: every frame it applies is a decided effect, and a decided effect that does not parse terminates the process ([ADP-007](007-recovery.md) §Hot replay). Its residency rule skips the frame only when the key is not resident. Quarantining it — refusing to advance past it — would be wrong: it protects nothing, and while the registry could still advertise such a command it let any client suspend log retention indefinitely. Quarantine is reserved for an entry whose parser exists and fails, which is a real skew bug because another tier accepted the same bytes.

**Arity mismatch** — known command with wrong argument count. Frontend rejects with `ERR wrong number of arguments for '<name>' command`, no queue interaction.

### HELLO and Handshake State

Per-connection state (frontend-local, not a queue concern):

| Field | Meaning |
|-------|---------|
| Client id | A monotonic frontend counter |
| Client name | Set by `CLIENT SETNAME` |
| Protocol version | 2, locked in Phase 1 |
| Authenticated | Always true in Phase 1 (no `AUTH`) |

`HELLO` responses (map-as-array per RESP2 convention — alternating key/value pairs):

- `HELLO` with no args → current handshake state: `server=abyss, version=<build>, proto=2, id=<client_id>, mode=cluster|standalone, role=master, modules=[]`.
- `HELLO 2` → same payload, confirms protocol negotiation.
- `HELLO 3` → `NOPROTO unsupported protocol version`.
- `HELLO 2 AUTH <user> <pass>` → Phase 1 ignores AUTH arguments (future: verify credentials).
- `HELLO 2 SETNAME <name>` → sets `client_name`, but only once every option has parsed; a failed `HELLO` leaves the name unchanged.

A client name may hold only printable ASCII without spaces (`!` to `~`). `CLIENT SETNAME` and `HELLO … SETNAME` refuse any other name with Redis's error and store nothing, because `CLIENT LIST` and `INFO` echo names back. Error replies that quote client bytes, such as an unknown command or subcommand, map CR and LF to spaces, so a client can never split or forge a reply frame (#204).

`CLIENT ID` reads from connection state; never a queue concern. `CLIENT SETNAME` / `CLIENT GETNAME` operate on connection state. All handshake commands are classed stateless — the engine is not involved.

### CLUSTER Commands in Phase 1

Phase 1 is single-pod. Cluster commands respond as if this pod owns the entire keyspace, so cluster-aware clients work against Phase 1 unchanged and migrate to Phase 2 without client-side changes.

Invariant: **slot computation is phase-invariant**. `CLUSTER KEYSLOT` always returns `CRC16(key) mod 16384` (with `{hashtag}` honoured), whether Phase 1 single-pod or Phase 2 multi-pod. Only the slot-to-pod mapping differs between phases.

`CLUSTER SLOTS` response (Phase 1):

```
[
  [0, 16383, [<this-pod-ip>, <port>, <node-id>]]
]
```

`CLUSTER SHARDS`: equivalent single-shard representation.

`CLUSTER NODES`: single node entry, role `master`.

`CLUSTER MYID`: a UUID generated at first startup and persisted to `<data-dir>/node.id`. Cluster-stable identity is required for Phase 2 routing; establishing it in Phase 1 costs one small file.

`MOVED` redirects are not emitted in Phase 1, but the emission path exists in the frontend. Phase 2 flips a config flag (`cluster.enabled: true`) that activates the redirect logic ([ADP-008](008-horizontal-scaling.md)).

### Multi-Key Commands

`MGET`, `MSET`, `DEL`, `UNLINK`, and `EXISTS` accept multiple keys that may target different shards. The registry tags each one with its multi-key kind (get, set, delete or exists), and the request pipeline routes tagged commands through the dispatcher's fan-out path. Reads are answered key by key; writes go to the sequencer as one command. `MSETNX` is the exception — it is always conditional, a write-path command the sequencer decides whole.

**Single-pod (Phase 1):** All keys are local. The engine handles them as follows:

- `MGET k1 k2 ...` → per-key single-key reads across hot → buffer → cold (tombstone-aware), assembled positionally. A `WRONGTYPE` on any key collapses to nil at that slot, matching Redis behaviour.
- `MSET k1 v1 k2 v2 ...` → one `SET` effect per key, logged as one batch.
- `DEL` / `UNLINK k1 k2 ...` → one `DEL` effect per key, logged as one batch; per-key integer replies are summed.
- `EXISTS k1 k2 ...` → per-key existence probe across hot → buffer → cold; the buffer probe overrides cold (a not-yet-flushed `DEL` reports the key as absent even if cold still holds a residual). Duplicate keys are counted once each, matching Redis.

Multi-key writes are atomic. The sequencer takes every involved shard's lock in ascending shard order, decides every key, makes one log reservation for all the frames, applies them to hot and publishes once ([ADP-015](015-write-path-and-durability.md) §Cross-shard atomic commands). A refused one applies none of its keys, and a crash keeps all of its frames or none, because the log closes a batch by position. `MSETNX`, `RENAMENX` and `COPY` are decided the same way: `MSETNX` checks every key's existence under the locks and logs all its `SET`s as one batch, or nothing.

A batch cannot span WAL logs. With `queue.log_count` above 1, a multi-key write whose keys map to different logs is rejected with `CROSSSLOT` and nothing is logged (#169); hash tags keep related keys on one shard.

Multi-key reads are not atomic: `MGET` and multi-key `EXISTS` read each shard in turn and can observe a cross-shard write half-applied (#170).

**Multi-pod (Phase 2+):** If keys span pods, Abyss responds with `CROSSSLOT` error. Clients handle this by fanning out per-slot and assembling results client-side. Users can use hash tags (e.g. `{user123}.name`, `{user123}.email`) to co-locate related keys on the same shard.

### Write Acknowledgement

A write is acknowledged to the client only after two things have happened:

1. The sequencer has decided it against the key's complete state, reserved its log frames, applied its effects to hot and published them, under the shard lock ([ADP-015](015-write-path-and-durability.md) §Sequenced write path).
2. Its frames have reached the configured durability class ([ADP-001](001-queue-wal.md) §Durability classes and group commit). Under `process_crash` that is publication to the page cache, which survives a process crash. Under `power_loss` it is a completed fdatasync covering them, which survives a power loss.

The reply is the one the sequencer decided: `+OK`, nil, the prior value for `SET … GET`, an integer for `SETNX` or a count, or an error such as `WRONGTYPE`. Conditional and unconditional writes take the same path. A decision with no effect (`SETNX` on a present key) logs nothing, and its reply waits only until what it observed is durable at the class.

The queue is the sole write path — there is no dual write. Hot is applied in the same critical section that sequences the write, so there is no consumer to wait for.

**Waiting never holds the reactor.** A write that must wait before it is applied, for WAL admission, a spare segment, hot memory backpressure, a load of its keys or another caller's load of one, registers to be woken and returns to its connection's reactor. Nothing of it is applied yet, so when it is resumed it decides again from the start. Once applied and published the write is sequenced: its reply waits in its connection's slot until its frames reach the class, and the connection dispatches its next command meanwhile, so pipelined writes share flushes. A durable wait is a registration as well: the thread that moves the durable end past the write, under `power_loss` usually the WAL's flusher, posts the result back to the reactor.

**Timeout:** `engine.command_timeout_ms`, default 5 s, is a command's whole budget from dispatch: a write's waits before it is applied and its durable wait, a read's cold read and its wait for what it saw to be durable. Each connection's reactor times its oldest command.
- A command that runs out of time before it is applied or answered is given up and never applied. Its error names what it was waiting for.
- A write that runs out on its durable wait gets an error, but it is already in the queue and applied to hot, and it becomes durable at its class when the flush catches up.
- [Failure Modes](../../operations/failure-modes.md) §Command Errors lists every error text a client can see.

See [ADP-006](006-read-write-paths.md) for read and write path details, and [ADP-015](015-write-path-and-durability.md) for the sequencer, decide-then-log and §Asynchronous request execution.

### Loading State

During recovery ([ADP-007](007-recovery.md)), the RESP port accepts connections but responds to data-plane commands with `LOADING Abyss is loading the dataset in memory`. A narrow admin set (`PING`, `INFO`, `CLUSTER SLOTS`, `HELLO`, `QUIT`, `COMMAND`) remains available so clients and orchestrators can observe state and validate readiness.

The `LOADING` gate is a frontend-level check before dispatch. It lifts automatically when recovery completes.

### Connection Handling

- **Readiness-based I/O** on non-blocking sockets, behind one poller interface: edge-triggered `epoll` on Linux, `kqueue` on macOS and the BSDs, and `WSAPoll` on Windows. There is no `io_uring` backend (§Trade-offs).
- **Connections:** at most `net.max_connections` (default 1024) at once. A connection idle for `net.idle_timeout_seconds` (default 300) is closed; one with a command in flight is never idle.
- **Pipelining.** The parser is incremental: it returns one command plus bytes-consumed per invocation, and the caller loops until no progress. A connection dispatches its commands in arrival order, and each reaches its linearisation point before the next is dispatched (§Invariants).
  - A command that must wait first (a cold read, a load, WAL admission, a spare segment, hot memory backpressure) holds back the commands behind it on its own connection, and only those.
  - A write that is applied and waits only for durability holds back nothing: the next command dispatches, and the durable waits of a pipeline overlap.
  - Replies are written in command order. A reply that is ready waits until every earlier one on its connection has been written.
- **In-flight limit.** A connection holds at most `net.max_inflight_commands` (default 1024) commands in flight: dispatched and not yet written, or written and still owed a durable post. At that limit, or once its unwritten replies, counting those held for commands waiting to be durable, reach `net.write_backpressure_bytes`, it stops dispatching and stops reading its socket (§Trade-offs).

#### TCP server implementation

Implementation-level commentary on the listener that drives each connection's request pipeline. This section records the choices a reviewer should expect to see in the net and resp layers.

- **Reactors.** A pool of reactor threads, sized by `net.io_threads` (default `min(hardware_concurrency, 16)`). Each reactor owns a poller and the connections handed to it at accept. A connection, the engine work its commands start and anything left over when it closes stay on that reactor for their whole life. The tiering engine is shared: a command runs on its connection's reactor until it must wait, and is resumed there.
- **Completion inbox.** Each reactor has an inbox, into which the threads that finish a wait post an event: the WAL's publishers and flushers for durability, the cold I/O pool for a read or a load, and whichever thread releases a capacity a command waits for. An event either resumes a waiting command or delivers a durable result to a sequenced one. A post wakes the reactor's poller at most once per loop iteration. Events are routed by connection id and command slot, and connection ids are never reused, so a late event never reaches another command.
- **Cold I/O pool.** Cold reads block in `pread`, so they run on `engine.cold_io_threads` threads, never on a reactor. The default is 16, with no CPU term: a thread blocked in `pread` costs no CPU, so the count sizes the cold device's queue. On macOS NVMe with direct reads, the rule set before the runs, the smallest count reaching at least 90% of the best `GET`s/s in both of two repetitions, chose 16, and 32 lengthened the p99 ([Deployment](../../operations/deployment.md#cold-io-sizing)). Loads and read misses share one foreground queue, taken oldest first. Scans take at most all threads but one, 15 at the default, so a point read always has a thread. Cache fills wait in a bounded background queue that runs only while the foreground queue is empty. A job whose command's deadline has passed by the time a thread takes it fails without running.
- **Head-of-line trade-off.** A reactor never waits on durability, cold I/O or a capacity wait. What it still does inline is bounded work: parsing and serialising, deciding, reserving and applying under the shard lock, a short wait for a shard lock another reactor holds, filling frames, and in Complete a brief park while an earlier reservation in the same log is still being filled or published ([ADP-015](015-write-path-and-durability.md) §Asynchronous request execution). Within a connection a waiting command holds back the commands behind it, because command order is Redis's contract. Across connections nothing waits on it, so a cold read held in the pool does not delay a hot `GET` on another connection of the same reactor. Waiting moves to the cold I/O pool's queue instead: a pool that cannot keep up shows as deadlines passing in the queue ([Failure Modes](../../operations/failure-modes.md)).
- **Timers.** Every command on a connection has the same timeout, so its oldest has the earliest deadline. Each connection keeps one timer, at that deadline, in its reactor's hashed timing wheel (1 ms ticks). The poll timeout is the next due timer or the idle reaper's tick. At expiry:
  - a command not yet applied or answered is given up with its wait's timeout error and is never applied; engine work it still has outstanding finishes as cleanup on the reactor;
  - a sequenced write gets the durable-wait timeout error, and stays applied.
- **Back-pressure.** Per-connection write buffer with three thresholds:
  - `write_backpressure_bytes` (default 4 MiB), with held replies counted, stops dispatching and pauses reading;
  - `write_resume_bytes` (default 1 MiB) resumes it, once fewer than half of `net.max_inflight_commands` are in flight;
  - `write_hard_limit_bytes` (default 16 MiB) closes the connection.

  TCP's own receive-window flow control then propagates the pressure back to the client. Pausing is per connection: the reactor drops its read interest without affecting siblings. On resume the connection first dispatches the input it already buffered, then reads again: an edge-triggered poller reports no new event for bytes already read.
- **Closing never waits for the engine.** When a client disconnects, a command still waiting runs on to its outcome or its deadline, unheard, so a write may still be applied. A sequenced write's durable wait is dropped, and unwritten replies and undispatched input go with the socket. Whatever the engine still owes the closed connection is kept on its reactor's orphan list until it lands (`abyss_net_orphans`), and an event for nothing live or orphaned is dropped and counted (`abyss_net_late_completions_total`).
- **`QUIT` and protocol errors** stop dispatch. The connection closes once every earlier command has replied and its output has drained.
- **Idle reaper.** A sweep on every poll cycle (`net.reaper_tick_ms`, default 1 s) closes connections whose last activity is older than `idle_timeout`. Activity is a successful recv or send, so a client paused on back-pressure but still receiving the server's drain is not idle. A connection with a command in flight is never reaped.
- **Phase 4 migration path.** The poller interface stays the kernel-abstraction boundary; an `io_uring` poller would be a fourth backend without touching reactor or connection code. The reactor pool collapses to one reactor per shard, the connection-to-reactor binding moves from round-robin to key-hash, and the shared engine decomposes into per-shard state — none of which require changes to the surface defined in this ADP.

#### Shutdown

On SIGTERM the server stops in this order:

1. **Readiness first.** The server enters draining, so `/ready` stops passing (503, then its listener closes) and Kubernetes takes the pod out of Service endpoints before the data plane stops.
2. **Accepting stops.** The RESP listener is disarmed, and every connection stops reading and dispatching.
3. **Grace.** Commands already dispatched get up to `net.shutdown_grace_seconds` (default 30) to finish and have their replies written. The engine runs normally meanwhile.
4. **Force close.** Then every connection is closed (`reason=server_shutdown`).
5. **Unread pipelined input is dropped.** Commands the server had not yet read or dispatched go with the socket, unanswered, as after a crash; clients retry them elsewhere.
6. **Pending commands are given up.** A command not yet linearised is abandoned with `kUnavailable` (`command abandoned: server shutting down`) and never applied, and counted as `abyss_net_commands_abandoned_total{reason="shutdown"}`, not as a timeout. Its connection is already closed, so no client sees that error.
7. **Sequenced writes stay applied, but their replies are lost.** Under `process_crash` such a write was published, and so durable at its class, before it was sequenced. Under `power_loss` it is power-durable only if the WAL's final flush covers it. The engine's stop makes that flush over everything filled, so only an exit at the shutdown timeout, before the flush, leaves it short.
8. **The rest of the stop is bounded.** The engine then stops while the reactors go on taking its events: the cold I/O pool fails its queued jobs and joins the running ones, the cold consumers drain within `cold_consumer.drain_grace_seconds`, offsets persist, the WAL makes its final flush and stops, and every waiter fails. Each reactor drains its inbox until it is empty and stops. All of this runs under `net.shutdown_timeout_seconds` (default 60; the validator requires at least `cold_consumer.drain_grace_seconds` + `engine.command_timeout_ms` + 5 s). Past it the process logs CRITICAL `shutdown timed out; exiting without cleanup` with the stuck jobs and orphans, and exits with status 70 without running destructors.
9. **The pod's budget covers both.** `terminationGracePeriodSeconds` must be at least `net.shutdown_grace_seconds` + `net.shutdown_timeout_seconds` plus a margin for the process to exit, or Kubernetes kills it first ([Deployment](../../operations/deployment.md) §Shutdown).

### Configuration

```yaml
net:
  bind: 0.0.0.0
  port: 6379
  max_connections: 1024
  idle_timeout_seconds: 300
  io_threads: 0                       # reactors; 0 = min(hardware_concurrency, 16)
  write_backpressure_bytes: 4194304   # unwritten and held replies: stop reading
  write_resume_bytes: 1048576         # resume, with under half the in-flight cap
  write_hard_limit_bytes: 16777216    # close
  max_inflight_commands: 1024         # per connection, 1..65536
  shutdown_grace_seconds: 30          # in-flight commands finish
  shutdown_timeout_seconds: 60        # the rest of the stop; past it, exit 70

engine:
  command_timeout_ms: 5000            # a command's whole budget, durable wait included
  cold_io_threads: 16                 # cold read pool, 2..64; 0 also means 16
```

## Invariants

1. Every write command is routed through the queue. There is no path that writes directly to a store.
2. Unknown command names are rejected at the frontend with `ERR unknown command` and never touch the queue.
3. A write the sequencer rejects when deciding it (a type mismatch, an unsupported form) logs nothing; only decided effects reach the queue.
4. A write is not acknowledged until the sequencer has applied it to hot and its log frames have reached the configured durability class: published under `process_crash`, so it survives a process crash, or fdatasynced under `power_loss`, so it survives a power loss.
5. A write that times out on its durable wait returns an error to the client, but it is not discarded: it is already in the queue and applied to hot, and becomes durable at its class when the flush catches up. A command that times out, or is given up at shutdown, before it is applied is never applied.
6. RESP2 is the only protocol. `HELLO 3` is rejected with `NOPROTO`. RESP3-specific types are not emitted on the wire.
7. `RespValue` simple strings and bulk strings are distinct types. Error values are constructed via the prefix-enum helper, never hand-formatted.
8. Slot computation (`CRC16(key) mod 16384`, honouring `{hashtag}`) is phase-invariant. Only the slot-to-pod mapping changes between phases.
9. During recovery, data-plane commands return `LOADING`; the narrow admin set remains available.
10. Inline protocol is supported alongside RESP arrays; both produce the same `RespCommand`.
11. **Commands on a connection keep their order.** Each command on a connection reaches its linearisation point (a write applied to hot and published, a read's answer taken) before the next command on that connection is dispatched, and replies are written in command order. That holds at any `queue.log_count`. Acknowledged writes are never lost at their class (invariant 4); the caveat below concerns unacknowledged writes only. With `queue.log_count` above 1, pipelined writes to different logs become durable independently, so a power loss can keep a later unacknowledged write and lose an earlier one. The writes on a connection that survive a power loss therefore form a prefix of its writes only at `log_count = 1`, as Redis Cluster promises no order across slots.
12. A reactor thread never waits on a flush, cold I/O or a capacity wait. Its only waits are the shard or buffer locks and the bounded park in Complete on earlier reservations in the same log (ADP-015 §Where a reactor still waits). Closing a connection never waits for the engine. A shutdown applies no command it had not linearised, and one that overruns `net.shutdown_timeout_seconds` exits with status 70.

## Trade-offs

**Why RESP2 and not RESP3?** RESP2 is universally supported. Every Redis client library speaks it. RESP3 adds features (client-side caching hints, attribute types) Abyss does not need in Phase 1. RESP3 support can be added later without breaking existing clients.

**Why readiness polling (`epoll`, `kqueue`, `WSAPoll`) and not `io_uring`?** A reactor's time does not go on socket system calls: cold reads, which block in `pread`, run on the cold I/O pool, and durability waits on the WAL's flushers. Readiness polling on non-blocking sockets works the same way on Linux, macOS and Windows behind one interface. `io_uring` would save system calls per read and write, but the default seccomp profiles of Docker 25 and containerd 2.0 block it, so it would need a runtime probe and a fallback anyway. It stays a possible fourth backend, taken up only if profiles show the socket calls dominate.

**Why does a waiting command hold back its own connection, and no other?** Redis runs a connection's pipelined commands one after another, each seeing the effects of those before it, and clients rely on that: a `GET` sent after a `SET` on the same connection must see it. Abyss keeps that order by dispatching a command only once the one before it has reached its linearisation point. Commands on different connections have no order to keep, so a command waiting on a cold read, a load or a capacity holds back only its own connection, and a write waiting only for durability holds back nothing, so pipelined writes share flushes. The cost is that a pipeline stalls behind a cold miss at its head for as long as the read takes, up to `engine.command_timeout_ms`. Dispatching independent commands past it would need per-key dependency tracking in the frontend.

**Why split `kString` into `kSimpleString` and `kBulkString`?** RESP2 wire format treats them differently (`+OK\r\n` vs `$2\r\nOK\r\n`). Collapsing them forces the serializer to guess, which produces wrong output for clients that distinguish simple-string protocol constants from binary-safe bulk values. The split is a one-line type change with zero runtime cost; the value is correctness.

**Why reject unknown commands at the frontend rather than queue them?** The prior "classify as write" rule was conservative, based on "unknown commands might modify state". In practice: (a) known write commands are in the registry; (b) clients sending genuinely unknown commands have always been bugs. Rejecting at the frontend gives the client an immediate, actionable error and keeps the queue clean. The "write bias for safety" is preserved for the narrower case of *known* commands whose store support is incomplete.

**Why Phase 1 direct-key-only?** Enumeration commands (`KEYS`, `SCAN`) are expensive in any realistic workload and harder to implement correctly across tiered storage. Deferring them lets Phase 1 ship a smaller, more correct surface. They are Phase 2 candidates once the tiering engine is battle-tested.

**Why support inline protocol?** It costs ~30 lines in the parser and enables `redis-cli`, raw telnet, and ad-hoc debugging. Zero cost for real clients since none use it.

**Why single-shard CLUSTER responses in Phase 1?** Redis Cluster clients (Jedis, Lettuce, redis-py cluster mode, ioredis cluster mode) expect a topology on connect. Returning a valid single-shard topology lets cluster-aware clients work against Phase 1 unchanged. The alternative (rejecting CLUSTER commands until Phase 2) would force users to re-configure clients when migrating — a cost we avoid for a few lines now. Phase-invariant slot computation means clients hash keys identically in both phases.

**Why keep unknown-command bias as write for known-but-unsupported commands?** Safety for the narrow case where the registry knows about a command but the configured store does not yet support it. Classifying it as a read would bypass the write path entirely and risk silent data divergence if the store later gains support. Classifying it as a write sends it to the sequencer, which rejects a form it cannot decide before anything is logged.

**Why does a full connection stop reading rather than reply with errors?** A connection has at most `net.max_inflight_commands` commands in flight, dispatched and not yet written or written and still owed a durable post, and stops dispatching while its unwritten replies, counting those held for commands waiting to be durable, reach `net.write_backpressure_bytes`. At either limit it stops reading the socket: the commands already sent stay in the kernel and the client's TCP window closes, so nothing is shed or answered with an error. The cost is the deadlock Dragonfly documents (#3997): a client that writes a deep pipeline without reading any replies fills its own receive buffer, the server stops reading, and both wait. Dispatch stops well short of `net.write_hard_limit_bytes`, so in practice the idle timeout ends it: once every reply is written to the connection's buffer, nothing is in flight, and the reaper closes it after `net.idle_timeout_seconds`. Redis instead lets its output buffer grow to `client-output-buffer-limit`; Abyss bounds the memory a connection can hold and asks clients to read their replies.

**Why are unread pipelined commands dropped at shutdown?** When the server stops, every connection stops reading and dispatching at once. Commands already dispatched get `net.shutdown_grace_seconds` to finish; then the connections close, and a command still pending is given up, never applied, and counted as a `shutdown` abandon rather than a timeout. Commands the client sent but the server had not yet read or dispatched are dropped with the socket. The client sees its connection close with those commands unanswered, which is what a crash shows it too, and retries them against another node. Answering them would mean starting engine work the stop then has to wait for.

**Why cross-key atomicity for `MSET` within a pod, and `CROSSSLOT` across logs?** Standalone Redis executes multi-key commands atomically, and Redis Cluster rejects them across slots. Inside one pod the sequencer already takes shard locks in a fixed order and the log closes a batch by position, so one batch carrying every shard's effects makes `MSET`, `DEL`, `UNLINK`, `MSETNX`, `RENAMENX` and `COPY` atomic at little cost. Across WAL logs it would need a two-phase commit with a recovery protocol of its own, contradicting Abyss's simplicity principle, so those writes are rejected with `CROSSSLOT` instead (#169).
