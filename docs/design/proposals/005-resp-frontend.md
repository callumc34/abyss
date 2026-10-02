# ADP-005: RESP Frontend

**Status:** Accepted
**Created:** 2026-04-09
**Updated:** 2026-05-31

> **Amended by [ADP-015](015-write-path-and-durability.md).** Dispatch becomes asynchronous: reactors never wait on durability or cold reads, each command is sequenced before the next on its connection, and replies keep command order (Phase 3). Conditional writes become ordinary sequenced writes that log their decided effects (Phase 2). §TCP server implementation's head-of-line trade-off and §Write Acknowledgement describe current behaviour until then.

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

Conditional writes (`SET NX`, `ZADD GT`, etc.) and the Consumer RPC primitive that underpins write acknowledgement are specified in [ADP-011](011-conditional-writes-and-consumer-rpc.md). This ADP references that design but does not duplicate it.

## Design

### Protocol Version Lock

RESP2 (Redis Serialisation Protocol version 2). This is the protocol spoken by every Redis client library in every language. Abyss does not implement RESP3 in Phase 1 — this is a hard invariant, not an aspiration.

- `HELLO 2` is accepted and responds with server info.
- `HELLO 3` is rejected with `NOPROTO unsupported protocol version`.
- `HELLO` with no args returns current handshake state.

Parser and serializer are RESP2-only. RESP3-specific types (maps, sets, doubles, big numbers, verbatim strings, push) are not emitted on the wire. When RESP3 lands in a future phase, it will be additive — existing RESP2 clients will not break.

### RespValue Type Contract

`RespValue` (core/resp_types.h) distinguishes simple and bulk strings as separate variants:

```cpp
enum class Type { kNull, kSimpleString, kBulkString, kInteger, kError, kArray };
```

Wire serialisation:

| Variant | Wire format | Use |
|---------|-------------|-----|
| `kNull` | `$-1\r\n` (bulk null) or `*-1\r\n` (array null) | Missing key, NX-fail, array absence |
| `kSimpleString` | `+<value>\r\n` | Protocol constants: `OK`, `PONG`, `QUEUED` |
| `kBulkString` | `$<len>\r\n<bytes>\r\n` | User values, binary-safe |
| `kInteger` | `:<n>\r\n` | Counts, bool as 0/1 |
| `kError` | `-<prefix> <message>\r\n` | See §Error Prefix Table |
| `kArray` | `*<len>\r\n<elems>` | Multi-value responses |

Parser emits the correct variant based on type byte (`+`/`-`/`:`/`$`/`*`). Serializer is deterministic — no guessing between simple and bulk.

Error construction is funneled through a single helper so the prefix vocabulary is enforced at the type level:

```cpp
RespValue::Error(ErrorPrefix::kWrongType,
                 "Operation against a key holding the wrong kind of value");
```

Hand-formatted error strings (`"-ERR ..."`) are disallowed.

### Error Prefix Table

| Prefix | Semantics | Owner | Emitted when |
|--------|-----------|-------|--------------|
| `ERR` | Generic: syntax, bad args, unknown command | Frontend | Unknown command, arity mismatch, malformed args, parse error |
| `WRONGTYPE` | Key exists with a different type | Hot / Cold store | Type mismatch at apply time |
| `LOADING` | Server is loading state from the queue | Frontend | Recovery gate open ([ADP-007](007-recovery.md)) |
| `MOVED <slot> <ip>:<port>` | Key belongs to a different shard | Frontend | Phase 2+ cluster routing ([ADP-008](008-horizontal-scaling.md)); emission path exists in Phase 1 but never triggered |
| `CROSSSLOT` | Multi-key command spans shards | Frontend | Phase 2+ multi-key across pods |
| `OOM` | Write rejected due to memory limit | Hot store | At `max_memory_bytes` and cannot evict |
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
| `kStateless` | Frontend computes locally; no consumer interaction | Direct return |
| `kTieredRead` | Read path: hot → buffer → cold ([ADP-006](006-read-write-paths.md)) | Direct sync tiered read |
| `kWritePath` | Unconditional write: queue append → hot apply | Consumer RPC ([ADP-011](011-conditional-writes-and-consumer-rpc.md)) |
| `kConditionalWrite` | Conditional: resolver resolves ([ADP-011](011-conditional-writes-and-consumer-rpc.md)) | Consumer RPC |
| `kConsumerRpc` | Requires live consumer state (e.g. `DBSIZE`, `OBJECT IDLETIME`) | Consumer RPC |
| `kFlush` | Broadcast wipe (`FLUSHDB`/`FLUSHALL`): per-shard `Flush` entry, fan-out apply ([ADP-006](006-read-write-paths.md) §Broadcast Write Path) | Consumer RPC (per shard × consumer) |

The request pipeline (issue #34) becomes a data-driven 6-way dispatch against the command registry, not a growing switch.

### Container Commands and Subcommands

`CLUSTER`, `CLIENT`, `CONFIG`, `COMMAND`, and `OBJECT` are container commands: their first argument selects a subcommand whose semantics may differ from the parent (different arity, dispatch class, or recovery-time visibility). The registry models these explicitly. Each container command's `CommandSpec` carries a list of `SubcommandSpec` entries, and the pipeline resolves the subcommand against that list before applying arity, dispatch, and `loading_safe` rules.

This means:

- The set of accepted subcommands is the registry's truth. Unknown subcommands surface `ERR Unknown <PARENT> subcommand` at the frontend and never reach the queue or any handler.
- Per-subcommand `loading_safe` is authoritative when a subcommand resolves. The narrow recovery-time allowlist (`CLUSTER SLOTS`, `CLUSTER INFO`, `CLUSTER MYID`, `COMMAND` / `COMMAND COUNT|INFO|DOCS`, `PING`, `INFO`, `HELLO`, `QUIT`) lives on the spec, not in handler-side `if` chains.
- `OBJECT ENCODING` (read) and `OBJECT IDLETIME` (consumer RPC) carry distinct `Dispatch` values on their respective subspecs; the parent's dispatch is unused when a subcommand resolves.
- Arity on `SubcommandSpec` is the total RESP arg count including the parent token, so `CLUSTER KEYSLOT key` is arity 3 and `COMMAND INFO [name ...]` is arity -2.

When a container is invoked with no subcommand argument (e.g. `COMMAND` alone), parent dispatch applies. Containers whose parent arity rules out the no-subcommand form (e.g. `CONFIG` with arity -3) cannot reach this fallback.

### Command Registry (Phase 1)

Phase 1 restricts the surface to **direct key access and modification**, plus the `FLUSHDB`/`FLUSHALL` global wipe (routed through the queue as a broadcast write — [ADP-006](006-read-write-paths.md) §Broadcast Write Path) — no enumeration (`KEYS`, `SCAN`, `RANDOMKEY`), no other global operations (`SWAPDB`, `MOVE`, `SELECT`), no pub/sub, no scripting, no transactions (groundwork exists in ADP-011; activation deferred), no streams.

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

**Admin — consumer RPC:**

| Command | Arity | Response | Reason |
|---------|-------|----------|--------|
| `DBSIZE` | 1 | Integer | Needs hot + cold key count |
| `INFO [section]` | -1 | Bulk string | Needs live stats from all consumers |

> **Phase 1 note.** Both commands are served from a synchronous `ServerStatsProvider` snapshot rather than a live Consumer RPC round-trip. The same stats already feed Prometheus gauges so a second query path would be redundant. When the Resolver lands, the plumbing can move to RPC without changing the command contract.

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

Both route through the queue as a per-shard `Flush` broadcast and ack only after every consumer on every owned shard has applied the wipe ([ADP-006](006-read-write-paths.md) §Broadcast Write Path). The `ASYNC`/`SYNC` modifier is accepted (hence arity -1) but ignored — the wipe is always synchronous. Single-pod Phase 1 has only DB 0, so `FLUSHALL` and `FLUSHDB` are equivalent. Both are `loading_safe = false`: during recovery they return `LOADING`.

**Strings (direct key only):**

| Command | Arity | Class | Dispatch | Response |
|---------|-------|-------|----------|----------|
| `GET key` | 2 | Read | TieredRead | Bulk or nil |
| `SET key value [options...]` | -3 | Write | Write or Conditional (§SET) | `+OK` / nil / bulk |
| `SETNX key value` | 3 | Write | Conditional | Integer 0/1 |
| `SETEX key seconds value` | 4 | Write | Write | `+OK` |
| `PSETEX key ms value` | 4 | Write | Write | `+OK` |
| `STRLEN key` | 2 | Read | TieredRead | Integer |
| `MGET key [key ...]` | -2 | Read | TieredRead (fan-out) | Array of bulk/nil |
| `MSET k v [k v ...]` | -3 | Write | Write (fan-out) | `+OK` |
| `MSETNX k v [k v ...]` | -3 | Write | Conditional (atomic) | Integer 0/1 |

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
| `ZADD key [NX\|XX\|GT\|LT] [CH] [INCR] score m ...` | -4 | Write | Write or Conditional | Integer / bulk |
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

`HSETNX` is registered separately under conditional writes. The remaining hash commands (`HINCRBY`, `HINCRBYFLOAT`, `HRANDFIELD`, `HSCAN`, `HSTRLEN`, `HEXPIRE` family) are deferred until the corresponding store ops are exposed.

When the hot store does not hold the key — typical after eviction — multi-field hash reads (`HGETALL`, `HKEYS`, `HVALS`, `HLEN`, `HMGET`, `HEXISTS`) are answered by merging the compaction buffer's overlay with cold's persisted state in the tiering engine. The buffer alone never holds the complete picture for these reads: its hash-field map captures only net writes since the last flush. The merge enforces buffer-removed fields and buffer-overridden values without dropping cold-resident fields.

**Generic / key management (direct key only):**

| Command | Arity | Class | Dispatch | Response |
|---------|-------|-------|----------|----------|
| `DEL key [key ...]` | -2 | Write | Write | Integer (count) |
| `UNLINK key [key ...]` | -2 | Write | Write | Integer (Phase 1: alias for DEL) |
| `EXISTS key [key ...]` | -2 | Read | TieredRead | Integer |
| `EXPIRE key s [NX\|XX\|GT\|LT]` | -3 | Write | Write or Conditional | Integer 0/1 |
| `PEXPIRE key ms [NX\|XX\|GT\|LT]` | -3 | Write | Write or Conditional | Integer 0/1 |
| `EXPIREAT key ts [NX\|XX\|GT\|LT]` | -3 | Write | Write or Conditional | Integer 0/1 |
| `PEXPIREAT key ts-ms [NX\|XX\|GT\|LT]` | -3 | Write | Write or Conditional | Integer 0/1 |
| `PERSIST key` | 2 | Write | Write | Integer 0/1 |
| `TTL key` | 2 | Read | TieredRead | Integer |
| `PTTL key` | 2 | Read | TieredRead | Integer |
| `EXPIRETIME key` | 2 | Read | TieredRead | Integer |
| `PEXPIRETIME key` | 2 | Read | TieredRead | Integer |
| `TYPE key` | 2 | Read | TieredRead | Simple string |
| `RENAMENX src dst` | 3 | Write | Conditional | Integer 0/1 |
| `COPY src dst [DB n] [REPLACE]` | -3 | Write | Conditional | Integer 0/1 |
| `OBJECT ENCODING key` | 3 | Read | TieredRead | Bulk |
| `OBJECT IDLETIME key` | 3 | Read | ConsumerRpc | Integer (from hot consumer LRU) |

**Excluded in Phase 1** (reject with `ERR unknown command`):

- Enumeration — `KEYS`, `SCAN`, `HSCAN`, `SSCAN`, `ZSCAN`, `RANDOMKEY`
- Global — `SWAPDB`, `MOVE`, `SELECT` (non-zero DB). `FLUSHDB`/`FLUSHALL` are **supported** — see §Command Registry above and [ADP-006](006-read-write-paths.md) §Broadcast Write Path.
- Transactions — `MULTI`, `EXEC`, `DISCARD`, `WATCH`, `UNWATCH` ([ADP-011](011-conditional-writes-and-consumer-rpc.md) lays the groundwork; activation deferred to Phase 2+)
- Pub/sub — `SUBSCRIBE`, `UNSUBSCRIBE`, `PSUBSCRIBE`, `PUNSUBSCRIBE`, `PUBLISH`, `PUBSUB`
- Scripting — `EVAL`, `EVALSHA`, `SCRIPT`, `FUNCTION` (`NOSCRIPT` where Redis semantics demand it)
- Streams — `XADD`, `XREAD`, `XRANGE`, `XREVRANGE`, `XLEN`, `XDEL`, `XGROUP`, `XACK`, ...
- Debug — `DEBUG`, `MONITOR`, `LATENCY`, `SLOWLOG`
- Dump / restore — `DUMP`, `RESTORE`, `MIGRATE`
- Deferred types — lists (`LPUSH` et al.), bitmaps, hyperloglog, geo. Rejected with `ERR unknown command` in Phase 1; added in Phase 2 when the hot store supports them. Hashes are partially supported (see "Hashes" above); the remaining hash commands listed there are deferred.
- Read-modify-write — `APPEND`, `DECR`, `DECRBY`, `GETDEL`, `GETSET`, `INCR`, `INCRBY`, `INCRBYFLOAT`, `SPOP`, `ZINCRBY`. See below.
- Multi-key rename — `RENAME`. `RENAMENX` is supported; the unconditional form is excluded with the read-modify-write group because it shares their resolver dependency and additionally spans two keys, so under [ADP-014](014-slot-routing-and-topology.md) slot routing it needs cross-shard atomicity that does not exist yet.

#### Why read-modify-write commands cannot be plain writes

These were briefly advertised as unconditional writes with no typed op behind them, so the frontend accepted them, durably logged them, and only then failed at apply. The fix is not to add parsers — it is that the unconditional write path is the wrong home for them.

Every one of these commands computes its result from the key's current value. The compaction buffer that feeds the cold tier absorbs and emits *typed ops*, never raw commands, and it holds only what it has seen in the current window — the key may be cold-resident, in which case the buffer cannot know the value at all. A statement-form increment therefore cannot be absorbed: N increments cannot collapse into one op without evaluating them, and cold would have to perform its own read-modify-write to apply what it was given. Two tiers independently re-deriving a value from a statement is precisely the divergence invariant 3 exists to prevent.

This is why Redis can record `INCR` verbatim in its AOF and Abyss cannot: Redis has a single authoritative copy and no compacting secondary view. Systems that do have one resolve atomic operations to concrete values before the log records them.

Abyss already has the mechanism — the resolver, which reads current state and emits a `Resolved` entry carrying materialised ops and a return value ([ADP-011](011-conditional-writes-and-consumer-rpc.md)). The correct implementation routes these commands through it, so the log records the computed result rather than the intent to compute. Hot and cold then apply an ordinary concrete write, compaction collapses it normally, and replay is deterministic because the resolved value is in the log. Until that lands they are excluded rather than advertised, so clients get an honest, feature-detectable `ERR unknown command` instead of an accepted write that fails after a round trip.

### SET Command Full Specification

`SET key value [EX s | PX ms | EXAT ts | PXAT ts-ms | KEEPTTL] [NX | XX] [GET]`

Supported options:

| Option | Semantics | Interaction with two-TTL model |
|--------|-----------|--------------------------------|
| `EX seconds` | Set absolute TTL to `seconds` from now | Sets `ttl`. Does NOT set `eviction` — eviction remains global/per-prefix config. |
| `PX ms` | Set absolute TTL to `ms` from now | Same, ms precision. |
| `EXAT unix-seconds` | Set absolute TTL to a Unix timestamp | Same, absolute. |
| `PXAT unix-ms` | Set absolute TTL to a Unix ms timestamp | Same, absolute ms. |
| `KEEPTTL` | Preserve existing TTL on the key | Conditional — Resolver reads existing TTL ([ADP-011](011-conditional-writes-and-consumer-rpc.md)). |
| `NX` | Only set if key does not exist | Conditional ([ADP-011](011-conditional-writes-and-consumer-rpc.md)). |
| `XX` | Only set if key exists | Conditional. |
| `GET` | Return prior value, set new | Conditional; value returned via Consumer RPC return value. |

Excluded options:

| Option | Reason |
|--------|--------|
| `IDLE seconds` | Redis-specific LRU knob; does not fit Abyss's eviction model (eviction is duration-based, not LRU-based except under memory pressure). |

Dispatch rule:

- No conditional options (no `NX`/`XX`/`KEEPTTL`/`GET`) → `kWritePath`.
- Any conditional option present → `kConditionalWrite`.

TTL option conflict detection (multiple TTL forms in one command) produces `ERR syntax error` at the frontend.

### Unknown Command Behaviour

Two distinct layers of "unknown":

**1. Unknown command name** — name not in the registry.

Frontend immediately responds `ERR unknown command '<NAME>', with args beginning with: <first-arg>` (Redis-canonical format) and does not touch the queue. This includes the Phase-1-deferred types (hashes, lists, streams, geo) — rejected at the frontend, not queued for rejection at apply time.

This supersedes the prior ambiguous "classify unknown as write" language.

**2. Known command name, malformed arguments** — the command has a typed-operation parser and that parser rejects these arguments.

Arity is a weaker check than the parser: `SET k v BOGUS` and `HSET k f v f` both satisfy their registry arity and are still malformed. The frontend validates such a command against its canonical parser before routing and rejects it with `ERR` (the prefix table's "malformed args" case). Nothing unparseable reaches the queue. Recording a command in the log that no tier can materialise would durably preserve an intent that can never be applied, and the queue is the single source of truth — it should not accumulate entries that are meaningless to every materialised view.

**3. Known command name, no typed-operation parser exists** — a log entry names a command no tier in this build can materialise.

This is no longer reachable from the wire. The registry advertises an unconditional write only when a typed-operation parser backs it, and that agreement is asserted rather than maintained by hand (`CommandRegistryTest.EveryUnconditionalWriteCommandHasAParser`). Fan-out writes are exempt because the engine decomposes them into per-key commands before anything is queued; conditional writes are exempt because the resolver materialises them into concrete ops.

The case survives for replay: a log written by a build with a wider command surface can still contain such an entry. Consumers must distinguish it from genuine decoder skew. An entry no parser can decode was not applied by any tier, so the materialised views agree it produced nothing and the consumer skips it, counting the occurrence. Quarantining it — refusing to advance past it — would be wrong: it protects nothing, and while the registry could still advertise such a command it let any client suspend log retention indefinitely. Quarantine is reserved for an entry whose parser exists and fails, which is a real skew bug because another tier accepted the same bytes.

**Arity mismatch** — known command with wrong argument count. Frontend rejects with `ERR wrong number of arguments for '<name>' command`, no queue interaction.

### HELLO and Handshake State

Per-connection state (frontend-local, not a queue concern):

```
ConnectionState {
  uint64_t client_id;           // monotonic frontend counter
  std::string client_name;      // set by CLIENT SETNAME
  int protocol_version = 2;     // locked to 2 in Phase 1
  bool authenticated = true;    // Phase 1: always true (no AUTH)
}
```

`HELLO` responses (map-as-array per RESP2 convention — alternating key/value pairs):

- `HELLO` with no args → current handshake state: `server=abyss, version=<build>, proto=2, id=<client_id>, mode=cluster|standalone, role=master, modules=[]`.
- `HELLO 2` → same payload, confirms protocol negotiation.
- `HELLO 3` → `NOPROTO unsupported protocol version`.
- `HELLO 2 AUTH <user> <pass>` → Phase 1 ignores AUTH arguments (future: verify credentials).
- `HELLO 2 SETNAME <name>` → sets `client_name`.

`CLIENT ID` reads from connection state; never a queue concern. `CLIENT SETNAME` / `CLIENT GETNAME` operate on connection state. All handshake commands are `kStateless` — Consumer RPC is not involved.

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

`MGET`, `MSET`, `DEL`, `UNLINK`, and `EXISTS` accept multiple keys that may target different shards. The registry tags each one with a `MultiKeyKind` (kMget / kMset / kDelete / kExists); the request pipeline routes those through `CommandDispatcher::DispatchFanOut`, and the engine decomposes per key before queueing or aggregating. `MSETNX` is the exception — it is conditional and routes through the Resolver instead.

**Single-pod (Phase 1):** All keys are local. The engine decomposes:

- `MGET k1 k2 ...` → per-key single-key reads across hot → buffer → cold (tombstone-aware), assembled positionally. A `WRONGTYPE` on any key collapses to nil at that slot, matching Redis behaviour.
- `MSET k1 v1 k2 v2 ...` → N `SET` `Write` queue entries, each appended to its owning shard's WAL.
- `DEL` / `UNLINK k1 k2 ...` → N single-key `DEL` `Write` queue entries; per-key integer replies are summed.
- `EXISTS k1 k2 ...` → per-key existence probe across hot → buffer → cold; the buffer probe overrides cold (a not-yet-flushed `DEL` reports the key as absent even if cold still holds a residual). Duplicate keys are counted once each, matching Redis.

There is no cross-key atomicity guarantee for `MSET`, `DEL`, or `UNLINK` — a crash or partial failure mid-decomposition may persist some keys but not others. The client receives an error and may retry. This matches DragonflyDB's behaviour under internal sharding and is consistent with how Redis Cluster clients handle cross-slot fan-out.

For partial failures specifically: if one sub-command's `BeginAppend` fails after prior subs succeeded, the prior pendings still auto-publish on scope exit (their destructor calls `Publish`) — the queue durably absorbs them, the responsible consumers apply them, and the client sees the first error. Subsequent reads observe the partial state.

`MSETNX` is a conditional multi-key write — routed to the Resolver ([ADP-011](011-conditional-writes-and-consumer-rpc.md)), which evaluates existence of all keys atomically under shard-striped locks and emits a single `Resolved` entry that either applies all sets or none.

**Multi-pod (Phase 2+):** If keys span pods, Abyss responds with `CROSSSLOT` error. Clients handle this by fanning out per-slot and assembling results client-side. Users can use hash tags (e.g. `{user123}.name`, `{user123}.email`) to co-locate related keys on the same shard.

### Write Acknowledgement

A write is acknowledged to the client only after two things have happened:

1. The queue `Append()` completes (write is durable — for group commit, the batch fsync has completed).
2. The responsible consumer has applied the write and fulfilled the Consumer RPC promise ([ADP-011](011-conditional-writes-and-consumer-rpc.md)).

For **unconditional writes** (`kWritePath`), the responsible consumer is the hot consumer. Fulfillment carries `RespValue::SimpleString("OK")` or an error.

For **conditional writes** (`kConditionalWrite`), the responsible consumer is the Resolver, which fulfils the promise with the resolved `RespValue` (`+OK` / nil / bulk for `SET GET` / integer for `SETNX` / array for `ZADD CH INCR`).

These happen in parallel with the queue fsync — see [ADP-011](011-conditional-writes-and-consumer-rpc.md) for the full parallel-execution pattern and latency profile.

The queue is the sole write path — there is no dual write. Consumer RPC fulfillment is an in-process synchronisation: the write handler registers a promise keyed by the queue sequence ID, then awaits it. The responsible consumer fulfils the promise after applying.

**Timeout:** Configurable, default 5s. If the responsible consumer fails to fulfil within this window, the handler returns a Redis error to the client. The write is still durable in the queue and will eventually be applied.

See [ADP-006](006-read-write-paths.md) for read and write path details, and [ADP-011](011-conditional-writes-and-consumer-rpc.md) for Consumer RPC and conditional resolution.

### Loading State

During recovery ([ADP-007](007-recovery.md)), the RESP port accepts connections but responds to data-plane commands with `LOADING Abyss is loading the dataset in memory`. A narrow admin set (`PING`, `INFO`, `CLUSTER SLOTS`, `HELLO`, `QUIT`, `COMMAND`) remains available so clients and orchestrators can observe state and validate readiness.

The `LOADING` gate is a frontend-level check before dispatch. It lifts automatically when recovery completes.

### Connection Handling

- Async I/O via `io_uring` (Linux) with `epoll` fallback. macOS uses `kqueue` via the abstraction layer for development parity.
- Max concurrent connections: configurable, default 1024.
- Idle timeout: configurable, default 300s.
- **Pipelining** — the parser is incremental: it returns one command plus bytes-consumed per invocation, and the caller loops until no progress. Pipelined requests are processed sequentially per connection; responses are written in arrival order. No special pipeline buffering is required beyond the per-connection socket buffer.

#### TCP server implementation

Implementation-level commentary on the listener that drives the per-connection `RequestPipeline`. The interface contract above is unchanged; this section records the choices a reviewer should expect to see in `include/abyss/net/`.

- **Concurrency.** A pool of reactor threads, sized by the `net.io_threads` config (default `min(hardware_concurrency, 16)`). Each reactor owns a `Poller` (epoll on Linux, kqueue on macOS) and a sticky set of connections — connections do not migrate across reactors after accept. The shared `CommandDispatcher` (the tiering engine) handles the cross-reactor synchronisation. Phase 4 shared-nothing per-core (#59-#63) tightens reactor count to shard count and removes the shared dispatcher.
- **Head-of-line trade-off.** `RequestPipeline::Dispatch` is synchronous from the reactor's view. A slow dispatch (group-commit fsync stall, cold-store p99 spike) blocks one reactor's other connections for the duration of that call, but not the other reactors. Operators tune `net.io_threads` to bound the blast radius of a single slow dispatch.
- **Back-pressure.** Per-connection write buffer with three thresholds: `write_backpressure_bytes` (default 4 MiB) pauses reading on the connection; `write_resume_bytes` (default 1 MiB) re-enables it; `write_hard_limit_bytes` (default 16 MiB) closes the connection. TCP's own receive-window flow control then propagates the pressure back to the client. The pause/resume transitions are fd-level — the reactor disarms `kReadable` on the offending connection without affecting siblings.
- **Shutdown grace.** `RequestStop` is non-blocking: it flips a flag and wakes every reactor. Each reactor disarms its listener registration (acceptor only), then continues to drain in-flight responses for `net.shutdown_grace_seconds` (default 30, aligned with Kubernetes' `terminationGracePeriodSeconds`). On expiry the reactor force-closes remaining connections with `reason=server_shutdown`. `Join` waits for every reactor to exit; the split mirrors the per-shard `RequestStop`/`Join` pattern from #94 — never call a blocking join inside a stop loop.
- **Idle reaper.** A wall-clock-cheap sweep on every Poll-Wait cycle (default 1 s) closes connections whose `last_activity` is older than `idle_timeout`. Activity refers to either successful recv or successful send, so a client paused on back-pressure but still receiving the server's drain is not idle.
- **Phase 4 migration path.** The `Poller` interface stays as the kernel-abstraction boundary; an `IoUringPoller` becomes a third backend without touching reactor or connection code. The reactor pool collapses to one reactor per shard, the connection-to-reactor binding moves from round-robin to key-hash, and the shared dispatcher decomposes into per-shard state — none of which require changes to the surface defined in this ADP.

### Configuration

```yaml
resp:
  bind: 0.0.0.0
  port: 6379
  max_connections: 1024
  idle_timeout_seconds: 300
  consumer_rpc_timeout_ms: 5000
```

## Invariants

1. Every write command is routed through the queue. There is no path that writes directly to a store.
2. Unknown command names are rejected at the frontend with `ERR unknown command` and never touch the queue.
3. Known commands whose store does not support them reach the queue and surface errors at apply time.
4. A write is not acknowledged until the queue append is durable AND the responsible consumer (hot for `kWritePath`, Resolver for `kConditionalWrite`) has fulfilled its Consumer RPC promise.
5. The promise timeout returns an error to the client but does not discard the write from the queue.
6. RESP2 is the only protocol. `HELLO 3` is rejected with `NOPROTO`. RESP3-specific types are not emitted on the wire.
7. `RespValue` simple strings and bulk strings are distinct types. Error values are constructed via the prefix-enum helper, never hand-formatted.
8. Slot computation (`CRC16(key) mod 16384`, honouring `{hashtag}`) is phase-invariant. Only the slot-to-pod mapping changes between phases.
9. During recovery, data-plane commands return `LOADING`; the narrow admin set remains available.
10. Inline protocol is supported alongside RESP arrays; both produce the same `RespCommand`.

## Trade-offs

**Why RESP2 and not RESP3?** RESP2 is universally supported. Every Redis client library speaks it. RESP3 adds features (client-side caching hints, attribute types) Abyss does not need in Phase 1. RESP3 support can be added later without breaking existing clients.

**Why `io_uring` with `epoll` fallback?** `io_uring` offers the best async I/O performance on modern Linux kernels. `epoll` is the fallback for older kernels and environments where `io_uring` is unavailable (some container runtimes restrict it).

**Why split `kString` into `kSimpleString` and `kBulkString`?** RESP2 wire format treats them differently (`+OK\r\n` vs `$2\r\nOK\r\n`). Collapsing them forces the serializer to guess, which produces wrong output for clients that distinguish simple-string protocol constants from binary-safe bulk values. The split is a one-line type change with zero runtime cost; the value is correctness.

**Why reject unknown commands at the frontend rather than queue them?** The prior "classify as write" rule was conservative, based on "unknown commands might modify state". In practice: (a) known write commands are in the registry; (b) clients sending genuinely unknown commands have always been bugs. Rejecting at the frontend gives the client an immediate, actionable error and keeps the queue clean. The "write bias for safety" is preserved for the narrower case of *known* commands whose store support is incomplete.

**Why Phase 1 direct-key-only?** Enumeration commands (`KEYS`, `SCAN`) are expensive in any realistic workload and harder to implement correctly across tiered storage. Deferring them lets Phase 1 ship a smaller, more correct surface. They are Phase 2 candidates once the tiering engine is battle-tested.

**Why support inline protocol?** It costs ~30 lines in the parser and enables `redis-cli`, raw telnet, and ad-hoc debugging. Zero cost for real clients since none use it.

**Why single-shard CLUSTER responses in Phase 1?** Redis Cluster clients (Jedis, Lettuce, redis-py cluster mode, ioredis cluster mode) expect a topology on connect. Returning a valid single-shard topology lets cluster-aware clients work against Phase 1 unchanged. The alternative (rejecting CLUSTER commands until Phase 2) would force users to re-configure clients when migrating — a cost we avoid for a few lines now. Phase-invariant slot computation means clients hash keys identically in both phases.

**Why keep unknown-command bias as write for known-but-unsupported commands?** Safety for the narrow case where the registry knows about a command but the configured store does not yet support it. Classifying it as a read would bypass the queue entirely and risk silent data divergence if the store later gains support. Classifying it as a write means it's captured durably in the queue; the store returns an error at apply time.

**Why no cross-key atomicity for `MSET`?** Redis itself doesn't guarantee atomicity for multi-key commands across slots in cluster mode. `MSET` is syntactic sugar for multiple `SET` commands. Providing atomicity would require distributed transactions, contradicting Abyss's simplicity principle. Partial failures are explicitly acceptable — the client can retry failed keys. `MSETNX` is the exception: it is conditional and routes through the Resolver, which does evaluate all keys atomically under shard-striped locks.
