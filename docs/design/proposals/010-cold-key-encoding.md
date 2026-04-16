# ADP-010: Cold Store Key Encoding

**Status:** Accepted
**Created:** 2026-04-15

## Context

[ADP-003](003-cold-store.md) defines the cold store interface and high-level behaviour but does not specify how Redis-level keys and data structures map onto RocksDB key-value pairs. Implementers of the RocksDB integration (#16), batch write (#17), TTL expiry (#18, #19), and the cold-side of compaction (#22-24, #27) need a precise, shared encoding to work against. Without it each will make incompatible ad-hoc decisions about layout, type tagging, TTL storage, and tombstones, and the cost of reconciling later compounds.

This proposal pins down the encoding for the Phase 1 Redis types — strings, hashes, sets, and sorted sets — and reserves prefix bytes so Phase 2 types (lists, streams, geo, HLL) can be added without renumbering or migrating existing data.

## Design

### Storage Model: One RocksDB KV per Member

Each Redis collection member, hash field, or sorted-set entry is stored as an individual RocksDB KV pair. Strings are stored as a single KV with their TTL header inlined alongside the value.

**Why not a serialized blob per Redis key?** Blob-per-key requires a read-modify-write of the entire collection on any mutation. That defeats the cold consumer's compaction value: a flush of "add three members to an existing 100K-member set" would rewrite the entire 100K-member blob. Per-member KVs make flush cost proportional to the number of changed members.

**Why not a hybrid (small-as-blob, large-as-per-member)?** A threshold introduces a transition point that operations near the boundary repeatedly cross. Not justified for Phase 1.

### Column Families

The cold store opens two column families:

| CF | Purpose |
|----|---------|
| `default` | All primary data: strings, meta records, hash fields, set members, zset member-indexed entries |
| `zset_score_idx` | Score-ordered secondary index for `ZRANGEBYSCORE`, `ZREVRANGEBYSCORE`, `ZRANGE BYSCORE`, etc. |

Single-CF for primary data keeps the hot path simple and lets WriteBatch atomicity hold across all primary types in one structure. The score index is split out because its access pattern (range scans by score) differs fundamentally from primary lookups (point lookups by member) and would otherwise pollute the default CF's block cache and bloom-filter selectivity.

There is deliberately no TTL-sorted secondary index. [ADP-003](003-cold-store.md) §TTL Expiry specifies active expiry as **random sampling with adaptive rate**, not deterministic sweep over a sorted index. A `ttl_idx` CF would impose a per-write index update on every TTL'd key (a measurable steady-state cost) to enable a strategy ADP-003 explicitly did not adopt. Lazy expiry consults the TTL fields already present in the primary record (inline for strings, meta record for collections), so no separate structure is needed for the read path either.

RocksDB `WriteBatch` operations span column families atomically, so the invariant "primary record + score-index entries flushed together" holds across CFs.

### Composite Key Layout

Redis keys are binary-safe — they may contain any byte, including `\x00`. This rules out sentinel-separator schemes (`h:<key>:<field>`, `h:<key>\x00<field>`). Composite keys instead use length-prefix encoding:

```
<type:1B> <keylen:varint> <key bytes> <field-or-member bytes>
```

`varint` is the unsigned LEB128 encoding (1 byte for keys ≤ 127 bytes, which covers the overwhelming majority of Redis key lengths in practice). The length prefix does not need to be decoded for prefix scans; iterating `<type><keylen><key>` is guaranteed to return only entries belonging to that key, because the next byte after `<key>` is the start of the field/member, not a separator that user data could collide with.

Strings (`0x01`) and the format-version record (`0xFF`) do not have a field/member tail, so they omit the length prefix and the key bytes form the entire suffix.

#### Type Byte Allocations

```
0x01  String                  default CF
0x02  Meta record             default CF   (collections only — strings inline their meta)
0x03  Hash field              default CF
0x04  Set member              default CF
0x05  Zset member-indexed     default CF   (member → score)
0x06  Zset score-indexed      zset_score_idx CF   (score‖member → ∅)

0x07-0x0F  Reserved for Phase 2+ (list, stream, geo, HLL)
0xFF       Reserved for system records (format version, statistics)
```

Type bytes are never reused or reordered. New Redis types take new bytes from the reserved range.

### Per-Type Encoding

#### String (`0x01`)

```
Key:    0x01 <key>
Value:  <flags:1> <abs_ttl_ms:8 BE> <payload bytes>
```

`flags`:
- bit 0: `1` if `abs_ttl_ms` is set, `0` if no TTL
- bits 1-7: reserved (must be zero)

`abs_ttl_ms` is the wall-clock expiry timestamp in milliseconds since Unix epoch, big-endian. The field is fixed-width even when no TTL is set so the payload offset is constant and decoding is branch-free. The flag bit is authoritative for "TTL present"; the timestamp is meaningless when the flag is unset.

Inlining TTL in the string value avoids a second `Get` on the string read path, which is the highest-frequency type.

#### Meta Record (`0x02`)

```
Key:    0x02 <inner_type:1> <key>
Value:  <flags:1> <abs_ttl_ms:8 BE> <cardinality:8 BE>
```

`inner_type` is the type byte of the collection (`0x03` hash, `0x04` set, `0x05` zset). Including it in the key disambiguates if a key were ever to exist as multiple types (which Redis forbids — see invariant 1) and keeps the layout correct even at the boundaries.

`cardinality` is the cached element count (serves `HLEN`, `SCARD`, `ZCARD`). The cold consumer's compaction buffer knows the net cardinality change per flush, so updating the meta record costs one extra KV write per flush and avoids an O(N) prefix scan on every cardinality query.

When a collection's cardinality drops to zero, the meta record is deleted along with the (already-empty) member range. This matches Redis semantics: a hash with no fields is a key that does not exist.

#### Hash Field (`0x03`)

```
Key:    0x03 <keylen:varint> <key> <field>
Value:  <field value bytes>
```

#### Set Member (`0x04`)

```
Key:    0x04 <keylen:varint> <key> <member>
Value:  empty
```

Empty value as sentinel — existence of the KV is the membership signal.

#### Zset Member-Indexed (`0x05`)

```
Key:    0x05 <keylen:varint> <key> <member>
Value:  <score:8 IEEE 754 double, native endianness>
```

Used for `ZSCORE` and `ZINCRBY` reads. Member-keyed lookup is the natural index for these.

#### Zset Score-Indexed (`0x06`, in `zset_score_idx` CF)

```
Key:    0x06 <keylen:varint> <key> <score:8 sortable, BE> <member>
Value:  empty
```

`score:8 sortable` is the IEEE 754 double encoded so that lexicographic byte comparison matches numeric comparison:

```cpp
uint64_t SortableDouble(double d) {
  uint64_t bits = std::bit_cast<uint64_t>(d);
  constexpr uint64_t kSignBit = 0x8000000000000000ULL;
  return (bits & kSignBit) ? ~bits : (bits | kSignBit);
}
```

Stored big-endian in the key. Including `<member>` after `<score>` in the key handles ties: Redis breaks score ties by lexicographic member order, which falls out of the byte ordering naturally.

Every `ZADD` writes both `0x05` and `0x06` records in the same WriteBatch. Every `ZREM` and `ZREMRANGEBYSCORE` deletes both.

#### System Record: Format Version (`0xFF`)

```
Key:    0xFF 0x00 "format"
Value:  <version:2 BE> <reserved bytes>
```

Written once at store initialization. Read on every `open()` to detect mismatched on-disk encoding. A version mismatch fails `open()` with a migration-required error rather than reading data with the wrong decoder.

The current version is `1`. Bumping the version requires a one-shot migration; no migration tooling is in scope for Phase 1, but the version byte exists so it is possible.

### Tombstones (Deletes)

**Strings:** single `Delete` on `0x01<key>` in a WriteBatch.

**Collections:** the cold consumer issues `DEL <key>`. The cold store:

1. Reads the meta record (`0x02 <inner_type> <key>`) to learn the type and cardinality. If absent, the key does not exist and the operation is a no-op.
2. Iterates the prefix `<inner_type> <keylen> <key>` collecting all member keys.
3. Issues `Delete` for each member key, the meta record, and (for zsets) every corresponding score-index entry, all in a single WriteBatch.

**Why iterate-and-delete instead of `DeleteRange`?** `DeleteRange` is the obvious shortcut — one call per type prefix, atomic, O(1) at write time. It has well-documented sharp edges:

- Long-lived snapshots prevent range tombstones from being garbage-collected during compaction; they accumulate in memory and on disk.
- Every `Get` and iterator traversal must consult active range tombstones, paying a small cost on every read regardless of whether range deletes are actually common.
- The memtable uses a separate range-deletion data structure with its own performance profile and code path.

Phase 1 doesn't strictly need long-lived snapshots, so most of these would not bite immediately. But "doesn't bite immediately" is exactly how tech debt accrues: the encoding decision propagates into every read path, and removing `DeleteRange` later means revisiting all of it. Iterate-and-delete has none of these caveats. Its only cost is O(cardinality) at delete time — and cardinality is bounded by what we're willing to write to a collection in the first place. Bulk deletes of multi-million-member collections are rare; even when they happen, RocksDB handles a single batched WriteBatch of that size cleanly.

The encoding scheme itself does not preclude `DeleteRange` — length-prefix keys give exact-byte prefix ranges. If a future workload demonstrates iterate-and-delete is the bottleneck, `DeleteRange` can be enabled per-operation as a tunable without changing the on-disk layout.

### TTL Encoding Summary

| Surface | Storage |
|---------|---------|
| String TTL | Inline in `0x01` value (`<flags><abs_ttl_ms><payload>`) |
| Collection TTL | `0x02` meta record (`<flags><abs_ttl_ms><cardinality>`) |

`abs_ttl_ms == 0` and `flags.bit0 == 0` together mean "no TTL". The flag bit is authoritative; the timestamp field is fixed-width purely so payload offset is constant.

`EXPIRE` / `PERSIST` semantics:
- On a string: rewrite the `0x01` value with new flags/ttl.
- On a collection: rewrite the `0x02` meta record.

#### Expiry Workflow (per ADP-003)

**Lazy expiry (#18)**: every cold-store read consults the TTL field of the primary record (inline for strings, meta for collections). If `flags.bit0` is set and `abs_ttl_ms <= now`, the read deletes the key (per the Tombstones rules above) and returns nil. No additional storage is consulted — the TTL is already on the read path.

**Active expiry (#19)**: a background thread samples N random keys from the keyspace and deletes any that have expired. Sampling is restricted to the `0x01` (string) and `0x02` (collection-meta) ranges, since these are the only types that carry a TTL. RocksDB iterators seeded from a randomised start position satisfy the "random sample" requirement; sampled keys without a TTL flag count as "not expired" for the adaptive-rate hit-ratio calculation. The adaptive sampling and disk-pressure-mode logic is fully specified in ADP-003 §TTL Expiry.

A TTL-sorted secondary index was considered and rejected — see Trade-offs.

### Type Discovery (`EXISTS`, `TYPE`, generic key ops)

Some commands operate on a key without knowing its type. Two candidates were considered:

1. **Probe in order with bloom filters.** Try `0x01<key>` (string), then `0x02 0x03 <key>`, `0x02 0x04 <key>`, `0x02 0x05 <key>` (meta records for hash, set, zset). Each miss is a bloom filter check, sub-microsecond on a hot bloom block.
2. **Maintain a key registry KV** (`0x00<key>` → `<type:1>`). One `Get` for `EXISTS`/`TYPE`, but every key creation and deletion now writes/deletes a registry entry too.

Probe-with-bloom is the default. The registry would add write amplification on the common path to optimise an uncommon path. If `EXISTS`/`TYPE` ever dominate the workload, the registry stays available as a future optimisation — adding it later does not break the current encoding.

### Worked Examples

Assume key `k = "user:42"` (7 bytes), no TTL unless stated. `varint(7)` is `0x07`.

**`SET user:42 "alice"`:**
```
default CF:  0x01 "user:42"  →  0x00  0x0000000000000000  "alice"
```

**`SET user:42 "alice" PX 60000`** (60s TTL, current wall ms = `T`):
```
default CF:  0x01 "user:42"                       →  0x01  (T+60000 BE)  "alice"
```

**`HSET h:1 name "bob" age "30"`** (key length 3):
```
default CF:  0x02 0x03 "h:1"               →  0x00  0x0000000000000000  0x0000000000000002
default CF:  0x03 0x03 "h:1" "name"        →  "bob"
default CF:  0x03 0x03 "h:1" "age"         →  "30"
```

**`SADD s:1 "a" "b"`:**
```
default CF:  0x02 0x04 "s:1"               →  0x00  0x0000000000000000  0x0000000000000002
default CF:  0x04 0x03 "s:1" "a"           →  ∅
default CF:  0x04 0x03 "s:1" "b"           →  ∅
```

**`ZADD z:1 1.5 "a" 2.5 "b"`:**
```
default CF:         0x02 0x05 "z:1"                            →  0x00  0x00...  0x02
default CF:         0x05 0x03 "z:1" "a"                        →  <1.5 native LE>
default CF:         0x05 0x03 "z:1" "b"                        →  <2.5 native LE>
zset_score_idx CF:  0x06 0x03 "z:1" <sortable(1.5)> "a"        →  ∅
zset_score_idx CF:  0x06 0x03 "z:1" <sortable(2.5)> "b"        →  ∅
```

**`DEL h:1`** (assuming the hash has 2 fields):
```
1. Get(0x02 0x03 "h:1")           → cardinality = 2, no TTL
2. Iterate prefix 0x03 0x03 "h:1" → ["name", "age"]
3. WriteBatch:
     Delete(0x02 0x03 "h:1")
     Delete(0x03 0x03 "h:1" "name")
     Delete(0x03 0x03 "h:1" "age")
```

**`HGETALL h:1`:**
```
1. Get(0x02 0x03 "h:1")           → cardinality > 0
2. Iterate prefix 0x03 0x03 "h:1" → all fields
```

## Invariants

1. Every Redis key has exactly one type at any given time. Cross-type entries for the same key are an upstream bug; the encoding does not enforce mutual exclusion across type prefixes, but the meta-record design makes such bugs detectable.
2. For collections, the meta record exists if and only if the collection has at least one member. Cardinality dropping to zero ⇒ meta and member range are both empty after the flush.
3. `WriteBatch` atomicity: primary records, meta records, and the zset score index are written and deleted in the same batch. A reader never observes a partial state (e.g., a zset member without its score-index counterpart).
4. Length-prefix composite keys make prefix scans tight: iterating `<type><keylen><key>` yields only entries for `<key>`, regardless of what user bytes follow.
5. The format-version record is read on every `open()`. A mismatch fails the open with a migration-required error rather than reading data with the wrong decoder.
6. Type bytes are immutable. Once allocated, a type byte's meaning never changes. New types take new bytes from the reserved range.
7. TTL is sourced exclusively from the primary record (inline for strings, meta for collections). No separate TTL index exists; lazy and active expiry both consult the primary record per ADP-003.

## Trade-offs

**TTL-sorted secondary index vs. inline TTL only.** Rejected the secondary index. ADP-003 §TTL Expiry specifies active expiry as random sampling with adaptive rate, deliberately matching Redis's probabilistic model. A `ttl_idx` CF would have enabled deterministic expiry-order iteration, but at the cost of an extra index write on every TTL'd key write, every TTL update, every `PERSIST`, every expiry, and every `DEL` of a TTL'd key. That is a measurable steady-state write amplification to enable a strategy ADP-003 explicitly did not adopt. Inline TTL on the primary record covers both lazy expiry (already on the read path) and active expiry (random sampling consults the same field). If a future ADP revisits ADP-003 and the random-sampling model proves inadequate, a TTL index can be added under a new type byte without changing the primary-record layout.

**Per-member vs. blob-per-key.** Resolved in favour of per-member: amortises with the compaction buffer, supports unbounded collection sizes without read-modify-write, and keeps `HGETALL`/`SMEMBERS` as a single prefix scan. Cost: more KV pairs in RocksDB. Acceptable; RocksDB is designed for this scale.

**Single CF + prefixes vs. CF-per-type.** Resolved as a middle path: one default CF for primary data plus a dedicated CF for the zset score index, whose access pattern (range scans by score) differs fundamentally from the point-lookup hot path on the default CF. Avoids over-engineering while isolating the score-range workload.

**Iterate-and-delete vs. `DeleteRange`.** Resolved in favour of iterate-and-delete: zero impact on the read path, no snapshot interaction, no range-tombstone garbage to manage. Pays O(cardinality) per `DEL` of a collection. The encoding leaves room to switch to `DeleteRange` per-operation later without on-disk changes if a workload demands it.

**Inline TTL on strings vs. uniform meta record.** Resolved as type-split: strings inline (one `Get`), collections use a meta record (one extra `Get`, but the meta record also caches cardinality). Strings are the highest-frequency type; saving them a `Get` is worth the asymmetry.

**Probe-with-bloom vs. key registry for `EXISTS`/`TYPE`.** Resolved in favour of probing: bloom filters make misses sub-microsecond, and the registry would add a write to every key mutation to optimise a less-common read. The registry stays available as a future optimisation if `EXISTS`/`TYPE` ever dominate the workload.

**Cardinality cached in meta vs. computed on demand.** Cached. Computing cardinality on demand is O(N) per call and would dominate `HLEN`/`SCARD`/`ZCARD` latency for large collections. The compaction-buffer flush already knows the delta, so maintaining the cache is cheap.

**Format version record.** Cheap insurance. The cost of writing it now is one extra KV. The cost of needing it later without it is a forced rewrite of the entire cold store.
