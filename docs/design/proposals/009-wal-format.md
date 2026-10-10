# ADP-009: WAL Segment and Entry Format

**Status:** Accepted
**Created:** 2026-04-15
**Updated:** 2026-04-18

> **Amended by [ADP-015](015-write-path-and-durability.md).** Reads use a sparse in-memory sequence-to-offset index rebuilt at open, as §Positioned reads describes. Segments become preallocated and zero-filled, with a zero body length marking the end of the log, and are organised as one physical log per volume carrying per-shard streams (Phase 1b). Entries carry decided effects (Phase 2). The format below is current until each phase lands.

## Context

ADP-001 defines the Queue interface and describes the embedded WAL as per-shard fixed-size segment files. This document specifies the concrete on-disk format: how segment headers are laid out, how entries are encoded, how the format evolves, and how readers detect corruption.

The format must:

- Decode quickly on the hot consumer's critical path — every entry passes through this loop.
- Survive partial writes at segment tails without poisoning the rest of the file.
- Support additive schema changes without requiring a major format version bump or a data migration.
- Add no heavy build dependencies.
- Be inspectable with `xxd` or `hexdump` for debugging.

These goals rule out protobuf (tag iteration and heap allocation overhead on the hot path, codegen at build time) and flatbuffers (codegen at build time, cache-unfriendly layout for our access pattern). The format defined here is a custom binary layout with per-entry CRC32C integrity checks and an explicit version dimension for schema evolution.

This ADP applies only to the embedded WAL. External broker profiles (ADP-001 "External Broker Mapping") use the broker's native record format.

## Design

### Directory layout

```
/data/wal/
├── shard-0000/
│   ├── 00000000000000000000.log
│   ├── 00000000000000065536.log
│   └── 00000000000000131072.log
├── shard-0001/
│   └── ...
└── offsets/
    └── offsets.ckpt
```

`offsets.ckpt` is the dual-slot checkpoint of every retention consumer's committed offset per shard ([ADP-001](001-queue-wal.md) §Offset persistence). Hot keeps no offset: recovery rebuilds it from each shard's oldest retained entry.

Segment filenames are `{base_seq:020d}.log` — the base sequence ID of the segment, zero-padded to 20 digits. This produces a total order under lexicographic sort that matches sequence order, so directory listings can be iterated without parsing filenames.

### Segment header

Fixed size: 32 bytes. Little-endian throughout.

| Offset | Size | Field           | Description |
|--------|------|-----------------|-------------|
| 0      | 4    | `magic`         | Byte sequence `0x41 0x57 0x41 0x4C` ("AWAL") |
| 4      | 1    | `format_major`  | Breaking format version. Starts at `1` |
| 5      | 1    | `format_minor`  | Additive format version. Starts at `0` |
| 6      | 2    | `flags`         | Reserved. Zero on writes. Readers must ignore unknown flags |
| 8      | 4    | `shard_id`      | Redundant with directory path; sanity check |
| 12     | 8    | `base_seq`      | Redundant with filename; self-describing |
| 20     | 8    | `created_at_us` | Wall-clock creation time in microseconds since Unix epoch |
| 28     | 4    | `header_crc`    | CRC32C over bytes 0–27 |

The header is written once, when the segment is created, and never modified.

### Entry

Each entry consists of a length prefix, a body, and a body CRC:

```
┌─────────────────────────────────────────────────┐
│  body_len   u32   byte length of body           │
│  body:                                          │
│    type           u8    entry kind              │
│    seq            u64   sequence id             │
│    appended_us    i64   wall-clock µs           │
│    arg_count      u32   number of args          │
│    for each arg:                                │
│      arg_len     u32                            │
│      arg_bytes   raw bytes                      │
│    batch_last_seq u64   (format ≥1.1) closing   │
│                         seq of this entry's     │
│                         batch; equals `seq` for │
│                         single-entry appends    │
│  body_crc   u32   CRC32C over body              │
└─────────────────────────────────────────────────┘
```

Fixed envelope cost: 37 bytes per entry at format 1.1, plus 4 bytes per argument on top of the argument payload. A typical `SET foo bar` (three 3-byte args) costs 58 bytes on disk; a `SET key <1 KiB value>` costs ~1079 bytes (~5% overhead).

**Entry types** (`type` byte):

| Value | Meaning | Body fields beyond the common header |
|-------|---------|--------------------------------------|
| `0`   | `Write` — unconditional RESP command | None beyond `arg_count`/args |
| `1`   | `Conditional` — RESP command + predicate. See [ADP-011](011-conditional-writes-and-consumer-rpc.md). | Predicate tag + predicate-specific args, appended after args |
| `2`   | `Resolved` — decision for a prior `Conditional`. See [ADP-011](011-conditional-writes-and-consumer-rpc.md). | Ref seq (u64), decision (u8), optional materialised RESP command, optional return value (RESP2-serialised) |
| `3`   | `Flush` — FLUSHDB / FLUSHALL tombstone. See [ADP-006](006-read-write-paths.md) §Broadcast write path. | None — the type byte alone is the entry. `arg_count` is zero. |
| `4–255` | Reserved for future use | |

All four entry types are part of Format 1. Abyss is pre-alpha and greenfield — no deployed WAL exists outside development, so there is no backward-compatibility burden. The initial WAL implementation may land support for the types incrementally (type=0 first, then 1 and 2 alongside the Resolver, then 3 alongside FLUSHDB), but the format-version surface is fixed from the start. Byte-level layouts for `Conditional` and `Resolved` are specified in a follow-up PR that lands with the Resolver implementation; the type-byte assignments are pinned here so ADP-001 and ADP-011 can reference stable values without cross-doc drift.

`Flush` is a "cannot safely skip" entry — a reader that ignored it would surface keys the wipe has already removed. Strict post-alpha rules (§Schema evolution) would require a major bump to introduce it. Pre-alpha we keep `format_major = 1` and `format_minor = 1`: there is no deployed reader to fall behind. Post-alpha, any further entry type with the same skip-unsafe profile triggers a major bump.

Post-alpha, the major/minor compatibility rules in the Schema Evolution section become binding and any new entry type that older readers cannot safely skip (analogous to `Conditional`/`Resolved`'s pairing dependency, or `Flush`'s wipe semantics) requires a major version bump.

### Integrity: CRC32C

The format uses CRC32C (Castagnoli polynomial) for integrity. CRC32C is:

- Fast: hardware-accelerated on x86-64 (SSE4.2) and ARMv8 (CRC extension), running at cache-line throughput.
- Strong enough for storage corruption detection: not cryptographic, but catches the accidental bit flips, torn writes, and truncations we care about.

Two CRCs are computed:

- **Header CRC:** covers the 28 bytes preceding it. Verified on segment open.
- **Body CRC:** covers the `body_len` bytes between `body_len` and `body_crc`. Verified on each entry decode.

`body_len` itself is not covered by the CRC. If it is corrupted, the reader will either compute CRC over the wrong bytes (mismatch) or attempt a short read past the segment end. Either is detected at that entry.

### Schema evolution

The format supports two axes of versioning:

- **`format_major` (u8)** — breaking changes. Readers refuse to open segments with a different major version than they understand.
- **`format_minor` (u8)** — additive changes. Readers of the same major version can read any minor version, higher or lower.

**Format 1.1** adds `batch_last_seq: u64` at the end of the body, before `body_crc`. Its value is the sequence ID of the final entry of the batch this entry belongs to. Single-entry appends set it to `entry.seq`. `AppendBatch` assigns the same value — the batch's last seq — to every entry in the batch. Recovery advances the segment's durable tail only at entries where `seq == batch_last_seq`; mid-batch entries whose closer failed to land are truncated together with the closer, so consumers never observe a partial batch. Readers at minor 1.0 ignore the trailing field (ADP-009 skip-unknown rule); they still parse 1.1 segments correctly but lose the batch-atomicity invariant for entries older than their own minor. This is acceptable pre-alpha — there is no deployed 1.0 WAL.

The minor-version compatibility guarantee rests on two layout invariants:

1. New fields are appended at the end of the entry body, before `body_crc`.
2. `body_len` is the authoritative byte count of the body.

Together these allow:

**Old reader, new segment** (downgrade or rolling restart):
- Reader parses the fields it knows about.
- After the last known field, it computes how many body bytes remain and skips them.
- Body CRC is computed over all `body_len` bytes — matches.
- Unknown fields are ignored but not corrupted.

**New reader, old segment** (upgrade):
- Reader inspects `format_minor` and knows which fields are absent.
- Uses default values for missing fields.
- Body CRC verifies as normal.

**Compatibility matrix:**

| Writer vs. reader                 | Result |
|-----------------------------------|--------|
| Same major, same minor            | Full compatibility |
| Same major, reader newer minor    | Reader uses defaults for absent fields |
| Same major, reader older minor    | Reader skips unknown trailing body bytes and unknown entry types |
| Different major                   | Reader refuses to open segment |

**Changes requiring a major bump:**

- Removing or renaming an existing field
- Changing the type or meaning of an existing field
- Reordering fields
- Adding an entry type that older consumers cannot safely skip

**Changes compatible with a minor bump:**

- Adding a new field at the end of the entry body
- Adding a new entry type that older consumers can skip without loss of correctness
- Setting previously reserved `flags` bits

Each minor-version bump ships with a round-trip compatibility test asserting both directions of the matrix.

### Write semantics

Entries are serialized into an in-memory buffer and appended to the active segment's file descriptor. The group-commit mechanism (ADP-001) batches multiple appends into a single `fsync` boundary. An entry is durable once the `fsync` covering its bytes returns.

Segment rotation:

- Before each append, the writer checks whether adding the entry would push the segment past `segment_size_bytes` (default 64 MiB).
- If so, the active segment is closed (no more writes), and a new segment is created with `base_seq = last_seq + 1`.
- Segments never exceed their configured size.

### Positioned reads

Each segment keeps a sparse in-memory index of (sequence, file offset) points, one roughly every 64 KiB of entries, rebuilt by the scan at open and trimmed to the recovered tail. A read at a given sequence:
1. finds the nearest preceding index point;
2. walks forward reading only each frame's length and sequence fields;
3. fully decodes and checksums only the frames it returns.

Frames below the published write offset were verified at open or written by this process. During the skip, a frame whose sequence is out of order, or a frame that fails its checksum when decoded, is reported as corruption rather than silently ending the read. The index changes nothing on disk.

### Read semantics and crash recovery

Opening a segment:

1. Read the 32-byte header.
2. Verify `magic`.
3. Verify `header_crc` over bytes 0–27.
4. Verify `format_major` is supported.
5. If `format_minor` is higher than the reader's known minor, enter permissive mode (skip unknown fields and unknown entry types).

Reading entries:

```
loop:
  read u32 body_len                                  (EOF here = clean end)
  read body_len bytes                                (EOF here = torn tail)
  read u32 body_crc                                  (EOF here = torn tail)
  compute CRC32C over body; compare to body_crc      (mismatch = torn tail)
  decode body per format version
  yield entry
```

On any of the three "torn tail" conditions, the reader:

- Stops iteration at this point.
- Records the last valid entry's sequence ID as the segment's durable tail.
- Truncates the segment file to that position during recovery (removes the torn bytes so future appends start from a clean offset).

This gives the Abyss durability guarantee: any entry whose `Append` returned OK is read back successfully; any entry that was mid-write at crash time is discarded during recovery, and the client (which never received OK) is free to retry.

## Invariants

1. An entry is durable iff the `fsync` covering its bytes has returned successfully.
2. Entries are read in sequence order. There are no gaps or reorderings within a segment or across segments for a shard.
3. A CRC mismatch or short read at an entry stops reader progress at that entry. Bytes after are not considered durable.
4. Segment header `magic` and `header_crc` must verify before any entries are read.
5. A reader opening a segment with a different `format_major` than it understands refuses to proceed.
6. Within the same `format_major`, readers can process segments written by any `format_minor`.
7. Sequence IDs are monotonically increasing across entries within a segment and across segments for a given shard.
8. `base_seq` of segment N+1 equals `last_seq` of segment N plus 1 — there are no gaps at segment boundaries.
9. (format ≥1.1) After recovery, for every batch appended via `AppendBatch`, either every entry of the batch is present or none is. Recovery never exposes a proper subset of a batch's entries.
10. Every `Write` entry holds a command that parses. Unconditional writes are canonicalised before the append (see [ADP-006](006-read-write-paths.md) §Canonical form on the write path), so the log carries one spelling per operation with TTLs already absolute; conditional writes retain the client's spelling but are validated before the append. A parse failure on a `Write` entry is therefore a corruption signal, not a report about what the client sent — the only remaining way to get one is a command whose parser is absent from the reading build, which is a capability gap in that build rather than a property of the entry.

This is a constraint on producers, not a layout change: the byte-level encoding of a command is unchanged, and no version bump is implied. It does mean the set of byte sequences a conforming writer can emit is now a strict subset of what the format can express, and readers must not assume otherwise — a reader that encounters a non-canonical `Write` should still process it, since older development logs and the resolver's materialised commands both predate the rule.

## Trade-offs

**Why custom binary instead of protobuf or flatbuffers?**
Decode speed is critical; the hot consumer reads every entry. Protobuf's tag iteration, varint decoding, and heap allocation are measurable overhead we don't need. Flatbuffers offers zero-copy reads but requires a codegen build step and has a cache-unfriendly layout for our access pattern. The Abyss entry payload is simple (`RespCommand` is `vector<string>`), so the schema-management complexity those libraries solve doesn't exist for us. Custom binary fits the job.

**Why fixed u32 lengths instead of varint?**
Varints save ~3 bytes per small arg but require a branching decode loop. Fixed u32 is branchless, aligned, and simpler. Disk space is cheap relative to decode latency. If future profiling shows envelope overhead dominating on small entries, varints can be introduced in a future major version.

**Why major and minor version bytes instead of a single counter?**
A single counter forces every schema change through the same gate. Splitting the axis distinguishes bytes-compatible evolution (minor) from readers-must-upgrade (major). This mirrors Kafka's record-batch format and protobuf's proto3 rules, and matches the actual cost shape of changes: most additions are additive.

**Why body-only CRC instead of covering `body_len` too?**
If `body_len` is corrupted, the body CRC still catches it — the reader either computes CRC over the wrong bytes (mismatch) or reads past the segment end (short read). Covering `body_len` explicitly adds no detection power. Keeping the CRC scoped to the body bytes simplifies the encoder/decoder pair.

**Why per-entry CRC instead of per-segment?**
A per-segment CRC would invalidate the entire segment on any single-byte corruption. Per-entry CRC lets us recover up to the last good entry — essential for torn-tail handling, where only the last entry was mid-write at crash time and everything prior is intact.

**Why CRC32C instead of MD5, SHA, or xxHash?**
CRC32C has hardware acceleration on both target architectures, runs at cache-line throughput, and detects the corruption classes that matter for storage. MD5 and SHA are cryptographic, overkill, and ~10× slower. xxHash is faster than software CRC but is a hash, not a CRC — its error-detection properties differ, particularly on short inputs. CRC32C is the standard choice and matches RocksDB and LevelDB.

**Why append-only schema evolution (no middle insertions)?**
Allowing insertions at arbitrary positions would break the "skip unknown trailing bytes" rule — the reader couldn't know where a known field begins. Strict append-only is a simple rule with predictable compatibility. When a field truly belongs in the middle (rare), a major bump is the honest answer.
