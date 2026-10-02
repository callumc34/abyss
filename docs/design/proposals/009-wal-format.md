# ADP-009: WAL Log and Frame Format

**Status:** Accepted
**Created:** 2026-04-15
**Updated:** 2026-10-02

> **Format 2.** This revision replaces format 1's per-shard segment files with the physical log of [ADP-015](015-write-path-and-durability.md) §Log durability pipeline: one log per data volume carrying every shard's stream, in fixed-size, recycled, memory-mapped segments. A data directory in the format 1 layout refuses to start; there is no migration, because no format 1 WAL exists outside development. Entries will carry decided effects in Phase 2 of ADP-015.

## Context

ADP-001 defines the Queue interface and the embedded WAL. This document specifies the on-disk format: the directory layout, segment headers, how frames are encoded and committed, how readers tell the end of the log from corruption, and how the format evolves.

The format must:

- **Commit lock-free.** Many appenders on different shards share one log. Each fills its own reserved range, and a single store makes a frame visible.
- **Make the end of the log unambiguous** after a crash or power loss, including in a recycled segment that still holds the frames of its previous life.
- **Decode quickly** on the hot consumer's critical path, and let a reader skip other shards' frames by reading headers only.
- **Support additive schema changes** without a major version bump or a data migration.
- **Add no heavy build dependencies**, and stay inspectable with `xxd` or `hexdump`.

These goals rule out protobuf and flatbuffers (codegen at build time, heap allocation, and layouts that don't suit this access pattern). The format is a custom binary layout with per-frame CRC32C and an explicit version dimension.

This ADP applies only to the embedded WAL. External broker profiles (ADP-001 "External Broker Mapping") use the broker's native record format.

## Design

### Directory layout

```
/data/wal/
├── log-0000/
│   ├── 00000000000000000041.seg      sealed
│   ├── 00000000000000000042.seg      active
│   ├── 00000000000000000043.seg      spare, ready
│   ├── 00000000000000000044.seg      spare, ready
│   └── free-0.seg                    reclaimed, awaiting reuse
├── log-0001/                         only when queue.log_count > 1
└── offsets/
    └── offsets.ckpt
```

- **Logs.** There is one log per `queue.log_count`; the default is 1, one per data volume. Shard `s` belongs to log `s mod log_count`.
- **Segment names.** Segments are named by ordinal, zero-padded to 20 digits, so lexicographic order is log order.
- **Lifecycle.** A segment is first prepared as `{ordinal}.seg.tmp` and renamed into place. After reclamation it waits as `free-{n}.seg` until it is recycled.
- **Fixed size.** Every segment is `queue.segment_size_bytes`. Its blocks were written either in a previous life or by the zero-fill that created it, so the WAL's flush is data-only.

A *log position* is `ordinal × frame_space + offset`, with `frame_space = segment_size_bytes − 4096`. Positions only grow; position `p` lives in segment `p / frame_space`, at file offset `4096 + p mod frame_space`.

### Segment header

One 4 KiB block, so frames start page-aligned. Little-endian throughout.

| Field | Description |
|---|---|
| `magic` | Identifies an Abyss WAL segment |
| `format_major`, `format_minor` | 2 and 0 |
| `log_id` | The log this segment belongs to |
| `ordinal` | The segment's place in the log. Its low 32 bits are the segment's **generation** |
| `created_at` | Wall-clock time the segment was prepared |
| `shard_count` | The shard count the log was created with. Open refuses a log made for another count |
| `durability_window_bytes` | The window in force when the segment was written. It sets the recovery CRC scope (§Recovery) |
| `salt` | 64 random bits, fresh for each life of the segment, sealed into every frame's CRC |
| `header_crc` | CRC32C over the fields above |

The header is written when a segment is prepared, synced, and never modified while the segment is in use.

### Frame

Every frame starts 8-byte aligned.

| Field | Size | Description |
|---|---|---|
| `len` | u32 | Body length. `len` and `gen` form one 64-bit **commit word**, stored last |
| `gen` | u32 | The segment's generation. A frame is **filled** iff `gen` matches its segment and `len` is non-zero |
| `crc32c` | u32 | Over the body (an entry's whole body, or a padding frame's fixed header), then the segment's salt, then the commit word |
| `kind` | u8 | `1` entry, `2` padding. Any other kind under a valid CRC is corruption (a newer format) |
| `type` | u8 | The entry type (below) |
| `shard` | u16 | The shard whose stream the entry belongs to |
| reserved | u32 | Zero |
| `seq` | u64 | The entry's sequence id within its shard |
| `batch_rest` | u64 | Bytes from this frame's start to the end of its batch. A single entry's equals its own frame size |
| `appended_at_us` | i64 | Wall-clock microseconds |
| payload | | The entry payload (below) |
| pad | | Zeros up to the next 8-byte boundary |

The fixed overhead is 44 bytes plus padding, against format 1's 37. It buys lock-free commit, an unambiguous end of log, and header-only skipping.

**Payload by entry type:**

| `type` | Meaning | Payload |
|---|---|---|
| `0` | `Write`: an unconditional RESP command | `arg_count` u32, then each arg as `len` u32 plus bytes |
| `1` | `Conditional`: a command with a predicate ([ADP-011](011-conditional-writes-and-consumer-rpc.md)) | flags u16, then the command as for `Write` |
| `2` | `Resolved`: the decision for a prior `Conditional` | ref seq u64, decision u8, op count u32, each materialised op as a command, then the return value (RESP2, length-prefixed) |
| `3` | `Flush`: a FLUSHDB / FLUSHALL tombstone ([ADP-006](006-read-write-paths.md) §Broadcast write path) | None |
| `4–255` | Reserved | |

### Writing a frame

1. **Encode.** The appender encodes its frames into a private buffer. For a batch, it then closes the batch: it writes each frame's `batch_rest` and records each body's CRC.
2. **Reserve.** It reserves the whole batch as one contiguous range of the log.
   - A reservation that doesn't fit the active segment moves the log to the next prepared segment.
   - The reservation that triggered the move pads the old segment's remainder with one padding frame.
   - A batch therefore never straddles a segment, and no padding frame falls inside a batch.
3. **Fill.** It copies each frame into the mapping, minus its commit word. It extends the frame's CRC with the commit word for the reserved segment's generation, and stores the commit word last, as one 64-bit release store.
4. **Filled prefix.** Each fill, and each roll's padding, records its completed range in memory. The log's *filled prefix* `P` advances through contiguous completed ranges, so every byte below `P` is a filled frame, and `P` is the `process_crash` watermark (ADP-015).
   - `P` is never derived from bytes read out of the mapping. In a recycled segment, a new frame start that is not yet filled holds the previous life's bytes. Those can be any payload, not only old frames, and generations are small, predictable numbers, so 8 stale bytes can look like a filled commit word.
5. **Flush.** The flusher syncs the segments covering `[D, P)`, then publishes `D = P`. `D` is the `power_loss` watermark.

A frame's generation and salt are known only once it is reserved. Putting them last in the CRC lets the fill extend a precomputed body CRC in constant time.

**Which ordinal a segment file may take.** A reclaimed or renumbered file is only ever given an ordinal above the one in its header. Every stale frame in it then carries a smaller generation. A file whose header cannot be read is deleted rather than reused.

### Integrity

The format uses CRC32C (Castagnoli). It is hardware-accelerated on x86-64 and ARMv8, and detects the accidental corruption that matters for storage.

**Why the commit word is inside the CRC.**
- A 512e device persists only whole 512-byte sectors atomically, and a frame's commit word can sit in a different sector from its body.
- In a recycled segment, frames of uniform size land at exactly the old offsets with the old lengths. A power cut can then persist the new commit word over the old body and old CRC.
- If the CRC did not cover the commit word, that stale frame would validate. Its old sequence id would read as corruption, and the server would refuse to start after an ordinary power loss.
- Because the CRC does cover it, the mismatch is a clean torn tail. RocksDB's recyclable log format covers its log number for the same reason.

**Why a per-life salt is in the CRC.** Generations are predictable. A client could store a value whose bytes form a frame with a future generation and a valid CRC, placed where a later frame will start in a recycled segment. If a crash left that position as the first unfilled one, recovery would accept the forged frame. A random salt the client never sees makes such a frame fail its CRC.

**A padding frame's CRC covers only its fixed header**, never the stale bytes it spans.

### Recovery

Recovery scans each log once, from its oldest segment, and demultiplexes the frames into their shards' streams.

1. **Segments.**
   - Leftover `.tmp` files are deleted, and `free-*` files rejoin the free pool.
   - Every header must verify.
   - Ordinals must be contiguous among segments that hold frames.
   - A prepared spare past the tail, with a valid header and no frames, stays a spare. A spare with an invalid or torn header returns to the pool.
2. **The end of the log** is the first frame that is not filled, or that fails its CRC. An entry frame's shard must be in range, and its seq must be the next one for its shard; anything else is corruption.
3. **CRC scope.** Every segment that can hold unflushed bytes is fully verified. That is every segment overlapping the last `max(durability_window_bytes, the largest window recorded in any header)` bytes before the recovered end, plus one more. Below that range the scan reads only commit words and headers, because those bytes were synced long before. Recording the window in each header means lowering it across a restart cannot shrink the verified range.
4. **Batch closure, by position.** Only the trailing batch can be incomplete: the first frame whose `position + batch_rest` passes the recovered end starts it. That frame and everything after it are dropped, and the recovered end moves back to it. After recovery, every batch is present in full or not at all.
5. **The recovered tail is sealed.**
   - First, every segment past the recovered end is moved out of the log, to be renumbered, and the directory is synced. A segment that was active before the crash can hold filled frames of its own generation past a hole.
   - Then one padding frame is written from the recovered end to the end of its segment, and synced.
   - Appends continue in a fresh segment, so stale bytes past the old end can never be scanned as part of the log.
   - A crash between those steps leaves either the hole or a clean gap to stop the next scan.

Below the CRC scope, a CRC failure that a reader finds later is media corruption. It is fatal, naming the log, ordinal and offset; it is never treated as a poison entry.

A shard's next sequence id is the larger of two values: the next one after its retained frames, and every retention consumer's persisted offset plus one (ADP-015 §Retention). A shard whose frames were all reclaimed therefore keeps its place.

### Positioned reads

Each shard stream keeps two in-memory structures, both rebuilt by the recovery scan:
- a ring of `{seq, position}` for its recent frames;
- a sparse index with one point every 64 KiB of the shard's frame bytes.

A read at a sequence id finds it in the ring, or else starts at the nearest index point and skips forward, reading only commit words and headers. The skip passes over other shards' frames and padding. The read fully decodes and checksums only the frames it returns. Nothing about this is on disk.

### Schema evolution

- **`format_major`:** breaking changes. Readers refuse a segment with another major version.
- **`format_minor`:** additive changes. Readers of the same major read any minor.

The minor-version guarantee rests on three layout rules:
1. New payload fields are appended at the end of an entry's payload.
2. `len` is the authoritative body length, so a reader skips trailing payload bytes it does not know.
3. A new frame **kind** is a major change unless older readers can skip it without loss of correctness. An unknown kind under a valid CRC fails closed, which keeps that decision explicit.

| Writer vs. reader | Result |
|---|---|
| Same major, same minor | Full compatibility |
| Same major, reader newer | The reader uses defaults for absent fields |
| Same major, reader older | The reader skips unknown trailing payload bytes |
| Different major | The reader refuses to open the log |

**Changes requiring a major bump:**
- removing, renaming, retyping or reordering a field;
- adding an entry type or frame kind that older readers cannot safely skip.

`Flush` and the `Conditional`/`Resolved` pairing are skip-unsafe.

Each minor bump ships with a round-trip compatibility test in both directions.

## Invariants

1. A frame is visible to any reader only after its commit word is stored. Its commit word is stored only after every other byte of it is in place.
2. Every byte below the filled prefix `P` belongs to a filled frame. An append acknowledged at `process_crash` lies below `P`; one acknowledged at `power_loss` lies below `D`.
3. A frame left over from a segment's previous life never reads as filled: its generation is smaller. The filled prefix never depends on bytes read from the mapping.
4. The end of the log is the first frame that is unfilled or fails its CRC. No frame past it is ever replayed.
5. Within a shard, sequence ids are contiguous and their positions increase. Per-shard seq order is log order.
6. After recovery, every batch is present in full or not at all.
7. A reader refuses a log with an unsupported `format_major`, an unknown frame kind, or a header that does not verify.
8. Every `Write` entry holds a command that parses. Unconditional writes are canonicalised before the append ([ADP-006](006-read-write-paths.md) §Canonical form on the write path). A parse failure on a `Write` is therefore a corruption signal, or a command whose parser is absent from the reading build.

## Trade-offs

**Why one log per volume instead of per-shard files?**
One sequential write stream and one flush per volume, however many shards there are. Per-shard files cost one flush per shard per batch and scatter writes across the device. Kafka, RocksDB and FoundationDB's tlog all share one log among many logical streams for the same reason.

**Why a commit word instead of a length prefix?**
With concurrent appenders, a length prefix written first cannot say whether the bytes after it are complete. One 64-bit store of `{len, gen}`, made last, makes a frame's visibility atomic and its staleness checkable in a single load.

**Why recycle segments with a generation instead of zero-filling each one?**
Zero-filling every segment writes each byte twice. On a 125 MiB/s volume that halves WAL bandwidth. Recycling writes each byte once in steady state; the generation makes stale frames inert. New segments are zero-filled only when the pool has to grow (ADP-015).

**Why a relative `batch_rest` instead of a batch's last sequence id?**
Batch closure by position is independent of shards, so ADP-015 Phase 2's cross-shard batch (consecutive frames of several shards in one reservation) needs no format change. A relative length is also known at encode time, before the frame is reserved.

**Why fixed u32 lengths instead of varints?**
They are branchless and simple. Disk is cheap relative to decode latency.

**Why CRC32C?**
Hardware-accelerated on both target architectures, with error detection suited to storage. It matches RocksDB and LevelDB.
