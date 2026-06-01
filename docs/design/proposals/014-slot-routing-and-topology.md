# ADP-014: Slot-Based Routing and Persisted Topology

**Status:** Accepted
**Created:** 2026-06-01
**Supersedes:** ADP-008 §Hash Function (xxHash placement)
**Refines:** ADP-005 §CLUSTER Commands / Invariant 8, ADP-010 §Shard Slot / §Format Version

## Context

Two foundational defects could not be fixed without changing previously Accepted decisions.

The first is a routing divergence. The wire-visible slot and the data-placement shard were computed by two different hash functions. `CLUSTER KEYSLOT`/`SLOTS`/`SHARDS` reported `CRC16` of the hash-tag content modulo 16384, the Redis Cluster standard, while every placement decision — hot striping, the cold key's 2-byte shard prefix, the WAL shard directories, the resolver stripes, and consumer routing — used a separate xxHash of the raw key modulo the shard count. The two hashes are independent. A cluster-aware client routes by the CRC16 slot, but the data lives where xxHash placed it. In single-pod this is latent because `MOVED` never fires; in multi-pod a `MOVED` derived from CRC16 slot ownership would redirect a client to a pod that does not own the key's xxHash shard, making ADP-008's redirect-correctness invariant unsatisfiable.

A slot-to-shard lookup table cannot reconcile the two, because there is no function of the slot alone that equals the xxHash placement: many distinct keys collide into one of the 16384 slots, and those colliding keys scatter across the xxHash shards. The only coherent fix is to make placement a function of the slot — the actual Redis Cluster model.

The second defect is that the shard count and the routing/encoding identity the data was written under were never persisted or validated. The shard count flowed from a single configuration knob into the WAL, the cold store, and every consumer, but nothing on disk recorded it. Changing the shard count between restarts would silently re-route the WAL shard directories and re-encode every cold key's shard prefix, orphaning all prior data while the server reported healthy.

## Decision

### Single routing hash

The keyspace is partitioned into 16384 slots via the Redis Cluster standard: the slot is the CRC16 of the hash-tag content modulo 16384. This is the only routing hash. xxHash is removed from the placement path entirely.

### Shard is a slot range

A shard is the unit of placement and ownership, derived from the slot by a documented, deterministic, total function that partitions the 16384 slots into contiguous ranges — one per shard. Contiguous range division (the slot scaled into the shard-count space) gives each shard a single contiguous slot range, matching ADP-008's over-provisioning and rebalance model: a reshard moves shard-to-pod ownership, not the slot count and not the on-disk encoding.

Hot striping, the WAL shard directories, the cold key prefix, the resolver stripes, and consumer routing all derive the shard from the slot through this one function. The single placement entry point becomes the composition of slot-from-key and shard-from-slot. Every existing caller already routes through that entry point, so the change is transparent to them.

### Cold key encoding and format epoch

The 2-byte shard prefix in cold keys stores the slot-derived shard. Because the key-to-slot mapping is immutable and shard ranges are derived from slots, the on-disk encoding is stable across a rebalance. Because the placement scheme changed, the cold-store format version is bumped from 2 to 3. The cold store retains its own per-open format-version check as defence in depth; the persisted topology manifest below is the primary gate.

### Hash-tag co-location is honest end-to-end

Keys sharing a hash tag produce the same slot and therefore the same shard. Multi-key and cross-slot semantics are now honest to cluster-aware clients: co-located keys land on the same data shard, not merely on the same wire slot. No separate hash-tag-extraction toggle is needed — co-location is inherent in routing through the slot.

### CLUSTER topology

`CLUSTER SLOTS` and `CLUSTER SHARDS` advertise slot ranges grouped by their owning shard, replacing the previous single full-range stub. In single-pod every shard maps to this node, so the union of the advertised ranges is the full slot space, partitioned disjointly by shard. `MOVED` is computed from slot ownership and therefore always names the pod whose shard holds the key — true by construction. `MOVED` and cross-slot emission remain gated behind the multi-pod cluster flag, so neither fires in single-pod, but the routing map and topology validation always run. `CLUSTER KEYSLOT` returns the wire slot unchanged.

### Persisted topology manifest

A topology manifest file is written atomically — using the durable tmp-write, fsync, rename, and parent-directory-fsync discipline — on the first start of a fresh data directory, next to the node identity file. It records the manifest version, the shard count, the cold-format epoch, and stable identifiers for the wire-slot scheme and the data-shard scheme. On every subsequent start the composition root reads the manifest and validates it against the effective configuration before the WAL, the cold store, the hot store, or any consumer opens. Any mismatch — a different shard count, a different cold-format epoch, or a different scheme identifier — is a refuse-to-start corruption error naming the field, the persisted value, and the configured value. A manifest whose version is newer than this binary understands also refuses to start.

This generalises the cold store's per-open format-version check from the cold encoding to the full routing topology and centralises it in one place, so all data subsystems are gated by one decision before any of them touch potentially mis-routed data.

## Invariants

1. The wire slot is `CRC16` of the hash-tag content modulo 16384, identical in single-pod and multi-pod. Placement derives from the slot, never from a second hash.
2. Shard assignment is a pure, total, deterministic function of the key and shard count, identical across the frontend, hot store, cold store, consumers, and recovery.
3. `MOVED` targets the pod whose shard owns the key's slot, by construction.
4. The shard count and encoding identity the data was written under are persisted and validated on every start. A mismatch refuses to start rather than silently re-routing or re-encoding.
5. Recovery (pure queue replay) runs only against a WAL whose shard layout matches the persisted shard count.

ADP-008's invariants on consistent placement, deterministic shard assignment, and redirect correctness are unchanged in intent but become enforceable for the first time: the first two via the manifest, the last via slot-derived placement.

## Trade-offs

**Why drop xxHash rather than keep both hashes?** Keeping both and reconciling them with a lookup table was considered and rejected as mathematically unsound: no function of the slot alone reproduces the xxHash placement, so redirect correctness and hash-tag co-location could never hold for non-co-tagged keys. Slot-unified routing is the standard Redis Cluster model and makes both properties structural.

**Why bump the cold-format epoch now?** Re-encoding the shard prefix is a one-way door. The system is greenfield, so the epoch bump costs nothing now (dev and test stores are wiped) and is gated by the manifest so it can never be applied silently. Deferring it would make the change a breaking, migration-requiring operation later. This is recorded as a deliberate one-way greenfield decision.

**Why scheme identifiers rather than implicit versioning?** Persisting stable identifier strings for the wire-slot and data-shard schemes lets a future change to either scheme be detected as a mismatch instead of silently re-routing on-disk data. The identifiers are versioned constants; a scheme change bumps its identifier rather than reusing it.

**Single-pod shard-count cap.** In-memory consumer-RPC identity packing caps the single-pod shard count at 2^15. The configuration validator enforces this bound; the manifest records the actual count. The cold on-disk shard envelope is wider (2^16) than the in-memory packing limit, which is intentional and documented — on-disk width need not equal in-memory packing width until the RPC identity becomes a structured key (deferred).

**Manifest scope.** The manifest lives under the WAL data directory and so binds topology identity to that directory. It cannot, on its own, detect a cold volume from a different deployment pointed at a mismatched path; the cold-format epoch recorded in both the manifest and the cold store's own format record cross-check the encoding epoch, and a future enhancement may stamp the shard count into a cold system record so each volume self-describes.
