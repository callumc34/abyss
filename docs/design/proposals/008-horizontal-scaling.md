# ADP-008: Horizontal Scaling

**Status:** Accepted
**Created:** 2026-04-09

## Context

Phase 1 runs on a single pod. Phase 2 scales horizontally by partitioning the keyspace across multiple pods. Each key hashes (xxHash) to exactly one pod. Pods share nothing — each has its own queue, hot store, cold store, and compaction buffer. No cross-pod communication is needed for reads or writes.

This proposal covers the routing protocol, shard model, queue partitioning, and resharding mechanics.

## Design

### Shard Model

The keyspace is divided into a fixed number of shards. Each shard maps to exactly one pod. Pods may own multiple shards.

- **Shard count:** Fixed at deployment. A rebalance changes the shard-to-pod assignment but not the total shard count.
- **Hash function:** xxHash applied to the key (or hash tag portion of the key).
- **Over-provisioning:** Choosing a shard count larger than the initial pod count (e.g., 64 shards across 4 pods) allows scaling up without changing the hash function — just reassign shard ownership.

```yaml
cluster:
  enabled: true
  shard_count: 64
  hash_function: xxhash
  this_pod_shards: [0, 1, 2, 3]      # Assigned via Helm values per pod ordinal
```

### Routing: Redis Cluster Protocol

Abyss implements the Redis Cluster protocol for topology discovery. This is the industry-standard approach and avoids a proxy bottleneck.

**How it works:**
1. Each pod knows the full shard map and responds to `CLUSTER SLOTS` / `CLUSTER SHARDS` queries.
2. Clients that support Redis Cluster (Jedis, Lettuce, redis-py, ioredis, etc.) connect to any pod, discover the topology, and route commands directly to the owning pod.
3. If a command arrives at the wrong pod, Abyss responds with a `MOVED` redirect.

**Multi-key commands in multi-pod mode:** If keys in a multi-key command span pods, Abyss responds with `CROSSSLOT` error, matching Redis Cluster semantics. Clients handle this by fanning out per-slot and assembling results client-side. Users can use hash tags (e.g., `{user123}.name`, `{user123}.email`) to co-locate related keys on the same shard.

### Queue Partitioning

Each shard corresponds to a queue partition. The `Queue` interface is shard-aware from the start — every method takes a `ShardId` parameter.

**External broker mapping:**
- **Kafka:** One partition per shard. Key-based partitioning ensures ordering within a key.
- **NATS JetStream:** One subject per shard (`abyss.shard.{N}`). Durable consumers track ack position.

Since we always partition by key hash, ordering within a key is guaranteed by every broker.

### Resharding

Adding pods requires migrating shard ownership:

1. The new pod starts consuming its assigned partitions from the beginning of the topic.
2. It filters for its shards and rebuilds state via queue replay (same mechanism as recovery).
3. Existing pods stop processing the migrated shards.
4. Cold store data for migrated shards remains on the old pod's PVC — the new pod rebuilds cold from queue replay, and the old pod's data is eventually garbage collected.

### Constraints

**Horizontal scaling requires the external queue profile.** The embedded WAL is per-pod and cannot be consumed by other pods. The embedded profile is single-pod only.

**Phase 1 alignment:** The number of lock shards in Phase 1's hot store should equal the planned horizontal shard count. Phase 1's internal sharding boundaries then match Phase 2's pod boundaries, easing migration.

### Configuration

```yaml
# External profile, multi-pod
profile: external

cluster:
  enabled: true
  shard_count: 64
  hash_function: xxhash
  this_pod_shards: [0, 1, 2, 3]

hot:
  backend: redis_client
  endpoint: "dragonfly.svc:6379"
  pool_size: 32
  default_eviction_seconds: 86400

cold:
  backend: redis_client
  endpoint: "kvrocks.svc:6666"
  pool_size: 16

queue:
  backend: kafka
  brokers: "kafka.svc:9092"
  topic: "abyss-log"
  partitions_per_shard: 1
  retention_ms: 86400000
```

## Invariants

1. Shard count is fixed at deployment. It never changes without a full redeployment.
2. Each key hashes to exactly one shard. Shard assignment is deterministic and consistent across all pods.
3. Pods share nothing. No cross-pod communication for reads or writes.
4. `MOVED` redirects are correct — the target pod always owns the shard for the requested key.
5. Resharding rebuilds state from the queue. No data is copied between pods directly.

## Trade-offs

**Why Redis Cluster protocol instead of a proxy?** A proxy adds a network hop to every request, becoming a latency and throughput bottleneck. The Redis Cluster protocol pushes routing to the client, which connects directly to the owning pod. Most production Redis client libraries already support cluster mode.

**Why fixed shard count?** Dynamic shard counts require consistent hashing with virtual nodes, which adds complexity to the routing layer and makes resharding harder to reason about. A fixed shard count with over-provisioning (more shards than pods) achieves the same elasticity in practice — you scale by reassigning shards to new pods, not by creating new shards.

**Why full queue replay for resharding instead of cold store snapshots?** Queue replay reuses the existing recovery mechanism with zero additional code. It's also correct by construction — the new pod builds its state from the single source of truth. The downside is replay time for large queues. If this becomes a problem, a "snapshot cold store + replay recent" optimisation can be evaluated (see Open Questions in [Requirements](../requirements.md)).

**Why require external queue for horizontal scaling?** The embedded WAL writes to a local PVC. Other pods cannot read a local PVC's files. An external queue (Kafka, NATS) provides shared access to the log, which is necessary for resharding (new pods must read from the beginning of the relevant partitions). Requiring external queue for multi-pod is a hard architectural constraint, not a temporary limitation.
