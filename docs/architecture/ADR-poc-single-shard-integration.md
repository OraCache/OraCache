# ADR: single-region, single-shard integration PoC

Status: accepted for the `integration/poc` branch (local PoC, not a release design).

## Scope

Exactly one region, one shard (partition 0), three storage-node **processes**,
one control plane (CP) process and one gateway process. Raft membership is
static and in memory only: there is no disk persistence and a stopped node is
not restarted into the group (a Raft member that loses term/vote must not
rejoin under the same ID). Out of scope: multi-region replication, dynamic
membership, TTL, scheduling/rebalancing and production hardening (TLS, auth,
quotas).

## Components and boundaries

```
CLI --gRPC--> gateway --gRPC (forward, bounded retry)--> storage node (any of 3)
                 |                                           |
                 +--- polls GetConfig --> CP <-- heartbeat --+
storage node = [ gRPC adapter -> shardraft.Instance -> versioned LRU store ]
                 ^ Raft messages over HTTP between the three processes
```

* **Storage node** (`cmd/storage-node`, package `internal/node`): a module
  inside each storage process that wraps the store and the shard Raft member.
  It owns config/constructors, the gRPC server delegating to
  `shardraft.GatewayServer`, the HTTP Raft transport, the CP-agent heartbeat
  (reporting the real `shardraft.Status`) and lifecycle. There is **no new
  network service between Raft and the store**; the store is called in-process
  from Raft apply.
* **Raft** (`shardraft`, `go.etcd.io/raft/v3`): reused unchanged in protocol and
  algorithm. The default read mode stays `Linearizable` (leader ReadIndex).
* **Store** (`internal/store`): the versioned LRU store is the final backend.
  A thin adapter (`internal/node`, new files) implements `shardraft.StateMachine`
  on top of it; the legacy `internal/cache` stays for the existing gateway.
* **CP** (`cmd/cp-api`, `internal/cp*`, `internal/statestore`): reused from the
  earlier CP implementation (commit c8e28c7, owner-confirmed as valid).
  Static bootstrap config only. Heartbeats are **soft state** and do not mutate
  assignments; CP is not a scheduler.
* **Gateway** (`cmd/cache-gateway`): the existing `internal/gateway` router only
  resolves addresses, so a forwarding layer is added. It forwards Get/Put/
  Delete (and batches) to the CP-assigned leader and fails over using the
  **actual Raft `NOT_LEADER` hint** and, if that is missing, the other replicas
  from the assignment, within a fixed attempt count and one overall deadline.
  The last node that answered successfully is remembered in process memory.

## Key decisions

1. **Deterministic eviction.** `Store.Get` promotes the key in the LRU, but Raft
   serves reads locally on one node while all writes replicate: this would make
   LRU order, and therefore eviction, diverge between replicas. State-machine
   reads use a non-touching `Peek`. LRU order is therefore driven by writes only
   (Put and Delete) and eviction happens only inside Put apply. This is an
   explicit PoC semantic: reading a key does not keep it alive.
2. **Checkpoint/Restore** is added to the store (separate commit): it captures
   versions, the global revision sequence, LRU order and the configuration
   consistently, in memory only; Restore validates before replacing state.
3. **Revisions.** The protobuf contract says the cache assigns `rev` and
   `expected_rev` compares with the latest revision (0 = key absent). The store
   uses one global, monotonic sequence instead of the legacy per-key counter.
   This satisfies the contract (revisions of a key strictly increase and are
   never reused, including after delete and restore) but `rev` values are not
   dense per key.
4. **Retry safety.** NOT_LEADER is a guaranteed pre-proposal refusal and is
   always retried. Transport failures may be ambiguous for writes; an
   unconditional Put retried after such a failure may create an extra version.
   Callers needing exactly-once semantics use `expected_rev`.
5. **Process-level failover.** Killing the leader process leaves a 2-of-3
   quorum, which elects a new leader; the same gateway endpoint resumes
   serving. Data written before the kill remains readable (it was replicated).

## Consequences

Dependency and toolchain versions stay as on `main` (Go 1.27.3). The Go
workspace modules (root, `gen`, `cmd/oracache`, `integration/e2e`) stay separate
and are each tested. Local demo ports are in the 28080+ range.
