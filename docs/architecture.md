# Architecture & System Design

This document details the internal design of the Distributed Key-Value Store.

## 1. Write Path & Persistence

When a write (PUT or DELETE) is received by a node, it goes through the following sequence:

```mermaid
sequenceDiagram
    autonumber
    actor Client
    participant API as HTTP API Handler
    participant WAL as Write-Ahead Log
    participant Store as Sharded Store
    participant Repl as Replicator
    actor Peer as Peer Nodes

    Client->>API: PUT /v1/kv/{key} (JSON value & optional TTL)
    API->>Store: Get Monotonic Version Number
    Store-->>API: New Version
    API->>WAL: Append mutation record (with CRC32)
    WAL->>WAL: Fsync to disk
    WAL-->>API: Success
    API->>Store: Set (key, val, ttl, version) under Shard Lock
    Store->>Store: Verify version (LWW) & update in-memory map
    Store-->>API: Success
    API-->>Client: HTTP 200 OK (returns Version)
    API->>Repl: Replicate (Key, Val, Version, Expiry, Action) (Async)
    activate Repl
    Repl-->>Peer: POST /internal/replicate
    deactivate Repl
```

---

## 2. Gossip Membership Protocol (SWIM-inspired)

Peer discovery and health tracking are managed via periodic SWIM-inspired gossip:

```mermaid
stateDiagram-v2
    [*] --> Alive : Node discovered (Seed / Gossip merge)
    
    Alive --> Suspect : Periodic ping to random peer times out / fails
    Alive --> Alive : Ping succeeds
    
    Suspect --> Alive : Direct ping succeeds OR refutation received
    Suspect --> Dead : Suspicion window expires without refutation
    
    Dead --> [*] : Removed from active peer list
```

### Gossip Loop Details:
1. **Periodic Tick:** Every node selects a random peer from its list and sends a `GET /internal/ping`.
2. **State Merge:** The recipient responds with its full peer directory. The caller merges this directory using the state precedence `Dead > Suspect > Alive`.
3. **Bootstrapping:** New nodes join by sending a ping to the configured seed node, immediately downloading the entire active peer list.

---

## 3. Asynchronous Replication (LWW)

Updates are replicated asynchronously across the cluster. If different updates for the same key arrive out of order, the conflict is resolved using Last-Write-Wins (LWW):

```mermaid
flowchart TD
    A[Replicate Request received] --> B{Does key exist locally?}
    B -- No --> C[Apply mutation and write to local WAL]
    B -- Yes --> D{Is incoming version >= local version?}
    D -- Yes --> E[Overwrite in-memory & append local WAL]
    D -- No --> F[Discard update (Older write - Out of order)]
    C --> G[HTTP 200 OK]
    E --> G
    F --> G
```
