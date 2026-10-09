# distributed-KV-system
# Distributed Key-Value Store (`kvraft`) - Accomplishments & Architecture

A production-grade, distributed key-value store implemented in Go using the Raft consensus algorithm. The architecture features atomic disk persistence, Write-Ahead Logging (WAL), snapshot compaction, linearizable state machine replication, and client request deduplication.

---

## Architecture Overview

```text
+-------------------------------------------------------+
|                    Client / TCP Layer                 |
|             (SET, GET, DELETE Command Parser)         |
+-------------------------------------------------------+
                           |
                           v
+-------------------------------------------------------+
|            Replicated State Machine (RaftKV)          |
|       - Application Loop & De-duplication Cache       |
+-------------------------------------------------------+
                           |
                           v
+-------------------------------------------------------+
|                     Raft Engine                       |
|   - Leader Election, Log Replication, RPC Processing  |
+-------------------------------------------------------+
      |                                           |
      v                                           v
+-----------------------+               +-----------------------+
|   Persister Engine    |               |  Storage / Memory Store |
| (Raft State + Snap)   |               |   (Thread-Safe Map)   |
+-----------------------+               +-----------------------+

```

---

## Accomplishments by Phase

### Phase 0: Project Foundation & Infrastructure

* **Repository Layout**: Clean idiomatic Go layout (`cmd/`, `internal/`) under Go module `[github.com/iam-iftekhar/kvraft](https://github.com/iam-iftekhar/kvraft)`.
* **Structured Observability**: Integrated standard `log/slog` with contextual node-ID tags and configurable log levels.
* **Lifecycle Management**: Configured flag parsing (`--node-id`, `--port`, `--peers`, `--data-dir`) and signal traps (`SIGINT`, `SIGTERM`) for context-aware graceful teardowns.

### Phase 1: Single-Node In-Memory Storage Engine

* **Concurrent Memory Store**: Thread-safe key-value store (`MemoryStore`) utilizing `sync.RWMutex` for concurrent read/write isolation.
* **Basic Operations**: Native support for `SET`, `GET`, and `DELETE` operations with race detector validation.

### Phase 2: Line-Oriented TCP Network Protocol

* **Custom Protocol Parser**: Built client-facing server parsing line-delimited commands:
* `SET <key> <value>`
* `GET <key>`
* `DELETE <key>`


* **Connection Lifecycle**: Non-blocking connection listener using `net.Listener`, concurrency management via goroutines, and clean shutdown draining active client connections using `sync.WaitGroup`.

### Phase 3: Single-Node Write-Ahead Log (WAL) Durability

* **Framed Binary Format**: Durable append-only storage engine structured as:

$$\text{[4B Length]} + \text{[4B CRC32 Checksum]} + \text{[JSON Payload]}$$


* **Durability Guarantees**: Configured explicit `fsync` flushes on write operations to prevent data loss on sudden machine failure.
* **Crash Recovery**: Sequential WAL log replay during startup with automatic tail truncation on partial/corrupted trailing writes.

### Phase 4: Raft Consensus Engine (In-Memory Core)

* **Role State Machine**: Core Raft algorithm supporting roles (`Follower`, `Candidate`, `Leader`), randomized election timeouts (150ms–300ms), and periodic leader heartbeats (50ms).
* **Consensus RPCs**:
* `RequestVote`: Candidate log up-to-date checks according to Raft Section 5.4.1.
* `AppendEntries`: Log consistency checks, conflict resolution, and majority quorum commit index advancement ($N > \text{commitIndex}$).


* **Transport Abstraction**: Decoupled `Transport` interface accompanied by `MemoryNetwork` for deterministic, race-free cluster simulation testing.

### Phase 5: Atomic Persistence & Recovery

* **Atomic State Writes**: Persists Raft persistent state (`currentTerm`, `votedFor`, `log[]`) to disk using atomic operations (`os.CreateTemp` $\rightarrow$ `fsync` $\rightarrow$ `os.Rename`) to prevent zero-byte corruptions.
* **Node Boot Recovery**: Implemented state reconstruction from persistent storage during node startup before election participation.

### Phase 6: Replicated Key-Value State Machine

* **Consensus Pipeline Integration**: Wired client key-value operations to Raft's `Start()` loop rather than mutating local memory directly.
* **Async Applier Loop**: Asynchronous channel processing loop reading `ApplyMsg` from Raft's `applyCh` and applying committed entries to `MemoryStore`.
* **Linearizability & Deduplication**: Tracking map (`lastAppliedSeq`) tracking `ClientID` and monotonically increasing `SeqNum` to guarantee exactly-once execution semantics across network retries.

### Phase 7: Log Compaction & Snapshotting

* **Raft Log Truncation**: Integrated `Snapshot(index, data)` method to truncate log entries up to `lastIncludedIndex` and set dummy entry at index 0.
* **Dual Persistence Engine**: Extended `Persister` to save Raft metadata and state machine snapshots side-by-side atomically.
* **InstallSnapshot RPC**: Implemented `InstallSnapshot` handling so leaders can transmit complete state snapshots to lagging followers whose logs have already been compacted.

---

## 🧪 Verification & Test Suite

All phases are verified using Go's data race detector (`go test -race -v ./...`).

```text
=== RUN   TestDefaultConfig
--- PASS: TestDefaultConfig (0.00s)
=== RUN   TestMemoryStore_BasicOperations
--- PASS: TestMemoryStore_BasicOperations (0.00s)
=== RUN   TestMemoryStore_Concurrency
--- PASS: TestMemoryStore_Concurrency (0.00s)
=== RUN   TestSetupLogger
--- PASS: TestSetupLogger (0.00s)
=== RUN   TestRaft_InitialElection
--- PASS: TestRaft_InitialElection (0.36s)
=== RUN   TestRaft_Replication
--- PASS: TestRaft_Replication (0.51s)
=== RUN   TestRaft_PersistenceRecovery
--- PASS: TestRaft_PersistenceRecovery (0.72s)
=== RUN   TestRaft_SnapshotCompaction
--- PASS: TestRaft_SnapshotCompaction (0.31s)
=== RUN   TestRaftKV_ReplicatedState
--- PASS: TestRaftKV_ReplicatedState (0.85s)
=== RUN   TestWAL_AppendAndReplay
--- PASS: TestWAL_AppendAndReplay (0.02s)
=== RUN   TestWAL_CorruptedTail
--- PASS: TestWAL_CorruptedTail (0.01s)
PASS
ok      github.com/iam-iftekhar/kvraft/internal/config     0.012s
ok      github.com/iam-iftekhar/kvraft/internal/kv         0.015s
ok      github.com/iam-iftekhar/kvraft/internal/logger     0.009s
ok      github.com/iam-iftekhar/kvraft/internal/raft       1.912s
ok      github.com/iam-iftekhar/kvraft/internal/server     0.864s
ok      github.com/iam-iftekhar/kvraft/internal/storage    0.035s

```

---

## Remaining Technical Roadmap
**Phase 8: Multi-Node Production TCP Transport** — Replace `MemoryNetwork` with real RPC over TCP sockets for inter-node communication.
**Phase 9: Production Application Binary & CLI Client** — Finalize `cmd/kvserver` and build interactive CLI client tool (`cmd/kvcli`).
**Phase 10: Fault Tolerance & Fault Injection Suite** — Add tests for partition tolerance, leader crash during replication, and recovery under high throughput.
