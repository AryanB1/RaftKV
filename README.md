# RaftKV

A distributed key-value store built from scratch in C++20, using the Raft consensus algorithm for fault-tolerant replication across a cluster of nodes.

## Performance

Benchmarked on a 3-node in-process cluster (Apple M-series, Release build):

| Workload | Throughput | p50 Latency | p99 Latency |
|---|---|---|---|
| Write (replicated consensus) | 27K ops/sec | 33 us | 95 us |
| Read (leader-local) | 3.2M ops/sec | 0.2 us | 0.3 us |
| Mixed (80R/20W) | 177K ops/sec | 0.2 us | 38 us |
| Concurrent write (4 threads) | 37K ops/sec | 58 us | 553 us |

## Architecture

```
                    +-----------+
                    |  Client   |
                    +-----+-----+
                          |
                    +-----v-----+
                    |  Leader    |  <-- All writes go through leader
                    |  RaftNode  |
                    +--+-----+--+
                       |     |
              +--------+     +--------+
              |                       |
        +-----v-----+         +------v----+
        |  Follower  |         |  Follower |
        |  RaftNode  |         |  RaftNode |
        +------------+         +-----------+
```

Each node runs the full Raft protocol: leader election, log replication, and commit advancement. Writes are replicated to a majority before being committed. Reads are served directly from the leader's state machine.

### Key Components

- **Raft Consensus** (`src/raft/`) - Leader election with randomized timeouts, log replication via AppendEntries, commit index advancement using sorted median of match indices (O(n log n) instead of naive O(n * log_size))
- **WAL Persistence** (`src/storage/`) - Write-ahead log with CRC32 checksums per entry. Atomic metadata persistence (term/votedFor) via temp-file-and-rename. Recovery replays WAL and rebuilds state machine on startup.
- **TCP Networking** (`src/network/`) - 4-byte length-prefixed message framing with protobuf serialization. Per-peer outbound connections with automatic reconnection.
- **KV State Machine** (`src/kv/`) - Deterministic PUT/GET/DELETE/LIST applied via committed log entries.
- **CLI Client** (`src/client/`) - REPL with automatic leader discovery and redirect following.

### Design Decisions

| Decision | Rationale |
|---|---|
| Single `tick_loop` thread | Avoids thread lifecycle bugs from separate heartbeat/election threads |
| `std::recursive_mutex` | Handlers call internal methods that also need the lock; simpler than restructuring the entire call graph |
| `SendFunction` callback | Decouples Raft from networking; enables in-process testing without TCP |
| WAL with per-entry CRC32 | Detects partial writes and disk corruption; stops recovery at first bad entry |
| Atomic metadata writes | Term/votedFor use temp+rename to prevent torn writes on crash |
| Leader-local reads | Trades strict linearizability for read performance (safe when leader lease is valid) |

## Building

Requires CMake 3.20+, C++20 compiler, and Homebrew dependencies:

```bash
brew install protobuf spdlog googletest

cmake -B build && cmake --build build

# Run tests (33 tests)
./build/raftkv_tests

# Run benchmarks
cmake -B build-release -DCMAKE_BUILD_TYPE=Release
cmake --build build-release
./build-release/raftkv_bench
```

## Running a Cluster

```bash
# Start a 3-node local cluster
./build/raftkv_node 1 5001 2:localhost:5002 3:localhost:5003 &
./build/raftkv_node 2 5002 1:localhost:5001 3:localhost:5003 &
./build/raftkv_node 3 5003 1:localhost:5001 2:localhost:5002 &

# Connect the CLI client
./build/raftkv_client localhost:5001 localhost:5002 localhost:5003
> PUT name aryan
OK
> GET name
aryan
> LIST
name
> DELETE name
OK
```
