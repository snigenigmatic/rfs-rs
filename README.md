# rfs-rs

rfs-rs is a small, educational Redis-like server implemented in Rust. It supports RESP2/RESP3 framing and a subset of Redis commands (strings, lists, sets, hashes), key expiry, and append-only file (AOF) persistence.

Status
------
- Minimal, self-contained server implementation
- Unit and integration tests included
- Build and run locally for development/testing

Quick start
-----------
Prerequisites: Rust toolchain (cargo + rustc).

Build:

    cargo build --release

Run (default bind 127.0.0.1:6379):

    cargo run -- --bind 127.0.0.1:6380

Talk to it with redis-cli:

    redis-cli -p 6380 PING

Testing
-------
Run the test suite:

    cargo test

Project layout (important files)
-------------------------------
- `src/protocol/` — RESP parser & encoder
- `src/store/` — in-memory data store and expiry logic
- `src/command/` — command dispatch and implementations
- `src/persistence/aof.rs` — append-only file persistence
- `src/server/` — TCP server and connection handling
- `tests/integration.rs` — integration tests using a spawned server

Notes
-----
- The repository is intended for learning and experimentation, not production use.
- See the source for details and implementation notes.
- Contributions and improvements are welcome!

TODO
----
- [x] Add more commands (sorted sets, transactions, pub/sub)
    - [x] ZADD
    - [x] ZRANGE, ZSCORE, ZRANK, ZCARD, ZREM, ZCOUNT for sorted sets
- [] Implement RESP3 features (maps, sets, etc.)
- [] Add configuration options (persistence, eviction policies)
- [] Improve error handling and logging
- [] Add benchmarks and performance tests