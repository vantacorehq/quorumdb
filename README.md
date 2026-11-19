![quorumdb](banner.svg)

# quorumdb

In-process replicated key-value store. Three Raft nodes in one process elect a
leader, replicate a log, and apply a committed put to each node's memory.
Zero external dependencies.

## Status

Not production-ready. The working path is the in-process cluster in
`src/cluster.rs`: leader election, log replication, and a committed put that
is readable from a follower after commit.

`TcpTransport::send` is not implemented; it returns `Unreachable`. There is no
network event loop. `Wal::append` can write one entry and `sync_all`, but the
cluster does not open that file and does not replay it. State lives in memory
for the process lifetime.

## Layout

- `src/raft.rs` — Raft node: votes, append-entries, commit rule
- `src/cluster.rs` — 3-node in-process driver
- `src/storage.rs` — in-memory state machine and an unused single-entry WAL helper
- `src/network.rs` — transport trait; TCP send is a stub
- `src/main.rs` — runs one in-process election and put

## Building

```
cargo test
cargo run --release
```

## License

MIT — see LICENSE.
