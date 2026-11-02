mod network;
mod raft;
mod storage;

use std::time::Duration;

use raft::RaftNode;
use storage::{StateMachine, Wal};

fn main() {
    let node_id: u64 = std::env::args()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(1);
    let peers: Vec<u64> = vec![1, 2, 3].into_iter().filter(|&id| id != node_id).collect();

    let mut _node = RaftNode::new(node_id, peers, Duration::from_millis(150));
    let _wal = Wal::open(format!("quorumdb-node-{node_id}.wal")).expect("open wal");
    let _state = StateMachine::default();

    println!("quorumdb node {node_id} starting (skeleton — no event loop wired yet)");

    // TODO: event loop — select! between election timer, heartbeat timer,
    // incoming RPCs, and client requests. This is where raft.rs, storage.rs
    // and network.rs actually get connected.
}
