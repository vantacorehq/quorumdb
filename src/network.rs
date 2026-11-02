//! Transport between Raft nodes. Swappable: start with plain TCP + a tiny
//! framed protocol, replace with anything else later without touching raft.rs.

use crate::raft::{AppendEntriesArgs, AppendEntriesReply, NodeId, RequestVoteArgs, RequestVoteReply};

#[derive(Debug, Clone)]
pub enum RpcRequest {
    RequestVote(RequestVoteArgs),
    AppendEntries(AppendEntriesArgs),
}

#[derive(Debug, Clone)]
pub enum RpcReply {
    RequestVote(RequestVoteReply),
    AppendEntries(AppendEntriesReply),
}

/// Abstraction over "send this RPC to that node and get a reply back".
/// Real implementation: TCP socket + bincode-ish framing.
/// Test implementation: in-memory channel with injectable latency/drops
/// (used by tests/chaos.rs to simulate partitions).
pub trait Transport {
    fn send(&self, to: NodeId, req: RpcRequest) -> Result<RpcReply, TransportError>;
}

#[derive(Debug)]
pub enum TransportError {
    Timeout,
    Unreachable,
}

pub struct TcpTransport {
    pub peer_addrs: std::collections::HashMap<NodeId, String>,
}

impl Transport for TcpTransport {
    fn send(&self, _to: NodeId, _req: RpcRequest) -> Result<RpcReply, TransportError> {
        // TODO: open connection (or reuse pooled one), write framed request,
        // read framed reply, deserialize. Return TransportError::Timeout on
        // deadline exceeded.
        Err(TransportError::Unreachable)
    }
}
