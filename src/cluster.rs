//! In-process 3-node Raft driver.
//!
//! Nodes call each other's handlers directly. Nothing is written to disk and
//! `TcpTransport` is not used.

use std::collections::BTreeMap;
use std::time::Duration;

use crate::raft::{NodeId, RaftNode, Role};
use crate::storage::{Command, StateMachine};

const IDS: [NodeId; 3] = [1, 2, 3];

struct ClusterNode {
    raft: RaftNode,
    sm: StateMachine,
}

pub struct Cluster {
    nodes: BTreeMap<NodeId, ClusterNode>,
}

impl Cluster {
    pub fn new() -> Self {
        let mut nodes = BTreeMap::new();
        for id in IDS {
            let peers = IDS.into_iter().filter(|peer| *peer != id).collect();
            nodes.insert(
                id,
                ClusterNode {
                    raft: RaftNode::new(id, peers, Duration::from_millis(150)),
                    sm: StateMachine::default(),
                },
            );
        }
        Self { nodes }
    }

    pub fn role(&self, id: NodeId) -> Role {
        self.nodes[&id].raft.role
    }

    /// Start an election on `candidate` and apply the votes of the other nodes.
    /// Returns true when that candidate becomes the unique leader.
    pub fn elect(&mut self, candidate: NodeId) -> bool {
        if !self.nodes.contains_key(&candidate) {
            return false;
        }
        let args = {
            let node = self.nodes.get_mut(&candidate).unwrap();
            node.raft.start_election();
            node.raft.vote_request()
        };
        let peers = self.nodes[&candidate].raft.peers.clone();
        let mut votes = 1usize;
        for peer in peers {
            let reply = self
                .nodes
                .get_mut(&peer)
                .unwrap()
                .raft
                .handle_request_vote(args.clone());
            if reply.term > args.term {
                self.nodes
                    .get_mut(&candidate)
                    .unwrap()
                    .raft
                    .step_down(reply.term);
                return false;
            }
            if reply.vote_granted {
                votes += 1;
            }
        }
        let majority = self.nodes.len() / 2 + 1;
        if votes >= majority {
            self.nodes.get_mut(&candidate).unwrap().raft.become_leader();
            true
        } else {
            false
        }
    }

    pub fn leader_count(&self) -> usize {
        self.nodes
            .values()
            .filter(|n| n.raft.role == Role::Leader)
            .count()
    }

    /// Append a put on the leader, replicate it, and apply the commit.
    pub fn put(&mut self, leader: NodeId, key: &str, value: &[u8]) -> Result<(), &'static str> {
        if self.nodes.get(&leader).map(|n| n.raft.role) != Some(Role::Leader) {
            return Err("not the leader");
        }
        let bytes = Command::Put {
            key: key.to_string(),
            value: value.to_vec(),
        }
        .encode();
        self.nodes
            .get_mut(&leader)
            .unwrap()
            .raft
            .append_command(bytes);
        self.replicate(leader);
        self.apply_all();
        let raft = &self.nodes[&leader].raft;
        if raft.last_index() > 0 && raft.commit_index >= raft.last_index() {
            Ok(())
        } else {
            Err("entry was not committed")
        }
    }

    pub fn get(&self, id: NodeId, key: &str) -> Option<Vec<u8>> {
        self.nodes.get(&id)?.sm.get(key).cloned()
    }

    fn replicate(&mut self, leader: NodeId) {
        for _ in 0..16 {
            if self.nodes.get(&leader).map(|n| n.raft.role) != Some(Role::Leader) {
                return;
            }
            let peers = self.nodes[&leader].raft.peers.clone();
            for peer in &peers {
                let args = self.nodes[&leader].raft.append_entries_for(*peer);
                let reply = self
                    .nodes
                    .get_mut(peer)
                    .unwrap()
                    .raft
                    .handle_append_entries(args.clone());
                self.nodes
                    .get_mut(&leader)
                    .unwrap()
                    .raft
                    .on_append_reply(*peer, &args, &reply);
            }
            self.nodes.get_mut(&leader).unwrap().raft.advance_commit();
            let leader_commit = self.nodes[&leader].raft.commit_index;
            let leader_last = self.nodes[&leader].raft.last_index();
            let caught_up = peers.iter().all(|peer| {
                let follower_commit = self.nodes[peer].raft.commit_index;
                let matched = self.nodes[&leader]
                    .raft
                    .match_index
                    .get(peer)
                    .copied()
                    .unwrap_or(0);
                follower_commit >= leader_commit && matched >= leader_last
            });
            if caught_up {
                break;
            }
        }
    }

    fn apply_all(&mut self) {
        for node in self.nodes.values_mut() {
            while node.raft.last_applied < node.raft.commit_index {
                node.raft.last_applied += 1;
                let idx = node.raft.last_applied;
                let command = node.raft.entry_at(idx).map(|e| e.command.clone());
                if let Some(bytes) = command {
                    if let Some(cmd) = Command::decode(&bytes) {
                        node.sm.apply(cmd);
                    }
                }
            }
        }
    }
}

impl Default for Cluster {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn three_nodes_elect_one_leader() {
        let mut cluster = Cluster::new();
        assert!(cluster.elect(1), "candidate 1 did not win a majority");
        assert_eq!(cluster.role(1), Role::Leader);
        assert_eq!(cluster.role(2), Role::Follower);
        assert_eq!(cluster.role(3), Role::Follower);
        assert_eq!(cluster.leader_count(), 1);
    }

    #[test]
    fn committed_put_is_readable_on_a_follower() {
        let mut cluster = Cluster::new();
        assert!(cluster.elect(1));
        cluster.put(1, "user", b"ada").expect("commit");
        assert_eq!(cluster.get(1, "user").as_deref(), Some(b"ada".as_slice()));
        assert_eq!(cluster.get(2, "user").as_deref(), Some(b"ada".as_slice()));
        assert_eq!(cluster.get(3, "user").as_deref(), Some(b"ada".as_slice()));
    }

    #[test]
    fn follower_rejects_a_put() {
        let mut cluster = Cluster::new();
        assert!(cluster.elect(1));
        assert!(cluster.put(2, "x", b"y").is_err());
        assert!(cluster.get(1, "x").is_none());
        assert!(cluster.get(2, "x").is_none());
    }
}
