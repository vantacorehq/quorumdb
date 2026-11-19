//! Raft consensus: leader election, log replication, cluster membership.
//!
//! The node methods are the protocol. `cluster` drives them in-process for a
//! fixed 3-node group. There is no TCP event loop.

use std::collections::HashMap;
use std::time::{Duration, Instant};

pub type NodeId = u64;
pub type Term = u64;
pub type LogIndex = u64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Follower,
    Candidate,
    Leader,
}

#[derive(Debug, Clone)]
pub struct LogEntry {
    pub term: Term,
    pub index: LogIndex,
    pub command: Vec<u8>,
}

pub struct RaftNode {
    pub id: NodeId,
    pub peers: Vec<NodeId>,
    pub role: Role,

    // Persistent state (flushed by a caller before responding, if a caller persists it).
    pub current_term: Term,
    pub voted_for: Option<NodeId>,
    pub log: Vec<LogEntry>,

    // Volatile state
    pub commit_index: LogIndex,
    pub last_applied: LogIndex,

    // Leader-only volatile state
    pub next_index: HashMap<NodeId, LogIndex>,
    pub match_index: HashMap<NodeId, LogIndex>,

    // Timing
    pub election_deadline: Instant,
    pub election_timeout: Duration,
}

#[derive(Debug, Clone)]
pub struct RequestVoteArgs {
    pub term: Term,
    pub candidate_id: NodeId,
    pub last_log_index: LogIndex,
    pub last_log_term: Term,
}

#[derive(Debug, Clone)]
pub struct RequestVoteReply {
    pub term: Term,
    pub vote_granted: bool,
}

#[derive(Debug, Clone)]
pub struct AppendEntriesArgs {
    pub term: Term,
    pub leader_id: NodeId,
    pub prev_log_index: LogIndex,
    pub prev_log_term: Term,
    pub entries: Vec<LogEntry>,
    pub leader_commit: LogIndex,
}

#[derive(Debug, Clone)]
pub struct AppendEntriesReply {
    pub term: Term,
    pub success: bool,
    pub conflict_index: Option<LogIndex>,
}

impl RaftNode {
    pub fn new(id: NodeId, peers: Vec<NodeId>, election_timeout: Duration) -> Self {
        Self {
            id,
            peers,
            role: Role::Follower,
            current_term: 0,
            voted_for: None,
            log: Vec::new(),
            commit_index: 0,
            last_applied: 0,
            next_index: HashMap::new(),
            match_index: HashMap::new(),
            election_deadline: Instant::now() + election_timeout,
            election_timeout,
        }
    }

    pub fn last_index(&self) -> LogIndex {
        self.log.last().map(|e| e.index).unwrap_or(0)
    }

    pub fn last_term(&self) -> Term {
        self.log.last().map(|e| e.term).unwrap_or(0)
    }

    pub fn entry_at(&self, index: LogIndex) -> Option<&LogEntry> {
        if index == 0 {
            return None;
        }
        self.log
            .get((index as usize) - 1)
            .filter(|e| e.index == index)
    }

    /// Local transition to candidate. The caller sends [`Self::vote_request`]
    /// to peers and calls [`Self::become_leader`] after a majority grants.
    pub fn start_election(&mut self) {
        self.role = Role::Candidate;
        self.current_term += 1;
        self.voted_for = Some(self.id);
        self.reset_election_deadline();
    }

    pub fn vote_request(&self) -> RequestVoteArgs {
        RequestVoteArgs {
            term: self.current_term,
            candidate_id: self.id,
            last_log_index: self.last_index(),
            last_log_term: self.last_term(),
        }
    }

    pub fn become_leader(&mut self) {
        self.role = Role::Leader;
        let next = self.last_index() + 1;
        self.next_index.clear();
        self.match_index.clear();
        for peer in &self.peers {
            self.next_index.insert(*peer, next);
            self.match_index.insert(*peer, 0);
        }
    }

    /// Append a command on the leader. Returns the new log index.
    pub fn append_command(&mut self, command: Vec<u8>) -> LogIndex {
        let index = self.last_index() + 1;
        let term = self.current_term;
        self.log.push(LogEntry {
            term,
            index,
            command,
        });
        index
    }

    pub fn append_entries_for(&self, peer: NodeId) -> AppendEntriesArgs {
        let next = self.next_index.get(&peer).copied().unwrap_or(1).max(1);
        let prev = next - 1;
        let prev_term = if prev == 0 {
            0
        } else {
            self.entry_at(prev).map(|e| e.term).unwrap_or(0)
        };
        let entries = self
            .log
            .iter()
            .filter(|e| e.index >= next)
            .cloned()
            .collect();
        AppendEntriesArgs {
            term: self.current_term,
            leader_id: self.id,
            prev_log_index: prev,
            prev_log_term: prev_term,
            entries,
            leader_commit: self.commit_index,
        }
    }

    pub fn on_append_reply(
        &mut self,
        peer: NodeId,
        args: &AppendEntriesArgs,
        reply: &AppendEntriesReply,
    ) {
        if reply.term > self.current_term {
            self.step_down(reply.term);
            return;
        }
        if self.role != Role::Leader || reply.term < self.current_term {
            return;
        }
        if reply.success {
            let last = args
                .entries
                .last()
                .map(|e| e.index)
                .unwrap_or(args.prev_log_index);
            let known = self.match_index.get(&peer).copied().unwrap_or(0);
            if last > known {
                self.match_index.insert(peer, last);
            }
            self.next_index.insert(peer, last.saturating_add(1));
        } else {
            let fallback = self
                .next_index
                .get(&peer)
                .copied()
                .unwrap_or(1)
                .saturating_sub(1)
                .max(1);
            let next = reply.conflict_index.unwrap_or(fallback).max(1);
            self.next_index.insert(peer, next);
        }
    }

    /// Advance `commit_index` to the highest index replicated to a majority
    /// whose term is the leader's current term.
    pub fn advance_commit(&mut self) -> bool {
        if self.role != Role::Leader {
            return false;
        }
        let old = self.commit_index;
        let last = self.last_index();
        let majority = (self.peers.len() + 1) / 2 + 1;
        let mut n = self.commit_index + 1;
        while n <= last {
            let term_ok = self.entry_at(n).map(|e| e.term) == Some(self.current_term);
            if term_ok {
                let mut matches = 1usize;
                for peer in &self.peers {
                    if self.match_index.get(peer).copied().unwrap_or(0) >= n {
                        matches += 1;
                    }
                }
                if matches >= majority {
                    self.commit_index = n;
                }
            }
            n += 1;
        }
        self.commit_index != old
    }

    pub fn handle_request_vote(&mut self, args: RequestVoteArgs) -> RequestVoteReply {
        if args.term < self.current_term {
            return RequestVoteReply {
                term: self.current_term,
                vote_granted: false,
            };
        }
        if args.term > self.current_term {
            self.step_down(args.term);
        }

        let can_vote = self.voted_for.is_none() || self.voted_for == Some(args.candidate_id);
        let candidate_log_ok = self.is_log_up_to_date(args.last_log_index, args.last_log_term);

        if can_vote && candidate_log_ok {
            self.voted_for = Some(args.candidate_id);
            self.reset_election_deadline();
            RequestVoteReply {
                term: self.current_term,
                vote_granted: true,
            }
        } else {
            RequestVoteReply {
                term: self.current_term,
                vote_granted: false,
            }
        }
    }

    pub fn handle_append_entries(&mut self, args: AppendEntriesArgs) -> AppendEntriesReply {
        if args.term < self.current_term {
            return AppendEntriesReply {
                term: self.current_term,
                success: false,
                conflict_index: None,
            };
        }

        self.step_down(args.term);
        self.reset_election_deadline();

        if args.prev_log_index > 0 {
            match self.entry_at(args.prev_log_index) {
                None => {
                    return AppendEntriesReply {
                        term: self.current_term,
                        success: false,
                        conflict_index: Some(self.last_index() + 1),
                    };
                }
                Some(existing) if existing.term != args.prev_log_term => {
                    let conflict = args.prev_log_index;
                    self.log.truncate((conflict - 1) as usize);
                    return AppendEntriesReply {
                        term: self.current_term,
                        success: false,
                        conflict_index: Some(conflict),
                    };
                }
                Some(_) => {}
            }
        }

        let mut index = args.prev_log_index;
        for entry in args.entries {
            index += 1;
            if let Some(existing) = self.entry_at(index) {
                if existing.term != entry.term {
                    self.log.truncate((index - 1) as usize);
                    self.log.push(entry);
                }
            } else {
                self.log.push(entry);
            }
        }

        if args.leader_commit > self.commit_index {
            let last = self.last_index();
            self.commit_index = args.leader_commit.min(last);
        }

        AppendEntriesReply {
            term: self.current_term,
            success: true,
            conflict_index: None,
        }
    }

    pub fn step_down(&mut self, term: Term) {
        if term > self.current_term {
            self.current_term = term;
            self.voted_for = None;
        }
        self.role = Role::Follower;
    }

    fn is_log_up_to_date(&self, last_log_index: LogIndex, last_log_term: Term) -> bool {
        let (my_term, my_index) = self.log.last().map(|e| (e.term, e.index)).unwrap_or((0, 0));
        last_log_term > my_term || (last_log_term == my_term && last_log_index >= my_index)
    }

    fn reset_election_deadline(&mut self) {
        self.election_deadline = Instant::now() + self.election_timeout;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn grants_vote_when_log_is_current() {
        let mut node = RaftNode::new(1, vec![2, 3], Duration::from_millis(150));
        let reply = node.handle_request_vote(RequestVoteArgs {
            term: 1,
            candidate_id: 2,
            last_log_index: 0,
            last_log_term: 0,
        });
        assert!(reply.vote_granted);
        assert_eq!(node.voted_for, Some(2));
    }

    #[test]
    fn rejects_stale_term() {
        let mut node = RaftNode::new(1, vec![2, 3], Duration::from_millis(150));
        node.current_term = 5;
        let reply = node.handle_request_vote(RequestVoteArgs {
            term: 3,
            candidate_id: 2,
            last_log_index: 0,
            last_log_term: 0,
        });
        assert!(!reply.vote_granted);
    }
}
