//! Raft consensus: leader election, log replication, cluster membership.
//! Single file on purpose — split into election.rs / replication.rs later
//! once this grows past ~500 lines.

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
    pub command: Vec<u8>, // opaque payload, interpreted by the state machine
}

pub struct RaftNode {
    pub id: NodeId,
    pub peers: Vec<NodeId>,
    pub role: Role,

    // Persistent state (must be flushed to WAL before responding to RPCs)
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
    // fast conflict-resolution hint (used instead of decrementing one at a time)
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

    /// Called when the election timer fires with no heartbeat received.
    pub fn start_election(&mut self) {
        self.role = Role::Candidate;
        self.current_term += 1;
        self.voted_for = Some(self.id);
        self.reset_election_deadline();
        // TODO: broadcast RequestVote to all peers, collect replies,
        // become leader on majority grant.
    }

    pub fn handle_request_vote(&mut self, args: RequestVoteArgs) -> RequestVoteReply {
        if args.term < self.current_term {
            return RequestVoteReply { term: self.current_term, vote_granted: false };
        }
        if args.term > self.current_term {
            self.step_down(args.term);
        }

        let can_vote = self.voted_for.is_none() || self.voted_for == Some(args.candidate_id);
        let candidate_log_ok = self.is_log_up_to_date(args.last_log_index, args.last_log_term);

        if can_vote && candidate_log_ok {
            self.voted_for = Some(args.candidate_id);
            self.reset_election_deadline();
            RequestVoteReply { term: self.current_term, vote_granted: true }
        } else {
            RequestVoteReply { term: self.current_term, vote_granted: false }
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

        // TODO: consistency check against prev_log_index/prev_log_term,
        // truncate conflicting suffix, append new entries, advance commit_index.

        AppendEntriesReply { term: self.current_term, success: true, conflict_index: None }
    }

    fn step_down(&mut self, term: Term) {
        if term > self.current_term {
            self.current_term = term;
            self.voted_for = None;
        }
        self.role = Role::Follower;
    }

    fn is_log_up_to_date(&self, last_log_index: LogIndex, last_log_term: Term) -> bool {
        let (my_term, my_index) = self
            .log
            .last()
            .map(|e| (e.term, e.index))
            .unwrap_or((0, 0));
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
