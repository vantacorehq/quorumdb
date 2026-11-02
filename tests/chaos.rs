//! Deterministic chaos tests: simulate network partitions and node crashes,
//! then assert linearizability held across the whole run.
//!
//! Pattern borrowed from glassring/faultline — same style, applied here to
//! the actual quorumdb cluster instead of a standalone lab.

// TODO: once network::Transport has a real in-memory test impl, wire it
// here with a seeded RNG so failures are reproducible from the seed alone.

#[test]
fn three_node_cluster_survives_single_partition() {
    // 1. spin up 3 RaftNodes sharing an in-memory Transport
    // 2. elect a leader, commit a handful of entries
    // 3. partition the leader away from the other two
    // 4. assert a new leader is elected and progress continues
    // 5. heal the partition, assert the old leader's log converges
}

#[test]
fn no_two_leaders_in_same_term() {
    // safety property, not liveness — run many randomized partition
    // schedules and assert this never breaks, regardless of timing
}
