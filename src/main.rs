use quorumdb::cluster::Cluster;

fn main() {
    let mut cluster = Cluster::new();
    let elected = cluster.elect(1);
    println!("quorumdb in-process cluster: node 1 elected leader: {elected}");
    if !elected {
        eprintln!("election failed");
        std::process::exit(1);
    }
    cluster.put(1, "alpha", b"one").expect("committed put");
    let from_follower = cluster
        .get(2, "alpha")
        .expect("follower should observe the committed put");
    println!(
        "follower 2 read alpha={}",
        String::from_utf8_lossy(&from_follower)
    );
}
