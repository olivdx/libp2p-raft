//! Single Raft node binary for Docker / multi-process deployments.
//!
//! Cluster topology: one TOML file (`CLUSTER_CONFIG`, default `config/cluster.local.toml`).
//!
//! ```text
//! NODE_ID=1 cargo run --bin raft-node
//! cargo run --bin raft-node -- --list-nodes
//! ```
//!
//! Legacy env (no cluster file): `RAFT_PEERS=2:127.0.0.1:4102,...` + `RAFT_VOTERS=1,2,3`

use std::env;
use std::error::Error;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use futures::future::FutureExt;
use futures::StreamExt;
use libp2p::swarm::SwarmEvent;
use libp2p::{Multiaddr, PeerId, SwarmBuilder};
use libp2p_raft::cluster_config::{resolve_host_port, ClusterConfig};
use libp2p_raft::config::{RaftConfig, SeedPeer};
use libp2p_raft::node_identity::{keypair_for_node, peer_id_for_node};
use libp2p_raft::raft::types::{EntryType, Role};
use libp2p_raft::storage::MemoryStorage;
use libp2p_raft::{Event, RaftBehaviour};
use tracing_subscriber::EnvFilter;

const DEFAULT_CLUSTER_CONFIG: &str = "config/cluster.local.toml";

fn cluster_config_path() -> PathBuf {
    env::var("CLUSTER_CONFIG")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from(DEFAULT_CLUSTER_CONFIG))
}

/// `RAFT_PEERS` format: `node_id:host:port,...`
fn parse_peers_legacy(raw: &str, self_id: u64) -> Result<Vec<SeedPeer>, String> {
    let mut out = Vec::new();
    for part in raw.split(',').map(str::trim).filter(|s| !s.is_empty()) {
        let mut fields = part.split(':');
        let node_id: u64 = fields
            .next()
            .ok_or("peer entry missing node_id")?
            .parse()
            .map_err(|e| format!("peer node_id: {e}"))?;
        if node_id == self_id {
            continue;
        }
        let host = fields.next().ok_or("peer entry missing host")?.to_string();
        let port: u16 = fields
            .next()
            .ok_or("peer entry missing port")?
            .parse()
            .map_err(|e| format!("peer port: {e}"))?;
        let addr = resolve_host_port(&host, port)?;
        out.push(SeedPeer {
            node_id,
            peer_id: peer_id_for_node(node_id).map_err(|e| e.to_string())?,
            addrs: vec![addr],
        });
    }
    Ok(out)
}

async fn seed_peers_with_retry(
    load: impl Fn() -> Result<Vec<SeedPeer>, String>,
) -> Result<Vec<SeedPeer>, Box<dyn Error>> {
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        match load() {
            Ok(peers) => return Ok(peers),
            Err(e) if Instant::now() < deadline => {
                eprintln!("peer resolve retry: {e}");
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
            Err(e) => return Err(e.into()),
        }
    }
}

fn env_u64(name: &str, default: u64) -> u64 {
    env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn parse_voters(raw: &str) -> Result<Vec<u64>, Box<dyn Error>> {
    let voters: Result<Vec<_>, _> = raw
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|s| s.parse())
        .collect();
    let voters = voters?;
    if voters.is_empty() {
        return Err("RAFT_VOTERS must list at least one node id".into());
    }
    Ok(voters)
}

fn build_config(node_id: u64, voters: Vec<u64>, seed_peers: Vec<SeedPeer>) -> RaftConfig {
    let election_base = env_u64("ELECTION_TIMEOUT_MS", 500 + node_id * 200);
    let jitter = env_u64("ELECTION_JITTER_MS", 10 + node_id * 30);
    RaftConfig {
        node_id,
        voters,
        seed_peers,
        election_timeout: Duration::from_millis(election_base),
        election_jitter: Duration::from_millis(jitter),
        heartbeat_interval: Duration::from_millis(env_u64("HEARTBEAT_MS", 100)),
        rpc_timeout: Duration::from_millis(env_u64("RPC_TIMEOUT_MS", 500)),
        rpc_max_retries: 0,
        snapshot_threshold: 10_000,
    }
}

struct NodeRuntimeConfig {
    voters: Vec<u64>,
    seed_peers: Vec<SeedPeer>,
    listen_port: u16,
    propose_hello: bool,
}

fn load_from_cluster(path: &Path, node_id: u64) -> Result<NodeRuntimeConfig, String> {
    let cluster = ClusterConfig::load(path)?;
    let listen_port = cluster
        .listen_port(node_id)
        .ok_or_else(|| format!("node {node_id} not defined in {}", path.display()))?;
    let propose_hello = cluster.propose_hello_node == Some(node_id)
        || env::var("PROPOSE_HELLO")
            .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
            .unwrap_or(false);
    Ok(NodeRuntimeConfig {
        voters: cluster.voters.clone(),
        seed_peers: cluster.seed_peers(node_id)?,
        listen_port,
        propose_hello,
    })
}

async fn load_runtime_config(node_id: u64) -> Result<NodeRuntimeConfig, Box<dyn Error>> {
    let cluster_path = cluster_config_path();
    if cluster_path.is_file() {
        let path = cluster_path.clone();
        let seed_peers = seed_peers_with_retry(move || {
            ClusterConfig::load(&path).and_then(|c| c.seed_peers(node_id))
        })
        .await?;
        let mut rt = load_from_cluster(&cluster_path, node_id)?;
        rt.seed_peers = seed_peers;
        return Ok(rt);
    }

    let voters_raw = env::var("RAFT_VOTERS").unwrap_or_else(|_| "1,2,3".into());
    let voters = parse_voters(&voters_raw)?;
    let listen_port = env_u64("LISTEN_PORT", 4100 + node_id) as u16;
    let peers_raw = env::var("RAFT_PEERS").unwrap_or_default();
    let seed_peers = seed_peers_with_retry({
        let raw = peers_raw;
        move || parse_peers_legacy(&raw, node_id)
    })
    .await?;
    let propose_hello = env::var("PROPOSE_HELLO")
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false);
    Ok(NodeRuntimeConfig {
        voters,
        seed_peers,
        listen_port,
        propose_hello,
    })
}

fn list_nodes() -> Result<(), Box<dyn Error>> {
    let path = cluster_config_path();
    let cluster = ClusterConfig::load(&path)?;
    for id in cluster.node_ids() {
        println!("{id}");
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = env::args().collect();
    if args.iter().any(|a| a == "--list-nodes") {
        return list_nodes();
    }

    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let node_id = env_u64("NODE_ID", 0);
    if node_id == 0 {
        return Err("NODE_ID must be set (e.g. 1..5)".into());
    }

    let cluster_path = cluster_config_path();
    let NodeRuntimeConfig {
        voters,
        seed_peers,
        listen_port,
        propose_hello,
    } = load_runtime_config(node_id).await?;

    let kp = keypair_for_node(node_id)?;
    let self_peer = PeerId::from(kp.public());
    let listen: Multiaddr = format!("/ip4/0.0.0.0/tcp/{listen_port}").parse()?;

    println!(
        "starting node_id={node_id} peer_id={self_peer} listen={listen} config={} seeds={}",
        cluster_path.display(),
        seed_peers.len()
    );

    let cfg = build_config(node_id, voters, seed_peers.clone());
    let mut swarm = SwarmBuilder::with_existing_identity(kp)
        .with_tokio()
        .with_tcp(
            libp2p::tcp::Config::default(),
            libp2p::noise::Config::new,
            libp2p::yamux::Config::default,
        )?
        .with_behaviour(|_| RaftBehaviour::new(cfg, MemoryStorage::new()))?
        .with_swarm_config(|c| c.with_idle_connection_timeout(Duration::from_secs(60)))
        .build();
    swarm.listen_on(listen)?;

    loop {
        tokio::select! {
            ev = swarm.select_next_some() => {
                if matches!(ev, SwarmEvent::NewListenAddr { .. }) {
                    break;
                }
            }
            _ = tokio::time::sleep(Duration::from_secs(5)) => {
                return Err("timeout waiting for listen address".into());
            }
        }
    }

    for seed in &seed_peers {
        swarm.behaviour_mut().dial_seed(seed.peer_id);
    }
    tokio::time::sleep(Duration::from_millis(500)).await;

    let mut stable_leader_since: Option<Instant> = None;
    let mut proposed = false;
    let mut tick = tokio::time::interval(Duration::from_millis(50));

    loop {
        tokio::select! {
            ev = swarm.select_next_some() => handle_event(node_id, ev, &mut stable_leader_since),
            _ = tick.tick() => {
                while let Some(Some(ev)) = swarm.next().now_or_never() {
                    handle_event(node_id, ev, &mut stable_leader_since);
                }
            }
        }

        if propose_hello
            && !proposed
            && swarm.behaviour().role() == Role::Leader
        {
            if stable_leader_since.is_none() {
                stable_leader_since = Some(Instant::now());
            } else if stable_leader_since.unwrap().elapsed() >= Duration::from_secs(1) {
                let idx = swarm.behaviour_mut().propose(b"hello".to_vec())?;
                println!("node {node_id} proposed hello at index={idx}");
                proposed = true;
            }
        }
    }
}

fn handle_event(
    node_id: u64,
    ev: SwarmEvent<Event>,
    stable_leader_since: &mut Option<Instant>,
) {
    match ev {
        SwarmEvent::Behaviour(Event::RoleChanged { role, term, .. }) => {
            println!("node {node_id} -> {role:?} term={term}");
            if role != Role::Leader {
                *stable_leader_since = None;
            }
        }
        SwarmEvent::Behaviour(Event::Committed { entries }) => {
            for e in entries {
                let EntryType::Command(data) = e.entry_type;
                println!(
                    "node {node_id} Committed index={} data={:?}",
                    e.index,
                    String::from_utf8_lossy(&data)
                );
            }
        }
        SwarmEvent::Behaviour(Event::SnapshotNeeded { last_applied }) => {
            // Demo node keeps no state machine; a real app serializes its state here
            // and calls `swarm.behaviour_mut().compact(last_applied, blob)`.
            println!("node {node_id} SnapshotNeeded last_applied={last_applied}");
        }
        SwarmEvent::Behaviour(Event::SnapshotInstalled { snapshot }) => {
            println!(
                "node {node_id} SnapshotInstalled index={} term={} bytes={}",
                snapshot.last_included_index,
                snapshot.last_included_term,
                snapshot.state_blob.len()
            );
        }
        SwarmEvent::Behaviour(Event::PeerMapped { peer, node }) => {
            println!("node {node_id} PeerMapped peer={peer} node={node}");
        }
        SwarmEvent::Behaviour(Event::RpcFailed { peer, error }) => {
            eprintln!("node {node_id} RpcFailed peer={peer} error={error}");
        }
        SwarmEvent::NewListenAddr { address, .. } => {
            println!("node {node_id} listening on {address}");
        }
        SwarmEvent::ConnectionEstablished { peer_id, .. } => {
            println!("node {node_id} connected to {peer_id}");
        }
        SwarmEvent::ConnectionClosed { peer_id, .. } => {
            println!("node {node_id} disconnected from {peer_id}");
        }
        _ => {}
    }
}
