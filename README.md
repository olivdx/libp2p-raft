# libp2p-raft

Learning / research crate: a **DIY mini-Raft** exposed as a rust-libp2p **`NetworkBehaviour`**, with a **custom** `ConnectionHandler` and stream protocol `/libp2p-raft/1.0.0`.

> **Not production Raft.** Intentional simplifications: log + hard state in memory (only the snapshot is persisted to disk, via `FileSnapshotStorage`), single-step membership (add/remove one node), simple `next_index` decrement, no OpenRaft.

**Why a custom `ConnectionHandler` instead of `libp2p::request_response`?**  
Learning goal: understand Behaviour ↔ Handler lifecycle, substream framing, and unary RPC plumbing end-to-end. Production code would often compose `request_response`; we hand-roll it on purpose.

**Docs:** [`docs/architecture.md`](docs/architecture.md) · [`docs/diagrams.md`](docs/diagrams.md) (component & data-flow diagrams)

---

## Goals

- Learn how `NetworkBehaviour` + `ConnectionHandler` interact under a Swarm
- Map a pure consensus state machine onto libp2p I/O
- Cover mini-Raft: **election (RequestVote) → log replication (AppendEntries) → snapshot → basic membership**

---

## Architecture

### Glossary (libp2p newcomers)

| Term | Meaning here |
|------|----------------|
| **Swarm** | libp2p event loop: dials, accepts connections, polls behaviours |
| **PeerId** | libp2p identity from a **pinned keypair** (do not regenerate) |
| **NodeId** | Raft identity (`u64`), stable in membership config |
| **Connection** | Encrypted multiplexed link Swarm manages between two PeerIds |
| **ConnectionHandler** | Per-connection worker: opens `/libp2p-raft/1.0.0` substreams, reads/writes bytes |
| **NetworkBehaviour** | Swarm-facing logic: owns handlers' commands, emits dials & app events |
| **Unary RPC** | One substream = one request + one response, then close (not a long-lived bi-di session) |
| **Action** | Pure output from `RaftEngine` (e.g. `Send`); Behaviour translates to Dial / NotifyHandler |

### Big picture

```mermaid
flowchart TB
  subgraph App["App / examples/three_node.rs"]
    APP["SwarmBuilder TCP+Noise+Yamux<br/>listen · dial · event loop<br/>propose / consume Event"]
  end

  subgraph Libp2p["★ PROVIDED BY libp2p"]
    SW["Swarm<br/>polls Behaviour · Dial · connections"]
  end

  subgraph WeWrite["★ WE WRITE"]
    BH["RaftBehaviour — behaviour.rs<br/>owns Engine, Storage, PeerMap,<br/>PendingRequests, deadline timer<br/>Action::Send → Dial if needed → NotifyHandler<br/>match responses by correlation_id"]
    ENG["RaftEngine — raft/engine.rs<br/>pure sync SM · no PeerId/Dial/stream<br/>tick / handle_rpc → Actions"]
    HND["ConnectionHandler — handler.rs<br/>/libp2p-raft/1.0.0<br/>unary framed substreams<br/>length + bincode"]
    ST["Storage / MemoryStorage<br/>persist atomic in-process<br/>not on disk"]
  end

  APP -->|"plugs Behaviour into"| SW
  SW -->|"poll / ToSwarm / handler events"| BH
  BH -->|"sync calls"| ENG
  BH -->|"per connection"| HND
  ENG -->|"persist()"| ST
```

**Threading model:** single Swarm event loop. `RaftEngine` is called synchronously from `NetworkBehaviour::poll` — no background consensus thread. Engine is `Send`-friendly state, but not driven on its own task.

**Swarm ownership:** this crate does **not** build or own a Swarm. It only provides `RaftBehaviour` for apps to plug into `SwarmBuilder` (keeps the library transport-agnostic). Swarm construction lives in `examples/` (and your app).

### libp2p provides vs we write

| ★ Provided by **libp2p** | ★ We **write** |
|--------------------------|----------------|
| `Swarm` / `SwarmBuilder` | `RaftBehaviour` |
| TCP / Noise / Yamux | `ConnectionHandler` + `/libp2p-raft/1.0.0` |
| `dial` / `listen_on` / connection events | `WireEnvelope` + codec |
| `NetworkBehaviour` / `ConnectionHandler` **traits** | `RaftEngine` (DIY Raft) |
| Stream multiplexing | `Storage` / `MemoryStorage` |
| | `PeerMap`, `PendingRequest`, examples |

### Layer responsibilities

| Layer | Owns | Does **not** own |
|--------|------|------------------|
| App / Swarm | Transport, listen/dial loop, consuming events | Raft rules |
| RaftBehaviour | Engine, peer map, pending RPCs, deadline wake, Action → network | Election/log algorithm details |
| ConnectionHandler | Open/read/write/close framed substreams | Term, votes, commit index |
| RaftEngine | Term, role, log, membership, logical deadlines | `PeerId`, dial, streams |
| Storage | In-memory hard state + log + snapshot; `persist` is one in-process atomic update | Disk durability / networking |

### Sequence (propose → commit)

```mermaid
sequenceDiagram
  participant App
  participant Swarm
  participant Behaviour
  participant Engine
  participant Handler
  participant Peer

  App->>Behaviour: propose(data)
  Behaviour->>Engine: propose / persist append
  Engine-->>Behaviour: Action::Send AppendEntries
  Behaviour->>Swarm: ToSwarm::Dial if no connection
  Swarm-->>Behaviour: connection established
  Behaviour->>Handler: NotifyHandler SendRequest
  Handler->>Peer: WireEnvelope on /libp2p-raft/1.0.0
  Peer-->>Handler: response
  Handler-->>Behaviour: Response correlation_id
  Behaviour->>Engine: handle_rpc
  Engine-->>Behaviour: Apply / commit actions
  Behaviour-->>App: Event::Committed
```

Same pattern for **election**: Sleep deadline → `engine.tick` → `Broadcast`/`Send` **RequestVote** → responses → majority → `BecomeLeader` → `Event::RoleChanged`. On leader failure, followers' election timeouts fire and a new election runs (standard Raft); connection loss alone does **not** remove a node from membership.

### Control flows (short)

1. **Timer** — Behaviour deadline timer fires → `engine.tick(now)` → Actions → Behaviour may request **Dial via `ToSwarm`** (engine never dials) → Handler unary RPC → match `correlation_id` → `handle_rpc`.
2. **Propose** — leader `persist` append → AppendEntries to followers → majority `match_index` → commit → `Event::Committed`.
3. **Inbound** — Handler reads request → `handle_rpc` → optional `SendResponse` on same unary RPC.
4. **Failure** — timeout → retry ≤ `rpc_max_retries` → `handle_rpc_failure` + `Event::RpcFailed`. **libp2p connection drop ≠ Raft membership remove**; membership only changes via `propose_membership`.

### Identity & bootstrap

- Raft **`NodeId = u64`**
- Each NodeId is statically mapped for the cluster lifetime to exactly one **`PeerId`** (from a pinned keypair) + seed `Multiaddr`s
- No Kademlia required for MVP — static `SeedPeer { node_id, peer_id, addrs }`
- Configure voting NodeIds **before** the first election timeout

### Protocol

| Item | Choice |
|------|--------|
| Protocol ID | `/libp2p-raft/1.0.0` |
| Framing | `u32 BE length` + bincode |
| RPC model | Unary request/response over a framed substream, then close |
| Envelope | `WireEnvelope { correlation_id, msg }` |
| Messages | RequestVote(+Resp), AppendEntries(+Resp), InstallSnapshot(+Resp) |
| Heartbeat | Empty AppendEntries |
| Snapshot | Sequential unary chunks; on failure **restart from offset 0** (no resume) |
| Not used | Gossipsub for Raft RPCs |

### RaftEngine surface

```text
tick(now)            → TickOutcome { actions, next_deadline }
handle_rpc(from,msg) → Vec<Action>
handle_rpc_failure   → Vec<Action>
propose(data)        → Index          // leader only
propose_membership(AddNode | RemoveNode) → Index
apply_ready()        → committed entries
```

**Actions:** `Send`, `Broadcast` (Behaviour expands to per-peer `Send`), `Apply`, `BecomeLeader|Follower|Candidate`, `SnapshotNeeded`, `SnapshotInstallComplete`.

**Snapshots:** when applied entries since the last snapshot reach `snapshot_threshold`, the engine emits `SnapshotNeeded { last_applied }`; the app serializes its state machine and calls `compact(last_applied, blob)`, which stores `Snapshot { last_included_index, last_included_term, conf, state_blob }` and truncates the log prefix. A follower whose `next_index` is below the leader's `first_index` receives the snapshot as sequential unary `InstallSnapshot` chunks (`offset`, `data`, `done`); any failure restarts from offset 0. On completion the follower emits `SnapshotInstalled { snapshot }`. See [docs/phase-4.md](docs/phase-4.md).

```mermaid
flowchart TB
    subgraph compact["Compaction at each node"]
        A["last_applied − snapshot_base ≥ threshold"] -->|"Event::SnapshotNeeded{last_applied}"| B["App serializes full state<br/>→ state_blob (bytes)"]
        B -->|"compact(index, blob)"| C["Engine builds Snapshot<br/>{index, term, conf, state_blob}"]
        C --> D["storage.install_snapshot"]
        D --> E["truncate log prefix 1..k<br/>first_index = k+1"]
        D --> F[("write snapshot.bin<br/>.tmp → fsync → rename")]
    end

    subgraph transfer["Leader → lagging follower"]
        G["follower next_index<br/>&lt; first_index (log truncated)"] --> H["OutboundTransfer:<br/>64KB chunks, pipeline depth 1"]
        H -->|"InstallSnapshot{index, term,<br/>offset, data, done}"| I["InboundAssembly:<br/>reassemble by offset"]
        I -->|"bad offset / index mismatch"| J["reset — expect offset 0"]
        I -->|"done=true"| K["install_snapshot<br/>+ Event::SnapshotInstalled"]
        K --> L["App resets state machine<br/>from state_blob"]
    end

    E -.->|"follower asks for truncated entry"| G
```

**Replication:** `next_index` / `match_index`. On reject → **simple decrement**. New followers start with `next_index = leader_last_log_index + 1`, `match_index = 0`.

**Membership:** only one Add **or** Remove at a time; reject while pending. Not joint consensus.

**Storage:** `persist(hard_state, entries)` — one in-process atomic update (teaches the Raft durability *contract*; process restart still loses all state).

### Tick scheduling

Engine holds absolute deadlines. Behaviour resets a deadline timer after state-changing events and calls `tick(now)` **only when due** (no busy-poll every `poll()`).

### Crate layout

```text
src/
  behaviour.rs   # NetworkBehaviour adapter
  handler.rs     # ConnectionHandler
  cluster_config.rs · node_identity.rs
  peer_map.rs · config.rs · error.rs
  protocol/      # messages, codec, upgrade
  raft/          # engine, types, log, snapshot, membership
  storage/       # trait + MemoryStorage + FileSnapshotStorage (snapshot.bin on disk)
  bin/
    raft-node.rs # deployable single-node binary
config/
  cluster.local.toml   # 3-node localhost topology
  cluster.docker.toml  # 6-node Docker static IPs
examples/
  three_node.rs  # in-process 3-node demo
  echo_two_peers.rs
scripts/
  run-cluster.ps1 · run-cluster.sh
tests/
  engine_*.rs · cluster_config.rs
```

### Running a multi-node cluster

**One TOML file** defines voters + all nodes (`id`, `host`, `port`). Each process only needs `NODE_ID`.

| Target | Config | Command |
|--------|--------|---------|
| Local 3-node | `config/cluster.local.toml` | `.\scripts\run-cluster.ps1` |
| Docker 6-node | `config/cluster.docker.toml` | `docker compose up --build` |
| Single node | same config | `NODE_ID=1 cargo run --bin raft-node` |

List node ids from config: `cargo run --bin raft-node -- --list-nodes`

Legacy env (`RAFT_PEERS`, `RAFT_VOTERS`) still works if no cluster file is present.

### Implementation phases

| Phase | Deliverable |
|-------|-------------|
| 0 | Scaffold + correlated echo RPC (2 peers) |
| 1 | Engine election + MemoryStorage (unit tests) |
| 2 | Wire votes; 3-node elects leader |
| 3 | AppendEntries + propose + commit |
| 4 | Snapshots |
| 5 | Membership add/remove one-at-a-time |

---

## Status

Phases 1–4 implemented: election over libp2p, log replication, `propose` / `Event::Committed`, and snapshots (compaction + chunked `InstallSnapshot`). Next: membership change.

## License

MIT OR Apache-2.0 (intended)
