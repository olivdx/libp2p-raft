# Kiến trúc Handler → Behaviour → Engine

Tài liệu chi tiết về ba tầng chính của crate `libp2p-raft`: cách `ConnectionHandler` xử lý I/O, `RaftBehaviour` làm adapter libp2p, và `RaftEngine` chạy state machine Raft thuần.

**Nguồn mã:** `src/handler.rs`, `src/behaviour.rs`, `src/raft/engine.rs`, `src/protocol/messages.rs`

**Liên quan:** [libp2p fundamentals](libp2p-fundamentals.md) (connection → substream → protocol), [design spec](superpowers/specs/2026-07-23-libp2p-raft-design.md), [English version](handler-behaviour-engine-en.md)

---

## 1. Bức tranh tổng thể

Crate này tách **consensus logic** khỏi **networking**. Engine không biết `PeerId`, dial, hay stream. Behaviour là cầu nối duy nhất giữa thế giới Raft (`NodeId`) và thế giới libp2p (`PeerId`, `ConnectionId`, substream).

```mermaid
flowchart TB
  subgraph App["App / examples/three_node.rs"]
    APP["SwarmBuilder · listen/dial · event loop<br/>propose() · consume Event"]
  end

  subgraph Libp2p["libp2p (Swarm)"]
    SW["Swarm<br/>poll Behaviour · Dial · Connection events"]
  end

  subgraph HandlerLayer["RaftHandler — handler.rs<br/>1 instance per connection"]
    HND["Unary RPC on /libp2p-raft/1.0.0<br/>u32 BE length + bincode<br/>concurrent substreams"]
  end

  subgraph BehaviourLayer["RaftBehaviour — behaviour.rs"]
    BH["PeerMap · PendingRequest · inbound_channels<br/>Sleep deadline · execute_actions()"]
  end

  subgraph EngineLayer["RaftEngine — raft/engine.rs"]
    ENG["Pure sync state machine<br/>term · role · log · membership"]
    ST["Storage / MemoryStorage<br/>persist() atomic"]
  end

  APP --> SW
  SW <-->|"ToSwarm / FromSwarm<br/>NotifyHandler"| BH
  BH <-->|"FromBehaviour / ToBehaviour"| HND
  BH -->|"sync: tick / handle_rpc / propose"| ENG
  ENG --> ST
  HND -->|"bytes on Yamux substream"| PEER["Peer node"]
```

### Mô hình luồng (threading)

- **Một** Swarm event loop duy nhất.
- `RaftEngine` được gọi **đồng bộ** từ `RaftBehaviour::poll`, `on_connection_handler_event`, hoặc `propose` — không có background consensus thread.
- Handler chạy async I/O qua `FuturesUnordered` nhưng vẫn nằm trong cùng task poll của Swarm.

---

## 2. Trách nhiệm từng tầng

| Tầng | File | Sở hữu | Không sở hữu |
|------|------|--------|--------------|
| **RaftHandler** | `handler.rs` | Mở/đọc/ghi/đóng substream; encode/decode `WireEnvelope`; nhiều RPC đồng thời; `channel_id` cho inbound response | term, vote, commit_index; PeerMap; timeout RPC |
| **RaftBehaviour** | `behaviour.rs` | `RaftEngine` + Storage; `PeerMap`; `PendingRequest` + `correlation_id`; `inbound_channels`; Sleep; Action → Dial/NotifyHandler/Event | Chi tiết thuật toán Raft; raw bytes |
| **RaftEngine** | `raft/engine.rs` | role, term, leader, votes; log qua `Storage::persist`; next_index/match_index; election/heartbeat deadline; membership; `Vec<Action>` | PeerId, Multiaddr, Dial; correlation_id; stream |

### Quy tắc ranh giới (boundary rule)

```text
Engine  → chỉ NodeId, RaftMessage, Instant
Behaviour → dịch NodeId ↔ PeerId, gán correlation_id, quản lý RPC lifecycle
Handler → chỉ di chuyển WireEnvelope trên substream
```

Engine **không bao giờ** gọi dial hay mở stream. Mọi `Action::Send` đều do Behaviour chuyển thành `NotifyHandler` hoặc `ToSwarm::Dial`.

---

## 3. Cấu trúc dữ liệu chính

### 3.1 RaftHandler (`handler.rs`)

```rust
pub enum FromBehaviour {
    SendRequest { correlation_id, msg },
    SendResponse { channel_id, correlation_id, msg },
}

pub enum ToBehaviour {
    Request { correlation_id, msg, channel_id },
    Response { correlation_id, msg },
    Failure { correlation_id: Option<u64>, error },
}
```

**State nội bộ Handler:**

| Field | Vai trò |
|-------|---------|
| `pending_outbound` | Hàng đợi RPC outbound chờ mở substream |
| `inflight` | `FuturesUnordered` — read/write frame async |
| `response_txs` | `channel_id → oneshot::Sender` — chờ Behaviour gửi response inbound |
| `pending_events` | Sự kiện sẵn sàng báo lên Behaviour |

### 3.2 RaftBehaviour (`behaviour.rs`)

```rust
struct PendingRequest {
    correlation_id: u64,
    to: NodeId,
    peer: PeerId,
    kind: RpcKind,
    sent_at: Instant,
    msg: RaftMessage,
}

struct InboundChannel {
    peer: PeerId,
    conn: ConnectionId,
    correlation_id: u64,
}
```

**State nội bộ Behaviour:**

| Field | Vai trò |
|-------|---------|
| `engine` | Pure Raft state machine |
| `peer_map` | `NodeId ↔ PeerId` + seed addresses |
| `connected` | `PeerId → Set<ConnectionId>` |
| `pending` | `correlation_id → PendingRequest` (outbound đang chờ response) |
| `inbound_channels` | `channel_id → InboundChannel` (đang xử lý request inbound) |
| `sleep` | Deadline timer — min(engine deadline, RPC timeout) |
| `pending_events` | Hàng đợi `ToSwarm` (Dial, NotifyHandler, GenerateEvent) |

### 3.3 RaftEngine (`raft/engine.rs`)

```rust
pub enum Action {
    Send { to: NodeId, msg: RaftMessage },
    Broadcast { msg: RaftMessage },
    Apply { entries: Vec<LogEntry> },
    BecomeLeader { term },
    BecomeFollower { term, leader },
    BecomeCandidate { term },
    SnapshotNeeded { last_applied },
    SnapshotInstallComplete { index },
}
```

**State nội bộ Engine:**

| Field | Vai trò |
|-------|---------|
| `storage` | Hard state + log + snapshot (trait `Storage`) |
| `role`, `leader`, `votes` | Election state |
| `next_index`, `match_index` | Leader replication tracking |
| `ae_inflight` | Pipeline depth 1 — không gửi AE tiếp cho peer cho đến khi có response |
| `election_deadline`, `heartbeat_deadline` | Absolute deadlines cho `tick()` |
| `commit_index`, `last_applied` | Commit / apply progress |

---

## 4. Hợp đồng sự kiện (event contracts)

### 4.1 Behaviour ↔ Handler

```mermaid
flowchart LR
  subgraph BH["RaftBehaviour"]
    B1["send_rpc()"]
    B2["execute_actions() inbound reply"]
    B3["on_connection_handler_event()"]
  end

  subgraph H["RaftHandler"]
    H1["on_behaviour_event()"]
    H2["poll() → NotifyBehaviour"]
  end

  B1 -->|"FromBehaviour::SendRequest"| H1
  B2 -->|"FromBehaviour::SendResponse"| H1
  H2 -->|"ToBehaviour::Request"| B3
  H2 -->|"ToBehaviour::Response"| B3
  H2 -->|"ToBehaviour::Failure"| B3
```

| Hướng | Event | Ý nghĩa |
|-------|-------|---------|
| BH → H | `SendRequest { correlation_id, msg }` | Mở outbound substream; ghi envelope; đọc một response |
| BH → H | `SendResponse { channel_id, correlation_id, msg }` | Trả lời request inbound trên cùng substream |
| H → BH | `Request { correlation_id, msg, channel_id }` | RPC inbound; Behaviour map PeerId → NodeId rồi gọi engine |
| H → BH | `Response { correlation_id, msg }` | Outbound hoàn tất; match `PendingRequest` |
| H → BH | `Failure { correlation_id?, error }` | Lỗi upgrade/IO — **không** xóa node khỏi membership |

### 4.2 Engine → Behaviour (Actions)

| Action | Behaviour dịch thành |
|--------|----------------------|
| `Send { to, msg }` | `PeerMap` → PeerId → (Dial nếu chưa connected) → `NotifyHandler SendRequest`; hoặc `SendResponse` nếu trả lời inbound |
| `Broadcast { msg }` | Mở rộng thành `Send` cho mỗi voter khác |
| `BecomeLeader/Follower/Candidate` | `ToSwarm::GenerateEvent(Event::RoleChanged)` |
| `Apply { entries }` | `Event::Committed { entries }` |
| `SnapshotNeeded { last_applied }` | `Event::SnapshotNeeded` — app serialize state rồi gọi `RaftBehaviour::compact(last_applied, blob)` |
| `SnapshotInstallComplete` | `Event::SnapshotInstalled { snapshot }` — app reset state machine từ `state_blob` |

### 4.3 Behaviour → App (public events)

| Event | Khi nào |
|-------|---------|
| `RoleChanged { role, term, leader }` | Engine chuyển role (election xong) |
| `Committed { entries }` | Majority replicate → commit_index tăng |
| `PeerMapped { peer, node }` | Startup từ seed_peers |
| `RpcFailed { peer, error }` | Timeout, dial fail, connection closed, unknown peer |

---

## 5. Luồng chi tiết

### 5.1 Inbound RPC (peer gửi tới ta)

Peer mở substream inbound → Handler đọc frame → Engine xử lý → Handler ghi response.

```mermaid
sequenceDiagram
  participant Peer
  participant Handler as RaftHandler
  participant Behaviour as RaftBehaviour
  participant Engine as RaftEngine
  participant Storage

  Peer->>Handler: FullyNegotiatedInbound (substream)
  Handler->>Handler: read_frame() → WireEnvelope
  Handler->>Behaviour: ToBehaviour::Request { correlation_id, msg, channel_id }
  Note over Handler: oneshot channel chờ SendResponse

  Behaviour->>Behaviour: PeerId → NodeId (PeerMap)
  Behaviour->>Behaviour: inbound_channels.insert(channel_id)
  Behaviour->>Engine: handle_rpc(from, msg, now)
  Engine->>Storage: persist() nếu cần
  Engine-->>Behaviour: Vec<Action> (thường Send response về from)

  Behaviour->>Behaviour: execute_actions(..., Some((from, channel_id)))
  Behaviour->>Handler: FromBehaviour::SendResponse { channel_id, ... }
  Handler->>Handler: oneshot.send → write_frame(response)
  Handler->>Peer: response bytes, đóng unary substream
```

**Điểm quan trọng:**

- `channel_id` do Handler cấp phát — dùng để ghép response đúng substream inbound.
- Nếu Engine không trả `Send { to: from }`, Behaviour xóa `inbound_channels` để tránh leak.
- Handler giữ substream mở qua oneshot cho đến khi Behaviour gửi `SendResponse`.

### 5.2 Outbound RPC (ta gửi tới peer)

Engine emit `Action::Send` → Behaviour gán `correlation_id` → Handler mở substream outbound.

```mermaid
sequenceDiagram
  participant Engine as RaftEngine
  participant Behaviour as RaftBehaviour
  participant Swarm
  participant Handler as RaftHandler
  participant Peer

  Engine-->>Behaviour: Action::Send { to: NodeId, msg }
  Behaviour->>Behaviour: NodeId → PeerId (PeerMap)
  alt chưa connected
    Behaviour->>Swarm: ToSwarm::Dial
    Behaviour->>Engine: handle_rpc_failure (lossy)
    Behaviour-->>App: Event::RpcFailed
  else đã connected
    Behaviour->>Behaviour: pending.insert(correlation_id)
    Behaviour->>Swarm: NotifyHandler SendRequest
    Swarm->>Handler: FromBehaviour::SendRequest
    Handler->>Swarm: OutboundSubstreamRequest /libp2p-raft/1.0.0
    Swarm->>Peer: negotiate + write WireEnvelope
    Peer-->>Handler: read response frame
    Handler->>Behaviour: ToBehaviour::Response { correlation_id, msg }
    Behaviour->>Behaviour: pending.remove(correlation_id)
    Behaviour->>Engine: handle_rpc(to, msg, now)
    Engine-->>Behaviour: Vec<Action> (votes, match_index, commit, ...)
  end
```

**Điểm quan trọng:**

- `correlation_id` do Behaviour cấp — Engine không biết.
- RPC outbound **lossy** khi chưa connected: Behaviour dial rồi drop RPC hiện tại; heartbeat/election tick sẽ retry sau.
- `PendingRequest` lưu `sent_at` để timeout.

### 5.3 Deadline timer (`tick`)

```mermaid
sequenceDiagram
  participant Behaviour as RaftBehaviour
  participant Engine as RaftEngine
  participant Handler as RaftHandler

  loop RaftBehaviour::poll
    Behaviour->>Behaviour: expire PendingRequest ≥ rpc_timeout
    Behaviour->>Engine: handle_rpc_failure(to, kind)
    alt now ≥ engine.next_deadline()
      Behaviour->>Engine: tick(now)
      Engine-->>Behaviour: TickOutcome { actions, next_deadline }
      Note over Engine: Follower timeout → Candidate + Broadcast RequestVote<br/>Leader heartbeat → AppendEntries
      Behaviour->>Behaviour: execute_actions(actions)
    end
    Behaviour->>Behaviour: arm_sleep(earliest_wake)
  end
  Behaviour->>Handler: NotifyHandler (nếu có Send actions)
```

**Follower:** election timeout → `BecomeCandidate` + `Broadcast RequestVote`.

**Leader:** heartbeat interval → gửi `AppendEntries` (entries rỗng = heartbeat).

**Timeout RPC:** không retry trực tiếp — gọi `handle_rpc_failure` để clear `ae_inflight`; tick/heartbeat retry sau.

### 5.4 `propose(data)` → commit

```mermaid
sequenceDiagram
  participant App
  participant Behaviour as RaftBehaviour
  participant Engine as RaftEngine
  participant Storage
  participant Followers as Follower handlers

  App->>Behaviour: propose(data)
  Behaviour->>Engine: propose(data)
  Engine->>Storage: persist(None, [LogEntry])
  Engine-->>Behaviour: (index, replicate Actions)
  Behaviour->>Followers: AppendEntries (per follower, ae_inflight=1)

  Followers-->>Behaviour: AppendEntriesResp { success, match_index }
  Behaviour->>Engine: handle_rpc(follower, resp)
  Engine->>Engine: advance match_index; check majority
  Engine-->>Behaviour: Action::Apply { entries }
  Behaviour-->>App: Event::Committed { entries }
```

**Leader replication:**

- `next_index[peer]` / `match_index[peer]` theo dõi tiến độ từng follower.
- Reject → simple `next_index` decrement (không conflict-index hint).
- Commit khi majority có `match_index >= N` và entry term == current term.

---

## 6. Vòng `poll()` — thứ tự thực thi

### 6.1 RaftBehaviour::poll

```mermaid
flowchart TD
  START([poll cx]) --> DRAIN{pending_events<br/>non-empty?}
  DRAIN -->|yes| READY1[Poll::Ready ToSwarm]
  DRAIN -->|no| TIMEOUT[Scan pending RPC timeouts]
  TIMEOUT --> FAIL[fail_pending → handle_rpc_failure]
  FAIL --> DRAIN2{pending_events?}
  DRAIN2 -->|yes| READY2[Poll::Ready]
  DRAIN2 -->|no| TICK{now ≥ next_deadline?}
  TICK -->|yes| ENG[engine.tick → execute_actions]
  ENG --> DRAIN3{pending_events?}
  DRAIN3 -->|yes| READY3[Poll::Ready]
  DRAIN3 -->|no| SLEEP[poll Sleep]
  TICK -->|no| SLEEP
  SLEEP -->|Ready| REARM[arm_sleep, loop]
  SLEEP -->|Pending| PEND[Poll::Pending]
  REARM --> DRAIN
```

| Bước | Mô tả |
|------|-------|
| 1 | Drain `pending_events` → trả `ToSwarm` (Dial / NotifyHandler / GenerateEvent) |
| 2 | Hết hạn `PendingRequest` ≥ `rpc_timeout` → `handle_rpc_failure` |
| 3 | Nếu `now ≥ engine.next_deadline()` → `engine.tick(now)` |
| 4 | Poll `Sleep`; Ready → re-arm và loop; Pending → return |

`earliest_wake = min(engine.next_deadline(), min(pending.sent_at + rpc_timeout))`.

### 6.2 RaftHandler::poll

```mermaid
flowchart TD
  HSTART([poll cx]) --> HEV{pending_events?}
  HEV -->|yes| HREADY1[NotifyBehaviour]
  HEV -->|no| HOUT{pending_outbound?}
  HOUT -->|yes| HOPEN[OutboundSubstreamRequest]
  HOUT -->|no| HINF[poll inflight FuturesUnordered]
  HINF -->|OutboundDone| HRESP[queue ToBehaviour::Response]
  HINF -->|InboundRead| HREQ[queue Request + oneshot + inbound write task]
  HINF -->|Failed| HFAIL[queue Failure]
  HINF -->|Pending| HPEND[Poll::Pending]
  HRESP --> HEV
  HREQ --> HEV
  HFAIL --> HEV
```

---

## 7. Identity: NodeId vs PeerId

```mermaid
flowchart LR
  subgraph RaftWorld["Thế giới Raft (Engine)"]
    N1["NodeId = u64"]
    N2["RequestVote.candidate_id"]
    N3["Action::Send.to"]
  end

  subgraph Libp2pWorld["Thế giới libp2p (Behaviour)"]
    P1["PeerId (keypair)"]
    P2["ConnectionId"]
    P3["Multiaddr dial"]
  end

  PM["PeerMap<br/>SeedPeer config"]
  N1 <-->|"static map"| PM
  PM <-->|"seed_peers"| P1
```

| Khái niệm | Layer | Ghi chú |
|----------|-------|---------|
| `NodeId` | Engine, membership | Voting identity ổn định trong cluster |
| `PeerId` | libp2p connection | Từ keypair — phải pin, không regenerate |
| `PeerMap` | Behaviour | Map tĩnh từ `SeedPeer { node_id, peer_id, addrs }` |
| Connection drop | Behaviour | `fail_peer_pending` — **không** remove khỏi membership |

---

## 8. Wire format

```mermaid
flowchart LR
  subgraph Frame["Một frame trên substream"]
    LEN["u32 BE length"]
    PAYLOAD["bincode(WireEnvelope)"]
  end

  subgraph Envelope["WireEnvelope"]
    CID["correlation_id: u64"]
    MSG["msg: RaftMessage"]
  end

  LEN --> PAYLOAD
  PAYLOAD --> Envelope
```

```rust
pub struct WireEnvelope {
    pub correlation_id: u64,
    pub msg: RaftMessage,
}

// RaftMessage variants:
// RequestVote / RequestVoteResp
// AppendEntries / AppendEntriesResp
// InstallSnapshot / InstallSnapshotResp
// Heartbeat = AppendEntries { entries: [] }
```

| Thuộc tính | Giá trị |
|------------|---------|
| Protocol ID | `/libp2p-raft/1.0.0` |
| Framing | `u32` big-endian length + bincode payload |
| RPC model | Unary — 1 substream = 1 request + 1 response, rồi đóng |
| Max frame | 4 MiB (`MAX_FRAME_BYTES`) |
| Concurrent | Nhiều substream song song trên cùng connection |

---

## 9. Xử lý lỗi & edge cases

| Tình huống | Hành vi |
|------------|---------|
| Chưa connected khi `Send` | Dial + `handle_rpc_failure` + `RpcFailed` (lossy) |
| RPC timeout | `fail_pending` → `handle_rpc_failure`; tick retry sau |
| Connection closed | `fail_peer_pending` cho mọi pending tới peer đó |
| Dial failure | `fail_peer_pending` + `RpcFailed` |
| Unknown PeerId | `RpcFailed` — không gọi engine |
| Stale term trong response | Engine ignore (validate term mỗi inbound) |
| Inbound không có response action | Behaviour drop `inbound_channels` |
| `ae_inflight` | Leader không pipeline AE — depth 1 per follower |

**Lưu ý:** Connection drop ≠ Raft membership remove. Membership chỉ đổi qua log entry `EntryType::Config`.

---

## 10. Sơ đồ module trong crate

```mermaid
flowchart TB
  subgraph src["src/"]
    behaviour["behaviour.rs<br/>NetworkBehaviour adapter"]
    handler["handler.rs<br/>ConnectionHandler"]
    peer_map["peer_map.rs"]
    config["config.rs"]

    subgraph protocol["protocol/"]
      messages["messages.rs — WireEnvelope, RaftMessage"]
      codec["codec.rs — encode/decode"]
      upgrade["upgrade.rs — /libp2p-raft/1.0.0"]
    end

    subgraph raft["raft/"]
      engine["engine.rs — RaftEngine, Action"]
      types["types.rs — NodeId, LogEntry, Role"]
      log["log.rs"]
      membership["membership.rs"]
      snapshot["snapshot.rs"]
    end

    subgraph storage["storage/"]
      mem["memory.rs — MemoryStorage"]
    end
  end

  behaviour --> handler
  behaviour --> engine
  behaviour --> peer_map
  handler --> codec
  handler --> messages
  engine --> storage
  engine --> messages
```

---

## 11. Tóm tắt nhanh

1. **Handler** = I/O worker per connection; không hiểu Raft.
2. **Behaviour** = adapter; owns networking state + gọi Engine sync.
3. **Engine** = pure Raft SM; output `Action`, input RPC + time.
4. **correlation_id** = Behaviour/Handler — match outbound request/response.
5. **channel_id** = Handler — route inbound response về đúng substream.
6. **NodeId** = Raft; **PeerId** = libp2p; **PeerMap** = cầu nối.
7. **Single poll loop** — không background consensus task.
