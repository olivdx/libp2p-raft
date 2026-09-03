# Sơ đồ kiến trúc & data flow

Ba sơ đồ mermaid tổng hợp: component, luồng replicate một command, và luồng snapshot / compaction. Tên trong ngoặc là file nguồn tương ứng.

## 1. Component

Engine là lõi thuần logic (không async, không IO mạng) — nhận message, trả về danh sách `Action`. Behaviour dịch Action thành RPC libp2p và ngược lại. Storage là trait cắm được: log + hard_state trong RAM, riêng snapshot được `FileSnapshotStorage` ghi đĩa dạng bincode.

```mermaid
flowchart TB
    subgraph app["Application layer"]
        BIN["raft-node<br/>(bin/raft-node.rs)"]
        CFG["ClusterConfig · RaftConfig · node_identity<br/>(cluster_config.rs, config.rs)"]
    end

    subgraph net["libp2p integration"]
        BEH["RaftBehaviour&lt;S&gt;<br/>(behaviour.rs)<br/>dial/backoff · pending RPC · PeerMap"]
        HND["ConnectionHandler + upgrade<br/>(handler.rs, protocol/upgrade.rs)"]
        COD["Codec: length-prefix + bincode<br/>(protocol/codec.rs)"]
        MSG["RaftMessage<br/>(protocol/messages.rs)"]
    end

    subgraph core["Raft core (pure logic)"]
        ENG["RaftEngine&lt;S&gt;<br/>(raft/engine.rs)<br/>role · election · replication"]
        LOG["log helpers<br/>(raft/log.rs)"]
        MEM["Membership<br/>(raft/membership.rs)"]
        SNP["OutboundTransfer / InboundAssembly<br/>(raft/snapshot.rs)"]
    end

    subgraph sto["Storage"]
        TRT["trait Storage<br/>(storage/mod.rs)"]
        MST["MemoryStorage<br/>(storage/memory.rs)"]
        FST["FileSnapshotStorage<br/>(storage/file_snapshot.rs)"]
        FILE[("snapshot.bin<br/>bincode trên đĩa")]
    end

    BIN -->|"propose / compact"| BEH
    BEH -->|"Event: Committed, SnapshotNeeded,<br/>SnapshotInstalled, RoleChanged"| BIN
    CFG --> BIN
    BEH <-->|"tick / handle_message → Vec&lt;Action&gt;"| ENG
    BEH --> HND
    HND --> COD
    COD --- MSG
    ENG --> LOG
    ENG --> MEM
    ENG --> SNP
    ENG --> TRT
    TRT --> MST
    TRT --> FST
    FST -->|"delegate log/hard_state"| MST
    FST -->|"tmp + fsync + rename"| FILE
```

## 2. Data flow: Propose → Replicate → Commit

Đường đi của một command từ lúc app đề xuất đến lúc cả cluster apply. Follower ghi log trước khi trả lời (durability barrier); leader chỉ commit khi quá bán số voter đã match.

```mermaid
sequenceDiagram
    participant App as App (leader)
    participant LB as RaftBehaviour
    participant LE as RaftEngine (leader)
    participant LS as Storage (leader)
    participant FB as RaftBehaviour (follower)
    participant FE as RaftEngine (follower)
    participant FA as App (follower)

    App->>LB: propose(data)
    LB->>LE: propose(data)
    LE->>LS: persist(entry idx=n)
    LE-->>LB: Action::Send AppendEntries
    LB->>FB: AppendEntries {prev, entries, commit}<br/>(bincode qua libp2p stream)
    FB->>FE: handle_append_entries
    FE->>FE: check prev_log match,<br/>truncate conflict nếu có
    FE->>FB: persist rồi mới trả lời
    FB->>LB: AppendEntriesResp {success, match_index}
    LB->>LE: handle resp
    LE->>LE: match quá bán → commit_index = n
    LE-->>LB: Action::Apply
    LB-->>App: Event::Committed(entries)
    Note over LB,FB: Heartbeat kế tiếp mang commit_index mới
    FB->>FE: AppendEntries (commit=n)
    FE-->>FB: Action::Apply
    FB-->>FA: Event::Committed(entries)
```

## 3. Data flow: Snapshot — compact tại chỗ & truyền cho follower tụt hậu

Nhánh trái: node tự compact khi log applied vượt `snapshot_threshold` — app serialize full state thành `state_blob`, log 1..k bị cắt, file `snapshot.bin` được ghi atomic. Nhánh phải: follower cần entry đã bị cắt thì leader stream blob theo chunk 64 KB, hỏng giữa chừng làm lại từ offset 0.

```mermaid
flowchart TB
    subgraph compact["Compaction tại mỗi node"]
        A["last_applied − snapshot_base ≥ threshold"] -->|"Event::SnapshotNeeded{last_applied}"| B["App serialize full state<br/>→ state_blob (bytes)"]
        B -->|"compact(index, blob)"| C["Engine dựng Snapshot<br/>{index, term, conf, state_blob}"]
        C --> D["storage.install_snapshot"]
        D --> E["cắt log prefix 1..k<br/>first_index = k+1"]
        D --> F[("ghi snapshot.bin<br/>.tmp → fsync → rename")]
    end

    subgraph transfer["Leader → follower tụt hậu"]
        G["next_index của follower<br/>&lt; first_index (log đã cắt)"] --> H["OutboundTransfer:<br/>chunk 64KB, pipeline depth 1"]
        H -->|"InstallSnapshot{index, term,<br/>offset, data, done}"| I["InboundAssembly:<br/>ghép theo offset"]
        I -->|"sai offset / lệch index"| J["reset — chờ lại offset 0"]
        I -->|"done=true"| K["install_snapshot<br/>+ Event::SnapshotInstalled"]
        K --> L["App reset state machine<br/>từ state_blob"]
    end

    E -.->|"follower xin entry đã cắt"| G
```

Điểm cần nhớ:

- **Snapshot = state cuối, không phải lịch sử** — blob chỉ chứa trạng thái tại mốc k; snapshot mới ghi đè hoàn toàn snapshot cũ.
- **Stale bị chặn trước khi chạm đĩa** — snapshot có index ≤ snapshot hiện tại bị bỏ qua ở cả RAM lẫn file.
- **Không resume** — transfer đứt giữa chừng thì làm lại từ offset 0 (ràng buộc thiết kế, pipeline depth 1).
