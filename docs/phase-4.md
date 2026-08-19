# Phase 4 — Snapshots + InstallSnapshot

**Goal:** Bound log growth via compaction; bring lagging/new followers up to date with chunked `InstallSnapshot` instead of full log replay.

Spec: [`docs/superpowers/specs/2026-07-23-libp2p-raft-design.md`](superpowers/specs/2026-07-23-libp2p-raft-design.md) — "Snapshot (MVP)".

---

## Delivered

| Area | Notes |
|------|-------|
| Storage | `MemoryStorage` keeps the log as a suffix from `first_index = snapshot.last_included_index + 1`; `install_snapshot` compacts the prefix (Raft §7: keep suffix if entry at `last_included_index` matches term, else discard log); stale snapshots ignored; `Storage::first_index()` added |
| Compaction | After apply, if `last_applied - snapshot_index >= snapshot_threshold` → `Action::SnapshotNeeded { last_applied }` (once until answered). App calls `RaftEngine::compact(index, state_blob)` (index ≤ `last_applied`) → snapshot `{index, term, conf, blob}` stored, log prefix truncated |
| Leader | `next_index[peer] < first_index` → `InstallSnapshot` chunks (`snapshot_chunk_size`, default 64 KiB) instead of AE. One chunk in flight per peer; ack advances `offset`; final ack → `match_index = K`, `next_index = K+1`, resume AE. RPC failure / step-down → transfer dropped, **restart from offset 0**. Local snapshot advancing mid-transfer restarts from 0 |
| Follower | Term/leader handling like AE (reset election deadline). `offset == 0` starts a fresh buffer; out-of-order chunk or metadata mismatch → buffer reset, wait for offset 0. `done` → `Storage::install_snapshot`, `commit_index`/`last_applied = max(., K)`, `Action::SnapshotInstallComplete { index }`. Snapshot with `K <= commit_index` is acked but ignored |
| AE on compacted log | `prev_log_index == K` verified against snapshot term; `prev_log_index < K` accepted; entries `<= K` skipped (leader that never compacted may send from index 1) |
| Behaviour | `Event::SnapshotNeeded { last_applied }`, `Event::SnapshotInstalled { snapshot }`, `RaftBehaviour::compact`, `RaftBehaviour::snapshot` |

Protocol messages unchanged (`InstallSnapshot` / `InstallSnapshotResp { term }`).

---

## Application contract

```text
Event::Committed { entries }        → apply to state machine
Event::SnapshotNeeded { last_applied } → blob = serialize(state); behaviour.compact(last_applied, blob)
Event::SnapshotInstalled { snapshot }  → state = deserialize(snapshot.state_blob)
```

The engine has no access to the state machine, so `SnapshotNeeded` is a request, not an automatic action; until the app answers it, the log keeps growing.

---

## Tests

- `tests/storage_memory.rs` — compaction, suffix retention, conflict discard, offset-aware `persist`/`truncate_from`
- `tests/engine_snapshot.rs` — threshold trigger, `compact`, AE against compacted log, leader chunk sequencing / restart-from-0 / step-down, follower assembly / reset / stale term / stale snapshot

```bash
cargo test
```

---

## Out of scope

Resume mid-transfer, disk persistence, membership change (`conf` is recorded in the snapshot but not applied on install).
