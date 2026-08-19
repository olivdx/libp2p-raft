//! RaftEngine snapshot: compaction, leader chunked InstallSnapshot, follower assembly.

use std::time::{Duration, Instant};

use libp2p_raft::config::RaftConfig;
use libp2p_raft::protocol::RaftMessage;
use libp2p_raft::raft::engine::{Action, RaftEngine, RpcKind};
use libp2p_raft::raft::types::{EntryType, HardState, LogEntry, Role, Snapshot};
use libp2p_raft::storage::{MemoryStorage, Storage};

fn cfg(id: u64, threshold: u64) -> RaftConfig {
    RaftConfig {
        node_id: id,
        voters: vec![1, 2, 3],
        election_timeout: Duration::from_millis(150),
        election_jitter: Duration::ZERO,
        heartbeat_interval: Duration::from_millis(50),
        rpc_timeout: Duration::from_millis(100),
        rpc_max_retries: 0,
        snapshot_threshold: threshold,
        seed_peers: vec![],
    }
}

fn entry(index: u64, term: u64) -> LogEntry {
    LogEntry {
        index,
        term,
        entry_type: EntryType::Command(vec![index as u8]),
    }
}

fn find_send_to(actions: &[Action], peer: u64) -> Option<&RaftMessage> {
    actions.iter().find_map(|a| match a {
        Action::Send { to, msg } if *to == peer => Some(msg),
        _ => None,
    })
}

fn ae(prev: u64, prev_term: u64, entries: Vec<LogEntry>, commit: u64) -> RaftMessage {
    RaftMessage::AppendEntries {
        term: 2,
        leader_id: 2,
        prev_log_index: prev,
        prev_log_term: prev_term,
        entries,
        leader_commit: commit,
    }
}

// ---------- compaction ----------

#[test]
fn follower_emits_snapshot_needed_when_applied_exceeds_threshold() {
    let mut eng = RaftEngine::new(cfg(1, 3), MemoryStorage::new());
    let t0 = Instant::now();
    let a = eng.handle_rpc(2, ae(0, 0, vec![entry(1, 2), entry(2, 2), entry(3, 2)], 3), t0);
    assert!(a.iter().any(|x| matches!(x, Action::SnapshotNeeded { last_applied: 3 })));

    // Not re-emitted while still pending.
    let a = eng.handle_rpc(2, ae(3, 2, vec![entry(4, 2)], 4), t0);
    assert!(!a.iter().any(|x| matches!(x, Action::SnapshotNeeded { .. })));
}

#[test]
fn compact_stores_snapshot_and_truncates_log() {
    let mut eng = RaftEngine::new(cfg(1, 3), MemoryStorage::new());
    let t0 = Instant::now();
    let _ = eng.handle_rpc(
        2,
        ae(0, 0, vec![entry(1, 2), entry(2, 2), entry(3, 2), entry(4, 2)], 3),
        t0,
    );
    eng.compact(3, b"state@3".to_vec()).unwrap();
    let snap = eng.snapshot().unwrap();
    assert_eq!(snap.last_included_index, 3);
    assert_eq!(snap.last_included_term, 2);
    assert_eq!(snap.state_blob, b"state@3");
    assert_eq!(snap.conf.len(), 3);
    assert_eq!(eng.first_index(), 4);
    assert_eq!(eng.last_index(), 4);

    // Cannot compact beyond last_applied.
    assert!(eng.compact(4, vec![]).is_err());
}

#[test]
fn snapshot_needed_reemitted_after_compact_when_threshold_crossed_again() {
    let mut eng = RaftEngine::new(cfg(1, 2), MemoryStorage::new());
    let t0 = Instant::now();
    let a = eng.handle_rpc(2, ae(0, 0, vec![entry(1, 2), entry(2, 2)], 2), t0);
    assert!(a.iter().any(|x| matches!(x, Action::SnapshotNeeded { last_applied: 2 })));
    eng.compact(2, vec![]).unwrap();
    let a = eng.handle_rpc(2, ae(2, 2, vec![entry(3, 2), entry(4, 2)], 4), t0);
    assert!(a.iter().any(|x| matches!(x, Action::SnapshotNeeded { last_applied: 4 })));
}

// ---------- follower: AppendEntries against a compacted log ----------

fn storage_with_snapshot_at_5() -> MemoryStorage {
    let mut st = MemoryStorage::new();
    st.install_snapshot(Snapshot {
        last_included_index: 5,
        last_included_term: 2,
        conf: vec![1, 2, 3],
        state_blob: vec![],
    });
    st
}

#[test]
fn follower_accepts_prev_equal_to_snapshot_index() {
    let mut eng = RaftEngine::new(cfg(1, 100), storage_with_snapshot_at_5());
    let t0 = Instant::now();
    let a = eng.handle_rpc(2, ae(5, 2, vec![entry(6, 2), entry(7, 2)], 7), t0);
    assert!(matches!(
        find_send_to(&a, 2),
        Some(RaftMessage::AppendEntriesResp {
            success: true,
            match_index: 7,
            ..
        })
    ));
    assert!(a
        .iter()
        .any(|x| matches!(x, Action::Apply { entries } if entries.len() == 2)));
}

#[test]
fn follower_skips_entries_covered_by_snapshot() {
    let mut eng = RaftEngine::new(cfg(1, 100), storage_with_snapshot_at_5());
    let t0 = Instant::now();
    // Leader (never compacted) sends from index 1.
    let entries: Vec<_> = (1..=7).map(|i| entry(i, 2)).collect();
    let a = eng.handle_rpc(2, ae(0, 0, entries, 7), t0);
    assert!(matches!(
        find_send_to(&a, 2),
        Some(RaftMessage::AppendEntriesResp {
            success: true,
            match_index: 7,
            ..
        })
    ));
    assert_eq!(eng.first_index(), 6);
    assert_eq!(eng.last_index(), 7);
    assert_eq!(eng.commit_index(), 7);
}

// ---------- leader: chunked InstallSnapshot ----------

/// Leader at term 1: log 1..7 compacted at 5 with a 10-byte blob.
fn leader_with_snapshot(chunk: usize) -> (RaftEngine<MemoryStorage>, Instant) {
    let log: Vec<_> = (1..=7).map(|i| entry(i, 1)).collect();
    let mut st = MemoryStorage::new();
    st.persist(None, &log).unwrap();
    st.install_snapshot(Snapshot {
        last_included_index: 5,
        last_included_term: 1,
        conf: vec![1, 2, 3],
        state_blob: b"0123456789".to_vec(),
    });
    let mut eng = RaftEngine::new(cfg(1, 100), st);
    eng.set_snapshot_chunk_size(chunk);
    let t1 = Instant::now() + Duration::from_millis(200);
    let _ = eng.tick(t1);
    let _ = eng.handle_rpc(
        2,
        RaftMessage::RequestVoteResp {
            term: 1,
            vote_granted: true,
        },
        t1,
    );
    assert_eq!(eng.role(), Role::Leader);
    (eng, t1)
}

/// Follower 3 has empty log: reject heartbeats until leader's next_index drops below first_index.
fn drive_to_snapshot(eng: &mut RaftEngine<MemoryStorage>, t: Instant) -> Vec<Action> {
    let mut actions = eng.tick(t + Duration::from_millis(60)).actions;
    for _ in 0..5 {
        let msg = find_send_to(&actions, 3).cloned().expect("send to 3");
        match msg {
            RaftMessage::AppendEntries { .. } => {
                actions = eng.handle_rpc(
                    3,
                    RaftMessage::AppendEntriesResp {
                        term: 1,
                        success: false,
                        match_index: 0,
                    },
                    t,
                );
            }
            RaftMessage::InstallSnapshot { .. } => return actions,
            other => panic!("unexpected {other:?}"),
        }
    }
    panic!("never switched to InstallSnapshot");
}

#[test]
fn leader_switches_to_install_snapshot_when_next_below_first_index() {
    let (mut eng, t) = leader_with_snapshot(4);
    let actions = drive_to_snapshot(&mut eng, t);
    match find_send_to(&actions, 3) {
        Some(RaftMessage::InstallSnapshot {
            term: 1,
            leader_id: 1,
            last_included_index: 5,
            last_included_term: 1,
            offset: 0,
            data,
            done: false,
        }) => assert_eq!(data, b"0123"),
        other => panic!("expected InstallSnapshot chunk 0, got {other:?}"),
    }
}

#[test]
fn leader_streams_chunks_sequentially_then_resumes_append_entries() {
    let (mut eng, t) = leader_with_snapshot(4);
    let _ = drive_to_snapshot(&mut eng, t);

    let a = eng.handle_rpc(3, RaftMessage::InstallSnapshotResp { term: 1 }, t);
    match find_send_to(&a, 3) {
        Some(RaftMessage::InstallSnapshot {
            offset: 4,
            data,
            done: false,
            ..
        }) => assert_eq!(data, b"4567"),
        other => panic!("expected chunk offset 4, got {other:?}"),
    }
    let a = eng.handle_rpc(3, RaftMessage::InstallSnapshotResp { term: 1 }, t);
    match find_send_to(&a, 3) {
        Some(RaftMessage::InstallSnapshot {
            offset: 8,
            data,
            done: true,
            ..
        }) => assert_eq!(data, b"89"),
        other => panic!("expected final chunk offset 8, got {other:?}"),
    }
    let a = eng.handle_rpc(3, RaftMessage::InstallSnapshotResp { term: 1 }, t);
    assert_eq!(eng.match_index(3), Some(5));
    assert_eq!(eng.next_index(3), Some(6));
    match find_send_to(&a, 3) {
        Some(RaftMessage::AppendEntries {
            prev_log_index: 5,
            prev_log_term: 1,
            entries,
            ..
        }) => assert_eq!(entries.len(), 2),
        other => panic!("expected AE prev=5, got {other:?}"),
    }
}

#[test]
fn leader_restarts_from_offset_zero_on_rpc_failure() {
    let (mut eng, t) = leader_with_snapshot(4);
    let _ = drive_to_snapshot(&mut eng, t);
    let _ = eng.handle_rpc(3, RaftMessage::InstallSnapshotResp { term: 1 }, t); // at offset 4
    let _ = eng.handle_rpc_failure(3, RpcKind::InstallSnapshot);
    let a = eng.tick(t + Duration::from_millis(120));
    match find_send_to(&a.actions, 3) {
        Some(RaftMessage::InstallSnapshot { offset: 0, .. }) => {}
        other => panic!("expected restart at offset 0, got {other:?}"),
    }
}

#[test]
fn leader_does_not_send_second_chunk_while_one_inflight() {
    let (mut eng, t) = leader_with_snapshot(4);
    let _ = drive_to_snapshot(&mut eng, t);
    let a = eng.tick(t + Duration::from_millis(120));
    assert!(find_send_to(&a.actions, 3).is_none());
}

#[test]
fn leader_steps_down_on_higher_term_snapshot_resp() {
    let (mut eng, t) = leader_with_snapshot(4);
    let _ = drive_to_snapshot(&mut eng, t);
    let a = eng.handle_rpc(3, RaftMessage::InstallSnapshotResp { term: 5 }, t);
    assert_eq!(eng.role(), Role::Follower);
    assert!(a
        .iter()
        .any(|x| matches!(x, Action::BecomeFollower { term: 5, .. })));
}

// ---------- follower: chunk assembly ----------

fn chunk(offset: u64, data: &[u8], done: bool) -> RaftMessage {
    RaftMessage::InstallSnapshot {
        term: 2,
        leader_id: 2,
        last_included_index: 5,
        last_included_term: 2,
        offset,
        data: data.to_vec(),
        done,
    }
}

#[test]
fn follower_assembles_chunks_and_installs() {
    let mut eng = RaftEngine::new(cfg(1, 100), MemoryStorage::new());
    let t0 = Instant::now();
    let a = eng.handle_rpc(2, chunk(0, b"abc", false), t0);
    assert!(matches!(
        find_send_to(&a, 2),
        Some(RaftMessage::InstallSnapshotResp { term: 2 })
    ));
    assert!(!a
        .iter()
        .any(|x| matches!(x, Action::SnapshotInstallComplete { .. })));
    let a = eng.handle_rpc(2, chunk(3, b"def", true), t0);
    assert!(a
        .iter()
        .any(|x| matches!(x, Action::SnapshotInstallComplete { index: 5 })));
    let snap = eng.snapshot().unwrap();
    assert_eq!(snap.state_blob, b"abcdef");
    assert_eq!(snap.last_included_index, 5);
    assert_eq!(eng.commit_index(), 5);
    assert_eq!(eng.last_applied(), 5);
    assert_eq!(eng.first_index(), 6);
    assert_eq!(eng.role(), Role::Follower);
    assert_eq!(eng.leader(), Some(2));
    assert_eq!(eng.current_term(), 2);
}

#[test]
fn follower_resets_buffer_when_offset_zero_restarts_transfer() {
    let mut eng = RaftEngine::new(cfg(1, 100), MemoryStorage::new());
    let t0 = Instant::now();
    let _ = eng.handle_rpc(2, chunk(0, b"abc", false), t0);
    let _ = eng.handle_rpc(2, chunk(0, b"xyz", false), t0);
    let _ = eng.handle_rpc(2, chunk(3, b"w", true), t0);
    assert_eq!(eng.snapshot().unwrap().state_blob, b"xyzw");
}

#[test]
fn follower_discards_out_of_order_chunk_and_expects_zero() {
    let mut eng = RaftEngine::new(cfg(1, 100), MemoryStorage::new());
    let t0 = Instant::now();
    let _ = eng.handle_rpc(2, chunk(0, b"abc", false), t0);
    let a = eng.handle_rpc(2, chunk(6, b"zzz", true), t0);
    assert!(!a
        .iter()
        .any(|x| matches!(x, Action::SnapshotInstallComplete { .. })));
    assert!(eng.snapshot().is_none());
    // Continuing at 3 is also rejected (buffer was reset); only offset 0 restarts.
    let _ = eng.handle_rpc(2, chunk(3, b"def", true), t0);
    assert!(eng.snapshot().is_none());
    let _ = eng.handle_rpc(2, chunk(0, b"ok", true), t0);
    assert_eq!(eng.snapshot().unwrap().state_blob, b"ok");
}

#[test]
fn follower_rejects_snapshot_from_stale_term() {
    let mut st = MemoryStorage::new();
    st.persist(
        Some(HardState {
            current_term: 5,
            voted_for: None,
        }),
        &[],
    )
    .unwrap();
    let mut eng = RaftEngine::new(cfg(1, 100), st);
    let t0 = Instant::now();
    let a = eng.handle_rpc(2, chunk(0, b"abc", true), t0);
    assert!(matches!(
        find_send_to(&a, 2),
        Some(RaftMessage::InstallSnapshotResp { term: 5 })
    ));
    assert!(eng.snapshot().is_none());
}

#[test]
fn follower_ignores_snapshot_older_than_commit_index() {
    let mut eng = RaftEngine::new(cfg(1, 100), MemoryStorage::new());
    let t0 = Instant::now();
    let entries: Vec<_> = (1..=7).map(|i| entry(i, 2)).collect();
    let _ = eng.handle_rpc(2, ae(0, 0, entries, 7), t0);
    let a = eng.handle_rpc(2, chunk(0, b"old", true), t0);
    assert!(matches!(
        find_send_to(&a, 2),
        Some(RaftMessage::InstallSnapshotResp { term: 2 })
    ));
    assert!(eng.snapshot().is_none());
    assert_eq!(eng.last_index(), 7);
}

#[test]
fn engine_starts_with_commit_and_applied_at_stored_snapshot() {
    let eng = RaftEngine::new(cfg(1, 100), storage_with_snapshot_at_5());
    assert_eq!(eng.commit_index(), 5);
    assert_eq!(eng.last_applied(), 5);
}
