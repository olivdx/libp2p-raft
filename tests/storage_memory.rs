use libp2p_raft::raft::types::{EntryType, HardState, LogEntry};
use libp2p_raft::storage::{MemoryStorage, Storage};

#[test]
fn persist_hard_state_and_entries_atomically() {
    let mut s = MemoryStorage::new();
    let hs = HardState {
        current_term: 3,
        voted_for: Some(1),
    };
    let e = LogEntry {
        index: 1,
        term: 3,
        entry_type: EntryType::Command(vec![1, 2, 3]),
    };
    s.persist(Some(hs.clone()), &[e.clone()]).unwrap();
    assert_eq!(s.hard_state(), hs);
    assert_eq!(s.entry(1).unwrap(), e);
}

#[test]
fn truncate_from_removes_suffix() {
    let mut s = MemoryStorage::new();
    let entries = vec![
        LogEntry {
            index: 1,
            term: 1,
            entry_type: EntryType::Command(vec![]),
        },
        LogEntry {
            index: 2,
            term: 1,
            entry_type: EntryType::Command(vec![]),
        },
    ];
    s.persist(None, &entries).unwrap();
    s.truncate_from(2);
    assert!(s.entry(2).is_none());
    assert!(s.entry(1).is_some());
}

fn e(index: u64, term: u64) -> LogEntry {
    LogEntry {
        index,
        term,
        entry_type: EntryType::Command(vec![index as u8]),
    }
}

fn snap(index: u64, term: u64) -> libp2p_raft::raft::types::Snapshot {
    libp2p_raft::raft::types::Snapshot {
        last_included_index: index,
        last_included_term: term,
        conf: vec![1, 2, 3],
        state_blob: vec![9],
    }
}

#[test]
fn install_snapshot_compacts_prefix_and_keeps_suffix() {
    let mut s = MemoryStorage::new();
    s.persist(None, &[e(1, 1), e(2, 1), e(3, 2), e(4, 2)]).unwrap();
    s.install_snapshot(snap(2, 1));
    assert_eq!(s.first_index(), 3);
    assert!(s.entry(1).is_none());
    assert!(s.entry(2).is_none());
    assert_eq!(s.entry(3).unwrap(), e(3, 2));
    assert_eq!(s.last_index_term(), (4, 2));
    assert_eq!(s.snapshot().unwrap().last_included_index, 2);
}

#[test]
fn install_snapshot_with_conflicting_log_discards_all() {
    let mut s = MemoryStorage::new();
    s.persist(None, &[e(1, 1), e(2, 1), e(3, 1)]).unwrap();
    s.install_snapshot(snap(2, 2));
    assert_eq!(s.first_index(), 3);
    assert!(s.entry(3).is_none());
    assert_eq!(s.last_index_term(), (2, 2));
}

#[test]
fn install_snapshot_beyond_log_resets_log() {
    let mut s = MemoryStorage::new();
    s.persist(None, &[e(1, 1)]).unwrap();
    s.install_snapshot(snap(5, 3));
    assert_eq!(s.first_index(), 6);
    assert_eq!(s.last_index_term(), (5, 3));
    s.persist(None, &[e(6, 3)]).unwrap();
    assert_eq!(s.entry(6).unwrap(), e(6, 3));
    assert_eq!(s.last_index_term(), (6, 3));
    assert!(s.persist(None, &[e(2, 3)]).is_err());
}

#[test]
fn truncate_from_respects_offset() {
    let mut s = MemoryStorage::new();
    s.persist(None, &[e(1, 1), e(2, 1), e(3, 1), e(4, 1)]).unwrap();
    s.install_snapshot(snap(2, 1));
    s.truncate_from(4);
    assert_eq!(s.last_index_term(), (3, 1));
    s.truncate_from(1);
    assert_eq!(s.last_index_term(), (2, 1));
    assert_eq!(s.first_index(), 3);
}

#[test]
fn stale_snapshot_is_ignored() {
    let mut s = MemoryStorage::new();
    s.persist(None, &[e(1, 1), e(2, 1), e(3, 1)]).unwrap();
    s.install_snapshot(snap(3, 1));
    s.install_snapshot(snap(2, 1));
    assert_eq!(s.snapshot().unwrap().last_included_index, 3);
    assert_eq!(s.first_index(), 4);
}
