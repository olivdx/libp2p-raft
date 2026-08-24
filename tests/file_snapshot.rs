//! FileSnapshotStorage: snapshot persisted to disk as bincode, log/hard_state in RAM.

use libp2p_raft::raft::types::{EntryType, LogEntry, Snapshot};
use libp2p_raft::storage::{FileSnapshotStorage, Storage};
use tempfile::TempDir;

fn sample_snapshot(index: u64) -> Snapshot {
    Snapshot {
        last_included_index: index,
        last_included_term: 1,
        conf: vec![1, 2, 3],
        state_blob: vec![0xAA, 0xBB, 0xCC],
    }
}

#[test]
fn fresh_dir_has_no_snapshot() {
    let dir = TempDir::new().unwrap();
    let store = FileSnapshotStorage::new(dir.path()).unwrap();
    assert_eq!(store.snapshot(), None);
    assert_eq!(store.first_index(), 1);
}

#[test]
fn snapshot_survives_restart() {
    let dir = TempDir::new().unwrap();
    let snap = sample_snapshot(5);
    {
        let mut store = FileSnapshotStorage::new(dir.path()).unwrap();
        store.install_snapshot(snap.clone());
    }
    let store = FileSnapshotStorage::new(dir.path()).unwrap();
    assert_eq!(store.snapshot(), Some(snap));
    assert_eq!(store.first_index(), 6);
}

#[test]
fn stale_snapshot_is_not_persisted() {
    let dir = TempDir::new().unwrap();
    {
        let mut store = FileSnapshotStorage::new(dir.path()).unwrap();
        store.install_snapshot(sample_snapshot(5));
        store.install_snapshot(sample_snapshot(3)); // stale: must be ignored
    }
    let store = FileSnapshotStorage::new(dir.path()).unwrap();
    assert_eq!(store.snapshot().unwrap().last_included_index, 5);
}

#[test]
fn corrupt_file_is_an_error() {
    let dir = TempDir::new().unwrap();
    std::fs::write(dir.path().join("snapshot.bin"), b"not bincode at all").unwrap();
    assert!(FileSnapshotStorage::new(dir.path()).is_err());
}

#[test]
fn no_tmp_file_left_behind() {
    let dir = TempDir::new().unwrap();
    let mut store = FileSnapshotStorage::new(dir.path()).unwrap();
    store.install_snapshot(sample_snapshot(5));
    assert!(dir.path().join("snapshot.bin").exists());
    assert!(!dir.path().join("snapshot.bin.tmp").exists());
}

#[test]
fn log_operations_delegate_to_memory() {
    let dir = TempDir::new().unwrap();
    let mut store = FileSnapshotStorage::new(dir.path()).unwrap();
    let entries: Vec<LogEntry> = (1..=7u64)
        .map(|i| LogEntry {
            index: i,
            term: 1,
            entry_type: EntryType::Command(vec![i as u8]),
        })
        .collect();
    store.persist(None, &entries).unwrap();
    assert_eq!(store.last_index_term(), (7, 1));

    store.install_snapshot(sample_snapshot(5));
    assert_eq!(store.first_index(), 6);
    assert!(store.entry(5).is_none());
    assert!(store.entry(6).is_some());
}
