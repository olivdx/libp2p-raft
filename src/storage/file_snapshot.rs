use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

use crate::error::StorageError;
use crate::raft::types::{HardState, Index, LogEntry, Snapshot, Term};
use crate::storage::{MemoryStorage, Storage};

const SNAPSHOT_FILE: &str = "snapshot.bin";

/// `MemoryStorage` with the snapshot additionally persisted to disk as bincode.
/// Log entries and hard state remain in RAM; on restart the node recovers state
/// up to the snapshot boundary and rejoins for the tail.
#[derive(Debug)]
pub struct FileSnapshotStorage {
    inner: MemoryStorage,
    dir: PathBuf,
}

impl FileSnapshotStorage {
    /// Open (or create) `dir` and load `snapshot.bin` if present.
    /// A file that exists but fails to decode is an error, not a silent reset.
    pub fn new(dir: impl AsRef<Path>) -> Result<Self, StorageError> {
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir).map_err(|e| StorageError::Msg(e.to_string()))?;

        let mut inner = MemoryStorage::new();
        let path = dir.join(SNAPSHOT_FILE);
        if path.exists() {
            let bytes = fs::read(&path).map_err(|e| StorageError::Msg(e.to_string()))?;
            let snap: Snapshot = bincode::deserialize(&bytes)
                .map_err(|e| StorageError::Msg(format!("corrupt {SNAPSHOT_FILE}: {e}")))?;
            inner.install_snapshot(snap);
        }
        Ok(Self { inner, dir })
    }

    /// Write atomically: tmp file + fsync + rename, so a crash mid-write
    /// leaves the previous snapshot intact.
    fn write_to_disk(&self, snap: &Snapshot) -> Result<(), StorageError> {
        let bytes = bincode::serialize(snap).map_err(|e| StorageError::Msg(e.to_string()))?;
        let tmp = self.dir.join(format!("{SNAPSHOT_FILE}.tmp"));
        let io = |e: std::io::Error| StorageError::Msg(e.to_string());
        {
            let mut f = fs::File::create(&tmp).map_err(io)?;
            f.write_all(&bytes).map_err(io)?;
            f.sync_all().map_err(io)?;
        }
        fs::rename(&tmp, self.dir.join(SNAPSHOT_FILE)).map_err(io)
    }
}

impl Storage for FileSnapshotStorage {
    fn hard_state(&self) -> HardState {
        self.inner.hard_state()
    }

    fn first_index(&self) -> Index {
        self.inner.first_index()
    }

    fn entry(&self, index: Index) -> Option<LogEntry> {
        self.inner.entry(index)
    }

    fn last_index_term(&self) -> (Index, Term) {
        self.inner.last_index_term()
    }

    fn truncate_from(&mut self, index: Index) {
        self.inner.truncate_from(index)
    }

    fn snapshot(&self) -> Option<Snapshot> {
        self.inner.snapshot()
    }

    fn install_snapshot(&mut self, snap: Snapshot) {
        let stale = self
            .inner
            .snapshot()
            .is_some_and(|cur| snap.last_included_index <= cur.last_included_index);
        if stale {
            return;
        }
        if let Err(e) = self.write_to_disk(&snap) {
            // Trait can't surface the error; keep RAM consistent and log it.
            tracing::error!("failed to persist snapshot: {e}");
        }
        self.inner.install_snapshot(snap);
    }

    fn persist(
        &mut self,
        hard_state: Option<HardState>,
        entries: &[LogEntry],
    ) -> Result<(), StorageError> {
        self.inner.persist(hard_state, entries)
    }
}
