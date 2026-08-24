use crate::error::StorageError;
use crate::raft::types::{HardState, Index, LogEntry, Snapshot, Term};

pub mod file_snapshot;
pub mod memory;

pub use file_snapshot::FileSnapshotStorage;
pub use memory::MemoryStorage;

pub trait Storage {
    fn hard_state(&self) -> HardState;
    /// First index still present in the log (`snapshot.last_included_index + 1`, or 1).
    fn first_index(&self) -> Index;
    fn entry(&self, index: Index) -> Option<LogEntry>;
    fn last_index_term(&self) -> (Index, Term);
    fn truncate_from(&mut self, index: Index);
    fn snapshot(&self) -> Option<Snapshot>;
    /// Store `snap` and compact the log prefix up to `last_included_index`.
    /// Stale snapshots (index <= current snapshot) are ignored.
    fn install_snapshot(&mut self, snap: Snapshot);
    fn persist(
        &mut self,
        hard_state: Option<HardState>,
        entries: &[LogEntry],
    ) -> Result<(), StorageError>;
}
