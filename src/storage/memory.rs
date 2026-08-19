use crate::error::StorageError;
use crate::raft::types::{HardState, Index, LogEntry, Snapshot, Term};
use crate::storage::Storage;

/// In-memory storage. After compaction the log is stored as a suffix starting at
/// `first_index = snapshot.last_included_index + 1`.
#[derive(Debug, Clone)]
pub struct MemoryStorage {
    hard_state: HardState,
    /// Log entries `first_index..`, contiguous.
    entries: Vec<LogEntry>,
    snapshot: Option<Snapshot>,
}

impl MemoryStorage {
    pub fn new() -> Self {
        Self {
            hard_state: HardState::default(),
            entries: Vec::new(),
            snapshot: None,
        }
    }

    fn snap_index_term(&self) -> (Index, Term) {
        self.snapshot
            .as_ref()
            .map(|s| (s.last_included_index, s.last_included_term))
            .unwrap_or((0, 0))
    }

    fn slot(&self, index: Index) -> Option<usize> {
        let first = self.first_index();
        if index < first {
            return None;
        }
        Some((index - first) as usize)
    }
}

impl Default for MemoryStorage {
    fn default() -> Self {
        Self::new()
    }
}

impl Storage for MemoryStorage {
    fn hard_state(&self) -> HardState {
        self.hard_state.clone()
    }

    fn first_index(&self) -> Index {
        self.snap_index_term().0 + 1
    }

    fn entry(&self, index: Index) -> Option<LogEntry> {
        if index == 0 {
            return None;
        }
        let slot = self.slot(index)?;
        self.entries.get(slot).cloned()
    }

    fn last_index_term(&self) -> (Index, Term) {
        match self.entries.last() {
            Some(e) => (e.index, e.term),
            None => self.snap_index_term(),
        }
    }

    fn truncate_from(&mut self, index: Index) {
        match self.slot(index) {
            Some(keep) if keep < self.entries.len() => self.entries.truncate(keep),
            Some(_) => {}
            None => self.entries.clear(),
        }
    }

    fn snapshot(&self) -> Option<Snapshot> {
        self.snapshot.clone()
    }

    fn install_snapshot(&mut self, snap: Snapshot) {
        let (cur_idx, _) = self.snap_index_term();
        if snap.last_included_index <= cur_idx {
            return; // stale
        }
        // Raft §7: if the log has the entry at last_included_index with matching term,
        // retain the suffix; otherwise discard the entire log.
        let retain_suffix = self
            .entry(snap.last_included_index)
            .map(|e| e.term == snap.last_included_term)
            .unwrap_or(false);
        if retain_suffix {
            let drop = self
                .slot(snap.last_included_index)
                .map(|s| s + 1)
                .unwrap_or(0);
            self.entries.drain(..drop);
        } else {
            self.entries.clear();
        }
        self.snapshot = Some(snap);
    }

    fn persist(
        &mut self,
        hard_state: Option<HardState>,
        entries: &[LogEntry],
    ) -> Result<(), StorageError> {
        if let Some(hs) = hard_state {
            self.hard_state = hs;
        }

        for entry in entries {
            if entry.index == 0 {
                return Err(StorageError::Msg("log index must be >= 1".into()));
            }
            let Some(slot) = self.slot(entry.index) else {
                return Err(StorageError::Msg(format!(
                    "append at index {} below first_index {}",
                    entry.index,
                    self.first_index()
                )));
            };
            if slot == self.entries.len() {
                self.entries.push(entry.clone());
            } else if slot < self.entries.len() {
                self.entries[slot] = entry.clone();
                self.entries.truncate(slot + 1);
            } else {
                return Err(StorageError::Msg(format!(
                    "non-contiguous append at index {}",
                    entry.index
                )));
            }
        }

        Ok(())
    }
}
