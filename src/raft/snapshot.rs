//! Snapshot compaction + chunk assembly helpers.
//!
//! Constraint: on transfer failure, restart from offset 0 (no resume).

use crate::raft::types::{Index, Snapshot, Term};

/// Default chunk size for leader → follower `InstallSnapshot` transfers.
pub const DEFAULT_SNAPSHOT_CHUNK_SIZE: usize = 64 * 1024;

/// Leader-side per-follower transfer state (pipeline depth 1).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutboundTransfer {
    /// Snapshot index being transferred; a newer local snapshot restarts from 0.
    pub last_included_index: Index,
    /// Next byte offset to send.
    pub offset: u64,
    /// Length of the chunk currently in flight (advances `offset` on ack).
    pub inflight_len: Option<u64>,
    /// Whether the in-flight chunk was the final one.
    pub inflight_done: bool,
}

impl OutboundTransfer {
    pub fn new(last_included_index: Index) -> Self {
        Self {
            last_included_index,
            offset: 0,
            inflight_len: None,
            inflight_done: false,
        }
    }

    /// Slice the next chunk out of `blob`. Returns `(offset, data, done)`.
    pub fn next_chunk<'a>(&self, blob: &'a [u8], chunk_size: usize) -> (u64, &'a [u8], bool) {
        let start = (self.offset as usize).min(blob.len());
        let end = (start + chunk_size.max(1)).min(blob.len());
        (self.offset, &blob[start..end], end >= blob.len())
    }
}

/// Follower-side chunk assembly buffer.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct InboundAssembly {
    last_included_index: Index,
    last_included_term: Term,
    expected_offset: u64,
    buf: Vec<u8>,
    active: bool,
}

pub enum AssembleOutcome {
    /// Chunk accepted; more to come.
    Partial,
    /// Final chunk accepted; assembled blob returned.
    Complete(Vec<u8>),
    /// Chunk out of order / mismatched; buffer reset, expecting offset 0.
    Rejected,
}

impl InboundAssembly {
    pub fn reset(&mut self) {
        *self = Self::default();
    }

    pub fn accept(
        &mut self,
        last_included_index: Index,
        last_included_term: Term,
        offset: u64,
        data: &[u8],
        done: bool,
    ) -> AssembleOutcome {
        if offset == 0 {
            // New transfer (or leader restarted from 0): drop anything buffered.
            self.reset();
            self.active = true;
            self.last_included_index = last_included_index;
            self.last_included_term = last_included_term;
        } else if !self.active
            || offset != self.expected_offset
            || last_included_index != self.last_included_index
            || last_included_term != self.last_included_term
        {
            self.reset();
            return AssembleOutcome::Rejected;
        }

        self.buf.extend_from_slice(data);
        self.expected_offset += data.len() as u64;

        if done {
            let blob = std::mem::take(&mut self.buf);
            self.reset();
            AssembleOutcome::Complete(blob)
        } else {
            AssembleOutcome::Partial
        }
    }
}

/// True when the log applied since the last snapshot exceeds `threshold`.
pub fn should_snapshot(last_applied: Index, snapshot: Option<&Snapshot>, threshold: u64) -> bool {
    let base = snapshot.map(|s| s.last_included_index).unwrap_or(0);
    threshold > 0 && last_applied.saturating_sub(base) >= threshold
}
