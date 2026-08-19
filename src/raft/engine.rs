use std::collections::{HashMap, HashSet};
use std::time::Instant;

use crate::config::RaftConfig;
use crate::error::RaftError;
use crate::protocol::messages::RaftMessage;
use crate::raft::log::log_is_up_to_date;
use crate::raft::membership::Membership;
use crate::raft::snapshot::{
    should_snapshot, AssembleOutcome, InboundAssembly, OutboundTransfer,
    DEFAULT_SNAPSHOT_CHUNK_SIZE,
};
use crate::raft::types::{EntryType, HardState, Index, LogEntry, NodeId, Role, Snapshot, Term};
use crate::storage::Storage;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RpcKind {
    RequestVote,
    AppendEntries,
    InstallSnapshot,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Action {
    Send { to: NodeId, msg: RaftMessage },
    Broadcast { msg: RaftMessage },
    Apply { entries: Vec<LogEntry> },
    BecomeLeader { term: Term },
    BecomeFollower { term: Term, leader: Option<NodeId> },
    BecomeCandidate { term: Term },
    /// Applied log has grown past `snapshot_threshold` since the last snapshot.
    /// The application should serialize its state machine (as of `last_applied`)
    /// and call `RaftEngine::compact(last_applied, blob)`.
    SnapshotNeeded { last_applied: Index },
    /// A leader snapshot was fully received and installed at `index`; the
    /// application must reset its state machine from `RaftEngine::snapshot()`.
    SnapshotInstallComplete { index: Index },
}

pub struct TickOutcome {
    pub actions: Vec<Action>,
    pub next_deadline: Instant,
}

pub struct RaftEngine<S: Storage> {
    config: RaftConfig,
    storage: S,
    role: Role,
    leader: Option<NodeId>,
    votes: HashSet<NodeId>,
    election_deadline: Instant,
    heartbeat_deadline: Instant,
    membership: Membership,
    next_index: HashMap<NodeId, Index>,
    match_index: HashMap<NodeId, Index>,
    /// Last outbound AE per peer: (prev_log_index, entries_len) for stale-resp guards.
    last_ae: HashMap<NodeId, (Index, usize)>,
    /// Pipeline depth 1: do not send another AE to a peer until its resp arrives.
    ae_inflight: HashSet<NodeId>,
    commit_index: Index,
    last_applied: Index,
    /// Leader -> follower snapshot transfers in progress.
    snap_out: HashMap<NodeId, OutboundTransfer>,
    /// Follower-side chunk assembly.
    snap_in: InboundAssembly,
    /// `SnapshotNeeded` already emitted and not yet answered by `compact`.
    snapshot_requested: bool,
    snapshot_chunk_size: usize,
}

impl<S: Storage> RaftEngine<S> {
    pub fn new(config: RaftConfig, storage: S) -> Self {
        let now = Instant::now();
        let membership = Membership::new(config.voters.iter().copied());
        // Everything up to a stored snapshot is committed and applied by definition.
        let snap_index = storage.first_index() - 1;

        Self {
            election_deadline: now + config.election_timeout + config.election_jitter,
            heartbeat_deadline: now + config.heartbeat_interval,
            config,
            storage,
            role: Role::Follower,
            leader: None,
            votes: HashSet::new(),
            membership,
            next_index: HashMap::new(),
            match_index: HashMap::new(),
            last_ae: HashMap::new(),
            ae_inflight: HashSet::new(),
            commit_index: snap_index,
            last_applied: snap_index,
            snap_out: HashMap::new(),
            snap_in: InboundAssembly::default(),
            snapshot_requested: false,
            snapshot_chunk_size: DEFAULT_SNAPSHOT_CHUNK_SIZE,
        }
    }

    /// Override the `InstallSnapshot` chunk size (bytes).
    pub fn set_snapshot_chunk_size(&mut self, bytes: usize) {
        self.snapshot_chunk_size = bytes.max(1);
    }

    /// Current snapshot held in storage, if any.
    pub fn snapshot(&self) -> Option<Snapshot> {
        self.storage.snapshot()
    }

    pub fn first_index(&self) -> Index {
        self.storage.first_index()
    }

    pub fn last_index(&self) -> Index {
        self.storage.last_index_term().0
    }

    /// Create a snapshot of the state machine as of `index` (must be
    /// `<= last_applied`) and compact the log prefix up to `index`.
    pub fn compact(&mut self, index: Index, state_blob: Vec<u8>) -> Result<(), RaftError> {
        if index == 0 || index > self.last_applied {
            return Err(RaftError::InvalidSnapshotIndex);
        }
        let Some(term) = self.term_at(index) else {
            return Err(RaftError::InvalidSnapshotIndex);
        };
        let mut conf: Vec<NodeId> = self.membership.voters().iter().copied().collect();
        conf.sort_unstable();
        self.storage.install_snapshot(Snapshot {
            last_included_index: index,
            last_included_term: term,
            conf,
            state_blob,
        });
        self.snapshot_requested = false;
        Ok(())
    }

    /// Term of the entry at `index`, consulting the snapshot boundary.
    fn term_at(&self, index: Index) -> Option<Term> {
        if index == 0 {
            return Some(0);
        }
        if let Some(e) = self.storage.entry(index) {
            return Some(e.term);
        }
        match self.storage.snapshot() {
            Some(s) if s.last_included_index == index => Some(s.last_included_term),
            _ => None,
        }
    }

    fn maybe_request_snapshot(&mut self, actions: &mut Vec<Action>) {
        if self.snapshot_requested {
            return;
        }
        let snap = self.storage.snapshot();
        if should_snapshot(self.last_applied, snap.as_ref(), self.config.snapshot_threshold) {
            self.snapshot_requested = true;
            actions.push(Action::SnapshotNeeded {
                last_applied: self.last_applied,
            });
        }
    }

    /// Append a command on the Leader and return replicate Actions.
    pub fn propose(&mut self, data: Vec<u8>) -> Result<(Index, Vec<Action>), RaftError> {
        if self.role != Role::Leader {
            return Err(RaftError::NotLeader);
        }
        let (last, _) = self.storage.last_index_term();
        let index = last + 1;
        let entry = LogEntry {
            index,
            term: self.current_term(),
            entry_type: EntryType::Command(data),
        };
        let _ = self.storage.persist(None, &[entry]);
        Ok((index, self.replicate_actions()))
    }

    pub fn next_index(&self, peer: NodeId) -> Option<Index> {
        self.next_index.get(&peer).copied()
    }

    pub fn match_index(&self, peer: NodeId) -> Option<Index> {
        self.match_index.get(&peer).copied()
    }

    pub fn role(&self) -> Role {
        self.role
    }

    pub fn current_term(&self) -> Term {
        self.storage.hard_state().current_term
    }

    pub fn leader(&self) -> Option<NodeId> {
        self.leader
    }

    pub fn node_id(&self) -> NodeId {
        self.config.node_id
    }

    pub fn other_voters(&self) -> Vec<NodeId> {
        self.membership.other_voters(self.config.node_id)
    }

    pub fn commit_index(&self) -> Index {
        self.commit_index
    }

    pub fn last_applied(&self) -> Index {
        self.last_applied
    }

    pub fn next_deadline(&self) -> Instant {
        match self.role {
            Role::Leader => self.heartbeat_deadline.min(self.election_deadline),
            Role::Follower | Role::Candidate => self.election_deadline,
        }
    }

    pub fn tick(&mut self, now: Instant) -> TickOutcome {
        let mut actions = Vec::new();

        if now < self.next_deadline() {
            return TickOutcome {
                actions,
                next_deadline: self.next_deadline(),
            };
        }

        match self.role {
            Role::Follower | Role::Candidate if now >= self.election_deadline => {
                actions.extend(self.start_election(now));
            }
            Role::Leader if now >= self.heartbeat_deadline => {
                actions.extend(self.replicate_actions());
                self.heartbeat_deadline = now + self.config.heartbeat_interval;
            }
            _ => {}
        }

        TickOutcome {
            actions,
            next_deadline: self.next_deadline(),
        }
    }

    pub fn handle_rpc(&mut self, from: NodeId, msg: RaftMessage, now: Instant) -> Vec<Action> {
        match msg {
            RaftMessage::RequestVote {
                term,
                candidate_id,
                last_log_index,
                last_log_term,
            } => self.handle_request_vote(
                from,
                term,
                candidate_id,
                last_log_index,
                last_log_term,
                now,
            ),
            RaftMessage::RequestVoteResp { term, vote_granted } => {
                self.handle_request_vote_resp(from, term, vote_granted, now)
            }
            RaftMessage::AppendEntries {
                term,
                leader_id,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit,
            } => self.handle_append_entries(
                from,
                term,
                leader_id,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit,
                now,
            ),
            RaftMessage::AppendEntriesResp {
                term,
                success,
                match_index,
            } => self.handle_append_entries_resp(from, term, success, match_index, now),
            RaftMessage::InstallSnapshot {
                term,
                leader_id,
                last_included_index,
                last_included_term,
                offset,
                data,
                done,
            } => self.handle_install_snapshot(
                from,
                term,
                leader_id,
                last_included_index,
                last_included_term,
                offset,
                data,
                done,
                now,
            ),
            RaftMessage::InstallSnapshotResp { term } => {
                self.handle_install_snapshot_resp(from, term)
            }
        }
    }

    pub fn handle_rpc_failure(&mut self, to: NodeId, kind: RpcKind) -> Vec<Action> {
        match kind {
            RpcKind::AppendEntries => {
                self.ae_inflight.remove(&to);
            }
            RpcKind::InstallSnapshot => {
                // No resume: next attempt restarts from offset 0.
                self.snap_out.remove(&to);
            }
            RpcKind::RequestVote => {}
        }
        Vec::new()
    }

    fn start_election(&mut self, now: Instant) -> Vec<Action> {
        let term = self.current_term() + 1;
        let mut actions = Vec::new();

        self.become_candidate(term, &mut actions);
        self.reset_election_deadline(now);

        let (last_log_index, last_log_term) = self.storage.last_index_term();
        actions.push(Action::Broadcast {
            msg: RaftMessage::RequestVote {
                term,
                candidate_id: self.config.node_id,
                last_log_index,
                last_log_term,
            },
        });

        actions
    }

    fn become_candidate(&mut self, term: Term, actions: &mut Vec<Action>) {
        self.role = Role::Candidate;
        self.leader = None;
        self.votes.clear();
        self.votes.insert(self.config.node_id);

        let hs = HardState {
            current_term: term,
            voted_for: Some(self.config.node_id),
        };
        let _ = self.storage.persist(Some(hs), &[]);

        actions.push(Action::BecomeCandidate { term });
    }

    fn become_follower(&mut self, term: Term, leader: Option<NodeId>, actions: &mut Vec<Action>) {
        if term > self.current_term() {
            let hs = HardState {
                current_term: term,
                voted_for: None,
            };
            let _ = self.storage.persist(Some(hs), &[]);
        }

        self.role = Role::Follower;
        self.leader = leader;
        self.votes.clear();
        self.next_index.clear();
        self.match_index.clear();
        self.last_ae.clear();
        self.ae_inflight.clear();
        self.snap_out.clear();

        actions.push(Action::BecomeFollower { term, leader });
    }

    fn become_leader(&mut self, term: Term, now: Instant, actions: &mut Vec<Action>) {
        self.role = Role::Leader;
        self.leader = Some(self.config.node_id);
        self.votes.clear();

        let (last_index, _) = self.storage.last_index_term();
        self.next_index.clear();
        self.match_index.clear();
        self.last_ae.clear();
        self.ae_inflight.clear();
        self.snap_out.clear();
        self.snap_in.reset();
        for voter in self.membership.voters() {
            if *voter != self.config.node_id {
                self.next_index.insert(*voter, last_index + 1);
                self.match_index.insert(*voter, 0);
            }
        }

        self.heartbeat_deadline = now + self.config.heartbeat_interval;
        self.reset_election_deadline(now);

        actions.push(Action::BecomeLeader { term });
        // Replication starts on propose / heartbeat tick (pipeline depth 1).
    }

    fn reset_election_deadline(&mut self, now: Instant) {
        self.election_deadline = now + self.config.election_timeout + self.config.election_jitter;
    }

    fn handle_request_vote(
        &mut self,
        from: NodeId,
        term: Term,
        candidate_id: NodeId,
        last_log_index: Index,
        last_log_term: Term,
        now: Instant,
    ) -> Vec<Action> {
        let mut actions = Vec::new();
        let current_term = self.current_term();

        if term < current_term {
            actions.push(Action::Send {
                to: from,
                msg: RaftMessage::RequestVoteResp {
                    term: current_term,
                    vote_granted: false,
                },
            });
            return actions;
        }

        if term > current_term {
            self.become_follower(term, None, &mut actions);
        }

        let hs = self.storage.hard_state();
        let mut vote_granted = false;

        let can_vote = hs.voted_for.is_none() || hs.voted_for == Some(candidate_id);
        if can_vote && log_is_up_to_date(&self.storage, last_log_index, last_log_term) {
            let new_hs = HardState {
                current_term: term.max(self.current_term()),
                voted_for: Some(candidate_id),
            };
            let _ = self.storage.persist(Some(new_hs), &[]);
            vote_granted = true;
            self.role = Role::Follower;
            self.leader = None;
            self.votes.clear();
            self.reset_election_deadline(now);
        }

        actions.push(Action::Send {
            to: from,
            msg: RaftMessage::RequestVoteResp {
                term: self.current_term(),
                vote_granted,
            },
        });

        actions
    }

    fn handle_request_vote_resp(
        &mut self,
        from: NodeId,
        term: Term,
        vote_granted: bool,
        now: Instant,
    ) -> Vec<Action> {
        let mut actions = Vec::new();
        let current_term = self.current_term();

        if term < current_term {
            return actions;
        }

        if term > current_term {
            self.become_follower(term, None, &mut actions);
            return actions;
        }

        if self.role != Role::Candidate || !vote_granted {
            return actions;
        }

        self.votes.insert(from);
        if self.votes.len() >= self.membership.quorum() {
            self.become_leader(term, now, &mut actions);
        }

        actions
    }

    fn handle_append_entries(
        &mut self,
        from: NodeId,
        term: Term,
        leader_id: NodeId,
        prev_log_index: Index,
        prev_log_term: Term,
        entries: Vec<LogEntry>,
        leader_commit: Index,
        now: Instant,
    ) -> Vec<Action> {
        let mut actions = Vec::new();
        let current_term = self.current_term();
        let (last_index, _) = self.storage.last_index_term();

        if term < current_term {
            actions.push(Action::Send {
                to: from,
                msg: RaftMessage::AppendEntriesResp {
                    term: current_term,
                    success: false,
                    match_index: last_index,
                },
            });
            return actions;
        }

        if term > current_term {
            self.become_follower(term, Some(leader_id), &mut actions);
        } else if self.role != Role::Follower {
            self.become_follower(term, Some(leader_id), &mut actions);
        } else {
            self.leader = Some(leader_id);
        }

        // Legitimate leader contact: reset deadline even if prev_log fails.
        self.reset_election_deadline(now);

        // Entries at or below the snapshot index are committed and already
        // reflected in the snapshot; a prev inside that range matches by definition.
        let snap_index = self.storage.first_index() - 1;
        let prev_ok = if prev_log_index == 0 {
            prev_log_term == 0
        } else if prev_log_index < snap_index {
            true
        } else {
            self.term_at(prev_log_index)
                .map(|t| t == prev_log_term)
                .unwrap_or(false)
        };

        if !prev_ok {
            let (last_index, _) = self.storage.last_index_term();
            actions.push(Action::Send {
                to: from,
                msg: RaftMessage::AppendEntriesResp {
                    term: self.current_term(),
                    success: false,
                    match_index: last_index,
                },
            });
            return actions;
        }

        // Conditional truncate + append: skip identical prefix; truncate only on conflict.
        let mut append_from = 0usize;
        for (i, e) in entries.iter().enumerate() {
            let idx = prev_log_index + 1 + i as Index;
            if idx <= snap_index {
                // Covered by snapshot; nothing to append.
                append_from = i + 1;
                continue;
            }
            match self.storage.entry(idx) {
                Some(existing) if existing.term == e.term => {
                    append_from = i + 1;
                }
                Some(_) => {
                    // Never truncate committed entries (safety net).
                    debug_assert!(idx > self.commit_index);
                    self.storage.truncate_from(idx);
                    append_from = i;
                    break;
                }
                None => {
                    append_from = i;
                    break;
                }
            }
        }

        // Durability barrier (MemoryStorage is sync): persist before success response.
        if append_from < entries.len() {
            let _ = self.storage.persist(None, &entries[append_from..]);
        }

        // last_new = index of last new entry in this RPC (Raft Fig. 2).
        let last_new = prev_log_index + entries.len() as Index;
        // commit_index is monotonic — stale/reordered AE must not regress it.
        self.advance_commit_and_apply(leader_commit, last_new, &mut actions);

        actions.push(Action::Send {
            to: from,
            msg: RaftMessage::AppendEntriesResp {
                term: self.current_term(),
                success: true,
                match_index: last_new,
            },
        });

        actions
    }

    fn advance_commit_and_apply(
        &mut self,
        leader_commit: Index,
        last_new: Index,
        actions: &mut Vec<Action>,
    ) {
        let new_commit = leader_commit.min(last_new);
        if new_commit > self.commit_index {
            self.commit_index = new_commit;
        }
        if self.commit_index <= self.last_applied {
            return;
        }
        let mut entries = Vec::new();
        for i in (self.last_applied + 1)..=self.commit_index {
            if let Some(e) = self.storage.entry(i) {
                entries.push(e);
            }
        }
        self.last_applied = self.commit_index;
        if !entries.is_empty() {
            actions.push(Action::Apply { entries });
        }
        self.maybe_request_snapshot(actions);
    }

    fn handle_append_entries_resp(
        &mut self,
        from: NodeId,
        term: Term,
        success: bool,
        _match_index: Index,
        _now: Instant,
    ) -> Vec<Action> {
        let mut actions = Vec::new();
        let current_term = self.current_term();

        if term > current_term {
            self.become_follower(term, None, &mut actions);
            return actions;
        }
        if self.role != Role::Leader || term != current_term {
            return actions;
        }

        let Some(&(req_prev, req_len)) = self.last_ae.get(&from) else {
            return actions;
        };
        self.ae_inflight.remove(&from);

        if success {
            // With pipeline depth 1, last_ae uniquely identifies this RPC.
            let matched = req_prev + req_len as Index;
            let cur = self.match_index.get(&from).copied().unwrap_or(0);
            // match_index is monotonic — stale success must not regress.
            self.match_index.insert(from, cur.max(matched));
            // Invariant: next_index == match_index + 1 after success.
            self.next_index.insert(from, matched + 1);
            self.maybe_commit(&mut actions);
            // Continue catching up only if log still has unsent entries.
            let next = self.next_index.get(&from).copied().unwrap_or(1);
            let (last, _) = self.storage.last_index_term();
            if next <= last {
                if let Some(action) = self.send_append_entries_to(from) {
                    actions.push(action);
                }
            }
        } else {
            let next = self.next_index.get(&from).copied().unwrap_or(1);
            // Stale reject: only apply if next_index still matches this request.
            if next != req_prev + 1 {
                return actions;
            }
            let match_i = self.match_index.get(&from).copied().unwrap_or(0);
            let new_next = next.saturating_sub(1).max(match_i + 1).max(1);
            self.next_index.insert(from, new_next);
            if let Some(action) = self.send_append_entries_to(from) {
                actions.push(action);
            }
        }

        actions
    }

    fn replicate_actions(&mut self) -> Vec<Action> {
        let mut actions = Vec::new();
        for peer in self.other_voters() {
            if let Some(action) = self.send_append_entries_to(peer) {
                actions.push(action);
            }
        }
        actions
    }

    fn send_append_entries_to(&mut self, to: NodeId) -> Option<Action> {
        if self.ae_inflight.contains(&to) {
            return None;
        }
        // Follower needs entries we have compacted away -> ship the snapshot.
        let next = *self.next_index.get(&to)?;
        if next < self.storage.first_index() {
            return self.send_snapshot_chunk_to(to);
        }
        let (msg, prev, len) = self.build_append_entries(to)?;
        self.last_ae.insert(to, (prev, len));
        self.ae_inflight.insert(to);
        Some(Action::Send { to, msg })
    }

    fn build_append_entries(&self, to: NodeId) -> Option<(RaftMessage, Index, usize)> {
        let next = *self.next_index.get(&to)?;
        let prev_log_index = next.saturating_sub(1);
        let prev_log_term = self.term_at(prev_log_index)?;
        let (last_index, _) = self.storage.last_index_term();
        let mut entries = Vec::new();
        let mut idx = next;
        while idx <= last_index {
            entries.push(self.storage.entry(idx)?);
            idx += 1;
        }
        let entries_len = entries.len();
        Some((
            RaftMessage::AppendEntries {
                term: self.current_term(),
                leader_id: self.config.node_id,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit: self.commit_index,
            },
            prev_log_index,
            entries_len,
        ))
    }

    /// Leader: send the next snapshot chunk to `to` (one chunk in flight per peer).
    fn send_snapshot_chunk_to(&mut self, to: NodeId) -> Option<Action> {
        let snap = self.storage.snapshot()?;
        let transfer = self
            .snap_out
            .entry(to)
            .or_insert_with(|| OutboundTransfer::new(snap.last_included_index));
        if transfer.inflight_len.is_some() {
            return None;
        }
        if transfer.last_included_index != snap.last_included_index {
            // Local snapshot advanced mid-transfer: restart from 0.
            *transfer = OutboundTransfer::new(snap.last_included_index);
        }
        let (offset, data, done) = transfer.next_chunk(&snap.state_blob, self.snapshot_chunk_size);
        transfer.inflight_len = Some(data.len() as u64);
        transfer.inflight_done = done;
        Some(Action::Send {
            to,
            msg: RaftMessage::InstallSnapshot {
                term: self.current_term(),
                leader_id: self.config.node_id,
                last_included_index: snap.last_included_index,
                last_included_term: snap.last_included_term,
                offset,
                data: data.to_vec(),
                done,
            },
        })
    }

    fn handle_install_snapshot_resp(&mut self, from: NodeId, term: Term) -> Vec<Action> {
        let mut actions = Vec::new();
        let current_term = self.current_term();
        if term > current_term {
            self.become_follower(term, None, &mut actions);
            return actions;
        }
        if self.role != Role::Leader || term != current_term {
            return actions;
        }
        let Some(transfer) = self.snap_out.get_mut(&from) else {
            return actions;
        };
        let Some(len) = transfer.inflight_len.take() else {
            return actions; // stale ack
        };
        if transfer.inflight_done {
            let index = transfer.last_included_index;
            self.snap_out.remove(&from);
            let cur = self.match_index.get(&from).copied().unwrap_or(0);
            self.match_index.insert(from, cur.max(index));
            self.next_index.insert(from, index + 1);
            self.maybe_commit(&mut actions);
            let (last, _) = self.storage.last_index_term();
            if index < last {
                if let Some(a) = self.send_append_entries_to(from) {
                    actions.push(a);
                }
            }
        } else {
            transfer.offset += len;
            if let Some(a) = self.send_snapshot_chunk_to(from) {
                actions.push(a);
            }
        }
        actions
    }

    #[allow(clippy::too_many_arguments)]
    fn handle_install_snapshot(
        &mut self,
        from: NodeId,
        term: Term,
        leader_id: NodeId,
        last_included_index: Index,
        last_included_term: Term,
        offset: u64,
        data: Vec<u8>,
        done: bool,
        now: Instant,
    ) -> Vec<Action> {
        let mut actions = Vec::new();
        let current_term = self.current_term();

        if term < current_term {
            actions.push(Action::Send {
                to: from,
                msg: RaftMessage::InstallSnapshotResp { term: current_term },
            });
            return actions;
        }

        if term > current_term || self.role != Role::Follower {
            self.become_follower(term, Some(leader_id), &mut actions);
        } else {
            self.leader = Some(leader_id);
        }
        self.reset_election_deadline(now);

        // Snapshot older than what we already committed carries nothing new.
        if last_included_index <= self.commit_index {
            self.snap_in.reset();
        } else {
            match self.snap_in.accept(
                last_included_index,
                last_included_term,
                offset,
                &data,
                done,
            ) {
                AssembleOutcome::Partial | AssembleOutcome::Rejected => {}
                AssembleOutcome::Complete(blob) => {
                    let conf: Vec<NodeId> = self.membership.voters().iter().copied().collect();
                    self.storage.install_snapshot(Snapshot {
                        last_included_index,
                        last_included_term,
                        conf,
                        state_blob: blob,
                    });
                    self.commit_index = self.commit_index.max(last_included_index);
                    self.last_applied = self.last_applied.max(last_included_index);
                    self.snapshot_requested = false;
                    actions.push(Action::SnapshotInstallComplete {
                        index: last_included_index,
                    });
                }
            }
        }

        actions.push(Action::Send {
            to: from,
            msg: RaftMessage::InstallSnapshotResp {
                term: self.current_term(),
            },
        });
        actions
    }

    fn maybe_commit(&mut self, actions: &mut Vec<Action>) {
        let (last_index, _) = self.storage.last_index_term();
        let term = self.current_term();
        let quorum = self.membership.quorum();

        // Scan high→low; only current-term entries may advance commit (Fig. 8).
        let mut new_commit = self.commit_index;
        let start = self.commit_index.saturating_add(1);
        if start > last_index {
            return;
        }
        for n in (start..=last_index).rev() {
            let Some(e) = self.storage.entry(n) else {
                continue;
            };
            if e.term != term {
                continue;
            }
            let mut count = 1; // self
            for peer in self.other_voters() {
                if self.match_index.get(&peer).copied().unwrap_or(0) >= n {
                    count += 1;
                }
            }
            if count >= quorum {
                new_commit = n;
                break;
            }
        }

        if new_commit > self.commit_index {
            self.advance_commit_and_apply(new_commit, new_commit, actions);
        }
    }
}
