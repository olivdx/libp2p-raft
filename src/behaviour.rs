//! `RaftBehaviour` — NetworkBehaviour adapter around `RaftEngine`.

use std::collections::{HashMap, HashSet, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};

use libp2p::core::transport::PortUse;
use libp2p::core::Endpoint;
use libp2p::swarm::behaviour::FromSwarm;
use libp2p::swarm::dial_opts::{DialOpts, PeerCondition};
use libp2p::swarm::{
    ConnectionDenied, ConnectionId, NetworkBehaviour, NotifyHandler, THandler, THandlerInEvent,
    THandlerOutEvent, ToSwarm,
};
use libp2p::{Multiaddr, PeerId};
use tokio::time::{sleep, Sleep};

use crate::config::RaftConfig;
use crate::error::Error;
use crate::handler::{FromBehaviour, RaftHandler, ToBehaviour};
use crate::peer_map::PeerMap;
use crate::protocol::messages::RaftMessage;
use crate::raft::engine::{Action, RaftEngine, RpcKind};
use crate::raft::types::{Index, LogEntry, NodeId, Role, Snapshot, Term};
use crate::storage::Storage;

#[derive(Debug)]
pub enum Event {
    RoleChanged {
        role: Role,
        term: Term,
        leader: Option<NodeId>,
    },
    Committed {
        entries: Vec<LogEntry>,
    },
    /// Applied log exceeded `snapshot_threshold`. Serialize the state machine as of
    /// `last_applied` and call [`RaftBehaviour::compact`].
    SnapshotNeeded {
        last_applied: Index,
    },
    /// A snapshot from the leader was installed; reset the state machine from
    /// `snapshot.state_blob` (entries <= `last_included_index` are already reflected).
    SnapshotInstalled {
        snapshot: Snapshot,
    },
    PeerMapped {
        peer: PeerId,
        node: NodeId,
    },
    RpcFailed {
        peer: PeerId,
        error: Error,
    },
}

struct PendingRequest {
    correlation_id: u64,
    to: NodeId,
    peer: PeerId,
    kind: RpcKind,
    sent_at: Instant,
    msg: RaftMessage,
}

struct InboundChannel {
    peer: PeerId,
    conn: ConnectionId,
    correlation_id: u64,
}

pub struct RaftBehaviour<S: Storage> {
    engine: RaftEngine<S>,
    config: RaftConfig,
    peer_map: PeerMap,
    connected: HashMap<PeerId, HashSet<ConnectionId>>,
    /// Peers we have an outbound dial in flight for (avoids DialPeerConditionFalse spam).
    dialing: HashSet<PeerId>,
    /// Do not redial until this instant (after transport failure to a dead/unreachable peer).
    dial_backoff_until: HashMap<PeerId, Instant>,
    pending_events: VecDeque<ToSwarm<Event, FromBehaviour>>,
    next_correlation_id: u64,
    pending: HashMap<u64, PendingRequest>,
    inbound_channels: HashMap<u64, InboundChannel>,
    sleep: Pin<Box<Sleep>>,
    waker: Option<Waker>,
}

impl<S: Storage + 'static> RaftBehaviour<S> {
    pub fn new(config: RaftConfig, storage: S) -> Self {
        let peer_map = PeerMap::from_seeds(&config.seed_peers);
        let engine = RaftEngine::new(config.clone(), storage);
        let mut pending_events = VecDeque::new();
        for seed in &config.seed_peers {
            pending_events.push_back(ToSwarm::GenerateEvent(Event::PeerMapped {
                peer: seed.peer_id,
                node: seed.node_id,
            }));
        }
        let next = engine.next_deadline();
        let mut behaviour = Self {
            engine,
            config,
            peer_map,
            connected: HashMap::new(),
            dialing: HashSet::new(),
            dial_backoff_until: HashMap::new(),
            pending_events,
            next_correlation_id: 1,
            pending: HashMap::new(),
            inbound_channels: HashMap::new(),
            sleep: Box::pin(sleep(std::time::Duration::from_secs(0))),
            waker: None,
        };
        behaviour.arm_sleep(next);
        behaviour
    }

    pub fn role(&self) -> Role {
        self.engine.role()
    }

    pub fn current_term(&self) -> Term {
        self.engine.current_term()
    }

    pub fn leader(&self) -> Option<NodeId> {
        self.engine.leader()
    }

    pub fn commit_index(&self) -> Index {
        self.engine.commit_index()
    }

    /// Current snapshot, if any.
    pub fn snapshot(&self) -> Option<Snapshot> {
        self.engine.snapshot()
    }

    /// Store a snapshot of the state machine as of `index` and compact the log.
    pub fn compact(&mut self, index: Index, state_blob: Vec<u8>) -> Result<(), Error> {
        Ok(self.engine.compact(index, state_blob)?)
    }

    pub fn peer_map(&self) -> &PeerMap {
        &self.peer_map
    }

    /// Propose a command on the Leader. Wakes the Behaviour poll loop to flush Actions.
    pub fn propose(&mut self, data: Vec<u8>) -> Result<Index, Error> {
        let (index, actions) = self.engine.propose(data)?;
        self.execute_actions(actions, None);
        self.arm_sleep(self.earliest_wake());
        if let Some(w) = self.waker.take() {
            w.wake();
        }
        Ok(index)
    }

    pub fn dial_seed(&mut self, peer: PeerId) {
        if self.is_connected(peer) || self.dialing.contains(&peer) {
            return;
        }
        if self
            .dial_backoff_until
            .get(&peer)
            .is_some_and(|until| Instant::now() < *until)
        {
            return;
        }
        let Some(node) = self.peer_map.node_id(peer) else {
            return;
        };
        let Some(addrs) = self.peer_map.addrs(node).map(|a| a.to_vec()) else {
            return;
        };
        if addrs.is_empty() {
            return;
        }
        self.dialing.insert(peer);
        self.pending_events.push_back(ToSwarm::Dial {
            opts: DialOpts::peer_id(peer)
                .condition(PeerCondition::DisconnectedAndNotDialing)
                .addresses(addrs)
                .build(),
        });
    }

    fn is_connected(&self, peer: PeerId) -> bool {
        self.connected
            .get(&peer)
            .map(|c| !c.is_empty())
            .unwrap_or(false)
    }

    fn is_benign_dial_error(error: &str) -> bool {
        error.contains("DialPeerConditionFalse")
            || error.contains("DisconnectedAndNotDialing")
            || error.contains("Timeout")
            || error.contains("HostUnreachable")
            || error.contains("ConnectionRefused")
            || error.contains("No route to host")
    }

    fn dial_backoff_jitter_ms(&self, peer: PeerId) -> u64 {
        let target = self.peer_map.node_id(peer).unwrap_or(0);
        (self.config.node_id
            .wrapping_mul(137)
            .wrapping_add(target.wrapping_mul(89))
            .wrapping_add(31))
            % 500
    }

    fn dial_backoff(&mut self, peer: PeerId) {
        const BASE_SECS: u64 = 5;
        let jitter_ms = self.dial_backoff_jitter_ms(peer);
        self.dial_backoff_until.insert(
            peer,
            Instant::now() + Duration::from_secs(BASE_SECS) + Duration::from_millis(jitter_ms),
        );
    }

    fn arm_sleep(&mut self, deadline: Instant) {
        let now = Instant::now();
        let dur = deadline.saturating_duration_since(now);
        self.sleep = Box::pin(sleep(dur));
    }

    fn earliest_wake(&self) -> Instant {
        let mut wake = self.engine.next_deadline();
        for p in self.pending.values() {
            let t = p.sent_at + self.config.rpc_timeout;
            if t < wake {
                wake = t;
            }
        }
        wake
    }

    fn rpc_kind(msg: &RaftMessage) -> RpcKind {
        match msg {
            RaftMessage::RequestVote { .. } | RaftMessage::RequestVoteResp { .. } => {
                RpcKind::RequestVote
            }
            RaftMessage::AppendEntries { .. } | RaftMessage::AppendEntriesResp { .. } => {
                RpcKind::AppendEntries
            }
            RaftMessage::InstallSnapshot { .. } | RaftMessage::InstallSnapshotResp { .. } => {
                RpcKind::InstallSnapshot
            }
        }
    }

    fn execute_actions(&mut self, actions: Vec<Action>, inbound_from: Option<(NodeId, u64)>) {
        for action in actions {
            match action {
                Action::Send { to, msg } => {
                    if let Some((from, channel_id)) = inbound_from {
                        if to == from {
                            if let Some(ch) = self.inbound_channels.remove(&channel_id) {
                                self.pending_events.push_back(ToSwarm::NotifyHandler {
                                    peer_id: ch.peer,
                                    handler: NotifyHandler::One(ch.conn),
                                    event: FromBehaviour::SendResponse {
                                        channel_id,
                                        correlation_id: ch.correlation_id,
                                        msg,
                                    },
                                });
                                continue;
                            }
                        }
                    }
                    self.send_rpc(to, msg);
                }
                Action::Broadcast { msg } => {
                    for voter in self.engine.other_voters() {
                        self.send_rpc(voter, msg.clone());
                    }
                }
                Action::BecomeLeader { term } => {
                    self.pending_events
                        .push_back(ToSwarm::GenerateEvent(Event::RoleChanged {
                            role: Role::Leader,
                            term,
                            leader: Some(self.engine.node_id()),
                        }));
                }
                Action::BecomeFollower { term, leader } => {
                    self.pending_events
                        .push_back(ToSwarm::GenerateEvent(Event::RoleChanged {
                            role: Role::Follower,
                            term,
                            leader,
                        }));
                }
                Action::BecomeCandidate { term } => {
                    self.pending_events
                        .push_back(ToSwarm::GenerateEvent(Event::RoleChanged {
                            role: Role::Candidate,
                            term,
                            leader: None,
                        }));
                }
                Action::Apply { entries } => {
                    self.pending_events
                        .push_back(ToSwarm::GenerateEvent(Event::Committed { entries }));
                }
                Action::SnapshotNeeded { last_applied } => {
                    self.pending_events
                        .push_back(ToSwarm::GenerateEvent(Event::SnapshotNeeded {
                            last_applied,
                        }));
                }
                Action::SnapshotInstallComplete { .. } => {
                    if let Some(snapshot) = self.engine.snapshot() {
                        self.pending_events
                            .push_back(ToSwarm::GenerateEvent(Event::SnapshotInstalled {
                                snapshot,
                            }));
                    }
                }
            }
        }
    }

    fn send_rpc(&mut self, to: NodeId, msg: RaftMessage) {
        let kind = Self::rpc_kind(&msg);
        let Some(peer) = self.peer_map.peer_id(to) else {
            let _ = self.engine.handle_rpc_failure(to, kind);
            self.pending_events
                .push_back(ToSwarm::GenerateEvent(Event::RpcFailed {
                    peer: PeerId::random(),
                    error: Error::UnknownNode(to),
                }));
            return;
        };

        if !self.is_connected(peer) {
            // Drop RPC; clear AE inflight; dial once. No RpcFailed — heartbeat/election retries.
            let _ = self.engine.handle_rpc_failure(to, kind);
            self.dial_seed(peer);
            return;
        }

        let correlation_id = self.next_correlation_id;
        self.next_correlation_id = self.next_correlation_id.wrapping_add(1);
        self.pending.insert(
            correlation_id,
            PendingRequest {
                correlation_id,
                to,
                peer,
                kind,
                sent_at: Instant::now(),
                msg: msg.clone(),
            },
        );
        self.pending_events.push_back(ToSwarm::NotifyHandler {
            peer_id: peer,
            handler: NotifyHandler::Any,
            event: FromBehaviour::SendRequest {
                correlation_id,
                msg,
            },
        });
    }

    fn fail_pending(&mut self, correlation_id: u64, error: String, emit_event: bool) {
        if let Some(p) = self.pending.remove(&correlation_id) {
            let actions = self.engine.handle_rpc_failure(p.to, p.kind);
            self.execute_actions(actions, None);
            if emit_event {
                self.pending_events
                    .push_back(ToSwarm::GenerateEvent(Event::RpcFailed {
                        peer: p.peer,
                        error: Error::Rpc(error),
                    }));
            }
        }
    }

    fn fail_peer_pending(&mut self, peer: PeerId, error: String, emit_event: bool) {
        let ids: Vec<u64> = self
            .pending
            .iter()
            .filter(|(_, p)| p.peer == peer)
            .map(|(id, _)| *id)
            .collect();
        for id in ids {
            self.fail_pending(id, error.clone(), emit_event);
        }
    }
}

impl<S: Storage + 'static> NetworkBehaviour for RaftBehaviour<S> {
    type ConnectionHandler = RaftHandler;
    type ToSwarm = Event;

    fn handle_established_inbound_connection(
        &mut self,
        _connection_id: ConnectionId,
        _peer: PeerId,
        _local_addr: &Multiaddr,
        _remote_addr: &Multiaddr,
    ) -> Result<THandler<Self>, ConnectionDenied> {
        Ok(RaftHandler::new())
    }

    fn handle_established_outbound_connection(
        &mut self,
        _connection_id: ConnectionId,
        _peer: PeerId,
        _addr: &Multiaddr,
        _role_override: Endpoint,
        _port_use: PortUse,
    ) -> Result<THandler<Self>, ConnectionDenied> {
        Ok(RaftHandler::new())
    }

    fn handle_pending_outbound_connection(
        &mut self,
        _connection_id: ConnectionId,
        maybe_peer: Option<PeerId>,
        _addresses: &[Multiaddr],
        _effective_role: Endpoint,
    ) -> Result<Vec<Multiaddr>, ConnectionDenied> {
        let Some(peer) = maybe_peer else {
            return Ok(vec![]);
        };
        let Some(node) = self.peer_map.node_id(peer) else {
            return Ok(vec![]);
        };
        Ok(self
            .peer_map
            .addrs(node)
            .map(|a| a.to_vec())
            .unwrap_or_default())
    }

    fn on_swarm_event(&mut self, event: FromSwarm) {
        match event {
            FromSwarm::ConnectionEstablished(e) => {
                self.dialing.remove(&e.peer_id);
                self.dial_backoff_until.remove(&e.peer_id);
                self.connected
                    .entry(e.peer_id)
                    .or_default()
                    .insert(e.connection_id);
            }
            FromSwarm::ConnectionClosed(e) => {
                if let Some(set) = self.connected.get_mut(&e.peer_id) {
                    set.remove(&e.connection_id);
                    if set.is_empty() {
                        self.connected.remove(&e.peer_id);
                    }
                }
                self.dialing.remove(&e.peer_id);

                if !self.is_connected(e.peer_id) {
                    self.fail_peer_pending(e.peer_id, "connection closed".into(), false);
                }
                // Transient disconnect; engine retries on next tick — no RpcFailed spam.
            }
            FromSwarm::DialFailure(e) => {
                if let Some(peer) = e.peer_id {
                    self.dialing.remove(&peer);
                    let err = format!("{:?}", e.error);
                    let benign = Self::is_benign_dial_error(&err);
                    if benign {
                        self.dial_backoff(peer);
                    }
                    self.fail_peer_pending(peer, format!("dial failure: {err}"), !benign);
                    if !benign {
                        self.pending_events
                            .push_back(ToSwarm::GenerateEvent(Event::RpcFailed {
                                peer,
                                error: Error::Rpc(format!("dial failure: {err}")),
                            }));
                    }
                }
            }
            _ => {}
        }
    }

    fn on_connection_handler_event(
        &mut self,
        peer_id: PeerId,
        connection_id: ConnectionId,
        event: THandlerOutEvent<Self>,
    ) {
        let now = Instant::now();
        match event {
            ToBehaviour::Request {
                correlation_id,
                msg,
                channel_id,
            } => {
                let Some(from) = self.peer_map.node_id(peer_id) else {
                    self.pending_events
                        .push_back(ToSwarm::GenerateEvent(Event::RpcFailed {
                            peer: peer_id,
                            error: Error::Rpc("unknown peer mapping".into()),
                        }));
                    return;
                };
                self.inbound_channels.insert(
                    channel_id,
                    InboundChannel {
                        peer: peer_id,
                        conn: connection_id,
                        correlation_id,
                    },
                );
                let actions = self.engine.handle_rpc(from, msg, now);
                self.execute_actions(actions, Some((from, channel_id)));
                // If engine produced no Send back to `from`, drop channel to avoid leak.
                self.inbound_channels.remove(&channel_id);
            }
            ToBehaviour::Response {
                correlation_id,
                msg,
            } => {
                let Some(pending) = self.pending.remove(&correlation_id) else {
                    return;
                };
                let actions = self.engine.handle_rpc(pending.to, msg, now);
                self.execute_actions(actions, None);
            }
            ToBehaviour::Failure {
                correlation_id,
                error,
            } => {
                if let Some(id) = correlation_id {
                    self.fail_pending(id, error, true);
                } else {
                    self.pending_events
                        .push_back(ToSwarm::GenerateEvent(Event::RpcFailed {
                            peer: peer_id,
                            error: Error::Rpc(error),
                        }));
                }
            }
        }
        self.arm_sleep(self.earliest_wake());
    }

    fn poll(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<ToSwarm<Self::ToSwarm, THandlerInEvent<Self>>> {
        self.waker = Some(cx.waker().clone());
        loop {
            if let Some(ev) = self.pending_events.pop_front() {
                return Poll::Ready(ev);
            }

            let now = Instant::now();

            // Expire pending RPCs (no retry — tick/election is the retry).
            let timed_out: Vec<u64> = self
                .pending
                .iter()
                .filter(|(_, p)| {
                    now.saturating_duration_since(p.sent_at) >= self.config.rpc_timeout
                })
                .map(|(id, _)| *id)
                .collect();
            for id in timed_out {
                self.fail_pending(id, "rpc timeout".into(), false);
            }
            if let Some(ev) = self.pending_events.pop_front() {
                self.arm_sleep(self.earliest_wake());
                return Poll::Ready(ev);
            }

            if now >= self.engine.next_deadline() {
                let out = self.engine.tick(now);
                self.execute_actions(out.actions, None);
                self.arm_sleep(self.earliest_wake());
                if let Some(ev) = self.pending_events.pop_front() {
                    return Poll::Ready(ev);
                }
            }

            match self.sleep.as_mut().poll(cx) {
                Poll::Ready(()) => {
                    // Timer fired; loop to tick / expire.
                    self.arm_sleep(self.earliest_wake());
                    continue;
                }
                Poll::Pending => {
                    return Poll::Pending;
                }
            }
        }
    }
}
