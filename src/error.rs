use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error(transparent)]
    Raft(#[from] RaftError),
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error("codec: {0}")]
    Codec(String),
    #[error("unknown node {0}")]
    UnknownNode(u64),
    #[error("rpc failed: {0}")]
    Rpc(String),
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum RaftError {
    #[error("not leader")]
    NotLeader,
    #[error("membership change pending")]
    MembershipPending,
    #[error("invalid membership change")]
    InvalidMembership,
    #[error("invalid snapshot index")]
    InvalidSnapshotIndex,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum StorageError {
    #[error("storage: {0}")]
    Msg(String),
}
