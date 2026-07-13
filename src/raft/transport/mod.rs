//! Node-to-node transport abstraction.
//!
//! ARCHITECTURE RULE (CLAUDE.md): the Raft core never touches the network —
//! it sends RPCs through the [`Transport`] trait and receives them as
//! [`Inbound`] values on a channel. Two implementations:
//! - [`sim`]: in-memory, deterministic, fault-injecting;
//! - [`http`]: JSON over HTTP/1.1 between real listeners.

use std::fmt;
use std::future::Future;

use tokio::sync::oneshot;

use super::rpc::{RpcRequest, RpcResponse};
use super::types::NodeId;

pub mod http;
pub mod sim;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransportError {
    /// The destination is not a known cluster member (misconfiguration —
    /// crashed-but-configured peers time out instead).
    Unreachable(NodeId),
    /// No reply within the transport's timeout: the request or reply was
    /// lost, delayed too long, or the peer is down. Indistinguishable by
    /// design, exactly as on a real network.
    Timeout,
}

impl fmt::Display for TransportError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TransportError::Unreachable(id) => write!(f, "node {id} is not a cluster member"),
            TransportError::Timeout => write!(f, "rpc timed out"),
        }
    }
}

impl std::error::Error for TransportError {}

/// Outbound half: send one RPC and await the peer's reply.
///
/// `Send + Sync + 'static` so a node can share one transport across its
/// per-peer tasks; implementations are expected to be cheaply cloneable.
pub trait Transport: Send + Sync + 'static {
    fn send(
        &self,
        to: NodeId,
        req: RpcRequest,
    ) -> impl Future<Output = Result<RpcResponse, TransportError>> + Send;
}

/// Inbound half: an RPC delivered to a node, with a one-shot channel to send
/// the reply back through the transport. Both transport implementations hand
/// the Raft core an `mpsc::Receiver<Inbound>` so its event loop can `select!`
/// over RPCs, timers, and client requests without owning any network code.
#[derive(Debug)]
pub struct Inbound {
    pub from: NodeId,
    pub request: RpcRequest,
    pub reply: oneshot::Sender<RpcResponse>,
}
