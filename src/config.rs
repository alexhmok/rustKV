//! Node configuration from environment variables (no CLI crate is on the
//! dependency whitelist). Fixed cluster membership, per the project scope.
//!
//! Variables:
//! - `RUSTKV_NODE_ID`            — this node's id (default 1)
//! - `RUSTKV_LISTEN`             — client API address (default 127.0.0.1:8080)
//! - `RUSTKV_RAFT_LISTEN`        — raft RPC address (default 127.0.0.1:9080)
//! - `RUSTKV_DATA_DIR`           — log + hard state (default ./rustkv-data)
//! - `RUSTKV_PEERS`              — other members' raft addresses:
//!   `2=host:port,3=host:port` (empty/unset = single-node cluster)
//! - `RUSTKV_PEER_CLIENT_URLS`   — other members' client base URLs for write
//!   redirects: `2=http://host:port,...` (optional; without it, non-leaders
//!   answer 503 instead of 307)
//! - `RUSTKV_SNAPSHOT_THRESHOLD` — compact the log every N applied entries
//!   (>= 1; unset = snapshotting off, the default)
//! - `RUSTKV_SNAPSHOT_TRAILING`  — keep the snapshot boundary at least N
//!   applied entries behind, so slightly-lagging peers catch up via
//!   AppendEntries instead of InstallSnapshot (default 0 = compact
//!   immediately; only meaningful with the threshold set)
//! - `RUSTKV_JOIN`               — `1`/`true`: start as a JOINER (phase 15):
//!   empty membership, no campaigning, waiting to be added to a running
//!   cluster via `PUT /cluster/members/{id}` on its leader. RUSTKV_PEERS is
//!   not needed — peer addresses arrive with the configuration.
//! - `RUSTKV_ADVERTISE_RAFT_ADDR`  — the raft address OTHER nodes should
//!   dial for this node (default: RUSTKV_RAFT_LISTEN). Set it whenever the
//!   bind address is not reachable as-is (0.0.0.0, Docker, NAT): the
//!   advertised pair is what a ConfigChange embeds in the log and what the
//!   whole cluster then uses (phase 15).
//! - `RUSTKV_ADVERTISE_CLIENT_URL` — the client base URL peers should
//!   redirect to for this node (default: `http://{RUSTKV_LISTEN}`); same
//!   caveat as above.
//! - `RUSTKV_RPC_TIMEOUT_MS`     — per-RPC budget for node-to-node calls
//!   (default 150). Everything an RPC needs — connect, a possible
//!   stale-connection retry, transmitting the payload, the peer's fsync,
//!   the reply — must fit; raise it when snapshots or catch-up batches
//!   outgrow what the network moves in the default budget. Election
//!   timeouts are not derived from it, so raising it mainly delays failure
//!   detection of individual RPCs.

use std::collections::HashMap;

use crate::raft::types::NodeId;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeConfig {
    pub id: NodeId,
    pub listen: String,
    pub raft_listen: String,
    pub data_dir: String,
    pub peers: HashMap<NodeId, String>,
    pub peer_client_urls: HashMap<NodeId, String>,
    pub snapshot_threshold: Option<u64>,
    pub snapshot_trailing: u64,
    pub join: bool,
    /// Per-RPC budget for node-to-node calls, in milliseconds.
    pub rpc_timeout_ms: u64,
    /// What OTHER nodes dial/redirect to for this node (phase 15). Resolved
    /// here — defaults derived from the listen addresses — so the rest of
    /// the binary never has to know about the distinction.
    pub advertise_raft_addr: String,
    pub advertise_client_url: String,
}

impl NodeConfig {
    pub fn from_env() -> Result<Self, String> {
        Self::from_vars(|name| std::env::var(name).ok())
    }

    /// Testable core: reads variables through `get`.
    pub fn from_vars(get: impl Fn(&str) -> Option<String>) -> Result<Self, String> {
        let id = match get("RUSTKV_NODE_ID") {
            Some(raw) => raw
                .parse::<NodeId>()
                .map_err(|_| format!("RUSTKV_NODE_ID must be a number, got {raw:?}"))?,
            None => 1,
        };
        let listen = get("RUSTKV_LISTEN").unwrap_or_else(|| "127.0.0.1:8080".to_string());
        let raft_listen = get("RUSTKV_RAFT_LISTEN").unwrap_or_else(|| "127.0.0.1:9080".to_string());
        let config = Self {
            id,
            advertise_raft_addr: get("RUSTKV_ADVERTISE_RAFT_ADDR")
                .unwrap_or_else(|| raft_listen.clone()),
            advertise_client_url: get("RUSTKV_ADVERTISE_CLIENT_URL")
                .unwrap_or_else(|| format!("http://{listen}")),
            listen,
            raft_listen,
            data_dir: get("RUSTKV_DATA_DIR").unwrap_or_else(|| "./rustkv-data".to_string()),
            peers: parse_peer_map(get("RUSTKV_PEERS").as_deref().unwrap_or(""))
                .map_err(|e| format!("RUSTKV_PEERS: {e}"))?,
            peer_client_urls: parse_peer_map(
                get("RUSTKV_PEER_CLIENT_URLS").as_deref().unwrap_or(""),
            )
            .map_err(|e| format!("RUSTKV_PEER_CLIENT_URLS: {e}"))?,
            snapshot_threshold: match get("RUSTKV_SNAPSHOT_THRESHOLD") {
                Some(raw) => match raw.parse::<u64>() {
                    Ok(n) if n >= 1 => Some(n),
                    _ => {
                        return Err(format!(
                            "RUSTKV_SNAPSHOT_THRESHOLD must be a number >= 1, got {raw:?}"
                        ));
                    }
                },
                None => None,
            },
            snapshot_trailing: match get("RUSTKV_SNAPSHOT_TRAILING") {
                Some(raw) => raw.parse::<u64>().map_err(|_| {
                    format!("RUSTKV_SNAPSHOT_TRAILING must be a number, got {raw:?}")
                })?,
                None => 0,
            },
            rpc_timeout_ms: match get("RUSTKV_RPC_TIMEOUT_MS") {
                Some(raw) => match raw.parse::<u64>() {
                    Ok(n) if n >= 1 => n,
                    _ => {
                        return Err(format!(
                            "RUSTKV_RPC_TIMEOUT_MS must be a number >= 1, got {raw:?}"
                        ));
                    }
                },
                None => 150,
            },
            join: match get("RUSTKV_JOIN").as_deref() {
                None | Some("") | Some("0") | Some("false") => false,
                Some("1") | Some("true") => true,
                Some(other) => {
                    return Err(format!("RUSTKV_JOIN must be 0/1/true/false, got {other:?}"));
                }
            },
        };
        if config.peers.contains_key(&config.id) {
            return Err(format!(
                "RUSTKV_PEERS must list only OTHER members, but contains this node's id {id}"
            ));
        }
        Ok(config)
    }
}

/// Parses `1=value,2=value` maps (whitespace around entries tolerated).
fn parse_peer_map(raw: &str) -> Result<HashMap<NodeId, String>, String> {
    let mut map = HashMap::new();
    for entry in raw.split(',').map(str::trim).filter(|e| !e.is_empty()) {
        let (id, value) = entry
            .split_once('=')
            .ok_or_else(|| format!("expected id=value, got {entry:?}"))?;
        let id: NodeId = id
            .trim()
            .parse()
            .map_err(|_| format!("bad node id in {entry:?}"))?;
        if map.insert(id, value.trim().to_string()).is_some() {
            return Err(format!("duplicate node id {id}"));
        }
    }
    Ok(map)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn vars<'a>(pairs: &'a [(&'a str, &'a str)]) -> impl Fn(&str) -> Option<String> + 'a {
        move |name| {
            pairs
                .iter()
                .find(|(k, _)| *k == name)
                .map(|(_, v)| v.to_string())
        }
    }

    #[test]
    fn defaults_to_a_single_node_cluster() {
        let config = NodeConfig::from_vars(|_| None).unwrap();
        assert_eq!(config.id, 1);
        assert_eq!(config.listen, "127.0.0.1:8080");
        assert_eq!(config.raft_listen, "127.0.0.1:9080");
        assert_eq!(config.data_dir, "./rustkv-data");
        assert!(config.peers.is_empty());
        assert!(config.peer_client_urls.is_empty());
        assert_eq!(config.snapshot_threshold, None, "snapshotting is opt-in");
        assert!(!config.join, "join mode is opt-in");
        assert_eq!(config.rpc_timeout_ms, 150, "rpc timeout defaults to 150ms");
    }

    #[test]
    fn parses_and_validates_the_rpc_timeout() {
        let config = NodeConfig::from_vars(vars(&[("RUSTKV_RPC_TIMEOUT_MS", "2000")])).unwrap();
        assert_eq!(config.rpc_timeout_ms, 2000);
        assert!(NodeConfig::from_vars(vars(&[("RUSTKV_RPC_TIMEOUT_MS", "0")])).is_err());
        assert!(NodeConfig::from_vars(vars(&[("RUSTKV_RPC_TIMEOUT_MS", "fast")])).is_err());
    }

    /// Advertise addresses default to the listen addresses (the phase-15
    /// membership log embeds whatever is advertised, so nodes binding
    /// 0.0.0.0 must override them).
    #[test]
    fn advertise_addresses_default_to_listen_and_can_be_overridden() {
        let config = NodeConfig::from_vars(vars(&[
            ("RUSTKV_LISTEN", "0.0.0.0:8080"),
            ("RUSTKV_RAFT_LISTEN", "0.0.0.0:9080"),
        ]))
        .unwrap();
        assert_eq!(config.advertise_raft_addr, "0.0.0.0:9080");
        assert_eq!(config.advertise_client_url, "http://0.0.0.0:8080");

        let config = NodeConfig::from_vars(vars(&[
            ("RUSTKV_LISTEN", "0.0.0.0:8080"),
            ("RUSTKV_RAFT_LISTEN", "0.0.0.0:9080"),
            ("RUSTKV_ADVERTISE_RAFT_ADDR", "node2-raft:9080"),
            ("RUSTKV_ADVERTISE_CLIENT_URL", "http://localhost:8082"),
        ]))
        .unwrap();
        assert_eq!(config.advertise_raft_addr, "node2-raft:9080");
        assert_eq!(config.advertise_client_url, "http://localhost:8082");
    }

    #[test]
    fn parses_the_join_flag() {
        for (value, expected) in [("1", true), ("true", true), ("0", false), ("false", false)] {
            let config = NodeConfig::from_vars(vars(&[("RUSTKV_JOIN", value)])).unwrap();
            assert_eq!(config.join, expected, "RUSTKV_JOIN={value}");
        }
        assert!(NodeConfig::from_vars(vars(&[("RUSTKV_JOIN", "yes")])).is_err());
    }

    #[test]
    fn parses_and_validates_the_snapshot_threshold() {
        let config = NodeConfig::from_vars(vars(&[("RUSTKV_SNAPSHOT_THRESHOLD", "1000")])).unwrap();
        assert_eq!(config.snapshot_threshold, Some(1000));
        assert_eq!(config.snapshot_trailing, 0, "trailing defaults to 0");
        // 0 would mean "compact on every apply, even at the boundary" —
        // rejected rather than silently clamped.
        assert!(NodeConfig::from_vars(vars(&[("RUSTKV_SNAPSHOT_THRESHOLD", "0")])).is_err());
        assert!(NodeConfig::from_vars(vars(&[("RUSTKV_SNAPSHOT_THRESHOLD", "abc")])).is_err());
    }

    #[test]
    fn parses_the_snapshot_trailing_window() {
        let config = NodeConfig::from_vars(vars(&[
            ("RUSTKV_SNAPSHOT_THRESHOLD", "1000"),
            ("RUSTKV_SNAPSHOT_TRAILING", "5000"),
        ]))
        .unwrap();
        assert_eq!(config.snapshot_trailing, 5000);
        // 0 is legal (compact immediately, the default behavior).
        let config = NodeConfig::from_vars(vars(&[("RUSTKV_SNAPSHOT_TRAILING", "0")])).unwrap();
        assert_eq!(config.snapshot_trailing, 0);
        assert!(NodeConfig::from_vars(vars(&[("RUSTKV_SNAPSHOT_TRAILING", "abc")])).is_err());
    }

    #[test]
    fn parses_a_full_cluster_config() {
        let config = NodeConfig::from_vars(vars(&[
            ("RUSTKV_NODE_ID", "2"),
            ("RUSTKV_LISTEN", "0.0.0.0:8082"),
            ("RUSTKV_RAFT_LISTEN", "0.0.0.0:9082"),
            ("RUSTKV_DATA_DIR", "/data"),
            ("RUSTKV_PEERS", "1=n1:9080, 3=n3:9080"),
            (
                "RUSTKV_PEER_CLIENT_URLS",
                "1=http://n1:8080,3=http://n3:8080",
            ),
        ]))
        .unwrap();
        assert_eq!(config.id, 2);
        assert_eq!(config.peers[&1], "n1:9080");
        assert_eq!(config.peers[&3], "n3:9080");
        assert_eq!(config.peer_client_urls[&3], "http://n3:8080");
    }

    #[test]
    fn rejects_bad_input() {
        assert!(NodeConfig::from_vars(vars(&[("RUSTKV_NODE_ID", "abc")])).is_err());
        assert!(NodeConfig::from_vars(vars(&[("RUSTKV_PEERS", "nonsense")])).is_err());
        assert!(NodeConfig::from_vars(vars(&[("RUSTKV_PEERS", "x=addr")])).is_err());
        assert!(NodeConfig::from_vars(vars(&[("RUSTKV_PEERS", "2=a,2=b")])).is_err());
        // A node must not list itself as a peer.
        assert!(NodeConfig::from_vars(vars(&[("RUSTKV_PEERS", "1=self:9080")])).is_err());
    }
}
