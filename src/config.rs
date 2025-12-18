//! P2P network configuration

use crate::error::{P2PError, Result};
use crate::types::NodeRole;
use serde::{Deserialize, Serialize};
use std::path::Path;

/// P2P network configuration
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NetworkConfig {
    /// Unique node identifier
    pub node_id: String,

    /// Node role in the network
    pub node_role: NodeRole,

    /// Listen address for incoming connections
    pub listen_addr: String,

    /// Listen port for incoming connections
    pub listen_port: u16,

    /// Initial peer addresses to connect to
    pub peers: Vec<String>,

    /// Bootstrap node addresses for peer discovery
    pub bootstrap_nodes: Vec<String>,

    /// Maximum number of peer connections
    #[serde(default = "default_max_peers")]
    pub max_peers: usize,

    /// Minimum number of peer connections to maintain
    #[serde(default = "default_min_peers")]
    pub min_peers: usize,

    /// Connection timeout in seconds
    #[serde(default = "default_connection_timeout")]
    pub connection_timeout_secs: u64,

    /// Ping interval in seconds
    #[serde(default = "default_ping_interval")]
    pub ping_interval_secs: u64,

    /// Pong timeout in seconds
    #[serde(default = "default_pong_timeout")]
    pub pong_timeout_secs: u64,

    /// Maximum backoff delay in seconds
    #[serde(default = "default_max_backoff")]
    pub max_backoff_secs: u64,

    /// Rate limit in messages per second
    #[serde(default = "default_rate_limit")]
    pub rate_limit_msgs_per_sec: u32,

    /// Maximum message size in bytes
    #[serde(default = "default_max_message_size")]
    pub max_message_size_bytes: usize,

    /// Maximum candidate peers to track
    #[serde(default = "default_max_candidates")]
    pub max_candidates: usize,

    /// Peer timeout in seconds (mark unhealthy if not seen)
    #[serde(default = "default_peer_timeout")]
    pub peer_timeout_secs: u64,

    /// Enable peer discovery
    #[serde(default = "default_enable_discovery")]
    pub enable_peer_discovery: bool,

    /// Enable health monitoring
    #[serde(default = "default_enable_monitoring")]
    pub enable_health_monitoring: bool,

    /// Broadcast timeout in seconds
    #[serde(default = "default_broadcast_timeout")]
    pub broadcast_timeout_secs: u64,

    /// Maximum concurrent broadcast sends
    #[serde(default = "default_max_concurrent_broadcasts")]
    pub max_concurrent_broadcasts: usize,

    /// Unicast delivery timeout in seconds
    #[serde(default = "default_unicast_timeout")]
    pub unicast_timeout_secs: u64,

    /// Message receive timeout in seconds (for event loop)
    #[serde(default = "default_message_timeout")]
    pub message_timeout_secs: u64,
}

// Default value functions
fn default_max_peers() -> usize {
    100
}

fn default_min_peers() -> usize {
    3
}

fn default_connection_timeout() -> u64 {
    30
}

fn default_ping_interval() -> u64 {
    30
}

fn default_pong_timeout() -> u64 {
    10
}

fn default_max_backoff() -> u64 {
    300
}

fn default_rate_limit() -> u32 {
    1000
}

fn default_max_message_size() -> usize {
    100 * 1024 * 1024 // 100 MB
}

fn default_max_candidates() -> usize {
    1000
}

fn default_peer_timeout() -> u64 {
    300 // 5 minutes
}

fn default_enable_discovery() -> bool {
    true
}

fn default_enable_monitoring() -> bool {
    true
}

fn default_broadcast_timeout() -> u64 {
    5
}

fn default_max_concurrent_broadcasts() -> usize {
    10
}

fn default_unicast_timeout() -> u64 {
    5
}

fn default_message_timeout() -> u64 {
    30
}

impl NetworkConfig {
    /// Create a new network configuration
    pub fn new(node_id: String, node_role: NodeRole) -> Self {
        Self {
            node_id,
            node_role,
            listen_addr: "0.0.0.0".to_string(),
            listen_port: 9000,
            peers: Vec::new(),
            bootstrap_nodes: Vec::new(),
            max_peers: default_max_peers(),
            min_peers: default_min_peers(),
            connection_timeout_secs: default_connection_timeout(),
            ping_interval_secs: default_ping_interval(),
            pong_timeout_secs: default_pong_timeout(),
            max_backoff_secs: default_max_backoff(),
            rate_limit_msgs_per_sec: default_rate_limit(),
            max_message_size_bytes: default_max_message_size(),
            max_candidates: default_max_candidates(),
            peer_timeout_secs: default_peer_timeout(),
            enable_peer_discovery: default_enable_discovery(),
            enable_health_monitoring: default_enable_monitoring(),
            broadcast_timeout_secs: default_broadcast_timeout(),
            max_concurrent_broadcasts: default_max_concurrent_broadcasts(),
            unicast_timeout_secs: default_unicast_timeout(),
            message_timeout_secs: default_message_timeout(),
        }
    }

    /// Load configuration from TOML file
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self> {
        let contents = std::fs::read_to_string(path)
            .map_err(|e| P2PError::ConfigError(format!("Failed to read config file: {}", e)))?;

        let config: NetworkConfig = toml::from_str(&contents)
            .map_err(|e| P2PError::ConfigError(format!("Failed to parse config: {}", e)))?;

        config.validate()?;
        Ok(config)
    }

    /// Load configuration from environment variables
    pub fn from_env() -> Result<Self> {
        let node_id = std::env::var("SILVER_NODE_ID")
            .map_err(|_| P2PError::ConfigError("SILVER_NODE_ID not set".to_string()))?;

        let node_role_str = std::env::var("SILVER_NODE_ROLE")
            .unwrap_or_else(|_| "Validator".to_string());

        let node_role = match node_role_str.as_str() {
            "Validator" => NodeRole::Validator,
            "RPC" => NodeRole::RPC,
            "Archive" => NodeRole::Archive,
            _ => {
                return Err(P2PError::ConfigError(
                    "Invalid SILVER_NODE_ROLE".to_string(),
                ))
            }
        };

        let mut config = Self::new(node_id, node_role);

        if let Ok(addr) = std::env::var("SILVER_LISTEN_ADDR") {
            config.listen_addr = addr;
        }

        if let Ok(port_str) = std::env::var("SILVER_LISTEN_PORT") {
            config.listen_port = port_str
                .parse()
                .map_err(|_| P2PError::ConfigError("Invalid SILVER_LISTEN_PORT".to_string()))?;
        }

        if let Ok(peers_str) = std::env::var("SILVER_PEERS") {
            config.peers = peers_str.split(',').map(|s| s.to_string()).collect();
        }

        if let Ok(bootstrap_str) = std::env::var("SILVER_BOOTSTRAP_NODES") {
            config.bootstrap_nodes = bootstrap_str.split(',').map(|s| s.to_string()).collect();
        }

        config.validate()?;
        Ok(config)
    }

    /// Validate configuration
    pub fn validate(&self) -> Result<()> {
        if self.node_id.is_empty() {
            return Err(P2PError::ConfigError("node_id cannot be empty".to_string()));
        }

        if self.listen_port == 0 {
            return Err(P2PError::ConfigError("listen_port must be > 0".to_string()));
        }

        if self.max_peers == 0 {
            return Err(P2PError::ConfigError("max_peers must be > 0".to_string()));
        }

        if self.min_peers > self.max_peers {
            return Err(P2PError::ConfigError(
                "min_peers cannot be greater than max_peers".to_string(),
            ));
        }

        if self.connection_timeout_secs == 0 {
            return Err(P2PError::ConfigError(
                "connection_timeout_secs must be > 0".to_string(),
            ));
        }

        if self.ping_interval_secs == 0 {
            return Err(P2PError::ConfigError(
                "ping_interval_secs must be > 0".to_string(),
            ));
        }

        if self.pong_timeout_secs == 0 {
            return Err(P2PError::ConfigError(
                "pong_timeout_secs must be > 0".to_string(),
            ));
        }

        if self.max_backoff_secs == 0 {
            return Err(P2PError::ConfigError(
                "max_backoff_secs must be > 0".to_string(),
            ));
        }

        if self.rate_limit_msgs_per_sec == 0 {
            return Err(P2PError::ConfigError(
                "rate_limit_msgs_per_sec must be > 0".to_string(),
            ));
        }

        if self.max_message_size_bytes == 0 {
            return Err(P2PError::ConfigError(
                "max_message_size_bytes must be > 0".to_string(),
            ));
        }

        if self.broadcast_timeout_secs == 0 {
            return Err(P2PError::ConfigError(
                "broadcast_timeout_secs must be > 0".to_string(),
            ));
        }

        if self.max_concurrent_broadcasts == 0 {
            return Err(P2PError::ConfigError(
                "max_concurrent_broadcasts must be > 0".to_string(),
            ));
        }

        if self.unicast_timeout_secs == 0 {
            return Err(P2PError::ConfigError(
                "unicast_timeout_secs must be > 0".to_string(),
            ));
        }

        if self.message_timeout_secs == 0 {
            return Err(P2PError::ConfigError(
                "message_timeout_secs must be > 0".to_string(),
            ));
        }

        // Validate peer addresses
        for peer in &self.peers {
            if !self.is_valid_address(peer) {
                return Err(P2PError::InvalidPeerAddress(peer.clone()));
            }
        }

        // Validate bootstrap node addresses
        for bootstrap in &self.bootstrap_nodes {
            if !self.is_valid_address(bootstrap) {
                return Err(P2PError::InvalidPeerAddress(bootstrap.clone()));
            }
        }

        Ok(())
    }

    /// Check if address is valid (IP:port format)
    fn is_valid_address(&self, addr: &str) -> bool {
        let parts: Vec<&str> = addr.split(':').collect();
        if parts.len() != 2 {
            return false;
        }

        // Check if port is valid number
        parts[1].parse::<u16>().is_ok()
    }

    /// Get full listen address
    pub fn full_listen_addr(&self) -> String {
        format!("{}:{}", self.listen_addr, self.listen_port)
    }
}

impl Default for NetworkConfig {
    fn default() -> Self {
        Self::new("node1".to_string(), NodeRole::Validator)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_creation() {
        let config = NetworkConfig::new("node1".to_string(), NodeRole::Validator);
        assert_eq!(config.node_id, "node1");
        assert_eq!(config.node_role, NodeRole::Validator);
        assert_eq!(config.max_peers, 100);
    }

    #[test]
    fn test_config_validation() {
        let mut config = NetworkConfig::default();
        assert!(config.validate().is_ok());

        config.node_id = String::new();
        assert!(config.validate().is_err());

        config.node_id = "node1".to_string();
        config.max_peers = 0;
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_valid_address() {
        let config = NetworkConfig::default();
        assert!(config.is_valid_address("127.0.0.1:9000"));
        assert!(config.is_valid_address("192.168.1.1:8080"));
        assert!(!config.is_valid_address("127.0.0.1"));
        assert!(!config.is_valid_address("127.0.0.1:invalid"));
    }

    #[test]
    fn test_full_listen_addr() {
        let config = NetworkConfig::new("node1".to_string(), NodeRole::Validator);
        assert_eq!(config.full_listen_addr(), "0.0.0.0:9000");
    }
}
