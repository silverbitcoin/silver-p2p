//! Core types for P2P network

use serde::{Deserialize, Serialize};
use std::time::SystemTime;
use std::collections::HashMap;

/// Network message types
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum NetworkMessage {
    /// Peer list request
    PeerListRequest,

    /// Peer list response with addresses
    PeerListResponse(Vec<String>),

    /// Ping message with nonce for latency measurement
    /// Ping message with nonce for latency measurement
    Ping {
        /// Nonce for matching pong response
        nonce: u64,
    },

    /// Pong response with matching nonce
    Pong {
        /// Nonce matching the ping request
        nonce: u64,
    },

    /// Block proposal for consensus
    BlockProposal {
        /// Block height
        height: u64,
        /// Block hash
        hash: String,
        /// Block data
        data: Vec<u8>,
    },

    /// Vote on block validity
    BlockVote {
        /// Block hash being voted on
        block_hash: String,
        /// Vote: true = valid, false = invalid
        vote: bool,
    },

    /// State synchronization request
    StateSync {
        /// Starting block height
        from_height: u64,
        /// Ending block height
        to_height: u64,
    },

    /// Request specific block
    BlockRequest {
        /// Block height
        height: u64,
    },

    /// Block response
    BlockResponse {
        /// Block height
        height: u64,
        /// Block data
        data: Vec<u8>,
    },

    /// Custom message type
    Custom {
        /// Message type identifier
        msg_type: String,
        /// Message data
        data: Vec<u8>,
    },
}

impl NetworkMessage {
    /// Get message type as string
    pub fn message_type(&self) -> String {
        match self {
            NetworkMessage::PeerListRequest => "PeerListRequest".to_string(),
            NetworkMessage::PeerListResponse(_) => "PeerListResponse".to_string(),
            NetworkMessage::Ping { .. } => "Ping".to_string(),
            NetworkMessage::Pong { .. } => "Pong".to_string(),
            NetworkMessage::BlockProposal { .. } => "BlockProposal".to_string(),
            NetworkMessage::BlockVote { .. } => "BlockVote".to_string(),
            NetworkMessage::StateSync { .. } => "StateSync".to_string(),
            NetworkMessage::BlockRequest { .. } => "BlockRequest".to_string(),
            NetworkMessage::BlockResponse { .. } => "BlockResponse".to_string(),
            NetworkMessage::Custom { msg_type, .. } => msg_type.clone(),
        }
    }

    /// Estimate message size in bytes
    pub fn estimated_size(&self) -> usize {
        match self {
            NetworkMessage::PeerListRequest => 1,
            NetworkMessage::PeerListResponse(addrs) => {
                addrs.iter().map(|a| a.len()).sum::<usize>() + 100
            }
            NetworkMessage::Ping { .. } => 16,
            NetworkMessage::Pong { .. } => 16,
            NetworkMessage::BlockProposal { data, .. } => data.len() + 100,
            NetworkMessage::BlockVote { .. } => 100,
            NetworkMessage::StateSync { .. } => 50,
            NetworkMessage::BlockRequest { .. } => 50,
            NetworkMessage::BlockResponse { data, .. } => data.len() + 50,
            NetworkMessage::Custom { data, .. } => data.len() + 50,
        }
    }
}

/// Node role in the network
#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum NodeRole {
    /// Validator node participating in consensus
    Validator,
    /// RPC node serving queries
    RPC,
    /// Archive node storing full history
    Archive,
}

impl std::fmt::Display for NodeRole {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NodeRole::Validator => write!(f, "Validator"),
            NodeRole::RPC => write!(f, "RPC"),
            NodeRole::Archive => write!(f, "Archive"),
        }
    }
}

/// Peer state information
#[derive(Clone, Debug)]
pub struct PeerState {
    /// Unique peer identifier
    pub peer_id: String,
    /// Network address (IP:port)
    pub address: String,
    /// Node role
    pub role: NodeRole,
    /// Whether peer is currently connected
    pub is_connected: bool,
    /// Whether peer is healthy
    pub is_healthy: bool,
    /// Last time peer was seen
    pub last_seen: SystemTime,
    /// Peer's current block height
    pub block_height: u64,
    /// Latency to peer in milliseconds
    pub latency_ms: u64,
    /// Messages sent to peer
    pub messages_sent: u64,
    /// Messages received from peer
    pub messages_received: u64,
    /// Bytes sent to peer
    pub bytes_sent: u64,
    /// Bytes received from peer
    pub bytes_received: u64,
    /// Number of connection attempts
    pub connection_attempts: u32,
    /// Last error message
    pub last_error: Option<String>,
    /// Health score (0-100): measures peer responsiveness and reliability
    pub health_score: u32,
    /// Consecutive successful pings
    pub consecutive_successful_pings: u32,
    /// Consecutive failed pings
    pub consecutive_failed_pings: u32,
}

impl PeerState {
    /// Create new peer state
    pub fn new(peer_id: String, address: String, role: NodeRole) -> Self {
        Self {
            peer_id,
            address,
            role,
            is_connected: false,
            is_healthy: true,
            last_seen: SystemTime::now(),
            block_height: 0,
            latency_ms: 0,
            messages_sent: 0,
            messages_received: 0,
            bytes_sent: 0,
            bytes_received: 0,
            connection_attempts: 0,
            last_error: None,
            health_score: 100,
            consecutive_successful_pings: 0,
            consecutive_failed_pings: 0,
        }
    }

    /// Update last seen time
    pub fn update_last_seen(&mut self) {
        self.last_seen = SystemTime::now();
    }

    /// Update latency
    pub fn update_latency(&mut self, latency_ms: u64) {
        self.latency_ms = latency_ms;
    }

    /// Increment messages sent
    pub fn increment_messages_sent(&mut self, bytes: u64) {
        self.messages_sent += 1;
        self.bytes_sent += bytes;
    }

    /// Increment messages received
    pub fn increment_messages_received(&mut self, bytes: u64) {
        self.messages_received += 1;
        self.bytes_received += bytes;
    }

    /// Record successful ping - increases health score
    pub fn record_successful_ping(&mut self) {
        self.consecutive_successful_pings += 1;
        self.consecutive_failed_pings = 0;
        
        // Increase health score by 5 points per successful ping, max 100
        self.health_score = std::cmp::min(100, self.health_score + 5);
    }

    /// Record failed ping - decreases health score
    pub fn record_failed_ping(&mut self) {
        self.consecutive_failed_pings += 1;
        self.consecutive_successful_pings = 0;
        
        // Decrease health score by 10 points per failed ping, min 0
        self.health_score = self.health_score.saturating_sub(10);
    }

    /// Get health score (0-100)
    pub fn get_health_score(&self) -> u32 {
        self.health_score
    }

    /// Check if peer is below unhealthy threshold (score < 30)
    pub fn is_below_unhealthy_threshold(&self) -> bool {
        self.health_score < 30
    }

    /// Check if peer can recover (score >= 50)
    pub fn can_recover(&self) -> bool {
        self.health_score >= 50
    }

    /// Reset health score to initial state (100)
    pub fn reset_health_score(&mut self) {
        self.health_score = 100;
        self.consecutive_successful_pings = 0;
        self.consecutive_failed_pings = 0;
    }
}

/// Peer information for queries
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PeerInfo {
    /// Peer ID
    pub peer_id: String,
    /// Network address
    pub address: String,
    /// Node role
    pub role: String,
    /// Connection status
    pub is_connected: bool,
    /// Health status
    pub is_healthy: bool,
    /// Last seen timestamp
    pub last_seen: u64,
    /// Block height
    pub block_height: u64,
    /// Latency in milliseconds
    pub latency_ms: u64,
}

impl From<&PeerState> for PeerInfo {
    fn from(state: &PeerState) -> Self {
        Self {
            peer_id: state.peer_id.clone(),
            address: state.address.clone(),
            role: state.role.to_string(),
            is_connected: state.is_connected,
            is_healthy: state.is_healthy,
            last_seen: state
                .last_seen
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            block_height: state.block_height,
            latency_ms: state.latency_ms,
        }
    }
}

/// Health status of the network
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HealthStatus {
    /// Number of connected peers
    pub connected_peers: usize,
    /// Total known peers
    pub total_peers: usize,
    /// Network is healthy
    pub is_healthy: bool,
    /// Average latency in milliseconds
    pub avg_latency_ms: u64,
    /// Messages per second
    pub messages_per_sec: u64,
    /// Bytes per second
    pub bytes_per_sec: u64,
}

/// Network statistics
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NetworkStats {
    /// Connected peer count
    pub connected_peers: usize,
    /// Total known peers
    pub total_peers: usize,
    /// Candidate peers waiting to connect
    pub candidate_peers: usize,
    /// Total messages sent
    pub total_messages_sent: u64,
    /// Total messages received
    pub total_messages_received: u64,
    /// Total bytes sent
    pub total_bytes_sent: u64,
    /// Total bytes received
    pub total_bytes_received: u64,
    /// Average peer latency
    pub avg_latency_ms: u64,
    /// Network uptime in seconds
    pub uptime_secs: u64,
    /// Per-peer statistics
    pub peer_stats: HashMap<String, PeerStats>,
}

/// Per-peer statistics
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PeerStats {
    /// Messages sent to peer
    pub messages_sent: u64,
    /// Messages received from peer
    pub messages_received: u64,
    /// Bytes sent to peer
    pub bytes_sent: u64,
    /// Bytes received from peer
    pub bytes_received: u64,
    /// Latency in milliseconds
    pub latency_ms: u64,
    /// Connection status
    pub is_connected: bool,
}

/// Backoff state for reconnection
#[derive(Clone, Debug)]
pub struct BackoffState {
    /// Current backoff delay in seconds
    pub current_delay_secs: u64,
    /// Maximum backoff delay in seconds
    pub max_delay_secs: u64,
    /// Number of failed attempts
    pub failed_attempts: u32,
    /// Last attempt time
    pub last_attempt: SystemTime,
}

impl BackoffState {
    /// Create new backoff state
    pub fn new(max_delay_secs: u64) -> Self {
        Self {
            current_delay_secs: 1,
            max_delay_secs,
            failed_attempts: 0,
            last_attempt: SystemTime::now(),
        }
    }

    /// Calculate next backoff delay
    pub fn next_delay(&mut self) -> u64 {
        self.failed_attempts += 1;
        self.current_delay_secs = std::cmp::min(
            self.current_delay_secs * 2,
            self.max_delay_secs,
        );
        self.last_attempt = SystemTime::now();
        self.current_delay_secs
    }

    /// Reset backoff state
    pub fn reset(&mut self) {
        self.current_delay_secs = 1;
        self.failed_attempts = 0;
        self.last_attempt = SystemTime::now();
    }

    /// Check if ready to retry
    pub fn is_ready_to_retry(&self) -> bool {
        let elapsed = self
            .last_attempt
            .elapsed()
            .unwrap_or_default()
            .as_secs();
        elapsed >= self.current_delay_secs
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_network_message_type() {
        assert_eq!(NetworkMessage::PeerListRequest.message_type(), "PeerListRequest");
        assert_eq!(
            NetworkMessage::Ping { nonce: 123 }.message_type(),
            "Ping"
        );
    }

    #[test]
    fn test_peer_state_creation() {
        let peer = PeerState::new(
            "peer1".to_string(),
            "127.0.0.1:9000".to_string(),
            NodeRole::Validator,
        );
        assert_eq!(peer.peer_id, "peer1");
        assert!(!peer.is_connected);
        assert!(peer.is_healthy);
        assert_eq!(peer.health_score, 100);
        assert_eq!(peer.consecutive_successful_pings, 0);
        assert_eq!(peer.consecutive_failed_pings, 0);
    }

    #[test]
    fn test_health_score_increase_on_successful_ping() {
        let mut peer = PeerState::new(
            "peer1".to_string(),
            "127.0.0.1:9000".to_string(),
            NodeRole::Validator,
        );
        
        // Start at 100
        assert_eq!(peer.health_score, 100);
        
        // Successful ping increases score by 5
        peer.record_successful_ping();
        assert_eq!(peer.health_score, 100); // Already at max
        assert_eq!(peer.consecutive_successful_pings, 1);
        assert_eq!(peer.consecutive_failed_pings, 0);
    }

    #[test]
    fn test_health_score_decrease_on_failed_ping() {
        let mut peer = PeerState::new(
            "peer1".to_string(),
            "127.0.0.1:9000".to_string(),
            NodeRole::Validator,
        );
        
        // Start at 100
        assert_eq!(peer.health_score, 100);
        
        // Failed ping decreases score by 10
        peer.record_failed_ping();
        assert_eq!(peer.health_score, 90);
        assert_eq!(peer.consecutive_failed_pings, 1);
        assert_eq!(peer.consecutive_successful_pings, 0);
    }

    #[test]
    fn test_health_score_recovery_after_failures() {
        let mut peer = PeerState::new(
            "peer1".to_string(),
            "127.0.0.1:9000".to_string(),
            NodeRole::Validator,
        );
        
        // Simulate 5 failed pings
        for _ in 0..5 {
            peer.record_failed_ping();
        }
        assert_eq!(peer.health_score, 50);
        
        // Now successful pings should increase score
        peer.record_successful_ping();
        assert_eq!(peer.health_score, 55);
        assert_eq!(peer.consecutive_successful_pings, 1);
        assert_eq!(peer.consecutive_failed_pings, 0);
    }

    #[test]
    fn test_health_score_below_threshold() {
        let mut peer = PeerState::new(
            "peer1".to_string(),
            "127.0.0.1:9000".to_string(),
            NodeRole::Validator,
        );
        
        // Simulate 8 failed pings (score = 20)
        for _ in 0..8 {
            peer.record_failed_ping();
        }
        assert_eq!(peer.health_score, 20);
        assert!(peer.is_below_unhealthy_threshold());
    }

    #[test]
    fn test_health_score_can_recover() {
        let mut peer = PeerState::new(
            "peer1".to_string(),
            "127.0.0.1:9000".to_string(),
            NodeRole::Validator,
        );
        
        // Simulate 5 failed pings (score = 50)
        for _ in 0..5 {
            peer.record_failed_ping();
        }
        assert_eq!(peer.health_score, 50);
        assert!(peer.can_recover()); // score >= 50
        
        // Simulate 1 more failed ping (score = 40)
        peer.record_failed_ping();
        assert_eq!(peer.health_score, 40);
        assert!(!peer.can_recover()); // score < 50
        
        // Simulate 4 more failed pings (score = 0)
        for _ in 0..4 {
            peer.record_failed_ping();
        }
        assert_eq!(peer.health_score, 0);
        assert!(!peer.can_recover());
    }

    #[test]
    fn test_health_score_reset() {
        let mut peer = PeerState::new(
            "peer1".to_string(),
            "127.0.0.1:9000".to_string(),
            NodeRole::Validator,
        );
        
        // Simulate failures
        for _ in 0..5 {
            peer.record_failed_ping();
        }
        assert_eq!(peer.health_score, 50);
        
        // Reset
        peer.reset_health_score();
        assert_eq!(peer.health_score, 100);
        assert_eq!(peer.consecutive_successful_pings, 0);
        assert_eq!(peer.consecutive_failed_pings, 0);
    }

    #[test]
    fn test_health_score_min_max_bounds() {
        let mut peer = PeerState::new(
            "peer1".to_string(),
            "127.0.0.1:9000".to_string(),
            NodeRole::Validator,
        );
        
        // Test max bound (100)
        for _ in 0..20 {
            peer.record_successful_ping();
        }
        assert_eq!(peer.health_score, 100);
        
        // Test min bound (0)
        for _ in 0..20 {
            peer.record_failed_ping();
        }
        assert_eq!(peer.health_score, 0);
    }

    #[test]
    fn test_backoff_state() {
        let mut backoff = BackoffState::new(300);
        assert_eq!(backoff.current_delay_secs, 1);

        let delay1 = backoff.next_delay();
        assert_eq!(delay1, 2);

        let delay2 = backoff.next_delay();
        assert_eq!(delay2, 4);

        backoff.reset();
        assert_eq!(backoff.current_delay_secs, 1);
        assert_eq!(backoff.failed_attempts, 0);
    }
}
