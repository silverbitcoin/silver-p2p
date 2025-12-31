//! Connection error recovery with automatic reconnection scheduling

use crate::connection_pool::ConnectionPool;
use crate::error::Result;
use crate::peer_manager::PeerManager;
use crate::reconnection_manager::{ReconnectionEvent, ReconnectionManager};
use dashmap::DashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

/// Connection error types for recovery handling
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ConnectionErrorType {
    /// Connection refused by peer
    ConnectionRefused,
    /// Connection timeout (no response within timeout period)
    ConnectionTimeout,
    /// Connection reset by peer
    ConnectionReset,
    /// Network unreachable
    NetworkUnreachable,
    /// Host unreachable
    HostUnreachable,
    /// Permission denied
    PermissionDenied,
    /// Invalid handshake response
    InvalidHandshake,
    /// Incompatible peer version
    IncompatibleVersion,
    /// Other IO error
    IoError,
}

impl ConnectionErrorType {
    /// Check if error is recoverable
    pub fn is_recoverable(&self) -> bool {
        matches!(
            self,
            ConnectionErrorType::ConnectionRefused
                | ConnectionErrorType::ConnectionTimeout
                | ConnectionErrorType::ConnectionReset
                | ConnectionErrorType::NetworkUnreachable
                | ConnectionErrorType::HostUnreachable
        )
    }

    /// Check if error is fatal (should not retry)
    pub fn is_fatal(&self) -> bool {
        matches!(
            self,
            ConnectionErrorType::PermissionDenied
                | ConnectionErrorType::InvalidHandshake
                | ConnectionErrorType::IncompatibleVersion
        )
    }

    /// Get human-readable description
    pub fn description(&self) -> &'static str {
        match self {
            ConnectionErrorType::ConnectionRefused => "Connection refused by peer",
            ConnectionErrorType::ConnectionTimeout => "Connection timeout",
            ConnectionErrorType::ConnectionReset => "Connection reset by peer",
            ConnectionErrorType::NetworkUnreachable => "Network unreachable",
            ConnectionErrorType::HostUnreachable => "Host unreachable",
            ConnectionErrorType::PermissionDenied => "Permission denied",
            ConnectionErrorType::InvalidHandshake => "Invalid handshake response",
            ConnectionErrorType::IncompatibleVersion => "Incompatible peer version",
            ConnectionErrorType::IoError => "IO error",
        }
    }
}

/// Connection error event for recovery processing
#[derive(Clone, Debug)]
pub struct ConnectionErrorEvent {
    /// Peer ID
    pub peer_id: String,
    /// Peer address
    pub peer_addr: SocketAddr,
    /// Error type
    pub error_type: ConnectionErrorType,
    /// Error message
    pub error_message: String,
    /// Timestamp of error
    pub timestamp: SystemTime,
    /// Attempt number
    pub attempt: u32,
}

/// Connection error recovery handler
pub struct ConnectionErrorRecovery {
    /// Peer manager reference
    peer_manager: Arc<PeerManager>,
    /// Reconnection manager
    reconnection_manager: Arc<ReconnectionManager>,
    /// Error event sender
    error_tx: mpsc::UnboundedSender<ConnectionErrorEvent>,
    /// Error event receiver
    error_rx: Arc<tokio::sync::Mutex<mpsc::UnboundedReceiver<ConnectionErrorEvent>>>,
    /// Error tracking per peer
    error_counts: Arc<DashMap<String, u32>>,
    /// Last error per peer
    last_errors: Arc<DashMap<String, (ConnectionErrorType, SystemTime)>>,
    /// Maximum consecutive errors before marking unhealthy
    max_consecutive_errors: u32,
}

impl ConnectionErrorRecovery {
    /// Create new connection error recovery handler
    pub fn new(
        peer_manager: Arc<PeerManager>,
        _connection_pool: Arc<ConnectionPool>,
        max_backoff_secs: u64,
        max_consecutive_errors: u32,
    ) -> Self {
        let (error_tx, error_rx) = mpsc::unbounded_channel();

        Self {
            peer_manager,
            reconnection_manager: Arc::new(ReconnectionManager::new(max_backoff_secs)),
            error_tx,
            error_rx: Arc::new(tokio::sync::Mutex::new(error_rx)),
            error_counts: Arc::new(DashMap::new()),
            last_errors: Arc::new(DashMap::new()),
            max_consecutive_errors,
        }
    }

    /// Handle connection error and schedule recovery
    pub async fn handle_connection_error(
        &self,
        peer_id: String,
        peer_addr: SocketAddr,
        error_type: ConnectionErrorType,
        error_message: String,
    ) -> Result<()> {
        let attempt = self
            .error_counts
            .entry(peer_id.clone())
            .or_insert(0)
            .value()
            + 1;

        // Update error count
        self.error_counts.insert(peer_id.clone(), attempt);

        // Record last error
        self.last_errors
            .insert(peer_id.clone(), (error_type.clone(), SystemTime::now()));

        // Log the error with context
        warn!(
            "Connection error for peer {}: {} (attempt: {}, error: {})",
            peer_id,
            error_type.description(),
            attempt,
            error_message
        );

        // Create error event
        let event = ConnectionErrorEvent {
            peer_id: peer_id.clone(),
            peer_addr,
            error_type: error_type.clone(),
            error_message: error_message.clone(),
            timestamp: SystemTime::now(),
            attempt,
        };

        // Send error event for processing
        if let Err(e) = self.error_tx.send(event) {
            error!("Failed to send connection error event: {}", e);
        }

        // Handle based on error type
        if error_type.is_fatal() {
            // Fatal errors: mark peer as unhealthy and don't retry
            self.peer_manager
                .mark_unhealthy(&peer_id, format!("Fatal error: {}", error_message))
                .await?;
            self.error_counts.remove(&peer_id);
            error!(
                "Fatal connection error for peer {}: {}. Peer marked unhealthy.",
                peer_id, error_message
            );
            return Ok(());
        }

        // Recoverable errors: mark unhealthy if too many consecutive errors
        if attempt >= self.max_consecutive_errors {
            self.peer_manager
                .mark_unhealthy(
                    &peer_id,
                    format!(
                        "Too many connection errors ({}/{}): {}",
                        attempt, self.max_consecutive_errors, error_message
                    ),
                )
                .await?;
            warn!(
                "Peer {} marked unhealthy after {} consecutive errors",
                peer_id, attempt
            );
        } else {
            // Still recoverable, mark as unhealthy but will retry
            self.peer_manager
                .mark_unhealthy(&peer_id, error_message.clone())
                .await?;
        }

        // Schedule reconnection with exponential backoff
        self.reconnection_manager
            .schedule_reconnect(peer_id.clone(), peer_addr)
            .await?;

        info!("Scheduled reconnection for peer {} with backoff", peer_id);

        Ok(())
    }

    /// Get next error event
    pub async fn next_error_event(&self) -> Option<ConnectionErrorEvent> {
        let mut rx = self.error_rx.lock().await;
        rx.recv().await
    }

    /// Reset error count for peer (on successful connection)
    pub async fn reset_error_count(&self, peer_id: &str) -> Result<()> {
        self.error_counts.remove(peer_id);
        debug!("Reset error count for peer: {}", peer_id);
        Ok(())
    }

    /// Get error count for peer
    pub async fn get_error_count(&self, peer_id: &str) -> u32 {
        self.error_counts
            .get(peer_id)
            .map(|c| *c.value())
            .unwrap_or(0)
    }

    /// Get last error for peer
    pub async fn get_last_error(&self, peer_id: &str) -> Option<(ConnectionErrorType, SystemTime)> {
        self.last_errors.get(peer_id).map(|e| e.value().clone())
    }

    /// Get all peers with active errors
    pub async fn get_peers_with_errors(&self) -> Vec<(String, u32)> {
        self.error_counts
            .iter()
            .map(|entry| (entry.key().clone(), *entry.value()))
            .collect()
    }

    /// Clear error state for peer
    pub async fn clear_error_state(&self, peer_id: &str) -> Result<()> {
        self.error_counts.remove(peer_id);
        self.last_errors.remove(peer_id);
        debug!("Cleared error state for peer: {}", peer_id);
        Ok(())
    }

    /// Get reconnection manager reference
    pub fn reconnection_manager(&self) -> Arc<ReconnectionManager> {
        self.reconnection_manager.clone()
    }

    /// Process reconnection event and attempt to reconnect
    pub async fn process_reconnection_event(&self, event: ReconnectionEvent) -> Result<()> {
        debug!(
            "Processing reconnection event for peer {} (attempt: {})",
            event.peer_id, event.attempt
        );

        // Check if peer still exists and is marked unhealthy
        if let Ok(peer) = self.peer_manager.get_peer(&event.peer_id).await {
            if !peer.is_healthy && peer.is_connected {
                // Peer is already connected, no need to reconnect
                debug!("Peer {} is already connected", event.peer_id);
                self.reset_error_count(&event.peer_id).await?;
                self.reconnection_manager
                    .reset_backoff(&event.peer_id)
                    .await?;
                return Ok(());
            }
        }

        // Reconnection will be attempted by the network manager
        // This handler just logs and tracks the event
        info!(
            "Reconnection event for peer {} scheduled (attempt: {}, backoff: {}s)",
            event.peer_id, event.attempt, event.backoff_secs
        );

        Ok(())
    }

    /// Get error statistics
    pub async fn get_error_statistics(&self) -> ErrorStatistics {
        let total_errors: u32 = self.error_counts.iter().map(|e| *e.value()).sum();
        let peers_with_errors = self.error_counts.len();

        ErrorStatistics {
            total_errors,
            peers_with_errors,
            max_consecutive_errors: self.max_consecutive_errors,
        }
    }

    /// Clear all error states
    pub async fn clear_all(&self) -> Result<()> {
        self.error_counts.clear();
        self.last_errors.clear();
        debug!("Cleared all error states");
        Ok(())
    }

    /// Check if peer should be retried
    pub async fn should_retry_peer(&self, peer_id: &str) -> bool {
        let error_count = self.get_error_count(peer_id).await;
        error_count < self.max_consecutive_errors
    }

    /// Get time until next retry for peer
    pub async fn time_until_retry(&self, peer_id: &str) -> Option<Duration> {
        self.reconnection_manager.time_until_retry(peer_id).await
    }
}

/// Error statistics
#[derive(Clone, Debug)]
pub struct ErrorStatistics {
    /// Total errors across all peers
    pub total_errors: u32,
    /// Number of peers with active errors
    pub peers_with_errors: usize,
    /// Maximum consecutive errors threshold
    pub max_consecutive_errors: u32,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::NodeRole;

    #[tokio::test]
    async fn test_connection_error_recovery_creation() {
        let peer_manager = Arc::new(PeerManager::new(1000));
        let connection_pool = Arc::new(ConnectionPool::new(100, 300));

        let recovery = ConnectionErrorRecovery::new(peer_manager, connection_pool, 300, 5);
        assert_eq!(recovery.max_consecutive_errors, 5);
    }

    #[tokio::test]
    async fn test_error_type_is_recoverable() {
        assert!(ConnectionErrorType::ConnectionRefused.is_recoverable());
        assert!(ConnectionErrorType::ConnectionTimeout.is_recoverable());
        assert!(ConnectionErrorType::ConnectionReset.is_recoverable());
        assert!(ConnectionErrorType::NetworkUnreachable.is_recoverable());
        assert!(ConnectionErrorType::HostUnreachable.is_recoverable());
    }

    #[tokio::test]
    async fn test_error_type_is_fatal() {
        assert!(ConnectionErrorType::PermissionDenied.is_fatal());
        assert!(ConnectionErrorType::InvalidHandshake.is_fatal());
        assert!(ConnectionErrorType::IncompatibleVersion.is_fatal());
    }

    #[tokio::test]
    async fn test_error_type_description() {
        assert_eq!(
            ConnectionErrorType::ConnectionRefused.description(),
            "Connection refused by peer"
        );
        assert_eq!(
            ConnectionErrorType::ConnectionTimeout.description(),
            "Connection timeout"
        );
    }

    #[tokio::test]
    async fn test_handle_recoverable_error() {
        let peer_manager = Arc::new(PeerManager::new(1000));
        let connection_pool = Arc::new(ConnectionPool::new(100, 300));

        peer_manager
            .add_peer(
                "peer1".to_string(),
                "127.0.0.1:9000".to_string(),
                NodeRole::Validator,
            )
            .await
            .unwrap();

        let recovery = ConnectionErrorRecovery::new(peer_manager.clone(), connection_pool, 300, 5);

        let addr: SocketAddr = "127.0.0.1:9000".parse().map_err(|e| format!("Parse failed: {}", e))?;
        recovery
            .handle_connection_error(
                "peer1".to_string(),
                addr,
                ConnectionErrorType::ConnectionRefused,
                "Connection refused".to_string(),
            )
            .await
            .unwrap();

        let error_count = recovery.get_error_count("peer1").await;
        assert_eq!(error_count, 1);

        let peer = peer_manager.get_peer("peer1").await.unwrap();
        assert!(!peer.is_healthy);
    }

    #[tokio::test]
    async fn test_handle_fatal_error() {
        let peer_manager = Arc::new(PeerManager::new(1000));
        let connection_pool = Arc::new(ConnectionPool::new(100, 300));

        peer_manager
            .add_peer(
                "peer1".to_string(),
                "127.0.0.1:9000".to_string(),
                NodeRole::Validator,
            )
            .await
            .unwrap();

        let recovery = ConnectionErrorRecovery::new(peer_manager.clone(), connection_pool, 300, 5);

        let addr: SocketAddr = "127.0.0.1:9000".parse().map_err(|e| format!("Parse failed: {}", e))?;
        recovery
            .handle_connection_error(
                "peer1".to_string(),
                addr,
                ConnectionErrorType::InvalidHandshake,
                "Invalid handshake".to_string(),
            )
            .await
            .unwrap();

        // Fatal errors should not increment error count
        let error_count = recovery.get_error_count("peer1").await;
        assert_eq!(error_count, 0);

        let peer = peer_manager.get_peer("peer1").await.unwrap();
        assert!(!peer.is_healthy);
    }

    #[tokio::test]
    async fn test_reset_error_count() {
        let peer_manager = Arc::new(PeerManager::new(1000));
        let connection_pool = Arc::new(ConnectionPool::new(100, 300));

        peer_manager
            .add_peer(
                "peer1".to_string(),
                "127.0.0.1:9000".to_string(),
                NodeRole::Validator,
            )
            .await
            .unwrap();

        let recovery = ConnectionErrorRecovery::new(peer_manager, connection_pool, 300, 5);

        let addr: SocketAddr = "127.0.0.1:9000".parse().map_err(|e| format!("Parse failed: {}", e))?;
        recovery
            .handle_connection_error(
                "peer1".to_string(),
                addr,
                ConnectionErrorType::ConnectionRefused,
                "Connection refused".to_string(),
            )
            .await
            .unwrap();

        assert_eq!(recovery.get_error_count("peer1").await, 1);

        recovery.reset_error_count("peer1").await.unwrap();
        assert_eq!(recovery.get_error_count("peer1").await, 0);
    }

    #[tokio::test]
    async fn test_get_last_error() {
        let peer_manager = Arc::new(PeerManager::new(1000));
        let connection_pool = Arc::new(ConnectionPool::new(100, 300));

        peer_manager
            .add_peer(
                "peer1".to_string(),
                "127.0.0.1:9000".to_string(),
                NodeRole::Validator,
            )
            .await
            .unwrap();

        let recovery = ConnectionErrorRecovery::new(peer_manager, connection_pool, 300, 5);

        let addr: SocketAddr = "127.0.0.1:9000".parse().map_err(|e| format!("Parse failed: {}", e))?;
        recovery
            .handle_connection_error(
                "peer1".to_string(),
                addr,
                ConnectionErrorType::ConnectionTimeout,
                "Connection timeout".to_string(),
            )
            .await
            .unwrap();

        let last_error = recovery.get_last_error("peer1").await;
        assert!(last_error.is_some());
        let (error_type, _) = last_error.unwrap();
        assert_eq!(error_type, ConnectionErrorType::ConnectionTimeout);
    }

    #[tokio::test]
    async fn test_get_peers_with_errors() {
        let peer_manager = Arc::new(PeerManager::new(1000));
        let connection_pool = Arc::new(ConnectionPool::new(100, 300));

        peer_manager
            .add_peer(
                "peer1".to_string(),
                "127.0.0.1:9000".to_string(),
                NodeRole::Validator,
            )
            .await
            .unwrap();
        peer_manager
            .add_peer(
                "peer2".to_string(),
                "127.0.0.1:9001".to_string(),
                NodeRole::Validator,
            )
            .await
            .unwrap();

        let recovery = ConnectionErrorRecovery::new(peer_manager, connection_pool, 300, 5);

        let addr1: SocketAddr = "127.0.0.1:9000".parse().map_err(|e| format!("Parse failed: {}", e))?;
        let addr2: SocketAddr = "127.0.0.1:9001".parse().map_err(|e| format!("Parse failed: {}", e))?;

        recovery
            .handle_connection_error(
                "peer1".to_string(),
                addr1,
                ConnectionErrorType::ConnectionRefused,
                "Connection refused".to_string(),
            )
            .await
            .unwrap();

        recovery
            .handle_connection_error(
                "peer2".to_string(),
                addr2,
                ConnectionErrorType::ConnectionTimeout,
                "Connection timeout".to_string(),
            )
            .await
            .unwrap();

        let peers_with_errors = recovery.get_peers_with_errors().await;
        assert_eq!(peers_with_errors.len(), 2);
    }

    #[tokio::test]
    async fn test_clear_error_state() {
        let peer_manager = Arc::new(PeerManager::new(1000));
        let connection_pool = Arc::new(ConnectionPool::new(100, 300));

        peer_manager
            .add_peer(
                "peer1".to_string(),
                "127.0.0.1:9000".to_string(),
                NodeRole::Validator,
            )
            .await
            .unwrap();

        let recovery = ConnectionErrorRecovery::new(peer_manager, connection_pool, 300, 5);

        let addr: SocketAddr = "127.0.0.1:9000".parse().map_err(|e| format!("Parse failed: {}", e))?;
        recovery
            .handle_connection_error(
                "peer1".to_string(),
                addr,
                ConnectionErrorType::ConnectionRefused,
                "Connection refused".to_string(),
            )
            .await
            .unwrap();

        assert_eq!(recovery.get_error_count("peer1").await, 1);
        assert!(recovery.get_last_error("peer1").await.is_some());

        recovery.clear_error_state("peer1").await.unwrap();

        assert_eq!(recovery.get_error_count("peer1").await, 0);
        assert!(recovery.get_last_error("peer1").await.is_none());
    }

    #[tokio::test]
    async fn test_should_retry_peer() {
        let peer_manager = Arc::new(PeerManager::new(1000));
        let connection_pool = Arc::new(ConnectionPool::new(100, 300));

        peer_manager
            .add_peer(
                "peer1".to_string(),
                "127.0.0.1:9000".to_string(),
                NodeRole::Validator,
            )
            .await
            .unwrap();

        let recovery = ConnectionErrorRecovery::new(peer_manager, connection_pool, 300, 3);

        let addr: SocketAddr = "127.0.0.1:9000".parse().map_err(|e| format!("Parse failed: {}", e))?;

        // First error
        recovery
            .handle_connection_error(
                "peer1".to_string(),
                addr,
                ConnectionErrorType::ConnectionRefused,
                "Connection refused".to_string(),
            )
            .await
            .unwrap();
        assert!(recovery.should_retry_peer("peer1").await);

        // Second error
        recovery
            .handle_connection_error(
                "peer1".to_string(),
                addr,
                ConnectionErrorType::ConnectionRefused,
                "Connection refused".to_string(),
            )
            .await
            .unwrap();
        assert!(recovery.should_retry_peer("peer1").await);

        // Third error (at threshold)
        recovery
            .handle_connection_error(
                "peer1".to_string(),
                addr,
                ConnectionErrorType::ConnectionRefused,
                "Connection refused".to_string(),
            )
            .await
            .unwrap();
        assert!(!recovery.should_retry_peer("peer1").await);
    }

    #[tokio::test]
    async fn test_get_error_statistics() {
        let peer_manager = Arc::new(PeerManager::new(1000));
        let connection_pool = Arc::new(ConnectionPool::new(100, 300));

        peer_manager
            .add_peer(
                "peer1".to_string(),
                "127.0.0.1:9000".to_string(),
                NodeRole::Validator,
            )
            .await
            .unwrap();
        peer_manager
            .add_peer(
                "peer2".to_string(),
                "127.0.0.1:9001".to_string(),
                NodeRole::Validator,
            )
            .await
            .unwrap();

        let recovery = ConnectionErrorRecovery::new(peer_manager, connection_pool, 300, 5);

        let addr1: SocketAddr = "127.0.0.1:9000".parse().map_err(|e| format!("Parse failed: {}", e))?;
        let addr2: SocketAddr = "127.0.0.1:9001".parse().map_err(|e| format!("Parse failed: {}", e))?;

        recovery
            .handle_connection_error(
                "peer1".to_string(),
                addr1,
                ConnectionErrorType::ConnectionRefused,
                "Connection refused".to_string(),
            )
            .await
            .unwrap();

        recovery
            .handle_connection_error(
                "peer2".to_string(),
                addr2,
                ConnectionErrorType::ConnectionTimeout,
                "Connection timeout".to_string(),
            )
            .await
            .unwrap();

        recovery
            .handle_connection_error(
                "peer2".to_string(),
                addr2,
                ConnectionErrorType::ConnectionTimeout,
                "Connection timeout".to_string(),
            )
            .await
            .unwrap();

        let stats = recovery.get_error_statistics().await;
        assert_eq!(stats.total_errors, 3);
        assert_eq!(stats.peers_with_errors, 2);
        assert_eq!(stats.max_consecutive_errors, 5);
    }

    #[tokio::test]
    async fn test_clear_all() {
        let peer_manager = Arc::new(PeerManager::new(1000));
        let connection_pool = Arc::new(ConnectionPool::new(100, 300));

        peer_manager
            .add_peer(
                "peer1".to_string(),
                "127.0.0.1:9000".to_string(),
                NodeRole::Validator,
            )
            .await
            .unwrap();

        let recovery = ConnectionErrorRecovery::new(peer_manager, connection_pool, 300, 5);

        let addr: SocketAddr = "127.0.0.1:9000".parse().map_err(|e| format!("Parse failed: {}", e))?;
        recovery
            .handle_connection_error(
                "peer1".to_string(),
                addr,
                ConnectionErrorType::ConnectionRefused,
                "Connection refused".to_string(),
            )
            .await
            .unwrap();

        assert_eq!(recovery.get_error_count("peer1").await, 1);

        recovery.clear_all().await.unwrap();

        assert_eq!(recovery.get_error_count("peer1").await, 0);
        let stats = recovery.get_error_statistics().await;
        assert_eq!(stats.total_errors, 0);
    }

    #[tokio::test]
    async fn test_consecutive_errors_mark_unhealthy() {
        let peer_manager = Arc::new(PeerManager::new(1000));
        let connection_pool = Arc::new(ConnectionPool::new(100, 300));

        peer_manager
            .add_peer(
                "peer1".to_string(),
                "127.0.0.1:9000".to_string(),
                NodeRole::Validator,
            )
            .await
            .unwrap();

        let recovery = ConnectionErrorRecovery::new(peer_manager.clone(), connection_pool, 300, 2);

        let addr: SocketAddr = "127.0.0.1:9000".parse().map_err(|e| format!("Parse failed: {}", e))?;

        // First error
        recovery
            .handle_connection_error(
                "peer1".to_string(),
                addr,
                ConnectionErrorType::ConnectionRefused,
                "Connection refused".to_string(),
            )
            .await
            .unwrap();

        let peer = peer_manager.get_peer("peer1").await.unwrap();
        assert!(!peer.is_healthy);

        // Second error (at threshold)
        recovery
            .handle_connection_error(
                "peer1".to_string(),
                addr,
                ConnectionErrorType::ConnectionRefused,
                "Connection refused".to_string(),
            )
            .await
            .unwrap();

        let peer = peer_manager.get_peer("peer1").await.unwrap();
        assert!(!peer.is_healthy);
    }
}
