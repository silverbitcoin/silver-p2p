//! Shutdown coordination for graceful P2P network shutdown
//!
//! This module handles coordinated shutdown of all P2P network components:
//! 1. Stop accepting new connections
//! 2. Wait for pending messages to complete (up to 30 seconds)
//! 3. Stop peer discovery and bootstrap components
//! 4. Close all peer connections
//! 5. Clear all peer state
//! 6. Release network resources

use crate::bootstrap_connector::BootstrapConnector;
use crate::connection_pool::ConnectionPool;
use crate::error::{P2PError, Result};
use crate::peer_discovery_coordinator::PeerDiscoveryCoordinator;
use crate::peer_manager::PeerManager;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::broadcast;
use tracing::{debug, info, warn};

/// Coordinates graceful shutdown of all P2P network components
pub struct ShutdownCoordinator {
    /// Peer manager reference
    peer_manager: Arc<PeerManager>,
    /// Connection pool reference
    connection_pool: Arc<ConnectionPool>,
    /// Peer discovery coordinator reference
    discovery_coordinator: Arc<PeerDiscoveryCoordinator>,
    /// Bootstrap connector reference
    bootstrap_connector: Arc<BootstrapConnector>,
    /// Shutdown signal broadcaster
    shutdown_tx: broadcast::Sender<()>,
    /// Whether shutdown is in progress
    is_shutting_down: Arc<AtomicBool>,
    /// Grace period for pending messages in seconds
    grace_period_secs: u64,
}

/// Statistics about the shutdown process
#[derive(Clone, Debug)]
pub struct ShutdownStats {
    /// Total time taken for shutdown in milliseconds
    pub shutdown_time_ms: u128,
    /// Number of peer connections closed
    pub connections_closed: usize,
    /// Number of peers cleared
    pub peers_cleared: usize,
    /// Number of candidates cleared
    pub candidates_cleared: usize,
    /// Whether grace period was exceeded
    pub grace_period_exceeded: bool,
    /// Whether all components stopped successfully
    pub all_components_stopped: bool,
}

impl ShutdownCoordinator {
    /// Create new shutdown coordinator
    pub fn new(
        peer_manager: Arc<PeerManager>,
        connection_pool: Arc<ConnectionPool>,
        discovery_coordinator: Arc<PeerDiscoveryCoordinator>,
        bootstrap_connector: Arc<BootstrapConnector>,
        shutdown_tx: broadcast::Sender<()>,
    ) -> Self {
        Self {
            peer_manager,
            connection_pool,
            discovery_coordinator,
            bootstrap_connector,
            shutdown_tx,
            is_shutting_down: Arc::new(AtomicBool::new(false)),
            grace_period_secs: 30,
        }
    }

    /// Create new shutdown coordinator with custom grace period
    pub fn with_grace_period(
        peer_manager: Arc<PeerManager>,
        connection_pool: Arc<ConnectionPool>,
        discovery_coordinator: Arc<PeerDiscoveryCoordinator>,
        bootstrap_connector: Arc<BootstrapConnector>,
        shutdown_tx: broadcast::Sender<()>,
        grace_period_secs: u64,
    ) -> Self {
        Self {
            peer_manager,
            connection_pool,
            discovery_coordinator,
            bootstrap_connector,
            shutdown_tx,
            is_shutting_down: Arc::new(AtomicBool::new(false)),
            grace_period_secs,
        }
    }

    /// Check if shutdown is in progress
    pub fn is_shutting_down(&self) -> bool {
        self.is_shutting_down.load(Ordering::SeqCst)
    }

    /// Perform coordinated graceful shutdown
    ///
    /// This method implements a coordinated shutdown sequence:
    /// 1. Stop accepting new connections
    /// 2. Wait up to grace_period_secs for pending messages to complete
    /// 3. Stop peer discovery coordinator
    /// 4. Stop bootstrap connector
    /// 5. Close all peer connections
    /// 6. Clear all peer state
    /// 7. Release network resources
    ///
    /// # Returns
    /// * `Result<ShutdownStats>` - Shutdown statistics or error
    pub async fn shutdown(&self) -> Result<ShutdownStats> {
        // Check if already shutting down
        if self.is_shutting_down.swap(true, Ordering::SeqCst) {
            return Err(P2PError::NetworkError(
                "Shutdown already in progress".to_string(),
            ));
        }

        let shutdown_start = SystemTime::now();
        let mut stats = ShutdownStats {
            shutdown_time_ms: 0,
            connections_closed: 0,
            peers_cleared: 0,
            candidates_cleared: 0,
            grace_period_exceeded: false,
            all_components_stopped: true,
        };

        info!("Starting coordinated shutdown of P2P network");

        // Step 1: Stop accepting new connections
        info!("Step 1: Stopping acceptance of new connections");
        match self.stop_accepting_connections().await {
            Ok(_) => info!("Stopped accepting new connections"),
            Err(e) => {
                warn!("Error stopping connection acceptance: {}", e);
                stats.all_components_stopped = false;
            }
        }

        // Step 2: Wait for pending messages with grace period
        info!(
            "Step 2: Waiting up to {} seconds for pending messages to complete",
            self.grace_period_secs
        );
        match self
            .wait_for_pending_messages(Duration::from_secs(self.grace_period_secs))
            .await
        {
            Ok(exceeded) => {
                if exceeded {
                    warn!("Grace period exceeded, proceeding with shutdown");
                    stats.grace_period_exceeded = true;
                } else {
                    info!("All pending messages completed within grace period");
                }
            }
            Err(e) => {
                warn!("Error waiting for pending messages: {}", e);
                stats.all_components_stopped = false;
            }
        }

        // Step 3: Stop peer discovery coordinator
        info!("Step 3: Stopping peer discovery coordinator");
        match self.stop_discovery_coordinator().await {
            Ok(_) => info!("Peer discovery coordinator stopped"),
            Err(e) => {
                warn!("Error stopping peer discovery coordinator: {}", e);
                stats.all_components_stopped = false;
            }
        }

        // Step 4: Stop bootstrap connector
        info!("Step 4: Stopping bootstrap connector");
        match self.stop_bootstrap_connector().await {
            Ok(_) => info!("Bootstrap connector stopped"),
            Err(e) => {
                warn!("Error stopping bootstrap connector: {}", e);
                stats.all_components_stopped = false;
            }
        }

        // Step 5: Close all peer connections
        info!("Step 5: Closing all peer connections");
        match self.close_all_connections().await {
            Ok(count) => {
                info!("Closed {} peer connections", count);
                stats.connections_closed = count;
            }
            Err(e) => {
                warn!("Error closing peer connections: {}", e);
                stats.all_components_stopped = false;
            }
        }

        // Step 6: Clear all peer state
        info!("Step 6: Clearing peer state");
        match self.clear_peer_state().await {
            Ok((peers_cleared, candidates_cleared)) => {
                info!(
                    "Cleared {} peers and {} candidates",
                    peers_cleared, candidates_cleared
                );
                stats.peers_cleared = peers_cleared;
                stats.candidates_cleared = candidates_cleared;
            }
            Err(e) => {
                warn!("Error clearing peer state: {}", e);
                stats.all_components_stopped = false;
            }
        }

        // Step 7: Release resources
        info!("Step 7: Releasing network resources");
        self.release_resources().await;

        // Calculate shutdown time
        if let Ok(elapsed) = shutdown_start.elapsed() {
            stats.shutdown_time_ms = elapsed.as_millis();
        }

        info!(
            "P2P network shutdown complete in {} ms",
            stats.shutdown_time_ms
        );

        Ok(stats)
    }

    /// Stop accepting new connections
    async fn stop_accepting_connections(&self) -> Result<()> {
        // Send shutdown signal to stop accepting connections
        let _ = self.shutdown_tx.send(());

        // Give the listener a moment to stop accepting
        tokio::time::sleep(Duration::from_millis(100)).await;

        Ok(())
    }

    /// Wait for pending messages to complete with timeout
    async fn wait_for_pending_messages(&self, grace_period: Duration) -> Result<bool> {
        let grace_start = SystemTime::now();

        loop {
            // Check if there are any pending operations
            let peers = self.peer_manager.get_all_peers().await;
            let has_pending = peers
                .iter()
                .any(|p| p.messages_sent > 0 || p.messages_received > 0);

            if !has_pending {
                debug!("No pending messages detected");
                return Ok(false);
            }

            if let Ok(elapsed) = grace_start.elapsed() {
                if elapsed >= grace_period {
                    warn!("Grace period expired, proceeding with shutdown");
                    return Ok(true);
                }
            }

            // Wait a bit before checking again
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    /// Stop peer discovery coordinator
    async fn stop_discovery_coordinator(&self) -> Result<()> {
        if self.discovery_coordinator.is_running() {
            self.discovery_coordinator.stop().await?;
            debug!("Peer discovery coordinator stopped");
        } else {
            debug!("Peer discovery coordinator not running");
        }

        Ok(())
    }

    /// Stop bootstrap connector
    async fn stop_bootstrap_connector(&self) -> Result<()> {
        if self.bootstrap_connector.is_running() {
            self.bootstrap_connector.stop().await?;
            debug!("Bootstrap connector stopped");
        } else {
            debug!("Bootstrap connector not running");
        }

        Ok(())
    }

    /// Close all peer connections
    async fn close_all_connections(&self) -> Result<usize> {
        let peers = self.peer_manager.get_all_peers().await;
        let peer_count = peers.len();

        for peer in peers {
            match self.connection_pool.remove_connection(&peer.peer_id).await {
                Ok(_) => debug!("Closed connection to peer: {}", peer.peer_id),
                Err(e) => warn!("Error closing connection to peer {}: {}", peer.peer_id, e),
            }
        }

        Ok(peer_count)
    }

    /// Clear all peer state
    async fn clear_peer_state(&self) -> Result<(usize, usize)> {
        let peers = self.peer_manager.get_all_peers().await;
        let peers_count = peers.len();

        let candidates = self.peer_manager.get_candidates().await;
        let candidates_count = candidates.len();

        self.peer_manager.clear_all().await?;

        Ok((peers_count, candidates_count))
    }

    /// Release network resources
    async fn release_resources(&self) {
        debug!("Releasing network resources");

        // Close all active peer connections through peer manager
        let peers = self.peer_manager.get_all_peers().await;
        for peer in peers {
            debug!("Closing connection to peer: {}", peer.peer_id);
            let _ = self.peer_manager.mark_disconnected(&peer.peer_id).await;
            let _ = self.connection_pool.remove_connection(&peer.peer_id).await;
        }

        // Clear all connections in the connection pool
        let _ = self.connection_pool.clear_all().await;

        // Stop discovery coordinator
        let _ = self.discovery_coordinator.stop().await;

        // Stop bootstrap connector
        let _ = self.bootstrap_connector.stop().await;

        info!("Network resources released successfully");
    }

    /// Propagate shutdown signal to all components
    pub fn propagate_shutdown_signal(&self) -> Result<()> {
        let _ = self.shutdown_tx.send(());
        Ok(())
    }

    /// Get grace period in seconds
    pub fn get_grace_period_secs(&self) -> u64 {
        self.grace_period_secs
    }

    /// Set grace period in seconds
    pub fn set_grace_period_secs(&mut self, grace_period_secs: u64) {
        self.grace_period_secs = grace_period_secs;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bootstrap_connector::BootstrapConnector;
    use crate::config::NetworkConfig;
    use crate::connection_pool::ConnectionPool;
    use crate::message_handler::MessageHandler;
    use crate::peer_discovery_coordinator::PeerDiscoveryCoordinator;
    use crate::peer_manager::PeerManager;
    use crate::types::NodeRole;
    use crate::unicast::UnicastManager;

    fn create_test_coordinator() -> ShutdownCoordinator {
        let config = NetworkConfig::new("test_node".to_string(), NodeRole::Validator);
        let peer_manager = Arc::new(PeerManager::new(1000));
        let connection_pool = Arc::new(ConnectionPool::new(100, 300));
        let message_handler = Arc::new(MessageHandler::new(100 * 1024 * 1024));
        let unicast_manager = Arc::new(UnicastManager::new(
            peer_manager.clone(),
            connection_pool.clone(),
            message_handler.clone(),
            5,
            "test_node".to_string(),
        ));

        let discovery_coordinator = Arc::new(PeerDiscoveryCoordinator::new(
            peer_manager.clone(),
            connection_pool.clone(),
            message_handler.clone(),
            unicast_manager,
            config.clone(),
        ));

        let bootstrap_connector = Arc::new(BootstrapConnector::new(
            peer_manager.clone(),
            connection_pool.clone(),
            config,
        ));

        let (shutdown_tx, _) = broadcast::channel(1);

        ShutdownCoordinator::new(
            peer_manager,
            connection_pool,
            discovery_coordinator,
            bootstrap_connector,
            shutdown_tx,
        )
    }

    #[tokio::test]
    async fn test_shutdown_coordinator_creation() {
        let coordinator = create_test_coordinator();
        assert!(!coordinator.is_shutting_down());
    }

    #[tokio::test]
    async fn test_shutdown_signal_propagation() {
        let coordinator = create_test_coordinator();
        let result = coordinator.propagate_shutdown_signal();
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_shutdown_basic() {
        let coordinator = create_test_coordinator();
        let result = coordinator.shutdown().await;
        assert!(result.is_ok());

        let stats = result.unwrap();
        assert_eq!(stats.peers_cleared, 0);
        assert_eq!(stats.candidates_cleared, 0);
        assert!(!stats.grace_period_exceeded);
    }

    #[tokio::test]
    async fn test_shutdown_with_peers() {
        let coordinator = create_test_coordinator();

        // Add some peers
        coordinator
            .peer_manager
            .add_peer(
                "peer1".to_string(),
                "127.0.0.1:9000".to_string(),
                NodeRole::Validator,
            )
            .await
            .unwrap();

        coordinator
            .peer_manager
            .add_peer(
                "peer2".to_string(),
                "127.0.0.1:9001".to_string(),
                NodeRole::Validator,
            )
            .await
            .unwrap();

        let result = coordinator.shutdown().await;
        assert!(result.is_ok());

        let stats = result.unwrap();
        assert_eq!(stats.peers_cleared, 2);
        assert_eq!(stats.connections_closed, 2);
    }

    #[tokio::test]
    async fn test_shutdown_with_candidates() {
        let coordinator = create_test_coordinator();

        // Add some candidates
        coordinator
            .peer_manager
            .add_candidates(vec![
                "192.168.1.1:9000".to_string(),
                "192.168.1.2:9000".to_string(),
            ])
            .await
            .unwrap();

        let result = coordinator.shutdown().await;
        assert!(result.is_ok());

        let stats = result.unwrap();
        assert_eq!(stats.candidates_cleared, 2);
    }

    #[tokio::test]
    async fn test_shutdown_idempotent() {
        let coordinator = create_test_coordinator();

        // First shutdown
        let result1 = coordinator.shutdown().await;
        assert!(result1.is_ok());

        // Second shutdown should fail (already shutting down)
        let result2 = coordinator.shutdown().await;
        assert!(result2.is_err());
    }

    #[tokio::test]
    async fn test_shutdown_with_custom_grace_period() {
        let config = NetworkConfig::new("test_node".to_string(), NodeRole::Validator);
        let peer_manager = Arc::new(PeerManager::new(1000));
        let connection_pool = Arc::new(ConnectionPool::new(100, 300));
        let message_handler = Arc::new(MessageHandler::new(100 * 1024 * 1024));
        let unicast_manager = Arc::new(UnicastManager::new(
            peer_manager.clone(),
            connection_pool.clone(),
            message_handler.clone(),
            5,
            "test_node".to_string(),
        ));

        let discovery_coordinator = Arc::new(PeerDiscoveryCoordinator::new(
            peer_manager.clone(),
            connection_pool.clone(),
            message_handler.clone(),
            unicast_manager,
            config.clone(),
        ));

        let bootstrap_connector = Arc::new(BootstrapConnector::new(
            peer_manager.clone(),
            connection_pool.clone(),
            config,
        ));

        let (shutdown_tx, _) = broadcast::channel(1);

        let coordinator = ShutdownCoordinator::with_grace_period(
            peer_manager,
            connection_pool,
            discovery_coordinator,
            bootstrap_connector,
            shutdown_tx,
            5, // 5 second grace period
        );

        assert_eq!(coordinator.get_grace_period_secs(), 5);

        let result = coordinator.shutdown().await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_shutdown_stats_collection() {
        let coordinator = create_test_coordinator();

        // Add peers
        coordinator
            .peer_manager
            .add_peer(
                "peer1".to_string(),
                "127.0.0.1:9000".to_string(),
                NodeRole::Validator,
            )
            .await
            .unwrap();

        coordinator
            .peer_manager
            .add_candidates(vec!["192.168.1.1:9000".to_string()])
            .await
            .unwrap();

        let result = coordinator.shutdown().await;
        assert!(result.is_ok());

        let stats = result.unwrap();
        assert_eq!(stats.peers_cleared, 1);
        assert_eq!(stats.candidates_cleared, 1);
        assert_eq!(stats.connections_closed, 1);
        assert!(!stats.grace_period_exceeded);
        assert!(stats.shutdown_time_ms > 0);
    }

    #[tokio::test]
    async fn test_shutdown_signal_propagation_multiple_times() {
        let coordinator = create_test_coordinator();

        // Propagate signal multiple times
        assert!(coordinator.propagate_shutdown_signal().is_ok());
        assert!(coordinator.propagate_shutdown_signal().is_ok());
        assert!(coordinator.propagate_shutdown_signal().is_ok());
    }

    #[tokio::test]
    async fn test_shutdown_with_mixed_peer_states() {
        let coordinator = create_test_coordinator();

        // Add peers in different states
        coordinator
            .peer_manager
            .add_peer(
                "peer1".to_string(),
                "127.0.0.1:9000".to_string(),
                NodeRole::Validator,
            )
            .await
            .unwrap();

        coordinator
            .peer_manager
            .add_peer(
                "peer2".to_string(),
                "127.0.0.1:9001".to_string(),
                NodeRole::Validator,
            )
            .await
            .unwrap();

        // Mark one as connected
        coordinator
            .peer_manager
            .mark_connected("peer1")
            .await
            .unwrap();

        // Mark one as unhealthy
        coordinator
            .peer_manager
            .mark_unhealthy("peer2", "Test error".to_string())
            .await
            .unwrap();

        let result = coordinator.shutdown().await;
        assert!(result.is_ok());

        let stats = result.unwrap();
        assert_eq!(stats.peers_cleared, 2);
    }
}
