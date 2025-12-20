//! Bootstrap node connection management with retry logic and health tracking

use crate::config::NetworkConfig;
use crate::connection_pool::ConnectionPool;
use crate::error::{P2PError, Result};
use crate::peer_manager::PeerManager;
use crate::types::NodeRole;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::net::TcpStream;
use tokio::sync::RwLock;
use tokio::time::{interval, timeout};
use tracing::{debug, info, warn};

/// Tracks the state of a bootstrap node connection attempt
#[derive(Clone, Debug)]
pub struct BootstrapNodeAttempt {
    /// Bootstrap node address
    pub address: String,
    /// Timestamp of last connection attempt
    pub last_attempt: SystemTime,
    /// Number of consecutive failures
    pub failure_count: u32,
    /// Whether this node is currently connected
    pub is_connected: bool,
    /// Whether a connection attempt is in progress
    pub in_progress: bool,
    /// Total successful connections from this bootstrap node
    pub successful_connections: u64,
    /// Total failed connection attempts
    pub total_failures: u64,
}

/// Statistics for bootstrap node connections
#[derive(Clone, Debug)]
pub struct BootstrapConnectionStats {
    /// Total connection attempts to bootstrap nodes
    pub total_attempts: u64,
    /// Successful connections to bootstrap nodes
    pub successful_connections: u64,
    /// Failed connection attempts
    pub failed_attempts: u64,
    /// Number of bootstrap nodes currently connected
    pub connected_bootstrap_nodes: usize,
    /// Number of bootstrap nodes being tracked
    pub tracked_bootstrap_nodes: usize,
    /// Average time to connect to bootstrap node (ms)
    pub avg_connection_time_ms: u64,
}

/// Manages bootstrap node connections with retry logic and health tracking
pub struct BootstrapConnector {
    /// Peer manager reference
    peer_manager: Arc<PeerManager>,
    /// Connection pool reference
    connection_pool: Arc<ConnectionPool>,
    /// Network configuration
    config: NetworkConfig,
    /// Tracks bootstrap node connection attempts
    pub bootstrap_attempts: Arc<RwLock<std::collections::HashMap<String, BootstrapNodeAttempt>>>,
    /// Whether connector is running
    is_running: Arc<AtomicBool>,
    /// Statistics
    stats: Arc<RwLock<BootstrapConnectionStats>>,
    /// Shutdown signal
    shutdown_tx: tokio::sync::broadcast::Sender<()>,
}

impl BootstrapConnector {
    /// Create new bootstrap connector
    pub fn new(
        peer_manager: Arc<PeerManager>,
        connection_pool: Arc<ConnectionPool>,
        config: NetworkConfig,
    ) -> Self {
        let (shutdown_tx, _) = tokio::sync::broadcast::channel(1);

        Self {
            peer_manager,
            connection_pool,
            config,
            bootstrap_attempts: Arc::new(RwLock::new(std::collections::HashMap::new())),
            is_running: Arc::new(AtomicBool::new(false)),
            stats: Arc::new(RwLock::new(BootstrapConnectionStats {
                total_attempts: 0,
                successful_connections: 0,
                failed_attempts: 0,
                connected_bootstrap_nodes: 0,
                tracked_bootstrap_nodes: 0,
                avg_connection_time_ms: 0,
            })),
            shutdown_tx,
        }
    }

    /// Start the bootstrap connector
    pub async fn start(&self) -> Result<()> {
        if self.is_running.swap(true, Ordering::SeqCst) {
            return Err(P2PError::NetworkError(
                "Bootstrap connector already running".to_string(),
            ));
        }

        info!(
            "Starting bootstrap connector with {} bootstrap nodes",
            self.config.bootstrap_nodes.len()
        );

        // Initialize bootstrap node tracking
        {
            let mut attempts = self.bootstrap_attempts.write().await;
            for bootstrap_addr in &self.config.bootstrap_nodes {
                attempts.insert(
                    bootstrap_addr.clone(),
                    BootstrapNodeAttempt {
                        address: bootstrap_addr.clone(),
                        last_attempt: SystemTime::now(),
                        failure_count: 0,
                        is_connected: false,
                        in_progress: false,
                        successful_connections: 0,
                        total_failures: 0,
                    },
                );
            }

            let mut stats = self.stats.write().await;
            stats.tracked_bootstrap_nodes = attempts.len();
        }

        let peer_manager = self.peer_manager.clone();
        let connection_pool = self.connection_pool.clone();
        let config = self.config.clone();
        let bootstrap_attempts = self.bootstrap_attempts.clone();
        let stats = self.stats.clone();
        let mut shutdown_rx = self.shutdown_tx.subscribe();

        tokio::spawn(async move {
            let mut ticker = interval(Duration::from_secs(5));

            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        // Check if we have any connected bootstrap nodes
                        let connected_count = {
                            let attempts = bootstrap_attempts.read().await;
                            attempts.iter().filter(|(_, a)| a.is_connected).count()
                        };

                        // If no bootstrap nodes connected and we have zero peers, attempt connections
                        let peer_count = peer_manager.get_connected_peer_count().await;
                        if connected_count == 0 && peer_count == 0 {
                            debug!("No bootstrap nodes connected and no peers, attempting bootstrap connections");

                            let bootstrap_addrs: Vec<String> = {
                                let attempts = bootstrap_attempts.read().await;
                                attempts.keys().cloned().collect()
                            };

                            for bootstrap_addr in bootstrap_addrs {
                                let should_attempt = {
                                    let mut attempts = bootstrap_attempts.write().await;
                                    let attempt = attempts.get_mut(&bootstrap_addr).unwrap();

                                    // Check if enough time has passed since last attempt
                                    let backoff_secs = std::cmp::min(
                                        (1u64 << attempt.failure_count.min(8)) as u64,
                                        config.max_backoff_secs,
                                    );

                                    if let Ok(elapsed) = SystemTime::now().duration_since(attempt.last_attempt) {
                                        elapsed.as_secs() >= backoff_secs && !attempt.in_progress
                                    } else {
                                        false
                                    }
                                };

                                if should_attempt {
                                    // Mark as in progress
                                    {
                                        let mut attempts = bootstrap_attempts.write().await;
                                        if let Some(attempt) = attempts.get_mut(&bootstrap_addr) {
                                            attempt.in_progress = true;
                                            attempt.last_attempt = SystemTime::now();
                                        }
                                    }

                                    // Attempt connection
                                    let start_time = SystemTime::now();
                                    let peer_id = format!("bootstrap_{}", bootstrap_addr.replace(":", "_"));

                                    let connection_result = timeout(
                                        Duration::from_secs(config.connection_timeout_secs),
                                        TcpStream::connect(&bootstrap_addr),
                                    )
                                    .await;

                                    let connection_time_ms = start_time
                                        .elapsed()
                                        .unwrap_or_default()
                                        .as_millis() as u64;

                                    match connection_result {
                                        Ok(Ok(stream)) => {
                                            // Connection successful
                                            debug!("Successfully connected to bootstrap node: {}", bootstrap_addr);

                                            // Add to connection pool
                                            if (connection_pool.add_connection(peer_id.clone(), stream).await).is_ok() {
                                                // Add to peer manager
                                                if (peer_manager.add_peer(
                                                    peer_id.clone(),
                                                    bootstrap_addr.clone(),
                                                    NodeRole::Validator,
                                                ).await).is_ok() {
                                                    // Mark as connected
                                                    let _ = peer_manager.mark_connected(&peer_id).await;

                                                    // Update bootstrap attempt tracking
                                                    {
                                                        let mut attempts = bootstrap_attempts.write().await;
                                                        if let Some(attempt) = attempts.get_mut(&bootstrap_addr) {
                                                            attempt.is_connected = true;
                                                            attempt.failure_count = 0;
                                                            attempt.in_progress = false;
                                                            attempt.successful_connections += 1;
                                                        }
                                                    }

                                                    // Update stats
                                                    {
                                                        let mut s = stats.write().await;
                                                        s.successful_connections += 1;
                                                        s.total_attempts += 1;
                                                        s.avg_connection_time_ms = (s.avg_connection_time_ms + connection_time_ms) / 2;
                                                        s.connected_bootstrap_nodes = bootstrap_attempts.read().await
                                                            .iter()
                                                            .filter(|(_, a)| a.is_connected)
                                                            .count();
                                                    }

                                                    info!("Bootstrap node connected: {} (connection time: {}ms)", bootstrap_addr, connection_time_ms);
                                                }
                                            }
                                        }
                                        Ok(Err(e)) => {
                                            // Connection failed
                                            warn!("Failed to connect to bootstrap node {}: {}", bootstrap_addr, e);

                                            {
                                                let mut attempts = bootstrap_attempts.write().await;
                                                if let Some(attempt) = attempts.get_mut(&bootstrap_addr) {
                                                    attempt.failure_count += 1;
                                                    attempt.in_progress = false;
                                                    attempt.total_failures += 1;
                                                }
                                            }

                                            {
                                                let mut s = stats.write().await;
                                                s.failed_attempts += 1;
                                                s.total_attempts += 1;
                                            }
                                        }
                                        Err(_) => {
                                            // Connection timeout
                                            warn!("Connection timeout to bootstrap node: {}", bootstrap_addr);

                                            {
                                                let mut attempts = bootstrap_attempts.write().await;
                                                if let Some(attempt) = attempts.get_mut(&bootstrap_addr) {
                                                    attempt.failure_count += 1;
                                                    attempt.in_progress = false;
                                                    attempt.total_failures += 1;
                                                }
                                            }

                                            {
                                                let mut s = stats.write().await;
                                                s.failed_attempts += 1;
                                                s.total_attempts += 1;
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                    _ = shutdown_rx.recv() => {
                        info!("Shutting down bootstrap connector");
                        break;
                    }
                }
            }
        });

        Ok(())
    }

    /// Stop the bootstrap connector
    pub async fn stop(&self) -> Result<()> {
        if !self.is_running.swap(false, Ordering::SeqCst) {
            return Err(P2PError::NetworkError(
                "Bootstrap connector not running".to_string(),
            ));
        }

        let _ = self.shutdown_tx.send(());
        info!("Bootstrap connector stopped");
        Ok(())
    }

    /// Check if connector is running
    pub fn is_running(&self) -> bool {
        self.is_running.load(Ordering::SeqCst)
    }

    /// Get bootstrap node connection status
    pub async fn get_bootstrap_status(&self) -> (usize, usize) {
        let attempts = self.bootstrap_attempts.read().await;
        let connected = attempts.iter().filter(|(_, a)| a.is_connected).count();
        let total = attempts.len();
        (connected, total)
    }

    /// Get connection attempt history for a bootstrap node
    pub async fn get_bootstrap_attempt_history(&self, address: &str) -> Option<BootstrapNodeAttempt> {
        let attempts = self.bootstrap_attempts.read().await;
        attempts.get(address).cloned()
    }

    /// Reset failure count for a bootstrap node (recovery mechanism)
    pub async fn reset_bootstrap_failures(&self, address: &str) -> Result<()> {
        let mut attempts = self.bootstrap_attempts.write().await;
        if let Some(attempt) = attempts.get_mut(address) {
            attempt.failure_count = 0;
            debug!("Reset failure count for bootstrap node: {}", address);
            Ok(())
        } else {
            Err(P2PError::NetworkError(format!(
                "Bootstrap node not found: {}",
                address
            )))
        }
    }

    /// Mark bootstrap node as disconnected
    pub async fn mark_bootstrap_disconnected(&self, address: &str) -> Result<()> {
        let mut attempts = self.bootstrap_attempts.write().await;
        if let Some(attempt) = attempts.get_mut(address) {
            attempt.is_connected = false;
            debug!("Marked bootstrap node as disconnected: {}", address);

            // Update stats
            let mut stats = self.stats.write().await;
            stats.connected_bootstrap_nodes = attempts.iter().filter(|(_, a)| a.is_connected).count();

            Ok(())
        } else {
            Err(P2PError::NetworkError(format!(
                "Bootstrap node not found: {}",
                address
            )))
        }
    }

    /// Get bootstrap connection statistics
    pub async fn get_stats(&self) -> BootstrapConnectionStats {
        self.stats.read().await.clone()
    }

    /// Get bootstrap nodes that have exceeded failure threshold
    pub async fn get_failed_bootstrap_nodes(&self, max_failures: u32) -> Vec<String> {
        let attempts = self.bootstrap_attempts.read().await;
        attempts
            .iter()
            .filter(|(_, attempt)| attempt.failure_count >= max_failures)
            .map(|(addr, _)| addr.clone())
            .collect()
    }

    /// Get all bootstrap nodes being tracked
    pub async fn get_tracked_bootstrap_nodes(&self) -> Vec<String> {
        let attempts = self.bootstrap_attempts.read().await;
        attempts.keys().cloned().collect()
    }

    /// Get connected bootstrap nodes
    pub async fn get_connected_bootstrap_nodes(&self) -> Vec<String> {
        let attempts = self.bootstrap_attempts.read().await;
        attempts
            .iter()
            .filter(|(_, attempt)| attempt.is_connected)
            .map(|(addr, _)| addr.clone())
            .collect()
    }

    /// Manually trigger bootstrap connection attempt
    pub async fn trigger_bootstrap_connection(&self, address: &str) -> Result<()> {
        let mut attempts = self.bootstrap_attempts.write().await;
        if let Some(attempt) = attempts.get_mut(address) {
            attempt.last_attempt = SystemTime::now() - Duration::from_secs(1000); // Force immediate retry
            debug!("Triggered bootstrap connection attempt for: {}", address);
            Ok(())
        } else {
            Err(P2PError::NetworkError(format!(
                "Bootstrap node not found: {}",
                address
            )))
        }
    }

    /// Get bootstrap node health information
    pub async fn get_bootstrap_health(&self, address: &str) -> Result<(bool, u32, u64)> {
        let attempts = self.bootstrap_attempts.read().await;
        if let Some(attempt) = attempts.get(address) {
            Ok((attempt.is_connected, attempt.failure_count, attempt.successful_connections))
        } else {
            Err(P2PError::NetworkError(format!(
                "Bootstrap node not found: {}",
                address
            )))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::NetworkConfig;
    use crate::connection_pool::ConnectionPool;
    use crate::peer_manager::PeerManager;

    #[tokio::test]
    async fn test_bootstrap_connector_creation() {
        let mut config = NetworkConfig::new("node1".to_string(), NodeRole::Validator);
        config.bootstrap_nodes = vec!["127.0.0.1:9000".to_string()];

        let peer_manager = Arc::new(PeerManager::new(1000));
        let connection_pool = Arc::new(ConnectionPool::new(100, 300));

        let connector = BootstrapConnector::new(peer_manager, connection_pool, config);
        assert!(!connector.is_running());
    }

    #[tokio::test]
    async fn test_bootstrap_connector_start_stop() {
        let mut config = NetworkConfig::new("node1".to_string(), NodeRole::Validator);
        config.bootstrap_nodes = vec!["127.0.0.1:9000".to_string()];

        let peer_manager = Arc::new(PeerManager::new(1000));
        let connection_pool = Arc::new(ConnectionPool::new(100, 300));

        let connector = BootstrapConnector::new(peer_manager, connection_pool, config);
        assert!(connector.start().await.is_ok());
        assert!(connector.is_running());

        assert!(connector.stop().await.is_ok());
        assert!(!connector.is_running());
    }

    #[tokio::test]
    async fn test_bootstrap_status() {
        let mut config = NetworkConfig::new("node1".to_string(), NodeRole::Validator);
        config.bootstrap_nodes = vec![
            "127.0.0.1:9000".to_string(),
            "127.0.0.1:9001".to_string(),
        ];

        let peer_manager = Arc::new(PeerManager::new(1000));
        let connection_pool = Arc::new(ConnectionPool::new(100, 300));

        let connector = BootstrapConnector::new(peer_manager, connection_pool, config);
        assert!(connector.start().await.is_ok());

        // Give it time to initialize
        tokio::time::sleep(Duration::from_millis(100)).await;

        let (connected, total) = connector.get_bootstrap_status().await;
        assert_eq!(total, 2);
        assert_eq!(connected, 0); // No actual connections made

        connector.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_bootstrap_attempt_history() {
        let mut config = NetworkConfig::new("node1".to_string(), NodeRole::Validator);
        config.bootstrap_nodes = vec!["127.0.0.1:9000".to_string()];

        let peer_manager = Arc::new(PeerManager::new(1000));
        let connection_pool = Arc::new(ConnectionPool::new(100, 300));

        let connector = BootstrapConnector::new(peer_manager, connection_pool, config);
        assert!(connector.start().await.is_ok());

        tokio::time::sleep(Duration::from_millis(100)).await;

        let history = connector.get_bootstrap_attempt_history("127.0.0.1:9000").await;
        assert!(history.is_some());
        assert_eq!(history.unwrap().address, "127.0.0.1:9000");

        connector.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_reset_bootstrap_failures() {
        let mut config = NetworkConfig::new("node1".to_string(), NodeRole::Validator);
        config.bootstrap_nodes = vec!["127.0.0.1:9000".to_string()];

        let peer_manager = Arc::new(PeerManager::new(1000));
        let connection_pool = Arc::new(ConnectionPool::new(100, 300));

        let connector = BootstrapConnector::new(peer_manager, connection_pool, config);
        assert!(connector.start().await.is_ok());

        tokio::time::sleep(Duration::from_millis(100)).await;

        // Manually set failure count
        {
            let mut attempts = connector.bootstrap_attempts.write().await;
            if let Some(attempt) = attempts.get_mut("127.0.0.1:9000") {
                attempt.failure_count = 5;
            }
        }

        assert!(connector.reset_bootstrap_failures("127.0.0.1:9000").await.is_ok());

        let history = connector.get_bootstrap_attempt_history("127.0.0.1:9000").await;
        assert_eq!(history.unwrap().failure_count, 0);

        connector.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_mark_bootstrap_disconnected() {
        let mut config = NetworkConfig::new("node1".to_string(), NodeRole::Validator);
        config.bootstrap_nodes = vec!["127.0.0.1:9000".to_string()];

        let peer_manager = Arc::new(PeerManager::new(1000));
        let connection_pool = Arc::new(ConnectionPool::new(100, 300));

        let connector = BootstrapConnector::new(peer_manager, connection_pool, config);
        assert!(connector.start().await.is_ok());

        tokio::time::sleep(Duration::from_millis(100)).await;

        // Manually mark as connected
        {
            let mut attempts = connector.bootstrap_attempts.write().await;
            if let Some(attempt) = attempts.get_mut("127.0.0.1:9000") {
                attempt.is_connected = true;
            }
        }

        assert!(connector.mark_bootstrap_disconnected("127.0.0.1:9000").await.is_ok());

        let history = connector.get_bootstrap_attempt_history("127.0.0.1:9000").await;
        assert!(!history.unwrap().is_connected);

        connector.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_get_failed_bootstrap_nodes() {
        let mut config = NetworkConfig::new("node1".to_string(), NodeRole::Validator);
        config.bootstrap_nodes = vec![
            "127.0.0.1:9000".to_string(),
            "127.0.0.1:9001".to_string(),
        ];

        let peer_manager = Arc::new(PeerManager::new(1000));
        let connection_pool = Arc::new(ConnectionPool::new(100, 300));

        let connector = BootstrapConnector::new(peer_manager, connection_pool, config);
        assert!(connector.start().await.is_ok());

        tokio::time::sleep(Duration::from_millis(100)).await;

        // Set different failure counts
        {
            let mut attempts = connector.bootstrap_attempts.write().await;
            if let Some(attempt) = attempts.get_mut("127.0.0.1:9000") {
                attempt.failure_count = 3;
            }
            if let Some(attempt) = attempts.get_mut("127.0.0.1:9001") {
                attempt.failure_count = 1;
            }
        }

        let failed = connector.get_failed_bootstrap_nodes(2).await;
        assert_eq!(failed.len(), 1);
        assert!(failed.contains(&"127.0.0.1:9000".to_string()));

        connector.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_get_connected_bootstrap_nodes() {
        let mut config = NetworkConfig::new("node1".to_string(), NodeRole::Validator);
        config.bootstrap_nodes = vec![
            "127.0.0.1:9000".to_string(),
            "127.0.0.1:9001".to_string(),
        ];

        let peer_manager = Arc::new(PeerManager::new(1000));
        let connection_pool = Arc::new(ConnectionPool::new(100, 300));

        let connector = BootstrapConnector::new(peer_manager, connection_pool, config);
        assert!(connector.start().await.is_ok());

        tokio::time::sleep(Duration::from_millis(100)).await;

        // Mark one as connected
        {
            let mut attempts = connector.bootstrap_attempts.write().await;
            if let Some(attempt) = attempts.get_mut("127.0.0.1:9000") {
                attempt.is_connected = true;
            }
        }

        let connected = connector.get_connected_bootstrap_nodes().await;
        assert_eq!(connected.len(), 1);
        assert!(connected.contains(&"127.0.0.1:9000".to_string()));

        connector.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_bootstrap_stats() {
        let mut config = NetworkConfig::new("node1".to_string(), NodeRole::Validator);
        config.bootstrap_nodes = vec!["127.0.0.1:9000".to_string()];

        let peer_manager = Arc::new(PeerManager::new(1000));
        let connection_pool = Arc::new(ConnectionPool::new(100, 300));

        let connector = BootstrapConnector::new(peer_manager, connection_pool, config);
        assert!(connector.start().await.is_ok());

        tokio::time::sleep(Duration::from_millis(100)).await;

        let stats = connector.get_stats().await;
        assert_eq!(stats.tracked_bootstrap_nodes, 1);
        assert_eq!(stats.connected_bootstrap_nodes, 0);

        connector.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_bootstrap_health() {
        let mut config = NetworkConfig::new("node1".to_string(), NodeRole::Validator);
        config.bootstrap_nodes = vec!["127.0.0.1:9000".to_string()];

        let peer_manager = Arc::new(PeerManager::new(1000));
        let connection_pool = Arc::new(ConnectionPool::new(100, 300));

        let connector = BootstrapConnector::new(peer_manager, connection_pool, config);
        assert!(connector.start().await.is_ok());

        tokio::time::sleep(Duration::from_millis(100)).await;

        let health = connector.get_bootstrap_health("127.0.0.1:9000").await;
        assert!(health.is_ok());
        let (is_connected, failure_count, successful) = health.unwrap();
        assert!(!is_connected);
        assert_eq!(failure_count, 0);
        assert_eq!(successful, 0);

        connector.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_trigger_bootstrap_connection() {
        let mut config = NetworkConfig::new("node1".to_string(), NodeRole::Validator);
        config.bootstrap_nodes = vec!["127.0.0.1:9000".to_string()];

        let peer_manager = Arc::new(PeerManager::new(1000));
        let connection_pool = Arc::new(ConnectionPool::new(100, 300));

        let connector = BootstrapConnector::new(peer_manager, connection_pool, config);
        assert!(connector.start().await.is_ok());

        tokio::time::sleep(Duration::from_millis(100)).await;

        // Manually set to a very old time
        {
            let mut attempts = connector.bootstrap_attempts.write().await;
            if let Some(attempt) = attempts.get_mut("127.0.0.1:9000") {
                attempt.last_attempt = SystemTime::now() - Duration::from_secs(2000);
            }
        }

        let before = connector.get_bootstrap_attempt_history("127.0.0.1:9000").await;
        let before_time = before.unwrap().last_attempt;

        assert!(connector.trigger_bootstrap_connection("127.0.0.1:9000").await.is_ok());

        let after = connector.get_bootstrap_attempt_history("127.0.0.1:9000").await;
        let after_time = after.unwrap().last_attempt;

        // After should be later than before (trigger sets it to 1000 seconds in the past, before was 2000 seconds in the past)
        assert!(after_time > before_time, "after_time {:?} should be > before_time {:?}", after_time, before_time);

        connector.stop().await.unwrap();
    }
}
