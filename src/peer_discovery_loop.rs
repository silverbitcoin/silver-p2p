//! Peer discovery loop - background task for continuous peer discovery and connection management

use crate::config::NetworkConfig;
use crate::connection_pool::ConnectionPool;
use crate::error::{P2PError, Result};
use crate::message_handler::MessageHandler;
use crate::peer_manager::PeerManager;
use crate::types::NetworkMessage;
use crate::unicast::UnicastManager;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::net::TcpStream;
use tokio::sync::RwLock;
use tokio::time::{interval, timeout};
use tracing::{debug, info, warn};

/// Statistics for peer discovery loop execution
#[derive(Clone, Debug)]
pub struct PeerDiscoveryLoopStats {
    /// Total discovery cycles executed
    pub total_cycles: u64,
    /// Total peer list requests sent
    pub peer_list_requests_sent: u64,
    /// Total peer list responses received
    pub peer_list_responses_received: u64,
    /// Total new peers discovered
    pub new_peers_discovered: u64,
    /// Total connection attempts made
    pub connection_attempts: u64,
    /// Successful connections from discovery
    pub successful_connections: u64,
    /// Failed connection attempts
    pub failed_connections: u64,
    /// Number of times minimum threshold was triggered
    pub threshold_triggers: u64,
    /// Last discovery cycle timestamp
    pub last_cycle_time: Option<SystemTime>,
    /// Average cycle duration in milliseconds
    pub avg_cycle_duration_ms: u64,
}

/// Manages the peer discovery loop - background task for continuous peer discovery
pub struct PeerDiscoveryLoop {
    /// Peer manager reference
    peer_manager: Arc<PeerManager>,
    /// Connection pool reference
    connection_pool: Arc<ConnectionPool>,
    /// Unicast manager for sending peer list requests
    unicast_manager: Arc<UnicastManager>,
    /// Network configuration
    config: NetworkConfig,
    /// Whether the loop is running
    is_running: Arc<AtomicBool>,
    /// Statistics
    stats: Arc<RwLock<PeerDiscoveryLoopStats>>,
    /// Shutdown signal
    shutdown_tx: tokio::sync::broadcast::Sender<()>,
    /// Discovery cycle interval in seconds
    discovery_cycle_interval_secs: u64,
    /// Peer list request interval in seconds
    peer_list_request_interval_secs: u64,
}

impl PeerDiscoveryLoop {
    /// Create new peer discovery loop
    pub fn new(
        peer_manager: Arc<PeerManager>,
        connection_pool: Arc<ConnectionPool>,
        _message_handler: Arc<MessageHandler>,
        unicast_manager: Arc<UnicastManager>,
        config: NetworkConfig,
    ) -> Self {
        let (shutdown_tx, _) = tokio::sync::broadcast::channel(1);

        Self {
            peer_manager,
            connection_pool,
            unicast_manager,
            config,
            is_running: Arc::new(AtomicBool::new(false)),
            stats: Arc::new(RwLock::new(PeerDiscoveryLoopStats {
                total_cycles: 0,
                peer_list_requests_sent: 0,
                peer_list_responses_received: 0,
                new_peers_discovered: 0,
                connection_attempts: 0,
                successful_connections: 0,
                failed_connections: 0,
                threshold_triggers: 0,
                last_cycle_time: None,
                avg_cycle_duration_ms: 0,
            })),
            shutdown_tx,
            discovery_cycle_interval_secs: 10,
            peer_list_request_interval_secs: 30,
        }
    }

    /// Start the peer discovery loop
    pub async fn start(&self) -> Result<()> {
        if self.is_running.swap(true, Ordering::SeqCst) {
            return Err(P2PError::NetworkError(
                "Peer discovery loop already running".to_string(),
            ));
        }

        info!(
            "Starting peer discovery loop (min_peers: {}, max_peers: {})",
            self.config.min_peers, self.config.max_peers
        );

        let peer_manager = self.peer_manager.clone();
        let connection_pool = self.connection_pool.clone();
        let config = self.config.clone();
        let stats = self.stats.clone();
        let unicast_manager = self.unicast_manager.clone();
        let mut shutdown_rx = self.shutdown_tx.subscribe();

        let discovery_cycle_interval = self.discovery_cycle_interval_secs;
        let peer_list_request_interval = self.peer_list_request_interval_secs;

        tokio::spawn(async move {
            let mut discovery_ticker = interval(Duration::from_secs(discovery_cycle_interval));
            let mut peer_list_ticker = interval(Duration::from_secs(peer_list_request_interval));

            loop {
                tokio::select! {
                    _ = discovery_ticker.tick() => {
                        let cycle_start = SystemTime::now();

                        // Execute discovery cycle
                        if let Err(e) = Self::execute_discovery_cycle(
                            &peer_manager,
                            &connection_pool,
                            &config,
                            &stats,
                        )
                        .await
                        {
                            warn!("Error during discovery cycle: {}", e);
                        }

                        // Update cycle statistics
                        let cycle_duration = cycle_start
                            .elapsed()
                            .unwrap_or_default()
                            .as_millis() as u64;

                        {
                            let mut s = stats.write().await;
                            s.total_cycles += 1;
                            s.last_cycle_time = Some(SystemTime::now());
                            s.avg_cycle_duration_ms = (s.avg_cycle_duration_ms + cycle_duration) / 2;
                        }

                        debug!(
                            "Discovery cycle completed in {}ms",
                            cycle_duration
                        );
                    }
                    _ = peer_list_ticker.tick() => {
                        // Execute peer list requests
                        if let Err(e) = Self::request_peer_lists(
                            &peer_manager,
                            &unicast_manager,
                            &stats,
                        )
                        .await
                        {
                            warn!("Error requesting peer lists: {}", e);
                        }
                    }
                    _ = shutdown_rx.recv() => {
                        info!("Shutting down peer discovery loop");
                        break;
                    }
                }
            }
        });

        Ok(())
    }

    /// Stop the peer discovery loop
    pub async fn stop(&self) -> Result<()> {
        if !self.is_running.swap(false, Ordering::SeqCst) {
            return Err(P2PError::NetworkError(
                "Peer discovery loop not running".to_string(),
            ));
        }

        let _ = self.shutdown_tx.send(());
        info!("Peer discovery loop stopped");
        Ok(())
    }

    /// Check if loop is running
    pub fn is_running(&self) -> bool {
        self.is_running.load(Ordering::SeqCst)
    }

    /// Execute a single discovery cycle
    async fn execute_discovery_cycle(
        peer_manager: &Arc<PeerManager>,
        connection_pool: &Arc<ConnectionPool>,
        config: &NetworkConfig,
        stats: &Arc<RwLock<PeerDiscoveryLoopStats>>,
    ) -> Result<()> {
        let connected_count = peer_manager.get_connected_peer_count().await;

        debug!(
            "Discovery cycle: connected={}, min_required={}",
            connected_count, config.min_peers
        );

        if connected_count < config.min_peers {
            debug!(
                "Below minimum peer threshold: {} < {}",
                connected_count, config.min_peers
            );

            // Increment threshold trigger counter
            {
                let mut s = stats.write().await;
                s.threshold_triggers += 1;
            }

            // Get candidates and attempt connections
            let candidates = peer_manager.get_candidates().await;
            let needed = config.min_peers - connected_count;

            if !candidates.is_empty() {
                debug!(
                    "Attempting to connect to {} candidates (need {} more peers)",
                    candidates.len().min(needed),
                    needed
                );

                // Attempt connections to candidates
                for candidate_addr in candidates.iter().take(needed) {
                    let peer_id = format!("candidate_{}", candidate_addr.replace(":", "_"));

                    // Attempt connection
                    let start_time = SystemTime::now();
                    let connection_result = timeout(
                        Duration::from_secs(config.connection_timeout_secs),
                        TcpStream::connect(candidate_addr),
                    )
                    .await;

                    let _connection_time_ms =
                        start_time.elapsed().unwrap_or_default().as_millis() as u64;

                    {
                        let mut s = stats.write().await;
                        s.connection_attempts += 1;
                    }

                    match connection_result {
                        Ok(Ok(stream)) => {
                            // Connection successful
                            debug!("Successfully connected to candidate: {}", candidate_addr);

                            // Add to connection pool
                            if (connection_pool
                                .add_connection(peer_id.clone(), stream)
                                .await)
                                .is_ok()
                            {
                                // Add to peer manager
                                if (peer_manager
                                    .add_peer(
                                        peer_id.clone(),
                                        candidate_addr.clone(),
                                        crate::types::NodeRole::Validator,
                                    )
                                    .await)
                                    .is_ok()
                                {
                                    // Mark as connected
                                    let _ = peer_manager.mark_connected(&peer_id).await;

                                    // Update stats
                                    {
                                        let mut s = stats.write().await;
                                        s.successful_connections += 1;
                                    }

                                    // Remove from candidates since we connected
                                    let _ = peer_manager.remove_candidate(candidate_addr).await;
                                }
                            }
                        }
                        Ok(Err(e)) => {
                            // Connection failed
                            warn!("Failed to connect to candidate {}: {}", candidate_addr, e);

                            {
                                let mut s = stats.write().await;
                                s.failed_connections += 1;
                            }
                        }
                        Err(_) => {
                            // Connection timeout
                            warn!("Connection timeout to candidate: {}", candidate_addr);

                            {
                                let mut s = stats.write().await;
                                s.failed_connections += 1;
                            }
                        }
                    }
                }
            } else {
                debug!("No candidates available for connection");
            }
        } else {
            debug!(
                "Peer count above threshold: {} >= {}",
                connected_count, config.min_peers
            );
        }

        Ok(())
    }

    /// Request peer lists from connected peers
    async fn request_peer_lists(
        peer_manager: &Arc<PeerManager>,
        unicast_manager: &Arc<UnicastManager>,
        stats: &Arc<RwLock<PeerDiscoveryLoopStats>>,
    ) -> Result<()> {
        // Get all connected and healthy peers
        let peers = peer_manager.get_all_peers().await;
        let connected_peers: Vec<_> = peers
            .iter()
            .filter(|p| p.is_connected && p.is_healthy)
            .collect();

        if !connected_peers.is_empty() {
            // Request from up to 3 random connected peers
            let request_count = std::cmp::min(3, connected_peers.len());

            // Use system time as seed for simple randomization
            let seed = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos() as usize;

            for i in 0..request_count {
                let idx = (seed + i) % connected_peers.len();
                let peer = connected_peers[idx];

                // Send peer list request
                let msg = NetworkMessage::PeerListRequest;
                if let Err(e) = unicast_manager.send_to_peer(&peer.peer_id, msg).await {
                    warn!("Failed to request peer list from {}: {}", peer.peer_id, e);
                } else {
                    debug!("Sent peer list request to peer: {}", peer.peer_id);

                    // Update statistics
                    let mut s = stats.write().await;
                    s.peer_list_requests_sent += 1;
                }
            }
        } else {
            debug!("No connected peers available for peer list requests");
        }

        Ok(())
    }

    /// Get current discovery loop statistics
    pub async fn get_stats(&self) -> PeerDiscoveryLoopStats {
        self.stats.read().await.clone()
    }

    /// Get current peer count status
    pub async fn get_peer_count_status(&self) -> (usize, usize, bool) {
        let connected = self.peer_manager.get_connected_peer_count().await;
        let total = self.peer_manager.get_peer_count().await;
        let below_threshold = connected < self.config.min_peers;

        (connected, total, below_threshold)
    }

    /// Get minimum peer threshold
    pub fn get_min_peer_threshold(&self) -> usize {
        self.config.min_peers
    }

    /// Get maximum peer capacity
    pub fn get_max_peer_capacity(&self) -> usize {
        self.config.max_peers
    }

    /// Manually trigger a discovery cycle
    pub async fn trigger_discovery_cycle(&self) -> Result<()> {
        debug!("Manual discovery cycle triggered");
        Self::execute_discovery_cycle(
            &self.peer_manager,
            &self.connection_pool,
            &self.config,
            &self.stats,
        )
        .await
    }

    /// Manually trigger peer list requests
    pub async fn trigger_peer_list_requests(&self) -> Result<()> {
        debug!("Manual peer list requests triggered");
        Self::request_peer_lists(&self.peer_manager, &self.unicast_manager, &self.stats).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::NetworkConfig;
    use crate::connection_pool::ConnectionPool;
    use crate::message_handler::MessageHandler;
    use crate::peer_manager::PeerManager;
    use crate::types::NodeRole;
    use crate::unicast::UnicastManager;

    fn create_test_loop(config: NetworkConfig) -> PeerDiscoveryLoop {
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

        PeerDiscoveryLoop::new(
            peer_manager,
            connection_pool,
            message_handler,
            unicast_manager,
            config,
        )
    }

    #[tokio::test]
    async fn test_discovery_loop_creation() {
        let config = NetworkConfig::new("node1".to_string(), NodeRole::Validator);
        let loop_mgr = create_test_loop(config);
        assert!(!loop_mgr.is_running());
    }

    #[tokio::test]
    async fn test_discovery_loop_start_stop() {
        let config = NetworkConfig::new("node1".to_string(), NodeRole::Validator);
        let loop_mgr = create_test_loop(config);

        assert!(loop_mgr.start().await.is_ok());
        assert!(loop_mgr.is_running());

        // Give it time to start
        tokio::time::sleep(Duration::from_millis(50)).await;

        assert!(loop_mgr.stop().await.is_ok());
        assert!(!loop_mgr.is_running());
    }

    #[tokio::test]
    async fn test_discovery_loop_double_start() {
        let config = NetworkConfig::new("node1".to_string(), NodeRole::Validator);
        let loop_mgr = create_test_loop(config);

        assert!(loop_mgr.start().await.is_ok());
        assert!(loop_mgr.is_running());

        // Try to start again - should fail
        assert!(loop_mgr.start().await.is_err());

        loop_mgr.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_discovery_loop_stats_initialization() {
        let config = NetworkConfig::new("node1".to_string(), NodeRole::Validator);
        let loop_mgr = create_test_loop(config);

        let stats = loop_mgr.get_stats().await;
        assert_eq!(stats.total_cycles, 0);
        assert_eq!(stats.peer_list_requests_sent, 0);
        assert_eq!(stats.connection_attempts, 0);
        assert_eq!(stats.successful_connections, 0);
        assert_eq!(stats.failed_connections, 0);
    }

    #[tokio::test]
    async fn test_peer_count_status() {
        let mut config = NetworkConfig::new("node1".to_string(), NodeRole::Validator);
        config.min_peers = 3;

        let loop_mgr = create_test_loop(config);
        let peer_manager = loop_mgr.peer_manager.clone();

        // Add a peer
        peer_manager
            .add_peer(
                "peer1".to_string(),
                "127.0.0.1:9000".to_string(),
                NodeRole::Validator,
            )
            .await
            .unwrap();

        peer_manager.mark_connected("peer1").await.unwrap();

        let (connected, total, below_threshold) = loop_mgr.get_peer_count_status().await;
        assert_eq!(connected, 1);
        assert_eq!(total, 1);
        assert!(below_threshold);
    }

    #[tokio::test]
    async fn test_min_peer_threshold_config() {
        let mut config = NetworkConfig::new("node1".to_string(), NodeRole::Validator);
        config.min_peers = 5;

        let loop_mgr = create_test_loop(config);

        assert_eq!(loop_mgr.get_min_peer_threshold(), 5);
        assert_eq!(loop_mgr.get_max_peer_capacity(), 100);
    }

    #[tokio::test]
    async fn test_trigger_discovery_cycle() {
        let mut config = NetworkConfig::new("node1".to_string(), NodeRole::Validator);
        config.min_peers = 3;

        let loop_mgr = create_test_loop(config);

        let stats_before = loop_mgr.get_stats().await;
        assert_eq!(stats_before.threshold_triggers, 0);

        // Trigger discovery cycle manually
        assert!(loop_mgr.trigger_discovery_cycle().await.is_ok());

        let stats_after = loop_mgr.get_stats().await;
        assert_eq!(stats_after.threshold_triggers, 1);
    }

    #[tokio::test]
    async fn test_trigger_peer_list_requests() {
        let config = NetworkConfig::new("node1".to_string(), NodeRole::Validator);
        let loop_mgr = create_test_loop(config);
        let peer_manager = loop_mgr.peer_manager.clone();

        // Add a connected and healthy peer
        peer_manager
            .add_peer(
                "peer1".to_string(),
                "127.0.0.1:9000".to_string(),
                NodeRole::Validator,
            )
            .await
            .unwrap();

        peer_manager.mark_connected("peer1").await.unwrap();
        peer_manager.mark_healthy("peer1").await.unwrap();

        let stats_before = loop_mgr.get_stats().await;
        assert_eq!(stats_before.peer_list_requests_sent, 0);

        // Trigger peer list requests manually
        // Note: This will attempt to send but may fail due to no actual connection
        // The important thing is that the function completes without error
        assert!(loop_mgr.trigger_peer_list_requests().await.is_ok());

        // Stats may or may not be updated depending on whether the send succeeded
        // In a test environment without actual connections, the send will fail
        // but the function should still complete successfully
        let stats_after = loop_mgr.get_stats().await;
        // Just verify the function ran without error
        assert_eq!(stats_after.total_cycles, 0); // No cycles executed yet
    }

    #[tokio::test]
    async fn test_discovery_loop_running_state() {
        let config = NetworkConfig::new("node1".to_string(), NodeRole::Validator);
        let loop_mgr = create_test_loop(config);

        assert!(!loop_mgr.is_running());

        loop_mgr.start().await.unwrap();
        assert!(loop_mgr.is_running());

        loop_mgr.stop().await.unwrap();
        assert!(!loop_mgr.is_running());
    }

    #[tokio::test]
    async fn test_discovery_loop_with_candidates() {
        let mut config = NetworkConfig::new("node1".to_string(), NodeRole::Validator);
        config.min_peers = 2;

        let loop_mgr = create_test_loop(config);
        let peer_manager = loop_mgr.peer_manager.clone();

        // Add candidates
        peer_manager
            .add_candidates(vec![
                "127.0.0.1:9001".to_string(),
                "127.0.0.1:9002".to_string(),
            ])
            .await
            .unwrap();

        assert_eq!(peer_manager.get_candidate_count().await, 2);

        // Trigger discovery cycle
        assert!(loop_mgr.trigger_discovery_cycle().await.is_ok());

        let stats = loop_mgr.get_stats().await;
        assert_eq!(stats.threshold_triggers, 1);
        assert!(stats.connection_attempts > 0);
    }
}
