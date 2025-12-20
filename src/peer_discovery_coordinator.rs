//! Peer discovery coordination with minimum peer threshold enforcement and periodic peer list requests

use crate::config::NetworkConfig;
use crate::connection_pool::ConnectionPool;
use crate::error::{P2PError, Result};
use crate::message_handler::MessageHandler;
use crate::peer_manager::PeerManager;
use crate::types::{NodeRole, NetworkMessage};
use crate::unicast::UnicastManager;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::net::TcpStream;
use tokio::sync::RwLock;
use tokio::time::{interval, timeout};
use tracing::{debug, info, warn};

/// Tracks the state of a candidate connection attempt
#[derive(Clone, Debug)]
pub struct CandidateConnectionAttempt {
    /// Address being attempted
    pub address: String,
    /// Timestamp of last attempt
    pub last_attempt: SystemTime,
    /// Number of consecutive failures
    pub failure_count: u32,
    /// Whether this attempt is currently in progress
    pub in_progress: bool,
}

/// Statistics for peer discovery coordination
#[derive(Clone, Debug)]
pub struct PeerDiscoveryStats {
    /// Total connection attempts made
    pub total_attempts: u64,
    /// Successful connections from candidates
    pub successful_connections: u64,
    /// Failed connection attempts
    pub failed_attempts: u64,
    /// Current number of candidates being tracked
    pub tracked_candidates: usize,
    /// Number of times minimum threshold was triggered
    pub threshold_triggers: u64,
    /// Average time to connect from candidate
    pub avg_connection_time_ms: u64,
    /// Total peer list requests sent
    pub peer_list_requests_sent: u64,
    /// Total peer list responses received
    pub peer_list_responses_received: u64,
    /// Total new peers discovered
    pub new_peers_discovered: u64,
    /// Last peer list request timestamp
    pub last_peer_list_request: Option<SystemTime>,
}

/// Coordinates peer discovery with minimum peer threshold enforcement
pub struct PeerDiscoveryCoordinator {
    /// Peer manager reference
    peer_manager: Arc<PeerManager>,
    /// Connection pool reference
    connection_pool: Arc<ConnectionPool>,
    /// Message handler reference for sending peer list requests
    #[allow(dead_code)]
    message_handler: Arc<MessageHandler>,
    /// Unicast manager for sending peer list requests
    unicast_manager: Arc<UnicastManager>,
    /// Network configuration
    config: NetworkConfig,
    /// Tracks candidate connection attempts
    pub candidate_attempts: Arc<RwLock<std::collections::HashMap<String, CandidateConnectionAttempt>>>,
    /// Whether discovery is running
    is_running: Arc<AtomicBool>,
    /// Statistics
    stats: Arc<RwLock<PeerDiscoveryStats>>,
    /// Shutdown signal
    shutdown_tx: tokio::sync::broadcast::Sender<()>,
    /// Last time peer list was requested from any peer
    last_peer_list_request_time: Arc<RwLock<SystemTime>>,
    /// Peer list request interval in seconds
    #[allow(dead_code)]
    peer_list_request_interval_secs: u64,
}

impl PeerDiscoveryCoordinator {
    /// Create new peer discovery coordinator
    pub fn new(
        peer_manager: Arc<PeerManager>,
        connection_pool: Arc<ConnectionPool>,
        message_handler: Arc<MessageHandler>,
        unicast_manager: Arc<UnicastManager>,
        config: NetworkConfig,
    ) -> Self {
        let (shutdown_tx, _) = tokio::sync::broadcast::channel(1);

        Self {
            peer_manager,
            connection_pool,
            message_handler,
            unicast_manager,
            config,
            candidate_attempts: Arc::new(RwLock::new(std::collections::HashMap::new())),
            is_running: Arc::new(AtomicBool::new(false)),
            stats: Arc::new(RwLock::new(PeerDiscoveryStats {
                total_attempts: 0,
                successful_connections: 0,
                failed_attempts: 0,
                tracked_candidates: 0,
                threshold_triggers: 0,
                avg_connection_time_ms: 0,
                peer_list_requests_sent: 0,
                peer_list_responses_received: 0,
                new_peers_discovered: 0,
                last_peer_list_request: None,
            })),
            shutdown_tx,
            last_peer_list_request_time: Arc::new(RwLock::new(SystemTime::now())),
            peer_list_request_interval_secs: 30,
        }
    }

    /// Request peer list from a specific peer
    pub async fn request_peer_list(&self, peer_id: &str) -> Result<()> {
        debug!("Requesting peer list from peer: {}", peer_id);

        // Send PeerListRequest message
        let msg = NetworkMessage::PeerListRequest;
        
        match self.unicast_manager.send_to_peer(peer_id, msg).await {
            Ok(_) => {
                // Update statistics
                let mut stats = self.stats.write().await;
                stats.peer_list_requests_sent += 1;
                stats.last_peer_list_request = Some(SystemTime::now());
                
                // Update last request time
                *self.last_peer_list_request_time.write().await = SystemTime::now();
                
                debug!("Successfully sent peer list request to peer: {}", peer_id);
                Ok(())
            }
            Err(e) => {
                warn!("Failed to send peer list request to peer {}: {}", peer_id, e);
                Err(e)
            }
        }
    }

    /// Handle peer list response from a peer
    pub async fn handle_peer_list_response(&self, peer_addresses: Vec<String>) -> Result<()> {
        debug!("Handling peer list response with {} addresses", peer_addresses.len());

        // Validate addresses
        let valid_addresses: Vec<String> = peer_addresses
            .into_iter()
            .filter(|addr| {
                // Validate address format: should contain colon for port
                if !addr.contains(':') {
                    warn!("Invalid peer address format (missing port): {}", addr);
                    return false;
                }
                
                // Parse to ensure it's a valid socket address
                if addr.parse::<std::net::SocketAddr>().is_err() {
                    warn!("Invalid peer address (cannot parse): {}", addr);
                    return false;
                }
                
                true
            })
            .collect();

        if valid_addresses.is_empty() {
            debug!("No valid addresses in peer list response");
            return Ok(());
        }

        debug!("Processing {} valid peer addresses from response", valid_addresses.len());

        // Count new peers before adding
        let existing_count = self.peer_manager.get_candidate_count().await;

        // Add to candidates
        self.peer_manager.add_candidates(valid_addresses.clone()).await?;

        // Count new peers after adding
        let new_count = self.peer_manager.get_candidate_count().await;
        let new_peers_count = (new_count as i64 - existing_count as i64).max(0) as u64;

        // Update statistics
        let mut stats = self.stats.write().await;
        stats.peer_list_responses_received += 1;
        stats.new_peers_discovered += new_peers_count;

        info!(
            "Added {} new peers from peer list response (total candidates: {})",
            new_peers_count, new_count
        );

        Ok(())
    }

    /// Periodically request peer lists from connected peers
    #[allow(dead_code)]
    async fn periodic_peer_list_requests(&self) -> Result<()> {
        // Get all connected peers
        let peers = self.peer_manager.get_all_peers().await;
        let connected_peers: Vec<_> = peers
            .iter()
            .filter(|p| p.is_connected && p.is_healthy)
            .collect();

        if connected_peers.is_empty() {
            debug!("No connected peers available for peer list requests");
            return Ok(());
        }

        // Request from a random subset of connected peers (up to 3)
        let request_count = std::cmp::min(3, connected_peers.len());
        
        // Simple shuffle using system time as seed
        let seed = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as usize;
        
        for i in 0..request_count {
            let idx = (seed + i) % connected_peers.len();
            let peer = connected_peers[idx];
            
            // Send peer list request
            if let Err(e) = self.request_peer_list(&peer.peer_id).await {
                warn!("Failed to request peer list from {}: {}", peer.peer_id, e);
            }
        }

        Ok(())
    }

    /// Start the peer discovery coordinator
    pub async fn start(&self) -> Result<()> {
        if self.is_running.swap(true, Ordering::SeqCst) {
            return Err(P2PError::NetworkError(
                "Peer discovery coordinator already running".to_string(),
            ));
        }

        info!(
            "Starting peer discovery coordinator with min_peers: {}",
            self.config.min_peers
        );

        let peer_manager = self.peer_manager.clone();
        let connection_pool = self.connection_pool.clone();
        let config = self.config.clone();
        let candidate_attempts = self.candidate_attempts.clone();
        let stats = self.stats.clone();
        let mut shutdown_rx = self.shutdown_tx.subscribe();

        let unicast_manager = self.unicast_manager.clone();
        
        tokio::spawn(async move {
            let mut discovery_ticker = interval(Duration::from_secs(10));
            let mut peer_list_ticker = interval(Duration::from_secs(30));

            loop {
                tokio::select! {
                    _ = discovery_ticker.tick() => {
                        // Check if we're below minimum peer threshold
                        let connected_count = peer_manager.get_connected_peer_count().await;

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

                                    // Check if we should attempt connection
                                    let should_attempt = {
                                        let mut attempts = candidate_attempts.write().await;
                                        let attempt = attempts.entry(candidate_addr.clone()).or_insert_with(|| {
                                            CandidateConnectionAttempt {
                                                address: candidate_addr.clone(),
                                                last_attempt: SystemTime::now(),
                                                failure_count: 0,
                                                in_progress: false,
                                            }
                                        });

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
                                            let mut attempts = candidate_attempts.write().await;
                                            if let Some(attempt) = attempts.get_mut(candidate_addr) {
                                                attempt.in_progress = true;
                                                attempt.last_attempt = SystemTime::now();
                                            }
                                        }

                                        // Attempt connection
                                        let start_time = SystemTime::now();
                                        let connection_result = timeout(
                                            Duration::from_secs(config.connection_timeout_secs),
                                            TcpStream::connect(candidate_addr),
                                        )
                                        .await;

                                        let connection_time_ms = start_time
                                            .elapsed()
                                            .unwrap_or_default()
                                            .as_millis() as u64;

                                        match connection_result {
                                            Ok(Ok(stream)) => {
                                                // Connection successful
                                                debug!("Successfully connected to candidate: {}", candidate_addr);

                                                // Add to connection pool
                                                if (connection_pool.add_connection(peer_id.clone(), stream).await).is_ok() {
                                                    // Add to peer manager
                                                    if (peer_manager.add_peer(
                                                        peer_id.clone(),
                                                        candidate_addr.clone(),
                                                        NodeRole::Validator,
                                                    ).await).is_ok() {
                                                        // Mark as connected
                                                        let _ = peer_manager.mark_connected(&peer_id).await;

                                                        // Update stats
                                                        {
                                                            let mut s = stats.write().await;
                                                            s.successful_connections += 1;
                                                            s.total_attempts += 1;
                                                            s.avg_connection_time_ms = (s.avg_connection_time_ms + connection_time_ms) / 2;
                                                        }

                                                        // Reset failure count
                                                        {
                                                            let mut attempts = candidate_attempts.write().await;
                                                            if let Some(attempt) = attempts.get_mut(candidate_addr) {
                                                                attempt.failure_count = 0;
                                                                attempt.in_progress = false;
                                                            }
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
                                                    let mut attempts = candidate_attempts.write().await;
                                                    if let Some(attempt) = attempts.get_mut(candidate_addr) {
                                                        attempt.failure_count += 1;
                                                        attempt.in_progress = false;
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
                                                warn!("Connection timeout to candidate: {}", candidate_addr);

                                                {
                                                    let mut attempts = candidate_attempts.write().await;
                                                    if let Some(attempt) = attempts.get_mut(candidate_addr) {
                                                        attempt.failure_count += 1;
                                                        attempt.in_progress = false;
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
                            } else {
                                debug!("No candidates available for connection");
                            }
                        } else {
                            debug!(
                                "Peer count above threshold: {} >= {}",
                                connected_count, config.min_peers
                            );
                        }

                        // Update tracked candidates count
                        {
                            let attempts = candidate_attempts.read().await;
                            let mut s = stats.write().await;
                            s.tracked_candidates = attempts.len();
                        }
                    }
                    _ = peer_list_ticker.tick() => {
                        // Periodically request peer lists from connected peers
                        debug!("Triggering periodic peer list requests");
                        
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
                                    s.last_peer_list_request = Some(SystemTime::now());
                                }
                            }
                        } else {
                            debug!("No connected peers available for peer list requests");
                        }
                    }
                    _ = shutdown_rx.recv() => {
                        info!("Shutting down peer discovery coordinator");
                        break;
                    }
                }
            }
        });

        Ok(())
    }

    /// Stop the peer discovery coordinator
    pub async fn stop(&self) -> Result<()> {
        if !self.is_running.swap(false, Ordering::SeqCst) {
            return Err(P2PError::NetworkError(
                "Peer discovery coordinator not running".to_string(),
            ));
        }

        let _ = self.shutdown_tx.send(());
        info!("Peer discovery coordinator stopped");
        Ok(())
    }

    /// Check if coordinator is running
    pub fn is_running(&self) -> bool {
        self.is_running.load(Ordering::SeqCst)
    }

    /// Get current peer count monitoring status
    pub async fn get_peer_count_status(&self) -> (usize, usize, bool) {
        let connected = self.peer_manager.get_connected_peer_count().await;
        let total = self.peer_manager.get_peer_count().await;
        let below_threshold = connected < self.config.min_peers;

        (connected, total, below_threshold)
    }

    /// Get candidates that are being monitored
    pub async fn get_monitored_candidates(&self) -> Vec<String> {
        let attempts = self.candidate_attempts.read().await;
        attempts.keys().cloned().collect()
    }

    /// Get connection attempt history for a candidate
    pub async fn get_candidate_attempt_history(&self, address: &str) -> Option<CandidateConnectionAttempt> {
        let attempts = self.candidate_attempts.read().await;
        attempts.get(address).cloned()
    }

    /// Clear failed candidate attempts (recovery mechanism)
    pub async fn reset_candidate_failures(&self, address: &str) -> Result<()> {
        let mut attempts = self.candidate_attempts.write().await;
        if let Some(attempt) = attempts.get_mut(address) {
            attempt.failure_count = 0;
            debug!("Reset failure count for candidate: {}", address);
            Ok(())
        } else {
            Err(P2PError::NetworkError(format!(
                "Candidate not found: {}",
                address
            )))
        }
    }

    /// Remove a candidate from monitoring
    pub async fn remove_monitored_candidate(&self, address: &str) -> Result<()> {
        let mut attempts = self.candidate_attempts.write().await;
        if attempts.remove(address).is_some() {
            debug!("Removed candidate from monitoring: {}", address);
            Ok(())
        } else {
            Err(P2PError::NetworkError(format!(
                "Candidate not found: {}",
                address
            )))
        }
    }

    /// Get discovery statistics
    pub async fn get_stats(&self) -> PeerDiscoveryStats {
        self.stats.read().await.clone()
    }

    /// Get candidates that have exceeded failure threshold
    pub async fn get_failed_candidates(&self, max_failures: u32) -> Vec<String> {
        let attempts = self.candidate_attempts.read().await;
        attempts
            .iter()
            .filter(|(_, attempt)| attempt.failure_count >= max_failures)
            .map(|(addr, _)| addr.clone())
            .collect()
    }

    /// Manually trigger peer discovery check
    pub async fn trigger_discovery_check(&self) -> Result<()> {
        let connected_count = self.peer_manager.get_connected_peer_count().await;

        if connected_count < self.config.min_peers {
            info!(
                "Manual discovery check triggered: {} < {}",
                connected_count, self.config.min_peers
            );

            let mut stats = self.stats.write().await;
            stats.threshold_triggers += 1;

            Ok(())
        } else {
            Ok(())
        }
    }

    /// Get minimum peer threshold configuration
    pub fn get_min_peer_threshold(&self) -> usize {
        self.config.min_peers
    }

    /// Get maximum peer capacity
    pub fn get_max_peer_capacity(&self) -> usize {
        self.config.max_peers
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::NetworkConfig;
    use crate::connection_pool::ConnectionPool;
    use crate::message_handler::MessageHandler;
    use crate::peer_manager::PeerManager;
    use crate::unicast::UnicastManager;

    fn create_test_coordinator(config: NetworkConfig) -> PeerDiscoveryCoordinator {
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

        PeerDiscoveryCoordinator::new(
            peer_manager,
            connection_pool,
            message_handler,
            unicast_manager,
            config,
        )
    }

    #[tokio::test]
    async fn test_coordinator_creation() {
        let config = NetworkConfig::new("node1".to_string(), NodeRole::Validator);
        let coordinator = create_test_coordinator(config);
        assert!(!coordinator.is_running());
    }

    #[tokio::test]
    async fn test_coordinator_start_stop() {
        let config = NetworkConfig::new("node1".to_string(), NodeRole::Validator);
        let coordinator = create_test_coordinator(config);
        assert!(coordinator.start().await.is_ok());
        assert!(coordinator.is_running());

        assert!(coordinator.stop().await.is_ok());
        assert!(!coordinator.is_running());
    }

    #[tokio::test]
    async fn test_peer_count_monitoring() {
        let mut config = NetworkConfig::new("node1".to_string(), NodeRole::Validator);
        config.min_peers = 3;

        let coordinator = create_test_coordinator(config);
        let peer_manager = coordinator.peer_manager.clone();

        // Add some peers
        peer_manager
            .add_peer(
                "peer1".to_string(),
                "127.0.0.1:9000".to_string(),
                NodeRole::Validator,
            )
            .await
            .unwrap();

        peer_manager.mark_connected("peer1").await.unwrap();

        let (connected, total, below_threshold) = coordinator.get_peer_count_status().await;
        assert_eq!(connected, 1);
        assert_eq!(total, 1);
        assert!(below_threshold);
    }

    #[tokio::test]
    async fn test_candidate_monitoring() {
        let config = NetworkConfig::new("node1".to_string(), NodeRole::Validator);
        let coordinator = create_test_coordinator(config);
        let peer_manager = coordinator.peer_manager.clone();

        // Add candidates
        peer_manager
            .add_candidates(vec![
                "127.0.0.1:9001".to_string(),
                "127.0.0.1:9002".to_string(),
            ])
            .await
            .unwrap();

        assert!(coordinator.start().await.is_ok());

        // Give it time to process
        tokio::time::sleep(Duration::from_millis(100)).await;

        let monitored = coordinator.get_monitored_candidates().await;
        assert!(!monitored.is_empty());

        coordinator.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_candidate_attempt_history() {
        let config = NetworkConfig::new("node1".to_string(), NodeRole::Validator);
        let coordinator = create_test_coordinator(config);

        let addr = "127.0.0.1:9001".to_string();

        // Manually add attempt
        {
            let mut attempts = coordinator.candidate_attempts.write().await;
            attempts.insert(
                addr.clone(),
                CandidateConnectionAttempt {
                    address: addr.clone(),
                    last_attempt: SystemTime::now(),
                    failure_count: 2,
                    in_progress: false,
                },
            );
        }

        let history = coordinator.get_candidate_attempt_history(&addr).await;
        assert!(history.is_some());
        assert_eq!(history.unwrap().failure_count, 2);
    }

    #[tokio::test]
    async fn test_reset_candidate_failures() {
        let config = NetworkConfig::new("node1".to_string(), NodeRole::Validator);
        let coordinator = create_test_coordinator(config);

        let addr = "127.0.0.1:9001".to_string();

        // Add attempt with failures
        {
            let mut attempts = coordinator.candidate_attempts.write().await;
            attempts.insert(
                addr.clone(),
                CandidateConnectionAttempt {
                    address: addr.clone(),
                    last_attempt: SystemTime::now(),
                    failure_count: 5,
                    in_progress: false,
                },
            );
        }

        assert!(coordinator.reset_candidate_failures(&addr).await.is_ok());

        let history = coordinator.get_candidate_attempt_history(&addr).await;
        assert_eq!(history.unwrap().failure_count, 0);
    }

    #[tokio::test]
    async fn test_remove_monitored_candidate() {
        let config = NetworkConfig::new("node1".to_string(), NodeRole::Validator);
        let coordinator = create_test_coordinator(config);

        let addr = "127.0.0.1:9001".to_string();

        // Add attempt
        {
            let mut attempts = coordinator.candidate_attempts.write().await;
            attempts.insert(
                addr.clone(),
                CandidateConnectionAttempt {
                    address: addr.clone(),
                    last_attempt: SystemTime::now(),
                    failure_count: 0,
                    in_progress: false,
                },
            );
        }

        assert!(coordinator.remove_monitored_candidate(&addr).await.is_ok());

        let history = coordinator.get_candidate_attempt_history(&addr).await;
        assert!(history.is_none());
    }

    #[tokio::test]
    async fn test_get_failed_candidates() {
        let config = NetworkConfig::new("node1".to_string(), NodeRole::Validator);
        let coordinator = create_test_coordinator(config);

        // Add candidates with different failure counts
        {
            let mut attempts = coordinator.candidate_attempts.write().await;
            attempts.insert(
                "127.0.0.1:9001".to_string(),
                CandidateConnectionAttempt {
                    address: "127.0.0.1:9001".to_string(),
                    last_attempt: SystemTime::now(),
                    failure_count: 3,
                    in_progress: false,
                },
            );
            attempts.insert(
                "127.0.0.1:9002".to_string(),
                CandidateConnectionAttempt {
                    address: "127.0.0.1:9002".to_string(),
                    last_attempt: SystemTime::now(),
                    failure_count: 1,
                    in_progress: false,
                },
            );
        }

        let failed = coordinator.get_failed_candidates(2).await;
        assert_eq!(failed.len(), 1);
        assert!(failed.contains(&"127.0.0.1:9001".to_string()));
    }

    #[tokio::test]
    async fn test_discovery_stats() {
        let config = NetworkConfig::new("node1".to_string(), NodeRole::Validator);
        let coordinator = create_test_coordinator(config);

        let stats = coordinator.get_stats().await;
        assert_eq!(stats.total_attempts, 0);
        assert_eq!(stats.successful_connections, 0);
        assert_eq!(stats.failed_attempts, 0);
    }

    #[tokio::test]
    async fn test_min_peer_threshold_config() {
        let mut config = NetworkConfig::new("node1".to_string(), NodeRole::Validator);
        config.min_peers = 5;

        let coordinator = create_test_coordinator(config);

        assert_eq!(coordinator.get_min_peer_threshold(), 5);
        assert_eq!(coordinator.get_max_peer_capacity(), 100);
    }

    #[tokio::test]
    async fn test_trigger_discovery_check() {
        let mut config = NetworkConfig::new("node1".to_string(), NodeRole::Validator);
        config.min_peers = 3;

        let coordinator = create_test_coordinator(config);

        let stats_before = coordinator.get_stats().await;
        assert_eq!(stats_before.threshold_triggers, 0);

        coordinator.trigger_discovery_check().await.unwrap();

        let stats_after = coordinator.get_stats().await;
        assert_eq!(stats_after.threshold_triggers, 1);
    }

    #[tokio::test]
    async fn test_handle_peer_list_response() {
        let config = NetworkConfig::new("node1".to_string(), NodeRole::Validator);
        let coordinator = create_test_coordinator(config);

        let peer_addresses = vec![
            "192.168.1.1:9000".to_string(),
            "192.168.1.2:9000".to_string(),
            "192.168.1.3:9000".to_string(),
        ];

        let result = coordinator.handle_peer_list_response(peer_addresses.clone()).await;
        assert!(result.is_ok());

        let stats = coordinator.get_stats().await;
        assert_eq!(stats.peer_list_responses_received, 1);
        assert_eq!(stats.new_peers_discovered, 3);

        // Verify candidates were added
        let candidates = coordinator.peer_manager.get_candidates().await;
        assert_eq!(candidates.len(), 3);
    }

    #[tokio::test]
    async fn test_handle_peer_list_response_with_invalid_addresses() {
        let config = NetworkConfig::new("node1".to_string(), NodeRole::Validator);
        let coordinator = create_test_coordinator(config);

        let peer_addresses = vec![
            "192.168.1.1:9000".to_string(),
            "invalid_address".to_string(), // Invalid: no port
            "192.168.1.2:9000".to_string(),
            "999.999.999.999:9000".to_string(), // Invalid: bad IP
        ];

        let result = coordinator.handle_peer_list_response(peer_addresses).await;
        assert!(result.is_ok());

        let stats = coordinator.get_stats().await;
        assert_eq!(stats.peer_list_responses_received, 1);
        // Only 2 valid addresses should be added
        assert_eq!(stats.new_peers_discovered, 2);

        let candidates = coordinator.peer_manager.get_candidates().await;
        assert_eq!(candidates.len(), 2);
    }

    #[tokio::test]
    async fn test_peer_list_response_deduplication() {
        let config = NetworkConfig::new("node1".to_string(), NodeRole::Validator);
        let coordinator = create_test_coordinator(config);

        // First response
        let peer_addresses_1 = vec![
            "192.168.1.1:9000".to_string(),
            "192.168.1.2:9000".to_string(),
        ];
        coordinator.handle_peer_list_response(peer_addresses_1).await.unwrap();

        // Second response with overlapping addresses
        let peer_addresses_2 = vec![
            "192.168.1.2:9000".to_string(), // Duplicate
            "192.168.1.3:9000".to_string(),
        ];
        coordinator.handle_peer_list_response(peer_addresses_2).await.unwrap();

        let stats = coordinator.get_stats().await;
        assert_eq!(stats.peer_list_responses_received, 2);
        // Should only discover 3 unique peers total
        assert_eq!(stats.new_peers_discovered, 3);

        let candidates = coordinator.peer_manager.get_candidates().await;
        assert_eq!(candidates.len(), 3);
    }

    #[tokio::test]
    async fn test_peer_list_stats_tracking() {
        let config = NetworkConfig::new("node1".to_string(), NodeRole::Validator);
        let coordinator = create_test_coordinator(config);

        let stats_initial = coordinator.get_stats().await;
        assert_eq!(stats_initial.peer_list_requests_sent, 0);
        assert_eq!(stats_initial.peer_list_responses_received, 0);
        assert_eq!(stats_initial.new_peers_discovered, 0);

        // Simulate peer list response
        let peer_addresses = vec![
            "192.168.1.1:9000".to_string(),
            "192.168.1.2:9000".to_string(),
        ];
        coordinator.handle_peer_list_response(peer_addresses).await.unwrap();

        let stats_after = coordinator.get_stats().await;
        assert_eq!(stats_after.peer_list_responses_received, 1);
        assert_eq!(stats_after.new_peers_discovered, 2);
    }
}
