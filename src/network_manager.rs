//! Main P2P network manager coordinating all components

use crate::bootstrap_connector::BootstrapConnector;
use crate::broadcast::BroadcastManager;
use crate::config::NetworkConfig;
use crate::connection_pool::ConnectionPool;
use crate::error::{P2PError, Result};
use crate::event_loop::NetworkEventLoop;
use crate::health_monitor::HealthMonitor;
use crate::message_handler::MessageHandler;
use crate::peer_discovery_coordinator::PeerDiscoveryCoordinator;
use crate::peer_manager::PeerManager;
use crate::shutdown_coordination::ShutdownCoordinator;
use crate::types::{NetworkMessage, PeerInfo, NetworkStats, PeerStats, HealthStatus, NodeRole};
use crate::unicast::UnicastManager;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::SystemTime;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::broadcast;
use tokio::time::{timeout, Duration};
use tracing::{error, info, warn};

/// Main P2P network manager coordinating all network operations
pub struct P2PNetworkManager {
    /// Network configuration
    config: NetworkConfig,
    /// Peer manager for tracking peers
    pub peer_manager: Arc<PeerManager>,
    /// Connection pool for managing TCP connections
    connection_pool: Arc<ConnectionPool>,
    /// Message handler for serialization/deserialization
    message_handler: Arc<MessageHandler>,
    /// Health monitor for peer health checks
    health_monitor: Arc<HealthMonitor>,
    /// Broadcast manager for sending messages to multiple peers
    broadcast_manager: Arc<BroadcastManager>,
    /// Unicast manager for sending messages to specific peers
    unicast_manager: Arc<UnicastManager>,
    /// Peer discovery coordinator for minimum peer threshold enforcement
    discovery_coordinator: Arc<PeerDiscoveryCoordinator>,
    /// Bootstrap connector for bootstrap node connections
    bootstrap_connector: Arc<BootstrapConnector>,
    /// Shutdown coordinator for coordinated shutdown
    shutdown_coordinator: Arc<ShutdownCoordinator>,
    /// Shutdown signal broadcaster
    shutdown_tx: broadcast::Sender<()>,
    /// Network manager start time
    start_time: SystemTime,
}

impl P2PNetworkManager {
    /// Create new network manager
    pub async fn new(config: NetworkConfig) -> Result<Self> {
        config.validate()?;

        let peer_manager = Arc::new(PeerManager::new(config.max_candidates));
        let connection_pool = Arc::new(ConnectionPool::new(config.max_peers, config.max_backoff_secs));
        let message_handler = Arc::new(MessageHandler::new_with_error_handler(
            config.max_message_size_bytes,
            peer_manager.clone(),
            connection_pool.clone(),
        ));
        let health_monitor = Arc::new(HealthMonitor::new(
            peer_manager.clone(),
            connection_pool.clone(),
            message_handler.clone(),
            config.ping_interval_secs,
            config.pong_timeout_secs,
            config.peer_timeout_secs,
        ));

        let broadcast_manager = Arc::new(BroadcastManager::new(
            peer_manager.clone(),
            connection_pool.clone(),
            message_handler.clone(),
            config.broadcast_timeout_secs,
            config.max_concurrent_broadcasts,
        ));

        let unicast_manager = Arc::new(UnicastManager::new(
            peer_manager.clone(),
            connection_pool.clone(),
            message_handler.clone(),
            config.unicast_timeout_secs,
            config.node_id.clone(),
        ));

        let discovery_coordinator = Arc::new(PeerDiscoveryCoordinator::new(
            peer_manager.clone(),
            connection_pool.clone(),
            message_handler.clone(),
            unicast_manager.clone(),
            config.clone(),
        ));

        let bootstrap_connector = Arc::new(BootstrapConnector::new(
            peer_manager.clone(),
            connection_pool.clone(),
            config.clone(),
        ));

        let (shutdown_tx, _) = broadcast::channel(1);

        let shutdown_coordinator = Arc::new(ShutdownCoordinator::new(
            peer_manager.clone(),
            connection_pool.clone(),
            discovery_coordinator.clone(),
            bootstrap_connector.clone(),
            shutdown_tx.clone(),
        ));

        Ok(Self {
            config,
            peer_manager,
            connection_pool,
            message_handler,
            health_monitor,
            broadcast_manager,
            unicast_manager,
            discovery_coordinator,
            bootstrap_connector,
            shutdown_coordinator,
            shutdown_tx,
            start_time: SystemTime::now(),
        })
    }

    /// Start the network manager
    pub async fn start(&self) -> Result<()> {
        info!("Starting P2P network manager for node: {}", self.config.node_id);

        // Start health monitoring
        if self.config.enable_health_monitoring {
            self.health_monitor.start_monitoring().await?;
            info!("Health monitoring started");
        }

        // Start TCP listener
        let listener = TcpListener::bind(self.config.full_listen_addr())
            .await
            .map_err(|e| P2PError::NetworkError(format!("Failed to bind listener: {}", e)))?;

        info!("TCP listener started on {}", self.config.full_listen_addr());

        // Connect to initial peers
        for peer_addr in &self.config.peers {
            let peer_id = format!("peer_{}", peer_addr.replace(":", "_"));
            self.connect_to_peer(&peer_id, peer_addr).await.ok();
        }

        // Connect to bootstrap nodes
        for bootstrap_addr in &self.config.bootstrap_nodes {
            let peer_id = format!("bootstrap_{}", bootstrap_addr.replace(":", "_"));
            self.connect_to_peer(&peer_id, bootstrap_addr).await.ok();
        }

        // Start bootstrap connector if bootstrap nodes are configured
        if !self.config.bootstrap_nodes.is_empty() {
            self.bootstrap_connector.start().await?;
            info!("Bootstrap connector started with {} bootstrap nodes", self.config.bootstrap_nodes.len());
        }

        // Start peer discovery coordinator if enabled
        if self.config.enable_peer_discovery {
            self.discovery_coordinator.start().await?;
            info!("Peer discovery coordinator started with min_peers: {}", self.config.min_peers);
        }

        // Start legacy peer discovery if enabled (for backward compatibility)
        if self.config.enable_peer_discovery {
            self.start_peer_discovery().await?;
            info!("Peer discovery started");
        }

        // Start the main event loop for accepting connections and handling messages
        let shutdown_rx = self.shutdown_tx.subscribe();
        let event_loop = NetworkEventLoop::new(
            listener,
            self.peer_manager.clone(),
            self.connection_pool.clone(),
            self.message_handler.clone(),
            shutdown_rx,
            self.config.message_timeout_secs,
            self.config.max_message_size_bytes,
        );

        tokio::spawn(async move {
            if let Err(e) = event_loop.run().await {
                error!("Event loop error: {}", e);
            }
        });

        info!("P2P network manager started successfully");
        Ok(())
    }

    /// Connect to a peer
    async fn connect_to_peer(&self, peer_id: &str, addr: &str) -> Result<()> {
        match timeout(
            Duration::from_secs(self.config.connection_timeout_secs),
            TcpStream::connect(addr),
        )
        .await
        {
            Ok(Ok(stream)) => {
                self.connection_pool
                    .add_connection(peer_id.to_string(), stream)
                    .await?;

                self.peer_manager
                    .add_peer(
                        peer_id.to_string(),
                        addr.to_string(),
                        crate::types::NodeRole::Validator,
                    )
                    .await?;

                info!("Connected to peer: {} at {}", peer_id, addr);
                Ok(())
            }
            Ok(Err(e)) => {
                warn!("Failed to connect to {}: {}", addr, e);
                Err(P2PError::ConnectionError(e.to_string()))
            }
            Err(_) => {
                warn!("Connection timeout to {}", addr);
                Err(P2PError::ConnectionTimeout(addr.to_string()))
            }
        }
    }

    /// Start peer discovery loop
    async fn start_peer_discovery(&self) -> Result<()> {
        let peer_manager = self.peer_manager.clone();
        let config = self.config.clone();

        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(Duration::from_secs(60));

            loop {
                ticker.tick().await;

                let mut connected_count = peer_manager.get_connected_peer_count().await;
                if connected_count < config.min_peers {
                    let candidates = peer_manager.get_candidates().await;
                    for candidate in candidates.iter().take(5) {
                        let peer_id = format!("candidate_{}", candidate.replace(":", "_"));
                        // Attempt to add candidate peer
                        match peer_manager.add_peer(peer_id.clone(), candidate.clone(), NodeRole::RPC).await {
                            Ok(_) => {
                                // Mark as connected
                                if (peer_manager.mark_connected(&peer_id).await).is_ok() {
                                    info!("Successfully connected to candidate: {}", candidate);
                                    connected_count += 1;
                                    // Update peer count after successful connection
                                    if connected_count >= config.min_peers {
                                        break;
                                    }
                                }
                            }
                            Err(e) => {
                                warn!("Failed to connect to candidate {}: {}", candidate, e);
                            }
                        }
                    }
                }
            }
        });

        Ok(())
    }

    /// Broadcast message to all connected peers
    pub async fn broadcast(&self, msg: NetworkMessage) -> Result<usize> {
        let result = self.broadcast_manager.broadcast(msg).await?;
        
        if result.failed_sends > 0 {
            warn!(
                "Broadcast completed with failures: {} successful, {} failed",
                result.successful_sends, result.failed_sends
            );
        } else {
            info!("Broadcast completed successfully to {} peers", result.successful_sends);
        }

        Ok(result.successful_sends)
    }

    /// Broadcast message to specific peers
    pub async fn broadcast_to_peers(
        &self,
        msg: NetworkMessage,
        peer_ids: Vec<String>,
    ) -> Result<usize> {
        let result = self.broadcast_manager.broadcast_to_peers(msg, peer_ids).await?;
        
        if result.failed_sends > 0 {
            warn!(
                "Targeted broadcast completed with failures: {} successful, {} failed",
                result.successful_sends, result.failed_sends
            );
        } else {
            info!("Targeted broadcast completed successfully to {} peers", result.successful_sends);
        }

        Ok(result.successful_sends)
    }

    /// Get broadcast statistics
    pub async fn get_broadcast_stats(&self) -> crate::broadcast::BroadcastStats {
        self.broadcast_manager.get_broadcast_stats().await
    }

    /// Send message to specific peer with timeout and health tracking
    pub async fn send_to_peer(&self, peer_id: &str, msg: NetworkMessage) -> Result<()> {
        match self.unicast_manager.send_to_peer(peer_id, msg).await {
            Ok(()) => {
                info!("Successfully sent message to peer: {}", peer_id);
                Ok(())
            }
            Err(e) => {
                warn!("Failed to send message to peer {}: {}", peer_id, e);
                Err(e)
            }
        }
    }

    /// Send message to multiple specific peers
    pub async fn send_to_peers(
        &self,
        peer_ids: Vec<String>,
        msg: NetworkMessage,
    ) -> Result<usize> {
        let result = self.unicast_manager.send_to_peers(peer_ids, msg).await?;
        
        if result.failed_sends > 0 {
            warn!(
                "Unicast to peers completed with failures: {} successful, {} failed",
                result.successful_sends, result.failed_sends
            );
        } else {
            info!("Unicast to peers completed successfully to {} peers", result.successful_sends);
        }

        Ok(result.successful_sends)
    }

    /// Get unicast statistics
    pub async fn get_unicast_stats(&self) -> crate::unicast::UnicastStats {
        self.unicast_manager.get_unicast_stats().await
    }

    /// Get peer information
    pub async fn get_peer_info(&self, peer_id: &str) -> Result<PeerInfo> {
        let peer = self.peer_manager.get_peer(peer_id).await?;
        Ok(PeerInfo::from(&peer))
    }

    /// Get all known peers with their status information
    /// Returns a vector of PeerInfo containing peer ID, address, role, connection status, health status, and block height
    /// This is a non-blocking operation that returns immediately without affecting network operations
    pub async fn get_all_peers(&self) -> Vec<PeerInfo> {
        self.peer_manager
            .get_all_peers()
            .await
            .iter()
            .map(PeerInfo::from)
            .collect()
    }

    /// Get network statistics
    pub async fn get_network_stats(&self) -> NetworkStats {
        let peers = self.peer_manager.get_all_peers().await;
        let candidates = self.peer_manager.get_candidates().await;

        let connected_count = peers.iter().filter(|p| p.is_connected).count();
        let total_messages_sent = peers.iter().map(|p| p.messages_sent).sum::<u64>();
        let total_messages_received = peers.iter().map(|p| p.messages_received).sum::<u64>();
        let total_bytes_sent = peers.iter().map(|p| p.bytes_sent).sum::<u64>();
        let total_bytes_received = peers.iter().map(|p| p.bytes_received).sum::<u64>();

        let avg_latency = if !peers.is_empty() {
            peers.iter().map(|p| p.latency_ms).sum::<u64>() / peers.len() as u64
        } else {
            0
        };

        let uptime = self
            .start_time
            .elapsed()
            .unwrap_or_default()
            .as_secs();

        let mut peer_stats = HashMap::new();
        for peer in &peers {
            peer_stats.insert(
                peer.peer_id.clone(),
                PeerStats {
                    messages_sent: peer.messages_sent,
                    messages_received: peer.messages_received,
                    bytes_sent: peer.bytes_sent,
                    bytes_received: peer.bytes_received,
                    latency_ms: peer.latency_ms,
                    is_connected: peer.is_connected,
                },
            );
        }

        NetworkStats {
            connected_peers: connected_count,
            total_peers: peers.len(),
            candidate_peers: candidates.len(),
            total_messages_sent,
            total_messages_received,
            total_bytes_sent,
            total_bytes_received,
            avg_latency_ms: avg_latency,
            uptime_secs: uptime,
            peer_stats,
        }
    }

    /// Get health status
    pub async fn get_health_status(&self) -> HealthStatus {
        self.health_monitor.get_health_status().await
    }

    /// Get peer discovery coordinator
    pub fn get_discovery_coordinator(&self) -> Arc<PeerDiscoveryCoordinator> {
        self.discovery_coordinator.clone()
    }

    /// Get bootstrap connector
    pub fn get_bootstrap_connector(&self) -> Arc<BootstrapConnector> {
        self.bootstrap_connector.clone()
    }

    /// Get peer discovery statistics
    pub async fn get_discovery_stats(&self) -> crate::peer_discovery_coordinator::PeerDiscoveryStats {
        self.discovery_coordinator.get_stats().await
    }

    /// Get current peer count status
    pub async fn get_peer_count_status(&self) -> (usize, usize, bool) {
        self.discovery_coordinator.get_peer_count_status().await
    }

    /// Get peer metrics for a specific peer
    /// Returns messages sent/received, bytes sent/received, and latency
    /// This is a non-blocking operation that returns immediately without affecting network operations
    /// 
    /// # Arguments
    /// * `peer_id` - The ID of the peer to retrieve metrics for
    /// 
    /// # Returns
    /// * `Result<PeerStats>` - Peer statistics including messages and bytes sent/received, and latency
    /// 
    /// # Errors
    /// Returns `P2PError::PeerNotFound` if the peer does not exist
    pub async fn get_peer_metrics(&self, peer_id: &str) -> Result<PeerStats> {
        let peer = self.peer_manager.get_peer(peer_id).await?;
        Ok(PeerStats {
            messages_sent: peer.messages_sent,
            messages_received: peer.messages_received,
            bytes_sent: peer.bytes_sent,
            bytes_received: peer.bytes_received,
            latency_ms: peer.latency_ms,
            is_connected: peer.is_connected,
        })
    }

    /// Get metrics for all peers
    /// Returns a map of peer IDs to their statistics
    /// This is a non-blocking operation that returns immediately without affecting network operations
    /// 
    /// # Returns
    /// * `HashMap<String, PeerStats>` - Map of peer ID to peer statistics
    pub async fn get_all_peer_metrics(&self) -> HashMap<String, PeerStats> {
        let peers = self.peer_manager.get_all_peers().await;
        let mut metrics = HashMap::new();
        
        for peer in peers {
            metrics.insert(
                peer.peer_id.clone(),
                PeerStats {
                    messages_sent: peer.messages_sent,
                    messages_received: peer.messages_received,
                    bytes_sent: peer.bytes_sent,
                    bytes_received: peer.bytes_received,
                    latency_ms: peer.latency_ms,
                    is_connected: peer.is_connected,
                },
            );
        }
        
        metrics
    }

    /// Graceful shutdown with 30-second grace period for pending messages
    /// 
    /// This method implements a graceful shutdown sequence using the ShutdownCoordinator:
    /// 1. Stop accepting new connections
    /// 2. Wait up to 30 seconds for pending messages to complete
    /// 3. Stop peer discovery and bootstrap components
    /// 4. Close all peer connections
    /// 5. Clear all peer state
    /// 6. Release all network resources
    /// 7. Exit cleanly
    /// 
    /// This method is idempotent - calling it multiple times is safe and will succeed.
    /// 
    /// # Returns
    /// * `Result<()>` - Success or error during shutdown
    pub async fn shutdown(&self) -> Result<()> {
        info!("Starting graceful shutdown of P2P network manager");

        // Use the shutdown coordinator to perform coordinated shutdown
        match self.shutdown_coordinator.shutdown().await {
            Ok(stats) => {
                info!(
                    "Graceful shutdown completed in {} ms: {} connections closed, {} peers cleared, {} candidates cleared",
                    stats.shutdown_time_ms, stats.connections_closed, stats.peers_cleared, stats.candidates_cleared
                );
                
                if stats.grace_period_exceeded {
                    warn!("Grace period was exceeded during shutdown");
                }
                
                if !stats.all_components_stopped {
                    warn!("Some components did not stop cleanly during shutdown");
                }
                
                Ok(())
            }
            Err(e) => {
                // If shutdown is already in progress, that's OK - just return success
                // This makes the shutdown method idempotent
                if e.to_string().contains("already in progress") || e.to_string().contains("Shutdown already in progress") {
                    info!("Shutdown already in progress, returning success");
                    Ok(())
                } else {
                    warn!("Error during graceful shutdown: {}", e);
                    Err(e)
                }
            }
        }
    }

    /// Get shutdown coordinator
    pub fn get_shutdown_coordinator(&self) -> Arc<ShutdownCoordinator> {
        self.shutdown_coordinator.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::NodeRole;

    #[tokio::test]
    async fn test_network_manager_creation() {
        let config = NetworkConfig::new("node1".to_string(), NodeRole::Validator);
        let manager = P2PNetworkManager::new(config).await.unwrap();
        assert_eq!(manager.config.node_id, "node1");
    }

    #[tokio::test]
    async fn test_network_stats() {
        let config = NetworkConfig::new("node1".to_string(), NodeRole::Validator);
        let manager = P2PNetworkManager::new(config).await.unwrap();
        let stats = manager.get_network_stats().await;
        assert_eq!(stats.connected_peers, 0);
    }

    #[tokio::test]
    async fn test_health_status() {
        let config = NetworkConfig::new("node1".to_string(), NodeRole::Validator);
        let manager = P2PNetworkManager::new(config).await.unwrap();
        let health = manager.get_health_status().await;
        assert!(!health.is_healthy);
    }
}
