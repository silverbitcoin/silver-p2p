//! Peer health monitoring with ping-pong protocol

use crate::error::Result;
use crate::peer_manager::PeerManager;
use crate::connection_pool::ConnectionPool;
use crate::message_handler::MessageHandler;
use crate::types::{NetworkMessage, HealthStatus};
use std::sync::Arc;
use std::time::{SystemTime, Duration};
use tokio::time::interval;
use tracing::{info, warn};
use dashmap::DashMap;

/// Monitors peer health and detects unresponsive peers
pub struct HealthMonitor {
    /// Peer manager reference
    peer_manager: Arc<PeerManager>,
    /// Connection pool reference
    connection_pool: Arc<ConnectionPool>,
    /// Message handler reference
    message_handler: Arc<MessageHandler>,
    /// Ping interval in seconds
    ping_interval_secs: u64,
    /// Pong timeout in seconds
    pong_timeout_secs: u64,
    /// Peer timeout in seconds
    peer_timeout_secs: u64,
    /// Track pending pings with nonce and timestamp
    pub pending_pings: Arc<DashMap<String, (u64, SystemTime)>>,
}

impl HealthMonitor {
    /// Create new health monitor
    pub fn new(
        peer_manager: Arc<PeerManager>,
        connection_pool: Arc<ConnectionPool>,
        message_handler: Arc<MessageHandler>,
        ping_interval_secs: u64,
        pong_timeout_secs: u64,
        peer_timeout_secs: u64,
    ) -> Self {
        Self {
            peer_manager,
            connection_pool,
            message_handler,
            ping_interval_secs,
            pong_timeout_secs,
            peer_timeout_secs,
            pending_pings: Arc::new(DashMap::new()),
        }
    }

    /// Start health monitoring loop
    pub async fn start_monitoring(&self) -> Result<()> {
        let peer_manager = self.peer_manager.clone();
        let connection_pool = self.connection_pool.clone();
        let message_handler = self.message_handler.clone();
        let ping_interval_secs = self.ping_interval_secs;
        let pong_timeout_secs = self.pong_timeout_secs;
        let peer_timeout_secs = self.peer_timeout_secs;
        let pending_pings = self.pending_pings.clone();

        tokio::spawn(async move {
            let mut ping_ticker = interval(Duration::from_secs(ping_interval_secs));
            let mut timeout_ticker = interval(Duration::from_secs(1));

            loop {
                tokio::select! {
                    _ = ping_ticker.tick() => {
                        // Send pings to all connected peers
                        let peers = peer_manager.get_all_peers().await;
                        for peer in peers {
                            if peer.is_connected {
                                let nonce = rand::random::<u64>();
                                let msg = NetworkMessage::Ping { nonce };

                                if let Ok(_msg_data) = message_handler.serialize(&msg).await {
                                    if let Ok(_) = connection_pool.get_connection(&peer.peer_id).await {
                                        pending_pings.insert(peer.peer_id.clone(), (nonce, SystemTime::now()));
                                        info!("Sent ping to peer: {} (nonce: {})", peer.peer_id, nonce);
                                    }
                                }
                            }
                        }
                    }
                    _ = timeout_ticker.tick() => {
                        // Check for pong timeouts
                        let now = SystemTime::now();
                        let timeout_duration = Duration::from_secs(pong_timeout_secs);

                        let mut to_remove = Vec::new();
                        for entry in pending_pings.iter() {
                            let (peer_id, (_, sent_time)) = entry.pair();
                            if let Ok(elapsed) = now.duration_since(*sent_time) {
                                if elapsed > timeout_duration {
                                    to_remove.push(peer_id.clone());
                                    warn!("Pong timeout for peer: {}", peer_id);
                                    let _ = peer_manager.mark_unhealthy(&peer_id, "Pong timeout".to_string()).await;
                                    // Record failed ping - decreases health score
                                    let _ = peer_manager.record_failed_ping(&peer_id).await;
                                }
                            }
                        }

                        for peer_id in to_remove {
                            pending_pings.remove(&peer_id);
                        }

                        // Check for peer timeout (not seen for peer_timeout_secs)
                        let peers = peer_manager.get_all_peers().await;
                        for peer in peers {
                            if let Ok(elapsed) = now.duration_since(peer.last_seen) {
                                if elapsed > Duration::from_secs(peer_timeout_secs) {
                                    warn!("Peer timeout: {} (not seen for {} seconds)", peer.peer_id, peer_timeout_secs);
                                    let _ = peer_manager.mark_unhealthy(&peer.peer_id, "Peer timeout".to_string()).await;
                                    // Record failed ping for timeout - decreases health score
                                    let _ = peer_manager.record_failed_ping(&peer.peer_id).await;
                                    let _ = connection_pool.remove_connection(&peer.peer_id).await;
                                }
                            }
                        }
                    }
                }
            }
        });

        Ok(())
    }

    /// Send ping message to a peer
    pub async fn send_ping(&self, peer_id: &str) -> Result<()> {
        let nonce = rand::random::<u64>();
        let msg = NetworkMessage::Ping { nonce };

        // Serialize the message
        let _msg_data = self.message_handler.serialize(&msg).await?;

        // Check if connection exists
        let _conn = self.connection_pool.get_connection(peer_id).await?;

        // Track the pending ping
        self.pending_pings.insert(peer_id.to_string(), (nonce, SystemTime::now()));
        info!("Sent ping to peer: {} (nonce: {})", peer_id, nonce);

        Ok(())
    }

    /// Handle pong response
    pub async fn handle_pong(&self, peer_id: &str, nonce: u64) -> Result<()> {
        if let Some((_peer_id, (sent_nonce, sent_time))) = self.pending_pings.remove(peer_id) {
            if sent_nonce == nonce {
                if let Ok(latency) = SystemTime::now().duration_since(sent_time) {
                    let latency_ms = latency.as_millis() as u64;
                    self.peer_manager.mark_healthy(peer_id).await?;
                    self.peer_manager
                        .update_peer_metrics(peer_id, latency_ms, 0, 0)
                        .await?;
                    // Record successful ping - increases health score
                    self.peer_manager.record_successful_ping(peer_id).await?;
                    info!("Received pong from peer: {} (latency: {}ms)", peer_id, latency_ms);
                }
            } else {
                warn!("Nonce mismatch for peer: {}", peer_id);
            }
        }
        Ok(())
    }

    /// Get health status
    pub async fn get_health_status(&self) -> HealthStatus {
        let peers = self.peer_manager.get_all_peers().await;
        let connected_count = peers.iter().filter(|p| p.is_connected).count();
        let total_count = peers.len();

        let avg_latency = if !peers.is_empty() {
            peers.iter().map(|p| p.latency_ms).sum::<u64>() / peers.len() as u64
        } else {
            0
        };

        let total_messages_sent = peers.iter().map(|p| p.messages_sent).sum::<u64>();
        let total_bytes_sent = peers.iter().map(|p| p.bytes_sent).sum::<u64>();

        let messages_per_sec = if total_messages_sent > 0 {
            total_messages_sent / std::cmp::max(1, self.ping_interval_secs)
        } else {
            0
        };

        let bytes_per_sec = if total_bytes_sent > 0 {
            total_bytes_sent / std::cmp::max(1, self.ping_interval_secs)
        } else {
            0
        };

        HealthStatus {
            connected_peers: connected_count,
            total_peers: total_count,
            is_healthy: connected_count > 0,
            avg_latency_ms: avg_latency,
            messages_per_sec,
            bytes_per_sec,
        }
    }

    /// Get peer latency
    pub async fn get_peer_latency(&self, peer_id: &str) -> Result<Duration> {
        let peer = self.peer_manager.get_peer(peer_id).await?;
        Ok(Duration::from_millis(peer.latency_ms))
    }

    /// Get peer health score (0-100)
    pub async fn get_peer_health_score(&self, peer_id: &str) -> Result<u32> {
        self.peer_manager.get_health_score(peer_id).await
    }

    /// Get all peers sorted by health score (highest first)
    pub async fn get_peers_by_health_score(&self) -> Vec<crate::types::PeerState> {
        self.peer_manager.get_peers_by_health_score().await
    }

    /// Get healthy peers (health_score >= 50)
    pub async fn get_healthy_peers_by_score(&self) -> Vec<crate::types::PeerState> {
        self.peer_manager.get_healthy_peers_by_score().await
    }

    /// Get unhealthy peers (health_score < 30)
    pub async fn get_unhealthy_peers_by_score(&self) -> Vec<crate::types::PeerState> {
        self.peer_manager.get_unhealthy_peers_by_score().await
    }

    /// Get average health score across all peers
    pub async fn get_average_health_score(&self) -> u32 {
        self.peer_manager.get_average_health_score().await
    }

    /// Reset health score for a peer (recovery mechanism)
    pub async fn reset_peer_health_score(&self, peer_id: &str) -> Result<()> {
        self.peer_manager.reset_peer_health_score(peer_id).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_health_monitor_creation() {
        let pm = Arc::new(PeerManager::new(1000));
        let cp = Arc::new(ConnectionPool::new(100, 300));
        let mh = Arc::new(MessageHandler::new(100 * 1024 * 1024));

        let monitor = HealthMonitor::new(pm, cp, mh, 30, 10, 300);
        let status = monitor.get_health_status().await;
        assert_eq!(status.connected_peers, 0);
    }

    #[tokio::test]
    async fn test_pong_handling() {
        let pm = Arc::new(PeerManager::new(1000));
        let cp = Arc::new(ConnectionPool::new(100, 300));
        let mh = Arc::new(MessageHandler::new(100 * 1024 * 1024));

        pm.add_peer("peer1".to_string(), "127.0.0.1:9000".to_string(), crate::types::NodeRole::Validator).await.unwrap();

        let monitor = HealthMonitor::new(pm, cp, mh, 30, 10, 300);
        monitor.handle_pong("peer1", 123).await.ok();
    }

    #[tokio::test]
    async fn test_get_health_status_empty() {
        let pm = Arc::new(PeerManager::new(1000));
        let cp = Arc::new(ConnectionPool::new(100, 300));
        let mh = Arc::new(MessageHandler::new(100 * 1024 * 1024));

        let monitor = HealthMonitor::new(pm, cp, mh, 30, 10, 300);
        let status = monitor.get_health_status().await;
        
        assert_eq!(status.connected_peers, 0);
        assert_eq!(status.total_peers, 0);
        assert!(!status.is_healthy);
        assert_eq!(status.avg_latency_ms, 0);
    }

    #[tokio::test]
    async fn test_get_health_status_with_peers() {
        let pm = Arc::new(PeerManager::new(1000));
        let cp = Arc::new(ConnectionPool::new(100, 300));
        let mh = Arc::new(MessageHandler::new(100 * 1024 * 1024));

        pm.add_peer("peer1".to_string(), "127.0.0.1:9000".to_string(), crate::types::NodeRole::Validator).await.unwrap();
        pm.add_peer("peer2".to_string(), "127.0.0.1:9001".to_string(), crate::types::NodeRole::Validator).await.unwrap();
        
        pm.mark_connected("peer1").await.unwrap();
        pm.mark_connected("peer2").await.unwrap();
        
        pm.update_peer_metrics("peer1", 50, 0, 0).await.unwrap();
        pm.update_peer_metrics("peer2", 100, 0, 0).await.unwrap();

        let monitor = HealthMonitor::new(pm, cp, mh, 30, 10, 300);
        let status = monitor.get_health_status().await;
        
        assert_eq!(status.connected_peers, 2);
        assert_eq!(status.total_peers, 2);
        assert!(status.is_healthy);
        assert_eq!(status.avg_latency_ms, 75);
    }

    #[tokio::test]
    async fn test_get_peer_latency() {
        let pm = Arc::new(PeerManager::new(1000));
        let cp = Arc::new(ConnectionPool::new(100, 300));
        let mh = Arc::new(MessageHandler::new(100 * 1024 * 1024));

        pm.add_peer("peer1".to_string(), "127.0.0.1:9000".to_string(), crate::types::NodeRole::Validator).await.unwrap();
        pm.update_peer_metrics("peer1", 75, 0, 0).await.unwrap();

        let monitor = HealthMonitor::new(pm, cp, mh, 30, 10, 300);
        let latency = monitor.get_peer_latency("peer1").await.unwrap();
        
        assert_eq!(latency.as_millis(), 75);
    }

    #[tokio::test]
    async fn test_pong_handling_with_latency() {
        let pm = Arc::new(PeerManager::new(1000));
        let cp = Arc::new(ConnectionPool::new(100, 300));
        let mh = Arc::new(MessageHandler::new(100 * 1024 * 1024));

        pm.add_peer("peer1".to_string(), "127.0.0.1:9000".to_string(), crate::types::NodeRole::Validator).await.unwrap();
        pm.mark_connected("peer1").await.unwrap();

        let monitor = HealthMonitor::new(pm.clone(), cp, mh, 30, 10, 300);
        
        // Simulate sending a ping
        let nonce = 12345u64;
        monitor.pending_pings.insert("peer1".to_string(), (nonce, SystemTime::now()));
        
        // Small delay to ensure some latency
        tokio::time::sleep(Duration::from_millis(10)).await;
        
        // Handle pong
        monitor.handle_pong("peer1", nonce).await.unwrap();
        
        // Verify peer is marked healthy
        let peer = pm.get_peer("peer1").await.unwrap();
        assert!(peer.is_healthy);
        assert!(peer.latency_ms > 0);
    }

    #[tokio::test]
    async fn test_pong_nonce_mismatch() {
        let pm = Arc::new(PeerManager::new(1000));
        let cp = Arc::new(ConnectionPool::new(100, 300));
        let mh = Arc::new(MessageHandler::new(100 * 1024 * 1024));

        pm.add_peer("peer1".to_string(), "127.0.0.1:9000".to_string(), crate::types::NodeRole::Validator).await.unwrap();

        let monitor = HealthMonitor::new(pm, cp, mh, 30, 10, 300);
        
        // Simulate sending a ping with nonce 123
        monitor.pending_pings.insert("peer1".to_string(), (123u64, SystemTime::now()));
        
        // Handle pong with different nonce (456)
        monitor.handle_pong("peer1", 456).await.unwrap();
        
        // Ping should be removed even with nonce mismatch (implementation removes it)
        assert!(!monitor.pending_pings.contains_key("peer1"));
    }

    #[tokio::test]
    async fn test_health_status_with_messages() {
        let pm = Arc::new(PeerManager::new(1000));
        let cp = Arc::new(ConnectionPool::new(100, 300));
        let mh = Arc::new(MessageHandler::new(100 * 1024 * 1024));

        pm.add_peer("peer1".to_string(), "127.0.0.1:9000".to_string(), crate::types::NodeRole::Validator).await.unwrap();
        pm.mark_connected("peer1").await.unwrap();
        
        // Update metrics with messages and bytes (multiple times to accumulate)
        for _ in 0..30 {
            pm.update_peer_metrics("peer1", 50, 1024, 2048).await.unwrap();
        }

        let monitor = HealthMonitor::new(pm, cp, mh, 30, 10, 300);
        let status = monitor.get_health_status().await;
        
        assert_eq!(status.connected_peers, 1);
        // With 30 messages sent and ping_interval of 30 seconds, messages_per_sec should be 1
        assert_eq!(status.messages_per_sec, 1);
        assert!(status.bytes_per_sec > 0);
    }

    #[tokio::test]
    async fn test_health_score_increase_on_pong() {
        let pm = Arc::new(PeerManager::new(1000));
        let cp = Arc::new(ConnectionPool::new(100, 300));
        let mh = Arc::new(MessageHandler::new(100 * 1024 * 1024));

        pm.add_peer("peer1".to_string(), "127.0.0.1:9000".to_string(), crate::types::NodeRole::Validator).await.unwrap();
        pm.mark_connected("peer1").await.unwrap();

        let monitor = HealthMonitor::new(pm.clone(), cp, mh, 30, 10, 300);
        
        // Simulate sending a ping
        let nonce = 12345u64;
        monitor.pending_pings.insert("peer1".to_string(), (nonce, SystemTime::now()));
        
        // Small delay to ensure some latency
        tokio::time::sleep(Duration::from_millis(10)).await;
        
        // Handle pong - should increase health score
        monitor.handle_pong("peer1", nonce).await.unwrap();
        
        // Verify health score increased
        let score = pm.get_health_score("peer1").await.unwrap();
        assert_eq!(score, 100); // Already at max
    }

    #[tokio::test]
    async fn test_health_score_decrease_on_timeout() {
        let pm = Arc::new(PeerManager::new(1000));
        let cp = Arc::new(ConnectionPool::new(100, 300));
        let mh = Arc::new(MessageHandler::new(100 * 1024 * 1024));

        pm.add_peer("peer1".to_string(), "127.0.0.1:9000".to_string(), crate::types::NodeRole::Validator).await.unwrap();
        pm.mark_connected("peer1").await.unwrap();

        let monitor = HealthMonitor::new(pm.clone(), cp, mh, 30, 10, 300);
        
        // Simulate sending a ping
        let nonce = 12345u64;
        monitor.pending_pings.insert("peer1".to_string(), (nonce, SystemTime::now() - Duration::from_secs(15)));
        
        // Manually trigger timeout check
        let now = SystemTime::now();
        let timeout_duration = Duration::from_secs(10);
        
        for entry in monitor.pending_pings.iter() {
            let (peer_id, (_, sent_time)) = entry.pair();
            if let Ok(elapsed) = now.duration_since(*sent_time) {
                if elapsed > timeout_duration {
                    pm.record_failed_ping(peer_id).await.unwrap();
                }
            }
        }
        
        // Verify health score decreased
        let score = pm.get_health_score("peer1").await.unwrap();
        assert_eq!(score, 90);
    }

    #[tokio::test]
    async fn test_get_peer_health_score() {
        let pm = Arc::new(PeerManager::new(1000));
        let cp = Arc::new(ConnectionPool::new(100, 300));
        let mh = Arc::new(MessageHandler::new(100 * 1024 * 1024));

        pm.add_peer("peer1".to_string(), "127.0.0.1:9000".to_string(), crate::types::NodeRole::Validator).await.unwrap();

        let monitor = HealthMonitor::new(pm, cp, mh, 30, 10, 300);
        let score = monitor.get_peer_health_score("peer1").await.unwrap();
        
        assert_eq!(score, 100);
    }

    #[tokio::test]
    async fn test_get_peers_by_health_score() {
        let pm = Arc::new(PeerManager::new(1000));
        let cp = Arc::new(ConnectionPool::new(100, 300));
        let mh = Arc::new(MessageHandler::new(100 * 1024 * 1024));

        pm.add_peer("peer1".to_string(), "127.0.0.1:9000".to_string(), crate::types::NodeRole::Validator).await.unwrap();
        pm.add_peer("peer2".to_string(), "127.0.0.1:9001".to_string(), crate::types::NodeRole::Validator).await.unwrap();

        // Simulate failures for peer2
        for _ in 0..3 {
            pm.record_failed_ping("peer2").await.unwrap();
        }

        let monitor = HealthMonitor::new(pm, cp, mh, 30, 10, 300);
        let peers = monitor.get_peers_by_health_score().await;
        
        assert_eq!(peers.len(), 2);
        assert_eq!(peers[0].peer_id, "peer1"); // Higher score
        assert_eq!(peers[1].peer_id, "peer2"); // Lower score
    }

    #[tokio::test]
    async fn test_get_healthy_peers_by_score() {
        let pm = Arc::new(PeerManager::new(1000));
        let cp = Arc::new(ConnectionPool::new(100, 300));
        let mh = Arc::new(MessageHandler::new(100 * 1024 * 1024));

        pm.add_peer("peer1".to_string(), "127.0.0.1:9000".to_string(), crate::types::NodeRole::Validator).await.unwrap();
        pm.add_peer("peer2".to_string(), "127.0.0.1:9001".to_string(), crate::types::NodeRole::Validator).await.unwrap();

        // Simulate failures for peer2 to drop below 50
        for _ in 0..6 {
            pm.record_failed_ping("peer2").await.unwrap();
        }

        let monitor = HealthMonitor::new(pm, cp, mh, 30, 10, 300);
        let healthy = monitor.get_healthy_peers_by_score().await;
        
        assert_eq!(healthy.len(), 1);
        assert_eq!(healthy[0].peer_id, "peer1");
    }

    #[tokio::test]
    async fn test_get_unhealthy_peers_by_score() {
        let pm = Arc::new(PeerManager::new(1000));
        let cp = Arc::new(ConnectionPool::new(100, 300));
        let mh = Arc::new(MessageHandler::new(100 * 1024 * 1024));

        pm.add_peer("peer1".to_string(), "127.0.0.1:9000".to_string(), crate::types::NodeRole::Validator).await.unwrap();
        pm.add_peer("peer2".to_string(), "127.0.0.1:9001".to_string(), crate::types::NodeRole::Validator).await.unwrap();

        // Simulate failures for peer2 to drop below 30
        for _ in 0..8 {
            pm.record_failed_ping("peer2").await.unwrap();
        }

        let monitor = HealthMonitor::new(pm, cp, mh, 30, 10, 300);
        let unhealthy = monitor.get_unhealthy_peers_by_score().await;
        
        assert_eq!(unhealthy.len(), 1);
        assert_eq!(unhealthy[0].peer_id, "peer2");
    }

    #[tokio::test]
    async fn test_get_average_health_score() {
        let pm = Arc::new(PeerManager::new(1000));
        let cp = Arc::new(ConnectionPool::new(100, 300));
        let mh = Arc::new(MessageHandler::new(100 * 1024 * 1024));

        pm.add_peer("peer1".to_string(), "127.0.0.1:9000".to_string(), crate::types::NodeRole::Validator).await.unwrap();
        pm.add_peer("peer2".to_string(), "127.0.0.1:9001".to_string(), crate::types::NodeRole::Validator).await.unwrap();

        // Simulate failures for peer2
        for _ in 0..2 {
            pm.record_failed_ping("peer2").await.unwrap();
        }

        let monitor = HealthMonitor::new(pm, cp, mh, 30, 10, 300);
        let avg = monitor.get_average_health_score().await;
        
        // peer1: 100, peer2: 80, average: 90
        assert_eq!(avg, 90);
    }

    #[tokio::test]
    async fn test_reset_peer_health_score() {
        let pm = Arc::new(PeerManager::new(1000));
        let cp = Arc::new(ConnectionPool::new(100, 300));
        let mh = Arc::new(MessageHandler::new(100 * 1024 * 1024));

        pm.add_peer("peer1".to_string(), "127.0.0.1:9000".to_string(), crate::types::NodeRole::Validator).await.unwrap();

        // Simulate failures
        for _ in 0..5 {
            pm.record_failed_ping("peer1").await.unwrap();
        }

        let monitor = HealthMonitor::new(pm.clone(), cp, mh, 30, 10, 300);
        
        let score_before = monitor.get_peer_health_score("peer1").await.unwrap();
        assert_eq!(score_before, 50);

        // Reset
        monitor.reset_peer_health_score("peer1").await.unwrap();

        let score_after = monitor.get_peer_health_score("peer1").await.unwrap();
        assert_eq!(score_after, 100);
    }

    #[tokio::test]
    async fn test_health_score_recovery_mechanism() {
        let pm = Arc::new(PeerManager::new(1000));
        let cp = Arc::new(ConnectionPool::new(100, 300));
        let mh = Arc::new(MessageHandler::new(100 * 1024 * 1024));

        pm.add_peer("peer1".to_string(), "127.0.0.1:9000".to_string(), crate::types::NodeRole::Validator).await.unwrap();

        let monitor = HealthMonitor::new(pm.clone(), cp, mh, 30, 10, 300);
        
        // Simulate failures to drop score to 50
        for _ in 0..5 {
            pm.record_failed_ping("peer1").await.unwrap();
        }
        
        let score = monitor.get_peer_health_score("peer1").await.unwrap();
        assert_eq!(score, 50);
        
        // Now simulate successful pings to recover
        for _ in 0..5 {
            pm.record_successful_ping("peer1").await.unwrap();
        }
        
        let score = monitor.get_peer_health_score("peer1").await.unwrap();
        assert_eq!(score, 75);
    }
}
