//! Unicast message sending to specific peers with timeout and health tracking

use crate::connection_pool::ConnectionPool;
use crate::error::{P2PError, Result};
use crate::message_handler::MessageHandler;
use crate::peer_manager::PeerManager;
use crate::types::NetworkMessage;
use std::sync::Arc;
use tokio::time::{timeout, Duration};
use tracing::{debug, error, warn};

/// Handles unicast message sending to specific peers
pub struct UnicastManager {
    /// Peer manager for accessing peer state
    peer_manager: Arc<PeerManager>,
    /// Connection pool for accessing connections
    connection_pool: Arc<ConnectionPool>,
    /// Message handler for serialization
    message_handler: Arc<MessageHandler>,
    /// Unicast delivery timeout in seconds
    delivery_timeout_secs: u64,
    /// Local peer ID for message headers
    local_peer_id: String,
}

impl UnicastManager {
    /// Create new unicast manager
    pub fn new(
        peer_manager: Arc<PeerManager>,
        connection_pool: Arc<ConnectionPool>,
        message_handler: Arc<MessageHandler>,
        delivery_timeout_secs: u64,
        local_peer_id: String,
    ) -> Self {
        Self {
            peer_manager,
            connection_pool,
            message_handler,
            delivery_timeout_secs,
            local_peer_id,
        }
    }

    /// Send message to a specific peer with timeout and health tracking
    pub async fn send_to_peer(&self, peer_id: &str, msg: NetworkMessage) -> Result<()> {
        // Verify peer exists
        let peer = self.peer_manager.get_peer(peer_id).await?;

        // Check if peer is connected
        if !peer.is_connected {
            return Err(P2PError::PeerNotFound(format!(
                "Peer {} is not connected",
                peer_id
            )));
        }

        // Check if peer is healthy
        if !peer.is_healthy {
            return Err(P2PError::PeerUnhealthy(format!(
                "Peer {} is marked as unhealthy",
                peer_id
            )));
        }

        debug!(
            "Sending {} message to peer: {} ({})",
            msg.message_type(),
            peer_id,
            peer.address
        );

        // Serialize message with sender peer ID in header
        let serialized_msg = self.message_handler.serialize(&msg).await?;
        let msg_size = serialized_msg.len();

        // Create message with sender header
        let mut message_with_header = Vec::with_capacity(msg_size + self.local_peer_id.len() + 4);

        // Add sender peer ID length (4 bytes, little-endian)
        message_with_header.extend_from_slice(&(self.local_peer_id.len() as u32).to_le_bytes());

        // Add sender peer ID
        message_with_header.extend_from_slice(self.local_peer_id.as_bytes());

        // Add serialized message
        message_with_header.extend_from_slice(&serialized_msg);

        // Verify connection exists
        match self.connection_pool.get_connection(peer_id).await {
            Ok(_) => {
                // Connection exists, proceed with sending
            }
            Err(e) => {
                warn!("Failed to get connection for peer {}: {}", peer_id, e);
                // Mark peer as unhealthy on connection failure
                let _ = self
                    .peer_manager
                    .mark_unhealthy(peer_id, format!("Connection unavailable: {}", e))
                    .await;
                return Err(e);
            }
        };

        // Send message with timeout
        let send_result = timeout(
            Duration::from_secs(self.delivery_timeout_secs),
            self.send_message_internal(&message_with_header, peer_id),
        )
        .await;

        match send_result {
            Ok(Ok(())) => {
                // Record metrics on successful send
                let _ = self
                    .connection_pool
                    .record_sent(peer_id, message_with_header.len() as u64)
                    .await;

                // Update peer metrics
                let _ = self
                    .peer_manager
                    .update_peer_metrics(peer_id, 0, message_with_header.len() as u64, 0)
                    .await;

                debug!(
                    "Successfully sent {} bytes to peer: {} (message type: {})",
                    message_with_header.len(),
                    peer_id,
                    msg.message_type()
                );

                Ok(())
            }
            Ok(Err(e)) => {
                error!("Failed to send message to peer {}: {}", peer_id, e);
                // Mark peer as unhealthy on send failure
                let _ = self
                    .peer_manager
                    .mark_unhealthy(peer_id, format!("Send failed: {}", e))
                    .await;
                Err(e)
            }
            Err(_) => {
                error!(
                    "Send timeout for peer {} (timeout: {}s)",
                    peer_id, self.delivery_timeout_secs
                );
                // Mark peer as unhealthy on timeout
                let _ = self
                    .peer_manager
                    .mark_unhealthy(
                        peer_id,
                        format!("Send timeout after {}s", self.delivery_timeout_secs),
                    )
                    .await;
                Err(P2PError::ConnectionTimeout(format!(
                    "Send timeout for peer: {}",
                    peer_id
                )))
            }
        }
    }

    /// Internal function to send message to peer
    async fn send_message_internal(&self, message_data: &[u8], peer_id: &str) -> Result<()> {
        // Write message to peer connection through connection pool
        self.connection_pool
            .write_message(peer_id, message_data)
            .await?;

        debug!("Sent {} bytes to peer: {}", message_data.len(), peer_id);

        Ok(())
    }

    /// Send message to multiple specific peers
    pub async fn send_to_peers(
        &self,
        peer_ids: Vec<String>,
        msg: NetworkMessage,
    ) -> Result<UnicastBatchResult> {
        let mut successful_sends = 0;
        let mut failed_sends = 0;
        let mut failed_peers = Vec::new();
        let mut successful_peers = Vec::new();

        for peer_id in peer_ids {
            match self.send_to_peer(&peer_id, msg.clone()).await {
                Ok(()) => {
                    successful_sends += 1;
                    successful_peers.push(peer_id);
                }
                Err(e) => {
                    failed_sends += 1;
                    failed_peers.push((peer_id, e.to_string()));
                }
            }
        }

        Ok(UnicastBatchResult {
            successful_sends,
            failed_sends,
            failed_peers,
            successful_peers,
        })
    }

    /// Get unicast statistics
    pub async fn get_unicast_stats(&self) -> UnicastStats {
        let peers = self.peer_manager.get_all_peers().await;
        let connected_count = peers.iter().filter(|p| p.is_connected).count();
        let healthy_count = peers.iter().filter(|p| p.is_healthy).count();
        let total_messages_sent = peers.iter().map(|p| p.messages_sent).sum::<u64>();
        let total_bytes_sent = peers.iter().map(|p| p.bytes_sent).sum::<u64>();

        UnicastStats {
            connected_peers: connected_count,
            healthy_peers: healthy_count,
            total_messages_sent,
            total_bytes_sent,
            total_peers: peers.len(),
        }
    }
}

/// Result of batch unicast operation
#[derive(Clone, Debug)]
pub struct UnicastBatchResult {
    /// Number of successful sends
    pub successful_sends: usize,
    /// Number of failed sends
    pub failed_sends: usize,
    /// List of failed peers with error messages
    pub failed_peers: Vec<(String, String)>,
    /// List of successful peers
    pub successful_peers: Vec<String>,
}

impl UnicastBatchResult {
    /// Check if all sends were successful
    pub fn is_successful(&self) -> bool {
        self.failed_sends == 0
    }

    /// Get success rate as percentage
    pub fn success_rate(&self) -> f64 {
        let total = self.successful_sends + self.failed_sends;
        if total == 0 {
            100.0
        } else {
            (self.successful_sends as f64 / total as f64) * 100.0
        }
    }
}

/// Unicast statistics
#[derive(Clone, Debug)]
pub struct UnicastStats {
    /// Number of connected peers
    pub connected_peers: usize,
    /// Number of healthy peers
    pub healthy_peers: usize,
    /// Total messages sent via unicast
    pub total_messages_sent: u64,
    /// Total bytes sent via unicast
    pub total_bytes_sent: u64,
    /// Total known peers
    pub total_peers: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::NodeRole;

    #[tokio::test]
    async fn test_unicast_manager_creation() {
        let peer_manager = Arc::new(PeerManager::new(1000));
        let connection_pool = Arc::new(ConnectionPool::new(100, 300));
        let message_handler = Arc::new(MessageHandler::new(100 * 1024 * 1024));

        let unicast_mgr = UnicastManager::new(
            peer_manager,
            connection_pool,
            message_handler,
            5,
            "local_peer".to_string(),
        );

        assert_eq!(unicast_mgr.delivery_timeout_secs, 5);
        assert_eq!(unicast_mgr.local_peer_id, "local_peer");
    }

    #[tokio::test]
    async fn test_unicast_batch_result_success_rate() {
        let result = UnicastBatchResult {
            successful_sends: 8,
            failed_sends: 2,
            failed_peers: vec![
                ("peer1".to_string(), "timeout".to_string()),
                ("peer2".to_string(), "disconnected".to_string()),
            ],
            successful_peers: vec![
                "peer3".to_string(),
                "peer4".to_string(),
                "peer5".to_string(),
                "peer6".to_string(),
                "peer7".to_string(),
                "peer8".to_string(),
                "peer9".to_string(),
                "peer10".to_string(),
            ],
        };

        assert_eq!(result.success_rate(), 80.0);
        assert!(!result.is_successful());
    }

    #[tokio::test]
    async fn test_unicast_batch_result_perfect_success() {
        let result = UnicastBatchResult {
            successful_sends: 5,
            failed_sends: 0,
            failed_peers: Vec::new(),
            successful_peers: vec![
                "peer1".to_string(),
                "peer2".to_string(),
                "peer3".to_string(),
                "peer4".to_string(),
                "peer5".to_string(),
            ],
        };

        assert_eq!(result.success_rate(), 100.0);
        assert!(result.is_successful());
    }

    #[tokio::test]
    async fn test_unicast_batch_result_zero_sends() {
        let result = UnicastBatchResult {
            successful_sends: 0,
            failed_sends: 0,
            failed_peers: Vec::new(),
            successful_peers: Vec::new(),
        };

        assert_eq!(result.success_rate(), 100.0);
        assert!(result.is_successful());
    }

    #[tokio::test]
    async fn test_unicast_stats() {
        let peer_manager = Arc::new(PeerManager::new(1000));
        let connection_pool = Arc::new(ConnectionPool::new(100, 300));
        let message_handler = Arc::new(MessageHandler::new(100 * 1024 * 1024));

        let unicast_mgr = UnicastManager::new(
            peer_manager.clone(),
            connection_pool,
            message_handler,
            5,
            "local_peer".to_string(),
        );

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

        let stats = unicast_mgr.get_unicast_stats().await;
        assert_eq!(stats.connected_peers, 1);
        assert_eq!(stats.healthy_peers, 1);
        assert_eq!(stats.total_peers, 1);
    }

    #[tokio::test]
    async fn test_send_to_peer_disconnected() {
        let peer_manager = Arc::new(PeerManager::new(1000));
        let connection_pool = Arc::new(ConnectionPool::new(100, 300));
        let message_handler = Arc::new(MessageHandler::new(100 * 1024 * 1024));

        let unicast_mgr = UnicastManager::new(
            peer_manager.clone(),
            connection_pool,
            message_handler,
            5,
            "local_peer".to_string(),
        );

        // Add peer but don't mark as connected
        peer_manager
            .add_peer(
                "peer1".to_string(),
                "127.0.0.1:9000".to_string(),
                NodeRole::Validator,
            )
            .await
            .unwrap();

        let msg = NetworkMessage::Ping { nonce: 123 };
        let result = unicast_mgr.send_to_peer("peer1", msg).await;

        assert!(result.is_err());
        match result {
            Err(P2PError::PeerNotFound(_)) => {
                // Expected error for disconnected peer
            }
            other => {
                panic!("Expected PeerNotFound error, got: {:?}", other);
            }
        }
    }

    #[tokio::test]
    async fn test_send_to_peer_unhealthy() {
        let peer_manager = Arc::new(PeerManager::new(1000));
        let connection_pool = Arc::new(ConnectionPool::new(100, 300));
        let message_handler = Arc::new(MessageHandler::new(100 * 1024 * 1024));

        let unicast_mgr = UnicastManager::new(
            peer_manager.clone(),
            connection_pool,
            message_handler,
            5,
            "local_peer".to_string(),
        );

        // Add peer and mark as connected but unhealthy
        peer_manager
            .add_peer(
                "peer1".to_string(),
                "127.0.0.1:9000".to_string(),
                NodeRole::Validator,
            )
            .await
            .unwrap();

        peer_manager.mark_connected("peer1").await.unwrap();
        peer_manager
            .mark_unhealthy("peer1", "test unhealthy".to_string())
            .await
            .unwrap();

        let msg = NetworkMessage::Ping { nonce: 123 };
        let result = unicast_mgr.send_to_peer("peer1", msg).await;

        assert!(result.is_err());
        match result {
            Err(P2PError::PeerUnhealthy(_)) => {
                // Expected error for unhealthy peer
            }
            other => {
                panic!("Expected PeerUnhealthy error, got: {:?}", other);
            }
        }
    }

    #[tokio::test]
    async fn test_send_to_peers_batch() {
        let peer_manager = Arc::new(PeerManager::new(1000));
        let connection_pool = Arc::new(ConnectionPool::new(100, 300));
        let message_handler = Arc::new(MessageHandler::new(100 * 1024 * 1024));

        let unicast_mgr = UnicastManager::new(
            peer_manager.clone(),
            connection_pool,
            message_handler,
            5,
            "local_peer".to_string(),
        );

        // Add peers
        for i in 1..=3 {
            let peer_id = format!("peer{}", i);
            peer_manager
                .add_peer(
                    peer_id.clone(),
                    format!("127.0.0.1:{}", 9000 + i),
                    NodeRole::Validator,
                )
                .await
                .unwrap();
        }

        let peer_ids = vec![
            "peer1".to_string(),
            "peer2".to_string(),
            "peer3".to_string(),
        ];

        let msg = NetworkMessage::Ping { nonce: 123 };
        let result = unicast_mgr.send_to_peers(peer_ids, msg).await.unwrap();

        // All should fail because peers are not connected
        assert_eq!(result.successful_sends, 0);
        assert_eq!(result.failed_sends, 3);
    }
}
