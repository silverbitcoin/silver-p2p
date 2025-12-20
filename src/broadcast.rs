//! Message broadcasting to multiple peers with parallel sending and failure handling

use crate::connection_pool::ConnectionPool;
use crate::error::{P2PError, Result};
use crate::message_handler::MessageHandler;
use crate::peer_manager::PeerManager;
use crate::types::NetworkMessage;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::task::JoinSet;
use tokio::time::{timeout, Duration};
use tokio::io::AsyncWriteExt;
use tracing::{debug, warn, info};

/// Broadcast result with detailed information
#[derive(Clone, Debug)]
pub struct BroadcastResult {
    /// Total peers targeted
    pub total_peers: usize,
    /// Number of successful sends
    pub successful_sends: usize,
    /// Number of failed sends
    pub failed_sends: usize,
    /// Peer IDs that failed
    pub failed_peers: Vec<String>,
    /// Peer IDs that succeeded
    pub successful_peers: Vec<String>,
}

impl BroadcastResult {
    /// Check if broadcast was successful
    pub fn is_successful(&self) -> bool {
        self.failed_sends == 0
    }

    /// Get success rate as percentage
    pub fn success_rate(&self) -> f64 {
        if self.total_peers == 0 {
            100.0
        } else {
            (self.successful_sends as f64 / self.total_peers as f64) * 100.0
        }
    }
}

/// Handles message broadcasting to multiple peers
pub struct BroadcastManager {
    /// Peer manager for accessing peer list
    peer_manager: Arc<PeerManager>,
    /// Connection pool for accessing connections
    connection_pool: Arc<ConnectionPool>,
    /// Message handler for serialization
    message_handler: Arc<MessageHandler>,
    /// Broadcast timeout in seconds
    broadcast_timeout_secs: u64,
    /// Maximum concurrent broadcasts
    max_concurrent_broadcasts: usize,
}

impl BroadcastManager {
    /// Create new broadcast manager
    pub fn new(
        peer_manager: Arc<PeerManager>,
        connection_pool: Arc<ConnectionPool>,
        message_handler: Arc<MessageHandler>,
        broadcast_timeout_secs: u64,
        max_concurrent_broadcasts: usize,
    ) -> Self {
        Self {
            peer_manager,
            connection_pool,
            message_handler,
            broadcast_timeout_secs,
            max_concurrent_broadcasts,
        }
    }

    /// Broadcast message to all connected peers
    pub async fn broadcast(&self, msg: NetworkMessage) -> Result<BroadcastResult> {
        // Get all connected peers
        let peers = self.peer_manager.get_all_peers().await;
        let connected_peers: Vec<_> = peers
            .iter()
            .filter(|p| p.is_connected && p.is_healthy)
            .collect();

        let total_peers_count = connected_peers.len();

        debug!(
            "Broadcasting message to {} connected peers",
            total_peers_count
        );

        // Handle zero connected peers case
        if connected_peers.is_empty() {
            info!("No connected peers available for broadcast");
            return Ok(BroadcastResult {
                total_peers: 0,
                successful_sends: 0,
                failed_sends: 0,
                failed_peers: Vec::new(),
                successful_peers: Vec::new(),
            });
        }

        // Serialize message once for all peers
        let serialized_msg = self.message_handler.serialize(&msg).await?;
        let msg_size = serialized_msg.len();

        debug!(
            "Serialized broadcast message: {} bytes, type: {}",
            msg_size,
            msg.message_type()
        );

        // Create tasks for parallel sending
        let mut join_set = JoinSet::new();
        let successful_sends = Arc::new(AtomicUsize::new(0));
        let failed_sends = Arc::new(AtomicUsize::new(0));
        let successful_peers = Arc::new(tokio::sync::Mutex::new(Vec::new()));
        let failed_peers = Arc::new(tokio::sync::Mutex::new(Vec::new()));

        // Spawn send tasks with concurrency limit
        for peer in &connected_peers {
            if join_set.len() >= self.max_concurrent_broadcasts {
                // Wait for at least one task to complete before spawning more
                let _ = join_set.join_next().await;
            }

            let peer_id = peer.peer_id.clone();
            let peer_addr = peer.address.clone();
            let connection_pool = self.connection_pool.clone();
            let msg_data = serialized_msg.clone();
            let timeout_secs = self.broadcast_timeout_secs;
            let successful_sends = successful_sends.clone();
            let failed_sends = failed_sends.clone();
            let successful_peers = successful_peers.clone();
            let failed_peers = failed_peers.clone();

            join_set.spawn(async move {
                match Self::send_to_peer_internal(
                    &connection_pool,
                    &peer_id,
                    &peer_addr,
                    &msg_data,
                    timeout_secs,
                )
                .await
                {
                    Ok(_) => {
                        successful_sends.fetch_add(1, Ordering::SeqCst);
                        successful_peers.lock().await.push(peer_id.clone());
                        debug!("Successfully sent broadcast to peer: {}", peer_id);
                    }
                    Err(e) => {
                        failed_sends.fetch_add(1, Ordering::SeqCst);
                        failed_peers.lock().await.push(peer_id.clone());
                        warn!("Failed to send broadcast to peer {}: {}", peer_id, e);
                    }
                }
            });
        }

        // Wait for all tasks to complete
        while let Some(_) = join_set.join_next().await {}

        let successful = successful_sends.load(Ordering::SeqCst);
        let failed = failed_sends.load(Ordering::SeqCst);
        let successful_list = successful_peers.lock().await.clone();
        let failed_list = failed_peers.lock().await.clone();

        let result = BroadcastResult {
            total_peers: total_peers_count,
            successful_sends: successful,
            failed_sends: failed,
            failed_peers: failed_list,
            successful_peers: successful_list,
        };

        info!(
            "Broadcast complete: {} successful, {} failed out of {} peers ({}% success rate)",
            result.successful_sends,
            result.failed_sends,
            result.total_peers,
            result.success_rate() as u32
        );

        Ok(result)
    }

    /// Broadcast to specific peer list
    pub async fn broadcast_to_peers(
        &self,
        msg: NetworkMessage,
        peer_ids: Vec<String>,
    ) -> Result<BroadcastResult> {
        let total_peers_count = peer_ids.len();

        if peer_ids.is_empty() {
            return Ok(BroadcastResult {
                total_peers: 0,
                successful_sends: 0,
                failed_sends: 0,
                failed_peers: Vec::new(),
                successful_peers: Vec::new(),
            });
        }

        // Serialize message once
        let serialized_msg = self.message_handler.serialize(&msg).await?;

        debug!(
            "Broadcasting message to {} specific peers",
            total_peers_count
        );

        let mut join_set = JoinSet::new();
        let successful_sends = Arc::new(AtomicUsize::new(0));
        let failed_sends = Arc::new(AtomicUsize::new(0));
        let successful_peers = Arc::new(tokio::sync::Mutex::new(Vec::new()));
        let failed_peers = Arc::new(tokio::sync::Mutex::new(Vec::new()));

        for peer_id in peer_ids.iter() {
            if join_set.len() >= self.max_concurrent_broadcasts {
                let _ = join_set.join_next().await;
            }

            let peer_id_clone = peer_id.clone();
            let peer_manager = self.peer_manager.clone();
            let connection_pool = self.connection_pool.clone();
            let msg_data = serialized_msg.clone();
            let timeout_secs = self.broadcast_timeout_secs;
            let successful_sends = successful_sends.clone();
            let failed_sends = failed_sends.clone();
            let successful_peers = successful_peers.clone();
            let failed_peers = failed_peers.clone();

            join_set.spawn(async move {
                // Get peer info
                match peer_manager.get_peer(&peer_id_clone).await {
                    Ok(peer) => {
                        match Self::send_to_peer_internal(
                            &connection_pool,
                            &peer_id_clone,
                            &peer.address,
                            &msg_data,
                            timeout_secs,
                        )
                        .await
                        {
                            Ok(_) => {
                                successful_sends.fetch_add(1, Ordering::SeqCst);
                                successful_peers.lock().await.push(peer_id_clone.clone());
                                debug!("Successfully sent broadcast to peer: {}", peer_id_clone);
                            }
                            Err(e) => {
                                failed_sends.fetch_add(1, Ordering::SeqCst);
                                failed_peers.lock().await.push(peer_id_clone.clone());
                                warn!("Failed to send broadcast to peer {}: {}", peer_id_clone, e);
                            }
                        }
                    }
                    Err(e) => {
                        failed_sends.fetch_add(1, Ordering::SeqCst);
                        failed_peers.lock().await.push(peer_id_clone.clone());
                        warn!("Peer not found: {}: {}", peer_id_clone, e);
                    }
                }
            });
        }

        while let Some(_) = join_set.join_next().await {}

        let successful = successful_sends.load(Ordering::SeqCst);
        let failed = failed_sends.load(Ordering::SeqCst);
        let successful_list = successful_peers.lock().await.clone();
        let failed_list = failed_peers.lock().await.clone();

        let result = BroadcastResult {
            total_peers: total_peers_count,
            successful_sends: successful,
            failed_sends: failed,
            failed_peers: failed_list,
            successful_peers: successful_list,
        };

        info!(
            "Targeted broadcast complete: {} successful, {} failed out of {} peers",
            result.successful_sends, result.failed_sends, result.total_peers
        );

        Ok(result)
    }

    /// Internal function to send message to a single peer
    async fn send_to_peer_internal(
        connection_pool: &ConnectionPool,
        peer_id: &str,
        peer_addr: &str,
        msg_data: &[u8],
        timeout_secs: u64,
    ) -> Result<()> {
        // Get connection
        let stream = connection_pool
            .get_connection(peer_id)
            .await
            .map_err(|_| P2PError::PeerNotFound(peer_id.to_string()))?;

        // Send with timeout
        let send_future = async {
            // Write to the stream using proper async write
            // The stream is behind an Arc<Mutex<TcpStream>> for thread-safe access
            
            let mut stream_guard = stream.lock().await;
            
            // Write the message data to the stream
            stream_guard.write_all(msg_data).await?;
            
            // Flush to ensure data is sent immediately
            stream_guard.flush().await?;
            
            Ok::<(), P2PError>(())
        };

        match timeout(Duration::from_secs(timeout_secs), send_future).await {
            Ok(Ok(())) => {
                // Record metrics
                let _ = connection_pool.record_sent(peer_id, msg_data.len() as u64).await;
                debug!("Successfully sent {} bytes to peer: {}", msg_data.len(), peer_id);
                Ok(())
            }
            Ok(Err(e)) => Err(e),
            Err(_) => Err(P2PError::ConnectionTimeout(peer_addr.to_string())),
        }
    }

    /// Get broadcast statistics
    pub async fn get_broadcast_stats(&self) -> BroadcastStats {
        let peers = self.peer_manager.get_all_peers().await;
        let connected_count = peers.iter().filter(|p| p.is_connected).count();
        let healthy_count = peers.iter().filter(|p| p.is_healthy).count();
        let total_messages_sent = peers.iter().map(|p| p.messages_sent).sum::<u64>();

        BroadcastStats {
            connected_peers: connected_count,
            healthy_peers: healthy_count,
            total_messages_sent,
            total_peers: peers.len(),
        }
    }
}

/// Broadcast statistics
#[derive(Clone, Debug)]
pub struct BroadcastStats {
    /// Number of connected peers
    pub connected_peers: usize,
    /// Number of healthy peers
    pub healthy_peers: usize,
    /// Total messages sent via broadcast
    pub total_messages_sent: u64,
    /// Total known peers
    pub total_peers: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::NodeRole;

    #[tokio::test]
    async fn test_broadcast_result_success_rate() {
        let result = BroadcastResult {
            total_peers: 10,
            successful_sends: 8,
            failed_sends: 2,
            failed_peers: vec!["peer1".to_string(), "peer2".to_string()],
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
    async fn test_broadcast_result_perfect_success() {
        let result = BroadcastResult {
            total_peers: 5,
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
    async fn test_broadcast_result_zero_peers() {
        let result = BroadcastResult {
            total_peers: 0,
            successful_sends: 0,
            failed_sends: 0,
            failed_peers: Vec::new(),
            successful_peers: Vec::new(),
        };

        assert_eq!(result.success_rate(), 100.0);
        assert!(result.is_successful());
    }

    #[tokio::test]
    async fn test_broadcast_manager_creation() {
        let peer_manager = Arc::new(PeerManager::new(1000));
        let connection_pool = Arc::new(ConnectionPool::new(100, 300));
        let message_handler = Arc::new(MessageHandler::new(100 * 1024 * 1024));

        let broadcast_mgr = BroadcastManager::new(
            peer_manager,
            connection_pool,
            message_handler,
            5,
            10,
        );

        assert_eq!(broadcast_mgr.broadcast_timeout_secs, 5);
        assert_eq!(broadcast_mgr.max_concurrent_broadcasts, 10);
    }

    #[tokio::test]
    async fn test_broadcast_no_peers() {
        let peer_manager = Arc::new(PeerManager::new(1000));
        let connection_pool = Arc::new(ConnectionPool::new(100, 300));
        let message_handler = Arc::new(MessageHandler::new(100 * 1024 * 1024));

        let broadcast_mgr = BroadcastManager::new(
            peer_manager,
            connection_pool,
            message_handler,
            5,
            10,
        );

        let msg = NetworkMessage::Ping { nonce: 123 };
        let result = broadcast_mgr.broadcast(msg).await.unwrap();

        assert_eq!(result.total_peers, 0);
        assert_eq!(result.successful_sends, 0);
        assert_eq!(result.failed_sends, 0);
    }

    #[tokio::test]
    async fn test_broadcast_to_peers_empty_list() {
        let peer_manager = Arc::new(PeerManager::new(1000));
        let connection_pool = Arc::new(ConnectionPool::new(100, 300));
        let message_handler = Arc::new(MessageHandler::new(100 * 1024 * 1024));

        let broadcast_mgr = BroadcastManager::new(
            peer_manager,
            connection_pool,
            message_handler,
            5,
            10,
        );

        let msg = NetworkMessage::Ping { nonce: 123 };
        let result = broadcast_mgr.broadcast_to_peers(msg, Vec::new()).await.unwrap();

        assert_eq!(result.total_peers, 0);
        assert_eq!(result.successful_sends, 0);
    }

    #[tokio::test]
    async fn test_broadcast_stats() {
        let peer_manager = Arc::new(PeerManager::new(1000));
        let connection_pool = Arc::new(ConnectionPool::new(100, 300));
        let message_handler = Arc::new(MessageHandler::new(100 * 1024 * 1024));

        let broadcast_mgr = BroadcastManager::new(
            peer_manager.clone(),
            connection_pool,
            message_handler,
            5,
            10,
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

        peer_manager
            .mark_connected("peer1")
            .await
            .unwrap();

        let stats = broadcast_mgr.get_broadcast_stats().await;
        assert_eq!(stats.connected_peers, 1);
        assert_eq!(stats.healthy_peers, 1);
        assert_eq!(stats.total_peers, 1);
    }
}
