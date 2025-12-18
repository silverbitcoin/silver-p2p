//! Message error handling and malformed data detection

use crate::error::{P2PError, Result};
use crate::types::NetworkMessage;
use crate::peer_manager::PeerManager;
use crate::connection_pool::ConnectionPool;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tracing::{error, warn, debug};

/// Message error statistics
pub struct MessageErrorStats {
    /// Total parsing errors
    pub parsing_errors: AtomicU64,
    /// Total malformed messages
    pub malformed_messages: AtomicU64,
    /// Total deserialization errors
    pub deserialization_errors: AtomicU64,
    /// Total format validation errors
    pub format_errors: AtomicU64,
    /// Peers disconnected due to malformed data
    pub peers_disconnected: AtomicU64,
}

/// Handles message parsing errors and malformed data detection
pub struct MessageErrorHandler {
    /// Peer manager for marking peers as unhealthy
    peer_manager: Arc<PeerManager>,
    /// Connection pool for disconnecting peers
    connection_pool: Arc<ConnectionPool>,
    /// Error statistics
    stats: Arc<MessageErrorStats>,
}

impl MessageErrorHandler {
    /// Create new message error handler
    pub fn new(
        peer_manager: Arc<PeerManager>,
        connection_pool: Arc<ConnectionPool>,
    ) -> Self {
        Self {
            peer_manager,
            connection_pool,
            stats: Arc::new(MessageErrorStats {
                parsing_errors: AtomicU64::new(0),
                malformed_messages: AtomicU64::new(0),
                deserialization_errors: AtomicU64::new(0),
                format_errors: AtomicU64::new(0),
                peers_disconnected: AtomicU64::new(0),
            }),
        }
    }

    /// Handle message parsing error
    /// Logs error, marks peer as unhealthy, and disconnects peer
    pub async fn handle_parsing_error(
        &self,
        peer_id: &str,
        error: &P2PError,
    ) -> Result<()> {
        self.stats.parsing_errors.fetch_add(1, Ordering::SeqCst);

        error!(
            "Message parsing error from peer {}: {}",
            peer_id, error
        );

        // Mark peer as unhealthy
        self.peer_manager
            .mark_unhealthy(peer_id, format!("Parsing error: {}", error))
            .await?;

        // Disconnect the peer
        self.disconnect_peer(peer_id).await?;

        Ok(())
    }

    /// Handle malformed message error
    /// Logs error, marks peer as unhealthy, and disconnects peer
    pub async fn handle_malformed_message(
        &self,
        peer_id: &str,
        reason: &str,
    ) -> Result<()> {
        self.stats.malformed_messages.fetch_add(1, Ordering::SeqCst);

        error!(
            "Malformed message from peer {}: {}",
            peer_id, reason
        );

        // Mark peer as unhealthy
        self.peer_manager
            .mark_unhealthy(peer_id, format!("Malformed message: {}", reason))
            .await?;

        // Disconnect the peer
        self.disconnect_peer(peer_id).await?;

        Ok(())
    }

    /// Handle deserialization error
    /// Logs error, marks peer as unhealthy, and disconnects peer
    pub async fn handle_deserialization_error(
        &self,
        peer_id: &str,
        error: &P2PError,
    ) -> Result<()> {
        self.stats.deserialization_errors.fetch_add(1, Ordering::SeqCst);

        error!(
            "Deserialization error from peer {}: {}",
            peer_id, error
        );

        // Mark peer as unhealthy
        self.peer_manager
            .mark_unhealthy(peer_id, format!("Deserialization error: {}", error))
            .await?;

        // Disconnect the peer
        self.disconnect_peer(peer_id).await?;

        Ok(())
    }

    /// Handle message format validation error
    /// Logs error, marks peer as unhealthy, and disconnects peer
    pub async fn handle_format_error(
        &self,
        peer_id: &str,
        error: &P2PError,
    ) -> Result<()> {
        self.stats.format_errors.fetch_add(1, Ordering::SeqCst);

        error!(
            "Message format error from peer {}: {}",
            peer_id, error
        );

        // Mark peer as unhealthy
        self.peer_manager
            .mark_unhealthy(peer_id, format!("Format error: {}", error))
            .await?;

        // Disconnect the peer
        self.disconnect_peer(peer_id).await?;

        Ok(())
    }

    /// Validate message format without deserializing
    /// Returns Ok if format is valid, Err if format is invalid
    pub async fn validate_message_format(
        &self,
        peer_id: &str,
        data: &[u8],
        max_message_size: usize,
    ) -> Result<usize> {
        // Check minimum length for header
        if data.len() < 4 {
            let error = P2PError::InvalidMessageFormat(
                "Message too short for length prefix".to_string(),
            );
            self.handle_format_error(peer_id, &error).await?;
            return Err(error);
        }

        // Extract length prefix (little-endian)
        let len = u32::from_le_bytes([data[0], data[1], data[2], data[3]]) as usize;

        // Validate length is within bounds
        if len > max_message_size {
            let error = P2PError::MessageTooLarge(len, max_message_size);
            self.handle_format_error(peer_id, &error).await?;
            return Err(error);
        }

        // Validate we have complete message
        if data.len() < len + 4 {
            let error = P2PError::InvalidMessageFormat(
                "Incomplete message data".to_string(),
            );
            self.handle_format_error(peer_id, &error).await?;
            return Err(error);
        }

        debug!(
            "Message format validation passed for peer {}: {} bytes",
            peer_id, len
        );

        Ok(len)
    }

    /// Validate deserialized message
    /// Returns Ok if message is valid, Err if message is invalid
    pub async fn validate_message(
        &self,
        peer_id: &str,
        msg: &NetworkMessage,
    ) -> Result<()> {
        // Validate message type is not empty
        let msg_type = msg.message_type();
        if msg_type.is_empty() {
            let error = P2PError::InvalidMessageFormat(
                "Message type cannot be empty".to_string(),
            );
            self.handle_malformed_message(peer_id, "Empty message type").await?;
            return Err(error);
        }

        // Validate message size is reasonable
        let estimated_size = msg.estimated_size();
        if estimated_size == 0 {
            warn!(
                "Message from peer {} has zero estimated size: {}",
                peer_id, msg_type
            );
        }

        debug!(
            "Message validation passed for peer {}: type={}, size={}",
            peer_id, msg_type, estimated_size
        );

        Ok(())
    }

    /// Disconnect peer and clean up resources
    async fn disconnect_peer(&self, peer_id: &str) -> Result<()> {
        self.stats.peers_disconnected.fetch_add(1, Ordering::SeqCst);

        // Remove connection from connection pool
        if let Err(e) = self.connection_pool.remove_connection(peer_id).await {
            warn!("Error removing connection for peer {}: {}", peer_id, e);
        }

        // Mark as disconnected in peer manager
        if let Err(e) = self.peer_manager.mark_disconnected(peer_id).await {
            warn!("Error marking peer {} as disconnected: {}", peer_id, e);
        }

        debug!("Disconnected peer {} due to message error", peer_id);

        Ok(())
    }

    /// Get error statistics
    pub async fn get_stats(&self) -> (u64, u64, u64, u64, u64) {
        (
            self.stats.parsing_errors.load(Ordering::SeqCst),
            self.stats.malformed_messages.load(Ordering::SeqCst),
            self.stats.deserialization_errors.load(Ordering::SeqCst),
            self.stats.format_errors.load(Ordering::SeqCst),
            self.stats.peers_disconnected.load(Ordering::SeqCst),
        )
    }

    /// Reset error statistics
    pub async fn reset_stats(&self) {
        self.stats.parsing_errors.store(0, Ordering::SeqCst);
        self.stats.malformed_messages.store(0, Ordering::SeqCst);
        self.stats.deserialization_errors.store(0, Ordering::SeqCst);
        self.stats.format_errors.store(0, Ordering::SeqCst);
        self.stats.peers_disconnected.store(0, Ordering::SeqCst);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::NodeRole;

    #[tokio::test]
    async fn test_validate_message_format_too_short() {
        let peer_manager = Arc::new(PeerManager::new(1000));
        let connection_pool = Arc::new(ConnectionPool::new(100, 300));
        let handler = MessageErrorHandler::new(peer_manager.clone(), connection_pool.clone());

        // Add peer first
        peer_manager
            .add_peer("peer1".to_string(), "127.0.0.1:9000".to_string(), NodeRole::Validator)
            .await
            .unwrap();

        let data = vec![1, 2];
        let result = handler.validate_message_format("peer1", &data, 100 * 1024 * 1024).await;

        assert!(result.is_err());
        let (_parsing, _malformed, _deser, format, disconnected) = handler.get_stats().await;
        assert_eq!(format, 1);
        assert_eq!(disconnected, 1);
    }

    #[tokio::test]
    async fn test_validate_message_format_too_large() {
        let peer_manager = Arc::new(PeerManager::new(1000));
        let connection_pool = Arc::new(ConnectionPool::new(100, 300));
        let handler = MessageErrorHandler::new(peer_manager.clone(), connection_pool.clone());

        // Add peer first
        peer_manager
            .add_peer("peer1".to_string(), "127.0.0.1:9000".to_string(), NodeRole::Validator)
            .await
            .unwrap();

        // Create message with size exceeding max
        let mut data = vec![0u8; 10];
        let size = 200u32; // Exceeds max of 100
        data[0..4].copy_from_slice(&size.to_le_bytes());

        let result = handler.validate_message_format("peer1", &data, 100).await;

        assert!(result.is_err());
        let (_parsing, _malformed, _deser, format, disconnected) = handler.get_stats().await;
        assert_eq!(format, 1);
        assert_eq!(disconnected, 1);
    }

    #[tokio::test]
    async fn test_validate_message_format_incomplete() {
        let peer_manager = Arc::new(PeerManager::new(1000));
        let connection_pool = Arc::new(ConnectionPool::new(100, 300));
        let handler = MessageErrorHandler::new(peer_manager.clone(), connection_pool.clone());

        // Add peer first
        peer_manager
            .add_peer("peer1".to_string(), "127.0.0.1:9000".to_string(), NodeRole::Validator)
            .await
            .unwrap();

        // Create message with incomplete data
        let mut data = vec![0u8; 10];
        let size = 100u32; // Says 100 bytes but only have 10
        data[0..4].copy_from_slice(&size.to_le_bytes());

        let result = handler.validate_message_format("peer1", &data, 1000).await;

        assert!(result.is_err());
        let (_parsing, _malformed, _deser, format, disconnected) = handler.get_stats().await;
        assert_eq!(format, 1);
        assert_eq!(disconnected, 1);
    }

    #[tokio::test]
    async fn test_validate_message_format_valid() {
        let peer_manager = Arc::new(PeerManager::new(1000));
        let connection_pool = Arc::new(ConnectionPool::new(100, 300));
        let handler = MessageErrorHandler::new(peer_manager.clone(), connection_pool.clone());

        // Add peer first
        peer_manager
            .add_peer("peer1".to_string(), "127.0.0.1:9000".to_string(), NodeRole::Validator)
            .await
            .unwrap();

        // Create valid message
        let mut data = vec![0u8; 10];
        let size = 6u32; // 6 bytes of data
        data[0..4].copy_from_slice(&size.to_le_bytes());

        let result = handler.validate_message_format("peer1", &data, 1000).await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 6);
        let (_parsing, _malformed, _deser, format, disconnected) = handler.get_stats().await;
        assert_eq!(format, 0);
        assert_eq!(disconnected, 0);
    }

    #[tokio::test]
    async fn test_handle_parsing_error() {
        let peer_manager = Arc::new(PeerManager::new(1000));
        let connection_pool = Arc::new(ConnectionPool::new(100, 300));
        let handler = MessageErrorHandler::new(peer_manager.clone(), connection_pool.clone());

        // Add peer first
        peer_manager
            .add_peer("peer1".to_string(), "127.0.0.1:9000".to_string(), NodeRole::Validator)
            .await
            .unwrap();

        let error = P2PError::InvalidMessageFormat("test error".to_string());
        handler.handle_parsing_error("peer1", &error).await.unwrap();

        let (parsing, malformed, deser, format, disconnected) = handler.get_stats().await;
        assert_eq!(parsing, 1);
        assert_eq!(malformed, 0);
        assert_eq!(deser, 0);
        assert_eq!(format, 0);
        assert_eq!(disconnected, 1);

        // Verify peer is marked unhealthy
        let peer = peer_manager.get_peer("peer1").await.unwrap();
        assert!(!peer.is_healthy);
    }

    #[tokio::test]
    async fn test_handle_malformed_message() {
        let peer_manager = Arc::new(PeerManager::new(1000));
        let connection_pool = Arc::new(ConnectionPool::new(100, 300));
        let handler = MessageErrorHandler::new(peer_manager.clone(), connection_pool.clone());

        // Add peer first
        peer_manager
            .add_peer("peer1".to_string(), "127.0.0.1:9000".to_string(), NodeRole::Validator)
            .await
            .unwrap();

        handler
            .handle_malformed_message("peer1", "Invalid data")
            .await
            .unwrap();

        let (parsing, malformed, deser, format, disconnected) = handler.get_stats().await;
        assert_eq!(parsing, 0);
        assert_eq!(malformed, 1);
        assert_eq!(deser, 0);
        assert_eq!(format, 0);
        assert_eq!(disconnected, 1);

        // Verify peer is marked unhealthy
        let peer = peer_manager.get_peer("peer1").await.unwrap();
        assert!(!peer.is_healthy);
    }

    #[tokio::test]
    async fn test_handle_deserialization_error() {
        let peer_manager = Arc::new(PeerManager::new(1000));
        let connection_pool = Arc::new(ConnectionPool::new(100, 300));
        let handler = MessageErrorHandler::new(peer_manager.clone(), connection_pool.clone());

        // Add peer first
        peer_manager
            .add_peer("peer1".to_string(), "127.0.0.1:9000".to_string(), NodeRole::Validator)
            .await
            .unwrap();

        let error = P2PError::DeserializationError("test error".to_string());
        handler
            .handle_deserialization_error("peer1", &error)
            .await
            .unwrap();

        let (parsing, malformed, deser, format, disconnected) = handler.get_stats().await;
        assert_eq!(parsing, 0);
        assert_eq!(malformed, 0);
        assert_eq!(deser, 1);
        assert_eq!(format, 0);
        assert_eq!(disconnected, 1);

        // Verify peer is marked unhealthy
        let peer = peer_manager.get_peer("peer1").await.unwrap();
        assert!(!peer.is_healthy);
    }

    #[tokio::test]
    async fn test_handle_format_error() {
        let peer_manager = Arc::new(PeerManager::new(1000));
        let connection_pool = Arc::new(ConnectionPool::new(100, 300));
        let handler = MessageErrorHandler::new(peer_manager.clone(), connection_pool.clone());

        // Add peer first
        peer_manager
            .add_peer("peer1".to_string(), "127.0.0.1:9000".to_string(), NodeRole::Validator)
            .await
            .unwrap();

        let error = P2PError::InvalidMessageFormat("test error".to_string());
        handler.handle_format_error("peer1", &error).await.unwrap();

        let (parsing, malformed, deser, format, disconnected) = handler.get_stats().await;
        assert_eq!(parsing, 0);
        assert_eq!(malformed, 0);
        assert_eq!(deser, 0);
        assert_eq!(format, 1);
        assert_eq!(disconnected, 1);

        // Verify peer is marked unhealthy
        let peer = peer_manager.get_peer("peer1").await.unwrap();
        assert!(!peer.is_healthy);
    }

    #[tokio::test]
    async fn test_validate_message_valid() {
        let peer_manager = Arc::new(PeerManager::new(1000));
        let connection_pool = Arc::new(ConnectionPool::new(100, 300));
        let handler = MessageErrorHandler::new(peer_manager.clone(), connection_pool.clone());

        // Add peer first
        peer_manager
            .add_peer("peer1".to_string(), "127.0.0.1:9000".to_string(), NodeRole::Validator)
            .await
            .unwrap();

        let msg = NetworkMessage::Ping { nonce: 123 };
        let result = handler.validate_message("peer1", &msg).await;

        assert!(result.is_ok());
        let (parsing, malformed, deser, format, disconnected) = handler.get_stats().await;
        assert_eq!(parsing, 0);
        assert_eq!(malformed, 0);
        assert_eq!(deser, 0);
        assert_eq!(format, 0);
        assert_eq!(disconnected, 0);
    }

    #[tokio::test]
    async fn test_reset_stats() {
        let peer_manager = Arc::new(PeerManager::new(1000));
        let connection_pool = Arc::new(ConnectionPool::new(100, 300));
        let handler = MessageErrorHandler::new(peer_manager.clone(), connection_pool.clone());

        // Add peer first
        peer_manager
            .add_peer("peer1".to_string(), "127.0.0.1:9000".to_string(), NodeRole::Validator)
            .await
            .unwrap();

        let error = P2PError::InvalidMessageFormat("test error".to_string());
        handler.handle_parsing_error("peer1", &error).await.unwrap();

        let (parsing, malformed, deser, format, disconnected) = handler.get_stats().await;
        assert_eq!(parsing, 1);
        assert_eq!(malformed, 0);
        assert_eq!(deser, 0);
        assert_eq!(format, 0);
        assert_eq!(disconnected, 1);

        handler.reset_stats().await;

        let (parsing, malformed, deser, format, disconnected) = handler.get_stats().await;
        assert_eq!(parsing, 0);
        assert_eq!(malformed, 0);
        assert_eq!(deser, 0);
        assert_eq!(format, 0);
        assert_eq!(disconnected, 0);
    }
}
