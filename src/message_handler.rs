//! Message serialization and handling

use crate::error::{P2PError, Result};
use crate::types::NetworkMessage;
use crate::message_chunking::{MessageChunker, ChunkedMessage};
use crate::message_error_handler::MessageErrorHandler;
use crate::peer_manager::PeerManager;
use crate::connection_pool::ConnectionPool;
use bytes::{BytesMut, BufMut, Buf};
use dashmap::DashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use async_trait::async_trait;
use tracing::{debug, warn, error};

/// Message handler function trait
#[async_trait]
pub trait MessageHandlerFn: Send + Sync {
    /// Handle a message from a peer
    async fn handle(&self, peer_id: String, msg: NetworkMessage) -> Result<()>;
}

/// Handles message serialization, deserialization, and routing
pub struct MessageHandler {
    /// Maximum message size in bytes
    max_message_size: usize,
    /// Registered message handlers by type
    handlers: Arc<DashMap<String, Arc<dyn MessageHandlerFn>>>,
    /// Message statistics
    stats: Arc<MessageStats>,
    /// Message chunker for large messages
    chunker: Arc<MessageChunker>,
    /// Message error handler for malformed data detection
    error_handler: Option<Arc<MessageErrorHandler>>,
}

/// Message statistics
pub struct MessageStats {
    /// Total messages processed
    pub total_messages: AtomicU64,
    /// Total bytes processed
    pub total_bytes: AtomicU64,
    /// Messages by type
    pub messages_by_type: Arc<DashMap<String, u64>>,
    /// Errors encountered
    pub total_errors: AtomicU64,
}

impl MessageHandler {
    /// Create new message handler
    pub fn new(max_message_size: usize) -> Self {
        Self {
            max_message_size,
            handlers: Arc::new(DashMap::new()),
            stats: Arc::new(MessageStats {
                total_messages: AtomicU64::new(0),
                total_bytes: AtomicU64::new(0),
                messages_by_type: Arc::new(DashMap::new()),
                total_errors: AtomicU64::new(0),
            }),
            chunker: Arc::new(MessageChunker::new()),
            error_handler: None,
        }
    }

    /// Create new message handler with error handling
    pub fn new_with_error_handler(
        max_message_size: usize,
        peer_manager: Arc<PeerManager>,
        connection_pool: Arc<ConnectionPool>,
    ) -> Self {
        let error_handler = Arc::new(MessageErrorHandler::new(peer_manager, connection_pool));
        Self {
            max_message_size,
            handlers: Arc::new(DashMap::new()),
            stats: Arc::new(MessageStats {
                total_messages: AtomicU64::new(0),
                total_bytes: AtomicU64::new(0),
                messages_by_type: Arc::new(DashMap::new()),
                total_errors: AtomicU64::new(0),
            }),
            chunker: Arc::new(MessageChunker::new()),
            error_handler: Some(error_handler),
        }
    }

    /// Set error handler for message validation
    pub fn set_error_handler(&mut self, error_handler: Arc<MessageErrorHandler>) {
        self.error_handler = Some(error_handler);
    }

    /// Get error handler reference
    pub fn get_error_handler(&self) -> Option<Arc<MessageErrorHandler>> {
        self.error_handler.clone()
    }

    /// Register a handler for a message type
    pub async fn register_handler(
        &self,
        msg_type: String,
        handler: Arc<dyn MessageHandlerFn>,
    ) -> Result<()> {
        self.handlers.insert(msg_type.clone(), handler);
        debug!("Registered handler for message type: {}", msg_type);
        Ok(())
    }

    /// Unregister a handler for a message type
    pub async fn unregister_handler(&self, msg_type: &str) -> Result<()> {
        self.handlers.remove(msg_type);
        debug!("Unregistered handler for message type: {}", msg_type);
        Ok(())
    }

    /// Get registered handler for message type
    pub async fn get_handler(&self, msg_type: &str) -> Option<Arc<dyn MessageHandlerFn>> {
        self.handlers.get(msg_type).map(|h| h.clone())
    }

    /// Check if handler is registered for message type
    pub async fn has_handler(&self, msg_type: &str) -> bool {
        self.handlers.contains_key(msg_type)
    }

    /// Get all registered message types
    pub async fn get_registered_types(&self) -> Vec<String> {
        self.handlers
            .iter()
            .map(|entry| entry.key().clone())
            .collect()
    }

    /// Handle incoming message by routing to appropriate handler
    pub async fn handle_message(&self, peer_id: String, msg: NetworkMessage) -> Result<()> {
        let msg_type = msg.message_type();
        let msg_size = msg.estimated_size();

        // Update statistics
        self.stats.total_messages.fetch_add(1, Ordering::SeqCst);
        self.stats.total_bytes.fetch_add(msg_size as u64, Ordering::SeqCst);
        self.stats
            .messages_by_type
            .entry(msg_type.clone())
            .and_modify(|count| *count += 1)
            .or_insert(1);

        debug!(
            "Handling message type: {} from peer: {} (size: {} bytes)",
            msg_type, peer_id, msg_size
        );

        // Get handler for message type
        if let Some(handler) = self.get_handler(&msg_type).await {
            match handler.handle(peer_id.clone(), msg).await {
                Ok(()) => {
                    debug!("Successfully handled {} message from peer: {}", msg_type, peer_id);
                    Ok(())
                }
                Err(e) => {
                    warn!(
                        "Error handling {} message from peer: {}: {}",
                        msg_type, peer_id, e
                    );
                    self.stats.total_errors.fetch_add(1, Ordering::SeqCst);
                    Err(e)
                }
            }
        } else {
            error!("No handler registered for message type: {}", msg_type);
            self.stats.total_errors.fetch_add(1, Ordering::SeqCst);
            Err(P2PError::InternalError(format!(
                "No handler for message type: {}",
                msg_type
            )))
        }
    }

    /// Serialize message with 4-byte length prefix in little-endian format
    /// For messages exceeding 10 MB, returns the first chunk only (caller handles chunking)
    pub async fn serialize(&self, msg: &NetworkMessage) -> Result<Vec<u8>> {
        let data = bincode::serialize(msg)
            .map_err(|e| P2PError::SerializationError(e.to_string()))?;

        if data.len() > self.max_message_size {
            return Err(P2PError::MessageTooLarge(data.len(), self.max_message_size));
        }

        let mut buf = BytesMut::with_capacity(data.len() + 4);
        buf.put_u32_le(data.len() as u32);
        buf.put_slice(&data);

        Ok(buf.to_vec())
    }

    /// Serialize message with automatic chunking for large messages
    /// Returns a vector of serialized chunks if message exceeds 10 MB, otherwise returns single serialized message
    pub async fn serialize_with_chunking(&self, msg: &NetworkMessage) -> Result<Vec<Vec<u8>>> {
        let data = bincode::serialize(msg)
            .map_err(|e| P2PError::SerializationError(e.to_string()))?;

        if data.len() > self.max_message_size {
            return Err(P2PError::MessageTooLarge(data.len(), self.max_message_size));
        }

        // Check if message needs chunking
        if self.chunker.needs_chunking(&data) {
            let chunks = self.chunker.chunk_message(&data)?;
            let mut serialized_chunks = Vec::new();

            for chunk in chunks {
                // Serialize each chunk with metadata
                let chunk_data = bincode::serialize(&chunk)
                    .map_err(|e| P2PError::SerializationError(e.to_string()))?;

                let mut buf = BytesMut::with_capacity(chunk_data.len() + 4);
                buf.put_u32_le(chunk_data.len() as u32);
                buf.put_slice(&chunk_data);

                serialized_chunks.push(buf.to_vec());
            }

            debug!(
                "Serialized message into {} chunks (total size: {} bytes)",
                serialized_chunks.len(),
                data.len()
            );

            Ok(serialized_chunks)
        } else {
            // Single message, serialize normally
            let mut buf = BytesMut::with_capacity(data.len() + 4);
            buf.put_u32_le(data.len() as u32);
            buf.put_slice(&data);

            Ok(vec![buf.to_vec()])
        }
    }

    /// Deserialize message with 4-byte length prefix validation
    pub async fn deserialize(&self, data: &[u8]) -> Result<NetworkMessage> {
        if data.len() < 4 {
            return Err(P2PError::InvalidMessageFormat(
                "Message too short for length prefix".to_string(),
            ));
        }

        let mut buf = data;
        let len = buf.get_u32_le() as usize;

        if len > self.max_message_size {
            return Err(P2PError::MessageTooLarge(len, self.max_message_size));
        }

        if buf.len() < len {
            return Err(P2PError::InvalidMessageFormat(
                "Incomplete message data".to_string(),
            ));
        }

        let msg_data = &buf[..len];
        bincode::deserialize(msg_data)
            .map_err(|e| P2PError::DeserializationError(e.to_string()))
    }

    /// Deserialize message with error handling for malformed data
    pub async fn deserialize_with_error_handling(
        &self,
        peer_id: &str,
        data: &[u8],
    ) -> Result<NetworkMessage> {
        // Validate format first
        if data.len() < 4 {
            let error = P2PError::InvalidMessageFormat(
                "Message too short for length prefix".to_string(),
            );
            if let Some(handler) = &self.error_handler {
                handler.handle_format_error(peer_id, &error).await.ok();
            }
            return Err(error);
        }

        let mut buf = data;
        let len = buf.get_u32_le() as usize;

        if len > self.max_message_size {
            let error = P2PError::MessageTooLarge(len, self.max_message_size);
            if let Some(handler) = &self.error_handler {
                handler.handle_format_error(peer_id, &error).await.ok();
            }
            return Err(error);
        }

        if buf.len() < len {
            let error = P2PError::InvalidMessageFormat(
                "Incomplete message data".to_string(),
            );
            if let Some(handler) = &self.error_handler {
                handler.handle_format_error(peer_id, &error).await.ok();
            }
            return Err(error);
        }

        let msg_data = &buf[..len];
        match bincode::deserialize::<NetworkMessage>(msg_data) {
            Ok(msg) => {
                // Validate deserialized message
                if let Some(handler) = &self.error_handler {
                    handler.validate_message(peer_id, &msg).await?;
                }
                Ok(msg)
            }
            Err(e) => {
                let error = P2PError::DeserializationError(e.to_string());
                if let Some(handler) = &self.error_handler {
                    handler.handle_deserialization_error(peer_id, &error).await.ok();
                }
                Err(error)
            }
        }
    }

    /// Deserialize a chunk and attempt reassembly
    /// Returns Some(NetworkMessage) if reassembly is complete, None if waiting for more chunks
    pub async fn deserialize_chunk(&self, peer_id: &str, data: &[u8]) -> Result<Option<NetworkMessage>> {
        if data.len() < 4 {
            return Err(P2PError::InvalidMessageFormat(
                "Chunk too short for length prefix".to_string(),
            ));
        }

        let mut buf = data;
        let len = buf.get_u32_le() as usize;

        if len > self.max_message_size {
            return Err(P2PError::MessageTooLarge(len, self.max_message_size));
        }

        if buf.len() < len {
            return Err(P2PError::InvalidMessageFormat(
                "Incomplete chunk data".to_string(),
            ));
        }

        let chunk_data = &buf[..len];
        
        // Try to deserialize as ChunkedMessage first (for multi-chunk messages)
        if let Ok(chunk) = bincode::deserialize::<ChunkedMessage>(chunk_data) {
            // This is a chunked message, add it and check if reassembly is complete
            if let Some(reassembled_data) = self.chunker.add_chunk(peer_id, chunk).await? {
                // Deserialize the reassembled message
                match bincode::deserialize::<NetworkMessage>(&reassembled_data) {
                    Ok(msg) => {
                        // Validate reassembled message
                        if let Some(handler) = &self.error_handler {
                            handler.validate_message(peer_id, &msg).await?;
                        }
                        Ok(Some(msg))
                    }
                    Err(e) => {
                        let error = P2PError::DeserializationError(e.to_string());
                        if let Some(handler) = &self.error_handler {
                            handler.handle_deserialization_error(peer_id, &error).await.ok();
                        }
                        Err(error)
                    }
                }
            } else {
                Ok(None)
            }
        } else {
            // Not a ChunkedMessage, try to deserialize as a regular NetworkMessage
            // This handles single messages that were serialized with serialize_with_chunking
            match bincode::deserialize::<NetworkMessage>(chunk_data) {
                Ok(msg) => {
                    // Validate deserialized message
                    if let Some(handler) = &self.error_handler {
                        handler.validate_message(peer_id, &msg).await?;
                    }
                    Ok(Some(msg))
                }
                Err(e) => {
                    let error = P2PError::DeserializationError(e.to_string());
                    if let Some(handler) = &self.error_handler {
                        handler.handle_deserialization_error(peer_id, &error).await.ok();
                    }
                    Err(error)
                }
            }
        }
    }

    /// Validate message format without deserializing
    pub async fn validate_format(&self, data: &[u8]) -> Result<usize> {
        if data.len() < 4 {
            return Err(P2PError::InvalidMessageFormat(
                "Message too short for length prefix".to_string(),
            ));
        }

        let mut buf = data;
        let len = buf.get_u32_le() as usize;

        if len > self.max_message_size {
            return Err(P2PError::MessageTooLarge(len, self.max_message_size));
        }

        Ok(len)
    }

    /// Validate message format with error handling
    pub async fn validate_format_with_error_handling(
        &self,
        peer_id: &str,
        data: &[u8],
    ) -> Result<usize> {
        if let Some(handler) = &self.error_handler {
            handler.validate_message_format(peer_id, data, self.max_message_size).await
        } else {
            self.validate_format(data).await
        }
    }

    /// Get message statistics
    pub async fn get_stats(&self) -> (u64, u64, u64) {
        (
            self.stats.total_messages.load(Ordering::SeqCst),
            self.stats.total_bytes.load(Ordering::SeqCst),
            self.stats.total_errors.load(Ordering::SeqCst),
        )
    }

    /// Get message count by type
    pub async fn get_message_count_by_type(&self, msg_type: &str) -> u64 {
        self.stats
            .messages_by_type
            .get(msg_type)
            .map(|count| *count)
            .unwrap_or(0)
    }

    /// Get all message type statistics
    pub async fn get_all_message_stats(&self) -> Vec<(String, u64)> {
        self.stats
            .messages_by_type
            .iter()
            .map(|entry| (entry.key().clone(), *entry.value()))
            .collect()
    }

    /// Clear statistics
    pub async fn clear_stats(&self) {
        self.stats.messages_by_type.clear();
    }

    /// Clean up timed-out incomplete messages
    pub async fn cleanup_timed_out_chunks(&self) -> usize {
        self.chunker.cleanup_timed_out().await
    }

    /// Get number of incomplete messages being reassembled
    pub async fn get_incomplete_chunk_count(&self) -> usize {
        self.chunker.get_incomplete_count().await
    }

    /// Clear all incomplete messages for a peer
    pub async fn clear_peer_chunks(&self, peer_id: &str) {
        self.chunker.clear_peer_messages(peer_id).await;
    }

    /// Get chunker reference for advanced operations
    pub fn get_chunker(&self) -> Arc<MessageChunker> {
        self.chunker.clone()
    }
}

/// Test handler implementation
#[cfg(test)]
struct TestHandler;

#[cfg(test)]
#[async_trait]
impl MessageHandlerFn for TestHandler {
    async fn handle(&self, _peer_id: String, _msg: NetworkMessage) -> Result<()> {
        Ok(())
    }
}

/// Error handler for testing
#[cfg(test)]
struct ErrorHandler;

#[cfg(test)]
#[async_trait]
impl MessageHandlerFn for ErrorHandler {
    async fn handle(&self, _peer_id: String, _msg: NetworkMessage) -> Result<()> {
        Err(P2PError::InternalError("Test error".to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_message_serialization() {
        let handler = MessageHandler::new(100 * 1024 * 1024);
        let msg = NetworkMessage::Ping { nonce: 123 };

        let serialized = handler.serialize(&msg).await.unwrap();
        assert!(serialized.len() > 4);

        let deserialized = handler.deserialize(&serialized).await.unwrap();
        assert_eq!(deserialized, msg);
    }

    #[tokio::test]
    async fn test_message_too_large() {
        let handler = MessageHandler::new(100);
        let msg = NetworkMessage::Custom {
            msg_type: "test".to_string(),
            data: vec![0u8; 1000],
        };

        let result = handler.serialize(&msg).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_invalid_message_format() {
        let handler = MessageHandler::new(100 * 1024 * 1024);
        let result = handler.deserialize(&[1, 2]).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_handler_registration() {
        let handler = MessageHandler::new(100 * 1024 * 1024);
        let test_handler = Arc::new(TestHandler);

        handler
            .register_handler("test".to_string(), test_handler)
            .await
            .unwrap();

        assert!(handler.has_handler("test").await);
        assert!(handler.get_handler("test").await.is_some());
    }

    #[tokio::test]
    async fn test_handler_unregistration() {
        let handler = MessageHandler::new(100 * 1024 * 1024);
        let test_handler = Arc::new(TestHandler);

        handler
            .register_handler("test".to_string(), test_handler)
            .await
            .unwrap();

        assert!(handler.has_handler("test").await);

        handler.unregister_handler("test").await.unwrap();

        assert!(!handler.has_handler("test").await);
    }

    #[tokio::test]
    async fn test_get_registered_types() {
        let handler = MessageHandler::new(100 * 1024 * 1024);
        let test_handler = Arc::new(TestHandler);

        handler
            .register_handler("type1".to_string(), test_handler.clone())
            .await
            .unwrap();
        handler
            .register_handler("type2".to_string(), test_handler.clone())
            .await
            .unwrap();

        let types = handler.get_registered_types().await;
        assert_eq!(types.len(), 2);
        assert!(types.contains(&"type1".to_string()));
        assert!(types.contains(&"type2".to_string()));
    }

    #[tokio::test]
    async fn test_handle_message_success() {
        let handler = MessageHandler::new(100 * 1024 * 1024);
        let test_handler = Arc::new(TestHandler);

        handler
            .register_handler("Ping".to_string(), test_handler)
            .await
            .unwrap();

        let msg = NetworkMessage::Ping { nonce: 123 };
        let result = handler
            .handle_message("peer1".to_string(), msg)
            .await;

        assert!(result.is_ok());
        let (total, _, errors) = handler.get_stats().await;
        assert_eq!(total, 1);
        assert_eq!(errors, 0);
    }

    #[tokio::test]
    async fn test_handle_message_no_handler() {
        let handler = MessageHandler::new(100 * 1024 * 1024);

        let msg = NetworkMessage::Ping { nonce: 123 };
        let result = handler
            .handle_message("peer1".to_string(), msg)
            .await;

        assert!(result.is_err());
        let (total, _, errors) = handler.get_stats().await;
        assert_eq!(total, 1);
        assert_eq!(errors, 1);
    }

    #[tokio::test]
    async fn test_handle_message_handler_error() {
        let handler = MessageHandler::new(100 * 1024 * 1024);
        let error_handler = Arc::new(ErrorHandler);

        handler
            .register_handler("Ping".to_string(), error_handler)
            .await
            .unwrap();

        let msg = NetworkMessage::Ping { nonce: 123 };
        let result = handler
            .handle_message("peer1".to_string(), msg)
            .await;

        assert!(result.is_err());
        let (total, _, errors) = handler.get_stats().await;
        assert_eq!(total, 1);
        assert_eq!(errors, 1);
    }

    #[tokio::test]
    async fn test_message_statistics() {
        let handler = MessageHandler::new(100 * 1024 * 1024);
        let test_handler = Arc::new(TestHandler);

        handler
            .register_handler("Ping".to_string(), test_handler.clone())
            .await
            .unwrap();
        handler
            .register_handler("Pong".to_string(), test_handler)
            .await
            .unwrap();

        let ping_msg = NetworkMessage::Ping { nonce: 123 };
        let pong_msg = NetworkMessage::Pong { nonce: 123 };

        handler
            .handle_message("peer1".to_string(), ping_msg.clone())
            .await
            .unwrap();
        handler
            .handle_message("peer1".to_string(), pong_msg.clone())
            .await
            .unwrap();
        handler
            .handle_message("peer1".to_string(), ping_msg)
            .await
            .unwrap();

        let (total, _, _) = handler.get_stats().await;
        assert_eq!(total, 3);

        let ping_count = handler.get_message_count_by_type("Ping").await;
        let pong_count = handler.get_message_count_by_type("Pong").await;
        assert_eq!(ping_count, 2);
        assert_eq!(pong_count, 1);
    }

    #[tokio::test]
    async fn test_clear_statistics() {
        let handler = MessageHandler::new(100 * 1024 * 1024);
        let test_handler = Arc::new(TestHandler);

        handler
            .register_handler("Ping".to_string(), test_handler)
            .await
            .unwrap();

        let msg = NetworkMessage::Ping { nonce: 123 };
        handler
            .handle_message("peer1".to_string(), msg)
            .await
            .unwrap();

        let stats_before = handler.get_all_message_stats().await;
        assert!(!stats_before.is_empty());

        handler.clear_stats().await;

        let stats_after = handler.get_all_message_stats().await;
        assert!(stats_after.is_empty());
    }

    #[tokio::test]
    async fn test_multiple_handlers() {
        let handler = MessageHandler::new(100 * 1024 * 1024);
        let test_handler = Arc::new(TestHandler);

        for i in 0..5 {
            handler
                .register_handler(format!("type{}", i), test_handler.clone())
                .await
                .unwrap();
        }

        let types = handler.get_registered_types().await;
        assert_eq!(types.len(), 5);
    }

    #[tokio::test]
    async fn test_serialization_round_trip() {
        let handler = MessageHandler::new(100 * 1024 * 1024);

        let messages = vec![
            NetworkMessage::Ping { nonce: 123 },
            NetworkMessage::Pong { nonce: 456 },
            NetworkMessage::PeerListRequest,
            NetworkMessage::PeerListResponse(vec!["127.0.0.1:9000".to_string()]),
        ];

        for msg in messages {
            let serialized = handler.serialize(&msg).await.unwrap();
            let deserialized = handler.deserialize(&serialized).await.unwrap();
            assert_eq!(msg, deserialized);
        }
    }

    #[tokio::test]
    async fn test_serialize_with_chunking_small_message() {
        let handler = MessageHandler::new(100 * 1024 * 1024);
        let msg = NetworkMessage::Ping { nonce: 123 };

        let chunks = handler.serialize_with_chunking(&msg).await.unwrap();
        assert_eq!(chunks.len(), 1);
    }

    #[tokio::test]
    async fn test_serialize_with_chunking_large_message() {
        let handler = MessageHandler::new(100 * 1024 * 1024);
        let large_data = vec![0u8; 25_000_000];
        let msg = NetworkMessage::Custom {
            msg_type: "large".to_string(),
            data: large_data,
        };

        let chunks = handler.serialize_with_chunking(&msg).await.unwrap();
        assert!(chunks.len() > 1);
    }

    #[tokio::test]
    async fn test_deserialize_chunk_single_message() {
        let handler = MessageHandler::new(100 * 1024 * 1024);
        let msg = NetworkMessage::Ping { nonce: 123 };

        // Serialize with chunking (even though it won't chunk small messages)
        let chunks = handler.serialize_with_chunking(&msg).await.unwrap();
        assert_eq!(chunks.len(), 1);
        
        // Single message chunk should deserialize directly
        let result = handler.deserialize_chunk("peer1", &chunks[0]).await.unwrap();
        
        // Single message should complete immediately
        assert!(result.is_some());
        assert_eq!(result.unwrap(), msg);
    }

    #[tokio::test]
    async fn test_deserialize_chunk_reassembly() {
        let handler = MessageHandler::new(100 * 1024 * 1024);
        let large_data = vec![42u8; 25_000_000];
        let msg = NetworkMessage::Custom {
            msg_type: "large".to_string(),
            data: large_data.clone(),
        };

        let chunks = handler.serialize_with_chunking(&msg).await.unwrap();
        assert!(chunks.len() > 1);

        // Add chunks out of order
        let result = handler.deserialize_chunk("peer1", &chunks[2]).await.unwrap();
        assert!(result.is_none());

        let result = handler.deserialize_chunk("peer1", &chunks[0]).await.unwrap();
        assert!(result.is_none());

        let result = handler.deserialize_chunk("peer1", &chunks[1]).await.unwrap();
        assert!(result.is_some());

        let reassembled = result.unwrap();
        assert_eq!(reassembled, msg);
    }

    #[tokio::test]
    async fn test_cleanup_timed_out_chunks() {
        let handler = MessageHandler::new(100 * 1024 * 1024);
        
        let count = handler.cleanup_timed_out_chunks().await;
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn test_get_incomplete_chunk_count() {
        let handler = MessageHandler::new(100 * 1024 * 1024);
        
        let count = handler.get_incomplete_chunk_count().await;
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn test_clear_peer_chunks() {
        let handler = MessageHandler::new(100 * 1024 * 1024);
        
        handler.clear_peer_chunks("peer1").await;
        assert_eq!(handler.get_incomplete_chunk_count().await, 0);
    }
}
