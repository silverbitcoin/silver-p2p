//! Message chunking for large messages exceeding 10 MB

use crate::error::{P2PError, Result};
use dashmap::DashMap;
use std::sync::Arc;
use std::time::{SystemTime, Duration};
use std::collections::HashMap;
use tracing::{debug, warn};

/// Chunk size for large messages (10 MB)
const CHUNK_SIZE: usize = 10 * 1024 * 1024;

/// Timeout for incomplete message reassembly (5 minutes)
const REASSEMBLY_TIMEOUT: Duration = Duration::from_secs(300);

/// Message chunk metadata
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct ChunkMetadata {
    /// Unique message ID
    pub message_id: u64,
    /// Total number of chunks
    pub total_chunks: u32,
    /// Current chunk index (0-based)
    pub chunk_index: u32,
    /// Total message size in bytes
    pub total_size: u64,
}

/// Chunked message for transmission
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct ChunkedMessage {
    /// Chunk metadata
    pub metadata: ChunkMetadata,
    /// Chunk data
    pub data: Vec<u8>,
}

/// Incomplete message being reassembled
#[derive(Clone, Debug)]
struct IncompleteMessage {
    /// Chunks received so far (indexed by chunk_index)
    chunks: HashMap<u32, Vec<u8>>,
    /// Total chunks expected
    total_chunks: u32,
    /// Total message size
    total_size: u64,
    /// Time when reassembly started
    start_time: SystemTime,
}

impl IncompleteMessage {
    /// Create new incomplete message
    fn new(total_chunks: u32, total_size: u64) -> Self {
        Self {
            chunks: HashMap::new(),
            total_chunks,
            total_size,
            start_time: SystemTime::now(),
        }
    }

    /// Add a chunk to the message
    fn add_chunk(&mut self, chunk_index: u32, data: Vec<u8>) -> Result<()> {
        if chunk_index >= self.total_chunks {
            return Err(P2PError::InvalidMessageFormat(
                format!("Invalid chunk index: {} (total: {})", chunk_index, self.total_chunks),
            ));
        }

        if self.chunks.contains_key(&chunk_index) {
            return Err(P2PError::InvalidMessageFormat(
                format!("Duplicate chunk index: {}", chunk_index),
            ));
        }

        self.chunks.insert(chunk_index, data);
        Ok(())
    }

    /// Check if all chunks have been received
    fn is_complete(&self) -> bool {
        self.chunks.len() == self.total_chunks as usize
    }

    /// Check if reassembly has timed out
    fn is_timed_out(&self) -> bool {
        self.start_time.elapsed().unwrap_or(Duration::from_secs(0)) > REASSEMBLY_TIMEOUT
    }

    /// Reassemble the complete message
    fn reassemble(&self) -> Result<Vec<u8>> {
        if !self.is_complete() {
            return Err(P2PError::InvalidMessageFormat(
                format!(
                    "Incomplete message: {}/{} chunks received",
                    self.chunks.len(),
                    self.total_chunks
                ),
            ));
        }

        let mut result = Vec::with_capacity(self.total_size as usize);

        for i in 0..self.total_chunks {
            if let Some(chunk) = self.chunks.get(&i) {
                result.extend_from_slice(chunk);
            } else {
                return Err(P2PError::InvalidMessageFormat(
                    format!("Missing chunk: {}", i),
                ));
            }
        }

        if result.len() != self.total_size as usize {
            return Err(P2PError::InvalidMessageFormat(
                format!(
                    "Reassembled message size mismatch: {} (expected: {})",
                    result.len(),
                    self.total_size
                ),
            ));
        }

        Ok(result)
    }
}

/// Message chunking manager
pub struct MessageChunker {
    /// Maximum message size before chunking (10 MB)
    chunk_threshold: usize,
    /// Incomplete messages being reassembled (keyed by peer_id:message_id)
    incomplete_messages: Arc<DashMap<String, IncompleteMessage>>,
    /// Next message ID to use
    next_message_id: Arc<std::sync::atomic::AtomicU64>,
}

impl MessageChunker {
    /// Create new message chunker
    pub fn new() -> Self {
        Self {
            chunk_threshold: CHUNK_SIZE,
            incomplete_messages: Arc::new(DashMap::new()),
            next_message_id: Arc::new(std::sync::atomic::AtomicU64::new(1)),
        }
    }

    /// Create new message chunker with custom threshold
    pub fn with_threshold(chunk_threshold: usize) -> Self {
        Self {
            chunk_threshold,
            incomplete_messages: Arc::new(DashMap::new()),
            next_message_id: Arc::new(std::sync::atomic::AtomicU64::new(1)),
        }
    }

    /// Check if message needs chunking
    pub fn needs_chunking(&self, data: &[u8]) -> bool {
        data.len() > self.chunk_threshold
    }

    /// Split message into chunks
    pub fn chunk_message(&self, data: &[u8]) -> Result<Vec<ChunkedMessage>> {
        if !self.needs_chunking(data) {
            return Err(P2PError::InvalidMessageFormat(
                "Message does not need chunking".to_string(),
            ));
        }

        let message_id = self
            .next_message_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let total_size = data.len() as u64;
        let total_chunks = data.len().div_ceil(self.chunk_threshold) as u32;

        let mut chunks = Vec::new();

        for (chunk_index, chunk_data) in data.chunks(self.chunk_threshold).enumerate() {
            let metadata = ChunkMetadata {
                message_id,
                total_chunks,
                chunk_index: chunk_index as u32,
                total_size,
            };

            chunks.push(ChunkedMessage {
                metadata,
                data: chunk_data.to_vec(),
            });
        }

        debug!(
            "Split message {} into {} chunks (total size: {} bytes)",
            message_id, total_chunks, total_size
        );

        Ok(chunks)
    }

    /// Add a chunk to reassembly buffer
    pub async fn add_chunk(&self, peer_id: &str, chunk: ChunkedMessage) -> Result<Option<Vec<u8>>> {
        let key = format!("{}:{}", peer_id, chunk.metadata.message_id);

        // Validate chunk
        if chunk.data.is_empty() {
            return Err(P2PError::InvalidMessageFormat(
                "Empty chunk data".to_string(),
            ));
        }

        if chunk.metadata.chunk_index >= chunk.metadata.total_chunks {
            return Err(P2PError::InvalidMessageFormat(
                format!(
                    "Invalid chunk index: {} (total: {})",
                    chunk.metadata.chunk_index, chunk.metadata.total_chunks
                ),
            ));
        }

        // Get or create incomplete message
        let mut incomplete = self
            .incomplete_messages
            .entry(key.clone())
            .or_insert_with(|| {
                IncompleteMessage::new(chunk.metadata.total_chunks, chunk.metadata.total_size)
            });

        // Check for timeout
        if incomplete.is_timed_out() {
            warn!(
                "Reassembly timeout for message {} from peer {}",
                chunk.metadata.message_id, peer_id
            );
            drop(incomplete);
            self.incomplete_messages.remove(&key);
            return Err(P2PError::InvalidMessageFormat(
                "Reassembly timeout".to_string(),
            ));
        }

        // Add chunk
        incomplete.add_chunk(chunk.metadata.chunk_index, chunk.data)?;

        // Check if complete
        if incomplete.is_complete() {
            let reassembled = incomplete.reassemble()?;
            drop(incomplete);
            self.incomplete_messages.remove(&key);

            debug!(
                "Reassembled message {} from peer {} ({} bytes)",
                chunk.metadata.message_id,
                peer_id,
                reassembled.len()
            );

            Ok(Some(reassembled))
        } else {
            debug!(
                "Received chunk {}/{} for message {} from peer {}",
                chunk.metadata.chunk_index + 1,
                chunk.metadata.total_chunks,
                chunk.metadata.message_id,
                peer_id
            );
            Ok(None)
        }
    }

    /// Clean up timed-out incomplete messages
    pub async fn cleanup_timed_out(&self) -> usize {
        let mut removed = 0;

        self.incomplete_messages.retain(|_, incomplete| {
            if incomplete.is_timed_out() {
                warn!(
                    "Removing timed-out incomplete message ({}% complete)",
                    (incomplete.chunks.len() * 100) / incomplete.total_chunks as usize
                );
                removed += 1;
                false
            } else {
                true
            }
        });

        removed
    }

    /// Get number of incomplete messages
    pub async fn get_incomplete_count(&self) -> usize {
        self.incomplete_messages.len()
    }

    /// Get incomplete message info for a peer
    pub async fn get_incomplete_info(&self, peer_id: &str) -> Vec<(u64, u32, u32)> {
        self.incomplete_messages
            .iter()
            .filter_map(|entry| {
                let key = entry.key();
                if key.starts_with(&format!("{}:", peer_id)) {
                    let incomplete = entry.value();
                    Some((
                        incomplete.total_size,
                        incomplete.chunks.len() as u32,
                        incomplete.total_chunks,
                    ))
                } else {
                    None
                }
            })
            .collect()
    }

    /// Clear all incomplete messages for a peer
    pub async fn clear_peer_messages(&self, peer_id: &str) {
        self.incomplete_messages.retain(|key, _| {
            !key.starts_with(&format!("{}:", peer_id))
        });
    }
}

impl Default for MessageChunker {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_incomplete_message_creation() {
        let incomplete = IncompleteMessage::new(4, 40_000_000);
        assert_eq!(incomplete.total_chunks, 4);
        assert_eq!(incomplete.total_size, 40_000_000);
        assert!(!incomplete.is_complete());
    }

    #[test]
    fn test_incomplete_message_add_chunk() {
        let mut incomplete = IncompleteMessage::new(2, 20_000_000);
        let chunk_data = vec![0u8; 10_000_000];

        incomplete.add_chunk(0, chunk_data.clone()).unwrap();
        assert_eq!(incomplete.chunks.len(), 1);
        assert!(!incomplete.is_complete());

        incomplete.add_chunk(1, chunk_data).unwrap();
        assert_eq!(incomplete.chunks.len(), 2);
        assert!(incomplete.is_complete());
    }

    #[test]
    fn test_incomplete_message_duplicate_chunk() {
        let mut incomplete = IncompleteMessage::new(2, 20_000_000);
        let chunk_data = vec![0u8; 10_000_000];

        incomplete.add_chunk(0, chunk_data.clone()).unwrap();
        let result = incomplete.add_chunk(0, chunk_data);
        assert!(result.is_err());
    }

    #[test]
    fn test_incomplete_message_invalid_chunk_index() {
        let mut incomplete = IncompleteMessage::new(2, 20_000_000);
        let chunk_data = vec![0u8; 10_000_000];

        let result = incomplete.add_chunk(5, chunk_data);
        assert!(result.is_err());
    }

    #[test]
    fn test_incomplete_message_reassemble() {
        let mut incomplete = IncompleteMessage::new(2, 20);
        let chunk1 = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
        let chunk2 = vec![11, 12, 13, 14, 15, 16, 17, 18, 19, 20];

        incomplete.add_chunk(0, chunk1).unwrap();
        incomplete.add_chunk(1, chunk2).unwrap();

        let reassembled = incomplete.reassemble().unwrap();
        assert_eq!(reassembled.len(), 20);
        assert_eq!(reassembled[0], 1);
        assert_eq!(reassembled[19], 20);
    }

    #[test]
    fn test_incomplete_message_reassemble_incomplete() {
        let incomplete = IncompleteMessage::new(2, 20);
        let chunk1 = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];

        let mut incomplete = incomplete;
        incomplete.add_chunk(0, chunk1).unwrap();

        let result = incomplete.reassemble();
        assert!(result.is_err());
    }

    #[test]
    fn test_message_chunker_needs_chunking() {
        let chunker = MessageChunker::new();
        let small_data = vec![0u8; 1_000_000]; // 1 MB
        let large_data = vec![0u8; 20_000_000]; // 20 MB

        assert!(!chunker.needs_chunking(&small_data));
        assert!(chunker.needs_chunking(&large_data));
    }

    #[test]
    fn test_message_chunker_chunk_message() {
        let chunker = MessageChunker::new();
        let data = vec![0u8; 25_000_000]; // 25 MB

        let chunks = chunker.chunk_message(&data).unwrap();
        assert_eq!(chunks.len(), 3); // 10 + 10 + 5 MB

        assert_eq!(chunks[0].metadata.chunk_index, 0);
        assert_eq!(chunks[0].metadata.total_chunks, 3);
        assert_eq!(chunks[0].metadata.total_size, 25_000_000);

        assert_eq!(chunks[1].metadata.chunk_index, 1);
        assert_eq!(chunks[2].metadata.chunk_index, 2);
    }

    #[test]
    fn test_message_chunker_chunk_message_exact_multiple() {
        let chunker = MessageChunker::new();
        let data = vec![0u8; 20_971_520]; // Exactly 2 chunks (2 * 10 MB)

        let chunks = chunker.chunk_message(&data).unwrap();
        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[0].data.len(), 10_485_760);
        assert_eq!(chunks[1].data.len(), 10_485_760);
    }

    #[test]
    fn test_message_chunker_chunk_message_small() {
        let chunker = MessageChunker::new();
        let data = vec![0u8; 1_000_000]; // 1 MB

        let result = chunker.chunk_message(&data);
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_message_chunker_add_chunk() {
        let chunker = MessageChunker::new();
        let data = vec![0u8; 25_000_000];

        let chunks = chunker.chunk_message(&data).unwrap();

        // Add first chunk
        let result = chunker.add_chunk("peer1", chunks[0].clone()).await;
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());

        // Add second chunk
        let result = chunker.add_chunk("peer1", chunks[1].clone()).await;
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());

        // Add final chunk - should complete
        let result = chunker.add_chunk("peer1", chunks[2].clone()).await;
        assert!(result.is_ok());
        let reassembled = result.unwrap();
        assert!(reassembled.is_some());
        assert_eq!(reassembled.unwrap().len(), 25_000_000);
    }

    #[tokio::test]
    async fn test_message_chunker_duplicate_chunk() {
        let chunker = MessageChunker::new();
        let data = vec![0u8; 25_000_000];

        let chunks = chunker.chunk_message(&data).unwrap();

        chunker.add_chunk("peer1", chunks[0].clone()).await.unwrap();
        let result = chunker.add_chunk("peer1", chunks[0].clone()).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_message_chunker_out_of_order() {
        let chunker = MessageChunker::new();
        let data = vec![0u8; 25_000_000];

        let chunks = chunker.chunk_message(&data).unwrap();

        // Add chunks out of order
        chunker.add_chunk("peer1", chunks[2].clone()).await.unwrap();
        chunker.add_chunk("peer1", chunks[0].clone()).await.unwrap();

        // Add final chunk - should complete
        let result = chunker.add_chunk("peer1", chunks[1].clone()).await;
        assert!(result.is_ok());
        let reassembled = result.unwrap();
        assert!(reassembled.is_some());
        assert_eq!(reassembled.unwrap().len(), 25_000_000);
    }

    #[tokio::test]
    async fn test_message_chunker_multiple_peers() {
        let chunker = MessageChunker::new();
        let data = vec![0u8; 25_000_000];

        let chunks = chunker.chunk_message(&data).unwrap();
        let chunks2 = chunker.chunk_message(&data).unwrap();

        // Add chunks from peer1
        chunker.add_chunk("peer1", chunks[0].clone()).await.unwrap();
        chunker.add_chunk("peer1", chunks[1].clone()).await.unwrap();

        // Add chunks from peer2
        chunker.add_chunk("peer2", chunks2[0].clone()).await.unwrap();
        chunker.add_chunk("peer2", chunks2[1].clone()).await.unwrap();

        // Complete peer1
        let result = chunker.add_chunk("peer1", chunks[2].clone()).await;
        assert!(result.unwrap().is_some());

        // Complete peer2
        let result = chunker.add_chunk("peer2", chunks2[2].clone()).await;
        assert!(result.unwrap().is_some());
    }

    #[tokio::test]
    async fn test_message_chunker_get_incomplete_count() {
        let chunker = MessageChunker::new();
        let data = vec![0u8; 25_000_000];

        let chunks = chunker.chunk_message(&data).unwrap();

        chunker.add_chunk("peer1", chunks[0].clone()).await.unwrap();
        assert_eq!(chunker.get_incomplete_count().await, 1);

        chunker.add_chunk("peer1", chunks[1].clone()).await.unwrap();
        assert_eq!(chunker.get_incomplete_count().await, 1);

        chunker.add_chunk("peer1", chunks[2].clone()).await.unwrap();
        assert_eq!(chunker.get_incomplete_count().await, 0);
    }

    #[tokio::test]
    async fn test_message_chunker_clear_peer_messages() {
        let chunker = MessageChunker::new();
        let data = vec![0u8; 25_000_000];

        let chunks = chunker.chunk_message(&data).unwrap();

        chunker.add_chunk("peer1", chunks[0].clone()).await.unwrap();
        chunker.add_chunk("peer1", chunks[1].clone()).await.unwrap();

        assert_eq!(chunker.get_incomplete_count().await, 1);

        chunker.clear_peer_messages("peer1").await;
        assert_eq!(chunker.get_incomplete_count().await, 0);
    }

    #[tokio::test]
    async fn test_message_chunker_empty_chunk() {
        let chunker = MessageChunker::new();
        let data = vec![0u8; 25_000_000];

        let mut chunks = chunker.chunk_message(&data).unwrap();
        chunks[0].data.clear();

        let result = chunker.add_chunk("peer1", chunks[0].clone()).await;
        assert!(result.is_err());
    }
}
