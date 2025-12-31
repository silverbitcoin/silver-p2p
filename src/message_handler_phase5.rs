//! P2P Message Handler - Network Integration
//!
//! Handles P2P message types, serialization, deserialization, and validation.
//!
//! # Message Types
//! - Block: Block propagation
//! - Transaction: Transaction propagation
//! - Peer: Peer discovery and management
//! - Sync: Blockchain synchronization
//!
//! # Features
//! - Message serialization/deserialization
//! - Message validation
//! - Error handling
//! - Logging and tracing

use serde::{Deserialize, Serialize};
use std::fmt;
use tracing::{debug, error, info};

/// P2P message error type
#[derive(Debug, Clone)]
pub struct MessageError {
    /// Error code
    pub code: u32,
    /// Error message
    pub message: String,
}

impl fmt::Display for MessageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Message Error {}: {}", self.code, self.message)
    }
}

impl std::error::Error for MessageError {}

/// Result type for message operations
pub type MessageResult<T> = std::result::Result<T, MessageError>;

/// Block message for P2P propagation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockMessage {
    /// Block hash (128 hex chars for SHA-512)
    pub hash: String,
    /// Block height
    pub height: u64,
    /// Block version
    pub version: u32,
    /// Previous block hash
    pub previous_hash: String,
    /// Merkle root
    pub merkle_root: String,
    /// Block timestamp
    pub timestamp: u64,
    /// Difficulty bits
    pub bits: String,
    /// Nonce
    pub nonce: u64,
    /// Transaction count
    pub tx_count: u32,
    /// Block data (serialized)
    pub data: Vec<u8>,
}

impl BlockMessage {
    /// Create a new block message
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        hash: String,
        height: u64,
        version: u32,
        previous_hash: String,
        merkle_root: String,
        timestamp: u64,
        bits: String,
        nonce: u64,
        tx_count: u32,
        data: Vec<u8>,
    ) -> Self {
        Self {
            hash,
            height,
            version,
            previous_hash,
            merkle_root,
            timestamp,
            bits,
            nonce,
            tx_count,
            data,
        }
    }

    /// Validate block message
    pub fn validate(&self) -> MessageResult<()> {
        debug!("Validating block message: {}", self.hash);

        // Validate hash format
        if self.hash.len() != 128 || !self.hash.chars().all(|c| c.is_ascii_hexdigit()) {
            error!("Invalid block hash format");
            return Err(MessageError {
                code: 1001,
                message: "Invalid block hash format".to_string(),
            });
        }

        // Validate previous hash format
        if self.previous_hash.len() != 128
            || !self.previous_hash.chars().all(|c| c.is_ascii_hexdigit())
        {
            error!("Invalid previous hash format");
            return Err(MessageError {
                code: 1002,
                message: "Invalid previous hash format".to_string(),
            });
        }

        // Validate merkle root format
        if self.merkle_root.len() != 128 || !self.merkle_root.chars().all(|c| c.is_ascii_hexdigit())
        {
            error!("Invalid merkle root format");
            return Err(MessageError {
                code: 1003,
                message: "Invalid merkle root format".to_string(),
            });
        }

        // Validate timestamp is reasonable
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        if self.timestamp > now + 7200 {
            // Allow 2 hours in the future
            error!("Block timestamp too far in the future");
            return Err(MessageError {
                code: 1004,
                message: "Block timestamp too far in the future".to_string(),
            });
        }

        // Validate transaction count
        if self.tx_count == 0 {
            error!("Block must contain at least one transaction");
            return Err(MessageError {
                code: 1005,
                message: "Block must contain at least one transaction".to_string(),
            });
        }

        info!("Block message validation passed: {}", self.hash);
        Ok(())
    }

    /// Serialize block message
    pub fn serialize(&self) -> MessageResult<Vec<u8>> {
        debug!("Serializing block message: {}", self.hash);

        serde_json::to_vec(self).map_err(|e| MessageError {
            code: 2001,
            message: format!("Serialization failed: {}", e),
        })
    }

    /// Deserialize block message
    pub fn deserialize(data: &[u8]) -> MessageResult<Self> {
        debug!("Deserializing block message");

        serde_json::from_slice(data).map_err(|e| MessageError {
            code: 2002,
            message: format!("Deserialization failed: {}", e),
        })
    }
}

/// Transaction message for P2P propagation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionMessage {
    /// Transaction ID (128 hex chars for SHA-512)
    pub txid: String,
    /// Transaction version
    pub version: u32,
    /// Input count
    pub input_count: u32,
    /// Output count
    pub output_count: u32,
    /// Lock time
    pub lock_time: u32,
    /// Transaction data (serialized)
    pub data: Vec<u8>,
    /// Fee in MIST
    pub fee: u128,
}

impl TransactionMessage {
    /// Create a new transaction message
    pub fn new(
        txid: String,
        version: u32,
        input_count: u32,
        output_count: u32,
        lock_time: u32,
        data: Vec<u8>,
        fee: u128,
    ) -> Self {
        Self {
            txid,
            version,
            input_count,
            output_count,
            lock_time,
            data,
            fee,
        }
    }

    /// Validate transaction message
    pub fn validate(&self) -> MessageResult<()> {
        debug!("Validating transaction message: {}", self.txid);

        // Validate TXID format
        if self.txid.len() != 128 || !self.txid.chars().all(|c| c.is_ascii_hexdigit()) {
            error!("Invalid transaction ID format");
            return Err(MessageError {
                code: 1101,
                message: "Invalid transaction ID format".to_string(),
            });
        }

        // Validate input/output counts
        if self.input_count == 0 {
            error!("Transaction must have at least one input");
            return Err(MessageError {
                code: 1102,
                message: "Transaction must have at least one input".to_string(),
            });
        }

        if self.output_count == 0 {
            error!("Transaction must have at least one output");
            return Err(MessageError {
                code: 1103,
                message: "Transaction must have at least one output".to_string(),
            });
        }

        // Validate fee is reasonable
        if self.fee > 1_000_000_000_000 {
            // Max 10,000 SLVR
            error!("Transaction fee too high");
            return Err(MessageError {
                code: 1104,
                message: "Transaction fee too high".to_string(),
            });
        }

        info!("Transaction message validation passed: {}", self.txid);
        Ok(())
    }

    /// Serialize transaction message
    pub fn serialize(&self) -> MessageResult<Vec<u8>> {
        debug!("Serializing transaction message: {}", self.txid);

        serde_json::to_vec(self).map_err(|e| MessageError {
            code: 2101,
            message: format!("Serialization failed: {}", e),
        })
    }

    /// Deserialize transaction message
    pub fn deserialize(data: &[u8]) -> MessageResult<Self> {
        debug!("Deserializing transaction message");

        serde_json::from_slice(data).map_err(|e| MessageError {
            code: 2102,
            message: format!("Deserialization failed: {}", e),
        })
    }
}

/// Peer message for peer discovery
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerMessage {
    /// Peer ID
    pub peer_id: String,
    /// Peer address (IP:port)
    pub address: String,
    /// Peer version
    pub version: String,
    /// Peer capabilities
    pub capabilities: Vec<String>,
    /// Timestamp
    pub timestamp: u64,
}

impl PeerMessage {
    /// Create a new peer message
    pub fn new(
        peer_id: String,
        address: String,
        version: String,
        capabilities: Vec<String>,
    ) -> Self {
        Self {
            peer_id,
            address,
            version,
            capabilities,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        }
    }

    /// Validate peer message
    pub fn validate(&self) -> MessageResult<()> {
        debug!("Validating peer message: {}", self.peer_id);

        // Validate peer ID format
        if self.peer_id.is_empty() || self.peer_id.len() > 128 {
            error!("Invalid peer ID");
            return Err(MessageError {
                code: 1201,
                message: "Invalid peer ID".to_string(),
            });
        }

        // Validate address format
        if !self.address.contains(':') {
            error!("Invalid peer address format");
            return Err(MessageError {
                code: 1202,
                message: "Invalid peer address format".to_string(),
            });
        }

        // Validate version
        if self.version.is_empty() {
            error!("Invalid peer version");
            return Err(MessageError {
                code: 1203,
                message: "Invalid peer version".to_string(),
            });
        }

        info!("Peer message validation passed: {}", self.peer_id);
        Ok(())
    }

    /// Serialize peer message
    pub fn serialize(&self) -> MessageResult<Vec<u8>> {
        debug!("Serializing peer message: {}", self.peer_id);

        serde_json::to_vec(self).map_err(|e| MessageError {
            code: 2201,
            message: format!("Serialization failed: {}", e),
        })
    }

    /// Deserialize peer message
    pub fn deserialize(data: &[u8]) -> MessageResult<Self> {
        debug!("Deserializing peer message");

        serde_json::from_slice(data).map_err(|e| MessageError {
            code: 2202,
            message: format!("Deserialization failed: {}", e),
        })
    }
}

/// Sync message for blockchain synchronization
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncMessage {
    /// Sync request ID
    pub request_id: String,
    /// Start block height
    pub start_height: u64,
    /// End block height
    pub end_height: u64,
    /// Requested block hashes
    pub block_hashes: Vec<String>,
    /// Sync type (full, partial, headers)
    pub sync_type: String,
}

impl SyncMessage {
    /// Create a new sync message
    pub fn new(
        request_id: String,
        start_height: u64,
        end_height: u64,
        block_hashes: Vec<String>,
        sync_type: String,
    ) -> Self {
        Self {
            request_id,
            start_height,
            end_height,
            block_hashes,
            sync_type,
        }
    }

    /// Validate sync message
    pub fn validate(&self) -> MessageResult<()> {
        debug!("Validating sync message: {}", self.request_id);

        // Validate request ID
        if self.request_id.is_empty() {
            error!("Invalid request ID");
            return Err(MessageError {
                code: 1301,
                message: "Invalid request ID".to_string(),
            });
        }

        // Validate height range
        if self.start_height > self.end_height {
            error!("Invalid height range");
            return Err(MessageError {
                code: 1302,
                message: "Invalid height range".to_string(),
            });
        }

        // Validate sync type
        let valid_types = ["full", "partial", "headers"];
        if !valid_types.contains(&self.sync_type.as_str()) {
            error!("Invalid sync type");
            return Err(MessageError {
                code: 1303,
                message: "Invalid sync type".to_string(),
            });
        }

        // Validate block hashes format
        for hash in &self.block_hashes {
            if hash.len() != 128 || !hash.chars().all(|c| c.is_ascii_hexdigit()) {
                error!("Invalid block hash format");
                return Err(MessageError {
                    code: 1304,
                    message: "Invalid block hash format".to_string(),
                });
            }
        }

        info!("Sync message validation passed: {}", self.request_id);
        Ok(())
    }

    /// Serialize sync message
    pub fn serialize(&self) -> MessageResult<Vec<u8>> {
        debug!("Serializing sync message: {}", self.request_id);

        serde_json::to_vec(self).map_err(|e| MessageError {
            code: 2301,
            message: format!("Serialization failed: {}", e),
        })
    }

    /// Deserialize sync message
    pub fn deserialize(data: &[u8]) -> MessageResult<Self> {
        debug!("Deserializing sync message");

        serde_json::from_slice(data).map_err(|e| MessageError {
            code: 2302,
            message: format!("Deserialization failed: {}", e),
        })
    }
}

/// Generic P2P message envelope
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessageEnvelope {
    /// Message type
    pub message_type: String,
    /// Message ID
    pub message_id: String,
    /// Sender peer ID
    pub sender: String,
    /// Receiver peer ID (optional for broadcast)
    pub receiver: Option<String>,
    /// Message payload (serialized)
    pub payload: Vec<u8>,
    /// Timestamp
    pub timestamp: u64,
}

impl MessageEnvelope {
    /// Create a new message envelope
    pub fn new(
        message_type: String,
        message_id: String,
        sender: String,
        receiver: Option<String>,
        payload: Vec<u8>,
    ) -> Self {
        Self {
            message_type,
            message_id,
            sender,
            receiver,
            payload,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        }
    }

    /// Validate message envelope
    pub fn validate(&self) -> MessageResult<()> {
        debug!("Validating message envelope: {}", self.message_id);

        // Validate message type
        let valid_types = ["block", "transaction", "peer", "sync"];
        if !valid_types.contains(&self.message_type.as_str()) {
            error!("Invalid message type");
            return Err(MessageError {
                code: 3001,
                message: "Invalid message type".to_string(),
            });
        }

        // Validate message ID
        if self.message_id.is_empty() {
            error!("Invalid message ID");
            return Err(MessageError {
                code: 3002,
                message: "Invalid message ID".to_string(),
            });
        }

        // Validate sender
        if self.sender.is_empty() {
            error!("Invalid sender");
            return Err(MessageError {
                code: 3003,
                message: "Invalid sender".to_string(),
            });
        }

        // Validate payload
        if self.payload.is_empty() {
            error!("Empty payload");
            return Err(MessageError {
                code: 3004,
                message: "Empty payload".to_string(),
            });
        }

        info!("Message envelope validation passed: {}", self.message_id);
        Ok(())
    }

    /// Serialize message envelope
    pub fn serialize(&self) -> MessageResult<Vec<u8>> {
        debug!("Serializing message envelope: {}", self.message_id);

        serde_json::to_vec(self).map_err(|e| MessageError {
            code: 3101,
            message: format!("Serialization failed: {}", e),
        })
    }

    /// Deserialize message envelope
    pub fn deserialize(data: &[u8]) -> MessageResult<Self> {
        debug!("Deserializing message envelope");

        serde_json::from_slice(data).map_err(|e| MessageError {
            code: 3102,
            message: format!("Deserialization failed: {}", e),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_block_message_validation() {
        let block = BlockMessage::new(
            "a".repeat(128),
            100,
            1,
            "b".repeat(128),
            "c".repeat(128),
            1234567890,
            "207fffff".to_string(),
            12345,
            1,
            vec![],
        );

        assert!(block.validate().is_ok());
    }

    #[test]
    fn test_block_message_invalid_hash() {
        let block = BlockMessage::new(
            "invalid".to_string(),
            100,
            1,
            "b".repeat(128),
            "c".repeat(128),
            1234567890,
            "207fffff".to_string(),
            12345,
            1,
            vec![],
        );

        assert!(block.validate().is_err());
    }

    #[test]
    fn test_transaction_message_validation() {
        let tx = TransactionMessage::new("a".repeat(128), 1, 1, 1, 0, vec![], 1000);

        assert!(tx.validate().is_ok());
    }

    #[test]
    fn test_peer_message_validation() {
        let peer = PeerMessage::new(
            "peer123".to_string(),
            "127.0.0.1:8333".to_string(),
            "1.0.0".to_string(),
            vec!["full_node".to_string()],
        );

        assert!(peer.validate().is_ok());
    }

    #[test]
    fn test_sync_message_validation() {
        let sync = SyncMessage::new(
            "sync123".to_string(),
            0,
            100,
            vec!["a".repeat(128)],
            "full".to_string(),
        );

        assert!(sync.validate().is_ok());
    }

    #[test]
    fn test_message_envelope_validation() {
        let envelope = MessageEnvelope::new(
            "block".to_string(),
            "msg123".to_string(),
            "peer1".to_string(),
            Some("peer2".to_string()),
            vec![1, 2, 3],
        );

        assert!(envelope.validate().is_ok());
    }

    #[test]
    fn test_block_message_serialization() {
        let block = BlockMessage::new(
            "a".repeat(128),
            100,
            1,
            "b".repeat(128),
            "c".repeat(128),
            1234567890,
            "207fffff".to_string(),
            12345,
            1,
            vec![],
        );

        let serialized = block.serialize().unwrap();
        let deserialized = BlockMessage::deserialize(&serialized).unwrap();

        assert_eq!(block.hash, deserialized.hash);
        assert_eq!(block.height, deserialized.height);
    }
}
