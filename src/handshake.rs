//! Connection handshake protocol for peer verification and compatibility checking

use crate::error::{P2PError, Result};
use crate::types::NodeRole;
use serde::{Deserialize, Serialize};
use serde_json;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::timeout;
use tracing::{debug, error, warn};

/// Handshake protocol version
const HANDSHAKE_VERSION: u32 = 1;

/// Handshake timeout in seconds
const HANDSHAKE_TIMEOUT_SECS: u64 = 10;

/// Handshake message sent during connection establishment
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct HandshakeMessage {
    /// Protocol version
    pub version: u32,
    /// Peer ID (public key hash)
    pub peer_id: String,
    /// Node role
    pub node_role: NodeRole,
    /// Supported features (bitmask)
    pub features: u32,
    /// Timestamp of handshake
    pub timestamp: u64,
}

impl HandshakeMessage {
    /// Create a new handshake message
    pub fn new(peer_id: String, node_role: NodeRole) -> Self {
        Self {
            version: HANDSHAKE_VERSION,
            peer_id,
            node_role,
            features: 0,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
        }
    }

    /// Serialize handshake message to bytes
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        serde_json::to_vec(self).map_err(|e| {
            P2PError::SerializationError(format!("Failed to serialize handshake: {}", e))
        })
    }

    /// Deserialize handshake message from bytes
    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        serde_json::from_slice(data).map_err(|e| {
            P2PError::DeserializationError(format!("Failed to deserialize handshake: {}", e))
        })
    }
}

/// Handshake result containing verified peer information
#[derive(Clone, Debug)]
pub struct HandshakeResult {
    /// Peer ID from handshake
    pub peer_id: String,
    /// Node role
    pub node_role: NodeRole,
    /// Whether handshake was successful
    pub success: bool,
    /// Error message if handshake failed
    pub error: Option<String>,
}

/// Handshake protocol handler
pub struct HandshakeHandler {
    /// Local peer ID
    local_peer_id: String,
    /// Local node role
    local_node_role: NodeRole,
    /// Handshake timeout in seconds
    timeout_secs: u64,
}

impl HandshakeHandler {
    /// Create a new handshake handler
    pub fn new(local_peer_id: String, local_node_role: NodeRole) -> Self {
        Self {
            local_peer_id,
            local_node_role,
            timeout_secs: HANDSHAKE_TIMEOUT_SECS,
        }
    }

    /// Perform handshake as initiator (client side)
    pub async fn initiate_handshake(&self, stream: &mut TcpStream) -> Result<HandshakeResult> {
        // Create handshake message
        let handshake = HandshakeMessage::new(self.local_peer_id.clone(), self.local_node_role);

        // Serialize and send
        let data = handshake.to_bytes()?;
        let len = data.len() as u32;

        // Send length prefix (4 bytes) + data
        let mut message = len.to_le_bytes().to_vec();
        message.extend_from_slice(&data);

        // Send with timeout
        let timeout_duration = Duration::from_secs(self.timeout_secs);
        match timeout(timeout_duration, stream.write_all(&message)).await {
            Ok(Ok(())) => {
                debug!("Sent handshake message to peer");
            }
            Ok(Err(e)) => {
                error!("Failed to send handshake: {}", e);
                return Err(P2PError::ConnectionError(format!(
                    "Failed to send handshake: {}",
                    e
                )));
            }
            Err(_) => {
                error!("Handshake send timeout");
                return Err(P2PError::ConnectionTimeout(
                    "Handshake send timeout".to_string(),
                ));
            }
        }

        // Receive handshake response
        self.receive_handshake(stream).await
    }

    /// Perform handshake as responder (server side)
    pub async fn respond_to_handshake(&self, stream: &mut TcpStream) -> Result<HandshakeResult> {
        // Receive handshake from peer
        let peer_handshake = self.receive_handshake_raw(stream).await?;

        // Verify peer handshake
        self.verify_handshake(&peer_handshake)?;

        // Create response handshake
        let response = HandshakeMessage::new(self.local_peer_id.clone(), self.local_node_role);

        // Serialize and send response
        let data = response.to_bytes()?;
        let len = data.len() as u32;

        let mut message = len.to_le_bytes().to_vec();
        message.extend_from_slice(&data);

        // Send with timeout
        let timeout_duration = Duration::from_secs(self.timeout_secs);
        match timeout(timeout_duration, stream.write_all(&message)).await {
            Ok(Ok(())) => {
                debug!("Sent handshake response to peer");
            }
            Ok(Err(e)) => {
                error!("Failed to send handshake response: {}", e);
                return Err(P2PError::ConnectionError(format!(
                    "Failed to send handshake response: {}",
                    e
                )));
            }
            Err(_) => {
                error!("Handshake response send timeout");
                return Err(P2PError::ConnectionTimeout(
                    "Handshake response send timeout".to_string(),
                ));
            }
        }

        Ok(HandshakeResult {
            peer_id: peer_handshake.peer_id,
            node_role: peer_handshake.node_role,
            success: true,
            error: None,
        })
    }

    /// Receive handshake message from peer
    async fn receive_handshake(&self, stream: &mut TcpStream) -> Result<HandshakeResult> {
        let handshake = self.receive_handshake_raw(stream).await?;

        // Verify handshake
        self.verify_handshake(&handshake)?;

        Ok(HandshakeResult {
            peer_id: handshake.peer_id,
            node_role: handshake.node_role,
            success: true,
            error: None,
        })
    }

    /// Receive raw handshake message
    async fn receive_handshake_raw(&self, stream: &mut TcpStream) -> Result<HandshakeMessage> {
        let timeout_duration = Duration::from_secs(self.timeout_secs);

        // Read length prefix (4 bytes)
        let mut len_bytes = [0u8; 4];
        match timeout(timeout_duration, stream.read_exact(&mut len_bytes)).await {
            Ok(Ok(_)) => {}
            Ok(Err(e)) => {
                error!("Failed to read handshake length: {}", e);
                return Err(P2PError::ConnectionError(format!(
                    "Failed to read handshake length: {}",
                    e
                )));
            }
            Err(_) => {
                error!("Handshake receive timeout");
                return Err(P2PError::ConnectionTimeout(
                    "Handshake receive timeout".to_string(),
                ));
            }
        }

        let len = u32::from_le_bytes(len_bytes) as usize;

        // Validate message length (max 10KB)
        if len > 10240 {
            error!("Handshake message too large: {} bytes", len);
            return Err(P2PError::InvalidMessageFormat(
                "Handshake message too large".to_string(),
            ));
        }

        // Read message data
        let mut data = vec![0u8; len];
        match timeout(timeout_duration, stream.read_exact(&mut data)).await {
            Ok(Ok(_)) => {}
            Ok(Err(e)) => {
                error!("Failed to read handshake data: {}", e);
                return Err(P2PError::ConnectionError(format!(
                    "Failed to read handshake data: {}",
                    e
                )));
            }
            Err(_) => {
                error!("Handshake data receive timeout");
                return Err(P2PError::ConnectionTimeout(
                    "Handshake data receive timeout".to_string(),
                ));
            }
        }

        // Deserialize
        HandshakeMessage::from_bytes(&data)
    }

    /// Verify handshake message
    fn verify_handshake(&self, handshake: &HandshakeMessage) -> Result<()> {
        // Check version compatibility
        if handshake.version != HANDSHAKE_VERSION {
            warn!(
                "Incompatible handshake version: {} (expected {})",
                handshake.version, HANDSHAKE_VERSION
            );
            return Err(P2PError::IncompatiblePeer(format!(
                "Incompatible protocol version: {}",
                handshake.version
            )));
        }

        // Verify peer ID is not empty
        if handshake.peer_id.is_empty() {
            error!("Peer ID is empty");
            return Err(P2PError::InvalidHandshake("Peer ID is empty".to_string()));
        }

        // Verify peer ID is not same as local
        if handshake.peer_id == self.local_peer_id {
            error!("Peer ID matches local peer ID");
            return Err(P2PError::InvalidHandshake(
                "Peer ID matches local peer ID".to_string(),
            ));
        }

        // Verify timestamp is reasonable (within 1 hour)
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        if handshake.timestamp > now + 3600 || handshake.timestamp + 3600 < now {
            warn!(
                "Handshake timestamp is unreasonable: {}",
                handshake.timestamp
            );
            return Err(P2PError::InvalidHandshake(
                "Handshake timestamp is unreasonable".to_string(),
            ));
        }

        debug!(
            "Handshake verified: peer_id={}, role={:?}",
            handshake.peer_id, handshake.node_role
        );

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_handshake_message_creation() {
        let msg = HandshakeMessage::new("peer1".to_string(), NodeRole::Validator);
        assert_eq!(msg.peer_id, "peer1");
        assert_eq!(msg.node_role, NodeRole::Validator);
        assert_eq!(msg.version, HANDSHAKE_VERSION);
    }

    #[test]
    fn test_handshake_message_serialization() {
        let msg = HandshakeMessage::new("peer1".to_string(), NodeRole::Validator);
        let bytes = msg.to_bytes().unwrap();
        assert!(!bytes.is_empty());
    }

    #[test]
    fn test_handshake_message_deserialization() {
        let msg = HandshakeMessage::new("peer1".to_string(), NodeRole::Validator);
        let bytes = msg.to_bytes().unwrap();
        let deserialized = HandshakeMessage::from_bytes(&bytes).unwrap();
        assert_eq!(deserialized.peer_id, msg.peer_id);
        assert_eq!(deserialized.node_role, msg.node_role);
    }

    #[test]
    fn test_handshake_message_round_trip() {
        let original = HandshakeMessage::new("peer1".to_string(), NodeRole::RPC);
        let bytes = original.to_bytes().unwrap();
        let restored = HandshakeMessage::from_bytes(&bytes).unwrap();
        assert_eq!(original, restored);
    }

    #[test]
    fn test_handshake_handler_creation() {
        let handler = HandshakeHandler::new("local_peer".to_string(), NodeRole::Validator);
        assert_eq!(handler.local_peer_id, "local_peer");
        assert_eq!(handler.local_node_role, NodeRole::Validator);
        assert_eq!(handler.timeout_secs, HANDSHAKE_TIMEOUT_SECS);
    }

    #[test]
    fn test_handshake_verification_version_mismatch() {
        let handler = HandshakeHandler::new("local".to_string(), NodeRole::Validator);
        let mut msg = HandshakeMessage::new("peer1".to_string(), NodeRole::Validator);
        msg.version = 999;

        let result = handler.verify_handshake(&msg);
        assert!(result.is_err());
    }

    #[test]
    fn test_handshake_verification_empty_peer_id() {
        let handler = HandshakeHandler::new("local".to_string(), NodeRole::Validator);
        let mut msg = HandshakeMessage::new("peer1".to_string(), NodeRole::Validator);
        msg.peer_id = String::new();

        let result = handler.verify_handshake(&msg);
        assert!(result.is_err());
    }

    #[test]
    fn test_handshake_verification_same_peer_id() {
        let handler = HandshakeHandler::new("peer1".to_string(), NodeRole::Validator);
        let msg = HandshakeMessage::new("peer1".to_string(), NodeRole::Validator);

        let result = handler.verify_handshake(&msg);
        assert!(result.is_err());
    }

    #[test]
    fn test_handshake_verification_valid() {
        let handler = HandshakeHandler::new("local".to_string(), NodeRole::Validator);
        let msg = HandshakeMessage::new("peer1".to_string(), NodeRole::Validator);

        let result = handler.verify_handshake(&msg);
        assert!(result.is_ok());
    }

    #[test]
    fn test_handshake_result_creation() {
        let result = HandshakeResult {
            peer_id: "peer1".to_string(),
            node_role: NodeRole::Validator,
            success: true,
            error: None,
        };
        assert_eq!(result.peer_id, "peer1");
        assert!(result.success);
        assert!(result.error.is_none());
    }

    #[test]
    fn test_handshake_message_different_roles() {
        let msg1 = HandshakeMessage::new("peer1".to_string(), NodeRole::Validator);
        let msg2 = HandshakeMessage::new("peer1".to_string(), NodeRole::RPC);
        assert_ne!(msg1.node_role, msg2.node_role);
    }

    #[test]
    fn test_handshake_message_timestamp() {
        let msg = HandshakeMessage::new("peer1".to_string(), NodeRole::Validator);
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        assert!(msg.timestamp <= now + 1); // Allow 1 second tolerance
    }

    #[test]
    fn test_handshake_verification_future_timestamp() {
        let handler = HandshakeHandler::new("local".to_string(), NodeRole::Validator);
        let mut msg = HandshakeMessage::new("peer1".to_string(), NodeRole::Validator);
        msg.timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + 7200; // 2 hours in future

        let result = handler.verify_handshake(&msg);
        assert!(result.is_err());
    }

    #[test]
    fn test_handshake_verification_old_timestamp() {
        let handler = HandshakeHandler::new("local".to_string(), NodeRole::Validator);
        let mut msg = HandshakeMessage::new("peer1".to_string(), NodeRole::Validator);
        msg.timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
            - 7200; // 2 hours in past

        let result = handler.verify_handshake(&msg);
        assert!(result.is_err());
    }

    #[test]
    fn test_handshake_message_serialization_size() {
        let msg = HandshakeMessage::new("peer1".to_string(), NodeRole::Validator);
        let bytes = msg.to_bytes().unwrap();
        // Should be reasonably small (less than 1KB)
        assert!(bytes.len() < 1024);
    }
}
