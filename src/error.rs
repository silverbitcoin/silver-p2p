//! Error types for P2P network operations

use thiserror::Error;

/// P2P network error types
#[derive(Error, Debug)]
pub enum P2PError {
    /// Connection error
    #[error("Connection error: {0}")]
    ConnectionError(String),

    /// Connection timeout
    #[error("Connection timeout: {0}")]
    ConnectionTimeout(String),

    /// Connection refused
    #[error("Connection refused: {0}")]
    ConnectionRefused(String),

    /// Peer not found
    #[error("Peer not found: {0}")]
    PeerNotFound(String),

    /// Peer already connected
    #[error("Peer already connected: {0}")]
    PeerAlreadyConnected(String),

    /// Invalid peer address
    #[error("Invalid peer address: {0}")]
    InvalidPeerAddress(String),

    /// Message serialization error
    #[error("Message serialization error: {0}")]
    SerializationError(String),

    /// Message deserialization error
    #[error("Message deserialization error: {0}")]
    DeserializationError(String),

    /// Message too large
    #[error("Message too large: {0} bytes (max: {1})")]
    MessageTooLarge(usize, usize),

    /// Invalid message format
    #[error("Invalid message format: {0}")]
    InvalidMessageFormat(String),

    /// Handshake failed
    #[error("Handshake failed: {0}")]
    HandshakeFailed(String),

    /// Invalid handshake
    #[error("Invalid handshake: {0}")]
    InvalidHandshake(String),

    /// Incompatible peer
    #[error("Incompatible peer: {0}")]
    IncompatiblePeer(String),

    /// Rate limit exceeded
    #[error("Rate limit exceeded for peer: {0}")]
    RateLimitExceeded(String),

    /// Connection limit reached
    #[error("Connection limit reached: {0}/{1}")]
    ConnectionLimitReached(usize, usize),

    /// Peer unhealthy
    #[error("Peer unhealthy: {0}")]
    PeerUnhealthy(String),

    /// Network error
    #[error("Network error: {0}")]
    NetworkError(String),

    /// IO error
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),

    /// Configuration error
    #[error("Configuration error: {0}")]
    ConfigError(String),

    /// Shutdown error
    #[error("Shutdown error: {0}")]
    ShutdownError(String),

    /// Internal error
    #[error("Internal error: {0}")]
    InternalError(String),

    /// Unknown error
    #[error("Unknown error: {0}")]
    Unknown(String),
}

/// Result type for P2P operations
pub type Result<T> = std::result::Result<T, P2PError>;

impl P2PError {
    /// Check if error is recoverable
    pub fn is_recoverable(&self) -> bool {
        matches!(
            self,
            P2PError::ConnectionError(_)
                | P2PError::ConnectionTimeout(_)
                | P2PError::ConnectionRefused(_)
                | P2PError::NetworkError(_)
                | P2PError::PeerUnhealthy(_)
                | P2PError::RateLimitExceeded(_)
        )
    }

    /// Check if error is fatal
    pub fn is_fatal(&self) -> bool {
        matches!(self, P2PError::ConfigError(_) | P2PError::InternalError(_))
    }
}
