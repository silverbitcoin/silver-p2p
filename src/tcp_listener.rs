//! TCP connection establishment and management

use crate::error::{P2PError, Result};
use crate::types::BackoffState;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};
use tokio::time::timeout;
use tracing::{debug, error, warn};

/// TCP connection manager for establishing and managing peer connections
pub struct TcpConnectionManager {
    /// Listen address for incoming connections
    listen_addr: SocketAddr,
    /// Connection timeout in seconds
    connection_timeout_secs: u64,
    /// TCP listener for incoming connections
    listener: Option<TcpListener>,
}

impl TcpConnectionManager {
    /// Create a new TCP connection manager
    pub fn new(listen_addr: SocketAddr, connection_timeout_secs: u64) -> Self {
        Self {
            listen_addr,
            connection_timeout_secs,
            listener: None,
        }
    }

    /// Start listening for incoming connections
    pub async fn start_listening(&mut self) -> Result<()> {
        let listener = TcpListener::bind(self.listen_addr)
            .await
            .map_err(|e| P2PError::ConnectionError(format!("Failed to bind listener: {}", e)))?;

        debug!("TCP listener started on {}", self.listen_addr);
        self.listener = Some(listener);
        Ok(())
    }

    /// Accept an incoming connection
    pub async fn accept_connection(&self) -> Result<(TcpStream, SocketAddr)> {
        if let Some(listener) = &self.listener {
            let (stream, addr) = listener.accept().await.map_err(|e| {
                P2PError::ConnectionError(format!("Failed to accept connection: {}", e))
            })?;

            debug!("Accepted incoming connection from {}", addr);
            Ok((stream, addr))
        } else {
            Err(P2PError::ConnectionError(
                "Listener not started".to_string(),
            ))
        }
    }

    /// Establish an outgoing TCP connection to a peer
    pub async fn connect_to_peer(&self, peer_addr: SocketAddr) -> Result<TcpStream> {
        let timeout_duration = Duration::from_secs(self.connection_timeout_secs);

        match timeout(timeout_duration, TcpStream::connect(peer_addr)).await {
            Ok(Ok(stream)) => {
                debug!("Successfully connected to peer at {}", peer_addr);
                Ok(stream)
            }
            Ok(Err(e)) => {
                error!("Failed to connect to peer at {}: {}", peer_addr, e);
                Err(P2PError::ConnectionError(format!(
                    "Failed to connect to {}: {}",
                    peer_addr, e
                )))
            }
            Err(_) => {
                error!("Connection timeout to peer at {}", peer_addr);
                Err(P2PError::ConnectionTimeout(format!(
                    "Connection to {} timed out after {} seconds",
                    peer_addr, self.connection_timeout_secs
                )))
            }
        }
    }

    /// Validate a TCP connection by checking if it's still alive
    pub async fn validate_connection(&self, stream: &TcpStream) -> Result<()> {
        // Get peer address to verify connection is valid
        let peer_addr = stream
            .peer_addr()
            .map_err(|e| P2PError::ConnectionError(format!("Failed to get peer address: {}", e)))?;

        debug!("Connection to {} is valid", peer_addr);
        Ok(())
    }

    /// Get the listener's local address
    pub fn local_addr(&self) -> Result<SocketAddr> {
        if let Some(listener) = &self.listener {
            listener.local_addr().map_err(|e| {
                P2PError::ConnectionError(format!("Failed to get local address: {}", e))
            })
        } else {
            Err(P2PError::ConnectionError(
                "Listener not started".to_string(),
            ))
        }
    }

    /// Close the listener
    pub async fn close(&mut self) -> Result<()> {
        self.listener = None;
        debug!("TCP listener closed");
        Ok(())
    }
}

/// Connection establishment helper with retry logic
pub struct ConnectionEstablisher {
    /// Connection timeout in seconds
    connection_timeout_secs: u64,
    /// Maximum backoff delay in seconds
    max_backoff_secs: u64,
}

impl ConnectionEstablisher {
    /// Create a new connection establisher
    pub fn new(connection_timeout_secs: u64, max_backoff_secs: u64) -> Self {
        Self {
            connection_timeout_secs,
            max_backoff_secs,
        }
    }

    /// Establish a connection with retry logic
    pub async fn establish_with_retry(
        &self,
        peer_addr: SocketAddr,
        max_attempts: u32,
    ) -> Result<TcpStream> {
        let mut backoff = BackoffState::new(self.max_backoff_secs);
        let mut attempt = 0;

        loop {
            attempt += 1;

            // Attempt connection
            let timeout_duration = Duration::from_secs(self.connection_timeout_secs);
            match timeout(timeout_duration, TcpStream::connect(peer_addr)).await {
                Ok(Ok(stream)) => {
                    debug!(
                        "Successfully connected to {} on attempt {}",
                        peer_addr, attempt
                    );
                    return Ok(stream);
                }
                Ok(Err(e)) => {
                    warn!(
                        "Connection attempt {} to {} failed: {}",
                        attempt, peer_addr, e
                    );
                }
                Err(_) => {
                    warn!("Connection attempt {} to {} timed out", attempt, peer_addr);
                }
            }

            // Check if we've exhausted retries
            if attempt >= max_attempts {
                return Err(P2PError::ConnectionError(format!(
                    "Failed to connect to {} after {} attempts",
                    peer_addr, max_attempts
                )));
            }

            // Calculate backoff and wait
            let backoff_secs = backoff.next_delay();
            debug!(
                "Retrying connection to {} in {} seconds (attempt {}/{})",
                peer_addr, backoff_secs, attempt, max_attempts
            );

            tokio::time::sleep(Duration::from_secs(backoff_secs)).await;
        }
    }

    /// Establish a single connection without retry
    pub async fn establish_single(&self, peer_addr: SocketAddr) -> Result<TcpStream> {
        let timeout_duration = Duration::from_secs(self.connection_timeout_secs);

        match timeout(timeout_duration, TcpStream::connect(peer_addr)).await {
            Ok(Ok(stream)) => {
                debug!("Successfully connected to {}", peer_addr);
                Ok(stream)
            }
            Ok(Err(e)) => {
                error!("Failed to connect to {}: {}", peer_addr, e);
                Err(P2PError::ConnectionError(format!(
                    "Failed to connect to {}: {}",
                    peer_addr, e
                )))
            }
            Err(_) => {
                error!("Connection timeout to {}", peer_addr);
                Err(P2PError::ConnectionTimeout(format!(
                    "Connection to {} timed out after {} seconds",
                    peer_addr, self.connection_timeout_secs
                )))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_tcp_connection_manager_creation() {
        let addr: SocketAddr = "127.0.0.1:9000".parse().map_err(|e| format!("Parse failed: {}", e))?;
        let manager = TcpConnectionManager::new(addr, 30);
        assert_eq!(manager.listen_addr, addr);
        assert_eq!(manager.connection_timeout_secs, 30);
    }

    #[tokio::test]
    async fn test_connection_establisher_creation() {
        let _establisher = ConnectionEstablisher::new(30, 300);
        assert_eq!(_establisher.connection_timeout_secs, 30);
        assert_eq!(_establisher.max_backoff_secs, 300);
    }

    #[tokio::test]
    async fn test_backoff_state_in_establisher() {
        let _establisher = ConnectionEstablisher::new(30, 300);
        let backoff = BackoffState::new(300);
        assert_eq!(backoff.current_delay_secs, 1);
    }

    #[tokio::test]
    async fn test_connection_timeout_duration() {
        let timeout_secs = 30u64;
        let duration = Duration::from_secs(timeout_secs);
        assert_eq!(duration.as_secs(), 30);
    }

    #[tokio::test]
    async fn test_socket_addr_parsing() {
        let addr: SocketAddr = "127.0.0.1:9000".parse().map_err(|e| format!("Parse failed: {}", e))?;
        assert_eq!(addr.port(), 9000);
    }

    #[tokio::test]
    async fn test_connection_establisher_backoff_calculation() {
        let establisher = ConnectionEstablisher::new(30, 300);
        let mut backoff = BackoffState::new(establisher.max_backoff_secs);

        let delay1 = backoff.next_delay();
        assert_eq!(delay1, 2); // First retry: 1 * 2 = 2

        let delay2 = backoff.next_delay();
        assert_eq!(delay2, 4); // Second retry: 2 * 2 = 4

        let delay3 = backoff.next_delay();
        assert_eq!(delay3, 8); // Third retry: 4 * 2 = 8
    }

    #[tokio::test]
    async fn test_backoff_max_limit() {
        let establisher = ConnectionEstablisher::new(30, 300);
        let mut backoff = BackoffState::new(establisher.max_backoff_secs);

        // Calculate backoff multiple times to reach max
        for _ in 0..15 {
            backoff.next_delay();
        }

        // Should not exceed max_backoff_secs
        assert!(backoff.current_delay_secs <= 300);
    }

    #[tokio::test]
    async fn test_connection_timeout_error() {
        let establisher = ConnectionEstablisher::new(1, 300);
        // Try to connect to an invalid address (should timeout)
        let result = establisher
            .establish_single("127.0.0.1:1".parse().map_err(|e| format!("Parse failed: {}", e))?)
            .await;

        // Should fail with timeout or connection error
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_tcp_manager_timeout_config() {
        let addr: SocketAddr = "127.0.0.1:9000".parse().map_err(|e| format!("Parse failed: {}", e))?;
        let manager = TcpConnectionManager::new(addr, 30);

        // Verify timeout is set correctly
        assert_eq!(manager.connection_timeout_secs, 30);
    }

    #[tokio::test]
    async fn test_connection_establisher_max_attempts() {
        let establisher = ConnectionEstablisher::new(1, 300);
        let max_attempts = 3u32;

        // Try to connect with max attempts
        let result = establisher
            .establish_with_retry("127.0.0.1:1".parse().map_err(|e| format!("Parse failed: {}", e))?, max_attempts)
            .await;

        // Should fail after max attempts
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_backoff_reset() {
        let mut backoff = BackoffState::new(300);
        backoff.next_delay();
        backoff.next_delay();
        assert_eq!(backoff.current_delay_secs, 4);

        backoff.reset();
        assert_eq!(backoff.current_delay_secs, 1);
        assert_eq!(backoff.failed_attempts, 0);
    }

    #[tokio::test]
    async fn test_connection_establisher_single_attempt() {
        let establisher = ConnectionEstablisher::new(1, 300);
        // Try to connect to an invalid address
        let result = establisher
            .establish_single("127.0.0.1:1".parse().map_err(|e| format!("Parse failed: {}", e))?)
            .await;

        // Should fail immediately without retry
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_tcp_listener_address_parsing() {
        let addr: SocketAddr = "0.0.0.0:9000".parse().map_err(|e| format!("Parse failed: {}", e))?;
        let manager = TcpConnectionManager::new(addr, 30);
        assert_eq!(manager.listen_addr.port(), 9000);
    }

    #[tokio::test]
    async fn test_connection_timeout_duration_calculation() {
        let timeout_secs = 30u64;
        let duration = Duration::from_secs(timeout_secs);
        let millis = duration.as_millis();
        assert_eq!(millis, 30000);
    }
}
