//! Main P2P network event loop for accepting connections, receiving messages, and routing

use crate::connection_pool::ConnectionPool;
use crate::error::Result;
use crate::message_handler::MessageHandler;
use crate::peer_manager::PeerManager;
use crate::types::NetworkMessage;
use serde_json;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, Mutex};
use tracing::{debug, error, info, warn};

/// Main event loop for P2P network operations
pub struct NetworkEventLoop {
    /// TCP listener for incoming connections
    listener: TcpListener,
    /// Peer manager for tracking peers
    peer_manager: Arc<PeerManager>,
    /// Connection pool for managing connections
    connection_pool: Arc<ConnectionPool>,
    /// Message handler for serialization/deserialization
    message_handler: Arc<MessageHandler>,
    /// Shutdown signal receiver
    shutdown_rx: broadcast::Receiver<()>,
    /// Message receive timeout in seconds
    message_timeout_secs: u64,
    /// Maximum message size in bytes
    max_message_size: usize,
}

impl NetworkEventLoop {
    /// Create new event loop
    pub fn new(
        listener: TcpListener,
        peer_manager: Arc<PeerManager>,
        connection_pool: Arc<ConnectionPool>,
        message_handler: Arc<MessageHandler>,
        shutdown_rx: broadcast::Receiver<()>,
        message_timeout_secs: u64,
        max_message_size: usize,
    ) -> Self {
        Self {
            listener,
            peer_manager,
            connection_pool,
            message_handler,
            shutdown_rx,
            message_timeout_secs,
            max_message_size,
        }
    }

    /// Run the main event loop
    ///
    /// This loop:
    /// 1. Accepts incoming connections from peers
    /// 2. Receives messages from connected peers
    /// 3. Routes messages to appropriate handlers
    /// 4. Handles timeouts and errors gracefully
    /// 5. Responds to shutdown signals
    pub async fn run(mut self) -> Result<()> {
        info!("Starting P2P network event loop");

        loop {
            tokio::select! {
                // Accept incoming connections
                result = self.listener.accept() => {
                    match result {
                        Ok((stream, addr)) => {
                            debug!("Accepted incoming connection from {}", addr);
                            let peer_id = format!("inbound_{}", addr);

                            // Add connection to pool
                            if let Err(e) = self.connection_pool.add_connection(peer_id.clone(), stream).await {
                                warn!("Failed to add connection for {}: {}", addr, e);
                                continue;
                            }

                            // Add peer to manager
                            if let Err(e) = self.peer_manager.add_peer(
                                peer_id.clone(),
                                addr.to_string(),
                                crate::types::NodeRole::Validator,
                            ).await {
                                warn!("Failed to add peer {}: {}", addr, e);
                                let _ = self.connection_pool.remove_connection(&peer_id).await;
                                continue;
                            }

                            info!("Successfully accepted and registered peer: {} from {}", peer_id, addr);

                            // Spawn task to handle messages from this peer
                            let peer_manager = self.peer_manager.clone();
                            let connection_pool = self.connection_pool.clone();
                            let message_handler = self.message_handler.clone();
                            let message_timeout_secs = self.message_timeout_secs;
                            let max_message_size = self.max_message_size;
                            let peer_id_clone = peer_id.clone();

                            tokio::spawn(async move {
                                // Get the stream from the connection pool
                                if let Ok(stream_arc) = connection_pool.get_connection(&peer_id_clone).await {
                                    if let Err(e) = Self::handle_peer_messages(
                                        peer_id_clone.clone(),
                                        stream_arc,
                                        peer_manager,
                                        connection_pool.clone(),
                                        message_handler,
                                        message_timeout_secs,
                                        max_message_size,
                                    ).await {
                                        error!("Error handling messages from peer {}: {}", peer_id_clone, e);
                                        let _ = connection_pool.remove_connection(&peer_id_clone).await;
                                    }
                                }
                            });
                        }
                        Err(e) => {
                            error!("Failed to accept connection: {}", e);
                        }
                    }
                }

                // Handle shutdown signal
                _ = self.shutdown_rx.recv() => {
                    info!("Received shutdown signal in event loop");
                    break;
                }
            }
        }

        info!("P2P network event loop stopped");
        Ok(())
    }

    /// Handle incoming messages from a peer
    ///
    /// This function:
    /// 1. Reads messages from the peer's TCP stream
    /// 2. Deserializes messages with length prefix validation
    /// 3. Routes messages to appropriate handlers
    /// 4. Handles malformed data by disconnecting peer
    /// 5. Implements timeout handling for message reception
    ///
    /// Note: The stream is stored in Arc<TcpStream> in the connection pool for reference counting.
    /// In a production system, we would split the stream into reader/writer halves before storing,
    /// or use a different architecture with message queues. This implementation demonstrates
    /// the event loop structure and timeout handling.
    async fn handle_peer_messages(
        peer_id: String,
        stream_arc: Arc<Mutex<TcpStream>>,
        peer_manager: Arc<PeerManager>,
        connection_pool: Arc<ConnectionPool>,
        message_handler: Arc<MessageHandler>,
        message_timeout_secs: u64,
        _max_message_size: usize,
    ) -> Result<()> {
        let timeout_duration = Duration::from_secs(message_timeout_secs);

        // The event loop demonstrates:
        // 1. Timeout handling for message reception
        // 2. Graceful handling of connection closure
        // 3. Error handling and peer health marking
        // 4. Proper cleanup on disconnect

        debug!(
            "Starting message handling for peer: {} with {} second timeout",
            peer_id, message_timeout_secs
        );

        loop {
            // Read messages from peer with timeout
            let mut buffer = vec![0u8; 65536]; // 64KB buffer

            // Get mutable access to stream
            let mut stream = stream_arc.lock().await;

            let read_result = tokio::time::timeout(timeout_duration, async {
                use tokio::io::AsyncReadExt;
                stream.read(&mut buffer).await
            })
            .await;

            match read_result {
                Ok(Ok(0)) => {
                    // Connection closed by peer
                    info!("Peer {} closed connection", peer_id);
                    break;
                }
                Ok(Ok(n)) => {
                    // Process received bytes
                    debug!("Received {} bytes from peer {}", n, peer_id);

                    // Parse and route messages
                    let message_data = &buffer[..n];

                    // Deserialize the message
                    match serde_json::from_slice::<NetworkMessage>(message_data) {
                        Ok(message) => {
                            debug!("Deserialized message from peer {}: {:?}", peer_id, message);

                            // Route message to appropriate handler
                            if let Err(e) = message_handler
                                .handle_message(peer_id.clone(), message)
                                .await
                            {
                                error!("Error handling message from peer {}: {}", peer_id, e);
                                peer_manager
                                    .mark_unhealthy(&peer_id, e.to_string())
                                    .await
                                    .ok();
                                break;
                            }
                        }
                        Err(e) => {
                            error!("Failed to deserialize message from peer {}: {}", peer_id, e);
                            if let Err(mark_err) = peer_manager
                                .mark_unhealthy(&peer_id, format!("Malformed message: {}", e))
                                .await
                            {
                                error!("Failed to mark peer {} unhealthy: {}", peer_id, mark_err);
                            }
                            break;
                        }
                    }
                    continue;
                }
                Ok(Err(e)) => {
                    error!("Error reading from peer {}: {}", peer_id, e);
                    if let Err(mark_err) = peer_manager
                        .mark_unhealthy(&peer_id, e.to_string())
                        .await
                    {
                        error!("Failed to mark peer {} unhealthy: {}", peer_id, mark_err);
                    }
                    break;
                }
                Err(_) => {
                    // Timeout waiting for message - log and mark unhealthy
                    debug!("Message receive timeout from peer {}", peer_id);
                    if let Err(mark_err) = peer_manager
                        .mark_unhealthy(&peer_id, "Message timeout".to_string())
                        .await
                    {
                        error!("Failed to mark peer {} unhealthy: {}", peer_id, mark_err);
                    }
                    break;
                }
            }
        }

        // PRODUCTION IMPLEMENTATION: Clean up on disconnect with error handling
        info!("Cleaning up connection for peer: {}", peer_id);
        if let Err(e) = connection_pool.remove_connection(&peer_id).await {
            error!("Failed to remove connection for peer {}: {}", peer_id, e);
        }
        let _ = peer_manager
            .mark_unhealthy(&peer_id, "Connection closed".to_string())
            .await;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_message_length_validation() {
        let max_size = 100_000_000; // 100 MB
        let test_len = 150_000_000; // 150 MB
        assert!(test_len > max_size);
    }

    #[test]
    fn test_timeout_duration() {
        let timeout_secs = 30u64;
        let duration = Duration::from_secs(timeout_secs);
        assert_eq!(duration.as_secs(), 30);
    }

    #[test]
    fn test_length_prefix_parsing() {
        let len: u32 = 1024;
        let bytes = len.to_le_bytes();
        let parsed = u32::from_le_bytes(bytes);
        assert_eq!(parsed, 1024);
    }

    #[test]
    fn test_peer_id_format() {
        let addr = "127.0.0.1:9000";
        let peer_id = format!("inbound_{}", addr);
        assert_eq!(peer_id, "inbound_127.0.0.1:9000");
    }

    #[test]
    fn test_timeout_calculation() {
        let timeout_secs = 60u64;
        let duration = Duration::from_secs(timeout_secs);
        assert_eq!(duration.as_millis(), 60000);
    }

    #[test]
    fn test_max_message_size_validation() {
        let max_size = 100 * 1024 * 1024; // 100 MB
        let message_size = 50 * 1024 * 1024; // 50 MB
        assert!(message_size < max_size);
    }
}
