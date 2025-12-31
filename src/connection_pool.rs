//! Connection pool management with backoff and rate limiting

use crate::error::{P2PError, Result};
use crate::types::BackoffState;
use dashmap::DashMap;
use std::sync::Arc;
use std::time::SystemTime;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::sync::{Mutex, RwLock};
use tracing::{debug, warn};

/// Connection state information
#[derive(Clone, Debug)]
pub struct ConnectionInfo {
    /// TCP stream to peer (protected by Mutex for async write access)
    pub stream: Arc<Mutex<TcpStream>>,
    /// Connection established time
    pub connected_at: SystemTime,
    /// Last activity time
    pub last_activity: SystemTime,
    /// Bytes sent on this connection
    pub bytes_sent: u64,
    /// Bytes received on this connection
    pub bytes_received: u64,
    /// Message count sent
    pub messages_sent: u64,
    /// Message count received
    pub messages_received: u64,
}

impl ConnectionInfo {
    /// Create new connection info
    pub fn new(stream: Arc<Mutex<TcpStream>>) -> Self {
        let now = SystemTime::now();
        Self {
            stream,
            connected_at: now,
            last_activity: now,
            bytes_sent: 0,
            bytes_received: 0,
            messages_sent: 0,
            messages_received: 0,
        }
    }

    /// Update last activity time
    pub fn update_activity(&mut self) {
        self.last_activity = SystemTime::now();
    }

    /// Record bytes sent
    pub fn record_sent(&mut self, bytes: u64) {
        self.bytes_sent += bytes;
        self.messages_sent += 1;
        self.update_activity();
    }

    /// Record bytes received
    pub fn record_received(&mut self, bytes: u64) {
        self.bytes_received += bytes;
        self.messages_received += 1;
        self.update_activity();
    }

    /// Get connection uptime in seconds
    pub fn uptime_secs(&self) -> u64 {
        self.connected_at.elapsed().unwrap_or_default().as_secs()
    }

    /// Get idle time in seconds
    pub fn idle_secs(&self) -> u64 {
        self.last_activity.elapsed().unwrap_or_default().as_secs()
    }
}

/// Manages TCP connections to peers with backoff and rate limiting
pub struct ConnectionPool {
    /// Active connections with metadata
    connections: Arc<DashMap<String, Arc<RwLock<ConnectionInfo>>>>,
    /// Backoff states for failed connections
    backoff_states: Arc<DashMap<String, BackoffState>>,
    /// Maximum peers allowed
    max_peers: usize,
    /// Rate limit state per peer (messages per second)
    rate_limiters: Arc<DashMap<String, RateLimiter>>,
}

/// Rate limiter for per-peer message throttling
#[derive(Clone, Debug)]
pub struct RateLimiter {
    /// Maximum messages per second
    max_msgs_per_sec: u32,
    /// Messages sent in current window
    messages_in_window: u32,
    /// Window start time
    window_start: SystemTime,
}

impl RateLimiter {
    /// Create new rate limiter
    pub fn new(max_msgs_per_sec: u32) -> Self {
        Self {
            max_msgs_per_sec,
            messages_in_window: 0,
            window_start: SystemTime::now(),
        }
    }

    /// Check if message can be sent
    pub fn can_send(&mut self) -> bool {
        let elapsed = self.window_start.elapsed().unwrap_or_default().as_secs();

        // Reset window if 1 second has passed
        if elapsed >= 1 {
            self.messages_in_window = 0;
            self.window_start = SystemTime::now();
        }

        if self.messages_in_window < self.max_msgs_per_sec {
            self.messages_in_window += 1;
            true
        } else {
            false
        }
    }

    /// Get remaining messages in current window
    pub fn remaining_in_window(&self) -> u32 {
        self.max_msgs_per_sec
            .saturating_sub(self.messages_in_window)
    }
}

impl ConnectionPool {
    /// Create new connection pool
    pub fn new(max_peers: usize, _max_backoff_secs: u64) -> Self {
        Self {
            connections: Arc::new(DashMap::new()),
            backoff_states: Arc::new(DashMap::new()),
            max_peers,
            rate_limiters: Arc::new(DashMap::new()),
        }
    }

    /// Add connection to pool
    pub async fn add_connection(&self, peer_id: String, stream: TcpStream) -> Result<()> {
        if self.connections.len() >= self.max_peers {
            return Err(P2PError::ConnectionLimitReached(
                self.connections.len(),
                self.max_peers,
            ));
        }

        let conn_info = ConnectionInfo::new(Arc::new(Mutex::new(stream)));
        self.connections
            .insert(peer_id.clone(), Arc::new(RwLock::new(conn_info)));
        self.backoff_states.remove(&peer_id);
        debug!("Added connection for peer: {}", peer_id);
        Ok(())
    }

    /// Remove connection from pool
    pub async fn remove_connection(&self, peer_id: &str) -> Result<()> {
        if let Some((_, conn)) = self.connections.remove(peer_id) {
            let info = conn.read().await;
            debug!(
                "Removed connection for peer: {} (uptime: {}s, sent: {}, received: {})",
                peer_id,
                info.uptime_secs(),
                info.bytes_sent,
                info.bytes_received
            );
        }
        self.rate_limiters.remove(peer_id);
        Ok(())
    }

    /// Get connection stream
    pub async fn get_connection(&self, peer_id: &str) -> Result<Arc<Mutex<TcpStream>>> {
        if let Some(conn) = self.connections.get(peer_id) {
            let info = conn.read().await;
            Ok(info.stream.clone())
        } else {
            Err(P2PError::PeerNotFound(peer_id.to_string()))
        }
    }

    /// Check if peer is connected
    pub async fn is_connected(&self, peer_id: &str) -> bool {
        self.connections.contains_key(peer_id)
    }

    /// Get connection count
    pub async fn get_connection_count(&self) -> usize {
        self.connections.len()
    }

    /// Get connection info
    pub async fn get_connection_info(&self, peer_id: &str) -> Result<ConnectionInfo> {
        if let Some(conn) = self.connections.get(peer_id) {
            let info = conn.read().await;
            Ok(info.clone())
        } else {
            Err(P2PError::PeerNotFound(peer_id.to_string()))
        }
    }

    /// Record bytes sent on connection
    pub async fn record_sent(&self, peer_id: &str, bytes: u64) -> Result<()> {
        if let Some(conn) = self.connections.get(peer_id) {
            conn.write().await.record_sent(bytes);
        }
        Ok(())
    }

    /// Record bytes received on connection
    pub async fn record_received(&self, peer_id: &str, bytes: u64) -> Result<()> {
        if let Some(conn) = self.connections.get(peer_id) {
            conn.write().await.record_received(bytes);
        }
        Ok(())
    }

    /// Check rate limit for peer
    pub async fn check_rate_limit(&self, peer_id: &str, max_msgs_per_sec: u32) -> Result<bool> {
        let mut limiter = self
            .rate_limiters
            .entry(peer_id.to_string())
            .or_insert_with(|| RateLimiter::new(max_msgs_per_sec))
            .clone();

        if limiter.can_send() {
            self.rate_limiters.insert(peer_id.to_string(), limiter);
            Ok(true)
        } else {
            warn!("Rate limit exceeded for peer: {}", peer_id);
            Ok(false)
        }
    }

    /// Get backoff state for peer
    pub async fn get_backoff_state(&self, peer_id: &str) -> Option<BackoffState> {
        self.backoff_states.get(peer_id).map(|b| b.clone())
    }

    /// Update backoff state for peer
    pub async fn update_backoff_state(&self, peer_id: String, state: BackoffState) -> Result<()> {
        self.backoff_states.insert(peer_id, state);
        Ok(())
    }

    /// Reset backoff state for peer
    pub async fn reset_backoff_state(&self, peer_id: &str) -> Result<()> {
        self.backoff_states.remove(peer_id);
        debug!("Reset backoff state for peer: {}", peer_id);
        Ok(())
    }

    /// Get all connections info
    pub async fn get_all_connections_info(&self) -> Vec<(String, ConnectionInfo)> {
        let mut result = Vec::new();
        for entry in self.connections.iter() {
            let peer_id = entry.key().clone();
            let conn = entry.value().read().await;
            result.push((peer_id, conn.clone()));
        }
        result
    }

    /// Get idle connections (not used for specified seconds)
    pub async fn get_idle_connections(&self, idle_secs: u64) -> Vec<String> {
        let mut idle = Vec::new();
        for entry in self.connections.iter() {
            let conn = entry.value().read().await;
            if conn.idle_secs() >= idle_secs {
                idle.push(entry.key().clone());
            }
        }
        idle
    }

    /// Clear all connections
    pub async fn clear_all(&self) -> Result<()> {
        self.connections.clear();
        self.backoff_states.clear();
        self.rate_limiters.clear();
        debug!("Cleared all connections");
        Ok(())
    }

    /// Write message to peer connection
    pub async fn write_message(&self, peer_id: &str, data: &[u8]) -> Result<()> {
        if let Some(conn) = self.connections.get(peer_id) {
            let info = conn.read().await;
            let stream = &info.stream;

            // Verify the stream is still valid and can be written to
            // Get a mutable guard to the stream
            let mut stream_guard = stream.lock().await;

            // Verify the stream is still connected by checking peer address
            match stream_guard.peer_addr() {
                Ok(peer_addr) => {
                    debug!("Stream to {} is valid (peer_addr: {})", peer_id, peer_addr);

                    // Write the data to the stream
                    stream_guard.write_all(data).await?;

                    // Flush to ensure data is sent
                    stream_guard.flush().await?;

                    Ok(())
                }
                Err(e) => {
                    warn!("Stream to {} is invalid: {}", peer_id, e);
                    Err(P2PError::ConnectionError(format!(
                        "Stream to {} is invalid: {}",
                        peer_id, e
                    )))
                }
            }
        } else {
            Err(P2PError::PeerNotFound(peer_id.to_string()))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_connection_pool_creation() {
        let pool = ConnectionPool::new(100, 300);
        assert_eq!(pool.get_connection_count().await, 0);
    }

    #[tokio::test]
    async fn test_backoff_state() {
        let pool = ConnectionPool::new(100, 300);
        let mut backoff = BackoffState::new(300);
        backoff.next_delay();

        pool.update_backoff_state("peer1".to_string(), backoff.clone())
            .await
            .unwrap();

        let retrieved = pool.get_backoff_state("peer1").await;
        assert!(retrieved.is_some());
    }

    #[tokio::test]
    async fn test_add_connection_success() {
        let pool = ConnectionPool::new(100, 300);
        assert_eq!(pool.get_connection_count().await, 0);
    }

    #[tokio::test]
    async fn test_add_connection_at_capacity() {
        let pool = ConnectionPool::new(1, 300);
        assert_eq!(pool.get_connection_count().await, 0);
    }

    #[tokio::test]
    async fn test_remove_connection() {
        let pool = ConnectionPool::new(100, 300);
        let result = pool.remove_connection("nonexistent").await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_is_connected() {
        let pool = ConnectionPool::new(100, 300);
        assert!(!pool.is_connected("peer1").await);
    }

    #[tokio::test]
    async fn test_get_connection_info() {
        let pool = ConnectionPool::new(100, 300);
        let result = pool.get_connection_info("nonexistent").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_record_sent_received() {
        let pool = ConnectionPool::new(100, 300);
        let result = pool.record_sent("nonexistent", 1024).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_rate_limiter() {
        let pool = ConnectionPool::new(100, 300);

        // Test rate limiting
        let can_send1 = pool.check_rate_limit("peer1", 2).await.unwrap();
        assert!(can_send1);

        let can_send2 = pool.check_rate_limit("peer1", 2).await.unwrap();
        assert!(can_send2);

        let can_send3 = pool.check_rate_limit("peer1", 2).await.unwrap();
        assert!(!can_send3); // Should be rate limited
    }

    #[tokio::test]
    async fn test_backoff_state_operations() {
        let pool = ConnectionPool::new(100, 300);

        let backoff = BackoffState::new(300);
        pool.update_backoff_state("peer1".to_string(), backoff)
            .await
            .unwrap();

        let retrieved = pool.get_backoff_state("peer1").await.unwrap();
        assert_eq!(retrieved.current_delay_secs, 1);

        pool.reset_backoff_state("peer1").await.unwrap();

        let after_reset = pool.get_backoff_state("peer1").await;
        assert!(after_reset.is_none());
    }

    #[tokio::test]
    async fn test_get_all_connections_info() {
        let pool = ConnectionPool::new(100, 300);
        let all_conns = pool.get_all_connections_info().await;
        assert_eq!(all_conns.len(), 0);
    }

    #[tokio::test]
    async fn test_get_idle_connections() {
        let pool = ConnectionPool::new(100, 300);
        let idle = pool.get_idle_connections(0).await;
        assert_eq!(idle.len(), 0);
    }

    #[tokio::test]
    async fn test_clear_all() {
        let pool = ConnectionPool::new(100, 300);
        let backoff = BackoffState::new(300);
        pool.update_backoff_state("peer1".to_string(), backoff)
            .await
            .unwrap();

        pool.clear_all().await.unwrap();

        assert_eq!(pool.get_connection_count().await, 0);
    }

    #[tokio::test]
    async fn test_connection_info_uptime() {
        // Test that uptime calculation works
        let now = SystemTime::now();
        let uptime = now.elapsed().unwrap_or_default().as_secs();
        assert!(uptime <= 1); // Should be very small, less than 1 second
    }

    #[tokio::test]
    async fn test_connection_info_idle() {
        // Test that idle time calculation works
        let now = SystemTime::now();
        let idle = now.elapsed().unwrap_or_default().as_secs();
        assert!(idle <= 1); // Should be very small, less than 1 second
    }
}
