//! Rate limiting and connection limit enforcement

use crate::error::{P2PError, Result};
use dashmap::DashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tracing::{debug, warn};

/// Token bucket rate limiter for per-peer message throttling
#[derive(Clone, Debug)]
pub struct TokenBucket {
    /// Maximum tokens (messages) per second
    max_tokens: u32,
    /// Current tokens available
    tokens: Arc<AtomicU32>,
    /// Last refill time
    last_refill: Arc<parking_lot::Mutex<SystemTime>>,
    /// Recovery state tracking
    recovery_start: Arc<parking_lot::Mutex<Option<SystemTime>>>,
}

impl TokenBucket {
    /// Create new token bucket
    pub fn new(max_tokens_per_sec: u32) -> Self {
        Self {
            max_tokens: max_tokens_per_sec,
            tokens: Arc::new(AtomicU32::new(max_tokens_per_sec)),
            last_refill: Arc::new(parking_lot::Mutex::new(SystemTime::now())),
            recovery_start: Arc::new(parking_lot::Mutex::new(None)),
        }
    }

    /// Try to consume a token
    pub fn try_consume(&self) -> bool {
        self.refill_tokens();

        let current = self.tokens.load(Ordering::SeqCst);
        if current > 0 {
            self.tokens.fetch_sub(1, Ordering::SeqCst);
            true
        } else {
            false
        }
    }

    /// Refill tokens based on elapsed time
    fn refill_tokens(&self) {
        let mut last_refill = self.last_refill.lock();
        let now = SystemTime::now();

        if let Ok(elapsed) = now.duration_since(*last_refill) {
            let seconds_elapsed = elapsed.as_secs_f64();
            let tokens_to_add = (seconds_elapsed * self.max_tokens as f64) as u32;

            if tokens_to_add > 0 {
                let current = self.tokens.load(Ordering::SeqCst);
                let new_tokens = std::cmp::min(current + tokens_to_add, self.max_tokens);
                self.tokens.store(new_tokens, Ordering::SeqCst);
                *last_refill = now;
            }
        }
    }

    /// Get current token count
    pub fn current_tokens(&self) -> u32 {
        self.refill_tokens();
        self.tokens.load(Ordering::SeqCst)
    }

    /// Check if peer is in recovery (normal behavior after rate limit)
    pub fn is_in_recovery(&self) -> bool {
        let recovery = self.recovery_start.lock();
        if let Some(start) = *recovery {
            if let Ok(elapsed) = start.elapsed() {
                elapsed < Duration::from_secs(60)
            } else {
                false
            }
        } else {
            false
        }
    }

    /// Mark peer as rate limited (start recovery tracking)
    pub fn mark_rate_limited(&self) {
        let mut recovery = self.recovery_start.lock();
        if recovery.is_none() {
            *recovery = Some(SystemTime::now());
        }
    }

    /// Check if recovery period is complete
    pub fn is_recovery_complete(&self) -> bool {
        let recovery = self.recovery_start.lock();
        if let Some(start) = *recovery {
            if let Ok(elapsed) = start.elapsed() {
                elapsed >= Duration::from_secs(60)
            } else {
                false
            }
        } else {
            false
        }
    }

    /// Reset recovery state
    pub fn reset_recovery(&self) {
        let mut recovery = self.recovery_start.lock();
        *recovery = None;
    }
}

/// Global rate limiter managing per-peer limits
pub struct GlobalRateLimiter {
    /// Per-peer token buckets
    peer_limiters: Arc<DashMap<String, TokenBucket>>,
    /// Per-peer rate limit (messages per second)
    per_peer_limit: u32,
    /// Rate limited peers tracking
    rate_limited_peers: Arc<DashMap<String, SystemTime>>,
}

impl GlobalRateLimiter {
    /// Create new global rate limiter
    pub fn new(_global_limit: u32, per_peer_limit: u32) -> Self {
        Self {
            peer_limiters: Arc::new(DashMap::new()),
            per_peer_limit,
            rate_limited_peers: Arc::new(DashMap::new()),
        }
    }

    /// Check if message can be sent (per-peer limits)
    pub fn can_send(&self, peer_id: &str) -> Result<bool> {
        // Check per-peer limit
        let limiter = self
            .peer_limiters
            .entry(peer_id.to_string())
            .or_insert_with(|| TokenBucket::new(self.per_peer_limit))
            .clone();

        if limiter.try_consume() {
            // Update the limiter in the map
            self.peer_limiters
                .insert(peer_id.to_string(), limiter.clone());

            // Check if peer was in recovery and is now complete
            if limiter.is_recovery_complete() {
                let updated_limiter = limiter;
                updated_limiter.reset_recovery();
                self.peer_limiters
                    .insert(peer_id.to_string(), updated_limiter);
                self.rate_limited_peers.remove(peer_id);
                debug!("Peer {} recovered from rate limiting", peer_id);
            }

            Ok(true)
        } else {
            // Mark peer as rate limited
            let updated_limiter = limiter;
            updated_limiter.mark_rate_limited();
            self.peer_limiters
                .insert(peer_id.to_string(), updated_limiter);
            self.rate_limited_peers
                .insert(peer_id.to_string(), SystemTime::now());

            warn!("Rate limit exceeded for peer: {}", peer_id);
            Ok(false)
        }
    }

    /// Get current tokens for peer
    pub fn get_peer_tokens(&self, peer_id: &str) -> u32 {
        self.peer_limiters
            .get(peer_id)
            .map(|limiter| limiter.current_tokens())
            .unwrap_or(self.per_peer_limit)
    }

    /// Get rate limited peers
    pub fn get_rate_limited_peers(&self) -> Vec<String> {
        self.rate_limited_peers
            .iter()
            .map(|entry| entry.key().clone())
            .collect()
    }

    /// Check if peer is rate limited
    pub fn is_peer_rate_limited(&self, peer_id: &str) -> bool {
        self.rate_limited_peers.contains_key(peer_id)
    }

    /// Clear rate limited status for peer
    pub fn clear_rate_limited(&self, peer_id: &str) {
        self.rate_limited_peers.remove(peer_id);
        if let Some(limiter) = self.peer_limiters.get_mut(peer_id) {
            limiter.reset_recovery();
        }
    }

    /// Get all peer limiters info
    pub fn get_all_peer_info(&self) -> Vec<(String, u32, bool)> {
        self.peer_limiters
            .iter()
            .map(|entry| {
                let peer_id = entry.key().clone();
                let tokens = entry.value().current_tokens();
                let is_limited = self.rate_limited_peers.contains_key(&peer_id);
                (peer_id, tokens, is_limited)
            })
            .collect()
    }
}

/// Connection limit enforcer
pub struct ConnectionLimiter {
    /// Maximum connections allowed
    max_connections: usize,
    /// Current connection count
    current_connections: Arc<AtomicU32>,
    /// Rejected connections tracking
    rejected_connections: Arc<DashMap<String, u32>>,
}

impl ConnectionLimiter {
    /// Create new connection limiter
    pub fn new(max_connections: usize) -> Self {
        Self {
            max_connections,
            current_connections: Arc::new(AtomicU32::new(0)),
            rejected_connections: Arc::new(DashMap::new()),
        }
    }

    /// Try to add a connection
    pub fn try_add_connection(&self) -> Result<()> {
        let current = self.current_connections.load(Ordering::SeqCst) as usize;

        if current >= self.max_connections {
            return Err(P2PError::ConnectionLimitReached(current, self.max_connections));
        }

        self.current_connections.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    /// Remove a connection
    pub fn remove_connection(&self) -> Result<()> {
        let current = self.current_connections.load(Ordering::SeqCst);
        if current > 0 {
            self.current_connections.fetch_sub(1, Ordering::SeqCst);
            Ok(())
        } else {
            Err(P2PError::InternalError(
                "Connection count underflow".to_string(),
            ))
        }
    }

    /// Get current connection count
    pub fn get_connection_count(&self) -> usize {
        self.current_connections.load(Ordering::SeqCst) as usize
    }

    /// Check if at capacity
    pub fn is_at_capacity(&self) -> bool {
        self.get_connection_count() >= self.max_connections
    }

    /// Get available slots
    pub fn available_slots(&self) -> usize {
        let current = self.get_connection_count();
        self.max_connections.saturating_sub(current)
    }

    /// Track rejected connection from peer
    pub fn track_rejection(&self, peer_addr: &str) {
        self.rejected_connections
            .entry(peer_addr.to_string())
            .and_modify(|count| *count += 1)
            .or_insert(1);
    }

    /// Get rejection count for peer
    pub fn get_rejection_count(&self, peer_addr: &str) -> u32 {
        self.rejected_connections
            .get(peer_addr)
            .map(|count| *count)
            .unwrap_or(0)
    }

    /// Get all rejection stats
    pub fn get_rejection_stats(&self) -> Vec<(String, u32)> {
        self.rejected_connections
            .iter()
            .map(|entry| (entry.key().clone(), *entry.value()))
            .collect()
    }

    /// Clear rejection stats
    pub fn clear_rejection_stats(&self) {
        self.rejected_connections.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_token_bucket_creation() {
        let bucket = TokenBucket::new(100);
        assert_eq!(bucket.current_tokens(), 100);
    }

    #[test]
    fn test_token_bucket_consume() {
        let bucket = TokenBucket::new(10);
        assert!(bucket.try_consume());
        assert_eq!(bucket.current_tokens(), 9);
    }

    #[test]
    fn test_token_bucket_exhaustion() {
        let bucket = TokenBucket::new(2);
        assert!(bucket.try_consume());
        assert!(bucket.try_consume());
        assert!(!bucket.try_consume());
    }

    #[test]
    fn test_token_bucket_recovery() {
        let bucket = TokenBucket::new(1);
        bucket.mark_rate_limited();
        assert!(bucket.is_in_recovery());
        assert!(!bucket.is_recovery_complete());
    }

    #[test]
    fn test_global_rate_limiter_creation() {
        let limiter = GlobalRateLimiter::new(1000, 100);
        assert_eq!(limiter.get_peer_tokens("peer1"), 100);
    }

    #[test]
    fn test_global_rate_limiter_can_send() {
        let limiter = GlobalRateLimiter::new(1000, 2);
        assert!(limiter.can_send("peer1").unwrap());
        assert!(limiter.can_send("peer1").unwrap());
        assert!(!limiter.can_send("peer1").unwrap());
    }

    #[test]
    fn test_global_rate_limiter_per_peer() {
        let limiter = GlobalRateLimiter::new(1000, 2);
        assert!(limiter.can_send("peer1").unwrap());
        assert!(limiter.can_send("peer1").unwrap());
        assert!(!limiter.can_send("peer1").unwrap());
        assert!(limiter.can_send("peer2").unwrap());
    }

    #[test]
    fn test_global_rate_limiter_rate_limited_tracking() {
        let limiter = GlobalRateLimiter::new(1000, 2);
        let _ = limiter.can_send("peer1").unwrap();
        let _ = limiter.can_send("peer1").unwrap();
        let _ = limiter.can_send("peer1").unwrap();

        assert!(limiter.is_peer_rate_limited("peer1"));
        let limited = limiter.get_rate_limited_peers();
        assert!(limited.contains(&"peer1".to_string()));
    }

    #[test]
    fn test_connection_limiter_creation() {
        let limiter = ConnectionLimiter::new(100);
        assert_eq!(limiter.get_connection_count(), 0);
        assert!(!limiter.is_at_capacity());
    }

    #[test]
    fn test_connection_limiter_add() {
        let limiter = ConnectionLimiter::new(2);
        assert!(limiter.try_add_connection().is_ok());
        assert_eq!(limiter.get_connection_count(), 1);
        assert!(limiter.try_add_connection().is_ok());
        assert_eq!(limiter.get_connection_count(), 2);
        assert!(limiter.try_add_connection().is_err());
    }

    #[test]
    fn test_connection_limiter_remove() {
        let limiter = ConnectionLimiter::new(100);
        limiter.try_add_connection().unwrap();
        assert_eq!(limiter.get_connection_count(), 1);
        limiter.remove_connection().unwrap();
        assert_eq!(limiter.get_connection_count(), 0);
    }

    #[test]
    fn test_connection_limiter_capacity() {
        let limiter = ConnectionLimiter::new(3);
        assert_eq!(limiter.available_slots(), 3);
        limiter.try_add_connection().unwrap();
        assert_eq!(limiter.available_slots(), 2);
        limiter.try_add_connection().unwrap();
        assert_eq!(limiter.available_slots(), 1);
        limiter.try_add_connection().unwrap();
        assert_eq!(limiter.available_slots(), 0);
        assert!(limiter.is_at_capacity());
    }

    #[test]
    fn test_connection_limiter_rejection_tracking() {
        let limiter = ConnectionLimiter::new(1);
        limiter.track_rejection("192.168.1.1:9000");
        limiter.track_rejection("192.168.1.1:9000");
        assert_eq!(limiter.get_rejection_count("192.168.1.1:9000"), 2);
    }

    #[test]
    fn test_connection_limiter_rejection_stats() {
        let limiter = ConnectionLimiter::new(1);
        limiter.track_rejection("192.168.1.1:9000");
        limiter.track_rejection("192.168.1.2:9000");
        let stats = limiter.get_rejection_stats();
        assert_eq!(stats.len(), 2);
    }

    #[test]
    fn test_connection_limiter_clear_stats() {
        let limiter = ConnectionLimiter::new(1);
        limiter.track_rejection("192.168.1.1:9000");
        limiter.clear_rejection_stats();
        assert_eq!(limiter.get_rejection_count("192.168.1.1:9000"), 0);
    }
}
