//! Reconnection management with exponential backoff

use crate::error::Result;
use crate::types::BackoffState;
use dashmap::DashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::mpsc;
use tokio::time::sleep;
use tracing::{debug, error, warn};

/// Reconnection event
#[derive(Clone, Debug)]
pub struct ReconnectionEvent {
    /// Peer ID
    pub peer_id: String,
    /// Peer address
    pub peer_addr: SocketAddr,
    /// Attempt number
    pub attempt: u32,
    /// Backoff delay in seconds
    pub backoff_secs: u64,
}

/// Reconnection manager for handling failed connections with exponential backoff
pub struct ReconnectionManager {
    /// Backoff states for each peer
    backoff_states: Arc<DashMap<String, BackoffState>>,
    /// Maximum backoff delay in seconds (5 minutes)
    max_backoff_secs: u64,
    /// Reconnection event sender
    event_tx: mpsc::UnboundedSender<ReconnectionEvent>,
    /// Reconnection event receiver
    event_rx: Arc<tokio::sync::Mutex<mpsc::UnboundedReceiver<ReconnectionEvent>>>,
}

impl ReconnectionManager {
    /// Create a new reconnection manager
    pub fn new(max_backoff_secs: u64) -> Self {
        let (event_tx, event_rx) = mpsc::unbounded_channel();

        Self {
            backoff_states: Arc::new(DashMap::new()),
            max_backoff_secs,
            event_tx,
            event_rx: Arc::new(tokio::sync::Mutex::new(event_rx)),
        }
    }

    /// Schedule a reconnection attempt for a peer
    pub async fn schedule_reconnect(&self, peer_id: String, peer_addr: SocketAddr) -> Result<()> {
        // Get or create backoff state
        let mut backoff = self
            .backoff_states
            .entry(peer_id.clone())
            .or_insert_with(|| BackoffState::new(self.max_backoff_secs))
            .clone();

        // Calculate next backoff
        let backoff_secs = backoff.next_delay();
        let attempt = backoff.failed_attempts;

        // Update backoff state
        self.backoff_states.insert(peer_id.clone(), backoff.clone());

        warn!(
            "Scheduling reconnect for peer {} in {} seconds (attempt {})",
            peer_id, backoff_secs, attempt
        );

        // Spawn reconnection task
        let event_tx = self.event_tx.clone();
        let peer_id_clone = peer_id.clone();

        tokio::spawn(async move {
            sleep(Duration::from_secs(backoff_secs)).await;

            let event = ReconnectionEvent {
                peer_id: peer_id_clone,
                peer_addr,
                attempt,
                backoff_secs,
            };

            if let Err(e) = event_tx.send(event) {
                error!("Failed to send reconnection event: {}", e);
            }
        });

        Ok(())
    }

    /// Get next reconnection event
    pub async fn next_event(&self) -> Option<ReconnectionEvent> {
        let mut rx = self.event_rx.lock().await;
        rx.recv().await
    }

    /// Reset backoff state for a peer (on successful connection)
    pub async fn reset_backoff(&self, peer_id: &str) -> Result<()> {
        if let Some(mut backoff) = self.backoff_states.get_mut(peer_id) {
            backoff.reset();
            debug!("Reset backoff state for peer: {}", peer_id);
        }
        Ok(())
    }

    /// Get backoff state for a peer
    pub async fn get_backoff_state(&self, peer_id: &str) -> Option<BackoffState> {
        self.backoff_states.get(peer_id).map(|b| b.clone())
    }

    /// Get current backoff delay for a peer
    pub async fn get_current_backoff(&self, peer_id: &str) -> Option<u64> {
        self.backoff_states
            .get(peer_id)
            .map(|b| b.current_delay_secs)
    }

    /// Get attempt count for a peer
    pub async fn get_attempt_count(&self, peer_id: &str) -> u32 {
        self.backoff_states
            .get(peer_id)
            .map(|b| b.failed_attempts)
            .unwrap_or(0)
    }

    /// Remove backoff state for a peer
    pub async fn remove_peer(&self, peer_id: &str) -> Result<()> {
        self.backoff_states.remove(peer_id);
        debug!("Removed backoff state for peer: {}", peer_id);
        Ok(())
    }

    /// Get all peers with active backoff
    pub async fn get_all_backoff_peers(&self) -> Vec<(String, BackoffState)> {
        self.backoff_states
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().clone()))
            .collect()
    }

    /// Clear all backoff states
    pub async fn clear_all(&self) -> Result<()> {
        self.backoff_states.clear();
        debug!("Cleared all backoff states");
        Ok(())
    }

    /// Check if a peer should retry now
    pub async fn should_retry_now(&self, peer_id: &str) -> bool {
        if let Some(backoff) = self.backoff_states.get(peer_id) {
            backoff.is_ready_to_retry()
        } else {
            false
        }
    }

    /// Get time until next retry for a peer
    pub async fn time_until_retry(&self, peer_id: &str) -> Option<Duration> {
        if let Some(backoff) = self.backoff_states.get(peer_id) {
            let now = SystemTime::now();
            let next_retry = backoff.last_attempt + Duration::from_secs(backoff.current_delay_secs);
            if let Ok(duration) = next_retry.duration_since(now) {
                return Some(duration);
            }
        }
        None
    }
}

/// Reconnection scheduler for managing multiple reconnection attempts
pub struct ReconnectionScheduler {
    /// Reconnection manager
    manager: Arc<ReconnectionManager>,
    /// Active reconnection tasks
    #[allow(dead_code)]
    active_tasks: Arc<DashMap<String, tokio::task::JoinHandle<()>>>,
}

impl ReconnectionScheduler {
    /// Create a new reconnection scheduler
    pub fn new(max_backoff_secs: u64) -> Self {
        Self {
            manager: Arc::new(ReconnectionManager::new(max_backoff_secs)),
            active_tasks: Arc::new(DashMap::new()),
        }
    }

    /// Schedule a reconnection attempt
    pub async fn schedule(&self, peer_id: String, peer_addr: SocketAddr) -> Result<()> {
        self.manager.schedule_reconnect(peer_id, peer_addr).await
    }

    /// Reset backoff for a peer
    pub async fn reset(&self, peer_id: &str) -> Result<()> {
        self.manager.reset_backoff(peer_id).await
    }

    /// Get next reconnection event
    pub async fn next_event(&self) -> Option<ReconnectionEvent> {
        self.manager.next_event().await
    }

    /// Get manager reference
    pub fn manager(&self) -> Arc<ReconnectionManager> {
        self.manager.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_reconnection_manager_creation() {
        let manager = ReconnectionManager::new(300);
        assert_eq!(manager.max_backoff_secs, 300);
    }

    #[tokio::test]
    async fn test_backoff_state_creation() {
        let backoff = BackoffState::new(300);
        assert_eq!(backoff.current_delay_secs, 1);
        assert_eq!(backoff.failed_attempts, 0);
    }

    #[tokio::test]
    async fn test_backoff_exponential_increase() {
        let mut backoff = BackoffState::new(300);

        let delay1 = backoff.next_delay();
        assert_eq!(delay1, 2);

        let delay2 = backoff.next_delay();
        assert_eq!(delay2, 4);

        let delay3 = backoff.next_delay();
        assert_eq!(delay3, 8);
    }

    #[tokio::test]
    async fn test_backoff_max_limit() {
        let mut backoff = BackoffState::new(300);

        for _ in 0..15 {
            backoff.next_delay();
        }

        assert!(backoff.current_delay_secs <= 300);
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
    async fn test_reconnection_manager_reset_backoff() {
        let manager = ReconnectionManager::new(300);
        let peer_id = "peer1".to_string();

        // Create a backoff state
        let mut backoff = BackoffState::new(300);
        backoff.next_delay();
        backoff.next_delay();
        manager.backoff_states.insert(peer_id.clone(), backoff);

        // Reset it
        manager.reset_backoff(&peer_id).await.unwrap();

        // Verify it's reset
        let state = manager.get_backoff_state(&peer_id).await.unwrap();
        assert_eq!(state.current_delay_secs, 1);
        assert_eq!(state.failed_attempts, 0);
    }

    #[tokio::test]
    async fn test_reconnection_manager_get_attempt_count() {
        let manager = ReconnectionManager::new(300);
        let peer_id = "peer1".to_string();

        let mut backoff = BackoffState::new(300);
        backoff.next_delay();
        backoff.next_delay();
        backoff.next_delay();
        manager.backoff_states.insert(peer_id.clone(), backoff);

        let count = manager.get_attempt_count(&peer_id).await;
        assert_eq!(count, 3);
    }

    #[tokio::test]
    async fn test_reconnection_manager_get_current_backoff() {
        let manager = ReconnectionManager::new(300);
        let peer_id = "peer1".to_string();

        let mut backoff = BackoffState::new(300);
        backoff.next_delay();
        backoff.next_delay();
        manager.backoff_states.insert(peer_id.clone(), backoff);

        let delay = manager.get_current_backoff(&peer_id).await.unwrap();
        assert_eq!(delay, 4);
    }

    #[tokio::test]
    async fn test_reconnection_manager_remove_peer() {
        let manager = ReconnectionManager::new(300);
        let peer_id = "peer1".to_string();

        let backoff = BackoffState::new(300);
        manager.backoff_states.insert(peer_id.clone(), backoff);

        assert!(manager.get_backoff_state(&peer_id).await.is_some());

        manager.remove_peer(&peer_id).await.unwrap();

        assert!(manager.get_backoff_state(&peer_id).await.is_none());
    }

    #[tokio::test]
    async fn test_reconnection_manager_clear_all() {
        let manager = ReconnectionManager::new(300);

        for i in 0..5 {
            let peer_id = format!("peer{}", i);
            let backoff = BackoffState::new(300);
            manager.backoff_states.insert(peer_id, backoff);
        }

        assert_eq!(manager.backoff_states.len(), 5);

        manager.clear_all().await.unwrap();

        assert_eq!(manager.backoff_states.len(), 0);
    }

    #[tokio::test]
    async fn test_reconnection_manager_get_all_backoff_peers() {
        let manager = ReconnectionManager::new(300);

        for i in 0..3 {
            let peer_id = format!("peer{}", i);
            let backoff = BackoffState::new(300);
            manager.backoff_states.insert(peer_id, backoff);
        }

        let peers = manager.get_all_backoff_peers().await;
        assert_eq!(peers.len(), 3);
    }

    #[tokio::test]
    async fn test_reconnection_scheduler_creation() {
        let scheduler = ReconnectionScheduler::new(300);
        let manager = scheduler.manager();
        assert_eq!(manager.max_backoff_secs, 300);
    }

    #[tokio::test]
    async fn test_backoff_should_retry() {
        let mut backoff = BackoffState::new(300);
        // After creation, should be ready to retry immediately
        assert_eq!(backoff.current_delay_secs, 1);

        // After next_delay, should have incremented attempts
        backoff.next_delay();
        assert_eq!(backoff.failed_attempts, 1);
    }

    #[tokio::test]
    async fn test_backoff_attempt_count_increment() {
        let mut backoff = BackoffState::new(300);
        assert_eq!(backoff.failed_attempts, 0);

        backoff.next_delay();
        assert_eq!(backoff.failed_attempts, 1);

        backoff.next_delay();
        assert_eq!(backoff.failed_attempts, 2);
    }

    #[tokio::test]
    async fn test_reconnection_event_creation() {
        let event = ReconnectionEvent {
            peer_id: "peer1".to_string(),
            peer_addr: "127.0.0.1:9000".parse().map_err(|e| format!("Parse failed: {}", e))?,
            attempt: 1,
            backoff_secs: 2,
        };

        assert_eq!(event.peer_id, "peer1");
        assert_eq!(event.attempt, 1);
        assert_eq!(event.backoff_secs, 2);
    }

    #[tokio::test]
    async fn test_backoff_starting_value() {
        let backoff = BackoffState::new(300);
        assert_eq!(backoff.current_delay_secs, 1);
    }

    #[tokio::test]
    async fn test_backoff_max_value() {
        let max_backoff = 300u64;
        let mut backoff = BackoffState::new(max_backoff);

        for _ in 0..20 {
            backoff.next_delay();
        }

        assert_eq!(backoff.current_delay_secs, max_backoff);
    }
}
