//! Peer management and discovery

use crate::error::{P2PError, Result};
use crate::types::{PeerState, NodeRole};
use dashmap::DashMap;
use std::sync::Arc;
use std::time::SystemTime;
use tracing::{debug, warn};

/// Manages peer connections and discovery
pub struct PeerManager {
    /// All known peers indexed by peer_id
    peers: Arc<DashMap<String, PeerState>>,
    /// Candidate peers waiting to connect indexed by address
    candidates: Arc<DashMap<String, String>>,
    /// Maximum candidates to track
    max_candidates: usize,
}

impl PeerManager {
    /// Create new peer manager
    pub fn new(max_candidates: usize) -> Self {
        Self {
            peers: Arc::new(DashMap::new()),
            candidates: Arc::new(DashMap::new()),
            max_candidates,
        }
    }

    /// Add a new peer to the peer list
    pub async fn add_peer(&self, peer_id: String, addr: String, role: NodeRole) -> Result<()> {
        if peer_id.is_empty() {
            return Err(P2PError::InvalidPeerAddress("peer_id cannot be empty".to_string()));
        }

        if addr.is_empty() {
            return Err(P2PError::InvalidPeerAddress("address cannot be empty".to_string()));
        }

        let peer = PeerState::new(peer_id.clone(), addr.clone(), role);
        self.peers.insert(peer_id.clone(), peer);
        debug!("Added peer: {} at {}", peer_id, addr);
        Ok(())
    }

    /// Remove a peer from the peer list
    pub async fn remove_peer(&self, peer_id: &str) -> Result<()> {
        if let Some((_, peer)) = self.peers.remove(peer_id) {
            debug!("Removed peer: {} at {}", peer.peer_id, peer.address);
        }
        Ok(())
    }

    /// Mark peer as healthy and clear error
    pub async fn mark_healthy(&self, peer_id: &str) -> Result<()> {
        if let Some(mut peer) = self.peers.get_mut(peer_id) {
            peer.is_healthy = true;
            peer.last_error = None;
            peer.update_last_seen();
            debug!("Marked peer as healthy: {}", peer_id);
        } else {
            warn!("Attempted to mark unknown peer as healthy: {}", peer_id);
        }
        Ok(())
    }

    /// Mark peer as unhealthy with error message
    pub async fn mark_unhealthy(&self, peer_id: &str, error: String) -> Result<()> {
        if let Some(mut peer) = self.peers.get_mut(peer_id) {
            peer.is_healthy = false;
            peer.last_error = Some(error.clone());
            peer.connection_attempts += 1;
            debug!("Marked peer as unhealthy: {} ({})", peer_id, error);
        } else {
            warn!("Attempted to mark unknown peer as unhealthy: {}", peer_id);
        }
        Ok(())
    }

    /// Get peer state by peer_id
    pub async fn get_peer(&self, peer_id: &str) -> Result<PeerState> {
        self.peers
            .get(peer_id)
            .map(|p| p.clone())
            .ok_or_else(|| P2PError::PeerNotFound(peer_id.to_string()))
    }

    /// Get all known peers
    pub async fn get_all_peers(&self) -> Vec<PeerState> {
        self.peers.iter().map(|p| p.value().clone()).collect()
    }

    /// Get total peer count
    pub async fn get_peer_count(&self) -> usize {
        self.peers.len()
    }

    /// Get count of currently connected peers
    pub async fn get_connected_peer_count(&self) -> usize {
        self.peers
            .iter()
            .filter(|p| p.value().is_connected)
            .count()
    }

    /// Get count of healthy peers
    pub async fn get_healthy_peer_count(&self) -> usize {
        self.peers
            .iter()
            .filter(|p| p.value().is_healthy)
            .count()
    }

    /// Mark peer as connected
    pub async fn mark_connected(&self, peer_id: &str) -> Result<()> {
        if let Some(mut peer) = self.peers.get_mut(peer_id) {
            peer.is_connected = true;
            peer.update_last_seen();
            debug!("Marked peer as connected: {}", peer_id);
        } else {
            warn!("Attempted to mark unknown peer as connected: {}", peer_id);
        }
        Ok(())
    }

    /// Mark peer as disconnected
    pub async fn mark_disconnected(&self, peer_id: &str) -> Result<()> {
        if let Some(mut peer) = self.peers.get_mut(peer_id) {
            peer.is_connected = false;
            debug!("Marked peer as disconnected: {}", peer_id);
        } else {
            warn!("Attempted to mark unknown peer as disconnected: {}", peer_id);
        }
        Ok(())
    }

    /// Update peer block height
    pub async fn update_peer_block_height(&self, peer_id: &str, height: u64) -> Result<()> {
        if let Some(mut peer) = self.peers.get_mut(peer_id) {
            peer.block_height = height;
            peer.update_last_seen();
        }
        Ok(())
    }

    /// Add candidate peers for future connection
    pub async fn add_candidates(&self, addrs: Vec<String>) -> Result<()> {
        for addr in addrs {
            // Skip if already a known peer
            if self.peers.iter().any(|p| p.value().address == addr) {
                continue;
            }

            // Skip if already a candidate
            if self.candidates.contains_key(&addr) {
                continue;
            }

            // Add if under limit
            if self.candidates.len() < self.max_candidates {
                debug!("Added candidate peer: {}", addr);
                self.candidates.insert(addr.clone(), addr);
            }
        }
        Ok(())
    }

    /// Get all candidate peers
    pub async fn get_candidates(&self) -> Vec<String> {
        self.candidates.iter().map(|c| c.value().clone()).collect()
    }

    /// Get candidate count
    pub async fn get_candidate_count(&self) -> usize {
        self.candidates.len()
    }

    /// Remove candidate peer
    pub async fn remove_candidate(&self, addr: &str) -> Result<()> {
        if let Some((_, addr)) = self.candidates.remove(addr) {
            debug!("Removed candidate peer: {}", addr);
        }
        Ok(())
    }

    /// Update peer metrics (latency, bytes sent/received)
    pub async fn update_peer_metrics(
        &self,
        peer_id: &str,
        latency_ms: u64,
        bytes_sent: u64,
        bytes_received: u64,
    ) -> Result<()> {
        if let Some(mut peer) = self.peers.get_mut(peer_id) {
            peer.update_latency(latency_ms);
            if bytes_sent > 0 {
                peer.increment_messages_sent(bytes_sent);
            }
            if bytes_received > 0 {
                peer.increment_messages_received(bytes_received);
            }
            peer.update_last_seen();
        }
        Ok(())
    }

    /// Get average latency across all connected peers
    pub async fn get_average_latency(&self) -> u64 {
        let peers: Vec<_> = self.peers.iter().collect();
        if peers.is_empty() {
            return 0;
        }

        let total_latency: u64 = peers.iter().map(|p| p.value().latency_ms).sum();
        total_latency / peers.len() as u64
    }

    /// Get peers that haven't been seen for a specified duration
    pub async fn get_stale_peers(&self, timeout_secs: u64) -> Vec<String> {
        let now = SystemTime::now();
        let timeout = std::time::Duration::from_secs(timeout_secs);

        self.peers
            .iter()
            .filter_map(|p| {
                if let Ok(elapsed) = now.duration_since(p.value().last_seen) {
                    if elapsed > timeout {
                        return Some(p.key().clone());
                    }
                }
                None
            })
            .collect()
    }

    /// Clear all peers and candidates
    pub async fn clear_all(&self) -> Result<()> {
        self.peers.clear();
        self.candidates.clear();
        debug!("Cleared all peers and candidates");
        Ok(())
    }

    /// Get peer list for peer exchange protocol
    pub async fn get_peer_list_for_exchange(&self) -> Vec<String> {
        self.peers
            .iter()
            .filter(|p| p.value().is_healthy && p.value().is_connected)
            .map(|p| p.value().address.clone())
            .collect()
    }

    /// Process peer list from peer exchange
    pub async fn process_peer_list_response(&self, peer_addresses: Vec<String>) -> Result<()> {
        // Validate addresses
        let valid_addresses: Vec<String> = peer_addresses
            .into_iter()
            .filter(|addr| !addr.is_empty() && addr.contains(':'))
            .collect();

        // Add to candidates
        self.add_candidates(valid_addresses).await?;
        Ok(())
    }

    /// Get peers by role
    pub async fn get_peers_by_role(&self, role: NodeRole) -> Vec<PeerState> {
        self.peers
            .iter()
            .filter(|p| p.value().role == role)
            .map(|p| p.value().clone())
            .collect()
    }

    /// Get connected peers by role
    pub async fn get_connected_peers_by_role(&self, role: NodeRole) -> Vec<PeerState> {
        self.peers
            .iter()
            .filter(|p| p.value().role == role && p.value().is_connected)
            .map(|p| p.value().clone())
            .collect()
    }

    /// Check if peer exists
    pub async fn peer_exists(&self, peer_id: &str) -> bool {
        self.peers.contains_key(peer_id)
    }

    /// Check if address is already known
    pub async fn address_exists(&self, addr: &str) -> bool {
        self.peers.iter().any(|p| p.value().address == addr)
    }

    /// Record successful ping for a peer - increases health score
    pub async fn record_successful_ping(&self, peer_id: &str) -> Result<()> {
        if let Some(mut peer) = self.peers.get_mut(peer_id) {
            peer.record_successful_ping();
            debug!("Recorded successful ping for peer: {} (health_score: {})", peer_id, peer.health_score);
        } else {
            warn!("Attempted to record ping for unknown peer: {}", peer_id);
        }
        Ok(())
    }

    /// Record failed ping for a peer - decreases health score
    pub async fn record_failed_ping(&self, peer_id: &str) -> Result<()> {
        if let Some(mut peer) = self.peers.get_mut(peer_id) {
            peer.record_failed_ping();
            debug!("Recorded failed ping for peer: {} (health_score: {})", peer_id, peer.health_score);
            
            // If health score drops below threshold, mark as unhealthy
            if peer.is_below_unhealthy_threshold() {
                peer.is_healthy = false;
                warn!("Peer {} health score below threshold: {}", peer_id, peer.health_score);
            }
        } else {
            warn!("Attempted to record failed ping for unknown peer: {}", peer_id);
        }
        Ok(())
    }

    /// Get health score for a peer
    pub async fn get_health_score(&self, peer_id: &str) -> Result<u32> {
        self.peers
            .get(peer_id)
            .map(|p| p.value().get_health_score())
            .ok_or_else(|| P2PError::PeerNotFound(peer_id.to_string()))
    }

    /// Get all peers sorted by health score (highest first)
    pub async fn get_peers_by_health_score(&self) -> Vec<PeerState> {
        let mut peers: Vec<_> = self.peers.iter().map(|p| p.value().clone()).collect();
        peers.sort_by(|a, b| b.health_score.cmp(&a.health_score));
        peers
    }

    /// Get healthy peers (health_score >= 50)
    pub async fn get_healthy_peers_by_score(&self) -> Vec<PeerState> {
        self.peers
            .iter()
            .filter(|p| p.value().can_recover())
            .map(|p| p.value().clone())
            .collect()
    }

    /// Get unhealthy peers (health_score < 30)
    pub async fn get_unhealthy_peers_by_score(&self) -> Vec<PeerState> {
        self.peers
            .iter()
            .filter(|p| p.value().is_below_unhealthy_threshold())
            .map(|p| p.value().clone())
            .collect()
    }

    /// Get average health score across all peers
    pub async fn get_average_health_score(&self) -> u32 {
        let peers: Vec<_> = self.peers.iter().collect();
        if peers.is_empty() {
            return 100;
        }

        let total_score: u32 = peers.iter().map(|p| p.value().health_score).sum();
        total_score / peers.len() as u32
    }

    /// Reset health score for a peer (recovery mechanism)
    pub async fn reset_peer_health_score(&self, peer_id: &str) -> Result<()> {
        if let Some(mut peer) = self.peers.get_mut(peer_id) {
            peer.reset_health_score();
            peer.is_healthy = true;
            debug!("Reset health score for peer: {}", peer_id);
        } else {
            warn!("Attempted to reset health score for unknown peer: {}", peer_id);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_peer_manager_creation() {
        let pm = PeerManager::new(1000);
        assert_eq!(pm.get_peer_count().await, 0);
        assert_eq!(pm.get_candidate_count().await, 0);
    }

    #[tokio::test]
    async fn test_add_peer() {
        let pm = PeerManager::new(1000);
        pm.add_peer(
            "peer1".to_string(),
            "127.0.0.1:9000".to_string(),
            NodeRole::Validator,
        )
        .await
        .unwrap();

        assert_eq!(pm.get_peer_count().await, 1);
        let peer = pm.get_peer("peer1").await.unwrap();
        assert_eq!(peer.peer_id, "peer1");
        assert_eq!(peer.address, "127.0.0.1:9000");
        assert_eq!(peer.role, NodeRole::Validator);
    }

    #[tokio::test]
    async fn test_remove_peer() {
        let pm = PeerManager::new(1000);
        pm.add_peer(
            "peer1".to_string(),
            "127.0.0.1:9000".to_string(),
            NodeRole::Validator,
        )
        .await
        .unwrap();

        assert_eq!(pm.get_peer_count().await, 1);
        pm.remove_peer("peer1").await.unwrap();
        assert_eq!(pm.get_peer_count().await, 0);
    }

    #[tokio::test]
    async fn test_mark_healthy_unhealthy() {
        let pm = PeerManager::new(1000);
        pm.add_peer(
            "peer1".to_string(),
            "127.0.0.1:9000".to_string(),
            NodeRole::Validator,
        )
        .await
        .unwrap();

        let peer = pm.get_peer("peer1").await.unwrap();
        assert!(peer.is_healthy);

        pm.mark_unhealthy("peer1", "test error".to_string())
            .await
            .unwrap();
        let peer = pm.get_peer("peer1").await.unwrap();
        assert!(!peer.is_healthy);
        assert_eq!(peer.last_error, Some("test error".to_string()));

        pm.mark_healthy("peer1").await.unwrap();
        let peer = pm.get_peer("peer1").await.unwrap();
        assert!(peer.is_healthy);
        assert_eq!(peer.last_error, None);
    }

    #[tokio::test]
    async fn test_mark_connected_disconnected() {
        let pm = PeerManager::new(1000);
        pm.add_peer(
            "peer1".to_string(),
            "127.0.0.1:9000".to_string(),
            NodeRole::Validator,
        )
        .await
        .unwrap();

        assert_eq!(pm.get_connected_peer_count().await, 0);

        pm.mark_connected("peer1").await.unwrap();
        assert_eq!(pm.get_connected_peer_count().await, 1);

        pm.mark_disconnected("peer1").await.unwrap();
        assert_eq!(pm.get_connected_peer_count().await, 0);
    }

    #[tokio::test]
    async fn test_add_candidates() {
        let pm = PeerManager::new(1000);
        let addrs = vec![
            "127.0.0.1:9001".to_string(),
            "127.0.0.1:9002".to_string(),
            "127.0.0.1:9003".to_string(),
        ];

        pm.add_candidates(addrs).await.unwrap();
        assert_eq!(pm.get_candidate_count().await, 3);

        let candidates = pm.get_candidates().await;
        assert_eq!(candidates.len(), 3);
    }

    #[tokio::test]
    async fn test_candidate_deduplication() {
        let pm = PeerManager::new(1000);
        let addrs = vec![
            "127.0.0.1:9001".to_string(),
            "127.0.0.1:9001".to_string(),
            "127.0.0.1:9002".to_string(),
        ];

        pm.add_candidates(addrs).await.unwrap();
        assert_eq!(pm.get_candidate_count().await, 2);
    }

    #[tokio::test]
    async fn test_candidate_limit() {
        let pm = PeerManager::new(3);
        let addrs = vec![
            "127.0.0.1:9001".to_string(),
            "127.0.0.1:9002".to_string(),
            "127.0.0.1:9003".to_string(),
            "127.0.0.1:9004".to_string(),
            "127.0.0.1:9005".to_string(),
        ];

        pm.add_candidates(addrs).await.unwrap();
        assert_eq!(pm.get_candidate_count().await, 3);
    }

    #[tokio::test]
    async fn test_update_peer_metrics() {
        let pm = PeerManager::new(1000);
        pm.add_peer(
            "peer1".to_string(),
            "127.0.0.1:9000".to_string(),
            NodeRole::Validator,
        )
        .await
        .unwrap();

        pm.update_peer_metrics("peer1", 50, 1024, 2048)
            .await
            .unwrap();

        let peer = pm.get_peer("peer1").await.unwrap();
        assert_eq!(peer.latency_ms, 50);
        assert_eq!(peer.messages_sent, 1);
        assert_eq!(peer.bytes_sent, 1024);
        assert_eq!(peer.messages_received, 1);
        assert_eq!(peer.bytes_received, 2048);
    }

    #[tokio::test]
    async fn test_get_average_latency() {
        let pm = PeerManager::new(1000);
        pm.add_peer(
            "peer1".to_string(),
            "127.0.0.1:9000".to_string(),
            NodeRole::Validator,
        )
        .await
        .unwrap();
        pm.add_peer(
            "peer2".to_string(),
            "127.0.0.1:9001".to_string(),
            NodeRole::Validator,
        )
        .await
        .unwrap();

        pm.update_peer_metrics("peer1", 40, 0, 0).await.unwrap();
        pm.update_peer_metrics("peer2", 60, 0, 0).await.unwrap();

        let avg = pm.get_average_latency().await;
        assert_eq!(avg, 50);
    }

    #[tokio::test]
    async fn test_get_stale_peers() {
        let pm = PeerManager::new(1000);
        pm.add_peer(
            "peer1".to_string(),
            "127.0.0.1:9000".to_string(),
            NodeRole::Validator,
        )
        .await
        .unwrap();

        // With 0 timeout, peer should be stale immediately
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        let stale = pm.get_stale_peers(0).await;
        assert_eq!(stale.len(), 1);

        // With large timeout, peer should not be stale
        let stale = pm.get_stale_peers(3600).await;
        assert_eq!(stale.len(), 0);
    }

    #[tokio::test]
    async fn test_clear_all() {
        let pm = PeerManager::new(1000);
        pm.add_peer(
            "peer1".to_string(),
            "127.0.0.1:9000".to_string(),
            NodeRole::Validator,
        )
        .await
        .unwrap();
        pm.add_candidates(vec!["127.0.0.1:9001".to_string()])
            .await
            .unwrap();

        assert_eq!(pm.get_peer_count().await, 1);
        assert_eq!(pm.get_candidate_count().await, 1);

        pm.clear_all().await.unwrap();
        assert_eq!(pm.get_peer_count().await, 0);
        assert_eq!(pm.get_candidate_count().await, 0);
    }

    #[tokio::test]
    async fn test_peer_list_for_exchange() {
        let pm = PeerManager::new(1000);
        pm.add_peer(
            "peer1".to_string(),
            "127.0.0.1:9000".to_string(),
            NodeRole::Validator,
        )
        .await
        .unwrap();
        pm.add_peer(
            "peer2".to_string(),
            "127.0.0.1:9001".to_string(),
            NodeRole::Validator,
        )
        .await
        .unwrap();

        pm.mark_connected("peer1").await.unwrap();
        pm.mark_connected("peer2").await.unwrap();

        let list = pm.get_peer_list_for_exchange().await;
        assert_eq!(list.len(), 2);
        assert!(list.contains(&"127.0.0.1:9000".to_string()));
        assert!(list.contains(&"127.0.0.1:9001".to_string()));
    }

    #[tokio::test]
    async fn test_process_peer_list_response() {
        let pm = PeerManager::new(1000);
        let peer_list = vec![
            "127.0.0.1:9001".to_string(),
            "127.0.0.1:9002".to_string(),
        ];

        pm.process_peer_list_response(peer_list).await.unwrap();
        assert_eq!(pm.get_candidate_count().await, 2);
    }

    #[tokio::test]
    async fn test_get_peers_by_role() {
        let pm = PeerManager::new(1000);
        pm.add_peer(
            "validator1".to_string(),
            "127.0.0.1:9000".to_string(),
            NodeRole::Validator,
        )
        .await
        .unwrap();
        pm.add_peer(
            "rpc1".to_string(),
            "127.0.0.1:9001".to_string(),
            NodeRole::RPC,
        )
        .await
        .unwrap();

        let validators = pm.get_peers_by_role(NodeRole::Validator).await;
        assert_eq!(validators.len(), 1);
        assert_eq!(validators[0].peer_id, "validator1");

        let rpcs = pm.get_peers_by_role(NodeRole::RPC).await;
        assert_eq!(rpcs.len(), 1);
        assert_eq!(rpcs[0].peer_id, "rpc1");
    }

    #[tokio::test]
    async fn test_peer_exists() {
        let pm = PeerManager::new(1000);
        pm.add_peer(
            "peer1".to_string(),
            "127.0.0.1:9000".to_string(),
            NodeRole::Validator,
        )
        .await
        .unwrap();

        assert!(pm.peer_exists("peer1").await);
        assert!(!pm.peer_exists("peer2").await);
    }

    #[tokio::test]
    async fn test_address_exists() {
        let pm = PeerManager::new(1000);
        pm.add_peer(
            "peer1".to_string(),
            "127.0.0.1:9000".to_string(),
            NodeRole::Validator,
        )
        .await
        .unwrap();

        assert!(pm.address_exists("127.0.0.1:9000").await);
        assert!(!pm.address_exists("127.0.0.1:9001").await);
    }

    #[tokio::test]
    async fn test_record_successful_ping() {
        let pm = PeerManager::new(1000);
        pm.add_peer(
            "peer1".to_string(),
            "127.0.0.1:9000".to_string(),
            NodeRole::Validator,
        )
        .await
        .unwrap();

        let score_before = pm.get_health_score("peer1").await.unwrap();
        assert_eq!(score_before, 100);

        pm.record_successful_ping("peer1").await.unwrap();
        let score_after = pm.get_health_score("peer1").await.unwrap();
        assert_eq!(score_after, 100); // Already at max
    }

    #[tokio::test]
    async fn test_record_failed_ping() {
        let pm = PeerManager::new(1000);
        pm.add_peer(
            "peer1".to_string(),
            "127.0.0.1:9000".to_string(),
            NodeRole::Validator,
        )
        .await
        .unwrap();

        let score_before = pm.get_health_score("peer1").await.unwrap();
        assert_eq!(score_before, 100);

        pm.record_failed_ping("peer1").await.unwrap();
        let score_after = pm.get_health_score("peer1").await.unwrap();
        assert_eq!(score_after, 90);
    }

    #[tokio::test]
    async fn test_health_score_below_threshold_marks_unhealthy() {
        let pm = PeerManager::new(1000);
        pm.add_peer(
            "peer1".to_string(),
            "127.0.0.1:9000".to_string(),
            NodeRole::Validator,
        )
        .await
        .unwrap();

        // Simulate 8 failed pings to drop score to 20
        for _ in 0..8 {
            pm.record_failed_ping("peer1").await.unwrap();
        }

        let peer = pm.get_peer("peer1").await.unwrap();
        assert_eq!(peer.health_score, 20);
        assert!(!peer.is_healthy);
    }

    #[tokio::test]
    async fn test_get_peers_by_health_score() {
        let pm = PeerManager::new(1000);
        pm.add_peer(
            "peer1".to_string(),
            "127.0.0.1:9000".to_string(),
            NodeRole::Validator,
        )
        .await
        .unwrap();
        pm.add_peer(
            "peer2".to_string(),
            "127.0.0.1:9001".to_string(),
            NodeRole::Validator,
        )
        .await
        .unwrap();

        // Simulate failures for peer2
        for _ in 0..3 {
            pm.record_failed_ping("peer2").await.unwrap();
        }

        let peers = pm.get_peers_by_health_score().await;
        assert_eq!(peers.len(), 2);
        assert_eq!(peers[0].peer_id, "peer1"); // Higher score (100)
        assert_eq!(peers[1].peer_id, "peer2"); // Lower score (70)
    }

    #[tokio::test]
    async fn test_get_healthy_peers_by_score() {
        let pm = PeerManager::new(1000);
        pm.add_peer(
            "peer1".to_string(),
            "127.0.0.1:9000".to_string(),
            NodeRole::Validator,
        )
        .await
        .unwrap();
        pm.add_peer(
            "peer2".to_string(),
            "127.0.0.1:9001".to_string(),
            NodeRole::Validator,
        )
        .await
        .unwrap();

        // Simulate failures for peer2 to drop below 50
        for _ in 0..6 {
            pm.record_failed_ping("peer2").await.unwrap();
        }

        let healthy = pm.get_healthy_peers_by_score().await;
        assert_eq!(healthy.len(), 1);
        assert_eq!(healthy[0].peer_id, "peer1");
    }

    #[tokio::test]
    async fn test_get_unhealthy_peers_by_score() {
        let pm = PeerManager::new(1000);
        pm.add_peer(
            "peer1".to_string(),
            "127.0.0.1:9000".to_string(),
            NodeRole::Validator,
        )
        .await
        .unwrap();
        pm.add_peer(
            "peer2".to_string(),
            "127.0.0.1:9001".to_string(),
            NodeRole::Validator,
        )
        .await
        .unwrap();

        // Simulate failures for peer2 to drop below 30
        for _ in 0..8 {
            pm.record_failed_ping("peer2").await.unwrap();
        }

        let unhealthy = pm.get_unhealthy_peers_by_score().await;
        assert_eq!(unhealthy.len(), 1);
        assert_eq!(unhealthy[0].peer_id, "peer2");
    }

    #[tokio::test]
    async fn test_get_average_health_score() {
        let pm = PeerManager::new(1000);
        pm.add_peer(
            "peer1".to_string(),
            "127.0.0.1:9000".to_string(),
            NodeRole::Validator,
        )
        .await
        .unwrap();
        pm.add_peer(
            "peer2".to_string(),
            "127.0.0.1:9001".to_string(),
            NodeRole::Validator,
        )
        .await
        .unwrap();

        // peer1: 100, peer2: 100, average: 100
        let avg = pm.get_average_health_score().await;
        assert_eq!(avg, 100);

        // Simulate failures for peer2
        for _ in 0..2 {
            pm.record_failed_ping("peer2").await.unwrap();
        }

        // peer1: 100, peer2: 80, average: 90
        let avg = pm.get_average_health_score().await;
        assert_eq!(avg, 90);
    }

    #[tokio::test]
    async fn test_reset_peer_health_score() {
        let pm = PeerManager::new(1000);
        pm.add_peer(
            "peer1".to_string(),
            "127.0.0.1:9000".to_string(),
            NodeRole::Validator,
        )
        .await
        .unwrap();

        // Simulate failures to drop below threshold
        for _ in 0..8 {
            pm.record_failed_ping("peer1").await.unwrap();
        }

        let peer = pm.get_peer("peer1").await.unwrap();
        assert_eq!(peer.health_score, 20);
        assert!(!peer.is_healthy);

        // Reset
        pm.reset_peer_health_score("peer1").await.unwrap();

        let peer = pm.get_peer("peer1").await.unwrap();
        assert_eq!(peer.health_score, 100);
        assert!(peer.is_healthy);
    }
}
