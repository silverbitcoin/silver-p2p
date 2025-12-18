//! Peer metrics tracking and statistics collection

use dashmap::DashMap;
use std::sync::Arc;
use std::time::SystemTime;
use tracing::debug;

/// Metrics for a single peer
#[derive(Clone, Debug)]
pub struct PeerMetrics {
    /// Peer ID
    pub peer_id: String,
    /// Messages sent to peer
    pub messages_sent: u64,
    /// Messages received from peer
    pub messages_received: u64,
    /// Bytes sent to peer
    pub bytes_sent: u64,
    /// Bytes received from peer
    pub bytes_received: u64,
    /// Current latency in milliseconds
    pub latency_ms: u64,
    /// Minimum latency observed
    pub min_latency_ms: u64,
    /// Maximum latency observed
    pub max_latency_ms: u64,
    /// Average latency
    pub avg_latency_ms: u64,
    /// Connection uptime in seconds
    pub uptime_secs: u64,
    /// Last update time
    pub last_updated: SystemTime,
    /// Connection errors count
    pub connection_errors: u32,
    /// Message errors count
    pub message_errors: u32,
    /// Reconnection attempts count
    pub reconnection_attempts: u32,
    /// Successful reconnections count
    pub successful_reconnections: u32,
}

impl PeerMetrics {
    /// Create new peer metrics
    pub fn new(peer_id: String) -> Self {
        Self {
            peer_id,
            messages_sent: 0,
            messages_received: 0,
            bytes_sent: 0,
            bytes_received: 0,
            latency_ms: 0,
            min_latency_ms: u64::MAX,
            max_latency_ms: 0,
            avg_latency_ms: 0,
            uptime_secs: 0,
            last_updated: SystemTime::now(),
            connection_errors: 0,
            message_errors: 0,
            reconnection_attempts: 0,
            successful_reconnections: 0,
        }
    }

    /// Update latency and calculate statistics
    pub fn update_latency(&mut self, latency_ms: u64) {
        self.latency_ms = latency_ms;
        self.min_latency_ms = self.min_latency_ms.min(latency_ms);
        self.max_latency_ms = self.max_latency_ms.max(latency_ms);

        // Update average latency (exponential moving average)
        if self.avg_latency_ms == 0 {
            self.avg_latency_ms = latency_ms;
        } else {
            self.avg_latency_ms = (self.avg_latency_ms * 3 + latency_ms) / 4;
        }

        self.last_updated = SystemTime::now();
    }

    /// Increment messages sent
    pub fn increment_messages_sent(&mut self, bytes: u64) {
        self.messages_sent += 1;
        self.bytes_sent += bytes;
        self.last_updated = SystemTime::now();
    }

    /// Increment messages received
    pub fn increment_messages_received(&mut self, bytes: u64) {
        self.messages_received += 1;
        self.bytes_received += bytes;
        self.last_updated = SystemTime::now();
    }

    /// Increment connection errors
    pub fn increment_connection_errors(&mut self) {
        self.connection_errors += 1;
        self.last_updated = SystemTime::now();
    }

    /// Increment message errors
    pub fn increment_message_errors(&mut self) {
        self.message_errors += 1;
        self.last_updated = SystemTime::now();
    }

    /// Increment reconnection attempts
    pub fn increment_reconnection_attempts(&mut self) {
        self.reconnection_attempts += 1;
        self.last_updated = SystemTime::now();
    }

    /// Increment successful reconnections
    pub fn increment_successful_reconnections(&mut self) {
        self.successful_reconnections += 1;
        self.last_updated = SystemTime::now();
    }

    /// Update uptime
    pub fn update_uptime(&mut self, uptime_secs: u64) {
        self.uptime_secs = uptime_secs;
        self.last_updated = SystemTime::now();
    }

    /// Get messages per second
    pub fn messages_per_second(&self) -> f64 {
        if self.uptime_secs == 0 {
            return 0.0;
        }
        (self.messages_sent + self.messages_received) as f64 / self.uptime_secs as f64
    }

    /// Get bytes per second
    pub fn bytes_per_second(&self) -> f64 {
        if self.uptime_secs == 0 {
            return 0.0;
        }
        (self.bytes_sent + self.bytes_received) as f64 / self.uptime_secs as f64
    }

    /// Get error rate
    pub fn error_rate(&self) -> f64 {
        let total_errors = (self.connection_errors + self.message_errors) as u64;
        let total_messages = self.messages_sent + self.messages_received;

        if total_messages == 0 {
            return 0.0;
        }

        total_errors as f64 / total_messages as f64
    }

    /// Reset metrics
    pub fn reset(&mut self) {
        self.messages_sent = 0;
        self.messages_received = 0;
        self.bytes_sent = 0;
        self.bytes_received = 0;
        self.latency_ms = 0;
        self.min_latency_ms = u64::MAX;
        self.max_latency_ms = 0;
        self.avg_latency_ms = 0;
        self.connection_errors = 0;
        self.message_errors = 0;
        self.reconnection_attempts = 0;
        self.successful_reconnections = 0;
        self.last_updated = SystemTime::now();
    }
}

/// Metrics tracker for all peers
pub struct MetricsTracker {
    /// Metrics for each peer
    metrics: Arc<DashMap<String, PeerMetrics>>,
    /// Start time for uptime calculation
    start_time: SystemTime,
}

impl MetricsTracker {
    /// Create new metrics tracker
    pub fn new() -> Self {
        Self {
            metrics: Arc::new(DashMap::new()),
            start_time: SystemTime::now(),
        }
    }

    /// Get or create metrics for peer
    pub fn get_or_create(&self, peer_id: &str) -> PeerMetrics {
        self.metrics
            .entry(peer_id.to_string())
            .or_insert_with(|| PeerMetrics::new(peer_id.to_string()))
            .clone()
    }

    /// Update peer latency
    pub fn update_latency(&self, peer_id: &str, latency_ms: u64) {
        if let Some(mut metrics) = self.metrics.get_mut(peer_id) {
            metrics.update_latency(latency_ms);
            debug!("Updated latency for peer {}: {}ms", peer_id, latency_ms);
        }
    }

    /// Record message sent
    pub fn record_message_sent(&self, peer_id: &str, bytes: u64) {
        if let Some(mut metrics) = self.metrics.get_mut(peer_id) {
            metrics.increment_messages_sent(bytes);
        }
    }

    /// Record message received
    pub fn record_message_received(&self, peer_id: &str, bytes: u64) {
        if let Some(mut metrics) = self.metrics.get_mut(peer_id) {
            metrics.increment_messages_received(bytes);
        }
    }

    /// Record connection error
    pub fn record_connection_error(&self, peer_id: &str) {
        if let Some(mut metrics) = self.metrics.get_mut(peer_id) {
            metrics.increment_connection_errors();
            debug!("Recorded connection error for peer {}", peer_id);
        }
    }

    /// Record message error
    pub fn record_message_error(&self, peer_id: &str) {
        if let Some(mut metrics) = self.metrics.get_mut(peer_id) {
            metrics.increment_message_errors();
            debug!("Recorded message error for peer {}", peer_id);
        }
    }

    /// Record reconnection attempt
    pub fn record_reconnection_attempt(&self, peer_id: &str) {
        if let Some(mut metrics) = self.metrics.get_mut(peer_id) {
            metrics.increment_reconnection_attempts();
            debug!("Recorded reconnection attempt for peer {}", peer_id);
        }
    }

    /// Record successful reconnection
    pub fn record_successful_reconnection(&self, peer_id: &str) {
        if let Some(mut metrics) = self.metrics.get_mut(peer_id) {
            metrics.increment_successful_reconnections();
            debug!("Recorded successful reconnection for peer {}", peer_id);
        }
    }

    /// Get metrics for peer
    pub fn get_metrics(&self, peer_id: &str) -> Option<PeerMetrics> {
        self.metrics.get(peer_id).map(|m| m.clone())
    }

    /// Get all metrics
    pub fn get_all_metrics(&self) -> Vec<PeerMetrics> {
        self.metrics.iter().map(|m| m.value().clone()).collect()
    }

    /// Get total messages sent across all peers
    pub fn total_messages_sent(&self) -> u64 {
        self.metrics.iter().map(|m| m.value().messages_sent).sum()
    }

    /// Get total messages received across all peers
    pub fn total_messages_received(&self) -> u64 {
        self.metrics.iter().map(|m| m.value().messages_received).sum()
    }

    /// Get total bytes sent across all peers
    pub fn total_bytes_sent(&self) -> u64 {
        self.metrics.iter().map(|m| m.value().bytes_sent).sum()
    }

    /// Get total bytes received across all peers
    pub fn total_bytes_received(&self) -> u64 {
        self.metrics.iter().map(|m| m.value().bytes_received).sum()
    }

    /// Get average latency across all peers
    pub fn average_latency(&self) -> u64 {
        let metrics: Vec<_> = self.metrics.iter().collect();
        if metrics.is_empty() {
            return 0;
        }

        let total: u64 = metrics.iter().map(|m| m.value().latency_ms).sum();
        total / metrics.len() as u64
    }

    /// Get minimum latency across all peers
    pub fn min_latency(&self) -> u64 {
        self.metrics
            .iter()
            .map(|m| m.value().min_latency_ms)
            .min()
            .unwrap_or(0)
    }

    /// Get maximum latency across all peers
    pub fn max_latency(&self) -> u64 {
        self.metrics
            .iter()
            .map(|m| m.value().max_latency_ms)
            .max()
            .unwrap_or(0)
    }

    /// Get total messages per second
    pub fn total_messages_per_second(&self) -> f64 {
        let uptime = self
            .start_time
            .elapsed()
            .unwrap_or_default()
            .as_secs();

        if uptime == 0 {
            return 0.0;
        }

        (self.total_messages_sent() + self.total_messages_received()) as f64 / uptime as f64
    }

    /// Get total bytes per second
    pub fn total_bytes_per_second(&self) -> f64 {
        let uptime = self
            .start_time
            .elapsed()
            .unwrap_or_default()
            .as_secs();

        if uptime == 0 {
            return 0.0;
        }

        (self.total_bytes_sent() + self.total_bytes_received()) as f64 / uptime as f64
    }

    /// Get total error count
    pub fn total_errors(&self) -> u32 {
        self.metrics
            .iter()
            .map(|m| m.value().connection_errors + m.value().message_errors)
            .sum()
    }

    /// Get total reconnection attempts
    pub fn total_reconnection_attempts(&self) -> u32 {
        self.metrics
            .iter()
            .map(|m| m.value().reconnection_attempts)
            .sum()
    }

    /// Get total successful reconnections
    pub fn total_successful_reconnections(&self) -> u32 {
        self.metrics
            .iter()
            .map(|m| m.value().successful_reconnections)
            .sum()
    }

    /// Get reconnection success rate
    pub fn reconnection_success_rate(&self) -> f64 {
        let attempts = self.total_reconnection_attempts();
        if attempts == 0 {
            return 0.0;
        }
        self.total_successful_reconnections() as f64 / attempts as f64
    }

    /// Get uptime in seconds
    pub fn uptime_secs(&self) -> u64 {
        self.start_time
            .elapsed()
            .unwrap_or_default()
            .as_secs()
    }

    /// Remove metrics for peer
    pub fn remove_metrics(&self, peer_id: &str) {
        self.metrics.remove(peer_id);
    }

    /// Clear all metrics
    pub fn clear_all(&self) {
        self.metrics.clear();
    }

    /// Get peer count
    pub fn peer_count(&self) -> usize {
        self.metrics.len()
    }

    /// Get peers sorted by latency (ascending)
    pub fn peers_by_latency(&self) -> Vec<PeerMetrics> {
        let mut metrics: Vec<_> = self.metrics.iter().map(|m| m.value().clone()).collect();
        metrics.sort_by_key(|m| m.latency_ms);
        metrics
    }

    /// Get peers sorted by throughput (descending)
    pub fn peers_by_throughput(&self) -> Vec<PeerMetrics> {
        let mut metrics: Vec<_> = self.metrics.iter().map(|m| m.value().clone()).collect();
        metrics.sort_by(|a, b| {
            let a_throughput = a.bytes_per_second();
            let b_throughput = b.bytes_per_second();
            b_throughput.partial_cmp(&a_throughput).unwrap_or(std::cmp::Ordering::Equal)
        });
        metrics
    }

    /// Get peers sorted by error rate (ascending)
    pub fn peers_by_reliability(&self) -> Vec<PeerMetrics> {
        let mut metrics: Vec<_> = self.metrics.iter().map(|m| m.value().clone()).collect();
        metrics.sort_by(|a, b| {
            let a_error_rate = a.error_rate();
            let b_error_rate = b.error_rate();
            a_error_rate.partial_cmp(&b_error_rate).unwrap_or(std::cmp::Ordering::Equal)
        });
        metrics
    }
}

impl Default for MetricsTracker {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_peer_metrics_creation() {
        let metrics = PeerMetrics::new("peer1".to_string());
        assert_eq!(metrics.peer_id, "peer1");
        assert_eq!(metrics.messages_sent, 0);
        assert_eq!(metrics.messages_received, 0);
    }

    #[test]
    fn test_update_latency() {
        let mut metrics = PeerMetrics::new("peer1".to_string());
        metrics.update_latency(50);
        assert_eq!(metrics.latency_ms, 50);
        assert_eq!(metrics.min_latency_ms, 50);
        assert_eq!(metrics.max_latency_ms, 50);

        metrics.update_latency(100);
        assert_eq!(metrics.latency_ms, 100);
        assert_eq!(metrics.min_latency_ms, 50);
        assert_eq!(metrics.max_latency_ms, 100);
    }

    #[test]
    fn test_messages_per_second() {
        let mut metrics = PeerMetrics::new("peer1".to_string());
        metrics.messages_sent = 100;
        metrics.uptime_secs = 10;
        assert_eq!(metrics.messages_per_second(), 10.0);
    }

    #[test]
    fn test_error_rate() {
        let mut metrics = PeerMetrics::new("peer1".to_string());
        metrics.messages_sent = 100;
        metrics.connection_errors = 10;
        let error_rate = metrics.error_rate();
        assert!(error_rate > 0.0 && error_rate < 1.0);
    }

    #[test]
    fn test_metrics_tracker_creation() {
        let tracker = MetricsTracker::new();
        assert_eq!(tracker.peer_count(), 0);
    }

    #[test]
    fn test_get_or_create_metrics() {
        let tracker = MetricsTracker::new();
        let metrics1 = tracker.get_or_create("peer1");
        assert_eq!(metrics1.peer_id, "peer1");

        let metrics2 = tracker.get_or_create("peer1");
        assert_eq!(metrics2.peer_id, "peer1");
    }

    #[test]
    fn test_record_message_sent() {
        let tracker = MetricsTracker::new();
        // Initialize metrics first
        let _ = tracker.get_or_create("peer1");
        
        tracker.record_message_sent("peer1", 1024);
        tracker.record_message_sent("peer1", 2048);

        let metrics = tracker.get_metrics("peer1").unwrap();
        assert_eq!(metrics.messages_sent, 2);
        assert_eq!(metrics.bytes_sent, 3072);
    }

    #[test]
    fn test_record_message_received() {
        let tracker = MetricsTracker::new();
        // Initialize metrics first
        let _ = tracker.get_or_create("peer1");
        
        tracker.record_message_received("peer1", 512);
        tracker.record_message_received("peer1", 1024);

        let metrics = tracker.get_metrics("peer1").unwrap();
        assert_eq!(metrics.messages_received, 2);
        assert_eq!(metrics.bytes_received, 1536);
    }

    #[test]
    fn test_total_metrics() {
        let tracker = MetricsTracker::new();
        // Initialize metrics first
        let _ = tracker.get_or_create("peer1");
        let _ = tracker.get_or_create("peer2");
        
        tracker.record_message_sent("peer1", 1000);
        tracker.record_message_sent("peer2", 2000);
        tracker.record_message_received("peer1", 500);
        tracker.record_message_received("peer2", 1500);

        assert_eq!(tracker.total_messages_sent(), 2);
        assert_eq!(tracker.total_messages_received(), 2);
        assert_eq!(tracker.total_bytes_sent(), 3000);
        assert_eq!(tracker.total_bytes_received(), 2000);
    }

    #[test]
    fn test_average_latency() {
        let tracker = MetricsTracker::new();
        // Initialize metrics first
        let _ = tracker.get_or_create("peer1");
        let _ = tracker.get_or_create("peer2");
        
        tracker.update_latency("peer1", 40);
        tracker.update_latency("peer2", 60);

        let avg = tracker.average_latency();
        assert_eq!(avg, 50);
    }

    #[test]
    fn test_peers_by_latency() {
        let tracker = MetricsTracker::new();
        // Initialize metrics first
        let _ = tracker.get_or_create("peer1");
        let _ = tracker.get_or_create("peer2");
        let _ = tracker.get_or_create("peer3");
        
        tracker.update_latency("peer1", 100);
        tracker.update_latency("peer2", 50);
        tracker.update_latency("peer3", 75);

        let sorted = tracker.peers_by_latency();
        assert_eq!(sorted[0].peer_id, "peer2");
        assert_eq!(sorted[1].peer_id, "peer3");
        assert_eq!(sorted[2].peer_id, "peer1");
    }

    #[test]
    fn test_record_errors() {
        let tracker = MetricsTracker::new();
        // Initialize metrics first
        let _ = tracker.get_or_create("peer1");
        
        tracker.record_connection_error("peer1");
        tracker.record_connection_error("peer1");
        tracker.record_message_error("peer1");

        let metrics = tracker.get_metrics("peer1").unwrap();
        assert_eq!(metrics.connection_errors, 2);
        assert_eq!(metrics.message_errors, 1);
    }

    #[test]
    fn test_clear_all() {
        let tracker = MetricsTracker::new();
        // Initialize metrics first
        let _ = tracker.get_or_create("peer1");
        let _ = tracker.get_or_create("peer2");
        
        tracker.record_message_sent("peer1", 1000);
        tracker.record_message_sent("peer2", 2000);

        assert_eq!(tracker.peer_count(), 2);
        tracker.clear_all();
        assert_eq!(tracker.peer_count(), 0);
    }
}
