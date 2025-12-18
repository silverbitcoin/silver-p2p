//! Unit tests for peer metrics queries functionality
//! Tests the get_peer_metrics() and get_all_peer_metrics() methods to ensure peer metrics retrieval works correctly

use silver_p2p::network_manager::P2PNetworkManager;
use silver_p2p::config::NetworkConfig;
use silver_p2p::types::NodeRole;

#[tokio::test]
async fn test_get_peer_metrics_not_found() {
    // Test that get_peer_metrics returns error for non-existent peer
    let config = NetworkConfig::new("node1".to_string(), NodeRole::Validator);
    let manager = P2PNetworkManager::new(config).await.unwrap();
    
    let result = manager.get_peer_metrics("nonexistent").await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_get_peer_metrics_initial_state() {
    // Test that get_peer_metrics returns correct initial metrics for a new peer
    let config = NetworkConfig::new("node1".to_string(), NodeRole::Validator);
    let manager = P2PNetworkManager::new(config).await.unwrap();
    
    manager.peer_manager.add_peer(
        "peer1".to_string(),
        "127.0.0.1:9000".to_string(),
        NodeRole::Validator,
    ).await.unwrap();
    
    let metrics = manager.get_peer_metrics("peer1").await.unwrap();
    assert_eq!(metrics.messages_sent, 0);
    assert_eq!(metrics.messages_received, 0);
    assert_eq!(metrics.bytes_sent, 0);
    assert_eq!(metrics.bytes_received, 0);
    assert_eq!(metrics.latency_ms, 0);
    assert!(!metrics.is_connected);
}

#[tokio::test]
async fn test_get_peer_metrics_after_update() {
    // Test that get_peer_metrics returns updated metrics after metrics update
    let config = NetworkConfig::new("node1".to_string(), NodeRole::Validator);
    let manager = P2PNetworkManager::new(config).await.unwrap();
    
    manager.peer_manager.add_peer(
        "peer1".to_string(),
        "127.0.0.1:9000".to_string(),
        NodeRole::Validator,
    ).await.unwrap();
    
    // Update metrics
    manager.peer_manager.update_peer_metrics("peer1", 50, 1024, 2048).await.unwrap();
    
    let metrics = manager.get_peer_metrics("peer1").await.unwrap();
    assert_eq!(metrics.messages_sent, 1);
    assert_eq!(metrics.messages_received, 1);
    assert_eq!(metrics.bytes_sent, 1024);
    assert_eq!(metrics.bytes_received, 2048);
    assert_eq!(metrics.latency_ms, 50);
}

#[tokio::test]
async fn test_get_peer_metrics_multiple_updates() {
    // Test that get_peer_metrics accumulates metrics correctly over multiple updates
    let config = NetworkConfig::new("node1".to_string(), NodeRole::Validator);
    let manager = P2PNetworkManager::new(config).await.unwrap();
    
    manager.peer_manager.add_peer(
        "peer1".to_string(),
        "127.0.0.1:9000".to_string(),
        NodeRole::Validator,
    ).await.unwrap();
    
    // First update
    manager.peer_manager.update_peer_metrics("peer1", 50, 1024, 2048).await.unwrap();
    
    // Second update
    manager.peer_manager.update_peer_metrics("peer1", 60, 512, 1024).await.unwrap();
    
    let metrics = manager.get_peer_metrics("peer1").await.unwrap();
    assert_eq!(metrics.messages_sent, 2);
    assert_eq!(metrics.messages_received, 2);
    assert_eq!(metrics.bytes_sent, 1536); // 1024 + 512
    assert_eq!(metrics.bytes_received, 3072); // 2048 + 1024
    assert_eq!(metrics.latency_ms, 60); // Latest latency
}

#[tokio::test]
async fn test_get_peer_metrics_connection_status() {
    // Test that get_peer_metrics includes connection status
    let config = NetworkConfig::new("node1".to_string(), NodeRole::Validator);
    let manager = P2PNetworkManager::new(config).await.unwrap();
    
    manager.peer_manager.add_peer(
        "peer1".to_string(),
        "127.0.0.1:9000".to_string(),
        NodeRole::Validator,
    ).await.unwrap();
    
    // Initially disconnected
    let metrics = manager.get_peer_metrics("peer1").await.unwrap();
    assert!(!metrics.is_connected);
    
    // Mark as connected
    manager.peer_manager.mark_connected("peer1").await.unwrap();
    
    let metrics = manager.get_peer_metrics("peer1").await.unwrap();
    assert!(metrics.is_connected);
}

#[tokio::test]
async fn test_get_all_peer_metrics_empty() {
    // Test that get_all_peer_metrics returns empty map when no peers exist
    let config = NetworkConfig::new("node1".to_string(), NodeRole::Validator);
    let manager = P2PNetworkManager::new(config).await.unwrap();
    
    let metrics = manager.get_all_peer_metrics().await;
    assert_eq!(metrics.len(), 0);
}

#[tokio::test]
async fn test_get_all_peer_metrics_single_peer() {
    // Test that get_all_peer_metrics returns metrics for a single peer
    let config = NetworkConfig::new("node1".to_string(), NodeRole::Validator);
    let manager = P2PNetworkManager::new(config).await.unwrap();
    
    manager.peer_manager.add_peer(
        "peer1".to_string(),
        "127.0.0.1:9000".to_string(),
        NodeRole::Validator,
    ).await.unwrap();
    
    manager.peer_manager.update_peer_metrics("peer1", 50, 1024, 2048).await.unwrap();
    
    let metrics = manager.get_all_peer_metrics().await;
    assert_eq!(metrics.len(), 1);
    assert!(metrics.contains_key("peer1"));
    
    let peer_metrics = &metrics["peer1"];
    assert_eq!(peer_metrics.messages_sent, 1);
    assert_eq!(peer_metrics.bytes_sent, 1024);
    assert_eq!(peer_metrics.latency_ms, 50);
}

#[tokio::test]
async fn test_get_all_peer_metrics_multiple_peers() {
    // Test that get_all_peer_metrics returns metrics for all peers
    let config = NetworkConfig::new("node1".to_string(), NodeRole::Validator);
    let manager = P2PNetworkManager::new(config).await.unwrap();
    
    // Add multiple peers with different metrics
    manager.peer_manager.add_peer(
        "peer1".to_string(),
        "127.0.0.1:9000".to_string(),
        NodeRole::Validator,
    ).await.unwrap();
    
    manager.peer_manager.add_peer(
        "peer2".to_string(),
        "127.0.0.1:9001".to_string(),
        NodeRole::RPC,
    ).await.unwrap();
    
    manager.peer_manager.add_peer(
        "peer3".to_string(),
        "127.0.0.1:9002".to_string(),
        NodeRole::Archive,
    ).await.unwrap();
    
    // Update metrics for each peer
    manager.peer_manager.update_peer_metrics("peer1", 50, 1024, 2048).await.unwrap();
    manager.peer_manager.update_peer_metrics("peer2", 60, 512, 1024).await.unwrap();
    manager.peer_manager.update_peer_metrics("peer3", 40, 2048, 4096).await.unwrap();
    
    let metrics = manager.get_all_peer_metrics().await;
    assert_eq!(metrics.len(), 3);
    
    // Verify each peer's metrics
    assert_eq!(metrics["peer1"].messages_sent, 1);
    assert_eq!(metrics["peer1"].bytes_sent, 1024);
    assert_eq!(metrics["peer1"].latency_ms, 50);
    
    assert_eq!(metrics["peer2"].messages_sent, 1);
    assert_eq!(metrics["peer2"].bytes_sent, 512);
    assert_eq!(metrics["peer2"].latency_ms, 60);
    
    assert_eq!(metrics["peer3"].messages_sent, 1);
    assert_eq!(metrics["peer3"].bytes_sent, 2048);
    assert_eq!(metrics["peer3"].latency_ms, 40);
}

#[tokio::test]
async fn test_get_all_peer_metrics_consistency() {
    // Test that get_all_peer_metrics and get_peer_metrics return consistent data
    let config = NetworkConfig::new("node1".to_string(), NodeRole::Validator);
    let manager = P2PNetworkManager::new(config).await.unwrap();
    
    manager.peer_manager.add_peer(
        "peer1".to_string(),
        "127.0.0.1:9000".to_string(),
        NodeRole::Validator,
    ).await.unwrap();
    
    manager.peer_manager.update_peer_metrics("peer1", 50, 1024, 2048).await.unwrap();
    
    let single_metrics = manager.get_peer_metrics("peer1").await.unwrap();
    let all_metrics = manager.get_all_peer_metrics().await;
    
    let peer1_metrics = &all_metrics["peer1"];
    assert_eq!(single_metrics.messages_sent, peer1_metrics.messages_sent);
    assert_eq!(single_metrics.messages_received, peer1_metrics.messages_received);
    assert_eq!(single_metrics.bytes_sent, peer1_metrics.bytes_sent);
    assert_eq!(single_metrics.bytes_received, peer1_metrics.bytes_received);
    assert_eq!(single_metrics.latency_ms, peer1_metrics.latency_ms);
    assert_eq!(single_metrics.is_connected, peer1_metrics.is_connected);
}

#[tokio::test]
async fn test_get_all_peer_metrics_after_peer_removal() {
    // Test that get_all_peer_metrics reflects peer removal
    let config = NetworkConfig::new("node1".to_string(), NodeRole::Validator);
    let manager = P2PNetworkManager::new(config).await.unwrap();
    
    manager.peer_manager.add_peer(
        "peer1".to_string(),
        "127.0.0.1:9000".to_string(),
        NodeRole::Validator,
    ).await.unwrap();
    
    manager.peer_manager.add_peer(
        "peer2".to_string(),
        "127.0.0.1:9001".to_string(),
        NodeRole::Validator,
    ).await.unwrap();
    
    manager.peer_manager.update_peer_metrics("peer1", 50, 1024, 2048).await.unwrap();
    manager.peer_manager.update_peer_metrics("peer2", 60, 512, 1024).await.unwrap();
    
    let metrics = manager.get_all_peer_metrics().await;
    assert_eq!(metrics.len(), 2);
    
    // Remove a peer
    manager.peer_manager.remove_peer("peer1").await.unwrap();
    
    let metrics = manager.get_all_peer_metrics().await;
    assert_eq!(metrics.len(), 1);
    assert!(!metrics.contains_key("peer1"));
    assert!(metrics.contains_key("peer2"));
}

#[tokio::test]
async fn test_get_peer_metrics_large_values() {
    // Test that get_peer_metrics handles large metric values correctly
    let config = NetworkConfig::new("node1".to_string(), NodeRole::Validator);
    let manager = P2PNetworkManager::new(config).await.unwrap();
    
    manager.peer_manager.add_peer(
        "peer1".to_string(),
        "127.0.0.1:9000".to_string(),
        NodeRole::Validator,
    ).await.unwrap();
    
    // Update with large values
    let large_bytes = 1_000_000_000u64; // 1GB
    let large_latency = 5000u64; // 5 seconds
    manager.peer_manager.update_peer_metrics("peer1", large_latency, large_bytes, large_bytes).await.unwrap();
    
    let metrics = manager.get_peer_metrics("peer1").await.unwrap();
    assert_eq!(metrics.bytes_sent, large_bytes);
    assert_eq!(metrics.bytes_received, large_bytes);
    assert_eq!(metrics.latency_ms, large_latency);
}

#[tokio::test]
async fn test_get_all_peer_metrics_non_blocking() {
    // Test that get_all_peer_metrics is non-blocking and returns immediately
    let config = NetworkConfig::new("node1".to_string(), NodeRole::Validator);
    let manager = P2PNetworkManager::new(config).await.unwrap();
    
    // Add many peers
    for i in 0..100 {
        manager.peer_manager.add_peer(
            format!("peer{}", i),
            format!("127.0.0.1:{}", 9000 + i),
            NodeRole::Validator,
        ).await.unwrap();
        
        manager.peer_manager.update_peer_metrics(
            &format!("peer{}", i),
            50 + i as u64,
            1024 * (i as u64 + 1),
            2048 * (i as u64 + 1),
        ).await.unwrap();
    }
    
    // Measure time to get all metrics
    let start = std::time::Instant::now();
    let metrics = manager.get_all_peer_metrics().await;
    let elapsed = start.elapsed();
    
    assert_eq!(metrics.len(), 100);
    // Should complete in less than 100ms even with 100 peers
    assert!(elapsed.as_millis() < 100);
}

#[tokio::test]
async fn test_get_peer_metrics_zero_bytes() {
    // Test that get_peer_metrics correctly handles zero bytes sent/received
    let config = NetworkConfig::new("node1".to_string(), NodeRole::Validator);
    let manager = P2PNetworkManager::new(config).await.unwrap();
    
    manager.peer_manager.add_peer(
        "peer1".to_string(),
        "127.0.0.1:9000".to_string(),
        NodeRole::Validator,
    ).await.unwrap();
    
    // Update with zero bytes (only latency)
    manager.peer_manager.update_peer_metrics("peer1", 50, 0, 0).await.unwrap();
    
    let metrics = manager.get_peer_metrics("peer1").await.unwrap();
    assert_eq!(metrics.messages_sent, 0);
    assert_eq!(metrics.messages_received, 0);
    assert_eq!(metrics.bytes_sent, 0);
    assert_eq!(metrics.bytes_received, 0);
    assert_eq!(metrics.latency_ms, 50);
}

#[tokio::test]
async fn test_get_all_peer_metrics_different_roles() {
    // Test that get_all_peer_metrics correctly includes peers with different roles
    let config = NetworkConfig::new("node1".to_string(), NodeRole::Validator);
    let manager = P2PNetworkManager::new(config).await.unwrap();
    
    manager.peer_manager.add_peer(
        "validator1".to_string(),
        "127.0.0.1:9000".to_string(),
        NodeRole::Validator,
    ).await.unwrap();
    
    manager.peer_manager.add_peer(
        "rpc1".to_string(),
        "127.0.0.1:9001".to_string(),
        NodeRole::RPC,
    ).await.unwrap();
    
    manager.peer_manager.add_peer(
        "archive1".to_string(),
        "127.0.0.1:9002".to_string(),
        NodeRole::Archive,
    ).await.unwrap();
    
    manager.peer_manager.update_peer_metrics("validator1", 50, 1024, 2048).await.unwrap();
    manager.peer_manager.update_peer_metrics("rpc1", 60, 512, 1024).await.unwrap();
    manager.peer_manager.update_peer_metrics("archive1", 40, 2048, 4096).await.unwrap();
    
    let metrics = manager.get_all_peer_metrics().await;
    assert_eq!(metrics.len(), 3);
    assert!(metrics.contains_key("validator1"));
    assert!(metrics.contains_key("rpc1"));
    assert!(metrics.contains_key("archive1"));
}
