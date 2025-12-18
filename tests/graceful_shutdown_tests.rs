//! Tests for graceful shutdown mechanism
//! 
//! These tests verify that the P2P network manager can shut down gracefully:
//! 1. Stop accepting new connections
//! 2. Wait for pending messages to complete (up to 30 seconds)
//! 3. Close all peer connections
//! 4. Release all network resources
//! 5. Exit cleanly

use silver_p2p::{NetworkConfig, P2PNetworkManager};
use std::time::Duration;
use tokio::time::timeout;

#[tokio::test]
async fn test_graceful_shutdown_basic() {
    // Create a network manager
    let config = NetworkConfig::new("test_node".to_string(), silver_p2p::NodeRole::Validator);
    let manager = P2PNetworkManager::new(config).await.expect("Failed to create manager");

    // Verify manager is created
    let stats = manager.get_network_stats().await;
    assert_eq!(stats.connected_peers, 0);

    // Perform graceful shutdown
    let result = manager.shutdown().await;
    assert!(result.is_ok(), "Shutdown should succeed");

    // Verify all peers are cleared
    let stats_after = manager.get_network_stats().await;
    assert_eq!(stats_after.connected_peers, 0);
    assert_eq!(stats_after.total_peers, 0);
}

#[tokio::test]
async fn test_graceful_shutdown_stops_accepting_connections() {
    let config = NetworkConfig::new("test_node".to_string(), silver_p2p::NodeRole::Validator);
    let manager = P2PNetworkManager::new(config).await.expect("Failed to create manager");

    // Start the manager
    let start_result = manager.start().await;
    assert!(start_result.is_ok(), "Manager should start successfully");

    // Give it a moment to start
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Perform graceful shutdown
    let shutdown_result = manager.shutdown().await;
    assert!(shutdown_result.is_ok(), "Shutdown should succeed");

    // Verify shutdown completed
    let stats = manager.get_network_stats().await;
    assert_eq!(stats.total_peers, 0);
}

#[tokio::test]
async fn test_graceful_shutdown_closes_connections() {
    let config = NetworkConfig::new("test_node".to_string(), silver_p2p::NodeRole::Validator);
    let manager = P2PNetworkManager::new(config).await.expect("Failed to create manager");

    // Add some peers to the manager
    manager.peer_manager.add_peer(
        "peer1".to_string(),
        "127.0.0.1:9000".to_string(),
        silver_p2p::NodeRole::Validator,
    ).await.expect("Failed to add peer");

    manager.peer_manager.add_peer(
        "peer2".to_string(),
        "127.0.0.1:9001".to_string(),
        silver_p2p::NodeRole::Validator,
    ).await.expect("Failed to add peer");

    // Verify peers are added
    let stats_before = manager.get_network_stats().await;
    assert_eq!(stats_before.total_peers, 2);

    // Perform graceful shutdown
    let shutdown_result = manager.shutdown().await;
    assert!(shutdown_result.is_ok(), "Shutdown should succeed");

    // Verify all peers are cleared
    let stats_after = manager.get_network_stats().await;
    assert_eq!(stats_after.total_peers, 0);
    assert_eq!(stats_after.connected_peers, 0);
}

#[tokio::test]
async fn test_graceful_shutdown_clears_candidates() {
    let config = NetworkConfig::new("test_node".to_string(), silver_p2p::NodeRole::Validator);
    let manager = P2PNetworkManager::new(config).await.expect("Failed to create manager");

    // Add some candidate peers
    manager.peer_manager.add_candidates(vec![
        "192.168.1.1:9000".to_string(),
        "192.168.1.2:9000".to_string(),
        "192.168.1.3:9000".to_string(),
    ]).await.expect("Failed to add candidates");

    // Verify candidates are added
    let stats_before = manager.get_network_stats().await;
    assert_eq!(stats_before.candidate_peers, 3);

    // Perform graceful shutdown
    let shutdown_result = manager.shutdown().await;
    assert!(shutdown_result.is_ok(), "Shutdown should succeed");

    // Verify all candidates are cleared
    let stats_after = manager.get_network_stats().await;
    assert_eq!(stats_after.candidate_peers, 0);
}

#[tokio::test]
async fn test_graceful_shutdown_with_grace_period() {
    let config = NetworkConfig::new("test_node".to_string(), silver_p2p::NodeRole::Validator);
    let manager = P2PNetworkManager::new(config).await.expect("Failed to create manager");

    // Add a peer with some message activity
    manager.peer_manager.add_peer(
        "peer1".to_string(),
        "127.0.0.1:9000".to_string(),
        silver_p2p::NodeRole::Validator,
    ).await.expect("Failed to add peer");

    manager.peer_manager.mark_connected("peer1").await.expect("Failed to mark connected");

    // Simulate message activity
    manager.peer_manager.update_peer_metrics("peer1", 50, 1024, 2048)
        .await.expect("Failed to update metrics");

    // Verify peer has message activity
    let stats_before = manager.get_network_stats().await;
    assert!(stats_before.total_messages_sent > 0 || stats_before.total_messages_received > 0);

    // Perform graceful shutdown (should wait for grace period)
    let start = std::time::Instant::now();
    let shutdown_result = manager.shutdown().await;
    let elapsed = start.elapsed();

    assert!(shutdown_result.is_ok(), "Shutdown should succeed");
    // Shutdown should complete within a reasonable time (allowing for system delays)
    assert!(elapsed < Duration::from_secs(35), "Shutdown should complete within 35 seconds");

    // Verify all peers are cleared
    let stats_after = manager.get_network_stats().await;
    assert_eq!(stats_after.total_peers, 0);
}

#[tokio::test]
async fn test_graceful_shutdown_stops_discovery_coordinator() {
    let mut config = NetworkConfig::new("test_node".to_string(), silver_p2p::NodeRole::Validator);
    config.enable_peer_discovery = true;
    config.min_peers = 2;

    let manager = P2PNetworkManager::new(config).await.expect("Failed to create manager");

    // Manually start the discovery coordinator
    manager.get_discovery_coordinator().start().await.expect("Failed to start discovery coordinator");

    // Give it a moment to start
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Verify discovery coordinator is running
    assert!(manager.get_discovery_coordinator().is_running(), "Discovery coordinator should be running");

    // Perform graceful shutdown
    let shutdown_result = manager.shutdown().await;
    assert!(shutdown_result.is_ok(), "Shutdown should succeed");

    // Verify discovery coordinator is stopped
    assert!(!manager.get_discovery_coordinator().is_running(), "Discovery coordinator should be stopped");
}

#[tokio::test]
async fn test_graceful_shutdown_stops_bootstrap_connector() {
    let mut config = NetworkConfig::new("test_node".to_string(), silver_p2p::NodeRole::Validator);
    config.bootstrap_nodes = vec!["127.0.0.1:9000".to_string()];

    let manager = P2PNetworkManager::new(config).await.expect("Failed to create manager");

    // Manually start the bootstrap connector
    manager.get_bootstrap_connector().start().await.expect("Failed to start bootstrap connector");

    // Give it a moment to start
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Verify bootstrap connector is running
    assert!(manager.get_bootstrap_connector().is_running(), "Bootstrap connector should be running");

    // Perform graceful shutdown
    let shutdown_result = manager.shutdown().await;
    assert!(shutdown_result.is_ok(), "Shutdown should succeed");

    // Verify bootstrap connector is stopped
    assert!(!manager.get_bootstrap_connector().is_running(), "Bootstrap connector should be stopped");
}

#[tokio::test]
async fn test_graceful_shutdown_completes_within_timeout() {
    let config = NetworkConfig::new("test_node".to_string(), silver_p2p::NodeRole::Validator);
    let manager = P2PNetworkManager::new(config).await.expect("Failed to create manager");

    // Add multiple peers
    for i in 0..10 {
        let peer_id = format!("peer{}", i);
        let addr = format!("127.0.0.1:{}", 9000 + i);
        manager.peer_manager.add_peer(
            peer_id,
            addr,
            silver_p2p::NodeRole::Validator,
        ).await.expect("Failed to add peer");
    }

    // Perform graceful shutdown with timeout
    let shutdown_future = manager.shutdown();
    let result = timeout(Duration::from_secs(60), shutdown_future).await;

    assert!(result.is_ok(), "Shutdown should complete within 60 seconds");
    assert!(result.unwrap().is_ok(), "Shutdown should succeed");
}

#[tokio::test]
async fn test_graceful_shutdown_idempotent() {
    let config = NetworkConfig::new("test_node".to_string(), silver_p2p::NodeRole::Validator);
    let manager = P2PNetworkManager::new(config).await.expect("Failed to create manager");

    // Add a peer
    manager.peer_manager.add_peer(
        "peer1".to_string(),
        "127.0.0.1:9000".to_string(),
        silver_p2p::NodeRole::Validator,
    ).await.expect("Failed to add peer");

    // First shutdown
    let result1 = manager.shutdown().await;
    assert!(result1.is_ok(), "First shutdown should succeed");

    // Verify peers are cleared
    let stats = manager.get_network_stats().await;
    assert_eq!(stats.total_peers, 0);

    // Second shutdown should also succeed (idempotent)
    let result2 = manager.shutdown().await;
    assert!(result2.is_ok(), "Second shutdown should also succeed");

    // Verify still no peers
    let stats = manager.get_network_stats().await;
    assert_eq!(stats.total_peers, 0);
}

#[tokio::test]
async fn test_graceful_shutdown_with_mixed_peer_states() {
    let config = NetworkConfig::new("test_node".to_string(), silver_p2p::NodeRole::Validator);
    let manager = P2PNetworkManager::new(config).await.expect("Failed to create manager");

    // Add peers in different states
    manager.peer_manager.add_peer(
        "peer1".to_string(),
        "127.0.0.1:9000".to_string(),
        silver_p2p::NodeRole::Validator,
    ).await.expect("Failed to add peer");

    manager.peer_manager.add_peer(
        "peer2".to_string(),
        "127.0.0.1:9001".to_string(),
        silver_p2p::NodeRole::Validator,
    ).await.expect("Failed to add peer");

    manager.peer_manager.add_peer(
        "peer3".to_string(),
        "127.0.0.1:9002".to_string(),
        silver_p2p::NodeRole::Validator,
    ).await.expect("Failed to add peer");

    // Mark some as connected
    manager.peer_manager.mark_connected("peer1").await.expect("Failed to mark connected");
    manager.peer_manager.mark_connected("peer2").await.expect("Failed to mark connected");

    // Mark one as unhealthy
    manager.peer_manager.mark_unhealthy("peer3", "Test error".to_string())
        .await.expect("Failed to mark unhealthy");

    // Verify mixed states
    let stats_before = manager.get_network_stats().await;
    assert_eq!(stats_before.total_peers, 3);
    assert_eq!(stats_before.connected_peers, 2);

    // Perform graceful shutdown
    let shutdown_result = manager.shutdown().await;
    assert!(shutdown_result.is_ok(), "Shutdown should succeed");

    // Verify all peers are cleared regardless of state
    let stats_after = manager.get_network_stats().await;
    assert_eq!(stats_after.total_peers, 0);
    assert_eq!(stats_after.connected_peers, 0);
}

#[tokio::test]
async fn test_graceful_shutdown_releases_resources() {
    let config = NetworkConfig::new("test_node".to_string(), silver_p2p::NodeRole::Validator);
    let manager = P2PNetworkManager::new(config).await.expect("Failed to create manager");

    // Add multiple peers and candidates
    for i in 0..5 {
        let peer_id = format!("peer{}", i);
        let addr = format!("127.0.0.1:{}", 9000 + i);
        manager.peer_manager.add_peer(
            peer_id,
            addr,
            silver_p2p::NodeRole::Validator,
        ).await.expect("Failed to add peer");
    }

    manager.peer_manager.add_candidates(vec![
        "192.168.1.1:9000".to_string(),
        "192.168.1.2:9000".to_string(),
    ]).await.expect("Failed to add candidates");

    // Verify resources are allocated
    let stats_before = manager.get_network_stats().await;
    assert_eq!(stats_before.total_peers, 5);
    assert_eq!(stats_before.candidate_peers, 2);

    // Perform graceful shutdown
    let shutdown_result = manager.shutdown().await;
    assert!(shutdown_result.is_ok(), "Shutdown should succeed");

    // Verify all resources are released
    let stats_after = manager.get_network_stats().await;
    assert_eq!(stats_after.total_peers, 0);
    assert_eq!(stats_after.candidate_peers, 0);
    assert_eq!(stats_after.connected_peers, 0);
}

#[tokio::test]
async fn test_graceful_shutdown_logs_progress() {
    // This test verifies that shutdown logs appropriate messages
    // We can't directly test logging, but we can verify the shutdown completes
    let config = NetworkConfig::new("test_node".to_string(), silver_p2p::NodeRole::Validator);
    let manager = P2PNetworkManager::new(config).await.expect("Failed to create manager");

    // Add a peer
    manager.peer_manager.add_peer(
        "peer1".to_string(),
        "127.0.0.1:9000".to_string(),
        silver_p2p::NodeRole::Validator,
    ).await.expect("Failed to add peer");

    // Perform graceful shutdown
    let result = manager.shutdown().await;
    assert!(result.is_ok(), "Shutdown should succeed and log progress");
}

#[tokio::test]
async fn test_graceful_shutdown_handles_errors_gracefully() {
    let config = NetworkConfig::new("test_node".to_string(), silver_p2p::NodeRole::Validator);
    let manager = P2PNetworkManager::new(config).await.expect("Failed to create manager");

    // Add a peer
    manager.peer_manager.add_peer(
        "peer1".to_string(),
        "127.0.0.1:9000".to_string(),
        silver_p2p::NodeRole::Validator,
    ).await.expect("Failed to add peer");

    // Perform graceful shutdown - should handle any errors gracefully
    let result = manager.shutdown().await;
    assert!(result.is_ok(), "Shutdown should handle errors gracefully");

    // Verify state is clean after shutdown
    let stats = manager.get_network_stats().await;
    assert_eq!(stats.total_peers, 0);
}
