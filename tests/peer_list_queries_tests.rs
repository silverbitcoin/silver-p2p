//! Unit tests for peer list queries functionality
//! Tests the get_all_peers() method to ensure peer list retrieval works correctly

use silver_p2p::network_manager::P2PNetworkManager;
use silver_p2p::config::NetworkConfig;
use silver_p2p::types::NodeRole;

#[tokio::test]
async fn test_get_all_peers_empty() {
    // Test that get_all_peers returns empty list when no peers are added
    let config = NetworkConfig::new("node1".to_string(), NodeRole::Validator);
    let manager = P2PNetworkManager::new(config).await.unwrap();
    
    let peers = manager.get_all_peers().await;
    assert_eq!(peers.len(), 0);
}

#[tokio::test]
async fn test_get_all_peers_single_peer() {
    // Test that get_all_peers returns correct peer information for a single peer
    let config = NetworkConfig::new("node1".to_string(), NodeRole::Validator);
    let manager = P2PNetworkManager::new(config).await.unwrap();
    
    // Add a peer through the peer manager
    manager.peer_manager.add_peer(
        "peer1".to_string(),
        "127.0.0.1:9000".to_string(),
        NodeRole::Validator,
    ).await.unwrap();
    
    let peers = manager.get_all_peers().await;
    assert_eq!(peers.len(), 1);
    assert_eq!(peers[0].peer_id, "peer1");
    assert_eq!(peers[0].address, "127.0.0.1:9000");
    assert_eq!(peers[0].role, "Validator");
    assert!(!peers[0].is_connected);
    assert!(peers[0].is_healthy);
}

#[tokio::test]
async fn test_get_all_peers_multiple_peers() {
    // Test that get_all_peers returns all peers with correct status
    let config = NetworkConfig::new("node1".to_string(), NodeRole::Validator);
    let manager = P2PNetworkManager::new(config).await.unwrap();
    
    // Add multiple peers
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
    
    let peers = manager.get_all_peers().await;
    assert_eq!(peers.len(), 3);
    
    // Verify all peers are present
    let peer_ids: Vec<_> = peers.iter().map(|p| p.peer_id.clone()).collect();
    assert!(peer_ids.contains(&"peer1".to_string()));
    assert!(peer_ids.contains(&"peer2".to_string()));
    assert!(peer_ids.contains(&"peer3".to_string()));
}

#[tokio::test]
async fn test_get_all_peers_includes_connection_status() {
    // Test that get_all_peers includes accurate connection status
    let config = NetworkConfig::new("node1".to_string(), NodeRole::Validator);
    let manager = P2PNetworkManager::new(config).await.unwrap();
    
    manager.peer_manager.add_peer(
        "peer1".to_string(),
        "127.0.0.1:9000".to_string(),
        NodeRole::Validator,
    ).await.unwrap();
    
    // Initially disconnected
    let peers = manager.get_all_peers().await;
    assert!(!peers[0].is_connected);
    
    // Mark as connected
    manager.peer_manager.mark_connected("peer1").await.unwrap();
    
    let peers = manager.get_all_peers().await;
    assert!(peers[0].is_connected);
}

#[tokio::test]
async fn test_get_all_peers_includes_health_status() {
    // Test that get_all_peers includes accurate health status
    let config = NetworkConfig::new("node1".to_string(), NodeRole::Validator);
    let manager = P2PNetworkManager::new(config).await.unwrap();
    
    manager.peer_manager.add_peer(
        "peer1".to_string(),
        "127.0.0.1:9000".to_string(),
        NodeRole::Validator,
    ).await.unwrap();
    
    // Initially healthy
    let peers = manager.get_all_peers().await;
    assert!(peers[0].is_healthy);
    
    // Mark as unhealthy
    manager.peer_manager.mark_unhealthy("peer1", "test error".to_string()).await.unwrap();
    
    let peers = manager.get_all_peers().await;
    assert!(!peers[0].is_healthy);
}

#[tokio::test]
async fn test_get_all_peers_includes_block_height() {
    // Test that get_all_peers includes peer block height
    let config = NetworkConfig::new("node1".to_string(), NodeRole::Validator);
    let manager = P2PNetworkManager::new(config).await.unwrap();
    
    manager.peer_manager.add_peer(
        "peer1".to_string(),
        "127.0.0.1:9000".to_string(),
        NodeRole::Validator,
    ).await.unwrap();
    
    // Update block height
    manager.peer_manager.update_peer_block_height("peer1", 12345).await.unwrap();
    
    let peers = manager.get_all_peers().await;
    assert_eq!(peers[0].block_height, 12345);
}

#[tokio::test]
async fn test_get_all_peers_includes_latency() {
    // Test that get_all_peers includes peer latency information
    let config = NetworkConfig::new("node1".to_string(), NodeRole::Validator);
    let manager = P2PNetworkManager::new(config).await.unwrap();
    
    manager.peer_manager.add_peer(
        "peer1".to_string(),
        "127.0.0.1:9000".to_string(),
        NodeRole::Validator,
    ).await.unwrap();
    
    // Update metrics including latency
    manager.peer_manager.update_peer_metrics("peer1", 50, 1024, 2048).await.unwrap();
    
    let peers = manager.get_all_peers().await;
    assert_eq!(peers[0].latency_ms, 50);
}

#[tokio::test]
async fn test_get_all_peers_non_blocking() {
    // Test that get_all_peers is non-blocking and returns immediately
    let config = NetworkConfig::new("node1".to_string(), NodeRole::Validator);
    let manager = P2PNetworkManager::new(config).await.unwrap();
    
    // Add multiple peers
    for i in 0..100 {
        manager.peer_manager.add_peer(
            format!("peer{}", i),
            format!("127.0.0.1:{}", 9000 + i),
            NodeRole::Validator,
        ).await.unwrap();
    }
    
    // Measure time to get all peers
    let start = std::time::Instant::now();
    let peers = manager.get_all_peers().await;
    let elapsed = start.elapsed();
    
    assert_eq!(peers.len(), 100);
    // Should complete in less than 100ms even with 100 peers
    assert!(elapsed.as_millis() < 100);
}

#[tokio::test]
async fn test_get_all_peers_after_removal() {
    // Test that get_all_peers reflects peer removal
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
    
    let peers = manager.get_all_peers().await;
    assert_eq!(peers.len(), 2);
    
    // Remove a peer
    manager.peer_manager.remove_peer("peer1").await.unwrap();
    
    let peers = manager.get_all_peers().await;
    assert_eq!(peers.len(), 1);
    assert_eq!(peers[0].peer_id, "peer2");
}

#[tokio::test]
async fn test_get_all_peers_different_roles() {
    // Test that get_all_peers correctly includes different node roles
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
    
    let peers = manager.get_all_peers().await;
    assert_eq!(peers.len(), 3);
    
    // Verify roles
    let roles: Vec<_> = peers.iter().map(|p| p.role.clone()).collect();
    assert!(roles.contains(&"Validator".to_string()));
    assert!(roles.contains(&"RPC".to_string()));
    assert!(roles.contains(&"Archive".to_string()));
}

#[tokio::test]
async fn test_get_all_peers_consistency() {
    // Test that multiple calls to get_all_peers return consistent data
    let config = NetworkConfig::new("node1".to_string(), NodeRole::Validator);
    let manager = P2PNetworkManager::new(config).await.unwrap();
    
    manager.peer_manager.add_peer(
        "peer1".to_string(),
        "127.0.0.1:9000".to_string(),
        NodeRole::Validator,
    ).await.unwrap();
    
    let peers1 = manager.get_all_peers().await;
    let peers2 = manager.get_all_peers().await;
    
    assert_eq!(peers1.len(), peers2.len());
    assert_eq!(peers1[0].peer_id, peers2[0].peer_id);
    assert_eq!(peers1[0].address, peers2[0].address);
}
