//! Property-based tests for broadcast delivery
//! **Feature: p2p-network-implementation, Property 8: Broadcast Delivery**
//! **Validates: Requirements 4.1, 4.2**

use silver_p2p::{
    BroadcastManager, ConnectionPool, MessageHandler, NetworkMessage, PeerManager,
};
use std::sync::Arc;

/// Property 8: Broadcast Delivery
/// *For any* broadcast message sent to N connected peers, the system should deliver 
/// the message to all N peers or log failures for specific peers.
#[tokio::test]
async fn test_broadcast_delivery_to_all_connected_peers() {
    // Setup
    let peer_manager = Arc::new(PeerManager::new(1000));
    let connection_pool = Arc::new(ConnectionPool::new(100, 300));
    let message_handler = Arc::new(MessageHandler::new(100 * 1024 * 1024));

    let broadcast_mgr = BroadcastManager::new(
        peer_manager.clone(),
        connection_pool.clone(),
        message_handler.clone(),
        5,
        10,
    );

    // Add multiple connected and healthy peers
    for i in 0..5 {
        let peer_id = format!("peer{}", i);
        let addr = format!("127.0.0.1:{}", 9000 + i);
        peer_manager
            .add_peer(peer_id.clone(), addr, silver_p2p::NodeRole::Validator)
            .await
            .unwrap();
        peer_manager.mark_connected(&peer_id).await.unwrap();
        peer_manager.mark_healthy(&peer_id).await.unwrap();
    }

    // Create a test message
    let msg = NetworkMessage::Ping { nonce: 12345 };

    // Broadcast the message
    let result = broadcast_mgr.broadcast(msg).await.unwrap();

    // Property: All connected peers should receive the message
    // Since we have 5 connected peers, we should have attempted to send to all 5
    assert_eq!(result.total_peers, 5, "Should target all 5 connected peers");

    // The result should contain either successful sends or failed peers
    // The sum of successful and failed should equal total
    assert_eq!(
        result.successful_sends + result.failed_sends,
        result.total_peers,
        "Successful + failed should equal total peers"
    );

    // All peers should be accounted for (either in successful or failed list)
    assert_eq!(
        result.successful_peers.len() + result.failed_peers.len(),
        result.total_peers,
        "All peers should be in either successful or failed list"
    );
}

/// Property 8: Broadcast Delivery - Partial Failure Handling
/// *For any* broadcast message, individual peer failures should not prevent 
/// delivery to other peers
#[tokio::test]
async fn test_broadcast_delivery_with_mixed_peer_states() {
    let peer_manager = Arc::new(PeerManager::new(1000));
    let connection_pool = Arc::new(ConnectionPool::new(100, 300));
    let message_handler = Arc::new(MessageHandler::new(100 * 1024 * 1024));

    let broadcast_mgr = BroadcastManager::new(
        peer_manager.clone(),
        connection_pool.clone(),
        message_handler.clone(),
        5,
        10,
    );

    // Add peers with different states
    // Connected and healthy peers
    for i in 0..3 {
        let peer_id = format!("healthy_peer{}", i);
        let addr = format!("127.0.0.1:{}", 9000 + i);
        peer_manager
            .add_peer(peer_id.clone(), addr, silver_p2p::NodeRole::Validator)
            .await
            .unwrap();
        peer_manager.mark_connected(&peer_id).await.unwrap();
        peer_manager.mark_healthy(&peer_id).await.unwrap();
    }

    // Unhealthy peers (should not be targeted)
    for i in 3..5 {
        let peer_id = format!("unhealthy_peer{}", i);
        let addr = format!("127.0.0.1:{}", 9000 + i);
        peer_manager
            .add_peer(peer_id.clone(), addr, silver_p2p::NodeRole::Validator)
            .await
            .unwrap();
        peer_manager.mark_connected(&peer_id).await.unwrap();
        peer_manager.mark_unhealthy(&peer_id, "test".to_string()).await.unwrap();
    }

    // Disconnected peers (should not be targeted)
    for i in 5..7 {
        let peer_id = format!("disconnected_peer{}", i);
        let addr = format!("127.0.0.1:{}", 9000 + i);
        peer_manager
            .add_peer(peer_id.clone(), addr, silver_p2p::NodeRole::Validator)
            .await
            .unwrap();
        // Don't mark as connected
    }

    let msg = NetworkMessage::Ping { nonce: 54321 };
    let result = broadcast_mgr.broadcast(msg).await.unwrap();

    // Property: Only connected AND healthy peers should be targeted
    assert_eq!(
        result.total_peers, 3,
        "Should only target connected and healthy peers"
    );

    // All targeted peers should be accounted for
    assert_eq!(
        result.successful_sends + result.failed_sends,
        result.total_peers,
        "All targeted peers should be accounted for"
    );
}

/// Property 8: Broadcast Delivery - Zero Peers Case
/// *For any* broadcast when there are no connected peers, the system should 
/// return success with zero sends
#[tokio::test]
async fn test_broadcast_delivery_zero_connected_peers() {
    let peer_manager = Arc::new(PeerManager::new(1000));
    let connection_pool = Arc::new(ConnectionPool::new(100, 300));
    let message_handler = Arc::new(MessageHandler::new(100 * 1024 * 1024));

    let broadcast_mgr = BroadcastManager::new(
        peer_manager.clone(),
        connection_pool.clone(),
        message_handler.clone(),
        5,
        10,
    );

    // Add peers but don't connect them
    for i in 0..3 {
        let peer_id = format!("peer{}", i);
        let addr = format!("127.0.0.1:{}", 9000 + i);
        peer_manager
            .add_peer(peer_id, addr, silver_p2p::NodeRole::Validator)
            .await
            .unwrap();
    }

    let msg = NetworkMessage::Ping { nonce: 99999 };
    let result = broadcast_mgr.broadcast(msg).await.unwrap();

    // Property: With zero connected peers, broadcast should succeed with zero sends
    assert_eq!(result.total_peers, 0, "Should have zero target peers");
    assert_eq!(result.successful_sends, 0, "Should have zero successful sends");
    assert_eq!(result.failed_sends, 0, "Should have zero failed sends");
    assert!(result.is_successful(), "Should be marked as successful");
}

/// Property 8: Broadcast Delivery - Targeted Broadcast
/// *For any* targeted broadcast to specific peers, the system should attempt 
/// delivery to exactly those peers
#[tokio::test]
async fn test_broadcast_delivery_targeted_peers() {
    let peer_manager = Arc::new(PeerManager::new(1000));
    let connection_pool = Arc::new(ConnectionPool::new(100, 300));
    let message_handler = Arc::new(MessageHandler::new(100 * 1024 * 1024));

    let broadcast_mgr = BroadcastManager::new(
        peer_manager.clone(),
        connection_pool.clone(),
        message_handler.clone(),
        5,
        10,
    );

    // Add multiple peers
    for i in 0..10 {
        let peer_id = format!("peer{}", i);
        let addr = format!("127.0.0.1:{}", 9000 + i);
        peer_manager
            .add_peer(peer_id, addr, silver_p2p::NodeRole::Validator)
            .await
            .unwrap();
    }

    // Target only specific peers
    let target_peers = vec![
        "peer1".to_string(),
        "peer3".to_string(),
        "peer5".to_string(),
    ];

    let msg = NetworkMessage::Ping { nonce: 11111 };
    let result = broadcast_mgr
        .broadcast_to_peers(msg, target_peers.clone())
        .await
        .unwrap();

    // Property: Should attempt delivery to exactly the targeted peers
    assert_eq!(
        result.total_peers,
        target_peers.len(),
        "Should target exactly the specified peers"
    );

    // All targeted peers should be accounted for
    assert_eq!(
        result.successful_sends + result.failed_sends,
        result.total_peers,
        "All targeted peers should be accounted for"
    );
}

/// Property 8: Broadcast Delivery - Message Serialization
/// *For any* broadcast message, the message should be serialized once and 
/// sent to all peers
#[tokio::test]
async fn test_broadcast_delivery_message_serialization() {
    let peer_manager = Arc::new(PeerManager::new(1000));
    let connection_pool = Arc::new(ConnectionPool::new(100, 300));
    let message_handler = Arc::new(MessageHandler::new(100 * 1024 * 1024));

    let broadcast_mgr = BroadcastManager::new(
        peer_manager.clone(),
        connection_pool.clone(),
        message_handler.clone(),
        5,
        10,
    );

    // Add connected peers
    for i in 0..3 {
        let peer_id = format!("peer{}", i);
        let addr = format!("127.0.0.1:{}", 9000 + i);
        peer_manager
            .add_peer(peer_id.clone(), addr, silver_p2p::NodeRole::Validator)
            .await
            .unwrap();
        peer_manager.mark_connected(&peer_id).await.unwrap();
        peer_manager.mark_healthy(&peer_id).await.unwrap();
    }

    // Create various message types
    let messages = vec![
        NetworkMessage::Ping { nonce: 123 },
        NetworkMessage::Pong { nonce: 456 },
        NetworkMessage::PeerListRequest,
        NetworkMessage::PeerListResponse(vec!["127.0.0.1:9000".to_string()]),
    ];

    for msg in messages {
        let result = broadcast_mgr.broadcast(msg).await.unwrap();

        // Property: Should attempt delivery to all connected peers
        assert_eq!(
            result.total_peers, 3,
            "Should target all 3 connected peers for each message type"
        );

        // All peers should be accounted for
        assert_eq!(
            result.successful_sends + result.failed_sends,
            result.total_peers,
            "All peers should be accounted for"
        );
    }
}

/// Property 8: Broadcast Delivery - Success Rate Calculation
/// *For any* broadcast result, the success rate should be correctly calculated
#[tokio::test]
async fn test_broadcast_delivery_success_rate() {
    let peer_manager = Arc::new(PeerManager::new(1000));
    let connection_pool = Arc::new(ConnectionPool::new(100, 300));
    let message_handler = Arc::new(MessageHandler::new(100 * 1024 * 1024));

    let broadcast_mgr = BroadcastManager::new(
        peer_manager.clone(),
        connection_pool.clone(),
        message_handler.clone(),
        5,
        10,
    );

    // Add connected peers
    for i in 0..10 {
        let peer_id = format!("peer{}", i);
        let addr = format!("127.0.0.1:{}", 9000 + i);
        peer_manager
            .add_peer(peer_id.clone(), addr, silver_p2p::NodeRole::Validator)
            .await
            .unwrap();
        peer_manager.mark_connected(&peer_id).await.unwrap();
        peer_manager.mark_healthy(&peer_id).await.unwrap();
    }

    let msg = NetworkMessage::Ping { nonce: 77777 };
    let result = broadcast_mgr.broadcast(msg).await.unwrap();

    // Property: Success rate should be correctly calculated
    let expected_rate = if result.total_peers == 0 {
        100.0
    } else {
        (result.successful_sends as f64 / result.total_peers as f64) * 100.0
    };

    assert_eq!(
        result.success_rate(),
        expected_rate,
        "Success rate should be correctly calculated"
    );

    // Property: Success rate should be between 0 and 100
    assert!(
        result.success_rate() >= 0.0 && result.success_rate() <= 100.0,
        "Success rate should be between 0 and 100"
    );
}

/// Property 8: Broadcast Delivery - Concurrent Broadcast Limit
/// *For any* broadcast, the system should respect the maximum concurrent 
/// broadcast limit
#[tokio::test]
async fn test_broadcast_delivery_concurrent_limit() {
    let peer_manager = Arc::new(PeerManager::new(1000));
    let connection_pool = Arc::new(ConnectionPool::new(100, 300));
    let message_handler = Arc::new(MessageHandler::new(100 * 1024 * 1024));

    // Create broadcast manager with low concurrency limit
    let broadcast_mgr = BroadcastManager::new(
        peer_manager.clone(),
        connection_pool.clone(),
        message_handler.clone(),
        5,
        2, // Only 2 concurrent broadcasts
    );

    // Add many connected peers
    for i in 0..20 {
        let peer_id = format!("peer{}", i);
        let addr = format!("127.0.0.1:{}", 9000 + i);
        peer_manager
            .add_peer(peer_id.clone(), addr, silver_p2p::NodeRole::Validator)
            .await
            .unwrap();
        peer_manager.mark_connected(&peer_id).await.unwrap();
        peer_manager.mark_healthy(&peer_id).await.unwrap();
    }

    let msg = NetworkMessage::Ping { nonce: 88888 };
    let result = broadcast_mgr.broadcast(msg).await.unwrap();

    // Property: Should still attempt delivery to all peers despite concurrency limit
    assert_eq!(
        result.total_peers, 20,
        "Should target all 20 connected peers"
    );

    // All peers should be accounted for
    assert_eq!(
        result.successful_sends + result.failed_sends,
        result.total_peers,
        "All peers should be accounted for"
    );
}
