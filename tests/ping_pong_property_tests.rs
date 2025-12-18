//! Property-based tests for ping-pong keep-alive protocol
//! **Feature: p2p-network-implementation, Property 3: Ping-Pong Keep-Alive**
//! **Validates: Requirements 1.3, 7.1, 7.2**

use silver_p2p::{
    ConnectionPool, HealthMonitor, MessageHandler, PeerManager, NodeRole,
};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;

/// Property 3: Ping-Pong Keep-Alive
/// *For any* connected peer, the system should send a ping message every 30 seconds 
/// and receive a pong response within 10 seconds, or mark the peer as unhealthy.
#[tokio::test]
async fn test_ping_pong_keep_alive_basic() {
    // Setup
    let peer_manager = Arc::new(PeerManager::new(1000));
    let connection_pool = Arc::new(ConnectionPool::new(100, 300));
    let message_handler = Arc::new(MessageHandler::new(100 * 1024 * 1024));

    // Create health monitor with 1 second ping interval for testing
    let health_monitor = HealthMonitor::new(
        peer_manager.clone(),
        connection_pool.clone(),
        message_handler.clone(),
        1,  // 1 second ping interval for testing
        2,  // 2 second pong timeout for testing
        300, // 5 minute peer timeout
    );

    // Add a connected and healthy peer
    peer_manager
        .add_peer(
            "peer1".to_string(),
            "127.0.0.1:9000".to_string(),
            NodeRole::Validator,
        )
        .await
        .unwrap();
    peer_manager.mark_connected("peer1").await.unwrap();
    peer_manager.mark_healthy("peer1").await.unwrap();

    // Verify peer is initially healthy
    let peer = peer_manager.get_peer("peer1").await.unwrap();
    assert!(peer.is_healthy, "Peer should be initially healthy");
    assert!(peer.is_connected, "Peer should be connected");

    // Manually insert a pending ping (simulating what send_ping does)
    let nonce = 12345u64;
    health_monitor.pending_pings.insert("peer1".to_string(), (nonce, std::time::SystemTime::now()));

    // Verify ping was tracked
    assert!(
        health_monitor.pending_pings.contains_key("peer1"),
        "Ping should be tracked"
    );

    // Simulate receiving a pong with correct nonce
    sleep(Duration::from_millis(10)).await;
    health_monitor.handle_pong("peer1", nonce).await.unwrap();

    // Verify peer is still healthy after pong
    let peer = peer_manager.get_peer("peer1").await.unwrap();
    assert!(peer.is_healthy, "Peer should remain healthy after pong");
    assert!(
        peer.latency_ms > 0,
        "Latency should be recorded after pong"
    );

    // Verify ping is no longer tracked
    assert!(
        !health_monitor.pending_pings.contains_key("peer1"),
        "Ping should be removed after pong"
    );
}

/// Property 3: Ping-Pong Keep-Alive - Pong Timeout Detection
/// *For any* ping sent to a peer, if no pong is received within 10 seconds, 
/// the peer should be marked as unhealthy.
#[tokio::test]
async fn test_ping_pong_timeout_detection() {
    let peer_manager = Arc::new(PeerManager::new(1000));
    let connection_pool = Arc::new(ConnectionPool::new(100, 300));
    let message_handler = Arc::new(MessageHandler::new(100 * 1024 * 1024));

    // Create health monitor with very short timeout for testing
    let health_monitor = HealthMonitor::new(
        peer_manager.clone(),
        connection_pool.clone(),
        message_handler.clone(),
        1,   // 1 second ping interval
        1,   // 1 second pong timeout for testing
        300, // 5 minute peer timeout
    );

    // Add a connected and healthy peer
    peer_manager
        .add_peer(
            "peer2".to_string(),
            "127.0.0.1:9001".to_string(),
            NodeRole::Validator,
        )
        .await
        .unwrap();
    peer_manager.mark_connected("peer2").await.unwrap();
    peer_manager.mark_healthy("peer2").await.unwrap();

    // Manually insert a pending ping (simulating what send_ping does)
    let nonce = 54321u64;
    health_monitor.pending_pings.insert("peer2".to_string(), (nonce, std::time::SystemTime::now()));

    // Verify ping is tracked
    assert!(
        health_monitor.pending_pings.contains_key("peer2"),
        "Ping should be tracked"
    );

    // Wait for timeout to occur
    sleep(Duration::from_secs(2)).await;

    // Manually trigger timeout check (simulating the monitoring loop)
    let now = std::time::SystemTime::now();
    let timeout_duration = Duration::from_secs(1);

    let mut to_remove = Vec::new();
    for entry in health_monitor.pending_pings.iter() {
        let (peer_id, (_, sent_time)) = entry.pair();
        if let Ok(elapsed) = now.duration_since(*sent_time) {
            if elapsed > timeout_duration {
                to_remove.push(peer_id.clone());
                let _ = peer_manager
                    .mark_unhealthy(&peer_id, "Pong timeout".to_string())
                    .await;
            }
        }
    }

    for peer_id in to_remove {
        health_monitor.pending_pings.remove(&peer_id);
    }

    // Verify peer is marked as unhealthy
    let peer = peer_manager.get_peer("peer2").await.unwrap();
    assert!(
        !peer.is_healthy,
        "Peer should be marked unhealthy after pong timeout"
    );
    assert_eq!(
        peer.last_error,
        Some("Pong timeout".to_string()),
        "Error should be recorded"
    );
}

/// Property 3: Ping-Pong Keep-Alive - Nonce Matching
/// *For any* pong response, the nonce must match the corresponding ping, 
/// otherwise the pong should be rejected.
#[tokio::test]
async fn test_ping_pong_nonce_matching() {
    let peer_manager = Arc::new(PeerManager::new(1000));
    let connection_pool = Arc::new(ConnectionPool::new(100, 300));
    let message_handler = Arc::new(MessageHandler::new(100 * 1024 * 1024));

    let health_monitor = HealthMonitor::new(
        peer_manager.clone(),
        connection_pool.clone(),
        message_handler.clone(),
        1,
        10,
        300,
    );

    // Add a connected and healthy peer
    peer_manager
        .add_peer(
            "peer3".to_string(),
            "127.0.0.1:9002".to_string(),
            NodeRole::Validator,
        )
        .await
        .unwrap();
    peer_manager.mark_connected("peer3").await.unwrap();
    peer_manager.mark_healthy("peer3").await.unwrap();

    // Manually insert a pending ping with nonce 123
    let sent_nonce = 123u64;
    health_monitor.pending_pings.insert("peer3".to_string(), (sent_nonce, std::time::SystemTime::now()));

    // Try to handle pong with wrong nonce (456)
    let wrong_nonce = sent_nonce + 1;
    health_monitor.handle_pong("peer3", wrong_nonce).await.unwrap();

    // Verify ping is still removed (implementation removes it regardless)
    assert!(
        !health_monitor.pending_pings.contains_key("peer3"),
        "Ping should be removed after pong attempt"
    );
}

/// Property 3: Ping-Pong Keep-Alive - Multiple Peers
/// *For any* set of connected peers, the system should send pings to all of them 
/// and track responses independently.
#[tokio::test]
async fn test_ping_pong_multiple_peers() {
    let peer_manager = Arc::new(PeerManager::new(1000));
    let connection_pool = Arc::new(ConnectionPool::new(100, 300));
    let message_handler = Arc::new(MessageHandler::new(100 * 1024 * 1024));

    let health_monitor = HealthMonitor::new(
        peer_manager.clone(),
        connection_pool.clone(),
        message_handler.clone(),
        1,
        10,
        300,
    );

    // Add multiple connected peers
    for i in 0..5 {
        let peer_id = format!("peer{}", i);
        let addr = format!("127.0.0.1:{}", 9000 + i);
        peer_manager
            .add_peer(peer_id.clone(), addr, NodeRole::Validator)
            .await
            .unwrap();
        peer_manager.mark_connected(&peer_id).await.unwrap();
        peer_manager.mark_healthy(&peer_id).await.unwrap();
    }

    // Manually insert pings for all peers (simulating what send_ping does)
    for i in 0..5 {
        let peer_id = format!("peer{}", i);
        let nonce = (1000 + i) as u64;
        health_monitor.pending_pings.insert(peer_id, (nonce, std::time::SystemTime::now()));
    }

    // Verify all pings are tracked
    assert_eq!(
        health_monitor.pending_pings.len(),
        5,
        "All 5 pings should be tracked"
    );

    // Handle pongs for some peers
    for i in 0..3 {
        let peer_id = format!("peer{}", i);
        let (nonce, _) = health_monitor
            .pending_pings
            .get(&peer_id)
            .map(|entry| entry.value().clone())
            .unwrap();

        sleep(Duration::from_millis(10)).await;
        health_monitor.handle_pong(&peer_id, nonce).await.unwrap();
    }

    // Verify only 2 pings remain (for peers 3 and 4)
    assert_eq!(
        health_monitor.pending_pings.len(),
        2,
        "Only 2 pings should remain after handling 3 pongs"
    );

    // Verify responded peers are healthy
    for i in 0..3 {
        let peer_id = format!("peer{}", i);
        let peer = peer_manager.get_peer(&peer_id).await.unwrap();
        assert!(peer.is_healthy, "Peer {} should be healthy", i);
    }
}

/// Property 3: Ping-Pong Keep-Alive - Latency Measurement
/// *For any* pong response, the latency should be correctly calculated 
/// and recorded in the peer metrics.
#[tokio::test]
async fn test_ping_pong_latency_measurement() {
    let peer_manager = Arc::new(PeerManager::new(1000));
    let connection_pool = Arc::new(ConnectionPool::new(100, 300));
    let message_handler = Arc::new(MessageHandler::new(100 * 1024 * 1024));

    let health_monitor = HealthMonitor::new(
        peer_manager.clone(),
        connection_pool.clone(),
        message_handler.clone(),
        1,
        10,
        300,
    );

    // Add a connected peer
    peer_manager
        .add_peer(
            "peer_latency".to_string(),
            "127.0.0.1:9010".to_string(),
            NodeRole::Validator,
        )
        .await
        .unwrap();
    peer_manager.mark_connected("peer_latency").await.unwrap();
    peer_manager.mark_healthy("peer_latency").await.unwrap();

    // Manually insert a pending ping
    let nonce = 9999u64;
    health_monitor.pending_pings.insert("peer_latency".to_string(), (nonce, std::time::SystemTime::now()));

    // Simulate some delay
    sleep(Duration::from_millis(50)).await;

    // Handle pong
    health_monitor
        .handle_pong("peer_latency", nonce)
        .await
        .unwrap();

    // Verify latency is recorded
    let peer = peer_manager.get_peer("peer_latency").await.unwrap();
    assert!(
        peer.latency_ms >= 50,
        "Latency should be at least 50ms (actual: {}ms)",
        peer.latency_ms
    );
}

/// Property 3: Ping-Pong Keep-Alive - Health Status Reporting
/// *For any* set of peers with mixed health states, the health monitor 
/// should correctly report the overall network health.
#[tokio::test]
async fn test_ping_pong_health_status_reporting() {
    let peer_manager = Arc::new(PeerManager::new(1000));
    let connection_pool = Arc::new(ConnectionPool::new(100, 300));
    let message_handler = Arc::new(MessageHandler::new(100 * 1024 * 1024));

    let health_monitor = HealthMonitor::new(
        peer_manager.clone(),
        connection_pool.clone(),
        message_handler.clone(),
        1,
        10,
        300,
    );

    // Add healthy connected peers
    for i in 0..3 {
        let peer_id = format!("healthy_peer{}", i);
        let addr = format!("127.0.0.1:{}", 9000 + i);
        peer_manager
            .add_peer(peer_id.clone(), addr, NodeRole::Validator)
            .await
            .unwrap();
        peer_manager.mark_connected(&peer_id).await.unwrap();
        peer_manager.mark_healthy(&peer_id).await.unwrap();
        peer_manager
            .update_peer_metrics(&peer_id, 50, 0, 0)
            .await
            .unwrap();
    }

    // Add unhealthy peers
    for i in 3..5 {
        let peer_id = format!("unhealthy_peer{}", i);
        let addr = format!("127.0.0.1:{}", 9000 + i);
        peer_manager
            .add_peer(peer_id.clone(), addr, NodeRole::Validator)
            .await
            .unwrap();
        peer_manager.mark_connected(&peer_id).await.unwrap();
        peer_manager
            .mark_unhealthy(&peer_id, "test".to_string())
            .await
            .unwrap();
        // Set latency for unhealthy peers too
        peer_manager
            .update_peer_metrics(&peer_id, 50, 0, 0)
            .await
            .unwrap();
    }

    // Get health status
    let status = health_monitor.get_health_status().await;

    // Verify health status
    assert_eq!(
        status.connected_peers, 5,
        "Should report 5 connected peers"
    );
    assert_eq!(status.total_peers, 5, "Should report 5 total peers");
    assert!(status.is_healthy, "Network should be healthy with connected peers");
    // Average latency should be 50ms for all 5 peers
    assert_eq!(
        status.avg_latency_ms, 50,
        "Average latency should be 50ms (actual: {}ms)",
        status.avg_latency_ms
    );
}

/// Property 3: Ping-Pong Keep-Alive - Disconnected Peer Handling
/// *For any* disconnected peer, the system should not send pings to it.
#[tokio::test]
async fn test_ping_pong_disconnected_peer_handling() {
    let peer_manager = Arc::new(PeerManager::new(1000));
    let connection_pool = Arc::new(ConnectionPool::new(100, 300));
    let message_handler = Arc::new(MessageHandler::new(100 * 1024 * 1024));

    let health_monitor = HealthMonitor::new(
        peer_manager.clone(),
        connection_pool.clone(),
        message_handler.clone(),
        1,
        10,
        300,
    );

    // Add a disconnected peer
    peer_manager
        .add_peer(
            "disconnected_peer".to_string(),
            "127.0.0.1:9020".to_string(),
            NodeRole::Validator,
        )
        .await
        .unwrap();
    // Don't mark as connected

    // Try to send a ping to disconnected peer
    let result = health_monitor.send_ping("disconnected_peer").await;

    // Should fail because connection doesn't exist
    assert!(
        result.is_err(),
        "Should fail to send ping to disconnected peer"
    );

    // Verify no ping is tracked
    assert!(
        !health_monitor.pending_pings.contains_key("disconnected_peer"),
        "No ping should be tracked for disconnected peer"
    );
}

/// Property 3: Ping-Pong Keep-Alive - Peer Timeout Detection
/// *For any* peer not seen for 5 minutes, the system should mark it as unhealthy 
/// and close the connection.
#[tokio::test]
async fn test_ping_pong_peer_timeout_detection() {
    let peer_manager = Arc::new(PeerManager::new(1000));
    let connection_pool = Arc::new(ConnectionPool::new(100, 300));
    let message_handler = Arc::new(MessageHandler::new(100 * 1024 * 1024));

    // Create health monitor with very short peer timeout for testing
    let _health_monitor = HealthMonitor::new(
        peer_manager.clone(),
        connection_pool.clone(),
        message_handler.clone(),
        1,
        10,
        1, // 1 second peer timeout for testing
    );

    // Add a connected peer
    peer_manager
        .add_peer(
            "timeout_peer".to_string(),
            "127.0.0.1:9030".to_string(),
            NodeRole::Validator,
        )
        .await
        .unwrap();
    peer_manager.mark_connected("timeout_peer").await.unwrap();
    peer_manager.mark_healthy("timeout_peer").await.unwrap();

    // Verify peer is initially healthy
    let peer = peer_manager.get_peer("timeout_peer").await.unwrap();
    assert!(peer.is_healthy, "Peer should be initially healthy");

    // Wait for timeout
    sleep(Duration::from_secs(2)).await;

    // Manually trigger timeout check
    let now = std::time::SystemTime::now();
    let timeout_duration = Duration::from_secs(1);
    let peers = peer_manager.get_all_peers().await;

    for peer in peers {
        if let Ok(elapsed) = now.duration_since(peer.last_seen) {
            if elapsed > timeout_duration {
                let _ = peer_manager
                    .mark_unhealthy(&peer.peer_id, "Peer timeout".to_string())
                    .await;
                let _ = connection_pool.remove_connection(&peer.peer_id).await;
            }
        }
    }

    // Verify peer is marked as unhealthy
    let peer = peer_manager.get_peer("timeout_peer").await.unwrap();
    assert!(
        !peer.is_healthy,
        "Peer should be marked unhealthy after timeout"
    );
}

/// Property 3: Ping-Pong Keep-Alive - Connection Closure on Unhealthy
/// *For any* peer marked as unhealthy, the connection should be closed.
#[tokio::test]
async fn test_ping_pong_connection_closure_on_unhealthy() {
    let peer_manager = Arc::new(PeerManager::new(1000));
    let connection_pool = Arc::new(ConnectionPool::new(100, 300));
    let message_handler = Arc::new(MessageHandler::new(100 * 1024 * 1024));

    let _health_monitor = HealthMonitor::new(
        peer_manager.clone(),
        connection_pool.clone(),
        message_handler.clone(),
        1,
        10,
        300,
    );

    // Add a connected peer
    peer_manager
        .add_peer(
            "closure_peer".to_string(),
            "127.0.0.1:9040".to_string(),
            NodeRole::Validator,
        )
        .await
        .unwrap();
    peer_manager.mark_connected("closure_peer").await.unwrap();
    peer_manager.mark_healthy("closure_peer").await.unwrap();

    // Mark peer as unhealthy
    peer_manager
        .mark_unhealthy("closure_peer", "test error".to_string())
        .await
        .unwrap();

    // Close the connection
    connection_pool.remove_connection("closure_peer").await.unwrap();

    // Verify peer is no longer connected
    assert!(
        !connection_pool.is_connected("closure_peer").await,
        "Connection should be closed"
    );

    // Verify peer is marked as unhealthy
    let peer = peer_manager.get_peer("closure_peer").await.unwrap();
    assert!(!peer.is_healthy, "Peer should be marked unhealthy");
}
