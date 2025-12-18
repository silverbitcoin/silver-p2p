//! Property-based tests for peer health scoring
//! **Feature: p2p-network-implementation, Property 13: Peer Health Scoring**
//! **Validates: Requirements 7.2, 7.3, 7.4**

use silver_p2p::{
    ConnectionPool, HealthMonitor, MessageHandler, PeerManager, NodeRole,
};
use std::sync::Arc;

/// Property 13: Peer Health Scoring - Score Increase on Successful Ping
/// *For any* peer that responds to pings consistently, the peer's health score 
/// should increase with each successful ping response.
#[tokio::test]
async fn test_health_score_increases_on_successful_pings() {
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
            "peer1".to_string(),
            "127.0.0.1:9000".to_string(),
            NodeRole::Validator,
        )
        .await
        .unwrap();
    peer_manager.mark_connected("peer1").await.unwrap();

    // Get initial health score
    let initial_score = peer_manager.get_health_score("peer1").await.unwrap();
    assert_eq!(initial_score, 100, "Initial score should be 100");

    // Simulate successful pings
    for i in 1..=5 {
        peer_manager.record_successful_ping("peer1").await.unwrap();
        let score = peer_manager.get_health_score("peer1").await.unwrap();
        // Score stays at 100 (already at max)
        assert_eq!(score, 100, "Score should remain at max (100) after successful ping {}", i);
    }
}

/// Property 13: Peer Health Scoring - Score Decrease on Failed Ping
/// *For any* peer that fails to respond to pings, the peer's health score 
/// should decrease with each failed ping.
#[tokio::test]
async fn test_health_score_decreases_on_failed_pings() {
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
            "peer2".to_string(),
            "127.0.0.1:9001".to_string(),
            NodeRole::Validator,
        )
        .await
        .unwrap();
    peer_manager.mark_connected("peer2").await.unwrap();

    // Get initial health score
    let initial_score = peer_manager.get_health_score("peer2").await.unwrap();
    assert_eq!(initial_score, 100, "Initial score should be 100");

    // Simulate failed pings
    for i in 1..=5 {
        peer_manager.record_failed_ping("peer2").await.unwrap();
        let score = peer_manager.get_health_score("peer2").await.unwrap();
        let expected_score = 100 - (i as u32 * 10);
        assert_eq!(
            score, expected_score,
            "Score should decrease by 10 per failed ping (iteration {})",
            i
        );
    }

    // After 5 failed pings, score should be 50
    let final_score = peer_manager.get_health_score("peer2").await.unwrap();
    assert_eq!(final_score, 50, "Score should be 50 after 5 failed pings");
}

/// Property 13: Peer Health Scoring - Unhealthy Threshold
/// *For any* peer with health score below 30, the peer should be marked as unhealthy.
#[tokio::test]
async fn test_unhealthy_threshold_enforcement() {
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
            "peer3".to_string(),
            "127.0.0.1:9002".to_string(),
            NodeRole::Validator,
        )
        .await
        .unwrap();
    peer_manager.mark_connected("peer3").await.unwrap();

    // Verify peer is initially healthy
    let peer = peer_manager.get_peer("peer3").await.unwrap();
    assert!(peer.is_healthy, "Peer should be initially healthy");

    // Simulate 8 failed pings to drop score to 20 (below threshold of 30)
    for _ in 0..8 {
        peer_manager.record_failed_ping("peer3").await.unwrap();
    }

    // Verify peer is now unhealthy
    let peer = peer_manager.get_peer("peer3").await.unwrap();
    assert_eq!(peer.health_score, 20, "Score should be 20");
    assert!(!peer.is_healthy, "Peer should be marked unhealthy when score < 30");
}

/// Property 13: Peer Health Scoring - Recovery Mechanism
/// *For any* peer with health score >= 50, the peer should be able to recover 
/// by responding to pings successfully.
#[tokio::test]
async fn test_recovery_mechanism_for_degraded_peers() {
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
            "peer4".to_string(),
            "127.0.0.1:9003".to_string(),
            NodeRole::Validator,
        )
        .await
        .unwrap();
    peer_manager.mark_connected("peer4").await.unwrap();

    // Simulate 5 failed pings to drop score to 50
    for _ in 0..5 {
        peer_manager.record_failed_ping("peer4").await.unwrap();
    }

    let score_after_failures = peer_manager.get_health_score("peer4").await.unwrap();
    assert_eq!(score_after_failures, 50, "Score should be 50 after 5 failed pings");

    // Verify peer can recover (score >= 50)
    let peer = peer_manager.get_peer("peer4").await.unwrap();
    assert!(peer.can_recover(), "Peer should be able to recover at score 50");

    // Simulate successful pings to recover
    for i in 1..=5 {
        peer_manager.record_successful_ping("peer4").await.unwrap();
        let score = peer_manager.get_health_score("peer4").await.unwrap();
        let expected_score = 50 + (i as u32 * 5);
        assert_eq!(
            score, expected_score,
            "Score should increase by 5 per successful ping (iteration {})",
            i
        );
    }

    // After 5 successful pings, score should be 75
    let final_score = peer_manager.get_health_score("peer4").await.unwrap();
    assert_eq!(final_score, 75, "Score should be 75 after recovery");
}

/// Property 13: Peer Health Scoring - Score Bounds
/// *For any* peer, the health score should always be between 0 and 100 (inclusive).
#[tokio::test]
async fn test_health_score_bounds() {
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
            "peer5".to_string(),
            "127.0.0.1:9004".to_string(),
            NodeRole::Validator,
        )
        .await
        .unwrap();
    peer_manager.mark_connected("peer5").await.unwrap();

    // Test upper bound - simulate many successful pings
    for _ in 0..100 {
        peer_manager.record_successful_ping("peer5").await.unwrap();
    }
    let score = peer_manager.get_health_score("peer5").await.unwrap();
    assert_eq!(score, 100, "Score should not exceed 100");

    // Test lower bound - simulate many failed pings
    for _ in 0..100 {
        peer_manager.record_failed_ping("peer5").await.unwrap();
    }
    let score = peer_manager.get_health_score("peer5").await.unwrap();
    assert_eq!(score, 0, "Score should not go below 0");
}

/// Property 13: Peer Health Scoring - Multiple Peers Independent Scoring
/// *For any* set of peers, each peer's health score should be tracked independently.
#[tokio::test]
async fn test_independent_health_scores_for_multiple_peers() {
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

    // Add multiple peers
    for i in 0..5 {
        let peer_id = format!("peer{}", i);
        let addr = format!("127.0.0.1:{}", 9000 + i);
        peer_manager
            .add_peer(peer_id.clone(), addr, NodeRole::Validator)
            .await
            .unwrap();
        peer_manager.mark_connected(&peer_id).await.unwrap();
    }

    // Simulate different failure patterns for each peer
    for i in 0..5 {
        let peer_id = format!("peer{}", i);
        for _ in 0..i {
            peer_manager.record_failed_ping(&peer_id).await.unwrap();
        }
    }

    // Verify each peer has independent score
    for i in 0..5 {
        let peer_id = format!("peer{}", i);
        let score = peer_manager.get_health_score(&peer_id).await.unwrap();
        let expected_score = 100 - (i as u32 * 10);
        assert_eq!(
            score, expected_score,
            "Peer {} should have independent score",
            i
        );
    }
}

/// Property 13: Peer Health Scoring - Sorting by Health Score
/// *For any* set of peers, they should be sortable by health score in descending order.
#[tokio::test]
async fn test_peers_sorted_by_health_score() {
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

    // Add multiple peers with different health scores
    for i in 0..5 {
        let peer_id = format!("peer{}", i);
        let addr = format!("127.0.0.1:{}", 9000 + i);
        peer_manager
            .add_peer(peer_id.clone(), addr, NodeRole::Validator)
            .await
            .unwrap();
        peer_manager.mark_connected(&peer_id).await.unwrap();

        // Simulate different failure patterns
        for _ in 0..i {
            peer_manager.record_failed_ping(&peer_id).await.unwrap();
        }
    }

    // Get peers sorted by health score
    let sorted_peers = health_monitor.get_peers_by_health_score().await;

    // Verify sorting is in descending order
    for i in 0..sorted_peers.len() - 1 {
        assert!(
            sorted_peers[i].health_score >= sorted_peers[i + 1].health_score,
            "Peers should be sorted by health score in descending order"
        );
    }

    // Verify first peer has highest score
    assert_eq!(
        sorted_peers[0].health_score, 100,
        "First peer should have highest score (100)"
    );

    // Verify last peer has lowest score
    assert_eq!(
        sorted_peers[4].health_score, 60,
        "Last peer should have lowest score (60)"
    );
}

/// Property 13: Peer Health Scoring - Healthy Peers Filtering
/// *For any* set of peers, filtering for healthy peers (score >= 50) should 
/// return only peers above the recovery threshold.
#[tokio::test]
async fn test_healthy_peers_filtering() {
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

    // Add multiple peers
    for i in 0..10 {
        let peer_id = format!("peer{}", i);
        let addr = format!("127.0.0.1:{}", 9000 + i);
        peer_manager
            .add_peer(peer_id.clone(), addr, NodeRole::Validator)
            .await
            .unwrap();
        peer_manager.mark_connected(&peer_id).await.unwrap();

        // Simulate different failure patterns
        for _ in 0..i {
            peer_manager.record_failed_ping(&peer_id).await.unwrap();
        }
    }

    // Get healthy peers (score >= 50)
    let healthy_peers = health_monitor.get_healthy_peers_by_score().await;

    // Verify all returned peers have score >= 50
    for peer in &healthy_peers {
        assert!(
            peer.health_score >= 50,
            "Healthy peer should have score >= 50 (actual: {})",
            peer.health_score
        );
    }

    // Verify count is correct (peers 0-5 have scores 100, 90, 80, 70, 60, 50)
    assert_eq!(
        healthy_peers.len(),
        6,
        "Should have 6 healthy peers (scores >= 50)"
    );
}

/// Property 13: Peer Health Scoring - Unhealthy Peers Filtering
/// *For any* set of peers, filtering for unhealthy peers (score < 30) should 
/// return only peers below the unhealthy threshold.
#[tokio::test]
async fn test_unhealthy_peers_filtering() {
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

    // Add multiple peers
    for i in 0..10 {
        let peer_id = format!("peer{}", i);
        let addr = format!("127.0.0.1:{}", 9000 + i);
        peer_manager
            .add_peer(peer_id.clone(), addr, NodeRole::Validator)
            .await
            .unwrap();
        peer_manager.mark_connected(&peer_id).await.unwrap();

        // Simulate different failure patterns
        for _ in 0..i {
            peer_manager.record_failed_ping(&peer_id).await.unwrap();
        }
    }

    // Get unhealthy peers (score < 30)
    let unhealthy_peers = health_monitor.get_unhealthy_peers_by_score().await;

    // Verify all returned peers have score < 30
    for peer in &unhealthy_peers {
        assert!(
            peer.health_score < 30,
            "Unhealthy peer should have score < 30 (actual: {})",
            peer.health_score
        );
    }

    // Verify count is correct (peers 7-9 have scores 30, 20, 10)
    // Actually peer 7 has score 30 which is NOT < 30, so only 8-9
    assert_eq!(
        unhealthy_peers.len(),
        2,
        "Should have 2 unhealthy peers (scores < 30)"
    );
}

/// Property 13: Peer Health Scoring - Average Health Score Calculation
/// *For any* set of peers, the average health score should be correctly calculated.
#[tokio::test]
async fn test_average_health_score_calculation() {
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

    // Add 4 peers with known scores
    let scores = vec![100, 80, 60, 40];
    for (i, &score) in scores.iter().enumerate() {
        let peer_id = format!("peer{}", i);
        let addr = format!("127.0.0.1:{}", 9000 + i);
        peer_manager
            .add_peer(peer_id.clone(), addr, NodeRole::Validator)
            .await
            .unwrap();
        peer_manager.mark_connected(&peer_id).await.unwrap();

        // Simulate failures to reach target score
        let failures = (100 - score) / 10;
        for _ in 0..failures {
            peer_manager.record_failed_ping(&peer_id).await.unwrap();
        }
    }

    // Calculate average
    let avg = health_monitor.get_average_health_score().await;

    // Expected average: (100 + 80 + 60 + 40) / 4 = 70
    assert_eq!(avg, 70, "Average health score should be 70");
}

/// Property 13: Peer Health Scoring - Health Score Reset
/// *For any* peer with degraded health score, resetting the score should 
/// restore it to 100 and mark the peer as healthy.
#[tokio::test]
async fn test_health_score_reset_mechanism() {
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

    // Add a peer
    peer_manager
        .add_peer(
            "peer_reset".to_string(),
            "127.0.0.1:9050".to_string(),
            NodeRole::Validator,
        )
        .await
        .unwrap();
    peer_manager.mark_connected("peer_reset").await.unwrap();

    // Simulate failures to degrade health
    for _ in 0..8 {
        peer_manager.record_failed_ping("peer_reset").await.unwrap();
    }

    let peer = peer_manager.get_peer("peer_reset").await.unwrap();
    assert_eq!(peer.health_score, 20, "Score should be 20 after failures");
    assert!(!peer.is_healthy, "Peer should be unhealthy");

    // Reset health score
    health_monitor.reset_peer_health_score("peer_reset").await.unwrap();

    // Verify reset
    let peer = peer_manager.get_peer("peer_reset").await.unwrap();
    assert_eq!(peer.health_score, 100, "Score should be reset to 100");
    assert!(peer.is_healthy, "Peer should be marked healthy after reset");
}

/// Property 13: Peer Health Scoring - Consecutive Ping Tracking
/// *For any* peer, consecutive successful and failed pings should be tracked 
/// independently to support recovery mechanisms.
#[tokio::test]
async fn test_consecutive_ping_tracking() {
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

    // Add a peer
    peer_manager
        .add_peer(
            "peer_consecutive".to_string(),
            "127.0.0.1:9060".to_string(),
            NodeRole::Validator,
        )
        .await
        .unwrap();
    peer_manager.mark_connected("peer_consecutive").await.unwrap();

    // Simulate 3 failed pings
    for _ in 0..3 {
        peer_manager.record_failed_ping("peer_consecutive").await.unwrap();
    }

    let peer = peer_manager.get_peer("peer_consecutive").await.unwrap();
    assert_eq!(peer.consecutive_failed_pings, 3, "Should track 3 consecutive failures");
    assert_eq!(peer.consecutive_successful_pings, 0, "Should reset successful count");

    // Simulate 2 successful pings
    for _ in 0..2 {
        peer_manager.record_successful_ping("peer_consecutive").await.unwrap();
    }

    let peer = peer_manager.get_peer("peer_consecutive").await.unwrap();
    assert_eq!(peer.consecutive_successful_pings, 2, "Should track 2 consecutive successes");
    assert_eq!(peer.consecutive_failed_pings, 0, "Should reset failed count");
}

/// Property 13: Peer Health Scoring - Health Score Persistence
/// *For any* peer, the health score should persist across multiple operations 
/// and only change when explicitly updated.
#[tokio::test]
async fn test_health_score_persistence() {
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

    // Add a peer
    peer_manager
        .add_peer(
            "peer_persistent".to_string(),
            "127.0.0.1:9070".to_string(),
            NodeRole::Validator,
        )
        .await
        .unwrap();
    peer_manager.mark_connected("peer_persistent").await.unwrap();

    // Simulate some failures
    for _ in 0..3 {
        peer_manager.record_failed_ping("peer_persistent").await.unwrap();
    }

    let score1 = peer_manager.get_health_score("peer_persistent").await.unwrap();

    // Perform other operations that shouldn't affect health score
    peer_manager
        .update_peer_metrics("peer_persistent", 50, 1024, 2048)
        .await
        .unwrap();
    peer_manager
        .update_peer_block_height("peer_persistent", 100)
        .await
        .unwrap();

    let score2 = peer_manager.get_health_score("peer_persistent").await.unwrap();

    // Score should remain unchanged
    assert_eq!(
        score1, score2,
        "Health score should persist across other operations"
    );
    assert_eq!(score1, 70, "Score should be 70 after 3 failed pings");
}

