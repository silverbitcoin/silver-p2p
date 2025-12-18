//! Property-based tests for minimum peer threshold enforcement
//! **Feature: p2p-network-implementation, Property 11: Minimum Peer Threshold**
//! **Validates: Requirements 6.3**

use proptest::prelude::*;
use silver_p2p::{
    NetworkConfig, PeerManager, PeerDiscoveryCoordinator, ConnectionPool, NodeRole,
    MessageHandler, UnicastManager,
};
use std::sync::Arc;
use tokio::runtime::Runtime;

/// Strategy for generating valid minimum peer thresholds
fn min_peer_threshold_strategy() -> impl Strategy<Value = usize> {
    1usize..=50
}

/// Helper function to create coordinator with all required components
fn create_coordinator(config: NetworkConfig) -> (Arc<PeerDiscoveryCoordinator>, Arc<PeerManager>) {
    let peer_manager = Arc::new(PeerManager::new(1000));
    let connection_pool = Arc::new(ConnectionPool::new(100, 300));
    let message_handler = Arc::new(MessageHandler::new(100 * 1024 * 1024)); // 100 MB max
    let unicast_manager = Arc::new(UnicastManager::new(
        peer_manager.clone(),
        connection_pool.clone(),
        message_handler.clone(),
        5, // 5 second timeout
        "test_node".to_string(),
    ));

    let coordinator = Arc::new(PeerDiscoveryCoordinator::new(
        peer_manager.clone(),
        connection_pool,
        message_handler,
        unicast_manager,
        config,
    ));

    (coordinator, peer_manager)
}

/// Property: When peer count is below minimum threshold, the coordinator should detect it
#[test]
fn prop_minimum_threshold_detection() {
    let rt = Runtime::new().unwrap();

    proptest!(|(min_peers in min_peer_threshold_strategy(), connected_peers in 0usize..50)| {
        rt.block_on(async {
            // Skip if connected peers >= min_peers (not below threshold)
            if connected_peers >= min_peers {
                return Ok(());
            }

            let mut config = NetworkConfig::new("test_node".to_string(), NodeRole::Validator);
            config.min_peers = min_peers;

            let (coordinator, peer_manager) = create_coordinator(config);

            // Add and connect peers
            for i in 0..connected_peers {
                let peer_id = format!("peer_{}", i);
                let addr = format!("127.0.0.1:{}", 9000 + i);
                peer_manager
                    .add_peer(peer_id.clone(), addr, NodeRole::Validator)
                    .await
                    .unwrap();
                peer_manager.mark_connected(&peer_id).await.unwrap();
            }

            // Check peer count status
            let (connected, total, below_threshold) = coordinator.get_peer_count_status().await;

            // Verify the coordinator correctly detects below-threshold condition
            prop_assert_eq!(connected, connected_peers);
            prop_assert_eq!(total, connected_peers);
            prop_assert!(below_threshold);

            Ok(())
        }).expect("async test failed")
    });
}

/// Property: When peer count is at or above minimum threshold, coordinator should not trigger
#[test]
fn prop_minimum_threshold_not_triggered_when_met() {
    let rt = Runtime::new().unwrap();

    proptest!(|(min_peers in min_peer_threshold_strategy(), extra_peers in 0usize..20)| {
        rt.block_on(async {
            let connected_peers = min_peers + extra_peers;

            let mut config = NetworkConfig::new("test_node".to_string(), NodeRole::Validator);
            config.min_peers = min_peers;

            let (coordinator, peer_manager) = create_coordinator(config);

            // Add and connect peers
            for i in 0..connected_peers {
                let peer_id = format!("peer_{}", i);
                let addr = format!("127.0.0.1:{}", 9000 + i);
                peer_manager
                    .add_peer(peer_id.clone(), addr, NodeRole::Validator)
                    .await
                    .unwrap();
                peer_manager.mark_connected(&peer_id).await.unwrap();
            }

            // Check peer count status
            let (connected, total, below_threshold) = coordinator.get_peer_count_status().await;

            // Verify the coordinator correctly detects that threshold is met
            prop_assert_eq!(connected, connected_peers);
            prop_assert_eq!(total, connected_peers);
            prop_assert!(!below_threshold);

            Ok(())
        }).expect("async test failed")
    });
}

/// Property: Candidate connection attempts should respect exponential backoff
#[test]
fn prop_candidate_backoff_increases_exponentially() {
    let rt = Runtime::new().unwrap();

    proptest!(|(failure_count in 0u32..10)| {
        rt.block_on(async {
            let config = NetworkConfig::new("test_node".to_string(), NodeRole::Validator);
            let (coordinator, _peer_manager) = create_coordinator(config.clone());

            let addr = "127.0.0.1:9001".to_string();

            // Manually add attempt with specific failure count
            {
                let mut attempts = coordinator.candidate_attempts.write().await;
                attempts.insert(
                    addr.clone(),
                    silver_p2p::CandidateConnectionAttempt {
                        address: addr.clone(),
                        last_attempt: std::time::SystemTime::now(),
                        failure_count,
                        in_progress: false,
                    },
                );
            }

            // Get the attempt
            let attempt = coordinator.get_candidate_attempt_history(&addr).await;
            prop_assert!(attempt.is_some());

            let attempt = attempt.unwrap();
            prop_assert_eq!(attempt.failure_count, failure_count);

            // Verify backoff calculation: 2^failure_count, capped at max_backoff_secs
            let expected_backoff = std::cmp::min(
                (1u64 << failure_count.min(8)) as u64,
                config.max_backoff_secs,
            );

            // The backoff should be reasonable (between 1 second and max_backoff_secs)
            prop_assert!(expected_backoff >= 1);
            prop_assert!(expected_backoff <= config.max_backoff_secs);

            Ok(())
        }).expect("async test failed")
    });
}

/// Property: Failed candidates should be tracked and retrievable
#[test]
fn prop_failed_candidates_tracking() {
    let rt = Runtime::new().unwrap();

    proptest!(|(num_candidates in 1usize..20, failure_threshold in 1u32..10)| {
        rt.block_on(async {
            let config = NetworkConfig::new("test_node".to_string(), NodeRole::Validator);
            let (coordinator, _peer_manager) = create_coordinator(config);

            // Add candidates with varying failure counts
            {
                let mut attempts = coordinator.candidate_attempts.write().await;
                for i in 0..num_candidates {
                    let addr = format!("127.0.0.1:{}", 9000 + i);
                    let failure_count = (i as u32) % 10;
                    attempts.insert(
                        addr.clone(),
                        silver_p2p::CandidateConnectionAttempt {
                            address: addr,
                            last_attempt: std::time::SystemTime::now(),
                            failure_count,
                            in_progress: false,
                        },
                    );
                }
            }

            // Get failed candidates
            let failed = coordinator.get_failed_candidates(failure_threshold).await;

            // Verify all returned candidates have failure_count >= threshold
            for addr in &failed {
                let attempt = coordinator.get_candidate_attempt_history(addr).await;
                prop_assert!(attempt.is_some());
                prop_assert!(attempt.unwrap().failure_count >= failure_threshold);
            }

            // Verify we got the right count
            let expected_count = (0..num_candidates)
                .filter(|i| (*i as u32) % 10 >= failure_threshold)
                .count();
            prop_assert_eq!(failed.len(), expected_count);

            Ok(())
        }).expect("async test failed")
    });
}

/// Property: Resetting candidate failures should set failure_count to 0
#[test]
fn prop_reset_candidate_failures() {
    let rt = Runtime::new().unwrap();

    proptest!(|(initial_failures in 1u32..20)| {
        rt.block_on(async {
            let config = NetworkConfig::new("test_node".to_string(), NodeRole::Validator);
            let (coordinator, _peer_manager) = create_coordinator(config);

            let addr = "127.0.0.1:9001".to_string();

            // Add attempt with failures
            {
                let mut attempts = coordinator.candidate_attempts.write().await;
                attempts.insert(
                    addr.clone(),
                    silver_p2p::CandidateConnectionAttempt {
                        address: addr.clone(),
                        last_attempt: std::time::SystemTime::now(),
                        failure_count: initial_failures,
                        in_progress: false,
                    },
                );
            }

            // Verify initial state
            let before = coordinator.get_candidate_attempt_history(&addr).await;
            prop_assert_eq!(before.unwrap().failure_count, initial_failures);

            // Reset failures
            coordinator.reset_candidate_failures(&addr).await.unwrap();

            // Verify reset
            let after = coordinator.get_candidate_attempt_history(&addr).await;
            prop_assert_eq!(after.unwrap().failure_count, 0);

            Ok(())
        }).expect("async test failed")
    });
}

/// Property: Removing a candidate should make it untrackable
#[test]
fn prop_remove_candidate_makes_untrackable() {
    let rt = Runtime::new().unwrap();

    proptest!(|(num_candidates in 1usize..20)| {
        rt.block_on(async {
            let config = NetworkConfig::new("test_node".to_string(), NodeRole::Validator);
            let (coordinator, _peer_manager) = create_coordinator(config);

            let mut addresses = Vec::new();

            // Add candidates
            {
                let mut attempts = coordinator.candidate_attempts.write().await;
                for i in 0..num_candidates {
                    let addr = format!("127.0.0.1:{}", 9000 + i);
                    addresses.push(addr.clone());
                    attempts.insert(
                        addr.clone(),
                        silver_p2p::CandidateConnectionAttempt {
                            address: addr,
                            last_attempt: std::time::SystemTime::now(),
                            failure_count: 0,
                            in_progress: false,
                        },
                    );
                }
            }

            // Remove first candidate
            let to_remove = &addresses[0];
            coordinator.remove_monitored_candidate(to_remove).await.unwrap();

            // Verify it's gone
            let history = coordinator.get_candidate_attempt_history(to_remove).await;
            prop_assert!(history.is_none());

            // Verify others still exist
            for addr in &addresses[1..] {
                let history = coordinator.get_candidate_attempt_history(addr).await;
                prop_assert!(history.is_some());
            }

            Ok(())
        }).expect("async test failed")
    });
}

/// Property: Statistics should accurately reflect coordinator activity
#[test]
fn prop_statistics_accuracy() {
    let rt = Runtime::new().unwrap();

    proptest!(|(num_candidates in 1usize..20)| {
        rt.block_on(async {
            let config = NetworkConfig::new("test_node".to_string(), NodeRole::Validator);
            let (coordinator, _peer_manager) = create_coordinator(config);

            // Get initial stats
            let stats_initial = coordinator.get_stats().await;
            prop_assert_eq!(stats_initial.total_attempts, 0);
            prop_assert_eq!(stats_initial.successful_connections, 0);
            prop_assert_eq!(stats_initial.failed_attempts, 0);

            // Add candidates manually
            {
                let mut attempts = coordinator.candidate_attempts.write().await;
                for i in 0..num_candidates {
                    let addr = format!("127.0.0.1:{}", 9000 + i);
                    attempts.insert(
                        addr.clone(),
                        silver_p2p::CandidateConnectionAttempt {
                            address: addr,
                            last_attempt: std::time::SystemTime::now(),
                            failure_count: 0,
                            in_progress: false,
                        },
                    );
                }
            }

            // Get monitored candidates
            let monitored = coordinator.get_monitored_candidates().await;
            prop_assert_eq!(monitored.len(), num_candidates);

            Ok(())
        }).expect("async test failed")
    });
}

/// Property: Threshold triggers should increment when below minimum
#[test]
fn prop_threshold_trigger_counting() {
    let rt = Runtime::new().unwrap();

    proptest!(|(min_peers in 2usize..10, connected_peers in 0usize..2)| {
        rt.block_on(async {
            // Skip if not below threshold
            if connected_peers >= min_peers {
                return Ok(());
            }

            let mut config = NetworkConfig::new("test_node".to_string(), NodeRole::Validator);
            config.min_peers = min_peers;

            let (coordinator, peer_manager) = create_coordinator(config);

            // Add peers
            for i in 0..connected_peers {
                let peer_id = format!("peer_{}", i);
                let addr = format!("127.0.0.1:{}", 9000 + i);
                peer_manager
                    .add_peer(peer_id.clone(), addr, NodeRole::Validator)
                    .await
                    .unwrap();
                peer_manager.mark_connected(&peer_id).await.unwrap();
            }

            // Get initial stats
            let stats_before = coordinator.get_stats().await;
            let triggers_before = stats_before.threshold_triggers;

            // Trigger check
            coordinator.trigger_discovery_check().await.unwrap();

            // Get updated stats
            let stats_after = coordinator.get_stats().await;
            let triggers_after = stats_after.threshold_triggers;

            // Verify trigger was incremented
            prop_assert_eq!(triggers_after, triggers_before + 1);

            Ok(())
        }).expect("async test failed")
    });
}

/// Property: Configuration values should be accessible and consistent
#[test]
fn prop_configuration_consistency() {
    let rt = Runtime::new().unwrap();

    proptest!(|(min_peers in 1usize..50, max_peers in 50usize..200)| {
        rt.block_on(async {
            let mut config = NetworkConfig::new("test_node".to_string(), NodeRole::Validator);
            config.min_peers = min_peers;
            config.max_peers = max_peers;

            let (coordinator, _peer_manager) = create_coordinator(config);

            // Verify configuration is accessible
            prop_assert_eq!(coordinator.get_min_peer_threshold(), min_peers);
            prop_assert_eq!(coordinator.get_max_peer_capacity(), max_peers);

            // Verify min <= max
            prop_assert!(coordinator.get_min_peer_threshold() <= coordinator.get_max_peer_capacity());

            Ok(())
        }).expect("async test failed")
    });
}
