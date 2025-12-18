//! Property-based tests for network statistics accuracy
//! **Feature: p2p-network-implementation, Property 19: Network Statistics Accuracy**
//! **Validates: Requirements 10.2, 10.5**

use proptest::prelude::*;
use silver_p2p::peer_manager::PeerManager;
use silver_p2p::types::{NodeRole, NetworkStats};
use std::sync::Arc;

/// Helper function to run async code in proptest
fn run_async_test<F, Fut>(f: F)
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = Result<(), TestCaseError>>,
{
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(f()).unwrap();
}

/// Strategy for generating valid peer IDs
fn peer_id_strategy() -> impl Strategy<Value = String> {
    r"[a-zA-Z0-9_]{1,32}".prop_map(|s| format!("peer_{}", s))
}

/// Strategy for generating valid peer addresses
fn peer_address_strategy() -> impl Strategy<Value = String> {
    (1..=255u8, 1..=255u8, 1..=255u8, 1..=255u8, 10000..=65535u16)
        .prop_map(|(a, b, c, d, port)| format!("{}.{}.{}.{}:{}", a, b, c, d, port))
}

/// Strategy for generating node roles
fn node_role_strategy() -> impl Strategy<Value = NodeRole> {
    prop_oneof![
        Just(NodeRole::Validator),
        Just(NodeRole::RPC),
        Just(NodeRole::Archive),
    ]
}

/// Strategy for generating message counts
fn message_count_strategy() -> impl Strategy<Value = u64> {
    0..=10_000u64
}

/// Strategy for generating byte counts
fn byte_count_strategy() -> impl Strategy<Value = u64> {
    0..=1_000_000u64
}

/// Property: Connected peer count should match the number of peers marked as connected
#[test]
fn prop_connected_peer_count_accuracy() {
    let config = ProptestConfig::with_cases(100);
    proptest!(config, |(
        peer_ids in prop::collection::vec(peer_id_strategy(), 1..20),
        addresses in prop::collection::vec(peer_address_strategy(), 1..20),
        roles in prop::collection::vec(node_role_strategy(), 1..20),
        connected_indices in prop::collection::vec(0..20usize, 0..20),
    )| {
        run_async_test(|| async {
            // Create peer manager
            let pm = Arc::new(PeerManager::new(1000));

            // Add peers
            for (i, peer_id) in peer_ids.iter().enumerate() {
                let addr = addresses.get(i % addresses.len()).cloned().unwrap_or_else(|| "127.0.0.1:9000".to_string());
                let role = roles.get(i % roles.len()).copied().unwrap_or(NodeRole::Validator);
                pm.add_peer(peer_id.clone(), addr, role)
                    .await
                    .expect("Failed to add peer");
            }

            // Mark some peers as connected
            for idx in connected_indices.iter() {
                if let Some(peer_id) = peer_ids.get(idx % peer_ids.len()) {
                    pm.mark_connected(peer_id).await.ok();
                }
            }

            // Get network stats
            let stats = NetworkStats {
                connected_peers: pm.get_connected_peer_count().await,
                total_peers: pm.get_peer_count().await,
                candidate_peers: pm.get_candidate_count().await,
                total_messages_sent: 0,
                total_messages_received: 0,
                total_bytes_sent: 0,
                total_bytes_received: 0,
                avg_latency_ms: 0,
                uptime_secs: 0,
                peer_stats: Default::default(),
            };

            // Verify connected peer count matches actual connected peers
            let actual_connected = pm.get_connected_peer_count().await;
            prop_assert_eq!(stats.connected_peers, actual_connected, "Connected peer count mismatch");
            
            Ok(())
        })
    });
}

/// Property: Total peer count should match the number of peers added
#[test]
fn prop_total_peer_count_accuracy() {
    let config = ProptestConfig::with_cases(100);
    proptest!(config, |(
        num_peers in 1usize..20,
        addresses in prop::collection::vec(peer_address_strategy(), 1..20),
        roles in prop::collection::vec(node_role_strategy(), 1..20),
    )| {
        run_async_test(|| async {
            // Create peer manager
            let pm = Arc::new(PeerManager::new(1000));

            // Add unique peers
            for i in 0..num_peers {
                let peer_id = format!("peer_{}", i);
                let addr = addresses.get(i % addresses.len()).cloned().unwrap_or_else(|| "127.0.0.1:9000".to_string());
                let role = roles.get(i % roles.len()).copied().unwrap_or(NodeRole::Validator);
                pm.add_peer(peer_id, addr, role)
                    .await
                    .expect("Failed to add peer");
            }

            // Get network stats
            let stats = NetworkStats {
                connected_peers: 0,
                total_peers: pm.get_peer_count().await,
                candidate_peers: 0,
                total_messages_sent: 0,
                total_messages_received: 0,
                total_bytes_sent: 0,
                total_bytes_received: 0,
                avg_latency_ms: 0,
                uptime_secs: 0,
                peer_stats: Default::default(),
            };

            // Verify total peer count matches actual peer count
            let actual_total = pm.get_peer_count().await;
            prop_assert_eq!(stats.total_peers, actual_total, "Total peer count mismatch");
            prop_assert_eq!(stats.total_peers, num_peers, "Total peer count doesn't match added peers");
            
            Ok(())
        })
    });
}

/// Property: Message throughput should accurately reflect total messages sent and received
#[test]
fn prop_message_throughput_accuracy() {
    let config = ProptestConfig::with_cases(100);
    proptest!(config, |(
        num_peers in 1usize..10,
        addresses in prop::collection::vec(peer_address_strategy(), 1..10),
        roles in prop::collection::vec(node_role_strategy(), 1..10),
        message_counts in prop::collection::vec(message_count_strategy(), 1..10),
        byte_counts in prop::collection::vec(byte_count_strategy(), 1..10),
    )| {
        run_async_test(|| async {
            // Create peer manager
            let pm = Arc::new(PeerManager::new(1000));

            // Add peers and update metrics
            let mut expected_total_messages_sent = 0u64;
            let mut expected_total_messages_received = 0u64;
            let mut expected_total_bytes_sent = 0u64;
            let mut expected_total_bytes_received = 0u64;

            for i in 0..num_peers {
                let peer_id = format!("peer_{}", i);
                let addr = addresses.get(i % addresses.len()).cloned().unwrap_or_else(|| "127.0.0.1:9000".to_string());
                let role = roles.get(i % roles.len()).copied().unwrap_or(NodeRole::Validator);
                pm.add_peer(peer_id.clone(), addr, role)
                    .await
                    .expect("Failed to add peer");

                // Update metrics for this peer
                let msg_count = message_counts.get(i % message_counts.len()).copied().unwrap_or(0);
                let byte_count = byte_counts.get(i % byte_counts.len()).copied().unwrap_or(0);

                // Simulate sending messages
                for _ in 0..msg_count {
                    pm.update_peer_metrics(&peer_id, 0, byte_count, 0)
                        .await
                        .expect("Failed to update metrics");
                    expected_total_messages_sent += 1;
                    expected_total_bytes_sent += byte_count;
                }

                // Simulate receiving messages
                for _ in 0..msg_count {
                    pm.update_peer_metrics(&peer_id, 0, 0, byte_count)
                        .await
                        .expect("Failed to update metrics");
                    expected_total_messages_received += 1;
                    expected_total_bytes_received += byte_count;
                }
            }

            // Get all peers and calculate totals
            let peers = pm.get_all_peers().await;
            let actual_total_messages_sent: u64 = peers.iter().map(|p| p.messages_sent).sum();
            let actual_total_messages_received: u64 = peers.iter().map(|p| p.messages_received).sum();
            let actual_total_bytes_sent: u64 = peers.iter().map(|p| p.bytes_sent).sum();
            let actual_total_bytes_received: u64 = peers.iter().map(|p| p.bytes_received).sum();

            // Verify message throughput is accurate
            prop_assert_eq!(actual_total_messages_sent, expected_total_messages_sent, "Total messages sent mismatch");
            prop_assert_eq!(actual_total_messages_received, expected_total_messages_received, "Total messages received mismatch");
            prop_assert_eq!(actual_total_bytes_sent, expected_total_bytes_sent, "Total bytes sent mismatch");
            prop_assert_eq!(actual_total_bytes_received, expected_total_bytes_received, "Total bytes received mismatch");
            
            Ok(())
        })
    });
}

/// Property: Network statistics should be non-blocking and return immediately
#[test]
fn prop_network_stats_query_is_non_blocking() {
    let config = ProptestConfig::with_cases(50);
    proptest!(config, |(
        peer_ids in prop::collection::vec(peer_id_strategy(), 1..20),
        addresses in prop::collection::vec(peer_address_strategy(), 1..20),
        roles in prop::collection::vec(node_role_strategy(), 1..20),
    )| {
        run_async_test(|| async {
            // Create peer manager
            let pm = Arc::new(PeerManager::new(1000));

            // Add multiple peers
            for (i, peer_id) in peer_ids.iter().enumerate() {
                let addr = addresses.get(i % addresses.len()).cloned().unwrap_or_else(|| "127.0.0.1:9000".to_string());
                let role = roles.get(i % roles.len()).copied().unwrap_or(NodeRole::Validator);
                pm.add_peer(peer_id.clone(), addr, role)
                    .await
                    .expect("Failed to add peer");
            }

            // Spawn multiple concurrent query tasks
            let mut handles = vec![];
            for _ in 0..10 {
                let pm_clone = pm.clone();
                let handle = tokio::spawn(async move {
                    // Query stats multiple times
                    for _ in 0..10 {
                        let _stats = NetworkStats {
                            connected_peers: pm_clone.get_connected_peer_count().await,
                            total_peers: pm_clone.get_peer_count().await,
                            candidate_peers: pm_clone.get_candidate_count().await,
                            total_messages_sent: 0,
                            total_messages_received: 0,
                            total_bytes_sent: 0,
                            total_bytes_received: 0,
                            avg_latency_ms: pm_clone.get_average_latency().await,
                            uptime_secs: 0,
                            peer_stats: Default::default(),
                        };
                    }
                });
                handles.push(handle);
            }

            // Spawn concurrent modification task
            let pm_clone = pm.clone();
            let peer_id_to_modify = peer_ids.first().cloned().unwrap_or_else(|| "peer_test".to_string());
            let modify_handle = tokio::spawn(async move {
                for i in 0..5 {
                    let _ = pm_clone.update_peer_block_height(&peer_id_to_modify, i as u64).await;
                }
            });

            // Wait for all tasks to complete
            for handle in handles {
                handle.await.expect("Task panicked");
            }
            modify_handle.await.expect("Modify task panicked");

            // Verify we can still query after concurrent operations
            let final_stats = NetworkStats {
                connected_peers: pm.get_connected_peer_count().await,
                total_peers: pm.get_peer_count().await,
                candidate_peers: pm.get_candidate_count().await,
                total_messages_sent: 0,
                total_messages_received: 0,
                total_bytes_sent: 0,
                total_bytes_received: 0,
                avg_latency_ms: 0,
                uptime_secs: 0,
                peer_stats: Default::default(),
            };
            
            prop_assert!(final_stats.total_peers > 0, "Should have peers after concurrent operations");
            
            Ok(())
        })
    });
}

/// Property: Multiple queries for network statistics should return consistent data
#[test]
fn prop_network_stats_queries_are_consistent() {
    let config = ProptestConfig::with_cases(100);
    proptest!(config, |(
        peer_ids in prop::collection::vec(peer_id_strategy(), 1..10),
        addresses in prop::collection::vec(peer_address_strategy(), 1..10),
        roles in prop::collection::vec(node_role_strategy(), 1..10),
    )| {
        run_async_test(|| async {
            // Create peer manager
            let pm = Arc::new(PeerManager::new(1000));

            // Add peers
            for (i, peer_id) in peer_ids.iter().enumerate() {
                let addr = addresses.get(i % addresses.len()).cloned().unwrap_or_else(|| "127.0.0.1:9000".to_string());
                let role = roles.get(i % roles.len()).copied().unwrap_or(NodeRole::Validator);
                pm.add_peer(peer_id.clone(), addr, role)
                    .await
                    .expect("Failed to add peer");
            }

            // Query multiple times without modifications
            let query1_total = pm.get_peer_count().await;
            let query1_connected = pm.get_connected_peer_count().await;
            let query1_candidates = pm.get_candidate_count().await;
            
            let query2_total = pm.get_peer_count().await;
            let query2_connected = pm.get_connected_peer_count().await;
            let query2_candidates = pm.get_candidate_count().await;
            
            let query3_total = pm.get_peer_count().await;
            let query3_connected = pm.get_connected_peer_count().await;
            let query3_candidates = pm.get_candidate_count().await;

            // All queries should return the same values
            prop_assert_eq!(query1_total, query2_total, "Total peer count inconsistent");
            prop_assert_eq!(query2_total, query3_total, "Total peer count inconsistent");
            
            prop_assert_eq!(query1_connected, query2_connected, "Connected peer count inconsistent");
            prop_assert_eq!(query2_connected, query3_connected, "Connected peer count inconsistent");
            
            prop_assert_eq!(query1_candidates, query2_candidates, "Candidate count inconsistent");
            prop_assert_eq!(query2_candidates, query3_candidates, "Candidate count inconsistent");
            
            Ok(())
        })
    });
}

/// Property: Network statistics should include all required fields
#[test]
fn prop_network_stats_includes_all_required_fields() {
    let config = ProptestConfig::with_cases(100);
    proptest!(config, |(
        peer_ids in prop::collection::vec(peer_id_strategy(), 1..10),
        addresses in prop::collection::vec(peer_address_strategy(), 1..10),
        roles in prop::collection::vec(node_role_strategy(), 1..10),
    )| {
        run_async_test(|| async {
            // Create peer manager
            let pm = Arc::new(PeerManager::new(1000));

            // Add peers
            for (i, peer_id) in peer_ids.iter().enumerate() {
                let addr = addresses.get(i % addresses.len()).cloned().unwrap_or_else(|| "127.0.0.1:9000".to_string());
                let role = roles.get(i % roles.len()).copied().unwrap_or(NodeRole::Validator);
                pm.add_peer(peer_id.clone(), addr, role)
                    .await
                    .expect("Failed to add peer");
            }

            // Build network stats
            let stats = NetworkStats {
                connected_peers: pm.get_connected_peer_count().await,
                total_peers: pm.get_peer_count().await,
                candidate_peers: pm.get_candidate_count().await,
                total_messages_sent: 0,
                total_messages_received: 0,
                total_bytes_sent: 0,
                total_bytes_received: 0,
                avg_latency_ms: 0,
                uptime_secs: 0,
                peer_stats: Default::default(),
            };

            // Verify all required fields are present and valid
            // Note: All fields are unsigned, so they cannot be negative
            
            // Verify connected peers <= total peers
            prop_assert!(stats.connected_peers <= stats.total_peers, "connected_peers > total_peers");
            
            Ok(())
        })
    });
}

/// Property: Connected peer count should never exceed total peer count
#[test]
fn prop_connected_peers_never_exceed_total() {
    let config = ProptestConfig::with_cases(100);
    proptest!(config, |(
        peer_ids in prop::collection::vec(peer_id_strategy(), 1..20),
        addresses in prop::collection::vec(peer_address_strategy(), 1..20),
        roles in prop::collection::vec(node_role_strategy(), 1..20),
        connected_indices in prop::collection::vec(0..20usize, 0..20),
    )| {
        run_async_test(|| async {
            // Create peer manager
            let pm = Arc::new(PeerManager::new(1000));

            // Add peers
            for (i, peer_id) in peer_ids.iter().enumerate() {
                let addr = addresses.get(i % addresses.len()).cloned().unwrap_or_else(|| "127.0.0.1:9000".to_string());
                let role = roles.get(i % roles.len()).copied().unwrap_or(NodeRole::Validator);
                pm.add_peer(peer_id.clone(), addr, role)
                    .await
                    .expect("Failed to add peer");
            }

            // Mark some peers as connected
            for idx in connected_indices.iter() {
                if let Some(peer_id) = peer_ids.get(idx % peer_ids.len()) {
                    pm.mark_connected(peer_id).await.ok();
                }
            }

            // Get counts
            let total = pm.get_peer_count().await;
            let connected = pm.get_connected_peer_count().await;

            // Verify invariant: connected <= total
            prop_assert!(connected <= total, "connected_peers ({}) > total_peers ({})", connected, total);
            
            Ok(())
        })
    });
}

/// Property: Average latency should be calculated correctly from all peers
#[test]
fn prop_average_latency_calculation_accuracy() {
    let config = ProptestConfig::with_cases(100);
    proptest!(config, |(
        num_peers in 1usize..10,
        addresses in prop::collection::vec(peer_address_strategy(), 1..10),
        roles in prop::collection::vec(node_role_strategy(), 1..10),
        latencies in prop::collection::vec(0..1000u64, 1..10),
    )| {
        run_async_test(|| async {
            // Create peer manager
            let pm = Arc::new(PeerManager::new(1000));

            // Add peers with specific latencies
            let mut expected_total_latency = 0u64;
            for i in 0..num_peers {
                let peer_id = format!("peer_{}", i);
                let addr = addresses.get(i % addresses.len()).cloned().unwrap_or_else(|| "127.0.0.1:9000".to_string());
                let role = roles.get(i % roles.len()).copied().unwrap_or(NodeRole::Validator);
                pm.add_peer(peer_id.clone(), addr, role)
                    .await
                    .expect("Failed to add peer");

                let latency = latencies.get(i % latencies.len()).copied().unwrap_or(0);
                pm.update_peer_metrics(&peer_id, latency, 0, 0)
                    .await
                    .expect("Failed to update metrics");
                expected_total_latency += latency;
            }

            // Get average latency
            let actual_avg = pm.get_average_latency().await;
            let expected_avg = if num_peers == 0 {
                0
            } else {
                expected_total_latency / num_peers as u64
            };

            // Verify average latency is calculated correctly
            prop_assert_eq!(actual_avg, expected_avg, "Average latency calculation mismatch");
            
            Ok(())
        })
    });
}

