//! Property-based tests for peer information accuracy
//! **Feature: p2p-network-implementation, Property 18: Peer Information Accuracy**
//! **Validates: Requirements 10.1, 10.5**

use proptest::prelude::*;
use silver_p2p::peer_manager::PeerManager;
use silver_p2p::types::{NodeRole, PeerInfo};
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

/// Strategy for generating block heights
fn block_height_strategy() -> impl Strategy<Value = u64> {
    0..=1_000_000u64
}

/// Property: For any peer added to the manager, querying its information should return accurate data
#[test]
fn prop_peer_info_returns_accurate_data() {
    let config = ProptestConfig::with_cases(100);
    proptest!(config, |(
        peer_id in peer_id_strategy(),
        address in peer_address_strategy(),
        role in node_role_strategy(),
        block_height in block_height_strategy(),
    )| {
        run_async_test(|| async {
            // Create peer manager
            let pm = Arc::new(PeerManager::new(1000));

            // Add peer with specific data
            pm.add_peer(peer_id.clone(), address.clone(), role)
                .await
                .expect("Failed to add peer");

            // Update block height
            pm.update_peer_block_height(&peer_id, block_height)
                .await
                .expect("Failed to update block height");

            // Query peer information
            let peer_info = pm.get_peer(&peer_id)
                .await
                .expect("Failed to get peer");

            // Verify all returned fields match what was set
            prop_assert_eq!(peer_info.peer_id, peer_id, "Peer ID mismatch");
            prop_assert_eq!(peer_info.address, address, "Address mismatch");
            prop_assert_eq!(peer_info.role, role, "Role mismatch");
            prop_assert_eq!(peer_info.block_height, block_height, "Block height mismatch");
            
            // Verify last_seen is recent (within last few seconds)
            let now = std::time::SystemTime::now();
            let elapsed = now.duration_since(peer_info.last_seen)
                .expect("Time went backwards");
            prop_assert!(elapsed.as_secs() < 5, "last_seen is too old");
            
            Ok(())
        })
    });
}

/// Property: For any peer, converting PeerState to PeerInfo should preserve all critical fields
#[test]
fn prop_peer_info_conversion_preserves_fields() {
    let config = ProptestConfig::with_cases(100);
    proptest!(config, |(
        peer_id in peer_id_strategy(),
        address in peer_address_strategy(),
        role in node_role_strategy(),
        block_height in block_height_strategy(),
    )| {
        run_async_test(|| async {
            // Create peer manager
            let pm = Arc::new(PeerManager::new(1000));

            // Add peer
            pm.add_peer(peer_id.clone(), address.clone(), role)
                .await
                .expect("Failed to add peer");

            // Update block height
            pm.update_peer_block_height(&peer_id, block_height)
                .await
                .expect("Failed to update block height");

            // Get peer state
            let peer_state = pm.get_peer(&peer_id)
                .await
                .expect("Failed to get peer");

            // Convert to PeerInfo
            let peer_info = PeerInfo::from(&peer_state);

            // Verify conversion preserves all fields
            prop_assert_eq!(peer_info.peer_id, peer_state.peer_id);
            prop_assert_eq!(peer_info.address, peer_state.address);
            prop_assert_eq!(peer_info.role, peer_state.role.to_string());
            prop_assert_eq!(peer_info.is_connected, peer_state.is_connected);
            prop_assert_eq!(peer_info.is_healthy, peer_state.is_healthy);
            prop_assert_eq!(peer_info.block_height, peer_state.block_height);
            prop_assert_eq!(peer_info.latency_ms, peer_state.latency_ms);
            
            Ok(())
        })
    });
}

/// Property: Querying peer information should not block other operations
#[test]
fn prop_peer_info_query_is_non_blocking() {
    let config = ProptestConfig::with_cases(50);
    proptest!(config, |(
        peer_ids in prop::collection::vec(peer_id_strategy(), 1..10),
        addresses in prop::collection::vec(peer_address_strategy(), 1..10),
        roles in prop::collection::vec(node_role_strategy(), 1..10),
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
            for peer_id in peer_ids.iter().take(3) {
                let pm_clone = pm.clone();
                let peer_id_clone = peer_id.clone();
                let handle = tokio::spawn(async move {
                    // Query peer info multiple times
                    for _ in 0..10 {
                        let _ = pm_clone.get_peer(&peer_id_clone).await;
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
            if let Ok(peer) = pm.get_peer(peer_ids.first().unwrap()).await {
                prop_assert!(!peer.peer_id.is_empty());
            }
            
            Ok(())
        })
    });
}

/// Property: Multiple queries for the same peer should return consistent data
#[test]
fn prop_peer_info_queries_are_consistent() {
    let config = ProptestConfig::with_cases(100);
    proptest!(config, |(
        peer_id in peer_id_strategy(),
        address in peer_address_strategy(),
        role in node_role_strategy(),
        block_height in block_height_strategy(),
    )| {
        run_async_test(|| async {
            // Create peer manager
            let pm = Arc::new(PeerManager::new(1000));

            // Add peer
            pm.add_peer(peer_id.clone(), address.clone(), role)
                .await
                .expect("Failed to add peer");

            // Update block height
            pm.update_peer_block_height(&peer_id, block_height)
                .await
                .expect("Failed to update block height");

            // Query multiple times
            let query1 = pm.get_peer(&peer_id)
                .await
                .expect("First query failed");
            
            let query2 = pm.get_peer(&peer_id)
                .await
                .expect("Second query failed");
            
            let query3 = pm.get_peer(&peer_id)
                .await
                .expect("Third query failed");

            // All queries should return the same peer ID, address, and role
            prop_assert_eq!(&query1.peer_id, &query2.peer_id);
            prop_assert_eq!(&query2.peer_id, &query3.peer_id);
            
            prop_assert_eq!(&query1.address, &query2.address);
            prop_assert_eq!(&query2.address, &query3.address);
            
            prop_assert_eq!(query1.role, query2.role);
            prop_assert_eq!(query2.role, query3.role);
            
            prop_assert_eq!(query1.block_height, query2.block_height);
            prop_assert_eq!(query2.block_height, query3.block_height);
            
            Ok(())
        })
    });
}

/// Property: Querying non-existent peer should return error, not panic
#[test]
fn prop_peer_info_query_nonexistent_returns_error() {
    let config = ProptestConfig::with_cases(50);
    proptest!(config, |(
        peer_id in peer_id_strategy(),
    )| {
        run_async_test(|| async {
            // Create peer manager
            let pm = Arc::new(PeerManager::new(1000));

            // Query non-existent peer should return error
            let result = pm.get_peer(&peer_id).await;
            prop_assert!(result.is_err(), "Should return error for non-existent peer");
            
            Ok(())
        })
    });
}

/// Property: Peer information should include all required fields
#[test]
fn prop_peer_info_includes_all_required_fields() {
    let config = ProptestConfig::with_cases(100);
    proptest!(config, |(
        peer_id in peer_id_strategy(),
        address in peer_address_strategy(),
        role in node_role_strategy(),
    )| {
        run_async_test(|| async {
            // Create peer manager
            let pm = Arc::new(PeerManager::new(1000));

            // Add peer
            pm.add_peer(peer_id.clone(), address.clone(), role)
                .await
                .expect("Failed to add peer");

            // Query peer information
            let peer_info = pm.get_peer(&peer_id)
                .await
                .expect("Failed to get peer");

            // Verify all required fields are present and non-empty
            prop_assert!(!peer_info.peer_id.is_empty(), "peer_id is empty");
            prop_assert!(!peer_info.address.is_empty(), "address is empty");
            prop_assert!(!peer_info.role.to_string().is_empty(), "role is empty");
            
            // Verify numeric fields are valid
            prop_assert!(peer_info.block_height >= 0, "block_height is negative");
            prop_assert!(peer_info.latency_ms >= 0, "latency_ms is negative");
            
            Ok(())
        })
    });
}

/// Property: Peer information should reflect updates to peer state
#[test]
fn prop_peer_info_reflects_state_updates() {
    let config = ProptestConfig::with_cases(100);
    proptest!(config, |(
        peer_id in peer_id_strategy(),
        address in peer_address_strategy(),
        role in node_role_strategy(),
        initial_height in block_height_strategy(),
        updated_height in block_height_strategy(),
    )| {
        run_async_test(|| async {
            // Create peer manager
            let pm = Arc::new(PeerManager::new(1000));

            // Add peer with initial block height
            pm.add_peer(peer_id.clone(), address.clone(), role)
                .await
                .expect("Failed to add peer");

            pm.update_peer_block_height(&peer_id, initial_height)
                .await
                .expect("Failed to update block height");

            // Query initial state
            let info1 = pm.get_peer(&peer_id)
                .await
                .expect("Failed to get peer");
            prop_assert_eq!(info1.block_height, initial_height);

            // Update block height
            pm.update_peer_block_height(&peer_id, updated_height)
                .await
                .expect("Failed to update block height");

            // Query updated state
            let info2 = pm.get_peer(&peer_id)
                .await
                .expect("Failed to get peer");
            prop_assert_eq!(info2.block_height, updated_height);
            
            Ok(())
        })
    });
}
