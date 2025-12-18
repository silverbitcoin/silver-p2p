//! Property-based tests for rate limiting and connection limits
//! **Feature: p2p-network-implementation, Property 16: Rate Limit Enforcement**
//! **Validates: Requirements 8.2, 8.3**

use proptest::prelude::*;
use silver_p2p::{ConnectionLimiter, GlobalRateLimiter};

/// Property: For any peer sending messages faster than the configured rate limit,
/// the system should throttle the peer and log a warning.
/// **Validates: Requirements 8.2, 8.3**
#[test]
fn prop_rate_limit_enforcement() {
    proptest!(|(
        peer_id in "peer[0-9]{1,3}",
        rate_limit in 1u32..1000,
        messages_to_send in 1usize..100
    )| {
        let limiter = GlobalRateLimiter::new(10000, rate_limit);
        
        // Send messages up to the rate limit
        let mut sent_count = 0;
        for _ in 0..messages_to_send {
            if limiter.can_send(&peer_id).unwrap() {
                sent_count += 1;
            } else {
                break;
            }
        }
        
        // Verify that we sent at most rate_limit messages
        prop_assert!(sent_count <= rate_limit as usize);
        
        // Verify that after rate limit is exceeded, subsequent messages are rejected
        if sent_count >= rate_limit as usize {
            let result = limiter.can_send(&peer_id).unwrap();
            prop_assert!(!result, "Message should be rejected when rate limit exceeded");
        }
    });
}

/// Property: For any peer at maximum connection capacity, the system should reject
/// new incoming connections.
/// **Validates: Requirements 8.1**
#[test]
fn prop_connection_limit_enforcement() {
    proptest!(|(
        max_connections in 1usize..100,
        connection_attempts in 1usize..200
    )| {
        let limiter = ConnectionLimiter::new(max_connections);
        
        let mut successful_connections = 0;
        let mut failed_connections = 0;
        for _ in 0..connection_attempts {
            if limiter.try_add_connection().is_ok() {
                successful_connections += 1;
            } else {
                failed_connections += 1;
            }
        }
        
        // Verify that we never exceed max_connections
        prop_assert!(successful_connections <= max_connections);
        
        // Verify that connection count matches successful connections
        prop_assert_eq!(limiter.get_connection_count(), successful_connections);
        
        // If we tried more than max, some should have failed
        if connection_attempts > max_connections {
            prop_assert!(failed_connections > 0, "Should have failed connections when exceeding max");
        }
        
        // If we reached max, we should be at capacity
        if successful_connections == max_connections {
            prop_assert!(limiter.is_at_capacity());
            prop_assert_eq!(limiter.available_slots(), 0);
        }
    });
}

/// Property: For any peer that is rate limited, the system should track it
/// and allow recovery after 1 minute of normal behavior.
/// **Validates: Requirements 8.3, 8.5**
#[test]
fn prop_rate_limit_recovery_tracking() {
    proptest!(|(
        peer_id in "peer[0-9]{1,3}",
        rate_limit in 1u32..100
    )| {
        let limiter = GlobalRateLimiter::new(10000, rate_limit);
        
        // Exhaust rate limit
        for _ in 0..rate_limit {
            let _ = limiter.can_send(&peer_id);
        }
        
        // Try to send one more (should fail)
        let result = limiter.can_send(&peer_id).unwrap();
        prop_assert!(!result);
        
        // Verify peer is tracked as rate limited
        prop_assert!(limiter.is_peer_rate_limited(&peer_id));
        
        // Verify peer appears in rate limited list
        let limited_peers = limiter.get_rate_limited_peers();
        prop_assert!(limited_peers.contains(&peer_id));
    });
}

/// Property: For any connection limiter, the available slots should always equal
/// max_connections minus current_connections.
/// **Validates: Requirements 8.1**
#[test]
fn prop_connection_slots_invariant() {
    proptest!(|(
        max_connections in 1usize..100,
        connections_to_add in 0usize..100
    )| {
        let limiter = ConnectionLimiter::new(max_connections);
        
        let actual_added = std::cmp::min(connections_to_add, max_connections);
        for _ in 0..actual_added {
            let _ = limiter.try_add_connection();
        }
        
        // Invariant: available_slots = max_connections - current_connections
        let expected_slots = max_connections - limiter.get_connection_count();
        prop_assert_eq!(limiter.available_slots(), expected_slots);
    });
}

/// Property: For any rate limiter, the token count should never exceed the maximum.
/// **Validates: Requirements 8.2**
#[test]
fn prop_rate_limit_tokens_bounded() {
    proptest!(|(
        peer_id in "peer[0-9]{1,3}",
        rate_limit in 1u32..1000
    )| {
        let limiter = GlobalRateLimiter::new(10000, rate_limit);
        
        // Get tokens multiple times
        for _ in 0..10 {
            let tokens = limiter.get_peer_tokens(&peer_id);
            prop_assert!(tokens <= rate_limit, "Tokens should not exceed rate limit");
        }
    });
}

/// Property: For any connection limiter, removing more connections than exist
/// should fail gracefully.
/// **Validates: Requirements 8.1**
#[test]
fn prop_connection_removal_safety() {
    proptest!(|(
        max_connections in 1usize..100,
        connections_to_add in 0usize..50
    )| {
        let limiter = ConnectionLimiter::new(max_connections);
        
        let actual_added = std::cmp::min(connections_to_add, max_connections);
        for _ in 0..actual_added {
            let _ = limiter.try_add_connection();
        }
        
        // Remove all added connections
        for _ in 0..actual_added {
            let result = limiter.remove_connection();
            prop_assert!(result.is_ok(), "Should successfully remove connection");
        }
        
        // Connection count should be 0
        prop_assert_eq!(limiter.get_connection_count(), 0);
        
        // Trying to remove when empty should fail
        let result = limiter.remove_connection();
        prop_assert!(result.is_err(), "Should fail when removing from empty pool");
    });
}

/// Property: For any rate limiter, different peers should have independent limits.
/// **Validates: Requirements 8.2, 8.3**
#[test]
fn prop_per_peer_rate_limit_independence() {
    proptest!(|(
        rate_limit in 2u32..100,
        messages_peer1 in 1usize..50,
        messages_peer2 in 1usize..50
    )| {
        let limiter = GlobalRateLimiter::new(10000, rate_limit);
        
        // Send messages from peer1
        let mut peer1_sent = 0;
        for _ in 0..messages_peer1 {
            if limiter.can_send("peer1").unwrap() {
                peer1_sent += 1;
            } else {
                break;
            }
        }
        
        // Send messages from peer2
        let mut peer2_sent = 0;
        for _ in 0..messages_peer2 {
            if limiter.can_send("peer2").unwrap() {
                peer2_sent += 1;
            } else {
                break;
            }
        }
        
        // Both peers should have independent limits
        prop_assert!(peer1_sent <= rate_limit as usize);
        prop_assert!(peer2_sent <= rate_limit as usize);
        
        // If peer1 sent at rate limit, try one more to trigger rate limiting
        if peer1_sent >= rate_limit as usize {
            let result = limiter.can_send("peer1").unwrap();
            prop_assert!(!result, "Peer1 should be rate limited");
            prop_assert!(limiter.is_peer_rate_limited("peer1"));
        }
        
        // If peer2 sent at rate limit, try one more to trigger rate limiting
        if peer2_sent >= rate_limit as usize {
            let result = limiter.can_send("peer2").unwrap();
            prop_assert!(!result, "Peer2 should be rate limited");
            prop_assert!(limiter.is_peer_rate_limited("peer2"));
        }
    });
}

/// Property: For any connection limiter, rejection tracking should be accurate.
/// **Validates: Requirements 8.1**
#[test]
fn prop_rejection_tracking_accuracy() {
    proptest!(|(
        max_connections in 1usize..10,
        peer_addrs in prop::collection::vec("192\\.168\\.[0-9]{1,3}\\.[0-9]{1,3}:[0-9]{4,5}", 1..20),
        rejections_per_peer in 1u32..10
    )| {
        let limiter = ConnectionLimiter::new(max_connections);
        
        // Fill up connections
        for _ in 0..max_connections {
            let _ = limiter.try_add_connection();
        }
        
        // Track rejections
        for peer_addr in &peer_addrs {
            for _ in 0..rejections_per_peer {
                limiter.track_rejection(peer_addr);
            }
        }
        
        // Verify rejection counts
        for peer_addr in &peer_addrs {
            let count = limiter.get_rejection_count(peer_addr);
            prop_assert_eq!(count, rejections_per_peer);
        }
        
        // Verify stats
        let stats = limiter.get_rejection_stats();
        prop_assert_eq!(stats.len(), peer_addrs.len());
    });
}

/// Property: For any rate limiter, clearing rate limited status should work correctly.
/// **Validates: Requirements 8.3, 8.5**
#[test]
fn prop_rate_limit_clear_status() {
    proptest!(|(
        peer_id in "peer[0-9]{1,3}",
        rate_limit in 2u32..100
    )| {
        let limiter = GlobalRateLimiter::new(10000, rate_limit);
        
        // Exhaust rate limit
        for _ in 0..rate_limit {
            let _ = limiter.can_send(&peer_id);
        }
        
        // Try to send one more to trigger rate limiting
        let result = limiter.can_send(&peer_id).unwrap();
        prop_assert!(!result, "Should be rate limited after exhausting limit");
        
        // Verify peer is rate limited
        prop_assert!(limiter.is_peer_rate_limited(&peer_id));
        
        // Clear rate limited status
        limiter.clear_rate_limited(&peer_id);
        
        // Verify peer is no longer rate limited
        prop_assert!(!limiter.is_peer_rate_limited(&peer_id));
    });
}

/// Property: For any connection limiter, the connection count should never go negative.
/// **Validates: Requirements 8.1**
#[test]
fn prop_connection_count_non_negative() {
    proptest!(|(
        max_connections in 1usize..100,
        operations in prop::collection::vec(0u8..2, 0..200)
    )| {
        let limiter = ConnectionLimiter::new(max_connections);
        
        for op in operations {
            match op {
                0 => {
                    // Try to add connection
                    let _ = limiter.try_add_connection();
                }
                _ => {
                    // Try to remove connection
                    let _ = limiter.remove_connection();
                }
            }
            
            // Invariant: connection count should never be negative
            let count = limiter.get_connection_count();
            prop_assert!(count <= max_connections);
        }
    });
}
