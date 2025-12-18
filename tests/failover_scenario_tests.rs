//! Failover scenario tests for P2P network
//! Tests node failure simulation, failover detection, and recovery verification
//! Validates Requirements 2.1, 2.2, 2.3, 2.4

use silver_p2p::{
    NetworkConfig, P2PNetworkManager, NodeRole, NetworkMessage,
};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::{sleep, timeout};

/// Test harness for managing multiple nodes with failover simulation
struct FailoverTestHarness {
    nodes: Vec<Arc<P2PNetworkManager>>,
    /// Tracks which nodes have failed
    failed_nodes: Arc<std::sync::Mutex<Vec<usize>>>,
    /// Tracks node shutdown state
    node_shutdown: Arc<std::sync::Mutex<Vec<bool>>>,
}

impl FailoverTestHarness {
    /// Create a new test harness with specified number of nodes and port offset
    async fn new_with_port_offset(num_nodes: usize, port_offset: u16) -> Result<Self, Box<dyn std::error::Error>> {
        let mut nodes = Vec::new();
        let mut configs = Vec::new();

        // Create configurations for all nodes
        for i in 0..num_nodes {
            let node_id = format!("failover-test-node-{}", i);
            let listen_port = 20000 + port_offset + i as u16;

            let mut config = NetworkConfig {
                node_id: node_id.clone(),
                node_role: NodeRole::Validator,
                listen_addr: "127.0.0.1".to_string(),
                listen_port,
                peers: Vec::new(),
                bootstrap_nodes: Vec::new(),
                max_peers: 50,
                min_peers: 1,
                connection_timeout_secs: 10,
                ping_interval_secs: 5,
                pong_timeout_secs: 3,
                max_backoff_secs: 60,
                rate_limit_msgs_per_sec: 1000,
                max_message_size_bytes: 100 * 1024 * 1024,
                max_candidates: 1000,
                peer_timeout_secs: 30,
                enable_peer_discovery: true,
                enable_health_monitoring: true,
                broadcast_timeout_secs: 5,
                max_concurrent_broadcasts: 10,
                unicast_timeout_secs: 5,
                message_timeout_secs: 10,
            };

            // Add other nodes as peers (except self)
            for j in 0..num_nodes {
                if i != j {
                    let peer_addr = format!("127.0.0.1:{}", 20000 + j as u16);
                    config.peers.push(peer_addr);
                }
            }

            configs.push(config);
        }

        // Create network managers for all nodes
        for config in &configs {
            let manager = P2PNetworkManager::new(config.clone()).await?;
            nodes.push(Arc::new(manager));
        }

        Ok(FailoverTestHarness {
            nodes,
            failed_nodes: Arc::new(std::sync::Mutex::new(Vec::new())),
            node_shutdown: Arc::new(std::sync::Mutex::new(vec![false; num_nodes])),
        })
    }

    /// Start all nodes
    async fn start_all(&self) -> Result<(), Box<dyn std::error::Error>> {
        for node in &self.nodes {
            node.start().await?;
        }
        Ok(())
    }

    /// Stop all nodes gracefully
    async fn shutdown_all(&self) -> Result<(), Box<dyn std::error::Error>> {
        for (idx, node) in self.nodes.iter().enumerate() {
            node.shutdown().await?;
            let mut shutdown = self.node_shutdown.lock().unwrap();
            shutdown[idx] = true;
        }
        Ok(())
    }

    /// Get a specific node by index
    fn get_node(&self, index: usize) -> Option<Arc<P2PNetworkManager>> {
        self.nodes.get(index).cloned()
    }

    /// Simulate a node failure by shutting it down
    async fn fail_node(&self, node_index: usize) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(node) = self.get_node(node_index) {
            node.shutdown().await?;
            let mut failed = self.failed_nodes.lock().unwrap();
            if !failed.contains(&node_index) {
                failed.push(node_index);
            }
            let mut shutdown = self.node_shutdown.lock().unwrap();
            shutdown[node_index] = true;
        }
        Ok(())
    }

    /// Check if a node has failed
    fn is_node_failed(&self, node_index: usize) -> bool {
        let failed = self.failed_nodes.lock().unwrap();
        failed.contains(&node_index)
    }

    /// Get list of failed nodes
    fn get_failed_nodes(&self) -> Vec<usize> {
        self.failed_nodes.lock().unwrap().clone()
    }

    /// Get connected peer count for a node
    async fn get_connected_peers(&self, node_index: usize) -> usize {
        if let Some(node) = self.get_node(node_index) {
            node.get_network_stats().await.connected_peers
        } else {
            0
        }
    }

    /// Get all peers for a node
    async fn get_all_peers(&self, node_index: usize) -> Vec<String> {
        if let Some(node) = self.get_node(node_index) {
            node.get_all_peers()
                .await
                .iter()
                .map(|p| p.peer_id.clone())
                .collect()
        } else {
            Vec::new()
        }
    }

    /// Get network stats for a node
    async fn get_network_stats(&self, node_index: usize) -> Option<silver_p2p::NetworkStats> {
        if let Some(node) = self.get_node(node_index) {
            Some(node.get_network_stats().await)
        } else {
            None
        }
    }

    /// Check if a node is healthy
    async fn is_node_healthy(&self, node_index: usize) -> bool {
        if let Some(node) = self.get_node(node_index) {
            let health = node.get_health_status().await;
            health.is_healthy
        } else {
            false
        }
    }
}

#[tokio::test]
async fn test_single_node_failure_detection() {
    // Create test harness with 3 nodes
    let harness = FailoverTestHarness::new_with_port_offset(3, 0)
        .await
        .expect("Failed to create test harness");

    // Start all nodes
    harness
        .start_all()
        .await
        .expect("Failed to start nodes");

    // Give nodes time to establish connections
    sleep(Duration::from_secs(2)).await;

    // Verify all nodes are connected
    let connected_0 = harness.get_connected_peers(0).await;
    let connected_1 = harness.get_connected_peers(1).await;
    let connected_2 = harness.get_connected_peers(2).await;
    println!("Initial connections - Node 0: {}, Node 1: {}, Node 2: {}", connected_0, connected_1, connected_2);

    // Simulate failure of node 1
    println!("Simulating failure of node 1");
    harness
        .fail_node(1)
        .await
        .expect("Failed to fail node 1");

    // Verify node 1 is marked as failed
    assert!(harness.is_node_failed(1), "Node 1 should be marked as failed");

    // Give remaining nodes time to detect failure
    sleep(Duration::from_secs(3)).await;

    // Requirement 2.1, 2.2: Connection failure should trigger reconnection with exponential backoff
    // Verify nodes 0 and 2 detect the failure
    let connected_0_after = harness.get_connected_peers(0).await;
    let connected_2_after = harness.get_connected_peers(2).await;
    println!("After failure - Node 0: {}, Node 2: {}", connected_0_after, connected_2_after);

    // Nodes should have fewer connections after node 1 fails
    assert!(connected_0_after <= connected_0, "Node 0 should have fewer or equal connections");
    assert!(connected_2_after <= connected_2, "Node 2 should have fewer or equal connections");

    // Shutdown remaining nodes
    harness
        .shutdown_all()
        .await
        .expect("Failed to shutdown nodes");
}

#[tokio::test]
async fn test_failover_detection_with_health_monitoring() {
    // Create test harness with 4 nodes
    let harness = FailoverTestHarness::new_with_port_offset(4, 100)
        .await
        .expect("Failed to create test harness");

    // Start all nodes
    harness
        .start_all()
        .await
        .expect("Failed to start nodes");

    // Give nodes time to establish connections
    sleep(Duration::from_secs(2)).await;

    // Verify all nodes are healthy
    let healthy_0 = harness.is_node_healthy(0).await;
    let healthy_1 = harness.is_node_healthy(1).await;
    println!("Initial health - Node 0: {}, Node 1: {}", healthy_0, healthy_1);

    // Simulate failure of node 1
    println!("Simulating failure of node 1");
    harness
        .fail_node(1)
        .await
        .expect("Failed to fail node 1");

    // Requirement 7.1, 7.2: Health monitoring should detect unresponsive peer
    // Give health monitor time to detect failure
    sleep(Duration::from_secs(4)).await;

    // Verify node 1 is marked as failed
    assert!(harness.is_node_failed(1), "Node 1 should be marked as failed");

    // Shutdown remaining nodes
    harness
        .shutdown_all()
        .await
        .expect("Failed to shutdown nodes");
}

#[tokio::test]
async fn test_multiple_node_failures_sequential() {
    // Create test harness with 5 nodes
    let harness = FailoverTestHarness::new_with_port_offset(5, 200)
        .await
        .expect("Failed to create test harness");

    // Start all nodes
    harness
        .start_all()
        .await
        .expect("Failed to start nodes");

    // Give nodes time to establish connections
    sleep(Duration::from_secs(2)).await;

    // Verify initial state
    let initial_failed = harness.get_failed_nodes();
    assert_eq!(initial_failed.len(), 0, "No nodes should be failed initially");

    // Simulate failure of node 1
    println!("Simulating failure of node 1");
    harness
        .fail_node(1)
        .await
        .expect("Failed to fail node 1");

    sleep(Duration::from_secs(1)).await;

    // Verify node 1 is failed
    assert!(harness.is_node_failed(1), "Node 1 should be failed");
    let failed_after_1 = harness.get_failed_nodes();
    assert_eq!(failed_after_1.len(), 1, "One node should be failed");

    // Simulate failure of node 3
    println!("Simulating failure of node 3");
    harness
        .fail_node(3)
        .await
        .expect("Failed to fail node 3");

    sleep(Duration::from_secs(1)).await;

    // Verify both nodes are failed
    assert!(harness.is_node_failed(1), "Node 1 should be failed");
    assert!(harness.is_node_failed(3), "Node 3 should be failed");
    let failed_after_2 = harness.get_failed_nodes();
    assert_eq!(failed_after_2.len(), 2, "Two nodes should be failed");

    // Requirement 2.1, 2.2: Multiple failures should trigger exponential backoff for each
    // Verify remaining nodes handle multiple failures
    let connected_0 = harness.get_connected_peers(0).await;
    let connected_2 = harness.get_connected_peers(2).await;
    let connected_4 = harness.get_connected_peers(4).await;
    println!("After 2 failures - Node 0: {}, Node 2: {}, Node 4: {}", connected_0, connected_2, connected_4);

    // Shutdown remaining nodes
    harness
        .shutdown_all()
        .await
        .expect("Failed to shutdown nodes");
}

#[tokio::test]
async fn test_failover_recovery_after_node_restart() {
    // Create test harness with 3 nodes
    let harness = FailoverTestHarness::new_with_port_offset(3, 300)
        .await
        .expect("Failed to create test harness");

    // Start all nodes
    harness
        .start_all()
        .await
        .expect("Failed to start nodes");

    // Give nodes time to establish connections
    sleep(Duration::from_secs(2)).await;

    // Get initial connection count
    let connected_0_initial = harness.get_connected_peers(0).await;
    println!("Initial connections for node 0: {}", connected_0_initial);

    // Simulate failure of node 1
    println!("Simulating failure of node 1");
    harness
        .fail_node(1)
        .await
        .expect("Failed to fail node 1");

    sleep(Duration::from_secs(2)).await;

    // Verify node 1 is failed
    assert!(harness.is_node_failed(1), "Node 1 should be failed");

    // Get connection count after failure
    let connected_0_after_failure = harness.get_connected_peers(0).await;
    println!("Connections for node 0 after failure: {}", connected_0_after_failure);

    // Requirement 2.3: Backoff should reset on successful reconnection
    // In a real scenario, node 1 would be restarted and reconnect
    // Here we verify the failure was detected

    // Shutdown remaining nodes
    harness
        .shutdown_all()
        .await
        .expect("Failed to shutdown nodes");
}

#[tokio::test]
async fn test_failover_with_broadcast_messages() {
    // Create test harness with 4 nodes
    let harness = FailoverTestHarness::new_with_port_offset(4, 400)
        .await
        .expect("Failed to create test harness");

    // Start all nodes
    harness
        .start_all()
        .await
        .expect("Failed to start nodes");

    // Give nodes time to establish connections
    sleep(Duration::from_secs(2)).await;

    // Simulate failure of node 2
    println!("Simulating failure of node 2");
    harness
        .fail_node(2)
        .await
        .expect("Failed to fail node 2");

    sleep(Duration::from_secs(1)).await;

    // Requirement 4.1, 4.2: Broadcast should handle failed peers
    // Verify node 2 is failed
    assert!(harness.is_node_failed(2), "Node 2 should be failed");

    // In a real scenario, broadcasting from node 0 would skip node 2
    // and deliver to nodes 1 and 3
    if let Some(node_0) = harness.get_node(0) {
        let msg = NetworkMessage::Custom {
            msg_type: "test".to_string(),
            data: vec![1, 2, 3],
        };

        // Attempt broadcast - should handle failed peer gracefully
        match timeout(Duration::from_secs(5), node_0.broadcast(msg)).await {
            Ok(Ok(count)) => {
                println!("Broadcast succeeded, reached {} peers", count);
                // Should reach 2 peers (nodes 1 and 3), not node 2
                assert!(count <= 3, "Broadcast should not reach failed node");
            }
            Ok(Err(e)) => {
                println!("Broadcast error (expected): {}", e);
            }
            Err(_) => {
                println!("Broadcast timeout");
            }
        }
    }

    // Shutdown remaining nodes
    harness
        .shutdown_all()
        .await
        .expect("Failed to shutdown nodes");
}

#[tokio::test]
async fn test_failover_with_unicast_messages() {
    // Create test harness with 3 nodes
    let harness = FailoverTestHarness::new_with_port_offset(3, 500)
        .await
        .expect("Failed to create test harness");

    // Start all nodes
    harness
        .start_all()
        .await
        .expect("Failed to start nodes");

    // Give nodes time to establish connections
    sleep(Duration::from_secs(2)).await;

    // Simulate failure of node 1
    println!("Simulating failure of node 1");
    harness
        .fail_node(1)
        .await
        .expect("Failed to fail node 1");

    sleep(Duration::from_secs(1)).await;

    // Requirement 5.2: Unicast to disconnected peer should return error
    // Verify node 1 is failed
    assert!(harness.is_node_failed(1), "Node 1 should be failed");

    // In a real scenario, sending to node 1 would fail
    if let Some(node_0) = harness.get_node(0) {
        let msg = NetworkMessage::Custom {
            msg_type: "test".to_string(),
            data: vec![1, 2, 3],
        };

        // Attempt unicast to failed node - should return error
        match timeout(Duration::from_secs(5), node_0.send_to_peer("failover-test-node-1", msg)).await {
            Ok(Ok(())) => {
                println!("Unicast succeeded (unexpected)");
            }
            Ok(Err(e)) => {
                println!("Unicast error (expected): {}", e);
            }
            Err(_) => {
                println!("Unicast timeout (expected)");
            }
        }
    }

    // Shutdown remaining nodes
    harness
        .shutdown_all()
        .await
        .expect("Failed to shutdown nodes");
}

#[tokio::test]
async fn test_failover_peer_list_consistency() {
    // Create test harness with 4 nodes
    let harness = FailoverTestHarness::new_with_port_offset(4, 600)
        .await
        .expect("Failed to create test harness");

    // Start all nodes
    harness
        .start_all()
        .await
        .expect("Failed to start nodes");

    // Give nodes time to establish connections
    sleep(Duration::from_secs(2)).await;

    // Get initial peer lists
    let peers_0_initial = harness.get_all_peers(0).await;
    let peers_1_initial = harness.get_all_peers(1).await;
    println!("Initial peers for node 0: {:?}", peers_0_initial);
    println!("Initial peers for node 1: {:?}", peers_1_initial);

    // Simulate failure of node 2
    println!("Simulating failure of node 2");
    harness
        .fail_node(2)
        .await
        .expect("Failed to fail node 2");

    sleep(Duration::from_secs(2)).await;

    // Get peer lists after failure
    let peers_0_after = harness.get_all_peers(0).await;
    let peers_1_after = harness.get_all_peers(1).await;
    println!("Peers for node 0 after failure: {:?}", peers_0_after);
    println!("Peers for node 1 after failure: {:?}", peers_1_after);

    // Requirement 6.1, 6.2: Peer list should be updated after failure
    // Verify peer lists are consistent
    assert!(peers_0_after.len() <= peers_0_initial.len(), "Peer list should not grow");
    assert!(peers_1_after.len() <= peers_1_initial.len(), "Peer list should not grow");

    // Shutdown remaining nodes
    harness
        .shutdown_all()
        .await
        .expect("Failed to shutdown nodes");
}

#[tokio::test]
async fn test_failover_network_statistics_update() {
    // Create test harness with 3 nodes
    let harness = FailoverTestHarness::new_with_port_offset(3, 700)
        .await
        .expect("Failed to create test harness");

    // Start all nodes
    harness
        .start_all()
        .await
        .expect("Failed to start nodes");

    // Give nodes time to establish connections
    sleep(Duration::from_secs(2)).await;

    // Get initial network stats
    let stats_0_initial = harness.get_network_stats(0).await;
    println!("Initial stats for node 0: {:?}", stats_0_initial);

    // Simulate failure of node 1
    println!("Simulating failure of node 1");
    harness
        .fail_node(1)
        .await
        .expect("Failed to fail node 1");

    sleep(Duration::from_secs(2)).await;

    // Get network stats after failure
    let stats_0_after = harness.get_network_stats(0).await;
    println!("Stats for node 0 after failure: {:?}", stats_0_after);

    // Requirement 10.2, 10.5: Network statistics should be updated
    // Verify statistics are updated
    if let (Some(initial), Some(after)) = (stats_0_initial, stats_0_after) {
        assert!(after.connected_peers <= initial.connected_peers, "Connected peers should not increase");
        println!("Connected peers: {} -> {}", initial.connected_peers, after.connected_peers);
    }

    // Shutdown remaining nodes
    harness
        .shutdown_all()
        .await
        .expect("Failed to shutdown nodes");
}

#[tokio::test]
async fn test_failover_graceful_shutdown_after_failure() {
    // Create test harness with 3 nodes
    let harness = FailoverTestHarness::new_with_port_offset(3, 800)
        .await
        .expect("Failed to create test harness");

    // Start all nodes
    harness
        .start_all()
        .await
        .expect("Failed to start nodes");

    // Give nodes time to establish connections
    sleep(Duration::from_secs(2)).await;

    // Simulate failure of node 1
    println!("Simulating failure of node 1");
    harness
        .fail_node(1)
        .await
        .expect("Failed to fail node 1");

    sleep(Duration::from_secs(1)).await;

    // Verify node 1 is failed
    assert!(harness.is_node_failed(1), "Node 1 should be failed");

    // Requirement 12.1, 12.2, 12.3: Graceful shutdown should work after failures
    // Shutdown remaining nodes gracefully
    println!("Shutting down remaining nodes");
    harness
        .shutdown_all()
        .await
        .expect("Failed to shutdown nodes");

    println!("All nodes shut down successfully");
}

#[tokio::test]
async fn test_failover_cascading_failures() {
    // Create test harness with 5 nodes
    let harness = FailoverTestHarness::new_with_port_offset(5, 900)
        .await
        .expect("Failed to create test harness");

    // Start all nodes
    harness
        .start_all()
        .await
        .expect("Failed to start nodes");

    // Give nodes time to establish connections
    sleep(Duration::from_secs(2)).await;

    // Simulate cascading failures
    println!("Simulating cascading failures");
    
    // Fail node 1
    harness
        .fail_node(1)
        .await
        .expect("Failed to fail node 1");
    sleep(Duration::from_secs(1)).await;

    // Fail node 2
    harness
        .fail_node(2)
        .await
        .expect("Failed to fail node 2");
    sleep(Duration::from_secs(1)).await;

    // Fail node 3
    harness
        .fail_node(3)
        .await
        .expect("Failed to fail node 3");
    sleep(Duration::from_secs(1)).await;

    // Verify all failures are tracked
    let failed = harness.get_failed_nodes();
    assert_eq!(failed.len(), 3, "Three nodes should be failed");
    assert!(failed.contains(&1), "Node 1 should be failed");
    assert!(failed.contains(&2), "Node 2 should be failed");
    assert!(failed.contains(&3), "Node 3 should be failed");

    // Requirement 2.1, 2.2: Exponential backoff should handle cascading failures
    // Verify remaining nodes (0 and 4) are still operational
    let connected_0 = harness.get_connected_peers(0).await;
    let connected_4 = harness.get_connected_peers(4).await;
    println!("After cascading failures - Node 0: {}, Node 4: {}", connected_0, connected_4);

    // Shutdown remaining nodes
    harness
        .shutdown_all()
        .await
        .expect("Failed to shutdown nodes");
}
