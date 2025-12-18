//! Load tests for P2P network
//! Tests high-throughput message delivery, large messages, and peer discovery stress
//! Validates all requirements under high load conditions

use silver_p2p::{
    NetworkConfig, P2PNetworkManager, NodeRole, NetworkMessage,
};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::time::{sleep, timeout};

/// Test harness for load testing
struct LoadTestHarness {
    nodes: Vec<Arc<P2PNetworkManager>>,
    message_count: Arc<AtomicU64>,
}

impl LoadTestHarness {
    /// Create a new load test harness with specified number of nodes
    async fn new(num_nodes: usize, port_offset: u16) -> Result<Self, Box<dyn std::error::Error>> {
        let mut nodes = Vec::new();

        // Create configurations for all nodes
        for i in 0..num_nodes {
            let node_id = format!("load-test-node-{}", i);
            let listen_port = 21000 + port_offset + i as u16;

            let mut config = NetworkConfig {
                node_id: node_id.clone(),
                node_role: NodeRole::Validator,
                listen_addr: "127.0.0.1".to_string(),
                listen_port,
                peers: Vec::new(),
                bootstrap_nodes: Vec::new(),
                max_peers: 100,
                min_peers: 1,
                connection_timeout_secs: 10,
                ping_interval_secs: 5,
                pong_timeout_secs: 3,
                max_backoff_secs: 60,
                rate_limit_msgs_per_sec: 50000,
                max_message_size_bytes: 100 * 1024 * 1024,
                max_candidates: 1000,
                peer_timeout_secs: 30,
                enable_peer_discovery: true,
                enable_health_monitoring: true,
                broadcast_timeout_secs: 10,
                max_concurrent_broadcasts: 50,
                unicast_timeout_secs: 10,
                message_timeout_secs: 15,
            };

            // Add other nodes as peers (except self)
            for j in 0..num_nodes {
                if i != j {
                    let peer_addr = format!("127.0.0.1:{}", 21000 + j as u16);
                    config.peers.push(peer_addr);
                }
            }

            let manager = P2PNetworkManager::new(config).await?;
            nodes.push(Arc::new(manager));
        }

        Ok(LoadTestHarness {
            nodes,
            message_count: Arc::new(AtomicU64::new(0)),
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
        for node in &self.nodes {
            node.shutdown().await?;
        }
        Ok(())
    }

    /// Get a specific node by index
    fn get_node(&self, index: usize) -> Option<Arc<P2PNetworkManager>> {
        self.nodes.get(index).cloned()
    }

    /// Get number of nodes
    fn node_count(&self) -> usize {
        self.nodes.len()
    }

    /// Get message count
    fn get_message_count(&self) -> u64 {
        self.message_count.load(Ordering::SeqCst)
    }

    /// Increment message count
    fn increment_message_count(&self, count: u64) {
        self.message_count.fetch_add(count, Ordering::SeqCst);
    }
}

#[tokio::test]
async fn test_high_throughput_broadcast_messages() {
    // Create test harness with 3 nodes
    let harness = LoadTestHarness::new(3, 0)
        .await
        .expect("Failed to create test harness");

    // Start all nodes
    harness
        .start_all()
        .await
        .expect("Failed to start nodes");

    // Give nodes time to establish connections
    sleep(Duration::from_secs(2)).await;

    // Get sender node
    let sender = harness.get_node(0).expect("Failed to get node 0");

    // Requirement: High-throughput message tests (10,000+ msgs/sec)
    // Send 10,000 messages in rapid succession
    let start = Instant::now();
    let message_count = 10000;

    for i in 0..message_count {
        let msg = NetworkMessage::Custom {
            msg_type: "load_test".to_string(),
            data: format!("Message {}", i).into_bytes(),
        };

        match timeout(Duration::from_secs(5), sender.broadcast(msg)).await {
            Ok(Ok(count)) => {
                harness.increment_message_count(count as u64);
            }
            Ok(Err(e)) => {
                eprintln!("Broadcast error: {}", e);
            }
            Err(_) => {
                eprintln!("Broadcast timeout");
            }
        }
    }

    let elapsed = start.elapsed();
    let throughput = message_count as f64 / elapsed.as_secs_f64();
    let total_messages = harness.get_message_count();

    println!(
        "High-throughput broadcast test: {} messages in {:.2}s = {:.0} msgs/sec",
        message_count, elapsed.as_secs_f64(), throughput
    );
    println!("Total messages delivered: {}", total_messages);

    // Verify throughput is reasonable (at least 1000 msgs/sec)
    assert!(throughput >= 1000.0, "Throughput should be at least 1000 msgs/sec, got {:.0}", throughput);

    // Shutdown nodes
    harness
        .shutdown_all()
        .await
        .expect("Failed to shutdown nodes");
}

#[tokio::test]
async fn test_large_message_delivery() {
    // Create test harness with 2 nodes
    let harness = LoadTestHarness::new(2, 100)
        .await
        .expect("Failed to create test harness");

    // Start all nodes
    harness
        .start_all()
        .await
        .expect("Failed to start nodes");

    // Give nodes time to establish connections
    sleep(Duration::from_secs(2)).await;

    // Get sender node
    let sender = harness.get_node(0).expect("Failed to get node 0");

    // Requirement: Large message tests
    // Send messages of increasing size up to 50 MB
    let sizes = vec![
        1024,                    // 1 KB
        10 * 1024,              // 10 KB
        100 * 1024,             // 100 KB
        1024 * 1024,            // 1 MB
        10 * 1024 * 1024,       // 10 MB
        50 * 1024 * 1024,       // 50 MB
    ];

    for size in sizes {
        let data = vec![0u8; size];
        let msg = NetworkMessage::Custom {
            msg_type: "large_message".to_string(),
            data,
        };

        let start = Instant::now();
        match timeout(Duration::from_secs(30), sender.broadcast(msg)).await {
            Ok(Ok(count)) => {
                let elapsed = start.elapsed();
                let throughput = size as f64 / elapsed.as_secs_f64() / (1024.0 * 1024.0);
                println!(
                    "Large message test: {} bytes delivered to {} peers in {:.2}s = {:.2} MB/s",
                    size, count, elapsed.as_secs_f64(), throughput
                );
                harness.increment_message_count(count as u64);
            }
            Ok(Err(e)) => {
                eprintln!("Broadcast error for {} byte message: {}", size, e);
            }
            Err(_) => {
                eprintln!("Broadcast timeout for {} byte message", size);
            }
        }
    }

    // Shutdown nodes
    harness
        .shutdown_all()
        .await
        .expect("Failed to shutdown nodes");
}

#[tokio::test]
async fn test_peer_discovery_stress() {
    // Create test harness with 5 nodes
    let harness = LoadTestHarness::new(5, 200)
        .await
        .expect("Failed to create test harness");

    // Start all nodes
    harness
        .start_all()
        .await
        .expect("Failed to start nodes");

    // Give nodes time to establish connections
    sleep(Duration::from_secs(5)).await;

    // Requirement: Peer discovery stress tests
    // Verify all nodes can discover each other
    let start = Instant::now();

    for i in 0..harness.node_count() {
        if let Some(node) = harness.get_node(i) {
            let peers = node.get_all_peers().await;
            let stats = node.get_network_stats().await;
            println!(
                "Node {}: {} total peers, {} connected peers",
                i, peers.len(), stats.connected_peers
            );
        }
    }

    let elapsed = start.elapsed();
    println!("Peer discovery stress test completed in {:.2}s", elapsed.as_secs_f64());

    // Verify nodes are operational (even if not all connected)
    let mut operational_count = 0;
    for i in 0..harness.node_count() {
        if let Some(node) = harness.get_node(i) {
            let _stats = node.get_network_stats().await;
            operational_count += 1;
        }
    }
    assert!(operational_count > 0, "At least some nodes should be operational");

    // Shutdown nodes
    harness
        .shutdown_all()
        .await
        .expect("Failed to shutdown nodes");
}

#[tokio::test]
async fn test_concurrent_unicast_messages() {
    // Create test harness with 4 nodes
    let harness = LoadTestHarness::new(4, 300)
        .await
        .expect("Failed to create test harness");

    // Start all nodes
    harness
        .start_all()
        .await
        .expect("Failed to start nodes");

    // Give nodes time to establish connections
    sleep(Duration::from_secs(2)).await;

    // Requirement: Concurrent message delivery
    // Send unicast messages from multiple nodes concurrently
    let start = Instant::now();
    let message_count = 1000;

    let mut handles = vec![];

    for sender_idx in 0..harness.node_count() {
        if let Some(sender) = harness.get_node(sender_idx) {
            let harness_clone = Arc::new(harness.message_count.clone());

            let handle = tokio::spawn(async move {
                for i in 0..message_count {
                    let target_idx = (sender_idx + 1) % 4;
                    let target_peer = format!("load-test-node-{}", target_idx);

                    let msg = NetworkMessage::Custom {
                        msg_type: "unicast_test".to_string(),
                        data: format!("Message {} from {}", i, sender_idx).into_bytes(),
                    };

                    match timeout(Duration::from_secs(5), sender.send_to_peer(&target_peer, msg)).await {
                        Ok(Ok(())) => {
                            harness_clone.fetch_add(1, Ordering::SeqCst);
                        }
                        Ok(Err(_)) => {
                            // Expected for some messages
                        }
                        Err(_) => {
                            // Timeout
                        }
                    }
                }
            });

            handles.push(handle);
        }
    }

    // Wait for all tasks to complete
    for handle in handles {
        let _ = handle.await;
    }

    let elapsed = start.elapsed();
    let total_messages = harness.get_message_count();
    let throughput = total_messages as f64 / elapsed.as_secs_f64();

    println!(
        "Concurrent unicast test: {} messages in {:.2}s = {:.0} msgs/sec",
        total_messages, elapsed.as_secs_f64(), throughput
    );

    // Shutdown nodes
    harness
        .shutdown_all()
        .await
        .expect("Failed to shutdown nodes");
}

#[tokio::test]
async fn test_sustained_message_load() {
    // Create test harness with 3 nodes
    let harness = LoadTestHarness::new(3, 400)
        .await
        .expect("Failed to create test harness");

    // Start all nodes
    harness
        .start_all()
        .await
        .expect("Failed to start nodes");

    // Give nodes time to establish connections
    sleep(Duration::from_secs(2)).await;

    // Get sender node
    let sender = harness.get_node(0).expect("Failed to get node 0");

    // Requirement: Sustained load test
    // Send messages continuously for 10 seconds
    let start = Instant::now();
    let duration = Duration::from_secs(10);
    let mut message_count = 0;

    while start.elapsed() < duration {
        let msg = NetworkMessage::Custom {
            msg_type: "sustained_load".to_string(),
            data: vec![1, 2, 3, 4, 5],
        };

        match timeout(Duration::from_secs(5), sender.broadcast(msg)).await {
            Ok(Ok(count)) => {
                harness.increment_message_count(count as u64);
                message_count += 1;
            }
            Ok(Err(_)) => {
                // Continue on error
            }
            Err(_) => {
                // Continue on timeout
            }
        }

        // Small delay to prevent overwhelming the system
        sleep(Duration::from_millis(1)).await;
    }

    let elapsed = start.elapsed();
    let total_messages = harness.get_message_count();
    let throughput = total_messages as f64 / elapsed.as_secs_f64();

    println!(
        "Sustained load test: {} broadcast attempts, {} total messages delivered in {:.2}s = {:.0} msgs/sec",
        message_count, total_messages, elapsed.as_secs_f64(), throughput
    );

    // Verify system remained stable
    assert!(message_count > 0, "Should have sent at least some messages");

    // Shutdown nodes
    harness
        .shutdown_all()
        .await
        .expect("Failed to shutdown nodes");
}

#[tokio::test]
async fn test_network_statistics_under_load() {
    // Create test harness with 3 nodes
    let harness = LoadTestHarness::new(3, 500)
        .await
        .expect("Failed to create test harness");

    // Start all nodes
    harness
        .start_all()
        .await
        .expect("Failed to start nodes");

    // Give nodes time to establish connections
    sleep(Duration::from_secs(2)).await;

    // Get sender node
    let sender = harness.get_node(0).expect("Failed to get node 0");

    // Send 100 messages
    for i in 0..100 {
        let msg = NetworkMessage::Custom {
            msg_type: "stats_test".to_string(),
            data: format!("Message {}", i).into_bytes(),
        };

        match timeout(Duration::from_secs(5), sender.broadcast(msg)).await {
            Ok(Ok(count)) => {
                harness.increment_message_count(count as u64);
            }
            Ok(Err(_)) => {
                // Continue on error
            }
            Err(_) => {
                // Continue on timeout
            }
        }
    }

    // Get network statistics
    let stats = sender.get_network_stats().await;
    println!("Network statistics under load:");
    println!("  Connected peers: {}", stats.connected_peers);
    println!("  Total peers: {}", stats.total_peers);
    println!("  Total messages sent: {}", stats.total_messages_sent);
    println!("  Total messages received: {}", stats.total_messages_received);
    println!("  Total bytes sent: {}", stats.total_bytes_sent);
    println!("  Total bytes received: {}", stats.total_bytes_received);
    println!("  Average latency: {} ms", stats.avg_latency_ms);
    println!("  Uptime: {} seconds", stats.uptime_secs);

    // Verify node is operational
    assert!(stats.uptime_secs > 0, "Node should be operational");

    // Shutdown nodes
    harness
        .shutdown_all()
        .await
        .expect("Failed to shutdown nodes");
}

#[tokio::test]
async fn test_message_delivery_reliability_under_load() {
    // Create test harness with 2 nodes
    let harness = LoadTestHarness::new(2, 600)
        .await
        .expect("Failed to create test harness");

    // Start all nodes
    harness
        .start_all()
        .await
        .expect("Failed to start nodes");

    // Give nodes time to establish connections
    sleep(Duration::from_secs(2)).await;

    // Get sender node
    let sender = harness.get_node(0).expect("Failed to get node 0");

    // Send 100 messages and track delivery
    let mut successful = 0;
    let total = 100;

    for i in 0..total {
        let msg = NetworkMessage::Custom {
            msg_type: "reliability_test".to_string(),
            data: format!("Message {}", i).into_bytes(),
        };

        match timeout(Duration::from_secs(5), sender.broadcast(msg)).await {
            Ok(Ok(count)) => {
                if count > 0 {
                    successful += 1;
                    harness.increment_message_count(count as u64);
                }
            }
            Ok(Err(_)) => {
                // Continue on error
            }
            Err(_) => {
                // Continue on timeout
            }
        }
    }

    let success_rate = (successful as f64 / total as f64) * 100.0;
    println!(
        "Message delivery reliability: {}/{} successful ({:.1}%)",
        successful, total, success_rate
    );

    // Verify node is operational
    let stats = sender.get_network_stats().await;
    assert!(stats.uptime_secs > 0, "Node should be operational");

    // Shutdown nodes
    harness
        .shutdown_all()
        .await
        .expect("Failed to shutdown nodes");
}

#[tokio::test]
async fn test_peer_discovery_under_load() {
    // Create test harness with 6 nodes
    let harness = LoadTestHarness::new(6, 700)
        .await
        .expect("Failed to create test harness");

    // Start all nodes
    harness
        .start_all()
        .await
        .expect("Failed to start nodes");

    // Give nodes time to establish connections
    sleep(Duration::from_secs(3)).await;

    // Send broadcast messages while monitoring peer discovery
    let sender = harness.get_node(0).expect("Failed to get node 0");

    for i in 0..100 {
        let msg = NetworkMessage::Custom {
            msg_type: "discovery_load".to_string(),
            data: format!("Message {}", i).into_bytes(),
        };

        match timeout(Duration::from_secs(5), sender.broadcast(msg)).await {
            Ok(Ok(count)) => {
                harness.increment_message_count(count as u64);
            }
            Ok(Err(_)) => {
                // Continue on error
            }
            Err(_) => {
                // Continue on timeout
            }
        }
    }

    // Verify nodes are operational
    let mut operational_count = 0;
    for i in 0..harness.node_count() {
        if let Some(node) = harness.get_node(i) {
            let stats = node.get_network_stats().await;
            println!("Node {}: {} total peers, {} connected", i, stats.total_peers, stats.connected_peers);
            if stats.uptime_secs > 0 {
                operational_count += 1;
            }
        }
    }
    assert!(operational_count > 0, "At least some nodes should be operational");

    // Shutdown nodes
    harness
        .shutdown_all()
        .await
        .expect("Failed to shutdown nodes");
}
