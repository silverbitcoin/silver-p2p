//! Integration tests for connection error recovery

use silver_p2p::{
    ConnectionErrorRecovery, ConnectionErrorType, PeerManager, ConnectionPool,
    NodeRole,
};
use std::sync::Arc;
use std::net::SocketAddr;

#[tokio::test]
async fn test_connection_error_recovery_full_workflow() {
    // Setup
    let peer_manager = Arc::new(PeerManager::new(1000));
    let connection_pool = Arc::new(ConnectionPool::new(100, 300));

    // Add peers
    peer_manager
        .add_peer(
            "peer1".to_string(),
            "127.0.0.1:9000".to_string(),
            NodeRole::Validator,
        )
        .await
        .unwrap();

    peer_manager
        .add_peer(
            "peer2".to_string(),
            "127.0.0.1:9001".to_string(),
            NodeRole::Validator,
        )
        .await
        .unwrap();

    // Create recovery handler
    let recovery = ConnectionErrorRecovery::new(peer_manager.clone(), connection_pool, 300, 3);

    // Simulate connection errors
    let addr1: SocketAddr = "127.0.0.1:9000".parse().unwrap();
    let addr2: SocketAddr = "127.0.0.1:9001".parse().unwrap();

    // First peer: connection refused
    recovery
        .handle_connection_error(
            "peer1".to_string(),
            addr1,
            ConnectionErrorType::ConnectionRefused,
            "Connection refused".to_string(),
        )
        .await
        .unwrap();

    // Second peer: connection timeout
    recovery
        .handle_connection_error(
            "peer2".to_string(),
            addr2,
            ConnectionErrorType::ConnectionTimeout,
            "Connection timeout".to_string(),
        )
        .await
        .unwrap();

    // Verify error tracking
    assert_eq!(recovery.get_error_count("peer1").await, 1);
    assert_eq!(recovery.get_error_count("peer2").await, 1);

    // Verify peers marked unhealthy
    let peer1 = peer_manager.get_peer("peer1").await.unwrap();
    assert!(!peer1.is_healthy);

    let peer2 = peer_manager.get_peer("peer2").await.unwrap();
    assert!(!peer2.is_healthy);

    // Verify error statistics
    let stats = recovery.get_error_statistics().await;
    assert_eq!(stats.total_errors, 2);
    assert_eq!(stats.peers_with_errors, 2);

    // Verify peers with errors
    let peers_with_errors = recovery.get_peers_with_errors().await;
    assert_eq!(peers_with_errors.len(), 2);

    // Reset error for peer1
    recovery.reset_error_count("peer1").await.unwrap();
    assert_eq!(recovery.get_error_count("peer1").await, 0);
    assert_eq!(recovery.get_error_count("peer2").await, 1);

    // Clear all errors
    recovery.clear_all().await.unwrap();
    assert_eq!(recovery.get_error_count("peer1").await, 0);
    assert_eq!(recovery.get_error_count("peer2").await, 0);
}

#[tokio::test]
async fn test_fatal_vs_recoverable_errors() {
    let peer_manager = Arc::new(PeerManager::new(1000));
    let connection_pool = Arc::new(ConnectionPool::new(100, 300));

    peer_manager
        .add_peer(
            "peer1".to_string(),
            "127.0.0.1:9000".to_string(),
            NodeRole::Validator,
        )
        .await
        .unwrap();

    peer_manager
        .add_peer(
            "peer2".to_string(),
            "127.0.0.1:9001".to_string(),
            NodeRole::Validator,
        )
        .await
        .unwrap();

    let recovery = ConnectionErrorRecovery::new(peer_manager.clone(), connection_pool, 300, 5);

    let addr1: SocketAddr = "127.0.0.1:9000".parse().unwrap();
    let addr2: SocketAddr = "127.0.0.1:9001".parse().unwrap();

    // Recoverable error
    recovery
        .handle_connection_error(
            "peer1".to_string(),
            addr1,
            ConnectionErrorType::ConnectionRefused,
            "Connection refused".to_string(),
        )
        .await
        .unwrap();

    // Fatal error
    recovery
        .handle_connection_error(
            "peer2".to_string(),
            addr2,
            ConnectionErrorType::InvalidHandshake,
            "Invalid handshake".to_string(),
        )
        .await
        .unwrap();

    // Recoverable error should increment count
    assert_eq!(recovery.get_error_count("peer1").await, 1);

    // Fatal error should NOT increment count (removed immediately)
    assert_eq!(recovery.get_error_count("peer2").await, 0);

    // Both should be marked unhealthy
    let peer1 = peer_manager.get_peer("peer1").await.unwrap();
    assert!(!peer1.is_healthy);

    let peer2 = peer_manager.get_peer("peer2").await.unwrap();
    assert!(!peer2.is_healthy);
}

#[tokio::test]
async fn test_consecutive_errors_threshold() {
    let peer_manager = Arc::new(PeerManager::new(1000));
    let connection_pool = Arc::new(ConnectionPool::new(100, 300));

    peer_manager
        .add_peer(
            "peer1".to_string(),
            "127.0.0.1:9000".to_string(),
            NodeRole::Validator,
        )
        .await
        .unwrap();

    let recovery = ConnectionErrorRecovery::new(peer_manager.clone(), connection_pool, 300, 3);

    let addr: SocketAddr = "127.0.0.1:9000".parse().unwrap();

    // First error
    recovery
        .handle_connection_error(
            "peer1".to_string(),
            addr,
            ConnectionErrorType::ConnectionRefused,
            "Connection refused".to_string(),
        )
        .await
        .unwrap();
    assert!(recovery.should_retry_peer("peer1").await);

    // Second error
    recovery
        .handle_connection_error(
            "peer1".to_string(),
            addr,
            ConnectionErrorType::ConnectionRefused,
            "Connection refused".to_string(),
        )
        .await
        .unwrap();
    assert!(recovery.should_retry_peer("peer1").await);

    // Third error (at threshold)
    recovery
        .handle_connection_error(
            "peer1".to_string(),
            addr,
            ConnectionErrorType::ConnectionRefused,
            "Connection refused".to_string(),
        )
        .await
        .unwrap();
    assert!(!recovery.should_retry_peer("peer1").await);

    // Verify error count
    assert_eq!(recovery.get_error_count("peer1").await, 3);
}

#[tokio::test]
async fn test_error_type_classification() {
    // Test recoverable errors
    assert!(ConnectionErrorType::ConnectionRefused.is_recoverable());
    assert!(ConnectionErrorType::ConnectionTimeout.is_recoverable());
    assert!(ConnectionErrorType::ConnectionReset.is_recoverable());
    assert!(ConnectionErrorType::NetworkUnreachable.is_recoverable());
    assert!(ConnectionErrorType::HostUnreachable.is_recoverable());

    // Test fatal errors
    assert!(ConnectionErrorType::PermissionDenied.is_fatal());
    assert!(ConnectionErrorType::InvalidHandshake.is_fatal());
    assert!(ConnectionErrorType::IncompatibleVersion.is_fatal());

    // Test descriptions
    assert!(!ConnectionErrorType::ConnectionRefused.description().is_empty());
    assert!(!ConnectionErrorType::ConnectionTimeout.description().is_empty());
    assert!(!ConnectionErrorType::InvalidHandshake.description().is_empty());
}

#[tokio::test]
async fn test_last_error_tracking() {
    let peer_manager = Arc::new(PeerManager::new(1000));
    let connection_pool = Arc::new(ConnectionPool::new(100, 300));

    peer_manager
        .add_peer(
            "peer1".to_string(),
            "127.0.0.1:9000".to_string(),
            NodeRole::Validator,
        )
        .await
        .unwrap();

    let recovery = ConnectionErrorRecovery::new(peer_manager, connection_pool, 300, 5);

    let addr: SocketAddr = "127.0.0.1:9000".parse().unwrap();

    // No error initially
    assert!(recovery.get_last_error("peer1").await.is_none());

    // First error
    recovery
        .handle_connection_error(
            "peer1".to_string(),
            addr,
            ConnectionErrorType::ConnectionRefused,
            "Connection refused".to_string(),
        )
        .await
        .unwrap();

    let last_error = recovery.get_last_error("peer1").await;
    assert!(last_error.is_some());
    let (error_type, _) = last_error.unwrap();
    assert_eq!(error_type, ConnectionErrorType::ConnectionRefused);

    // Second error with different type
    recovery
        .handle_connection_error(
            "peer1".to_string(),
            addr,
            ConnectionErrorType::ConnectionTimeout,
            "Connection timeout".to_string(),
        )
        .await
        .unwrap();

    let last_error = recovery.get_last_error("peer1").await;
    assert!(last_error.is_some());
    let (error_type, _) = last_error.unwrap();
    assert_eq!(error_type, ConnectionErrorType::ConnectionTimeout);
}

#[tokio::test]
async fn test_multiple_peers_error_tracking() {
    let peer_manager = Arc::new(PeerManager::new(1000));
    let connection_pool = Arc::new(ConnectionPool::new(100, 300));

    // Add multiple peers
    for i in 0..5 {
        peer_manager
            .add_peer(
                format!("peer{}", i),
                format!("127.0.0.1:{}", 9000 + i),
                NodeRole::Validator,
            )
            .await
            .unwrap();
    }

    let recovery = ConnectionErrorRecovery::new(peer_manager, connection_pool, 300, 5);

    // Simulate errors for different peers
    for i in 0..5 {
        let addr: SocketAddr = format!("127.0.0.1:{}", 9000 + i).parse().unwrap();
        recovery
            .handle_connection_error(
                format!("peer{}", i),
                addr,
                ConnectionErrorType::ConnectionRefused,
                "Connection refused".to_string(),
            )
            .await
            .unwrap();
    }

    // Verify all peers have errors
    let peers_with_errors = recovery.get_peers_with_errors().await;
    assert_eq!(peers_with_errors.len(), 5);

    // Verify statistics
    let stats = recovery.get_error_statistics().await;
    assert_eq!(stats.total_errors, 5);
    assert_eq!(stats.peers_with_errors, 5);

    // Clear errors for some peers
    recovery.clear_error_state("peer0").await.unwrap();
    recovery.clear_error_state("peer1").await.unwrap();

    // Verify remaining errors
    let peers_with_errors = recovery.get_peers_with_errors().await;
    assert_eq!(peers_with_errors.len(), 3);

    let stats = recovery.get_error_statistics().await;
    assert_eq!(stats.total_errors, 3);
    assert_eq!(stats.peers_with_errors, 3);
}

#[tokio::test]
async fn test_error_recovery_with_peer_manager_integration() {
    let peer_manager = Arc::new(PeerManager::new(1000));
    let connection_pool = Arc::new(ConnectionPool::new(100, 300));

    peer_manager
        .add_peer(
            "peer1".to_string(),
            "127.0.0.1:9000".to_string(),
            NodeRole::Validator,
        )
        .await
        .unwrap();

    let recovery = ConnectionErrorRecovery::new(peer_manager.clone(), connection_pool, 300, 3);

    let addr: SocketAddr = "127.0.0.1:9000".parse().unwrap();

    // Verify peer is initially healthy
    let peer = peer_manager.get_peer("peer1").await.unwrap();
    assert!(peer.is_healthy);

    // Handle connection error
    recovery
        .handle_connection_error(
            "peer1".to_string(),
            addr,
            ConnectionErrorType::ConnectionRefused,
            "Connection refused".to_string(),
        )
        .await
        .unwrap();

    // Verify peer is now unhealthy
    let peer = peer_manager.get_peer("peer1").await.unwrap();
    assert!(!peer.is_healthy);
    assert!(peer.last_error.is_some());

    // Verify error message is recorded
    let error_msg = peer.last_error.unwrap();
    assert!(error_msg.contains("Connection refused"));
}

#[tokio::test]
async fn test_reconnection_manager_integration() {
    let peer_manager = Arc::new(PeerManager::new(1000));
    let connection_pool = Arc::new(ConnectionPool::new(100, 300));

    peer_manager
        .add_peer(
            "peer1".to_string(),
            "127.0.0.1:9000".to_string(),
            NodeRole::Validator,
        )
        .await
        .unwrap();

    let recovery = ConnectionErrorRecovery::new(peer_manager, connection_pool, 300, 5);

    let addr: SocketAddr = "127.0.0.1:9000".parse().unwrap();

    // Handle connection error
    recovery
        .handle_connection_error(
            "peer1".to_string(),
            addr,
            ConnectionErrorType::ConnectionRefused,
            "Connection refused".to_string(),
        )
        .await
        .unwrap();

    // Get reconnection manager
    let reconnection_mgr = recovery.reconnection_manager();

    // Verify backoff state was created
    let backoff = reconnection_mgr.get_backoff_state("peer1").await;
    assert!(backoff.is_some());

    // Verify backoff delay (should be 2 after first error)
    let backoff = backoff.unwrap();
    assert_eq!(backoff.current_delay_secs, 2);
    assert_eq!(backoff.failed_attempts, 1);
}

#[tokio::test]
async fn test_error_statistics_comprehensive() {
    let peer_manager = Arc::new(PeerManager::new(1000));
    let connection_pool = Arc::new(ConnectionPool::new(100, 300));

    // Add peers
    for i in 0..3 {
        peer_manager
            .add_peer(
                format!("peer{}", i),
                format!("127.0.0.1:{}", 9000 + i),
                NodeRole::Validator,
            )
            .await
            .unwrap();
    }

    let recovery = ConnectionErrorRecovery::new(peer_manager, connection_pool, 300, 5);

    // Simulate different error patterns
    let addr0: SocketAddr = "127.0.0.1:9000".parse().unwrap();
    let addr1: SocketAddr = "127.0.0.1:9001".parse().unwrap();
    let addr2: SocketAddr = "127.0.0.1:9002".parse().unwrap();

    // Peer 0: 1 error
    recovery
        .handle_connection_error(
            "peer0".to_string(),
            addr0,
            ConnectionErrorType::ConnectionRefused,
            "Connection refused".to_string(),
        )
        .await
        .unwrap();

    // Peer 1: 2 errors
    recovery
        .handle_connection_error(
            "peer1".to_string(),
            addr1,
            ConnectionErrorType::ConnectionTimeout,
            "Connection timeout".to_string(),
        )
        .await
        .unwrap();
    recovery
        .handle_connection_error(
            "peer1".to_string(),
            addr1,
            ConnectionErrorType::ConnectionTimeout,
            "Connection timeout".to_string(),
        )
        .await
        .unwrap();

    // Peer 2: 3 errors
    recovery
        .handle_connection_error(
            "peer2".to_string(),
            addr2,
            ConnectionErrorType::ConnectionReset,
            "Connection reset".to_string(),
        )
        .await
        .unwrap();
    recovery
        .handle_connection_error(
            "peer2".to_string(),
            addr2,
            ConnectionErrorType::ConnectionReset,
            "Connection reset".to_string(),
        )
        .await
        .unwrap();
    recovery
        .handle_connection_error(
            "peer2".to_string(),
            addr2,
            ConnectionErrorType::ConnectionReset,
            "Connection reset".to_string(),
        )
        .await
        .unwrap();

    // Verify statistics
    let stats = recovery.get_error_statistics().await;
    assert_eq!(stats.total_errors, 6);
    assert_eq!(stats.peers_with_errors, 3);
    assert_eq!(stats.max_consecutive_errors, 5);

    // Verify individual error counts
    assert_eq!(recovery.get_error_count("peer0").await, 1);
    assert_eq!(recovery.get_error_count("peer1").await, 2);
    assert_eq!(recovery.get_error_count("peer2").await, 3);
}
