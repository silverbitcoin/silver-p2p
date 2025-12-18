//! Integration tests for message error handling

use silver_p2p::{
    MessageErrorHandler, MessageHandler, PeerManager, ConnectionPool,
    NetworkMessage, P2PError, NodeRole,
};
use std::sync::Arc;

#[tokio::test]
async fn test_malformed_message_detection_too_short() {
    let peer_manager = Arc::new(PeerManager::new(1000));
    let connection_pool = Arc::new(ConnectionPool::new(100, 300));
    let error_handler = MessageErrorHandler::new(peer_manager.clone(), connection_pool.clone());

    // Add peer
    peer_manager
        .add_peer("peer1".to_string(), "127.0.0.1:9000".to_string(), NodeRole::Validator)
        .await
        .unwrap();

    // Message too short for length prefix
    let malformed_data = vec![1, 2];
    let result = error_handler.validate_message_format("peer1", &malformed_data, 100 * 1024 * 1024).await;

    assert!(result.is_err());
    
    // Verify peer is marked unhealthy
    let peer = peer_manager.get_peer("peer1").await.unwrap();
    assert!(!peer.is_healthy);
    assert!(peer.last_error.is_some());
}

#[tokio::test]
async fn test_malformed_message_detection_exceeds_max_size() {
    let peer_manager = Arc::new(PeerManager::new(1000));
    let connection_pool = Arc::new(ConnectionPool::new(100, 300));
    let error_handler = MessageErrorHandler::new(peer_manager.clone(), connection_pool.clone());

    // Add peer
    peer_manager
        .add_peer("peer1".to_string(), "127.0.0.1:9000".to_string(), NodeRole::Validator)
        .await
        .unwrap();

    // Create message claiming to be 200 bytes but max is 100
    let mut data = vec![0u8; 10];
    let size = 200u32;
    data[0..4].copy_from_slice(&size.to_le_bytes());

    let result = error_handler.validate_message_format("peer1", &data, 100).await;

    assert!(result.is_err());
    
    // Verify peer is marked unhealthy
    let peer = peer_manager.get_peer("peer1").await.unwrap();
    assert!(!peer.is_healthy);
}

#[tokio::test]
async fn test_malformed_message_detection_incomplete_data() {
    let peer_manager = Arc::new(PeerManager::new(1000));
    let connection_pool = Arc::new(ConnectionPool::new(100, 300));
    let error_handler = MessageErrorHandler::new(peer_manager.clone(), connection_pool.clone());

    // Add peer
    peer_manager
        .add_peer("peer1".to_string(), "127.0.0.1:9000".to_string(), NodeRole::Validator)
        .await
        .unwrap();

    // Create message claiming 100 bytes but only provide 10
    let mut data = vec![0u8; 10];
    let size = 100u32;
    data[0..4].copy_from_slice(&size.to_le_bytes());

    let result = error_handler.validate_message_format("peer1", &data, 1000).await;

    assert!(result.is_err());
    
    // Verify peer is marked unhealthy
    let peer = peer_manager.get_peer("peer1").await.unwrap();
    assert!(!peer.is_healthy);
}

#[tokio::test]
async fn test_valid_message_format_passes() {
    let peer_manager = Arc::new(PeerManager::new(1000));
    let connection_pool = Arc::new(ConnectionPool::new(100, 300));
    let error_handler = MessageErrorHandler::new(peer_manager.clone(), connection_pool.clone());

    // Add peer
    peer_manager
        .add_peer("peer1".to_string(), "127.0.0.1:9000".to_string(), NodeRole::Validator)
        .await
        .unwrap();

    // Create valid message
    let mut data = vec![0u8; 10];
    let size = 6u32;
    data[0..4].copy_from_slice(&size.to_le_bytes());

    let result = error_handler.validate_message_format("peer1", &data, 1000).await;

    assert!(result.is_ok());
    assert_eq!(result.unwrap(), 6);
    
    // Verify peer is still healthy
    let peer = peer_manager.get_peer("peer1").await.unwrap();
    assert!(peer.is_healthy);
}

#[tokio::test]
async fn test_parsing_error_disconnects_peer() {
    let peer_manager = Arc::new(PeerManager::new(1000));
    let connection_pool = Arc::new(ConnectionPool::new(100, 300));
    let error_handler = MessageErrorHandler::new(peer_manager.clone(), connection_pool.clone());

    // Add peer
    peer_manager
        .add_peer("peer1".to_string(), "127.0.0.1:9000".to_string(), NodeRole::Validator)
        .await
        .unwrap();

    let error = P2PError::InvalidMessageFormat("test error".to_string());
    error_handler.handle_parsing_error("peer1", &error).await.unwrap();

    // Verify peer is marked unhealthy
    let peer = peer_manager.get_peer("peer1").await.unwrap();
    assert!(!peer.is_healthy);
    assert!(peer.last_error.is_some());
}

#[tokio::test]
async fn test_deserialization_error_disconnects_peer() {
    let peer_manager = Arc::new(PeerManager::new(1000));
    let connection_pool = Arc::new(ConnectionPool::new(100, 300));
    let error_handler = MessageErrorHandler::new(peer_manager.clone(), connection_pool.clone());

    // Add peer
    peer_manager
        .add_peer("peer1".to_string(), "127.0.0.1:9000".to_string(), NodeRole::Validator)
        .await
        .unwrap();

    let error = P2PError::DeserializationError("test error".to_string());
    error_handler.handle_deserialization_error("peer1", &error).await.unwrap();

    // Verify peer is marked unhealthy
    let peer = peer_manager.get_peer("peer1").await.unwrap();
    assert!(!peer.is_healthy);
}

#[tokio::test]
async fn test_malformed_message_error_disconnects_peer() {
    let peer_manager = Arc::new(PeerManager::new(1000));
    let connection_pool = Arc::new(ConnectionPool::new(100, 300));
    let error_handler = MessageErrorHandler::new(peer_manager.clone(), connection_pool.clone());

    // Add peer
    peer_manager
        .add_peer("peer1".to_string(), "127.0.0.1:9000".to_string(), NodeRole::Validator)
        .await
        .unwrap();

    error_handler
        .handle_malformed_message("peer1", "Invalid data structure")
        .await
        .unwrap();

    // Verify peer is marked unhealthy
    let peer = peer_manager.get_peer("peer1").await.unwrap();
    assert!(!peer.is_healthy);
}

#[tokio::test]
async fn test_format_error_disconnects_peer() {
    let peer_manager = Arc::new(PeerManager::new(1000));
    let connection_pool = Arc::new(ConnectionPool::new(100, 300));
    let error_handler = MessageErrorHandler::new(peer_manager.clone(), connection_pool.clone());

    // Add peer
    peer_manager
        .add_peer("peer1".to_string(), "127.0.0.1:9000".to_string(), NodeRole::Validator)
        .await
        .unwrap();

    let error = P2PError::InvalidMessageFormat("test error".to_string());
    error_handler.handle_format_error("peer1", &error).await.unwrap();

    // Verify peer is marked unhealthy
    let peer = peer_manager.get_peer("peer1").await.unwrap();
    assert!(!peer.is_healthy);
}

#[tokio::test]
async fn test_error_statistics_tracking() {
    let peer_manager = Arc::new(PeerManager::new(1000));
    let connection_pool = Arc::new(ConnectionPool::new(100, 300));
    let error_handler = MessageErrorHandler::new(peer_manager.clone(), connection_pool.clone());

    // Add peer
    peer_manager
        .add_peer("peer1".to_string(), "127.0.0.1:9000".to_string(), NodeRole::Validator)
        .await
        .unwrap();

    // Generate various errors
    let error1 = P2PError::InvalidMessageFormat("error1".to_string());
    error_handler.handle_parsing_error("peer1", &error1).await.unwrap();

    let error2 = P2PError::DeserializationError("error2".to_string());
    error_handler.handle_deserialization_error("peer1", &error2).await.unwrap();

    error_handler
        .handle_malformed_message("peer1", "error3")
        .await
        .unwrap();

    let (parsing, malformed, deser, _format, disconnected) = error_handler.get_stats().await;
    
    assert_eq!(parsing, 1);
    assert_eq!(malformed, 1);
    assert_eq!(deser, 1);
    assert_eq!(disconnected, 3);
}

#[tokio::test]
async fn test_message_handler_with_error_handling() {
    let peer_manager = Arc::new(PeerManager::new(1000));
    let connection_pool = Arc::new(ConnectionPool::new(100, 300));
    let message_handler = MessageHandler::new_with_error_handler(
        100 * 1024 * 1024,
        peer_manager.clone(),
        connection_pool.clone(),
    );

    // Add peer
    peer_manager
        .add_peer("peer1".to_string(), "127.0.0.1:9000".to_string(), NodeRole::Validator)
        .await
        .unwrap();

    // Test valid message
    let msg = NetworkMessage::Ping { nonce: 123 };
    let serialized = message_handler.serialize(&msg).await.unwrap();
    
    let deserialized = message_handler
        .deserialize_with_error_handling("peer1", &serialized)
        .await
        .unwrap();
    
    assert_eq!(deserialized, msg);
    
    // Verify peer is still healthy
    let peer = peer_manager.get_peer("peer1").await.unwrap();
    assert!(peer.is_healthy);
}

#[tokio::test]
async fn test_message_handler_rejects_malformed_data() {
    let peer_manager = Arc::new(PeerManager::new(1000));
    let connection_pool = Arc::new(ConnectionPool::new(100, 300));
    let message_handler = MessageHandler::new_with_error_handler(
        100 * 1024 * 1024,
        peer_manager.clone(),
        connection_pool.clone(),
    );

    // Add peer
    peer_manager
        .add_peer("peer1".to_string(), "127.0.0.1:9000".to_string(), NodeRole::Validator)
        .await
        .unwrap();

    // Test malformed message (too short)
    let malformed_data = vec![1, 2];
    let result = message_handler
        .deserialize_with_error_handling("peer1", &malformed_data)
        .await;

    assert!(result.is_err());
    
    // Verify peer is marked unhealthy
    let peer = peer_manager.get_peer("peer1").await.unwrap();
    assert!(!peer.is_healthy);
}

#[tokio::test]
async fn test_message_handler_rejects_oversized_message() {
    let peer_manager = Arc::new(PeerManager::new(1000));
    let connection_pool = Arc::new(ConnectionPool::new(100, 300));
    let message_handler = MessageHandler::new_with_error_handler(
        100, // Small max size
        peer_manager.clone(),
        connection_pool.clone(),
    );

    // Add peer
    peer_manager
        .add_peer("peer1".to_string(), "127.0.0.1:9000".to_string(), NodeRole::Validator)
        .await
        .unwrap();

    // Create message claiming to be 200 bytes
    let mut data = vec![0u8; 10];
    let size = 200u32;
    data[0..4].copy_from_slice(&size.to_le_bytes());

    let result = message_handler
        .deserialize_with_error_handling("peer1", &data)
        .await;

    assert!(result.is_err());
    
    // Verify peer is marked unhealthy
    let peer = peer_manager.get_peer("peer1").await.unwrap();
    assert!(!peer.is_healthy);
}

#[tokio::test]
async fn test_validate_format_with_error_handling() {
    let peer_manager = Arc::new(PeerManager::new(1000));
    let connection_pool = Arc::new(ConnectionPool::new(100, 300));
    let message_handler = MessageHandler::new_with_error_handler(
        100 * 1024 * 1024,
        peer_manager.clone(),
        connection_pool.clone(),
    );

    // Add peer
    peer_manager
        .add_peer("peer1".to_string(), "127.0.0.1:9000".to_string(), NodeRole::Validator)
        .await
        .unwrap();

    // Test valid format
    let mut data = vec![0u8; 10];
    let size = 6u32;
    data[0..4].copy_from_slice(&size.to_le_bytes());

    let result = message_handler
        .validate_format_with_error_handling("peer1", &data)
        .await;

    assert!(result.is_ok());
    assert_eq!(result.unwrap(), 6);
    
    // Verify peer is still healthy
    let peer = peer_manager.get_peer("peer1").await.unwrap();
    assert!(peer.is_healthy);
}

#[tokio::test]
async fn test_multiple_peers_error_isolation() {
    let peer_manager = Arc::new(PeerManager::new(1000));
    let connection_pool = Arc::new(ConnectionPool::new(100, 300));
    let error_handler = MessageErrorHandler::new(peer_manager.clone(), connection_pool.clone());

    // Add multiple peers
    peer_manager
        .add_peer("peer1".to_string(), "127.0.0.1:9000".to_string(), NodeRole::Validator)
        .await
        .unwrap();
    peer_manager
        .add_peer("peer2".to_string(), "127.0.0.1:9001".to_string(), NodeRole::Validator)
        .await
        .unwrap();

    // Generate error for peer1
    let error = P2PError::InvalidMessageFormat("test error".to_string());
    error_handler.handle_parsing_error("peer1", &error).await.unwrap();

    // Verify peer1 is unhealthy
    let peer1 = peer_manager.get_peer("peer1").await.unwrap();
    assert!(!peer1.is_healthy);

    // Verify peer2 is still healthy
    let peer2 = peer_manager.get_peer("peer2").await.unwrap();
    assert!(peer2.is_healthy);
}

#[tokio::test]
async fn test_error_statistics_reset() {
    let peer_manager = Arc::new(PeerManager::new(1000));
    let connection_pool = Arc::new(ConnectionPool::new(100, 300));
    let error_handler = MessageErrorHandler::new(peer_manager.clone(), connection_pool.clone());

    // Add peer
    peer_manager
        .add_peer("peer1".to_string(), "127.0.0.1:9000".to_string(), NodeRole::Validator)
        .await
        .unwrap();

    // Generate errors
    let error = P2PError::InvalidMessageFormat("test error".to_string());
    error_handler.handle_parsing_error("peer1", &error).await.unwrap();

    let (parsing, _, _, _, _) = error_handler.get_stats().await;
    assert_eq!(parsing, 1);

    // Reset stats
    error_handler.reset_stats().await;

    let (parsing, malformed, deser, format, disconnected) = error_handler.get_stats().await;
    assert_eq!(parsing, 0);
    assert_eq!(malformed, 0);
    assert_eq!(deser, 0);
    assert_eq!(format, 0);
    assert_eq!(disconnected, 0);
}

#[tokio::test]
async fn test_message_validation_after_deserialization() {
    let peer_manager = Arc::new(PeerManager::new(1000));
    let connection_pool = Arc::new(ConnectionPool::new(100, 300));
    let error_handler = MessageErrorHandler::new(peer_manager.clone(), connection_pool.clone());

    // Add peer
    peer_manager
        .add_peer("peer1".to_string(), "127.0.0.1:9000".to_string(), NodeRole::Validator)
        .await
        .unwrap();

    // Test valid message validation
    let msg = NetworkMessage::Ping { nonce: 123 };
    let result = error_handler.validate_message("peer1", &msg).await;

    assert!(result.is_ok());
    
    // Verify peer is still healthy
    let peer = peer_manager.get_peer("peer1").await.unwrap();
    assert!(peer.is_healthy);
}

#[tokio::test]
async fn test_concurrent_error_handling() {
    let peer_manager = Arc::new(PeerManager::new(1000));
    let connection_pool = Arc::new(ConnectionPool::new(100, 300));
    let error_handler = Arc::new(MessageErrorHandler::new(peer_manager.clone(), connection_pool.clone()));

    // Add multiple peers
    for i in 0..10 {
        let peer_id = format!("peer{}", i);
        let addr = format!("127.0.0.1:{}", 9000 + i);
        peer_manager
            .add_peer(peer_id, addr, NodeRole::Validator)
            .await
            .unwrap();
    }

    // Generate errors concurrently
    let mut handles = vec![];
    for i in 0..10 {
        let error_handler = error_handler.clone();
        let handle = tokio::spawn(async move {
            let peer_id = format!("peer{}", i);
            let error = P2PError::InvalidMessageFormat(format!("error{}", i));
            error_handler.handle_parsing_error(&peer_id, &error).await.unwrap();
        });
        handles.push(handle);
    }

    // Wait for all tasks
    for handle in handles {
        handle.await.unwrap();
    }

    // Verify all peers are unhealthy
    for i in 0..10 {
        let peer_id = format!("peer{}", i);
        let peer = peer_manager.get_peer(&peer_id).await.unwrap();
        assert!(!peer.is_healthy);
    }

    let (parsing, _, _, _, disconnected) = error_handler.get_stats().await;
    assert_eq!(parsing, 10);
    assert_eq!(disconnected, 10);
}
