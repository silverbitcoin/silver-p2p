pub mod bootstrap_connector;
pub mod broadcast;
pub mod config;
pub mod connection_error_recovery;
pub mod connection_pool;
pub mod error;
pub mod event_loop;
pub mod handshake;
pub mod health_monitor;
pub mod message_chunking;
pub mod message_error_handler;
pub mod message_handler;
pub mod network_manager;
pub mod peer_discovery_coordinator;
pub mod peer_discovery_loop;
pub mod peer_manager;
pub mod rate_limiter;
pub mod reconnection_manager;
pub mod shutdown_coordination;
pub mod tcp_listener;
pub mod types;
pub mod unicast;

pub use bootstrap_connector::{BootstrapConnector, BootstrapConnectionStats, BootstrapNodeAttempt};
pub use broadcast::{BroadcastManager, BroadcastResult, BroadcastStats};
pub use config::NetworkConfig;
pub use connection_error_recovery::{
    ConnectionErrorRecovery, ConnectionErrorType, ConnectionErrorEvent, ErrorStatistics,
};
pub use connection_pool::ConnectionPool;
pub use error::{P2PError, Result};
pub use event_loop::NetworkEventLoop;
pub use health_monitor::HealthMonitor;
pub use message_chunking::{MessageChunker, ChunkedMessage, ChunkMetadata};
pub use message_error_handler::MessageErrorHandler;
pub use message_handler::{MessageHandler, MessageHandlerFn};
pub use network_manager::P2PNetworkManager;
pub use peer_discovery_coordinator::{PeerDiscoveryCoordinator, PeerDiscoveryStats, CandidateConnectionAttempt};
pub use peer_discovery_loop::{PeerDiscoveryLoop, PeerDiscoveryLoopStats};
pub use peer_manager::PeerManager;
pub use rate_limiter::{ConnectionLimiter, GlobalRateLimiter, TokenBucket};
pub use shutdown_coordination::{ShutdownCoordinator, ShutdownStats};
pub use types::{
    BackoffState, HealthStatus, NetworkMessage, NetworkStats, NodeRole,
    PeerInfo, PeerState,
};
pub use unicast::{UnicastManager, UnicastBatchResult, UnicastStats};
