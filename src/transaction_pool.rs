//! Transaction Pool - Network Integration
//!
//! Manages the mempool of unconfirmed transactions with proper ordering,
//! fee calculation, and eviction policies.
//!
//! # Features
//! - Transaction acceptance and validation
//! - Transaction ordering by fee
//! - Fee calculation
//! - Mempool management
//! - Eviction policy
//! - Error handling

use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::fmt;
use tracing::{debug, error, info, warn};

/// Transaction pool error type
#[derive(Debug, Clone)]
pub struct PoolError {
    /// Error code
    pub code: u32,
    /// Error message
    pub message: String,
}

impl fmt::Display for PoolError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Pool Error {}: {}", self.code, self.message)
    }
}

impl std::error::Error for PoolError {}

/// Result type for pool operations
pub type PoolResult<T> = std::result::Result<T, PoolError>;

/// Transaction pool entry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PoolEntry {
    /// Transaction ID
    pub txid: String,
    /// Transaction size in bytes
    pub size: u64,
    /// Transaction fee in MIST
    pub fee: u128,
    /// Fee rate in MIST per byte
    pub fee_rate: f64,
    /// Timestamp when added to pool
    pub timestamp: u64,
    /// Number of confirmations
    pub confirmations: u32,
    /// Depends on transactions (parent TXIDs)
    pub depends_on: Vec<String>,
    /// Depended on by transactions (child TXIDs)
    pub depended_by: Vec<String>,
}

impl PoolEntry {
    /// Create a new pool entry
    pub fn new(txid: String, size: u64, fee: u128) -> Self {
        let fee_rate = if size > 0 {
            (fee as f64) / (size as f64)
        } else {
            0.0
        };

        Self {
            txid,
            size,
            fee,
            fee_rate,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            confirmations: 0,
            depends_on: Vec::new(),
            depended_by: Vec::new(),
        }
    }
}

/// Transaction pool (mempool)
pub struct TransactionPool {
    /// Pool entries by TXID
    entries: HashMap<String, PoolEntry>,
    /// Entries sorted by fee rate (descending)
    by_fee_rate: BTreeMap<(u64, String), String>, // (fee_rate_scaled, txid) -> txid
    /// Maximum pool size in bytes
    max_size: u64,
    /// Current pool size in bytes
    current_size: u64,
    /// Minimum fee rate in MIST per byte
    min_fee_rate: f64,
    /// Maximum pool entries
    max_entries: usize,
}

impl TransactionPool {
    /// Create a new transaction pool
    pub fn new(max_size: u64, min_fee_rate: f64, max_entries: usize) -> Self {
        Self {
            entries: HashMap::new(),
            by_fee_rate: BTreeMap::new(),
            max_size,
            current_size: 0,
            min_fee_rate,
            max_entries,
        }
    }

    /// Add transaction to pool
    pub fn add_transaction(&mut self, entry: PoolEntry) -> PoolResult<()> {
        debug!("Adding transaction to pool: {}", entry.txid);

        // Validate transaction
        self.validate_transaction(&entry)?;

        // Check if transaction already exists
        if self.entries.contains_key(&entry.txid) {
            error!("Transaction already in pool: {}", entry.txid);
            return Err(PoolError {
                code: 6001,
                message: "Transaction already in pool".to_string(),
            });
        }

        // Check pool size
        if self.current_size + entry.size > self.max_size {
            warn!("Pool size limit reached, evicting low-fee transactions");
            self.evict_low_fee_transactions(entry.size)?;
        }

        // Check pool entry count
        if self.entries.len() >= self.max_entries {
            warn!("Pool entry limit reached, evicting low-fee transactions");
            self.evict_low_fee_transactions(entry.size)?;
        }

        // Add to pool
        let fee_rate_scaled = (entry.fee_rate * 1_000_000.0) as u64;
        self.by_fee_rate
            .insert((fee_rate_scaled, entry.txid.clone()), entry.txid.clone());
        self.entries.insert(entry.txid.clone(), entry.clone());
        self.current_size += entry.size;

        info!(
            "Transaction added to pool: {} (fee_rate: {:.2} MIST/byte)",
            entry.txid, entry.fee_rate
        );

        Ok(())
    }

    /// Remove transaction from pool
    pub fn remove_transaction(&mut self, txid: &str) -> PoolResult<()> {
        debug!("Removing transaction from pool: {}", txid);

        if let Some(entry) = self.entries.remove(txid) {
            let fee_rate_scaled = (entry.fee_rate * 1_000_000.0) as u64;
            self.by_fee_rate
                .remove(&(fee_rate_scaled, entry.txid.clone()));
            self.current_size = self.current_size.saturating_sub(entry.size);

            info!("Transaction removed from pool: {}", txid);
            Ok(())
        } else {
            error!("Transaction not found in pool: {}", txid);
            Err(PoolError {
                code: 6002,
                message: "Transaction not found in pool".to_string(),
            })
        }
    }

    /// Get transaction from pool
    pub fn get_transaction(&self, txid: &str) -> PoolResult<PoolEntry> {
        self.entries.get(txid).cloned().ok_or_else(|| PoolError {
            code: 6003,
            message: "Transaction not found in pool".to_string(),
        })
    }

    /// Get all transactions sorted by fee rate (highest first)
    pub fn get_transactions_by_fee_rate(&self) -> Vec<PoolEntry> {
        self.by_fee_rate
            .iter()
            .rev()
            .filter_map(|(_, txid)| self.entries.get(txid).cloned())
            .collect()
    }

    /// Get transactions for block (up to max_size)
    pub fn get_transactions_for_block(&self, max_size: u64) -> Vec<PoolEntry> {
        debug!("Getting transactions for block (max_size: {})", max_size);

        let mut block_txs = Vec::new();
        let mut block_size = 0;

        for entry in self.get_transactions_by_fee_rate() {
            if block_size + entry.size <= max_size {
                block_txs.push(entry.clone());
                block_size += entry.size;
            } else {
                break;
            }
        }

        info!("Selected {} transactions for block", block_txs.len());
        block_txs
    }

    /// Validate transaction
    fn validate_transaction(&self, entry: &PoolEntry) -> PoolResult<()> {
        // Validate TXID format
        if entry.txid.len() != 128 || !entry.txid.chars().all(|c| c.is_ascii_hexdigit()) {
            error!("Invalid transaction ID format");
            return Err(PoolError {
                code: 6101,
                message: "Invalid transaction ID format".to_string(),
            });
        }

        // Validate size
        if entry.size == 0 {
            error!("Transaction size must be positive");
            return Err(PoolError {
                code: 6102,
                message: "Transaction size must be positive".to_string(),
            });
        }

        // Validate fee
        if entry.fee == 0 {
            error!("Transaction fee must be positive");
            return Err(PoolError {
                code: 6103,
                message: "Transaction fee must be positive".to_string(),
            });
        }

        // Validate fee rate
        if entry.fee_rate < self.min_fee_rate {
            error!("Transaction fee rate too low");
            return Err(PoolError {
                code: 6104,
                message: format!("Transaction fee rate too low: {}", entry.fee_rate),
            });
        }

        Ok(())
    }

    /// Evict low-fee transactions to make space
    fn evict_low_fee_transactions(&mut self, required_space: u64) -> PoolResult<()> {
        debug!(
            "Evicting low-fee transactions (required_space: {})",
            required_space
        );

        let mut evicted_size = 0;
        let mut to_remove = Vec::new();

        // Evict from lowest fee rate first
        for (_, txid) in self.by_fee_rate.iter() {
            if evicted_size >= required_space {
                break;
            }

            if let Some(entry) = self.entries.get(txid) {
                evicted_size += entry.size;
                to_remove.push(txid.clone());
            }
        }

        for txid in to_remove {
            self.remove_transaction(&txid)?;
        }

        info!("Evicted {} bytes from pool", evicted_size);
        Ok(())
    }

    /// Get pool statistics
    pub fn get_stats(&self) -> PoolStats {
        let entries = self.entries.values().collect::<Vec<_>>();
        let fee_rates: Vec<f64> = entries.iter().map(|e| e.fee_rate).collect();

        let avg_fee_rate = if !fee_rates.is_empty() {
            fee_rates.iter().sum::<f64>() / fee_rates.len() as f64
        } else {
            0.0
        };

        let min_fee_rate = fee_rates.iter().cloned().fold(f64::INFINITY, f64::min);
        let max_fee_rate = fee_rates.iter().cloned().fold(0.0, f64::max);

        PoolStats {
            entry_count: self.entries.len(),
            total_size: self.current_size,
            total_fees: entries.iter().map(|e| e.fee).sum(),
            avg_fee_rate,
            min_fee_rate: if min_fee_rate.is_infinite() {
                0.0
            } else {
                min_fee_rate
            },
            max_fee_rate,
        }
    }

    /// Clear pool
    pub fn clear(&mut self) {
        debug!("Clearing transaction pool");
        self.entries.clear();
        self.by_fee_rate.clear();
        self.current_size = 0;
        info!("Transaction pool cleared");
    }
}

/// Pool statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PoolStats {
    /// Number of entries in pool
    pub entry_count: usize,
    /// Total size in bytes
    pub total_size: u64,
    /// Total fees in MIST
    pub total_fees: u128,
    /// Average fee rate in MIST per byte
    pub avg_fee_rate: f64,
    /// Minimum fee rate in MIST per byte
    pub min_fee_rate: f64,
    /// Maximum fee rate in MIST per byte
    pub max_fee_rate: f64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pool_entry_creation() {
        let entry = PoolEntry::new("a".repeat(128), 1000, 50000);
        assert_eq!(entry.size, 1000);
        assert_eq!(entry.fee, 50000);
        assert_eq!(entry.fee_rate, 50.0);
    }

    #[test]
    fn test_transaction_pool_creation() {
        let pool = TransactionPool::new(1_000_000, 1.0, 10_000);
        assert_eq!(pool.max_size, 1_000_000);
        assert_eq!(pool.current_size, 0);
    }

    #[test]
    fn test_add_transaction() {
        let mut pool = TransactionPool::new(1_000_000, 1.0, 10_000);
        let entry = PoolEntry::new("a".repeat(128), 1000, 50000);

        let result = pool.add_transaction(entry);
        assert!(result.is_ok());
        assert_eq!(pool.entries.len(), 1);
    }

    #[test]
    fn test_remove_transaction() {
        let mut pool = TransactionPool::new(1_000_000, 1.0, 10_000);
        let txid = "a".repeat(128);
        let entry = PoolEntry::new(txid.clone(), 1000, 50000);

        pool.add_transaction(entry).unwrap();
        assert_eq!(pool.entries.len(), 1);

        let result = pool.remove_transaction(&txid);
        assert!(result.is_ok());
        assert_eq!(pool.entries.len(), 0);
    }

    #[test]
    fn test_get_transactions_by_fee_rate() {
        let mut pool = TransactionPool::new(1_000_000, 1.0, 10_000);

        let entry1 = PoolEntry::new("a".repeat(128), 1000, 50000); // 50 MIST/byte
        let entry2 = PoolEntry::new("b".repeat(128), 1000, 100000); // 100 MIST/byte

        pool.add_transaction(entry1).unwrap();
        pool.add_transaction(entry2).unwrap();

        let txs = pool.get_transactions_by_fee_rate();
        assert_eq!(txs.len(), 2);
        // Higher fee rate should be first
        assert!(txs[0].fee_rate > txs[1].fee_rate);
    }

    #[test]
    fn test_pool_stats() {
        let mut pool = TransactionPool::new(1_000_000, 1.0, 10_000);
        let entry = PoolEntry::new("a".repeat(128), 1000, 50000);

        pool.add_transaction(entry).unwrap();

        let stats = pool.get_stats();
        assert_eq!(stats.entry_count, 1);
        assert_eq!(stats.total_size, 1000);
        assert_eq!(stats.total_fees, 50000);
    }

    #[test]
    fn test_clear_pool() {
        let mut pool = TransactionPool::new(1_000_000, 1.0, 10_000);
        let entry = PoolEntry::new("a".repeat(128), 1000, 50000);

        pool.add_transaction(entry).unwrap();
        assert_eq!(pool.entries.len(), 1);

        pool.clear();
        assert_eq!(pool.entries.len(), 0);
        assert_eq!(pool.current_size, 0);
    }
}
