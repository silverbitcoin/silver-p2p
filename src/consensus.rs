//! Consensus Mechanism - Network Integration
//!
//! Implements Proof-of-Work consensus validation, block validation,
//! chain validation, and fork resolution.
//!
//! # Features
//! - Proof-of-Work validation
//! - Block validation
//! - Chain validation
//! - Fork resolution
//! - Difficulty adjustment
//! - Error handling

use sha2::{Digest, Sha512};
use std::fmt;
use tracing::{debug, error, info};

/// Consensus error type
#[derive(Debug, Clone)]
pub struct ConsensusError {
    /// Error code
    pub code: u32,
    /// Error message
    pub message: String,
}

impl fmt::Display for ConsensusError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Consensus Error {}: {}", self.code, self.message)
    }
}

impl std::error::Error for ConsensusError {}

/// Result type for consensus operations
pub type ConsensusResult<T> = std::result::Result<T, ConsensusError>;

/// Proof-of-Work validator
pub struct ProofOfWorkValidator {
    /// Target difficulty
    pub target_difficulty: u64,
    /// Minimum difficulty
    pub min_difficulty: u64,
    /// Maximum difficulty
    pub max_difficulty: u64,
}

impl ProofOfWorkValidator {
    /// Create a new PoW validator
    pub fn new(target_difficulty: u64) -> Self {
        Self {
            target_difficulty,
            min_difficulty: 1,
            max_difficulty: u64::MAX,
        }
    }

    /// Validate proof-of-work
    pub fn validate_pow(
        &self,
        block_hash: &str,
        _nonce: u64,
        difficulty: u64,
    ) -> ConsensusResult<()> {
        debug!("Validating PoW for block: {}", block_hash);

        // Validate difficulty is within range
        if difficulty < self.min_difficulty || difficulty > self.max_difficulty {
            error!("Difficulty out of range: {}", difficulty);
            return Err(ConsensusError {
                code: 4001,
                message: format!("Difficulty out of range: {}", difficulty),
            });
        }

        // Validate block hash format
        if block_hash.len() != 128 || !block_hash.chars().all(|c| c.is_ascii_hexdigit()) {
            error!("Invalid block hash format");
            return Err(ConsensusError {
                code: 4002,
                message: "Invalid block hash format".to_string(),
            });
        }

        // Verify PoW: hash must be less than target
        let target = self.difficulty_to_target(difficulty);
        let hash_value = self.hash_to_u256(block_hash);

        if hash_value > target {
            error!("PoW validation failed: hash exceeds target");
            return Err(ConsensusError {
                code: 4003,
                message: "PoW validation failed: hash exceeds target".to_string(),
            });
        }

        info!("PoW validation passed for block: {}", block_hash);
        Ok(())
    }

    /// Convert difficulty to target
    fn difficulty_to_target(&self, difficulty: u64) -> u128 {
        // Target = max_target / difficulty
        // For u128, max value is 2^128 - 1
        let max_target: u128 = u128::MAX;
        max_target / (difficulty as u128)
    }

    /// Convert hash to u256 value
    fn hash_to_u256(&self, hash: &str) -> u128 {
        // Parse first 32 hex chars (128 bits) of hash
        u128::from_str_radix(&hash[..32], 16).unwrap_or(0)
    }
}

/// Block validator
#[derive(Debug, Clone)]
pub struct BlockValidator {
    /// Maximum block size in bytes
    pub max_block_size: u64,
    /// Maximum transactions per block
    pub max_transactions: u32,
    /// Minimum block time in seconds
    pub min_block_time: u64,
}

impl BlockValidator {
    /// Create a new block validator
    pub fn new() -> Self {
        Self {
            max_block_size: 4_000_000, // 4 MB
            max_transactions: 10_000,
            min_block_time: 600, // 10 minutes
        }
    }

    /// Validate block structure
    #[allow(clippy::too_many_arguments)]
    pub fn validate_block_structure(
        &self,
        hash: &str,
        _height: u64,
        version: u32,
        previous_hash: &str,
        merkle_root: &str,
        timestamp: u64,
        bits: &str,
        _nonce: u64,
        tx_count: u32,
    ) -> ConsensusResult<()> {
        debug!("Validating block structure: {}", hash);

        // Validate hash format
        if hash.len() != 128 || !hash.chars().all(|c| c.is_ascii_hexdigit()) {
            error!("Invalid block hash format");
            return Err(ConsensusError {
                code: 4101,
                message: "Invalid block hash format".to_string(),
            });
        }

        // Validate previous hash format
        if previous_hash.len() != 128 || !previous_hash.chars().all(|c| c.is_ascii_hexdigit()) {
            error!("Invalid previous hash format");
            return Err(ConsensusError {
                code: 4102,
                message: "Invalid previous hash format".to_string(),
            });
        }

        // Validate merkle root format
        if merkle_root.len() != 128 || !merkle_root.chars().all(|c| c.is_ascii_hexdigit()) {
            error!("Invalid merkle root format");
            return Err(ConsensusError {
                code: 4103,
                message: "Invalid merkle root format".to_string(),
            });
        }

        // Validate version
        if version == 0 {
            error!("Invalid block version");
            return Err(ConsensusError {
                code: 4104,
                message: "Invalid block version".to_string(),
            });
        }

        // Validate transaction count
        if tx_count == 0 {
            error!("Block must contain at least one transaction");
            return Err(ConsensusError {
                code: 4105,
                message: "Block must contain at least one transaction".to_string(),
            });
        }

        if tx_count > self.max_transactions {
            error!("Too many transactions in block");
            return Err(ConsensusError {
                code: 4106,
                message: format!("Too many transactions: {}", tx_count),
            });
        }

        // Validate timestamp
        // PRODUCTION IMPLEMENTATION: Get current time with proper error handling
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|e| P2PError::InternalError(format!("Failed to get system time: {}", e)))?
            .as_secs();

        if timestamp > now + 7200 {
            // Allow 2 hours in the future
            error!("Block timestamp too far in the future");
            return Err(ConsensusError {
                code: 4107,
                message: "Block timestamp too far in the future".to_string(),
            });
        }

        // Validate bits format
        if bits.len() != 8 || !bits.chars().all(|c| c.is_ascii_hexdigit()) {
            error!("Invalid bits format");
            return Err(ConsensusError {
                code: 4108,
                message: "Invalid bits format".to_string(),
            });
        }

        info!("Block structure validation passed: {}", hash);
        Ok(())
    }

    /// Validate merkle root
    pub fn validate_merkle_root(
        &self,
        merkle_root: &str,
        transaction_hashes: &[String],
    ) -> ConsensusResult<()> {
        debug!("Validating merkle root");

        if transaction_hashes.is_empty() {
            error!("No transactions to validate");
            return Err(ConsensusError {
                code: 4201,
                message: "No transactions to validate".to_string(),
            });
        }

        // Calculate merkle root from transactions
        let calculated_root = self.calculate_merkle_root(transaction_hashes)?;

        if calculated_root != merkle_root {
            error!("Merkle root mismatch");
            return Err(ConsensusError {
                code: 4202,
                message: "Merkle root mismatch".to_string(),
            });
        }

        info!("Merkle root validation passed");
        Ok(())
    }

    /// Calculate merkle root from transaction hashes
    fn calculate_merkle_root(&self, tx_hashes: &[String]) -> ConsensusResult<String> {
        if tx_hashes.is_empty() {
            return Err(ConsensusError {
                code: 4203,
                message: "No transactions".to_string(),
            });
        }

        let mut hashes = tx_hashes.to_vec();

        while hashes.len() > 1 {
            let mut next_level = Vec::new();

            for i in (0..hashes.len()).step_by(2) {
                let left = &hashes[i];
                let right = if i + 1 < hashes.len() {
                    &hashes[i + 1]
                } else {
                    left
                };

                let combined = format!("{}{}", left, right);
                let mut hasher = Sha512::new();
                hasher.update(combined.as_bytes());
                let result = hasher.finalize();
                next_level.push(format!("{:x}", result));
            }

            hashes = next_level;
        }

        Ok(hashes[0].clone())
    }
}

impl Default for BlockValidator {
    fn default() -> Self {
        Self::new()
    }
}

/// Chain validator
pub struct ChainValidator {
    /// Maximum chain reorganization depth
    pub max_reorg_depth: u64,
}

impl ChainValidator {
    /// Create a new chain validator
    pub fn new() -> Self {
        Self {
            max_reorg_depth: 100,
        }
    }

    /// Validate chain continuity
    pub fn validate_chain_continuity(
        &self,
        previous_hash: &str,
        current_hash: &str,
        previous_height: u64,
        current_height: u64,
    ) -> ConsensusResult<()> {
        debug!("Validating chain continuity");

        // Validate height progression
        if current_height != previous_height + 1 {
            error!("Invalid height progression");
            return Err(ConsensusError {
                code: 4301,
                message: "Invalid height progression".to_string(),
            });
        }

        // Validate hash format
        if previous_hash.len() != 128 || !previous_hash.chars().all(|c| c.is_ascii_hexdigit()) {
            error!("Invalid previous hash format");
            return Err(ConsensusError {
                code: 4302,
                message: "Invalid previous hash format".to_string(),
            });
        }

        if current_hash.len() != 128 || !current_hash.chars().all(|c| c.is_ascii_hexdigit()) {
            error!("Invalid current hash format");
            return Err(ConsensusError {
                code: 4303,
                message: "Invalid current hash format".to_string(),
            });
        }

        info!("Chain continuity validation passed");
        Ok(())
    }

    /// Validate fork resolution
    pub fn validate_fork_resolution(
        &self,
        fork_depth: u64,
        fork_chain_work: u128,
        main_chain_work: u128,
    ) -> ConsensusResult<()> {
        debug!("Validating fork resolution");

        // Validate fork depth
        if fork_depth > self.max_reorg_depth {
            error!("Fork depth exceeds maximum");
            return Err(ConsensusError {
                code: 4401,
                message: "Fork depth exceeds maximum".to_string(),
            });
        }

        // Fork is valid if it has more chain work
        if fork_chain_work <= main_chain_work {
            error!("Fork has insufficient chain work");
            return Err(ConsensusError {
                code: 4402,
                message: "Fork has insufficient chain work".to_string(),
            });
        }

        info!("Fork resolution validation passed");
        Ok(())
    }
}

impl Default for ChainValidator {
    fn default() -> Self {
        Self::new()
    }
}

/// Difficulty adjustment calculator
pub struct DifficultyAdjustment {
    /// Target block time in seconds
    pub target_block_time: u64,
    /// Adjustment period in blocks
    pub adjustment_period: u64,
}

impl DifficultyAdjustment {
    /// Create a new difficulty adjustment calculator
    pub fn new() -> Self {
        Self {
            target_block_time: 600,  // 10 minutes
            adjustment_period: 2016, // 2 weeks of blocks
        }
    }

    /// Calculate new difficulty
    pub fn calculate_new_difficulty(
        &self,
        current_difficulty: u64,
        actual_time: u64,
    ) -> ConsensusResult<u64> {
        debug!("Calculating new difficulty");

        let expected_time = self.target_block_time * self.adjustment_period;

        // Prevent difficulty from changing too much
        // If actual_time < expected_time, blocks came faster, so increase difficulty
        // If actual_time > expected_time, blocks came slower, so decrease difficulty
        let ratio = if actual_time > 0 {
            (expected_time as f64) / (actual_time as f64)
        } else {
            1.0
        };

        // Clamp ratio between 0.25 and 4.0
        let clamped_ratio = ratio.clamp(0.25, 4.0);

        let new_difficulty = ((current_difficulty as f64) * clamped_ratio) as u64;

        // Ensure minimum difficulty
        let final_difficulty = new_difficulty.max(1);

        info!(
            "Difficulty adjusted from {} to {} (ratio: {})",
            current_difficulty, final_difficulty, clamped_ratio
        );

        Ok(final_difficulty)
    }
}

impl Default for DifficultyAdjustment {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pow_validator_creation() {
        let validator = ProofOfWorkValidator::new(1000);
        assert_eq!(validator.target_difficulty, 1000);
    }

    #[test]
    fn test_block_validator_creation() {
        let validator = BlockValidator::new();
        assert_eq!(validator.max_block_size, 4_000_000);
        assert_eq!(validator.max_transactions, 10_000);
    }

    #[test]
    fn test_chain_validator_creation() {
        let validator = ChainValidator::new();
        assert_eq!(validator.max_reorg_depth, 100);
    }

    #[test]
    fn test_difficulty_adjustment() {
        let adj = DifficultyAdjustment::new();
        let expected_time = adj.target_block_time * adj.adjustment_period;

        // If actual time equals expected time, difficulty should stay the same
        let new_diff = adj.calculate_new_difficulty(1000, expected_time).unwrap();
        assert_eq!(new_diff, 1000);
    }

    #[test]
    fn test_difficulty_adjustment_increase() {
        let adj = DifficultyAdjustment::new();
        let expected_time = adj.target_block_time * adj.adjustment_period;

        // If actual time is half expected, difficulty should increase
        let new_diff = adj
            .calculate_new_difficulty(1000, expected_time / 2)
            .unwrap();
        assert!(new_diff > 1000);
    }

    #[test]
    fn test_difficulty_adjustment_decrease() {
        let adj = DifficultyAdjustment::new();
        let expected_time = adj.target_block_time * adj.adjustment_period;

        // If actual time is double expected, difficulty should decrease
        let new_diff = adj
            .calculate_new_difficulty(1000, expected_time * 2)
            .unwrap();
        assert!(new_diff < 1000);
    }

    #[test]
    fn test_chain_validator_continuity() {
        let validator = ChainValidator::new();
        let result =
            validator.validate_chain_continuity(&"a".repeat(128), &"b".repeat(128), 100, 101);
        assert!(result.is_ok());
    }

    #[test]
    fn test_chain_validator_invalid_height() {
        let validator = ChainValidator::new();
        let result = validator.validate_chain_continuity(
            &"a".repeat(128),
            &"b".repeat(128),
            100,
            102, // Invalid: should be 101
        );
        assert!(result.is_err());
    }
}
