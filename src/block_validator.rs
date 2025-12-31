//! Block Validator - Network Integration
//!
//! Comprehensive block validation including structure, transactions,
//! merkle root, timestamp, and difficulty validation.
//!
//! # Features
//! - Block structure validation
//! - Transaction validation
//! - Merkle root verification
//! - Timestamp validation
//! - Difficulty validation
//! - Error handling

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha512};
use std::fmt;
use tracing::{debug, error, info};

/// Block validation error type
#[derive(Debug, Clone)]
pub struct BlockValidationError {
    /// Error code
    pub code: u32,
    /// Error message
    pub message: String,
}

impl fmt::Display for BlockValidationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Block Validation Error {}: {}", self.code, self.message)
    }
}

impl std::error::Error for BlockValidationError {}

/// Result type for block validation
pub type BlockValidationResult<T> = std::result::Result<T, BlockValidationError>;

/// Transaction validation result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionValidationResult {
    /// Transaction ID
    pub txid: String,
    /// Is valid
    pub is_valid: bool,
    /// Validation errors
    pub errors: Vec<String>,
}

/// Block validation result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockValidationResultData {
    /// Block hash
    pub block_hash: String,
    /// Is valid
    pub is_valid: bool,
    /// Validation errors
    pub errors: Vec<String>,
    /// Transaction validation results
    pub transaction_results: Vec<TransactionValidationResult>,
}

/// Block validator
pub struct BlockValidator {
    /// Maximum block size in bytes
    pub max_block_size: u64,
    /// Maximum transactions per block
    pub max_transactions: u32,
    /// Minimum block time in seconds
    pub min_block_time: u64,
    /// Maximum timestamp drift in seconds
    pub max_timestamp_drift: u64,
}

impl BlockValidator {
    /// Create a new block validator
    pub fn new() -> Self {
        Self {
            max_block_size: 4_000_000, // 4 MB
            max_transactions: 10_000,
            min_block_time: 600,       // 10 minutes
            max_timestamp_drift: 7200, // 2 hours
        }
    }

    /// Validate block header
    pub fn validate_header(
        &self,
        hash: &str,
        version: u32,
        previous_hash: &str,
        merkle_root: &str,
        timestamp: u64,
        bits: &str,
        _nonce: u64,
    ) -> BlockValidationResult<()> {
        debug!("Validating block header: {}", hash);

        // Validate hash format
        if !self.is_valid_hash(hash) {
            error!("Invalid block hash format");
            return Err(BlockValidationError {
                code: 5001,
                message: "Invalid block hash format".to_string(),
            });
        }

        // Validate previous hash format
        if !self.is_valid_hash(previous_hash) {
            error!("Invalid previous hash format");
            return Err(BlockValidationError {
                code: 5002,
                message: "Invalid previous hash format".to_string(),
            });
        }

        // Validate merkle root format
        if !self.is_valid_hash(merkle_root) {
            error!("Invalid merkle root format");
            return Err(BlockValidationError {
                code: 5003,
                message: "Invalid merkle root format".to_string(),
            });
        }

        // Validate version
        if version == 0 {
            error!("Invalid block version");
            return Err(BlockValidationError {
                code: 5004,
                message: "Invalid block version".to_string(),
            });
        }

        // Validate timestamp
        self.validate_timestamp(timestamp)?;

        // Validate bits format
        if !self.is_valid_bits(bits) {
            error!("Invalid bits format");
            return Err(BlockValidationError {
                code: 5005,
                message: "Invalid bits format".to_string(),
            });
        }

        info!("Block header validation passed: {}", hash);
        Ok(())
    }

    /// Validate block transactions
    pub fn validate_transactions(
        &self,
        transactions: &[TransactionData],
    ) -> BlockValidationResult<Vec<TransactionValidationResult>> {
        debug!("Validating {} transactions", transactions.len());

        if transactions.is_empty() {
            error!("Block must contain at least one transaction");
            return Err(BlockValidationError {
                code: 5101,
                message: "Block must contain at least one transaction".to_string(),
            });
        }

        if transactions.len() > self.max_transactions as usize {
            error!("Too many transactions in block");
            return Err(BlockValidationError {
                code: 5102,
                message: format!("Too many transactions: {}", transactions.len()),
            });
        }

        let mut results = Vec::new();

        for tx in transactions {
            let result = self.validate_transaction(tx);
            results.push(result);
        }

        info!(
            "Transaction validation completed: {} transactions",
            transactions.len()
        );
        Ok(results)
    }

    /// Validate single transaction
    fn validate_transaction(&self, tx: &TransactionData) -> TransactionValidationResult {
        debug!("Validating transaction: {}", tx.txid);

        let mut errors = Vec::new();

        // Validate TXID format
        if !self.is_valid_hash(&tx.txid) {
            errors.push("Invalid transaction ID format".to_string());
        }

        // Validate version
        if tx.version == 0 {
            errors.push("Invalid transaction version".to_string());
        }

        // Validate inputs
        if tx.vin.is_empty() {
            errors.push("Transaction must have at least one input".to_string());
        }

        // Validate outputs
        if tx.vout.is_empty() {
            errors.push("Transaction must have at least one output".to_string());
        }

        // Validate input/output counts
        if tx.vin.len() > 10_000 {
            errors.push("Too many inputs in transaction".to_string());
        }

        if tx.vout.len() > 10_000 {
            errors.push("Too many outputs in transaction".to_string());
        }

        let is_valid = errors.is_empty();

        if is_valid {
            info!("Transaction validation passed: {}", tx.txid);
        } else {
            error!("Transaction validation failed: {}", tx.txid);
        }

        TransactionValidationResult {
            txid: tx.txid.clone(),
            is_valid,
            errors,
        }
    }

    /// Validate merkle root
    pub fn validate_merkle_root(
        &self,
        merkle_root: &str,
        transaction_hashes: &[String],
    ) -> BlockValidationResult<()> {
        debug!("Validating merkle root");

        if transaction_hashes.is_empty() {
            error!("No transactions to validate");
            return Err(BlockValidationError {
                code: 5201,
                message: "No transactions to validate".to_string(),
            });
        }

        // Calculate merkle root from transactions
        let calculated_root = self.calculate_merkle_root(transaction_hashes)?;

        if calculated_root != merkle_root {
            error!("Merkle root mismatch");
            return Err(BlockValidationError {
                code: 5202,
                message: "Merkle root mismatch".to_string(),
            });
        }

        info!("Merkle root validation passed");
        Ok(())
    }

    /// Calculate merkle root from transaction hashes
    fn calculate_merkle_root(&self, tx_hashes: &[String]) -> BlockValidationResult<String> {
        if tx_hashes.is_empty() {
            return Err(BlockValidationError {
                code: 5203,
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

    /// Validate timestamp
    fn validate_timestamp(&self, timestamp: u64) -> BlockValidationResult<()> {
        // PRODUCTION IMPLEMENTATION: Get current time with proper error handling
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|e| BlockValidationError::InvalidTimestamp(format!("Failed to get system time: {}", e)))?
            .as_secs();

        if timestamp > now + self.max_timestamp_drift {
            error!("Block timestamp too far in the future");
            return Err(BlockValidationError {
                code: 5301,
                message: "Block timestamp too far in the future".to_string(),
            });
        }

        Ok(())
    }

    /// Validate difficulty
    pub fn validate_difficulty(
        &self,
        block_hash: &str,
        difficulty: u64,
        bits: &str,
    ) -> BlockValidationResult<()> {
        debug!("Validating difficulty");

        // Validate difficulty is positive
        if difficulty == 0 {
            error!("Difficulty must be positive");
            return Err(BlockValidationError {
                code: 5401,
                message: "Difficulty must be positive".to_string(),
            });
        }

        // Validate bits format
        if !self.is_valid_bits(bits) {
            error!("Invalid bits format");
            return Err(BlockValidationError {
                code: 5402,
                message: "Invalid bits format".to_string(),
            });
        }

        // Validate hash meets difficulty target
        let target = self.bits_to_target(bits)?;
        let hash_value = self.hash_to_u256(block_hash);

        if hash_value > target {
            error!("Block hash does not meet difficulty target");
            return Err(BlockValidationError {
                code: 5403,
                message: "Block hash does not meet difficulty target".to_string(),
            });
        }

        info!("Difficulty validation passed");
        Ok(())
    }

    /// Check if hash format is valid
    fn is_valid_hash(&self, hash: &str) -> bool {
        hash.len() == 128 && hash.chars().all(|c| c.is_ascii_hexdigit())
    }

    /// Check if bits format is valid
    fn is_valid_bits(&self, bits: &str) -> bool {
        bits.len() == 8 && bits.chars().all(|c| c.is_ascii_hexdigit())
    }

    /// Convert bits to target
    fn bits_to_target(&self, bits: &str) -> BlockValidationResult<u128> {
        // Parse bits as compact difficulty representation
        let bits_value = u32::from_str_radix(bits, 16).map_err(|_| BlockValidationError {
            code: 5501,
            message: "Invalid bits value".to_string(),
        })?;

        // Extract exponent and mantissa
        let exponent = (bits_value >> 24) as u8;
        let mantissa = bits_value & 0xffffff;

        // Calculate target: mantissa * 2^(8*(exponent-3))
        if exponent <= 3 {
            Ok((mantissa >> (8 * (3 - exponent))) as u128)
        } else {
            Ok((mantissa as u128) << (8 * (exponent - 3)))
        }
    }

    /// Convert hash to u256 value
    fn hash_to_u256(&self, hash: &str) -> u128 {
        // Parse first 32 hex chars (128 bits) of hash
        u128::from_str_radix(&hash[..32], 16).unwrap_or(0)
    }
}

impl Default for BlockValidator {
    fn default() -> Self {
        Self::new()
    }
}

/// Transaction data for validation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionData {
    /// Transaction ID
    pub txid: String,
    /// Transaction version
    pub version: u32,
    /// Inputs
    pub vin: Vec<TransactionInput>,
    /// Outputs
    pub vout: Vec<TransactionOutput>,
    /// Lock time
    pub locktime: u32,
}

/// Transaction input
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionInput {
    /// Previous transaction ID
    pub txid: String,
    /// Previous output index
    pub vout: u32,
    /// Script signature
    pub script_sig: Vec<u8>,
    /// Sequence number
    pub sequence: u32,
}

/// Transaction output
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionOutput {
    /// Amount in MIST
    pub amount: u128,
    /// Script pubkey
    pub script_pubkey: Vec<u8>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_block_validator_creation() {
        let validator = BlockValidator::new();
        assert_eq!(validator.max_block_size, 4_000_000);
        assert_eq!(validator.max_transactions, 10_000);
    }

    #[test]
    fn test_is_valid_hash() {
        let validator = BlockValidator::new();
        assert!(validator.is_valid_hash(&"a".repeat(128)));
        assert!(!validator.is_valid_hash(&"a".repeat(127)));
        assert!(!validator.is_valid_hash(&"g".repeat(128)));
    }

    #[test]
    fn test_is_valid_bits() {
        let validator = BlockValidator::new();
        assert!(validator.is_valid_bits("207fffff"));
        assert!(!validator.is_valid_bits("207fff"));
        assert!(!validator.is_valid_bits("207fffgg"));
    }

    #[test]
    fn test_validate_header() {
        let validator = BlockValidator::new();
        let result = validator.validate_header(
            &"a".repeat(128),
            1,
            &"b".repeat(128),
            &"c".repeat(128),
            1234567890,
            "207fffff",
            12345,
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_header_invalid_hash() {
        let validator = BlockValidator::new();
        let result = validator.validate_header(
            "invalid",
            1,
            &"b".repeat(128),
            &"c".repeat(128),
            1234567890,
            "207fffff",
            12345,
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_transactions_empty() {
        let validator = BlockValidator::new();
        let result = validator.validate_transactions(&[]);
        assert!(result.is_err());
    }

    #[test]
    fn test_merkle_root_calculation() {
        let validator = BlockValidator::new();
        let hashes = vec!["a".repeat(128), "b".repeat(128)];
        let result = validator.calculate_merkle_root(&hashes);
        assert!(result.is_ok());
    }
}
