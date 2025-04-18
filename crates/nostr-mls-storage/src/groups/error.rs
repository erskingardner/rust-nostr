//! Error types for the groups module

use std::fmt;

/// Error types for the groups module
#[derive(Debug)]
pub enum GroupError {
    /// Invalid parameters
    InvalidParameters(String),
    /// Database error
    DatabaseError(String),
    /// Group not found
    NotFound,
}

impl std::error::Error for GroupError {}

impl fmt::Display for GroupError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidParameters(message) => write!(f, "Invalid parameters: {}", message),
            Self::DatabaseError(message) => write!(f, "Database error: {}", message),
            Self::NotFound => write!(f, "Group not found"),
        }
    }
}
