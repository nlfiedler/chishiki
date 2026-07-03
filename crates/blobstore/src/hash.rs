//! The content hash used as a blob's key.

use std::fmt;
use std::str::FromStr;

/// A blake3 content hash (32 bytes) — the key under which a blob is stored.
///
/// Displays and parses as a 64-character lowercase hex string.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Hash([u8; 32]);

impl Hash {
    /// Compute the hash of `data`.
    pub fn of(data: &[u8]) -> Self {
        Self(*blake3::hash(data).as_bytes())
    }

    /// Wrap raw hash bytes.
    pub fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// The raw 32-byte hash.
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// The hash as a 64-character lowercase hex string.
    pub fn to_hex(&self) -> String {
        self.to_string()
    }
}

impl fmt::Display for Hash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Delegate hex formatting to blake3 (stack-allocated, no heap String).
        f.write_str(blake3::Hash::from_bytes(self.0).to_hex().as_str())
    }
}

impl fmt::Debug for Hash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Hash({self})")
    }
}

impl FromStr for Hash {
    type Err = ParseHashError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // Length is checked explicitly to give a precise error; blake3 handles
        // the hex decoding itself.
        if s.len() != 64 {
            return Err(ParseHashError::InvalidLength(s.len()));
        }
        blake3::Hash::from_hex(s)
            .map(|h| Self(*h.as_bytes()))
            .map_err(|_| ParseHashError::InvalidHex)
    }
}

/// Error returned when parsing a [`Hash`] from a hex string fails.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParseHashError {
    /// The string was not exactly 64 characters long.
    InvalidLength(usize),
    /// The string contained a non-hex character.
    InvalidHex,
}

impl fmt::Display for ParseHashError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidLength(n) => write!(f, "expected 64 hex characters, got {n}"),
            Self::InvalidHex => write!(f, "string contains a non-hex character"),
        }
    }
}

impl std::error::Error for ParseHashError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hex_roundtrip() {
        let hash = Hash::of(b"hello world");
        let hex = hash.to_hex();
        assert_eq!(hex.len(), 64);
        assert_eq!(hex.parse::<Hash>().unwrap(), hash);
    }

    #[test]
    fn known_blake3_value() {
        // blake3("") — the empty-input hash, a stable published constant.
        assert_eq!(
            Hash::of(b"").to_hex(),
            "af1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262"
        );
    }

    #[test]
    fn rejects_bad_hex() {
        assert_eq!("abc".parse::<Hash>(), Err(ParseHashError::InvalidLength(3)));
        let bad = "g".repeat(64);
        assert_eq!(bad.parse::<Hash>(), Err(ParseHashError::InvalidHex));
    }

    #[test]
    fn accepts_uppercase_hex() {
        let hash = Hash::of(b"case check");
        let upper = hash.to_hex().to_uppercase();
        assert_eq!(upper.parse::<Hash>().unwrap(), hash);
    }
}
