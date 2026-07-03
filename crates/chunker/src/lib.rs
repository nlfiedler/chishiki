//! Content-defined chunking via FastCDC.
//!
//! Splits a byte stream into content-defined chunks so that only the unique
//! chunks need to be persisted (see the `blobstore` crate). This crate is a thin,
//! storage-agnostic wrapper over [`fastcdc`]'s streaming v2020 chunker: it yields
//! [`Chunk`]s and validates chunk-size configuration up front so callers get a
//! [`ConfigError`] instead of a panic.

use std::fmt;
use std::io::{self, Read};

use fastcdc::v2020::{
    AVERAGE_MAX, AVERAGE_MIN, MAXIMUM_MAX, MAXIMUM_MIN, MINIMUM_MAX, MINIMUM_MIN, StreamCDC,
};

/// Default minimum chunk size: 16 KiB.
pub const DEFAULT_MIN_SIZE: u32 = 16 * 1024;
/// Default average (target) chunk size: 64 KiB.
pub const DEFAULT_AVG_SIZE: u32 = 64 * 1024;
/// Default maximum chunk size: 256 KiB.
pub const DEFAULT_MAX_SIZE: u32 = 256 * 1024;

/// A single content-defined chunk produced from a stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Chunk {
    /// Starting byte offset of this chunk within the source stream.
    pub offset: u64,
    /// Length of the chunk in bytes (equal to `data.len()`).
    pub length: u32,
    /// The chunk's bytes.
    pub data: Vec<u8>,
}

/// Validated minimum/average/maximum chunk sizes for the FastCDC chunker.
///
/// Construct via [`ChunkerConfig::new`] (checked) or [`ChunkerConfig::default`]
/// (16 KiB / 64 KiB / 256 KiB). The bounds mirror the ranges the underlying
/// `fastcdc` crate requires, so a [`ChunkerConfig`] can never trigger its size
/// assertions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChunkerConfig {
    min_size: u32,
    avg_size: u32,
    max_size: u32,
}

impl ChunkerConfig {
    /// Create a configuration, validating that each size is within the range
    /// FastCDC accepts and that `min_size <= avg_size <= max_size`.
    pub fn new(min_size: u32, avg_size: u32, max_size: u32) -> Result<Self, ConfigError> {
        if !(MINIMUM_MIN..=MINIMUM_MAX).contains(&min_size) {
            return Err(ConfigError::MinOutOfRange(min_size));
        }
        if !(AVERAGE_MIN..=AVERAGE_MAX).contains(&avg_size) {
            return Err(ConfigError::AvgOutOfRange(avg_size));
        }
        if !(MAXIMUM_MIN..=MAXIMUM_MAX).contains(&max_size) {
            return Err(ConfigError::MaxOutOfRange(max_size));
        }
        if !(min_size <= avg_size && avg_size <= max_size) {
            return Err(ConfigError::NotAscending {
                min: min_size,
                avg: avg_size,
                max: max_size,
            });
        }
        Ok(Self {
            min_size,
            avg_size,
            max_size,
        })
    }

    /// Minimum chunk size in bytes.
    pub fn min_size(&self) -> u32 {
        self.min_size
    }

    /// Average (target) chunk size in bytes.
    pub fn avg_size(&self) -> u32 {
        self.avg_size
    }

    /// Maximum chunk size in bytes.
    pub fn max_size(&self) -> u32 {
        self.max_size
    }
}

impl Default for ChunkerConfig {
    fn default() -> Self {
        // The default sizes are known-valid, so this cannot fail.
        Self::new(DEFAULT_MIN_SIZE, DEFAULT_AVG_SIZE, DEFAULT_MAX_SIZE)
            .expect("default chunker sizes are within range")
    }
}

/// Error returned when [`ChunkerConfig::new`] is given invalid sizes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConfigError {
    /// `min_size` is outside the range FastCDC accepts.
    MinOutOfRange(u32),
    /// `avg_size` is outside the range FastCDC accepts.
    AvgOutOfRange(u32),
    /// `max_size` is outside the range FastCDC accepts.
    MaxOutOfRange(u32),
    /// The sizes are not ordered `min <= avg <= max`.
    NotAscending { min: u32, avg: u32, max: u32 },
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MinOutOfRange(v) => write!(
                f,
                "minimum chunk size {v} out of range [{MINIMUM_MIN}, {MINIMUM_MAX}]"
            ),
            Self::AvgOutOfRange(v) => write!(
                f,
                "average chunk size {v} out of range [{AVERAGE_MIN}, {AVERAGE_MAX}]"
            ),
            Self::MaxOutOfRange(v) => write!(
                f,
                "maximum chunk size {v} out of range [{MAXIMUM_MIN}, {MAXIMUM_MAX}]"
            ),
            Self::NotAscending { min, avg, max } => write!(
                f,
                "chunk sizes must satisfy min <= avg <= max, got {min} / {avg} / {max}"
            ),
        }
    }
}

impl std::error::Error for ConfigError {}

/// Chunk `reader` with FastCDC, yielding each [`Chunk`] in order.
///
/// The returned iterator reads lazily: only enough of the source to produce the
/// next chunk is buffered at a time. An I/O error from the source surfaces as an
/// [`io::Error`] item.
pub fn chunk_reader<R: Read>(
    reader: R,
    config: ChunkerConfig,
) -> impl Iterator<Item = io::Result<Chunk>> {
    StreamCDC::new(reader, config.min_size, config.avg_size, config.max_size).map(|result| {
        result
            .map(|cd| Chunk {
                offset: cd.offset,
                length: cd.length as u32,
                data: cd.data,
            })
            .map_err(io::Error::from)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Deterministic splitmix64-based byte generator so tests are reproducible
    /// yet varied enough to produce content-defined chunk boundaries.
    fn pseudo_random(len: usize, mut seed: u64) -> Vec<u8> {
        let mut out = Vec::with_capacity(len + 8);
        while out.len() < len {
            seed = seed.wrapping_add(0x9E37_79B9_7F4A_7C15);
            let mut z = seed;
            z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
            z ^= z >> 31;
            out.extend_from_slice(&z.to_le_bytes());
        }
        out.truncate(len);
        out
    }

    fn chunk_all(data: &[u8]) -> Vec<Chunk> {
        chunk_reader(data, ChunkerConfig::default())
            .collect::<io::Result<Vec<_>>>()
            .expect("in-memory reader never errors")
    }

    #[test]
    fn chunks_reassemble_to_original() {
        let data = pseudo_random(1_000_000, 42);
        let chunks = chunk_all(&data);
        let rejoined: Vec<u8> = chunks.iter().flat_map(|c| c.data.clone()).collect();
        assert_eq!(rejoined, data);
    }

    #[test]
    fn large_input_produces_multiple_chunks() {
        let data = pseudo_random(1_000_000, 7);
        let chunks = chunk_all(&data);
        assert!(
            chunks.len() > 1,
            "expected many chunks, got {}",
            chunks.len()
        );
    }

    #[test]
    fn offsets_and_lengths_are_consistent() {
        let data = pseudo_random(500_000, 99);
        let chunks = chunk_all(&data);
        let mut expected_offset = 0u64;
        for chunk in &chunks {
            assert_eq!(chunk.offset, expected_offset);
            assert_eq!(chunk.length as usize, chunk.data.len());
            expected_offset += u64::from(chunk.length);
        }
        assert_eq!(expected_offset, data.len() as u64);
    }

    #[test]
    fn chunking_is_deterministic() {
        let data = pseudo_random(300_000, 5);
        let first = chunk_all(&data);
        let second = chunk_all(&data);
        assert_eq!(first, second);
    }

    #[test]
    fn empty_input_produces_no_chunks() {
        let chunks = chunk_all(&[]);
        assert!(chunks.is_empty());
    }

    #[test]
    fn default_config_is_valid_and_ordered() {
        let cfg = ChunkerConfig::default();
        assert!(cfg.min_size() <= cfg.avg_size());
        assert!(cfg.avg_size() <= cfg.max_size());
    }

    #[test]
    fn config_rejects_out_of_range_and_unordered_sizes() {
        assert_eq!(
            ChunkerConfig::new(1, DEFAULT_AVG_SIZE, DEFAULT_MAX_SIZE),
            Err(ConfigError::MinOutOfRange(1))
        );
        // min > avg violates ascending order.
        assert!(matches!(
            ChunkerConfig::new(DEFAULT_MAX_SIZE, DEFAULT_AVG_SIZE, DEFAULT_MAX_SIZE),
            Err(ConfigError::NotAscending { .. })
        ));
    }
}
