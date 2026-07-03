//! File manifests: the ordered list of chunk references that reconstructs a file.

use std::cmp::Ordering;
use std::io::{self, Cursor, Read, Seek, SeekFrom};

use crate::{BlobStore, Hash};

/// A reference to one stored chunk within a [`Manifest`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChunkRef {
    /// Content hash of the chunk (its key in the blob store).
    pub hash: Hash,
    /// Byte offset of this chunk's first byte within the reconstructed file.
    ///
    /// Recorded (rather than derived on the fly) so a reader can binary-search
    /// to the chunk containing an arbitrary offset — the basis for HTTP range
    /// requests / media seeking in a later phase.
    pub offset: u64,
    /// Length of the chunk in bytes.
    pub length: u32,
}

/// The recipe for reconstructing a file: its total size and its chunks in order.
///
/// A file is stored as its unique chunks in the blob store plus this manifest.
/// The metadata store (Phase 2) persists manifests; identical chunks — within a
/// file or across files and versions — are stored only once.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Manifest {
    /// Total size of the reconstructed file in bytes.
    pub total_size: u64,
    /// The file's chunks, in order.
    pub chunks: Vec<ChunkRef>,
}

/// A streaming [`Read`] + [`Seek`] over a file reconstructed from its [`Manifest`].
///
/// Chunks are fetched from the blob store lazily and one at a time, so
/// reconstructing a large file never holds more than a single chunk in memory.
/// [`Seek`] uses the per-chunk offsets recorded in the manifest to jump directly
/// to the chunk containing the target position.
#[derive(Debug)]
pub struct ManifestReader<'a> {
    store: &'a BlobStore,
    manifest: &'a Manifest,
    /// Index of the next chunk to load once `current` is exhausted.
    next_index: usize,
    /// The chunk currently being read from.
    current: Cursor<Vec<u8>>,
    /// File offset at which `current`'s chunk begins (its position within the
    /// whole file), so the overall stream position is `base + current.position()`.
    base: u64,
}

impl<'a> ManifestReader<'a> {
    pub(crate) fn new(store: &'a BlobStore, manifest: &'a Manifest) -> Self {
        Self {
            store,
            manifest,
            next_index: 0,
            current: Cursor::new(Vec::new()),
            base: 0,
        }
    }

    /// The current absolute position within the reconstructed file.
    fn position(&self) -> u64 {
        self.base + self.current.position()
    }
}

impl Read for ManifestReader<'_> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        // A read into an empty buffer must be a side-effect-free no-op; without
        // this guard the zero-byte read below would be mistaken for an exhausted
        // chunk and would advance (and discard) the remaining chunks.
        if buf.is_empty() {
            return Ok(0);
        }
        loop {
            let n = self.current.read(buf)?;
            if n > 0 {
                return Ok(n);
            }
            // Current chunk exhausted; advance to the next one, if any.
            let Some(chunk) = self.manifest.chunks.get(self.next_index) else {
                return Ok(0);
            };
            self.current = Cursor::new(self.store.get(&chunk.hash)?);
            self.base = chunk.offset;
            self.next_index += 1;
        }
    }
}

impl Seek for ManifestReader<'_> {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        let target = match pos {
            SeekFrom::Start(n) => i128::from(n),
            SeekFrom::Current(n) => i128::from(self.position()) + i128::from(n),
            SeekFrom::End(n) => i128::from(self.manifest.total_size) + i128::from(n),
        };
        if target < 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "cannot seek to a negative position",
            ));
        }
        let target = target as u64;

        // Seeking at or beyond EOF is legal: park past the last chunk so reads
        // return 0 while `position()` still reports the requested offset.
        if target >= self.manifest.total_size {
            self.next_index = self.manifest.chunks.len();
            self.current = Cursor::new(Vec::new());
            self.base = target;
            return Ok(target);
        }

        let idx = self
            .manifest
            .chunks
            .binary_search_by(|c| {
                if c.offset + u64::from(c.length) <= target {
                    Ordering::Less
                } else if c.offset > target {
                    Ordering::Greater
                } else {
                    Ordering::Equal
                }
            })
            .map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "manifest offsets are inconsistent",
                )
            })?;

        let chunk = &self.manifest.chunks[idx];
        let mut cursor = Cursor::new(self.store.get(&chunk.hash)?);
        cursor.set_position(target - chunk.offset);
        self.current = cursor;
        self.base = chunk.offset;
        self.next_index = idx + 1;
        Ok(target)
    }
}
