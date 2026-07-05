//! Content-addressable blob store.
//!
//! Stores byte content keyed by its blake3 [`Hash`], so identical content is
//! stored exactly once. Two layers are provided:
//!
//! - **Blobs** — [`BlobStore::put`] / [`BlobStore::get`] / [`BlobStore::has`]
//!   (plus streaming [`BlobStore::put_reader`] / [`BlobStore::open_read`]) store
//!   and retrieve an individual blob by hash.
//! - **Files** — [`BlobStore::store_file`] splits a stream into content-defined
//!   chunks (via the `chunker` crate), stores only the unique ones, and returns
//!   an ordered [`Manifest`]. [`BlobStore::open_file`] streams the file back by
//!   reconstructing it from that manifest.
//!
//! On disk, each blob is a file at `<root>/<first two hex chars>/<full hex>`.
//! Writes go to a temporary file and are atomically renamed into place, so a
//! blob file only ever exists with complete, correct content.
//!
//! Chunk reference-counting and garbage collection are intentionally out of
//! scope here (deferred to Phase 6): blobs are only ever added.

mod hash;
mod manifest;

pub use hash::{Hash, ParseHashError};
pub use manifest::{ChunkRef, Manifest, ManifestReader};

use std::fs::{self, File};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};

use chunker::{ChunkerConfig, chunk_reader};
use tempfile::NamedTempFile;

/// A content-addressable blob store rooted at a directory.
///
/// Cloning is cheap (just the root path) and every clone addresses the same
/// on-disk store, so a clone can be moved into a blocking task.
#[derive(Debug, Clone)]
pub struct BlobStore {
    root: PathBuf,
}

impl BlobStore {
    /// Open the blob store rooted at `root`, creating the directory if needed.
    pub fn open(root: impl Into<PathBuf>) -> io::Result<Self> {
        let root = root.into();
        fs::create_dir_all(&root)?;
        Ok(Self { root })
    }

    /// The root directory of the store.
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// The path at which the blob for `hash` is (or would be) stored.
    fn blob_path(&self, hash: &Hash) -> PathBuf {
        let hex = hash.to_hex();
        self.root.join(&hex[..2]).join(&hex)
    }

    /// Whether a blob with the given hash is present.
    pub fn has(&self, hash: &Hash) -> bool {
        self.blob_path(hash).exists()
    }

    /// Store `data`, returning its hash. A no-op (beyond hashing) if already present.
    ///
    /// The hash is computed from the in-memory slice first, so an already-present
    /// blob is deduplicated without ever touching the disk.
    pub fn put(&self, data: &[u8]) -> io::Result<Hash> {
        let hash = Hash::of(data);
        if self.has(&hash) {
            return Ok(hash);
        }
        let mut temp = NamedTempFile::new_in(&self.root)?;
        temp.write_all(data)?;
        self.commit(temp, hash)
    }

    /// Store the full contents of `reader`, returning the hash of what was read.
    ///
    /// The data is streamed into a temporary file while being hashed, then
    /// atomically renamed to its content-addressed path. If a blob with the same
    /// hash already exists, the temporary file is discarded (deduplication).
    pub fn put_reader<R: Read>(&self, mut reader: R) -> io::Result<Hash> {
        let mut temp = NamedTempFile::new_in(&self.root)?;
        let mut hasher = blake3::Hasher::new();
        let mut buf = [0u8; 64 * 1024];
        loop {
            let n = match reader.read(&mut buf) {
                Ok(0) => break,
                Ok(n) => n,
                // A signal interrupted the read; retry rather than fail the store.
                Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
                Err(e) => return Err(e),
            };
            hasher.update(&buf[..n]);
            temp.write_all(&buf[..n])?;
        }
        let hash = Hash::from_bytes(*hasher.finalize().as_bytes());
        self.commit(temp, hash)
    }

    /// Move a fully-written temporary file into its content-addressed location.
    ///
    /// The temp file's data is flushed to disk (`sync_all`) *before* the rename,
    /// so a crash can never leave a blob file present with incomplete content —
    /// upholding the content-addressable invariant that a blob's bytes always
    /// hash to its name. If the destination already exists (this content was
    /// stored concurrently or previously), the temp file is simply discarded.
    fn commit(&self, temp: NamedTempFile, hash: Hash) -> io::Result<Hash> {
        let dest = self.blob_path(&hash);
        if dest.exists() {
            // Already stored; the temp file is removed when dropped. Deduplication.
            return Ok(hash);
        }
        if let Some(parent) = dest.parent() {
            fs::create_dir_all(parent)?;
        }
        temp.as_file().sync_all()?;
        // Atomic on the same filesystem. If a concurrent writer beat us to it,
        // the content is identical, so treat an existing destination as success.
        if let Err(err) = temp.persist(&dest)
            && !dest.exists()
        {
            return Err(err.error);
        }
        Ok(hash)
    }

    /// Read the entire blob with the given hash into memory.
    pub fn get(&self, hash: &Hash) -> io::Result<Vec<u8>> {
        fs::read(self.blob_path(hash))
    }

    /// Open the blob with the given hash for streaming reads.
    pub fn open_read(&self, hash: &Hash) -> io::Result<File> {
        File::open(self.blob_path(hash))
    }

    /// Split `reader` into content-defined chunks, store the unique ones, and
    /// return the ordered [`Manifest`] describing how to reconstruct the file.
    pub fn store_file<R: Read>(&self, reader: R, config: ChunkerConfig) -> io::Result<Manifest> {
        let mut manifest = Manifest::default();
        for chunk in chunk_reader(reader, config) {
            let chunk = chunk?;
            let hash = self.put(&chunk.data)?;
            manifest.total_size += u64::from(chunk.length);
            manifest.chunks.push(ChunkRef {
                hash,
                offset: chunk.offset,
                length: chunk.length,
            });
        }
        Ok(manifest)
    }

    /// A streaming reader that reconstructs a file from its [`Manifest`].
    pub fn open_file<'a>(&'a self, manifest: &'a Manifest) -> ManifestReader<'a> {
        ManifestReader::new(self, manifest)
    }

    /// Enumerate the hashes of every blob currently stored.
    ///
    /// Scans the two-level `<root>/<xx>/<hex>` layout, yielding each *regular
    /// file* whose name parses as a [`Hash`]. In-progress temp files (which live
    /// directly under the root, not in a shard directory), directories, and any
    /// other stray entry are skipped, so the result is exactly the set of
    /// committed blobs. Intended for garbage collection (mark-and-sweep against
    /// the set of referenced hashes).
    ///
    /// Best-effort per entry: an unreadable shard or entry is skipped rather than
    /// aborting the whole scan — under-reporting a blob only defers its
    /// reclamation (conservative/safe), so one bad entry can't stall GC forever.
    /// Only a failure to read the store root itself is propagated.
    pub fn list_hashes(&self) -> io::Result<Vec<Hash>> {
        let mut hashes = Vec::new();
        for shard in fs::read_dir(&self.root)? {
            let Ok(shard) = shard else { continue };
            // Only shard *directories* hold blobs; skip temp files at the root
            // and any entry whose type can't be read.
            if !shard.file_type().map(|t| t.is_dir()).unwrap_or(false) {
                continue;
            }
            let Ok(entries) = fs::read_dir(shard.path()) else {
                continue;
            };
            for blob in entries {
                let Ok(blob) = blob else { continue };
                // A blob is always a regular file; skip a hash-named directory or
                // anything else that isn't one (it must never reach `remove`).
                if !blob.file_type().map(|t| t.is_file()).unwrap_or(false) {
                    continue;
                }
                if let Some(name) = blob.file_name().to_str()
                    && let Ok(hash) = name.parse::<Hash>()
                {
                    hashes.push(hash);
                }
            }
        }
        Ok(hashes)
    }

    /// Delete the blob with the given hash, returning the number of bytes freed.
    ///
    /// Returns `Ok(0)` if the blob is already absent (idempotent). Callers must
    /// ensure the blob is unreferenced — deleting a referenced blob corrupts the
    /// files that depend on it. The (possibly now-empty) shard directory is left
    /// in place, since a concurrent writer may be about to reuse it.
    pub fn remove(&self, hash: &Hash) -> io::Result<u64> {
        let path = self.blob_path(hash);
        let size = match fs::metadata(&path) {
            Ok(meta) => meta.len(),
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(0),
            Err(e) => return Err(e),
        };
        match fs::remove_file(&path) {
            Ok(()) => Ok(size),
            // Lost a race with another remover; treat as already freed.
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(0),
            Err(e) => Err(e),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    fn temp_store() -> (tempfile::TempDir, BlobStore) {
        let dir = tempfile::tempdir().unwrap();
        let store = BlobStore::open(dir.path()).unwrap();
        (dir, store)
    }

    /// Deterministic splitmix64 byte generator (see the chunker tests).
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

    /// Count the committed blobs under the store root (via the production scan).
    fn count_blobs(store: &BlobStore) -> usize {
        store.list_hashes().unwrap().len()
    }

    #[test]
    fn put_get_roundtrip() {
        let (_dir, store) = temp_store();
        let hash = store.put(b"hello world").unwrap();
        assert!(store.has(&hash));
        assert_eq!(store.get(&hash).unwrap(), b"hello world");
        assert_eq!(hash, Hash::of(b"hello world"));
    }

    #[test]
    fn get_missing_is_error() {
        let (_dir, store) = temp_store();
        let absent = Hash::of(b"never stored");
        assert!(!store.has(&absent));
        assert!(store.get(&absent).is_err());
    }

    #[test]
    fn identical_content_is_stored_once() {
        let (_dir, store) = temp_store();
        let h1 = store.put(b"duplicate me").unwrap();
        let h2 = store.put(b"duplicate me").unwrap();
        assert_eq!(h1, h2);
        assert_eq!(count_blobs(&store), 1);
    }

    #[test]
    fn distinct_content_is_stored_separately() {
        let (_dir, store) = temp_store();
        store.put(b"one").unwrap();
        store.put(b"two").unwrap();
        assert_eq!(count_blobs(&store), 2);
    }

    #[test]
    fn put_reader_streams_and_reads_back() {
        let (_dir, store) = temp_store();
        let data = pseudo_random(200_000, 1);
        let hash = store.put_reader(data.as_slice()).unwrap();
        let mut back = Vec::new();
        store
            .open_read(&hash)
            .unwrap()
            .read_to_end(&mut back)
            .unwrap();
        assert_eq!(back, data);
    }

    #[test]
    fn store_and_reconstruct_file() {
        let (_dir, store) = temp_store();
        let data = pseudo_random(1_000_000, 2);
        let manifest = store
            .store_file(data.as_slice(), ChunkerConfig::default())
            .unwrap();

        assert_eq!(manifest.total_size, data.len() as u64);
        assert!(manifest.chunks.len() > 1);

        let mut back = Vec::new();
        store.open_file(&manifest).read_to_end(&mut back).unwrap();
        assert_eq!(back, data);
    }

    #[test]
    fn re_storing_a_file_dedups_all_chunks() {
        let (_dir, store) = temp_store();
        let data = pseudo_random(1_000_000, 3);
        let cfg = ChunkerConfig::default();

        let m1 = store.store_file(data.as_slice(), cfg).unwrap();
        let unique_chunks: HashSet<Hash> = m1.chunks.iter().map(|c| c.hash).collect();
        // Every unique chunk is on disk exactly once after the first store.
        assert_eq!(count_blobs(&store), unique_chunks.len());

        // Storing the same bytes again yields an identical manifest and adds nothing.
        let m2 = store.store_file(data.as_slice(), cfg).unwrap();
        assert_eq!(m1, m2);
        assert_eq!(count_blobs(&store), unique_chunks.len());
    }

    #[test]
    fn list_hashes_enumerates_stored_blobs() {
        let (_dir, store) = temp_store();
        let h1 = store.put(b"alpha").unwrap();
        let h2 = store.put(b"beta").unwrap();
        store.put(b"alpha").unwrap(); // dedup — still one blob

        let listed: HashSet<Hash> = store.list_hashes().unwrap().into_iter().collect();
        assert_eq!(listed, HashSet::from([h1, h2]));
        // An in-progress temp file at the root must not be listed as a blob.
        NamedTempFile::new_in(store.root()).unwrap();
        assert_eq!(store.list_hashes().unwrap().len(), 2);
    }

    #[test]
    fn remove_frees_the_blob_and_reports_size() {
        let (_dir, store) = temp_store();
        let hash = store.put(b"remove me").unwrap();
        assert!(store.has(&hash));

        assert_eq!(store.remove(&hash).unwrap(), b"remove me".len() as u64);
        assert!(!store.has(&hash));
        // Removing an absent blob is an idempotent no-op reporting zero bytes.
        assert_eq!(store.remove(&hash).unwrap(), 0);
    }

    #[test]
    fn empty_file_roundtrips() {
        let (_dir, store) = temp_store();
        let manifest = store.store_file(&[][..], ChunkerConfig::default()).unwrap();
        assert_eq!(manifest.total_size, 0);
        assert!(manifest.chunks.is_empty());

        let mut back = Vec::new();
        store.open_file(&manifest).read_to_end(&mut back).unwrap();
        assert!(back.is_empty());
    }

    #[test]
    fn manifest_chunk_offsets_are_cumulative() {
        let (_dir, store) = temp_store();
        let data = pseudo_random(1_000_000, 4);
        let manifest = store
            .store_file(data.as_slice(), ChunkerConfig::default())
            .unwrap();

        let mut expected = 0u64;
        for chunk in &manifest.chunks {
            assert_eq!(chunk.offset, expected);
            expected += u64::from(chunk.length);
        }
        assert_eq!(expected, manifest.total_size);
    }

    #[test]
    fn zero_length_read_does_not_consume_chunks() {
        let (_dir, store) = temp_store();
        let data = pseudo_random(1_000_000, 5);
        let manifest = store
            .store_file(data.as_slice(), ChunkerConfig::default())
            .unwrap();
        assert!(manifest.chunks.len() > 1);

        let mut reader = store.open_file(&manifest);
        // A read into an empty buffer must not advance past any chunks.
        assert_eq!(reader.read(&mut [0u8; 0]).unwrap(), 0);
        // The whole file is still intact afterwards.
        let mut back = Vec::new();
        reader.read_to_end(&mut back).unwrap();
        assert_eq!(back, data);
    }

    #[test]
    fn seek_reads_from_the_target_offset() {
        use std::io::{Seek, SeekFrom};

        let (_dir, store) = temp_store();
        let data = pseudo_random(1_000_000, 6);
        let manifest = store
            .store_file(data.as_slice(), ChunkerConfig::default())
            .unwrap();
        assert!(
            manifest.chunks.len() > 1,
            "need multiple chunks to test seeking"
        );

        let mut reader = store.open_file(&manifest);

        // Seek into the middle and read the remainder.
        let mid = (data.len() / 2) as u64;
        assert_eq!(reader.seek(SeekFrom::Start(mid)).unwrap(), mid);
        let mut tail = Vec::new();
        reader.read_to_end(&mut tail).unwrap();
        assert_eq!(tail, &data[mid as usize..]);

        // SeekFrom::End reads the trailing bytes.
        assert_eq!(
            reader.seek(SeekFrom::End(-10)).unwrap(),
            data.len() as u64 - 10
        );
        let mut last = Vec::new();
        reader.read_to_end(&mut last).unwrap();
        assert_eq!(last, &data[data.len() - 10..]);

        // Seeking at/past EOF is legal and yields empty reads.
        let past = data.len() as u64 + 100;
        assert_eq!(reader.seek(SeekFrom::Start(past)).unwrap(), past);
        assert_eq!(reader.read(&mut [0u8; 16]).unwrap(), 0);

        // Seeking before the start is an error.
        reader.seek(SeekFrom::Start(0)).unwrap();
        assert!(reader.seek(SeekFrom::Current(-1)).is_err());
    }
}
