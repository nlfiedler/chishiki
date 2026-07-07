//! The [`DavFileSystem`] implementation over the metadata store + blob store.
//!
//! Every method follows the same shape the `dav-server` traits require: an
//! `async move { ... }.boxed()` future that does its (synchronous) metadata and
//! blob work without ever holding a lock across an `.await`, so the returned
//! future stays `Send`.

use std::io::Read;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::SystemTime;

use blobstore::{BlobStore, ChunkStream, Manifest};
use chunker::ChunkerConfig;
use dav_server::davpath::DavPath;
use dav_server::fs::{
    DavDirEntry, DavFile, DavFileSystem, DavMetaData, FsError, FsFuture, FsResult, FsStream,
    OpenOptions, ReadDirMeta,
};
use futures_util::future::FutureExt;
use futures_util::stream::{self, StreamExt};
use index::SearchIndex;

use crate::file::FileHandle;
use crate::meta::{MetaError, MetaStore, Node, ROOT_ID};
use crate::metadata::{DirEntry, Meta};

/// Shared state behind the (cheaply cloneable) [`DavFs`] handle.
pub(crate) struct Inner {
    pub(crate) meta: MetaStore,
    pub(crate) blobs: BlobStore,
    pub(crate) chunker: ChunkerConfig,
    /// Full-text reverse index over indexable file content (Phase 5).
    pub(crate) index: SearchIndex,
    /// Coordinates blob garbage collection with content writes (Phase 6).
    ///
    /// GC deletes a blob only if the live set (a snapshot of `version_chunks`)
    /// doesn't reference it. The hazard is a write that adds a *new* reference to
    /// a hash that was unreferenced at snapshot time; if GC then deletes that
    /// blob, the write dangles. So **every path that adds a chunk reference holds
    /// a shared (read) guard** across confirming the chunk is present/referenced
    /// and recording the new reference, and **GC holds the exclusive (write)
    /// guard** across mark + sweep. The reference-adding paths are:
    /// `FileHandle::flush` (stores brand-new blobs) and the reference-only writes
    /// `DavFs::copy` / `DavFs::revert_to_version` (via [`Inner::reference_under_guard`],
    /// which reuse chunks that a concurrent delete/prune could otherwise strand).
    /// Reads add no references and need no guard.
    pub(crate) gc_lock: std::sync::RwLock<()>,
    /// Directory for in-progress upload temp files.
    pub(crate) tmp_dir: PathBuf,
}

/// Largest file content we tokenize into the search index. A text document is
/// tiny; the cap avoids reading a large mislabeled file into memory to index.
const MAX_INDEX_BYTES: u64 = 4 * 1024 * 1024;

/// Candidate pool fetched from the index before scope filtering (see
/// [`DavFs::search`]). Bounds the work of a scoped query while being generous
/// enough that in-scope matches are rarely cut before filtering.
const MAX_SEARCH_FETCH: usize = 1000;

/// Whether `path` names a file strictly inside the collection at `prefix`
/// (`prefix` followed by a `/` boundary), so `/docs` matches `/docs/a` but not
/// `/docsX/a`. Both are absolute virtual paths from [`MetaStore::path_of`].
fn is_within(path: &[u8], prefix: &[u8]) -> bool {
    path.len() > prefix.len() && path.starts_with(prefix) && path[prefix.len()] == b'/'
}

impl Inner {
    /// Bring the search index in sync with a node's current content.
    ///
    /// Indexes the (capped) content of a text file, or drops a document a text
    /// file had while it was smaller than the cap. A node that was never a
    /// candidate for indexing (a directory, a binary/non-text name, or a file
    /// with no content) is left untouched — a node's name is fixed except via
    /// rename (which doesn't reindex), so such a node can hold no stale document,
    /// and skipping it avoids a needless index commit per binary-media upload.
    ///
    /// Best-effort: the metadata/blob store is the source of truth and the index
    /// is derived, so a failure is logged rather than propagated (it would
    /// otherwise fail a write whose content is already durably stored).
    pub(crate) fn reindex(&self, node_id: i64) {
        if let Err(e) = self.try_reindex(node_id) {
            eprintln!("chishiki: search index update failed for node {node_id}: {e}");
        }
    }

    fn try_reindex(&self, node_id: i64) -> Result<(), VfsError> {
        let node = self.meta.get_node(node_id)?;
        // Only a text file that currently has content is ever indexed.
        let is_text_file =
            !node.is_dir && node.current_version_id.is_some() && is_indexable_name(&node.name);
        if !is_text_file {
            // Never a candidate → nothing was ever indexed for it. Skip entirely
            // (no delete, no commit) so binary/media writes don't fsync the index.
            return Ok(());
        }
        if node.size > MAX_INDEX_BYTES {
            // A text file that grew past the cap: drop any document it had while
            // small so its stale content stops matching.
            self.index.remove_document(node_id as u64)?;
            self.index.commit()?;
            return Ok(());
        }
        let manifest = self.meta.load_manifest(node_id)?;
        let mut reader = self.blobs.open_file(&manifest);
        let mut buf = Vec::with_capacity(node.size as usize);
        reader.read_to_end(&mut buf).map_err(VfsError::Io)?;
        let text = String::from_utf8_lossy(&buf);
        self.index.index_document(node_id as u64, &text)?;
        self.index.commit()?;
        Ok(())
    }

    /// Reference an already-stored manifest as `node_id`'s new content, holding
    /// the shared GC guard across loading the manifest and recording the reference.
    ///
    /// For reference-only writes (copy, revert) that reuse chunks already in the
    /// blob store: the guard stops GC from sweeping a chunk in the window between
    /// confirming it's referenced (loading the source/version manifest) and
    /// re-referencing it — a concurrent delete/prune of the source could
    /// otherwise drop its last reference and make it collectible. See
    /// [`Inner::gc_lock`]; `flush` holds the same guard around storing *new* blobs.
    fn reference_under_guard<F>(&self, node_id: i64, load: F) -> Result<(), MetaError>
    where
        F: FnOnce(&MetaStore) -> Result<Manifest, MetaError>,
    {
        let _gc = self.gc_lock.read().unwrap_or_else(|e| e.into_inner());
        let manifest = load(&self.meta)?;
        self.meta.set_file_content(node_id, &manifest)?;
        Ok(())
    }

    /// Remove a node's document from the search index (best-effort, logged).
    fn deindex(&self, node_id: i64) {
        if let Err(e) = self
            .index
            .remove_document(node_id as u64)
            .and_then(|()| self.index.commit())
        {
            eprintln!("chishiki: search index removal failed for node {node_id}: {e}");
        }
    }
}

/// Whether a file name looks like indexable text (by extension). Binary media
/// (images, video, audio, archives) is deliberately excluded — only textual
/// documents are tokenized into the reverse index.
fn is_indexable_name(name: &[u8]) -> bool {
    // Dot-prefixed names (`.DS_Store`, AppleDouble `._*`, dotfiles) are never
    // indexed — they're hidden from the browser listing and would only pollute
    // search. This also rejects a name that is *only* an extension (e.g. `.md`).
    if name.first() == Some(&b'.') {
        return false;
    }
    let lower = name.to_ascii_lowercase();
    let ext = match lower.rsplit(|&b| b == b'.').next() {
        // A name with no extension isn't indexed.
        Some(e) if e.len() < lower.len() && !e.is_empty() => e,
        _ => return false,
    };
    matches!(
        ext,
        b"md"
            | b"markdown"
            | b"txt"
            | b"text"
            | b"log"
            | b"rst"
            | b"org"
            | b"tex"
            | b"json"
            | b"csv"
            | b"tsv"
            | b"toml"
            | b"yaml"
            | b"yml"
            | b"xml"
            | b"ini"
            | b"cfg"
            | b"conf"
            | b"html"
            | b"htm"
            | b"css"
            | b"rs"
            | b"py"
            | b"js"
            | b"ts"
            | b"go"
            | b"c"
            | b"h"
            | b"cpp"
            | b"hpp"
            | b"java"
            | b"rb"
            | b"sh"
            | b"sql"
    )
}

/// A `dav-server` filesystem backed by the SQLite metadata store and the
/// content-addressable blob store.
///
/// Cloning is cheap (an `Arc` bump) and every clone shares the same backing
/// store, matching how `dav-server` clones the filesystem per request.
#[derive(Clone)]
pub struct DavFs {
    inner: Arc<Inner>,
}

impl std::fmt::Debug for DavFs {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DavFs")
            .field("root", &self.inner.blobs.root())
            .finish()
    }
}

impl DavFs {
    /// Open (creating if needed) a filesystem rooted at `data_dir`.
    ///
    /// Lays out `data_dir/blobs` (blob store), `data_dir/metadata.sqlite`
    /// (metadata), and `data_dir/tmp` (upload staging).
    pub fn open(data_dir: impl Into<PathBuf>) -> Result<Self, VfsError> {
        let data_dir = data_dir.into();
        std::fs::create_dir_all(&data_dir)?;
        let blobs = BlobStore::open(data_dir.join("blobs"))?;
        let meta = MetaStore::open(data_dir.join("metadata.sqlite"))?;
        let index = SearchIndex::open(data_dir.join("index"))?;
        let tmp_dir = data_dir.join("tmp");
        std::fs::create_dir_all(&tmp_dir)?;
        Ok(Self {
            inner: Arc::new(Inner {
                meta,
                blobs,
                index,
                chunker: ChunkerConfig::default(),
                gc_lock: std::sync::RwLock::new(()),
                tmp_dir,
            }),
        })
    }

    fn resolve(&self, path: &DavPath) -> FsResult<Node> {
        self.inner
            .meta
            .lookup_path(&segments(path))
            .map_err(meta_to_fs)
    }

    /// Resolve `path` to a file node (not a collection), for the version APIs.
    fn resolve_file(&self, path: &DavPath) -> Result<Node, VfsError> {
        let node = self.inner.meta.lookup_path(&segments(path))?;
        if node.is_dir {
            return Err(VfsError::Meta(MetaError::IsADirectory));
        }
        Ok(node)
    }

    /// List the version history of the file at `path`, oldest first.
    ///
    /// This is the read side of auto-versioning, surfaced to the outer HTTP
    /// router (`dav-server` does not route Delta-V methods).
    pub fn list_versions(&self, path: &DavPath) -> Result<Vec<VersionInfo>, VfsError> {
        let node = self.resolve_file(path)?;
        let versions = self.inner.meta.list_versions(node.id)?;
        Ok(versions
            .into_iter()
            .map(|v| VersionInfo {
                number: v.number,
                size: v.size,
                created: v.created,
                is_current: Some(v.id) == node.current_version_id,
            })
            .collect())
    }

    /// Open an owned streaming reader over a specific version (by 1-based number)
    /// of the file at `path`.
    ///
    /// The returned [`VersionReader`] fetches chunks lazily, one at a time, so a
    /// historical version of **any size** streams with bounded memory — there is
    /// no in-memory cap. It is `Send + 'static`, so the router can move it into a
    /// `spawn_blocking` and stream it as the response body (see
    /// `docs/specs/0006-large-file-streaming.md`). Only cheap metadata work
    /// happens here; no blob bytes are read until the reader is polled.
    ///
    /// Like the live read path, this holds no GC guard: a version that is
    /// concurrently pruned *and* garbage-collected while being streamed can fail
    /// mid-read — a failed read, never corruption (see 0005 / 0006).
    pub fn open_version(&self, path: &DavPath, number: u64) -> Result<VersionReader, VfsError> {
        let node = self.resolve_file(path)?;
        let version = self.inner.meta.version_by_number(node.id, number)?;
        let manifest = self.inner.meta.load_version_manifest(version.id)?;
        Ok(VersionReader {
            size: version.size,
            chunks: self.inner.blobs.stream_chunks(manifest),
        })
    }

    /// Read the full content of a specific version (by 1-based number) of the
    /// file at `path` **into memory**.
    ///
    /// A convenience for in-memory callers; because it buffers, it is capped at
    /// [`MAX_IN_MEMORY_VERSION`] and returns [`VfsError::TooLarge`] above that.
    /// To serve a version of any size, stream it with [`open_version`] instead
    /// (that is what the HTTP `?version=N` path does).
    ///
    /// [`open_version`]: Self::open_version
    pub fn read_version(&self, path: &DavPath, number: u64) -> Result<Vec<u8>, VfsError> {
        let node = self.resolve_file(path)?;
        let version = self.inner.meta.version_by_number(node.id, number)?;
        let manifest = self.inner.meta.load_version_manifest(version.id)?;
        self.reconstruct(&manifest, version.size, MAX_IN_MEMORY_VERSION)
    }

    /// Read the file's *current* content into memory, rejecting content larger
    /// than `max_bytes` with [`VfsError::TooLarge`].
    ///
    /// The caller sets the cap so each use can bound memory appropriately — e.g.
    /// the browser Markdown renderer uses a small cap, since a document is tiny
    /// and this path is reachable by unauthenticated browser GETs.
    pub fn read_current(&self, path: &DavPath, max_bytes: u64) -> Result<Vec<u8>, VfsError> {
        let node = self.resolve_file(path)?;
        let manifest = self.inner.meta.load_manifest(node.id)?;
        self.reconstruct(&manifest, node.size, max_bytes)
    }

    /// Reconstruct a manifest's bytes into memory, rejecting content over `max_bytes`.
    fn reconstruct(
        &self,
        manifest: &Manifest,
        size: u64,
        max_bytes: u64,
    ) -> Result<Vec<u8>, VfsError> {
        if size > max_bytes {
            return Err(VfsError::TooLarge(size));
        }
        let mut reader = self.inner.blobs.open_file(manifest);
        let mut buf = Vec::with_capacity(size as usize);
        reader.read_to_end(&mut buf).map_err(VfsError::Io)?;
        Ok(buf)
    }

    /// Whether the node at `path` is a collection. Errors if `path` doesn't exist.
    ///
    /// Cheaper than [`list_dir`](Self::list_dir) (no child fetch); used to decide
    /// the trailing-slash redirect before listing.
    pub fn is_dir(&self, path: &DavPath) -> Result<bool, VfsError> {
        Ok(self.inner.meta.lookup_path(&segments(path))?.is_dir)
    }

    /// List the entries of the collection at `path`, ordered by name, for the
    /// browser directory index.
    ///
    /// Dot-prefixed entries (`.DS_Store`, AppleDouble `._*`, `.git`, …) are
    /// omitted — they're hidden clutter in a browser listing. WebDAV clients still
    /// see everything via `read_dir`/PROPFIND, so e.g. Finder keeps managing its
    /// own metadata files.
    pub fn list_dir(&self, path: &DavPath) -> Result<Vec<DirEntryInfo>, VfsError> {
        let node = self.inner.meta.lookup_path(&segments(path))?;
        if !node.is_dir {
            return Err(VfsError::Meta(MetaError::NotADirectory));
        }
        Ok(self
            .inner
            .meta
            .children(node.id)?
            .into_iter()
            .filter(|n| !n.name.starts_with(b"."))
            .map(|n| DirEntryInfo {
                name: String::from_utf8_lossy(&n.name).into_owned(),
                is_dir: n.is_dir,
                size: n.size,
                modified: n.modified,
            })
            .collect())
    }

    /// Revert the file at `path` to the content of version `number` by appending
    /// it as a new version.
    ///
    /// Non-destructive: history is preserved and the reverted content becomes the
    /// new current version. Cheap — the chunks are shared with the old version and
    /// nothing is copied. (Reverting to content identical to current is a no-op,
    /// since [`MetaStore::set_file_content`] deduplicates.)
    pub fn revert_to_version(&self, path: &DavPath, number: u64) -> Result<(), VfsError> {
        let node = self.resolve_file(path)?;
        let version_id = self.inner.meta.version_by_number(node.id, number)?.id;
        // Re-reference the old version's chunks under the GC guard (they could be
        // unique to that version, which a concurrent prune could strand).
        self.inner
            .reference_under_guard(node.id, |meta| meta.load_version_manifest(version_id))?;
        // Reverting changes the current content, so re-tokenize it.
        self.inner.reindex(node.id);
        Ok(())
    }

    /// Full-text search over indexed file content, returning up to `limit` hits.
    ///
    /// Results are scoped to the collection at `scope` (its subtree); pass the
    /// root (`/`) to search everything. Each hit's stable node id is resolved to
    /// its *current* path, so moved/renamed files show their up-to-date location
    /// and a hit whose file was since deleted is silently dropped (the index is
    /// lazily reconciled). Ordered by descending relevance.
    ///
    /// For a non-root scope, up to [`MAX_SEARCH_FETCH`] candidates are fetched
    /// from the index before filtering, so an in-scope match ranked below the
    /// global top-`limit` isn't lost to the cut; a match ranked beyond that pool
    /// can still be missed (acceptable at personal scale).
    pub fn search(
        &self,
        query: &str,
        limit: usize,
        scope: &DavPath,
    ) -> Result<Vec<SearchResult>, VfsError> {
        let scope_node = self.inner.meta.lookup_path(&segments(scope))?;
        // Resolve the scope to a path prefix once (root → None → matches all).
        let scope_prefix = if scope_node.id == ROOT_ID {
            None
        } else {
            self.inner.meta.path_of(scope_node.id)?
        };
        // When scoped, over-fetch so filtering doesn't starve on the global cut.
        let fetch = if scope_prefix.is_some() {
            MAX_SEARCH_FETCH
        } else {
            limit
        };
        let hits = self.inner.index.search(query, fetch)?;
        let mut results = Vec::with_capacity(hits.len().min(limit));
        for hit in hits {
            if results.len() >= limit {
                break;
            }
            // Resolve the stable id to its *current* path; a since-deleted node
            // resolves to `None` and is dropped (lazy index reconciliation).
            let Some(path) = self.inner.meta.path_of(hit.node_id as i64)? else {
                continue;
            };
            // Scope filter by path prefix — resolving first (above) means a stale
            // hit can't abort the whole search, and one walk serves both purposes.
            if let Some(prefix) = &scope_prefix
                && !is_within(&path, prefix)
            {
                continue;
            }
            results.push(SearchResult {
                path: String::from_utf8_lossy(&path).into_owned(),
                snippet: hit.snippet,
                score: hit.score,
            });
        }
        Ok(results)
    }

    /// Reclaim unreferenced chunk blobs (mark-and-sweep garbage collection).
    ///
    /// Deletes every blob whose hash is referenced by no version of any file —
    /// the storage that pruning a version or deleting a file leaves behind, plus
    /// any blob orphaned by a write that failed after storing chunks but before
    /// recording them. Returns a summary of what was scanned and freed.
    ///
    /// Concurrency-safe against writes: an exclusive [`Inner::gc_lock`] guard is
    /// held across collecting the live set and sweeping, so a blob can't be
    /// deleted in the window after a concurrent write stores or references it.
    /// This is an **in-process** guard — GC must run in the server process (e.g.
    /// via its admin endpoint), not a second process sharing the data directory
    /// while the server is live.
    ///
    /// The exclusive guard is held across the whole O(number of blobs) sweep, so
    /// content writes are blocked for its duration — acceptable for an
    /// operator-triggered, infrequent GC at personal scale; incremental GC that
    /// bounds the write-stall is a deferred refinement.
    ///
    /// A blob that can't be deleted (e.g. a transient I/O error) is counted in
    /// [`GcStats::blobs_failed`] and skipped rather than aborting the run. This is
    /// O(number of blobs) disk work; callers should run it off the async runtime
    /// (`spawn_blocking`).
    pub fn gc(&self) -> Result<GcStats, VfsError> {
        let _guard = self
            .inner
            .gc_lock
            .write()
            .unwrap_or_else(|e| e.into_inner());
        let live = self.inner.meta.referenced_chunk_hashes()?;
        let mut stats = GcStats::default();
        for hash in self.inner.blobs.list_hashes()? {
            stats.blobs_scanned += 1;
            if live.contains(&hash) {
                continue;
            }
            match self.inner.blobs.remove(&hash) {
                Ok(bytes) => {
                    stats.bytes_reclaimed += bytes;
                    stats.blobs_removed += 1;
                }
                // Skip and keep going so one bad blob can't stall reclamation.
                Err(e) => {
                    stats.blobs_failed += 1;
                    eprintln!("chishiki: gc failed to remove blob {}: {e}", hash.to_hex());
                }
            }
        }
        Ok(stats)
    }

    /// Delete a specific (non-current) historical version of the file at `path`.
    ///
    /// This frees version metadata only; the referenced chunk blobs are reclaimed
    /// the next time [`gc`](Self::gc) runs. See [`MetaStore::delete_version`].
    pub fn prune_version(&self, path: &DavPath, number: u64) -> Result<(), VfsError> {
        let node = self.resolve_file(path)?;
        self.inner.meta.delete_version(node.id, number)?;
        Ok(())
    }
}

/// One entry in a directory listing, as surfaced by [`DavFs::list_dir`].
#[derive(Debug, Clone)]
pub struct DirEntryInfo {
    /// Entry name (lossily decoded to UTF-8 for display).
    pub name: String,
    /// Whether the entry is a collection.
    pub is_dir: bool,
    /// File size in bytes (0 for collections).
    pub size: u64,
    /// Last-modified time.
    pub modified: SystemTime,
}

/// Upper bound on a historical version buffered in memory by the *convenience*
/// [`DavFs::read_version`] (256 MiB). The HTTP `?version=N` path streams via
/// [`DavFs::open_version`] and has no such cap.
pub const MAX_IN_MEMORY_VERSION: u64 = 256 * 1024 * 1024;

/// An owned, streaming reader over one historical file version.
///
/// Wraps a [`ChunkStream`] (which yields the version's chunks lazily, one owned
/// buffer at a time with no extra copy) and carries the version's total size for
/// a `Content-Length`. It is `Send + 'static`, so it can be moved into a blocking
/// task and streamed as an HTTP body without ever buffering the whole version.
/// Produced by [`DavFs::open_version`].
#[derive(Debug)]
pub struct VersionReader {
    size: u64,
    chunks: ChunkStream,
}

impl VersionReader {
    /// Total size of the version in bytes (the `Content-Length` to advertise).
    pub fn size(&self) -> u64 {
        self.size
    }

    /// The next chunk's bytes, or `None` once the whole version has been yielded.
    ///
    /// Does blocking blob I/O — call it from a blocking context. A per-chunk error
    /// (e.g. a blob concurrently garbage-collected mid-stream) is surfaced as
    /// `Some(Err(_))`, after which the caller should stop.
    pub fn next_chunk(&mut self) -> Option<std::io::Result<Vec<u8>>> {
        self.chunks.next_chunk()
    }
}

/// A single entry in a file's version history, as surfaced by
/// [`DavFs::list_versions`].
#[derive(Debug, Clone)]
pub struct VersionInfo {
    /// 1-based version number (1 = oldest).
    pub number: u64,
    /// Size in bytes of this version.
    pub size: u64,
    /// When this version was written.
    pub created: SystemTime,
    /// Whether this is the file's current content.
    pub is_current: bool,
}

/// Summary of a garbage-collection run, as returned by [`DavFs::gc`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GcStats {
    /// Total blobs examined on disk.
    pub blobs_scanned: u64,
    /// Blobs deleted because no version referenced them.
    pub blobs_removed: u64,
    /// Bytes freed by the deleted blobs.
    pub bytes_reclaimed: u64,
    /// Unreferenced blobs that could not be deleted (skipped; see the log).
    pub blobs_failed: u64,
}

/// One full-text search hit, as surfaced by [`DavFs::search`].
#[derive(Debug, Clone)]
pub struct SearchResult {
    /// Absolute virtual path of the matching file (e.g. `/docs/a.md`).
    pub path: String,
    /// A highlighted excerpt (matched terms in `<b>…</b>`, HTML-escaped), if one
    /// could be generated.
    pub snippet: Option<String>,
    /// Relevance score (higher is more relevant).
    pub score: f32,
}

impl DavFileSystem for DavFs {
    fn open<'a>(
        &'a self,
        path: &'a DavPath,
        options: OpenOptions,
    ) -> FsFuture<'a, Box<dyn DavFile>> {
        async move {
            let segs = segments(path);
            let Some((name, parent_segs)) = segs.split_last() else {
                // The root is a collection, not an openable file.
                return Err(FsError::Forbidden);
            };
            let parent = self
                .inner
                .meta
                .lookup_path(parent_segs)
                .map_err(meta_to_fs)?;
            if !parent.is_dir {
                return Err(FsError::Forbidden);
            }

            match self
                .inner
                .meta
                .lookup_child(parent.id, name)
                .map_err(meta_to_fs)?
            {
                Some(node) => {
                    if node.is_dir {
                        return Err(FsError::Forbidden);
                    }
                    if options.create_new {
                        return Err(FsError::Exists);
                    }
                    if options.write {
                        // Truncate starts empty; append/partial rewrite preserve
                        // the existing content in the staging file.
                        let existing = if options.truncate {
                            None
                        } else {
                            Some(self.inner.meta.load_manifest(node.id).map_err(meta_to_fs)?)
                        };
                        let handle = FileHandle::new_write(
                            self.inner.clone(),
                            &node,
                            options.append,
                            existing,
                            options.truncate,
                        )
                        .await
                        .map_err(io_to_fs)?;
                        Ok(Box::new(handle) as Box<dyn DavFile>)
                    } else {
                        let manifest =
                            self.inner.meta.load_manifest(node.id).map_err(meta_to_fs)?;
                        let handle = FileHandle::new_read(self.inner.clone(), &node, manifest);
                        Ok(Box::new(handle) as Box<dyn DavFile>)
                    }
                }
                None => {
                    if !options.create && !options.create_new {
                        return Err(FsError::NotFound);
                    }
                    let node = self
                        .inner
                        .meta
                        .create_file(parent.id, name)
                        .map_err(meta_to_fs)?;
                    let handle = FileHandle::new_write(
                        self.inner.clone(),
                        &node,
                        false,
                        None,
                        options.truncate,
                    )
                    .await
                    .map_err(io_to_fs)?;
                    Ok(Box::new(handle) as Box<dyn DavFile>)
                }
            }
        }
        .boxed()
    }

    fn metadata<'a>(&'a self, path: &'a DavPath) -> FsFuture<'a, Box<dyn DavMetaData>> {
        async move {
            let node = self.resolve(path)?;
            Ok(Box::new(Meta::from_node(&node)) as Box<dyn DavMetaData>)
        }
        .boxed()
    }

    fn read_dir<'a>(
        &'a self,
        path: &'a DavPath,
        _meta: ReadDirMeta,
    ) -> FsFuture<'a, FsStream<Box<dyn DavDirEntry>>> {
        async move {
            let node = self.resolve(path)?;
            if !node.is_dir {
                return Err(FsError::Forbidden);
            }
            let children = self.inner.meta.children(node.id).map_err(meta_to_fs)?;
            let entries: Vec<Box<dyn DavDirEntry>> = children
                .iter()
                .map(|n| Box::new(DirEntry::from_node(n)) as Box<dyn DavDirEntry>)
                .collect();
            let stream = stream::iter(entries).map(Ok);
            Ok(Box::pin(stream) as FsStream<Box<dyn DavDirEntry>>)
        }
        .boxed()
    }

    fn create_dir<'a>(&'a self, path: &'a DavPath) -> FsFuture<'a, ()> {
        async move {
            let segs = segments(path);
            let Some((name, parent_segs)) = segs.split_last() else {
                return Err(FsError::Forbidden);
            };
            let parent = self
                .inner
                .meta
                .lookup_path(parent_segs)
                .map_err(meta_to_fs)?;
            if !parent.is_dir {
                return Err(FsError::Forbidden);
            }
            self.inner
                .meta
                .create_dir(parent.id, name)
                .map_err(meta_to_fs)?;
            Ok(())
        }
        .boxed()
    }

    fn remove_dir<'a>(&'a self, path: &'a DavPath) -> FsFuture<'a, ()> {
        async move {
            let node = self.resolve(path)?;
            if node.id == ROOT_ID {
                return Err(FsError::Forbidden);
            }
            self.inner.meta.remove_dir(node.id).map_err(meta_to_fs)?;
            Ok(())
        }
        .boxed()
    }

    fn remove_file<'a>(&'a self, path: &'a DavPath) -> FsFuture<'a, ()> {
        async move {
            let node = self.resolve(path)?;
            self.inner.meta.remove_file(node.id).map_err(meta_to_fs)?;
            // Drop the file's search-index document (blocking commit off-thread).
            // Only a text-named file could have one, so a binary delete skips the
            // index entirely (no needless commit) — mirrors `try_reindex`.
            if is_indexable_name(&node.name) {
                let inner = self.inner.clone();
                let _ = tokio::task::spawn_blocking(move || inner.deindex(node.id)).await;
            }
            Ok(())
        }
        .boxed()
    }

    fn rename<'a>(&'a self, from: &'a DavPath, to: &'a DavPath) -> FsFuture<'a, ()> {
        async move {
            let src = self.resolve(from)?;
            if src.id == ROOT_ID {
                return Err(FsError::Forbidden);
            }
            let to_segs = segments(to);
            let Some((to_name, to_parent_segs)) = to_segs.split_last() else {
                return Err(FsError::Forbidden);
            };
            let to_parent = self
                .inner
                .meta
                .lookup_path(to_parent_segs)
                .map_err(meta_to_fs)?;
            if !to_parent.is_dir {
                return Err(FsError::Forbidden);
            }
            // Moving a collection into itself or a descendant would orphan the
            // subtree behind a parent cycle.
            if src.is_dir
                && self
                    .inner
                    .meta
                    .is_ancestor_or_self(src.id, to_parent.id)
                    .map_err(meta_to_fs)?
            {
                return Err(FsError::Forbidden);
            }
            // If the destination exists: replacing a file is allowed, a directory is not.
            if let Some(existing) = self
                .inner
                .meta
                .lookup_child(to_parent.id, to_name)
                .map_err(meta_to_fs)?
            {
                if existing.is_dir {
                    return Err(FsError::Exists);
                }
                // Overwriting a file deletes the destination node, so drop its
                // search-index document too — otherwise it orphans a doc that a
                // later rowid reuse could resurface as a phantom hit.
                let clobbered = is_indexable_name(&existing.name).then_some(existing.id);
                self.inner
                    .meta
                    .remove_file(existing.id)
                    .map_err(meta_to_fs)?;
                if let Some(id) = clobbered {
                    let inner = self.inner.clone();
                    let _ = tokio::task::spawn_blocking(move || inner.deindex(id)).await;
                }
            }
            self.inner
                .meta
                .rename(src.id, to_parent.id, to_name)
                .map_err(meta_to_fs)?;
            Ok(())
        }
        .boxed()
    }

    fn copy<'a>(&'a self, from: &'a DavPath, to: &'a DavPath) -> FsFuture<'a, ()> {
        async move {
            let src = self.resolve(from)?;
            let to_segs = segments(to);
            let Some((to_name, to_parent_segs)) = to_segs.split_last() else {
                return Err(FsError::Forbidden);
            };
            let to_parent = self
                .inner
                .meta
                .lookup_path(to_parent_segs)
                .map_err(meta_to_fs)?;
            if !to_parent.is_dir {
                return Err(FsError::Forbidden);
            }
            // Copying a collection into itself or a descendant would recurse
            // without bound as the freshly-created target reappears as a child.
            if src.is_dir
                && self
                    .inner
                    .meta
                    .is_ancestor_or_self(src.id, to_parent.id)
                    .map_err(meta_to_fs)?
            {
                return Err(FsError::Forbidden);
            }

            // A destination of a different node kind is a conflict.
            let existing = self
                .inner
                .meta
                .lookup_child(to_parent.id, to_name)
                .map_err(meta_to_fs)?;
            if let Some(ref e) = existing
                && e.is_dir != src.is_dir
            {
                return Err(FsError::Exists);
            }

            if src.is_dir {
                // Shallow copy: create the collection if absent (dav-server drives
                // recursion by walking children and issuing further copy calls).
                if existing.is_none() {
                    self.inner
                        .meta
                        .create_dir(to_parent.id, to_name)
                        .map_err(meta_to_fs)?;
                }
            } else {
                // Overwrite an existing destination file *in place* (reusing its
                // node) so its version history is preserved — consistent with a
                // PUT to the same path. Only a genuinely new file is created.
                //
                // NOTE: over HTTP this reuse path is not reached on overwrite,
                // because dav-server's COPY/MOVE handler deletes the destination
                // (cascading its versions) *before* calling us. So an HTTP COPY/MOVE
                // onto an existing file still loses that file's history today;
                // closing that gap needs the router to intercept COPY/MOVE (like the
                // version endpoints) or path-keyed history. This branch keeps the
                // fs-level operation correct for direct callers and future use.
                let dst_existed = existing.is_some();
                let dst_id = match existing {
                    Some(e) => e.id,
                    None => {
                        self.inner
                            .meta
                            .create_file(to_parent.id, to_name)
                            .map_err(meta_to_fs)?
                            .id
                    }
                };
                // Skip writing a version only when copying an unwritten (empty)
                // source onto a brand-new destination — both are then empty, and
                // we avoid creating a spurious empty version. Otherwise mirror the
                // source's current content (shared chunks make this cheap).
                if src.current_version_id.is_some() || dst_existed {
                    // Mirror the source's current chunks onto the destination under
                    // the GC guard (loading the manifest and recording the new
                    // reference as one guarded step, so a concurrent delete + GC
                    // can't sweep a chunk between them).
                    let src_id = src.id;
                    self.inner
                        .reference_under_guard(dst_id, |meta| meta.load_manifest(src_id))
                        .map_err(meta_to_fs)?;
                    // The destination gained content; index it (reads blobs, so
                    // run the blocking reindex off the async worker).
                    let inner = self.inner.clone();
                    let _ = tokio::task::spawn_blocking(move || inner.reindex(dst_id)).await;
                }
            }
            Ok(())
        }
        .boxed()
    }

    fn set_modified<'a>(
        &'a self,
        path: &'a DavPath,
        tm: std::time::SystemTime,
    ) -> FsFuture<'a, ()> {
        async move {
            let node = self.resolve(path)?;
            self.inner
                .meta
                .set_modified(node.id, tm)
                .map_err(meta_to_fs)?;
            Ok(())
        }
        .boxed()
    }

    fn set_accessed<'a>(
        &'a self,
        path: &'a DavPath,
        _tm: std::time::SystemTime,
    ) -> FsFuture<'a, ()> {
        // Access times are not tracked; accept and ignore so clients don't error.
        async move {
            self.resolve(path)?;
            Ok(())
        }
        .boxed()
    }
}

/// Split a `DavPath` into its non-empty name segments.
fn segments(path: &DavPath) -> Vec<&[u8]> {
    path.as_bytes()
        .split(|&c| c == b'/')
        .filter(|s| !s.is_empty())
        .collect()
}

/// Translate an `io::Error` into the closest `dav-server` status.
///
/// `dav-server` only provides `From<io::Error> for FsError` under its
/// `memfs`/`localfs` features, which we don't enable, so we map it ourselves.
pub(crate) fn io_to_fs(e: std::io::Error) -> FsError {
    use std::io::ErrorKind;
    match e.kind() {
        ErrorKind::NotFound => FsError::NotFound,
        ErrorKind::PermissionDenied => FsError::Forbidden,
        ErrorKind::AlreadyExists => FsError::Exists,
        _ => FsError::GeneralFailure,
    }
}

/// Translate a metadata-store error into the closest `dav-server` status.
pub(crate) fn meta_to_fs(e: MetaError) -> FsError {
    match e {
        MetaError::NotFound => FsError::NotFound,
        MetaError::Exists => FsError::Exists,
        MetaError::NotEmpty
        | MetaError::NotADirectory
        | MetaError::IsADirectory
        | MetaError::CurrentVersion => FsError::Forbidden,
        MetaError::Corrupt | MetaError::Sqlite(_) => FsError::GeneralFailure,
    }
}

/// Error from [`DavFs`] construction and the version APIs.
#[derive(Debug)]
pub enum VfsError {
    /// Filesystem I/O error while setting up the data directory or blob store.
    Io(std::io::Error),
    /// Metadata-store error.
    Meta(MetaError),
    /// Search-index error (opening, updating, or querying the reverse index).
    Index(index::IndexError),
    /// A historical version was too large to reconstruct in memory (its size in
    /// bytes); see [`DavFs::read_version`] and [`MAX_IN_MEMORY_VERSION`].
    TooLarge(u64),
}

impl std::fmt::Display for VfsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(e) => write!(f, "i/o error: {e}"),
            Self::Meta(e) => write!(f, "metadata error: {e}"),
            Self::Index(e) => write!(f, "search index error: {e}"),
            Self::TooLarge(n) => write!(f, "version too large to serve in memory: {n} bytes"),
        }
    }
}

impl VfsError {
    /// Whether this error is a "not found" (so the router can return 404).
    pub fn is_not_found(&self) -> bool {
        matches!(self, VfsError::Meta(MetaError::NotFound))
    }

    /// Whether this error is the caller naming an unsuitable target — e.g. asking
    /// for a version of a directory (so the router can return 400 rather than
    /// masking it as a 500 server fault).
    pub fn is_invalid_target(&self) -> bool {
        matches!(
            self,
            VfsError::Meta(MetaError::IsADirectory | MetaError::NotADirectory)
        )
    }

    /// Whether this error is a malformed user search query (so the router can
    /// return 400 rather than 500).
    pub fn is_bad_query(&self) -> bool {
        matches!(self, VfsError::Index(index::IndexError::BadQuery(_)))
    }
}

impl std::error::Error for VfsError {}

impl From<std::io::Error> for VfsError {
    fn from(e: std::io::Error) -> Self {
        VfsError::Io(e)
    }
}

impl From<MetaError> for VfsError {
    fn from(e: MetaError) -> Self {
        VfsError::Meta(e)
    }
}

impl From<index::IndexError> for VfsError {
    fn from(e: index::IndexError) -> Self {
        VfsError::Index(e)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use dav_server::davpath::DavPath;
    use futures_util::StreamExt;
    use tempfile::TempDir;

    fn dp(s: &str) -> DavPath {
        DavPath::new(s).unwrap()
    }

    fn temp_fs() -> (TempDir, DavFs) {
        let dir = tempfile::tempdir().unwrap();
        let fs = DavFs::open(dir.path()).unwrap();
        (dir, fs)
    }

    fn write_opts() -> OpenOptions {
        OpenOptions {
            write: true,
            create: true,
            truncate: true,
            ..Default::default()
        }
    }

    fn read_opts() -> OpenOptions {
        OpenOptions {
            read: true,
            ..Default::default()
        }
    }

    async fn write_file(fs: &DavFs, path: &str, data: &[u8]) {
        let mut f = fs.open(&dp(path), write_opts()).await.unwrap();
        f.write_bytes(Bytes::copy_from_slice(data)).await.unwrap();
        f.flush().await.unwrap();
    }

    async fn read_file(fs: &DavFs, path: &str) -> Vec<u8> {
        let mut f = fs.open(&dp(path), read_opts()).await.unwrap();
        let mut out = Vec::new();
        loop {
            let chunk = f.read_bytes(64 * 1024).await.unwrap();
            if chunk.is_empty() {
                break;
            }
            out.extend_from_slice(&chunk);
        }
        out
    }

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

    #[tokio::test]
    async fn put_then_get_small_file() {
        let (_dir, fs) = temp_fs();
        write_file(&fs, "/hello.md", b"# Hello, world").await;

        let meta = fs.metadata(&dp("/hello.md")).await.unwrap();
        assert!(meta.is_file());
        assert_eq!(meta.len(), 14);

        assert_eq!(read_file(&fs, "/hello.md").await, b"# Hello, world");
    }

    #[tokio::test]
    async fn put_then_get_large_file_roundtrips_through_chunking() {
        let (_dir, fs) = temp_fs();
        let data = pseudo_random(2_000_000, 1);
        write_file(&fs, "/video.bin", &data).await;

        assert_eq!(
            fs.metadata(&dp("/video.bin")).await.unwrap().len(),
            data.len() as u64
        );
        assert_eq!(read_file(&fs, "/video.bin").await, data);
    }

    #[tokio::test]
    async fn mkcol_then_list_directory() {
        let (_dir, fs) = temp_fs();
        fs.create_dir(&dp("/docs")).await.unwrap();
        write_file(&fs, "/docs/a.md", b"a").await;
        write_file(&fs, "/docs/b.md", b"b").await;

        let stream = fs.read_dir(&dp("/docs"), ReadDirMeta::None).await.unwrap();
        let mut names: Vec<Vec<u8>> = stream.map(|e| e.unwrap().name()).collect::<Vec<_>>().await;
        names.sort();
        assert_eq!(names, vec![b"a.md".to_vec(), b"b.md".to_vec()]);

        assert!(fs.metadata(&dp("/docs")).await.unwrap().is_dir());
    }

    #[tokio::test]
    async fn remove_file_makes_it_not_found() {
        let (_dir, fs) = temp_fs();
        write_file(&fs, "/x", b"data").await;
        fs.remove_file(&dp("/x")).await.unwrap();
        assert_eq!(fs.metadata(&dp("/x")).await.unwrap_err(), FsError::NotFound);
    }

    #[tokio::test]
    async fn rename_moves_content() {
        let (_dir, fs) = temp_fs();
        fs.create_dir(&dp("/a")).await.unwrap();
        fs.create_dir(&dp("/b")).await.unwrap();
        write_file(&fs, "/a/f", b"payload").await;

        fs.rename(&dp("/a/f"), &dp("/b/g")).await.unwrap();
        assert_eq!(
            fs.metadata(&dp("/a/f")).await.unwrap_err(),
            FsError::NotFound
        );
        assert_eq!(read_file(&fs, "/b/g").await, b"payload");
    }

    #[tokio::test]
    async fn copy_file_duplicates_content() {
        let (_dir, fs) = temp_fs();
        write_file(&fs, "/orig", b"shared bytes").await;
        fs.copy(&dp("/orig"), &dp("/dup")).await.unwrap();

        assert_eq!(read_file(&fs, "/orig").await, b"shared bytes");
        assert_eq!(read_file(&fs, "/dup").await, b"shared bytes");
    }

    #[tokio::test]
    async fn overwrite_replaces_content() {
        let (_dir, fs) = temp_fs();
        write_file(&fs, "/f", b"first version, longer").await;
        write_file(&fs, "/f", b"second").await;
        assert_eq!(read_file(&fs, "/f").await, b"second");
        assert_eq!(fs.metadata(&dp("/f")).await.unwrap().len(), 6);
    }

    #[tokio::test]
    async fn open_missing_without_create_is_not_found() {
        let (_dir, fs) = temp_fs();
        assert_eq!(
            fs.open(&dp("/nope"), read_opts()).await.unwrap_err(),
            FsError::NotFound
        );
    }

    #[tokio::test]
    async fn empty_truncating_put_clears_existing_content() {
        let (_dir, fs) = temp_fs();
        write_file(&fs, "/f", b"old content that must go").await;

        // A zero-body PUT: open write+truncate, write nothing, flush.
        let mut f = fs.open(&dp("/f"), write_opts()).await.unwrap();
        f.flush().await.unwrap();
        drop(f);

        assert!(read_file(&fs, "/f").await.is_empty());
        assert_eq!(fs.metadata(&dp("/f")).await.unwrap().len(), 0);
    }

    #[tokio::test]
    async fn move_into_own_descendant_is_forbidden() {
        let (_dir, fs) = temp_fs();
        fs.create_dir(&dp("/a")).await.unwrap();
        fs.create_dir(&dp("/a/b")).await.unwrap();

        assert_eq!(
            fs.rename(&dp("/a"), &dp("/a/b/a")).await.unwrap_err(),
            FsError::Forbidden
        );
        // The subtree is untouched and still reachable.
        assert!(fs.metadata(&dp("/a")).await.unwrap().is_dir());
        assert!(fs.metadata(&dp("/a/b")).await.unwrap().is_dir());
    }

    #[tokio::test]
    async fn copy_into_own_descendant_is_forbidden() {
        let (_dir, fs) = temp_fs();
        fs.create_dir(&dp("/a")).await.unwrap();
        fs.create_dir(&dp("/a/b")).await.unwrap();

        assert_eq!(
            fs.copy(&dp("/a"), &dp("/a/b/dup")).await.unwrap_err(),
            FsError::Forbidden
        );
    }

    #[tokio::test]
    async fn versions_accumulate_and_old_content_is_retrievable() {
        let (_dir, fs) = temp_fs();
        write_file(&fs, "/doc.md", b"# version one").await;
        write_file(&fs, "/doc.md", b"# version two, longer").await;

        let versions = fs.list_versions(&dp("/doc.md")).unwrap();
        assert_eq!(versions.len(), 2);
        assert_eq!(versions[0].number, 1);
        assert!(!versions[0].is_current);
        assert_eq!(versions[1].number, 2);
        assert!(versions[1].is_current);

        assert_eq!(
            fs.read_version(&dp("/doc.md"), 1).unwrap(),
            b"# version one"
        );
        assert_eq!(
            fs.read_version(&dp("/doc.md"), 2).unwrap(),
            b"# version two, longer"
        );
        // The GET path serves the current (latest) version.
        assert_eq!(read_file(&fs, "/doc.md").await, b"# version two, longer");
    }

    #[tokio::test]
    async fn versions_of_missing_or_unwritten_paths() {
        let (_dir, fs) = temp_fs();
        assert!(fs.list_versions(&dp("/nope")).unwrap_err().is_not_found());

        // A directory has no versions.
        fs.create_dir(&dp("/d")).await.unwrap();
        assert!(fs.list_versions(&dp("/d")).is_err());

        // Requesting a non-existent version number is not found.
        write_file(&fs, "/f", b"data").await;
        assert!(fs.read_version(&dp("/f"), 5).unwrap_err().is_not_found());
    }

    #[tokio::test]
    async fn open_version_streams_content_chunk_by_chunk() {
        let (_dir, fs) = temp_fs();
        // A ~1 MiB, non-constant body so it splits into several chunks and the
        // stream actually walks chunk-to-chunk (not one blob).
        let big: Vec<u8> = (0..1_000_000u32)
            .map(|i| (i.wrapping_mul(2_654_435_761) >> 13) as u8)
            .collect();
        write_file(&fs, "/big.bin", &big).await;
        // A second, smaller write so version 1 is historical, not current.
        write_file(&fs, "/big.bin", b"small current").await;

        let mut reader = fs.open_version(&dp("/big.bin"), 1).unwrap();
        assert_eq!(reader.size(), big.len() as u64);

        // Streaming every chunk reconstructs the exact bytes — with no in-memory
        // cap (this path has no MAX_IN_MEMORY_VERSION check) and more than one
        // chunk (so it exercises the multi-chunk walk).
        let mut out = Vec::new();
        let mut chunks = 0;
        while let Some(chunk) = reader.next_chunk() {
            out.extend_from_slice(&chunk.unwrap());
            chunks += 1;
        }
        assert_eq!(out, big);
        assert!(chunks > 1, "expected a multi-chunk version, got {chunks}");
    }

    #[tokio::test]
    async fn open_version_missing_is_not_found() {
        let (_dir, fs) = temp_fs();
        write_file(&fs, "/f", b"data").await;
        // Non-existent version number, and non-existent path.
        assert!(fs.open_version(&dp("/f"), 5).unwrap_err().is_not_found());
        assert!(fs.open_version(&dp("/nope"), 1).unwrap_err().is_not_found());
    }

    #[tokio::test]
    async fn identical_rewrite_creates_no_new_version() {
        let (_dir, fs) = temp_fs();
        write_file(&fs, "/f", b"same bytes").await;
        write_file(&fs, "/f", b"same bytes").await; // no-op
        write_file(&fs, "/f", b"same bytes").await; // no-op
        assert_eq!(fs.list_versions(&dp("/f")).unwrap().len(), 1);

        // A genuine change still appends.
        write_file(&fs, "/f", b"different bytes").await;
        assert_eq!(fs.list_versions(&dp("/f")).unwrap().len(), 2);
    }

    // NOTE: this drives `fs.copy` directly. Over HTTP, dav-server's COPY handler
    // deletes the destination before calling us, so an HTTP COPY onto an existing
    // file does NOT yet preserve history (see the note in `copy`). This test pins
    // the fs-level behavior, which a future router-level COPY interceptor would use.
    #[tokio::test]
    async fn fs_copy_over_existing_file_preserves_its_history() {
        let (_dir, fs) = temp_fs();
        write_file(&fs, "/dst", b"dst one").await;
        write_file(&fs, "/dst", b"dst two").await; // /dst has 2 versions
        write_file(&fs, "/src", b"src content").await;

        fs.copy(&dp("/src"), &dp("/dst")).await.unwrap();

        // /dst keeps its history and gains the copied content as the latest version.
        let versions = fs.list_versions(&dp("/dst")).unwrap();
        assert_eq!(versions.len(), 3);
        assert_eq!(fs.read_version(&dp("/dst"), 1).unwrap(), b"dst one");
        assert_eq!(fs.read_version(&dp("/dst"), 2).unwrap(), b"dst two");
        assert_eq!(read_file(&fs, "/dst").await, b"src content");
    }

    #[tokio::test]
    async fn read_current_returns_latest_content() {
        let (_dir, fs) = temp_fs();
        write_file(&fs, "/doc.md", b"# first").await;
        write_file(&fs, "/doc.md", b"# second (current)").await;
        assert_eq!(
            fs.read_current(&dp("/doc.md"), 1024).unwrap(),
            b"# second (current)"
        );

        // Content over the cap is rejected.
        assert!(matches!(
            fs.read_current(&dp("/doc.md"), 4),
            Err(VfsError::TooLarge(_))
        ));

        // A directory is not readable as a file.
        fs.create_dir(&dp("/d")).await.unwrap();
        assert!(fs.read_current(&dp("/d"), 1024).is_err());
    }

    #[tokio::test]
    async fn copy_of_unwritten_file_creates_no_spurious_version() {
        let (_dir, fs) = temp_fs();
        // Create a file but never write content (current version stays None).
        let opts = OpenOptions {
            write: true,
            create: true,
            ..Default::default()
        };
        fs.open(&dp("/empty"), opts)
            .await
            .unwrap()
            .flush()
            .await
            .unwrap();
        assert!(fs.list_versions(&dp("/empty")).unwrap().is_empty());

        fs.copy(&dp("/empty"), &dp("/empty_copy")).await.unwrap();
        assert!(fs.list_versions(&dp("/empty_copy")).unwrap().is_empty());
    }

    #[tokio::test]
    async fn list_dir_reports_entries() {
        let (_dir, fs) = temp_fs();
        fs.create_dir(&dp("/d")).await.unwrap();
        write_file(&fs, "/d/a.md", b"hello").await;
        fs.create_dir(&dp("/d/sub")).await.unwrap();

        let mut entries = fs.list_dir(&dp("/d")).unwrap();
        entries.sort_by(|a, b| a.name.cmp(&b.name));
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].name, "a.md");
        assert!(!entries[0].is_dir);
        assert_eq!(entries[0].size, 5);
        assert_eq!(entries[1].name, "sub");
        assert!(entries[1].is_dir);

        // Listing a file (not a collection) errors.
        assert!(fs.list_dir(&dp("/d/a.md")).is_err());
    }

    #[tokio::test]
    async fn list_dir_hides_dot_prefixed_entries() {
        let (_dir, fs) = temp_fs();
        write_file(&fs, "/visible.md", b"hi").await;
        write_file(&fs, "/.DS_Store", b"junk").await;
        write_file(&fs, "/._.DS_Store", b"junk").await;
        write_file(&fs, "/._visible.md", b"junk").await;
        fs.create_dir(&dp("/.hidden")).await.unwrap();

        // The browser listing shows only the non-dot entry.
        let entries = fs.list_dir(&dp("/")).unwrap();
        let names: Vec<&str> = entries.iter().map(|e| e.name.as_str()).collect();
        assert_eq!(names, vec!["visible.md"]);

        // But the hidden files still exist and are reachable (WebDAV visibility).
        assert!(fs.metadata(&dp("/.DS_Store")).await.is_ok());
    }

    #[tokio::test]
    async fn revert_appends_old_content_as_new_version() {
        let (_dir, fs) = temp_fs();
        write_file(&fs, "/f", b"one").await;
        write_file(&fs, "/f", b"two").await;
        write_file(&fs, "/f", b"three").await; // current = "three", 3 versions

        fs.revert_to_version(&dp("/f"), 1).unwrap(); // back to "one"

        let versions = fs.list_versions(&dp("/f")).unwrap();
        assert_eq!(versions.len(), 4); // history preserved, revert appended
        assert!(versions[3].is_current);
        assert_eq!(read_file(&fs, "/f").await, b"one");
        // The originals are still retrievable.
        assert_eq!(fs.read_version(&dp("/f"), 2).unwrap(), b"two");
    }

    #[tokio::test]
    async fn prune_deletes_noncurrent_but_refuses_current() {
        let (_dir, fs) = temp_fs();
        write_file(&fs, "/f", b"v1").await;
        write_file(&fs, "/f", b"v2").await;
        write_file(&fs, "/f", b"v3").await; // current = v3

        // Delete an old version.
        fs.prune_version(&dp("/f"), 1).unwrap();
        let numbers: Vec<u64> = fs
            .list_versions(&dp("/f"))
            .unwrap()
            .iter()
            .map(|v| v.number)
            .collect();
        assert_eq!(numbers, vec![2, 3]);

        // The current version can't be pruned; a missing one is not found.
        assert!(fs.prune_version(&dp("/f"), 3).is_err());
        assert!(fs.prune_version(&dp("/f"), 99).unwrap_err().is_not_found());
        // Current content is intact.
        assert_eq!(read_file(&fs, "/f").await, b"v3");
    }

    #[tokio::test]
    async fn search_finds_written_text_content() {
        let (_dir, fs) = temp_fs();
        write_file(
            &fs,
            "/notes.md",
            b"# Project chishiki\nA versioned webdav server.",
        )
        .await;
        write_file(&fs, "/other.md", b"unrelated grocery list").await;
        // Binary media is not indexed even with matching words in the name.
        write_file(&fs, "/chishiki.png", b"chishiki server webdav bytes").await;

        let hits = fs.search("webdav", 10, &dp("/")).unwrap();
        let paths: Vec<&str> = hits.iter().map(|h| h.path.as_str()).collect();
        assert_eq!(paths, vec!["/notes.md"]);

        // Conjunction semantics: all terms must match.
        assert!(fs.search("grocery list", 10, &dp("/")).unwrap().len() == 1);
        assert!(
            fs.search("grocery webdav", 10, &dp("/"))
                .unwrap()
                .is_empty()
        );

        // A hit carries a highlighted snippet.
        assert!(
            hits[0]
                .snippet
                .as_deref()
                .unwrap()
                .contains("<b>webdav</b>")
        );
    }

    #[tokio::test]
    async fn search_reflects_edits_moves_and_deletes() {
        let (_dir, fs) = temp_fs();
        fs.create_dir(&dp("/docs")).await.unwrap();
        write_file(&fs, "/docs/a.md", b"the aardvark forages at dawn").await;

        // Found at its original path.
        assert_eq!(
            fs.search("aardvark", 10, &dp("/")).unwrap()[0].path,
            "/docs/a.md"
        );

        // Move it: the stable node id means no reindex, and the hit shows the new path.
        fs.rename(&dp("/docs/a.md"), &dp("/moved.md"))
            .await
            .unwrap();
        assert_eq!(
            fs.search("aardvark", 10, &dp("/")).unwrap()[0].path,
            "/moved.md"
        );

        // Overwrite the content: the old term is gone, the new one is found.
        write_file(&fs, "/moved.md", b"now about badgers only").await;
        assert!(fs.search("aardvark", 10, &dp("/")).unwrap().is_empty());
        assert_eq!(fs.search("badgers", 10, &dp("/")).unwrap().len(), 1);

        // Delete it: dropped from results.
        fs.remove_file(&dp("/moved.md")).await.unwrap();
        assert!(fs.search("badgers", 10, &dp("/")).unwrap().is_empty());
    }

    #[test]
    fn is_indexable_name_excludes_dotfiles_and_binaries() {
        assert!(is_indexable_name(b"notes.md"));
        assert!(is_indexable_name(b"Config.TOML")); // case-insensitive
        // Dot-prefixed names (incl. macOS AppleDouble) are never indexed.
        assert!(!is_indexable_name(b".md"));
        assert!(!is_indexable_name(b".DS_Store"));
        assert!(!is_indexable_name(b"._notes.md"));
        // No extension / binary media.
        assert!(!is_indexable_name(b"README"));
        assert!(!is_indexable_name(b"photo.png"));
        assert!(!is_indexable_name(b"clip.mp4"));
    }

    #[test]
    fn is_within_respects_path_boundaries() {
        assert!(is_within(b"/docs/a.md", b"/docs"));
        assert!(is_within(b"/docs/sub/a.md", b"/docs"));
        // A sibling that merely shares a prefix is not inside.
        assert!(!is_within(b"/docsX/a.md", b"/docs"));
        // The scope directory itself is not a file within itself.
        assert!(!is_within(b"/docs", b"/docs"));
    }

    #[tokio::test]
    async fn dotfiles_are_not_searchable() {
        let (_dir, fs) = temp_fs();
        write_file(&fs, "/._notes.md", b"applesauce hidden appledouble junk").await;
        write_file(&fs, "/visible.md", b"applesauce visible content").await;
        // Only the non-dot file is indexed.
        let hits = fs.search("applesauce", 10, &dp("/")).unwrap();
        let paths: Vec<&str> = hits.iter().map(|h| h.path.as_str()).collect();
        assert_eq!(paths, vec!["/visible.md"]);
    }

    #[tokio::test]
    async fn rename_over_existing_file_deindexes_the_clobbered_doc() {
        let (_dir, fs) = temp_fs();
        write_file(&fs, "/keep.md", b"the wombat content").await;
        write_file(&fs, "/victim.md", b"the platypus content").await;
        assert_eq!(fs.search("platypus", 10, &dp("/")).unwrap().len(), 1);

        // Move keep.md onto victim.md: victim's node is deleted, so its index
        // document must go too (not linger to resurface via rowid reuse).
        fs.rename(&dp("/keep.md"), &dp("/victim.md")).await.unwrap();
        assert!(fs.search("platypus", 10, &dp("/")).unwrap().is_empty());
        // The moved content is still findable at its new path.
        assert_eq!(
            fs.search("wombat", 10, &dp("/")).unwrap()[0].path,
            "/victim.md"
        );
    }

    #[tokio::test]
    async fn scoped_search_survives_a_stale_index_hit() {
        // A directly-inserted index document for a node id that never existed
        // simulates a stale/orphaned hit. A scoped search must drop it, not abort.
        let (_dir, fs) = temp_fs();
        fs.create_dir(&dp("/scope")).await.unwrap();
        write_file(&fs, "/scope/real.md", b"zebra in scope").await;
        // Orphan doc: node id 999_999 has no row in `nodes`.
        fs.inner
            .index
            .index_document(999_999, "zebra orphaned ghost")
            .unwrap();
        fs.inner.index.commit().unwrap();

        // Scoped search still returns the real hit and silently drops the orphan.
        let hits = fs.search("zebra", 10, &dp("/scope")).unwrap();
        let paths: Vec<&str> = hits.iter().map(|h| h.path.as_str()).collect();
        assert_eq!(paths, vec!["/scope/real.md"]);
    }

    #[tokio::test]
    async fn gc_reclaims_only_unreferenced_blobs() {
        let (_dir, fs) = temp_fs();
        // Two versions with distinct content → distinct chunk blobs, both kept
        // (history references v1). Use content large enough to guarantee chunks.
        let v1 = pseudo_random(300_000, 10);
        let v2 = pseudo_random(300_000, 20);
        write_file(&fs, "/big.bin", &v1).await;
        write_file(&fs, "/big.bin", &v2).await;

        // Nothing is unreferenced yet: both versions are live.
        let before = tokio::task::spawn_blocking({
            let fs = fs.clone();
            move || fs.gc().unwrap()
        })
        .await
        .unwrap();
        assert_eq!(before.blobs_removed, 0);
        assert!(before.blobs_scanned > 0);

        // Prune v1: its unique chunks become unreferenced.
        fs.prune_version(&dp("/big.bin"), 1).unwrap();
        let after = fs.gc().unwrap();
        assert!(after.blobs_removed > 0);
        assert!(after.bytes_reclaimed > 0);

        // The current version is intact and fully reconstructable.
        assert_eq!(read_file(&fs, "/big.bin").await, v2);
        // A second GC has nothing left to do, and now scans fewer blobs than the
        // first run did (the pruned version's chunks are gone).
        let again = fs.gc().unwrap();
        assert_eq!(again.blobs_removed, 0);
        assert!(again.blobs_scanned < before.blobs_scanned);
    }

    #[tokio::test]
    async fn revert_then_gc_keeps_the_reverted_content() {
        let (_dir, fs) = temp_fs();
        let a = pseudo_random(300_000, 30);
        let b = pseudo_random(300_000, 40);
        write_file(&fs, "/f.bin", &a).await; // v1
        write_file(&fs, "/f.bin", &b).await; // v2 (current)

        // Revert to v1 (appends v3 re-referencing v1's chunks under the GC guard),
        // then prune v2 so its unique chunks become collectible.
        fs.revert_to_version(&dp("/f.bin"), 1).unwrap();
        fs.prune_version(&dp("/f.bin"), 2).unwrap();

        let stats = fs.gc().unwrap();
        assert!(stats.blobs_removed > 0); // v2's now-unreferenced chunks reclaimed
        assert_eq!(stats.blobs_failed, 0);
        // The reverted (current) content is intact — its chunks were not swept.
        assert_eq!(read_file(&fs, "/f.bin").await, a);
    }

    #[tokio::test]
    async fn gc_keeps_blobs_shared_by_another_file() {
        let (_dir, fs) = temp_fs();
        // Identical content dedups to shared blobs across the two files.
        write_file(&fs, "/a.txt", b"shared content that both files hold").await;
        fs.copy(&dp("/a.txt"), &dp("/b.txt")).await.unwrap();

        // Delete one file: its chunks are still referenced by the other, so GC
        // must not remove them.
        fs.remove_file(&dp("/a.txt")).await.unwrap();
        let stats = fs.gc().unwrap();
        assert_eq!(stats.blobs_removed, 0);
        // The surviving file still reads back correctly.
        assert_eq!(
            read_file(&fs, "/b.txt").await,
            b"shared content that both files hold"
        );

        // Deleting the last referencer frees the blobs.
        fs.remove_file(&dp("/b.txt")).await.unwrap();
        assert!(fs.gc().unwrap().blobs_removed > 0);
        assert_eq!(fs.gc().unwrap().bytes_reclaimed, 0); // idempotent afterward
    }

    #[tokio::test]
    async fn search_is_scoped_to_a_collection_subtree() {
        let (_dir, fs) = temp_fs();
        fs.create_dir(&dp("/a")).await.unwrap();
        fs.create_dir(&dp("/b")).await.unwrap();
        write_file(&fs, "/a/one.md", b"shared keyword apple").await;
        write_file(&fs, "/b/two.md", b"shared keyword banana").await;

        // Root scope sees both.
        assert_eq!(fs.search("shared", 10, &dp("/")).unwrap().len(), 2);
        // A subtree scope sees only its own.
        let in_a = fs.search("shared", 10, &dp("/a")).unwrap();
        assert_eq!(in_a.len(), 1);
        assert_eq!(in_a[0].path, "/a/one.md");
    }
}
