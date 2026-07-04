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

use blobstore::{BlobStore, Manifest};
use chunker::ChunkerConfig;
use dav_server::davpath::DavPath;
use dav_server::fs::{
    DavDirEntry, DavFile, DavFileSystem, DavMetaData, FsError, FsFuture, FsResult, FsStream,
    OpenOptions, ReadDirMeta,
};
use futures_util::future::FutureExt;
use futures_util::stream::{self, StreamExt};

use crate::file::FileHandle;
use crate::meta::{MetaError, MetaStore, Node, ROOT_ID};
use crate::metadata::{DirEntry, Meta};

/// Shared state behind the (cheaply cloneable) [`DavFs`] handle.
pub(crate) struct Inner {
    pub(crate) meta: MetaStore,
    pub(crate) blobs: BlobStore,
    pub(crate) chunker: ChunkerConfig,
    /// Directory for in-progress upload temp files.
    pub(crate) tmp_dir: PathBuf,
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
        let tmp_dir = data_dir.join("tmp");
        std::fs::create_dir_all(&tmp_dir)?;
        Ok(Self {
            inner: Arc::new(Inner {
                meta,
                blobs,
                chunker: ChunkerConfig::default(),
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

    /// Read the full content of a specific version (by 1-based number) of the
    /// file at `path`.
    ///
    /// The version is reconstructed **into memory**, so it is capped at
    /// [`MAX_IN_MEMORY_VERSION`]; larger historical versions return
    /// [`VfsError::TooLarge`].
    // TODO(phase-later): replace this with an owned streaming reader shared with
    // the live GET path (`FileHandle`), so historical versions of any size stream
    // chunk-by-chunk instead of being buffered.
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
}

/// Upper bound on a historical version served in memory by [`DavFs::read_version`]
/// (256 MiB). Guards against OOM until history streaming lands.
pub const MAX_IN_MEMORY_VERSION: u64 = 256 * 1024 * 1024;

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
                self.inner
                    .meta
                    .remove_file(existing.id)
                    .map_err(meta_to_fs)?;
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
                    let manifest = self.inner.meta.load_manifest(src.id).map_err(meta_to_fs)?;
                    self.inner
                        .meta
                        .set_file_content(dst_id, &manifest)
                        .map_err(meta_to_fs)?;
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
        MetaError::NotEmpty | MetaError::NotADirectory | MetaError::IsADirectory => {
            FsError::Forbidden
        }
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
    /// A historical version was too large to reconstruct in memory (its size in
    /// bytes); see [`DavFs::read_version`] and [`MAX_IN_MEMORY_VERSION`].
    TooLarge(u64),
}

impl std::fmt::Display for VfsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(e) => write!(f, "i/o error: {e}"),
            Self::Meta(e) => write!(f, "metadata error: {e}"),
            Self::TooLarge(n) => write!(f, "version too large to serve in memory: {n} bytes"),
        }
    }
}

impl VfsError {
    /// Whether this error is a "not found" (so the router can return 404).
    pub fn is_not_found(&self) -> bool {
        matches!(self, VfsError::Meta(MetaError::NotFound))
    }

    /// Whether this error is "too large" (so the router can return 413).
    pub fn is_too_large(&self) -> bool {
        matches!(self, VfsError::TooLarge(_))
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
}
