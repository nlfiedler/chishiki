//! The [`DavFileSystem`] implementation over the metadata store + blob store.
//!
//! Every method follows the same shape the `dav-server` traits require: an
//! `async move { ... }.boxed()` future that does its (synchronous) metadata and
//! blob work without ever holding a lock across an `.await`, so the returned
//! future stays `Send`.

use std::path::PathBuf;
use std::sync::Arc;

use blobstore::BlobStore;
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

            // Remove an existing destination of the same node kind first.
            if let Some(existing) = self
                .inner
                .meta
                .lookup_child(to_parent.id, to_name)
                .map_err(meta_to_fs)?
            {
                match (existing.is_dir, src.is_dir) {
                    (false, false) => self
                        .inner
                        .meta
                        .remove_file(existing.id)
                        .map_err(meta_to_fs)?,
                    (true, true) => {} // copy into the existing collection
                    _ => return Err(FsError::Exists),
                }
            }

            if src.is_dir {
                // Shallow copy: create the collection. dav-server drives recursion
                // by walking children and issuing further copy/create_dir calls.
                if self
                    .inner
                    .meta
                    .lookup_child(to_parent.id, to_name)
                    .map_err(meta_to_fs)?
                    .is_none()
                {
                    self.inner
                        .meta
                        .create_dir(to_parent.id, to_name)
                        .map_err(meta_to_fs)?;
                }
            } else {
                // Copying a file just points a new node at the same (shared) chunks.
                let manifest = self.inner.meta.load_manifest(src.id).map_err(meta_to_fs)?;
                let dst = self
                    .inner
                    .meta
                    .create_file(to_parent.id, to_name)
                    .map_err(meta_to_fs)?;
                self.inner
                    .meta
                    .set_file_content(dst.id, &manifest)
                    .map_err(meta_to_fs)?;
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

/// Error constructing a [`DavFs`].
#[derive(Debug)]
pub enum VfsError {
    /// Filesystem I/O error while setting up the data directory or blob store.
    Io(std::io::Error),
    /// Metadata-store error.
    Meta(MetaError),
}

impl std::fmt::Display for VfsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(e) => write!(f, "i/o error: {e}"),
            Self::Meta(e) => write!(f, "metadata error: {e}"),
        }
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
}
