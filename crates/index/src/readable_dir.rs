//! A tantivy [`Directory`] that keeps atomically-written files world-readable.
//!
//! tantivy's [`MmapDirectory`] implements `atomic_write` as "write a `tempfile`,
//! then rename it over the target". `tempfile` always creates at **0o600**
//! (ignoring the umask), so every rewrite of `meta.json` / `.managed.json` lands
//! owner-only — which breaks a backup process running as another user. Those
//! rewrites happen not just on our explicit commits but also on tantivy's own
//! background merge threads and when a fresh index is created, so widening the
//! files after [`commit`](crate::SearchIndex::commit) is not enough.
//!
//! [`ReadableDirectory`] wraps an [`MmapDirectory`], delegates everything, and
//! re-widens the target to **0o644** right after each `atomic_write`, which is
//! the single choke point for every such rewrite. (Files written via
//! `open_write` — the segment files — are created normally and already honor
//! the umask.) There is a microsecond window between the rename and the chmod
//! where the file is 0o600; that's harmless for a nightly backup.

use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use tantivy::directory::error::{DeleteError, LockError, OpenReadError, OpenWriteError};
use tantivy::directory::{
    Directory, DirectoryLock, FileHandle, Lock, MmapDirectory, WatchCallback, WatchHandle, WritePtr,
};

/// An [`MmapDirectory`] whose `atomic_write` leaves the file at mode 0o644.
#[derive(Clone, Debug)]
pub(crate) struct ReadableDirectory {
    inner: MmapDirectory,
    root: PathBuf,
}

impl ReadableDirectory {
    /// Open the directory at `root` (which must already exist).
    pub(crate) fn open(root: &Path) -> Result<Self, tantivy::TantivyError> {
        let inner = MmapDirectory::open(root).map_err(tantivy::TantivyError::from)?;
        Ok(Self {
            inner,
            root: root.to_path_buf(),
        })
    }
}

/// Widen `path` to 0o644. Called after a successful atomic write; a chmod
/// failure is surfaced, since the write just created the file and we own it.
#[cfg(unix)]
fn widen(path: &Path) -> io::Result<()> {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o644))
}

#[cfg(not(unix))]
fn widen(_path: &Path) -> io::Result<()> {
    Ok(())
}

impl Directory for ReadableDirectory {
    fn get_file_handle(&self, path: &Path) -> Result<Arc<dyn FileHandle>, OpenReadError> {
        self.inner.get_file_handle(path)
    }

    fn delete(&self, path: &Path) -> Result<(), DeleteError> {
        self.inner.delete(path)
    }

    fn exists(&self, path: &Path) -> Result<bool, OpenReadError> {
        self.inner.exists(path)
    }

    fn open_write(&self, path: &Path) -> Result<WritePtr, OpenWriteError> {
        self.inner.open_write(path)
    }

    fn atomic_read(&self, path: &Path) -> Result<Vec<u8>, OpenReadError> {
        self.inner.atomic_read(path)
    }

    fn atomic_write(&self, path: &Path, data: &[u8]) -> io::Result<()> {
        self.inner.atomic_write(path, data)?;
        widen(&self.root.join(path))
    }

    fn sync_directory(&self) -> io::Result<()> {
        self.inner.sync_directory()
    }

    // MmapDirectory overrides this (OS file locks); keep its behavior.
    fn acquire_lock(&self, lock: &Lock) -> Result<DirectoryLock, LockError> {
        self.inner.acquire_lock(lock)
    }

    fn watch(&self, watch_callback: WatchCallback) -> tantivy::Result<WatchHandle> {
        self.inner.watch(watch_callback)
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    #[test]
    fn atomic_write_lands_at_0o644() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = ReadableDirectory::open(tmp.path()).unwrap();
        dir.atomic_write(Path::new("meta.json"), b"{}").unwrap();
        // And again, since a rewrite replaces the inode with a fresh 0o600 file.
        dir.atomic_write(Path::new("meta.json"), b"{ }").unwrap();
        let mode = std::fs::metadata(tmp.path().join("meta.json"))
            .unwrap()
            .permissions()
            .mode();
        assert_eq!(mode & 0o777, 0o644);
        assert_eq!(dir.atomic_read(Path::new("meta.json")).unwrap(), b"{ }");
    }
}
