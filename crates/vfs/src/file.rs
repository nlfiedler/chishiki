//! The `DavFile` implementation: an open file handle.
//!
//! A read handle streams bytes by reconstructing the file from its manifest in
//! the blob store, caching the current chunk so sequential reads don't re-read
//! it. A write handle buffers writes into a temporary file and, on `flush`,
//! chunks that file into the blob store and updates the node's manifest.
//! Buffering to a temp file (rather than memory) keeps large uploads — e.g.
//! videos — off the heap.
//!
//! The two file-size-proportional operations — reconstructing prior content on a
//! non-truncating open, and chunking the temp file on flush — run on a blocking
//! thread (`spawn_blocking`) so they don't stall the async runtime.

use std::cmp::Ordering;
use std::fmt;
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::sync::Arc;
use std::time::SystemTime;

use blobstore::Manifest;
use bytes::{Buf, Bytes};
use dav_server::fs::{DavFile, DavMetaData, FsError, FsFuture};
use futures_util::future::FutureExt;
use tempfile::NamedTempFile;

use crate::davfs::{Inner, io_to_fs, meta_to_fs};
use crate::meta::Node;
use crate::metadata::Meta;

/// An open file handle, either reading or writing.
pub(crate) struct FileHandle {
    inner: Arc<Inner>,
    node_id: i64,
    created: SystemTime,
    modified: SystemTime,
    /// Current byte position within the file.
    pos: u64,
    state: State,
}

enum State {
    Read {
        manifest: Manifest,
        /// The most recently loaded chunk `(index, bytes)`, kept so sequential
        /// reads within a chunk (or continuing into it) avoid re-reading it.
        cache: Option<(usize, Vec<u8>)>,
    },
    Write {
        temp: NamedTempFile,
        /// Logical length of the content written so far.
        len: u64,
        append: bool,
        /// Whether there are unwritten changes to commit on flush.
        dirty: bool,
    },
}

impl fmt::Debug for FileHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mode = match self.state {
            State::Read { .. } => "read",
            State::Write { .. } => "write",
        };
        f.debug_struct("FileHandle")
            .field("node_id", &self.node_id)
            .field("mode", &mode)
            .field("pos", &self.pos)
            .finish()
    }
}

impl FileHandle {
    /// Open a handle for reading `node`'s content described by `manifest`.
    pub(crate) fn new_read(inner: Arc<Inner>, node: &Node, manifest: Manifest) -> Self {
        Self {
            inner,
            node_id: node.id,
            created: node.created,
            modified: node.modified,
            pos: 0,
            state: State::Read {
                manifest,
                cache: None,
            },
        }
    }

    /// Open a handle for writing `node`.
    ///
    /// If `existing` is `Some`, the current content is reconstructed into the
    /// temp file first (needed for append or partial-overwrite opens); otherwise
    /// the file starts empty. `truncate` seeds the dirty flag so that a
    /// truncating open commits an (empty) manifest even if nothing is written —
    /// otherwise an empty-body PUT over an existing file would leave stale bytes.
    pub(crate) async fn new_write(
        inner: Arc<Inner>,
        node: &Node,
        append: bool,
        existing: Option<Manifest>,
        truncate: bool,
    ) -> io::Result<Self> {
        let node_id = node.id;
        let created = node.created;
        let modified = node.modified;
        let tmp_dir = inner.tmp_dir.clone();
        let blobs = inner.blobs.clone();

        // Reconstructing prior content is proportional to file size; do it off
        // the async worker.
        let (temp, len) =
            tokio::task::spawn_blocking(move || -> io::Result<(NamedTempFile, u64)> {
                let mut temp = NamedTempFile::new_in(&tmp_dir)?;
                let mut len = 0u64;
                if let Some(manifest) = existing {
                    let mut reader = blobs.open_file(&manifest);
                    len = io::copy(&mut reader, temp.as_file_mut())?;
                }
                Ok((temp, len))
            })
            .await
            .map_err(io::Error::other)??;

        let pos = if append { len } else { 0 };
        Ok(Self {
            inner,
            node_id,
            created,
            modified,
            pos,
            state: State::Write {
                temp,
                len,
                append,
                dirty: truncate,
            },
        })
    }

    fn len(&self) -> u64 {
        match &self.state {
            State::Read { manifest, .. } => manifest.total_size,
            State::Write { len, .. } => *len,
        }
    }
}

impl DavFile for FileHandle {
    fn metadata(&mut self) -> FsFuture<'_, Box<dyn DavMetaData>> {
        let meta = Meta::new(self.len(), self.created, self.modified, false);
        async move { Ok(Box::new(meta) as Box<dyn DavMetaData>) }.boxed()
    }

    fn read_bytes(&mut self, count: usize) -> FsFuture<'_, Bytes> {
        async move {
            let len = self.len();
            if self.pos >= len {
                return Ok(Bytes::new());
            }
            let want = (len - self.pos).min(count as u64) as usize;
            let start = self.pos;
            let mut out = vec![0u8; want];
            match &mut self.state {
                State::Read { manifest, cache } => {
                    let mut filled = 0;
                    while filled < want {
                        let abs = start + filled as u64;
                        let idx = manifest
                            .chunks
                            .binary_search_by(|c| {
                                if c.offset + u64::from(c.length) <= abs {
                                    Ordering::Less
                                } else if c.offset > abs {
                                    Ordering::Greater
                                } else {
                                    Ordering::Equal
                                }
                            })
                            .map_err(|_| FsError::GeneralFailure)?;
                        // Load the containing chunk unless it's already cached.
                        if cache.as_ref().map(|(i, _)| *i) != Some(idx) {
                            let data = self
                                .inner
                                .blobs
                                .get(&manifest.chunks[idx].hash)
                                .map_err(io_to_fs)?;
                            *cache = Some((idx, data));
                        }
                        let (_, data) = cache.as_ref().unwrap();
                        let within = (abs - manifest.chunks[idx].offset) as usize;
                        let take = (data.len() - within).min(want - filled);
                        out[filled..filled + take].copy_from_slice(&data[within..within + take]);
                        filled += take;
                    }
                }
                State::Write { temp, .. } => {
                    let mut file: &std::fs::File = temp.as_file();
                    file.seek(SeekFrom::Start(start)).map_err(io_to_fs)?;
                    file.read_exact(&mut out).map_err(io_to_fs)?;
                }
            }
            self.pos += want as u64;
            Ok(Bytes::from(out))
        }
        .boxed()
    }

    fn write_bytes(&mut self, buf: Bytes) -> FsFuture<'_, ()> {
        async move {
            let mut pos = self.pos;
            match &mut self.state {
                State::Read { .. } => return Err(FsError::Forbidden),
                State::Write {
                    temp,
                    len,
                    append,
                    dirty,
                } => {
                    if *append {
                        pos = *len;
                    }
                    let file = temp.as_file_mut();
                    file.seek(SeekFrom::Start(pos)).map_err(io_to_fs)?;
                    file.write_all(&buf).map_err(io_to_fs)?;
                    pos += buf.len() as u64;
                    *len = (*len).max(pos);
                    *dirty = true;
                }
            }
            self.pos = pos;
            Ok(())
        }
        .boxed()
    }

    fn write_buf(&mut self, mut buf: Box<dyn Buf + Send>) -> FsFuture<'_, ()> {
        async move {
            let mut pos = self.pos;
            match &mut self.state {
                State::Read { .. } => return Err(FsError::Forbidden),
                State::Write {
                    temp,
                    len,
                    append,
                    dirty,
                } => {
                    if *append {
                        pos = *len;
                    }
                    let file = temp.as_file_mut();
                    file.seek(SeekFrom::Start(pos)).map_err(io_to_fs)?;
                    while buf.has_remaining() {
                        let chunk = buf.chunk();
                        file.write_all(chunk).map_err(io_to_fs)?;
                        pos += chunk.len() as u64;
                        buf.advance(chunk.len());
                    }
                    *len = (*len).max(pos);
                    *dirty = true;
                }
            }
            self.pos = pos;
            Ok(())
        }
        .boxed()
    }

    fn seek(&mut self, pos: SeekFrom) -> FsFuture<'_, u64> {
        async move {
            let target = match pos {
                SeekFrom::Start(n) => i128::from(n),
                SeekFrom::Current(n) => i128::from(self.pos) + i128::from(n),
                SeekFrom::End(n) => i128::from(self.len()) + i128::from(n),
            };
            if target < 0 {
                return Err(io_to_fs(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "negative seek",
                )));
            }
            self.pos = target as u64;
            Ok(self.pos)
        }
        .boxed()
    }

    fn flush(&mut self) -> FsFuture<'_, ()> {
        async move {
            let mut committed = None;
            if let State::Write {
                temp, len, dirty, ..
            } = &mut self.state
                && *dirty
            {
                let file = temp.as_file_mut();
                file.flush().map_err(io_to_fs)?;
                // Trim any bytes past the logical length (e.g. after a truncating
                // rewrite that seeked backwards) before chunking.
                file.set_len(*len).map_err(io_to_fs)?;
                let path = temp.path().to_path_buf();
                let inner = self.inner.clone();
                let node_id = self.node_id;
                // Chunking the whole file into the blob store is proportional to
                // file size; run it off the async worker.
                let modified =
                    tokio::task::spawn_blocking(move || -> Result<SystemTime, FsError> {
                        // Store the new blobs and record them under a shared GC
                        // guard, so garbage collection can't delete a freshly
                        // written blob in the window before it's referenced.
                        let modified = {
                            let _gc = inner.gc_lock.read().unwrap_or_else(|e| e.into_inner());
                            let file = std::fs::File::open(&path).map_err(io_to_fs)?;
                            let manifest = inner
                                .blobs
                                .store_file(&file, inner.chunker)
                                .map_err(io_to_fs)?;
                            inner
                                .meta
                                .set_file_content(node_id, &manifest)
                                .map_err(meta_to_fs)?
                        };
                        // Update the reverse index with the new content (still on
                        // this blocking thread). Best-effort; logged on failure.
                        // The content is now referenced, so no GC guard is needed.
                        inner.reindex(node_id);
                        Ok(modified)
                    })
                    .await
                    .map_err(|_| FsError::GeneralFailure)??;
                *dirty = false;
                committed = Some(modified);
            }
            // Keep our reported mtime consistent with what was just stored, so the
            // ETag/Last-Modified in the PUT response matches a later GET/PROPFIND.
            if let Some(modified) = committed {
                self.modified = modified;
            }
            Ok(())
        }
        .boxed()
    }
}
