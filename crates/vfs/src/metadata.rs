//! `DavMetaData` and `DavDirEntry` adapters over our [`Node`] rows.

use std::time::SystemTime;

use dav_server::fs::{DavDirEntry, DavMetaData, FsFuture, FsResult};
use futures_util::future::FutureExt;

use crate::meta::Node;

/// File/collection metadata handed to `dav-server`.
///
/// Cloneable (required by `DavMetaData: DynClone`) and cheap to copy.
#[derive(Debug, Clone)]
pub(crate) struct Meta {
    len: u64,
    created: SystemTime,
    modified: SystemTime,
    is_dir: bool,
}

impl Meta {
    pub(crate) fn new(len: u64, created: SystemTime, modified: SystemTime, is_dir: bool) -> Self {
        Self {
            len,
            created,
            modified,
            is_dir,
        }
    }

    pub(crate) fn from_node(node: &Node) -> Self {
        Self {
            len: if node.is_dir { 0 } else { node.size },
            created: node.created,
            modified: node.modified,
            is_dir: node.is_dir,
        }
    }
}

impl DavMetaData for Meta {
    fn len(&self) -> u64 {
        self.len
    }

    fn modified(&self) -> FsResult<SystemTime> {
        Ok(self.modified)
    }

    fn created(&self) -> FsResult<SystemTime> {
        Ok(self.created)
    }

    fn is_dir(&self) -> bool {
        self.is_dir
    }
}

/// A single entry returned from `read_dir`.
#[derive(Debug)]
pub(crate) struct DirEntry {
    name: Vec<u8>,
    meta: Meta,
}

impl DirEntry {
    pub(crate) fn from_node(node: &Node) -> Self {
        Self {
            name: node.name.clone(),
            meta: Meta::from_node(node),
        }
    }
}

impl DavDirEntry for DirEntry {
    fn name(&self) -> Vec<u8> {
        self.name.clone()
    }

    fn metadata(&self) -> FsFuture<'_, Box<dyn DavMetaData>> {
        let meta = self.meta.clone();
        async move { Ok(Box::new(meta) as Box<dyn DavMetaData>) }.boxed()
    }

    // Overridden so listing a directory doesn't round-trip through metadata().
    fn is_dir(&self) -> FsFuture<'_, bool> {
        let is_dir = self.meta.is_dir;
        async move { Ok(is_dir) }.boxed()
    }
}
