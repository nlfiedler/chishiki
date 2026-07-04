//! Virtualized filesystem.
//!
//! Maps the virtual path namespace clients see onto file manifests in the blob
//! store, holds metadata (SQLite via `rusqlite`), and implements the `dav-server`
//! filesystem traits. See `docs/specs/0001-initial-build-plan.md`.
//!
//! [`DavFs`] is the entry point: open it on a data directory and hand it to a
//! `dav-server` `DavHandler`.

mod davfs;
mod file;
pub mod meta;
mod metadata;

pub use davfs::{DavFs, DirEntryInfo, MAX_IN_MEMORY_VERSION, VersionInfo, VfsError};
pub use meta::{MetaError, MetaStore, Node, ROOT_ID, Version};
