//! WebDAV/HTTP server binary.
//!
//! Thin consumer of the storage-engine crates (`blobstore`, `chunker`, `vfs`,
//! `index`). The real server — an axum router in front of `dav-server`'s
//! `DavHandler` — is stood up from Phase 2 onward
//! (see `docs/specs/0001-initial-build-plan.md`).

fn main() {
    println!(
        "chishiki webdav-server: not yet implemented (see docs/specs/0001-initial-build-plan.md)"
    );
}
