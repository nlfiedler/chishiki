//! WebDAV/HTTP server binary.
//!
//! A thin axum router in front of `dav-server`'s `DavHandler`, backed by our
//! [`vfs::DavFs`] (SQLite metadata + content-addressable blob store). Phase 2
//! stands up a working WebDAV class-1/2 server; browser content negotiation
//! (Phase 4), versioning surfaces (Phase 3), and SEARCH (Phase 5) are layered on
//! later. See `docs/specs/0001-initial-build-plan.md`.

use std::net::SocketAddr;

use axum::response::IntoResponse;
use axum::routing::any;
use axum::{Extension, Router};
use dav_server::DavHandler;
use dav_server::memls::MemLs;
use vfs::DavFs;

/// Directory that holds the blob store, metadata database, and upload staging.
const DEFAULT_DATA_DIR: &str = "./data";
/// Default listen address (4918 is the WebDAV port from RFC 4918).
const DEFAULT_ADDR: &str = "127.0.0.1:4918";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let data_dir = std::env::var("CHISHIKI_DATA").unwrap_or_else(|_| DEFAULT_DATA_DIR.to_string());
    let addr: SocketAddr = std::env::var("CHISHIKI_ADDR")
        .unwrap_or_else(|_| DEFAULT_ADDR.to_string())
        .parse()?;

    let fs = DavFs::open(&data_dir)?;
    let dav = DavHandler::builder()
        .filesystem(Box::new(fs))
        // In-process lock system; enough LOCK/UNLOCK support for macOS Finder
        // and Windows Explorer to mount read/write.
        .locksystem(MemLs::new())
        .build_handler();

    // The whole namespace is served at the root. `/` handles the root collection;
    // `/{*path}` (axum 0.8 wildcard syntax) handles everything below it.
    let app = Router::new()
        .route("/", any(handle))
        .route("/{*path}", any(handle))
        .layer(Extension(dav));

    let listener = tokio::net::TcpListener::bind(addr).await?;
    println!("chishiki webdav-server listening on http://{addr} (data dir: {data_dir})");
    axum::serve(listener, app).await?;
    Ok(())
}

/// Hand every request to the `DavHandler`, which dispatches by HTTP method.
async fn handle(
    Extension(dav): Extension<DavHandler>,
    req: axum::extract::Request,
) -> impl IntoResponse {
    dav.handle(req).await
}
