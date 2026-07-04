//! WebDAV/HTTP server binary.
//!
//! A thin axum router in front of `dav-server`'s `DavHandler`, backed by our
//! [`vfs::DavFs`] (SQLite metadata + content-addressable blob store). Phase 2
//! stands up a working WebDAV class-1/2 server; Phase 3 adds auto-versioning with
//! a version-history HTTP surface (`?versions`, `?version=N`) here in the router,
//! since `dav-server` does not route Delta-V methods. Browser content negotiation
//! (Phase 4) and SEARCH (Phase 5) are layered on later. See
//! `docs/specs/0001-initial-build-plan.md`.

use std::net::SocketAddr;
use std::time::{SystemTime, UNIX_EPOCH};

use axum::http::{Method, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::any;
use axum::{Extension, Router};
use dav_server::DavHandler;
use dav_server::davpath::DavPath;
use dav_server::memls::MemLs;
use vfs::{DavFs, VersionInfo};

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
        // The DavHandler owns one clone of the filesystem; the version endpoints
        // below share the same store via a second clone (see the Extension).
        .filesystem(Box::new(fs.clone()))
        // In-process lock system; enough LOCK/UNLOCK support for macOS Finder
        // and Windows Explorer to mount read/write.
        .locksystem(MemLs::new())
        .build_handler();

    // The whole namespace is served at the root. `/` handles the root collection;
    // `/{*path}` (axum 0.8 wildcard syntax) handles everything below it.
    let app = Router::new()
        .route("/", any(handle))
        .route("/{*path}", any(handle))
        .layer(Extension(dav))
        .layer(Extension(fs));

    let listener = tokio::net::TcpListener::bind(addr).await?;
    println!("chishiki webdav-server listening on http://{addr} (data dir: {data_dir})");
    axum::serve(listener, app).await?;
    Ok(())
}

/// Dispatch a request.
///
/// A `GET` carrying `?versions` or `?version=N` is a version-history request
/// handled here; everything else is passed to the `DavHandler`, which dispatches
/// by WebDAV method.
async fn handle(
    Extension(dav): Extension<DavHandler>,
    Extension(fs): Extension<DavFs>,
    req: axum::extract::Request,
) -> Response {
    if req.method() == Method::GET {
        match parse_version_query(req.uri().query().unwrap_or("")) {
            VersionRequest::None => {}
            VersionRequest::Bad => {
                return (StatusCode::BAD_REQUEST, "invalid version selector").into_response();
            }
            // `DavPath::new` percent-decodes and normalizes the path the same way
            // the DavHandler does when storing, so names with spaces / non-ASCII
            // resolve identically here.
            VersionRequest::List => {
                return match DavPath::new(req.uri().path()) {
                    Ok(path) => list_versions(&fs, &path),
                    Err(_) => (StatusCode::BAD_REQUEST, "bad path").into_response(),
                };
            }
            VersionRequest::Version(number) => {
                return match DavPath::new(req.uri().path()) {
                    Ok(path) => serve_version(fs, path, number).await,
                    Err(_) => (StatusCode::BAD_REQUEST, "bad path").into_response(),
                };
            }
        }
    }
    dav.handle(req).await.into_response()
}

/// What a `GET`'s query string requests, version-wise.
enum VersionRequest {
    /// No version selector — pass through to the `DavHandler`.
    None,
    /// `?versions` — list the history.
    List,
    /// `?version=N` — a specific version.
    Version(u64),
    /// Malformed or contradictory (`?version=abc`, `?version=1&versions`).
    Bad,
}

/// Parse the query string for the version selectors. An unparseable `version=`
/// value, or both `versions` and `version=N` at once, is [`VersionRequest::Bad`]
/// (→ 400) rather than silently falling through to a normal GET.
fn parse_version_query(query: &str) -> VersionRequest {
    let mut list = false;
    let mut version: Option<Option<u64>> = None; // Some(None) = present but unparseable
    for pair in query.split('&').filter(|s| !s.is_empty()) {
        let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
        match key {
            "versions" => list = true,
            "version" => version = Some(value.parse::<u64>().ok()),
            _ => {}
        }
    }
    match (list, version) {
        (false, None) => VersionRequest::None,
        (true, None) => VersionRequest::List,
        (false, Some(Some(n))) => VersionRequest::Version(n),
        _ => VersionRequest::Bad,
    }
}

/// `GET /path?versions` → the file's version history as JSON.
fn list_versions(fs: &DavFs, path: &DavPath) -> Response {
    match fs.list_versions(path) {
        Ok(versions) => (
            [(header::CONTENT_TYPE, "application/json")],
            versions_to_json(&versions),
        )
            .into_response(),
        Err(e) if e.is_not_found() => (StatusCode::NOT_FOUND, "not found").into_response(),
        Err(_) => (StatusCode::BAD_REQUEST, "not a versioned file").into_response(),
    }
}

/// `GET /path?version=N` → the raw bytes of version N.
async fn serve_version(fs: DavFs, path: DavPath, number: u64) -> Response {
    // read_version reconstructs the whole version (blocking blob reads), so run
    // it off the async worker.
    match tokio::task::spawn_blocking(move || fs.read_version(&path, number)).await {
        Ok(Ok(bytes)) => {
            ([(header::CONTENT_TYPE, "application/octet-stream")], bytes).into_response()
        }
        Ok(Err(e)) if e.is_not_found() => {
            (StatusCode::NOT_FOUND, "version not found").into_response()
        }
        Ok(Err(e)) if e.is_too_large() => {
            (StatusCode::PAYLOAD_TOO_LARGE, "version too large to serve").into_response()
        }
        Ok(Err(_)) => (StatusCode::BAD_REQUEST, "not a versioned file").into_response(),
        Err(_) => (StatusCode::INTERNAL_SERVER_ERROR, "read failed").into_response(),
    }
}

/// Render version metadata as a JSON array. All fields are numeric/boolean, so no
/// string escaping is needed.
fn versions_to_json(versions: &[VersionInfo]) -> String {
    let items: Vec<String> = versions
        .iter()
        .map(|v| {
            format!(
                r#"{{"number":{},"size":{},"created":{},"current":{}}}"#,
                v.number,
                v.size,
                unix_millis(v.created),
                v.is_current
            )
        })
        .collect();
    format!("[{}]", items.join(","))
}

fn unix_millis(t: SystemTime) -> i64 {
    t.duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn version_query_classification() {
        use VersionRequest::*;
        assert!(matches!(parse_version_query(""), None));
        assert!(matches!(parse_version_query("v=1&cache=2"), None));
        assert!(matches!(parse_version_query("versions"), List));
        assert!(matches!(parse_version_query("versions&x=1"), List));
        assert!(matches!(parse_version_query("version=3"), Version(3)));
        // Malformed or contradictory selectors must be rejected, not ignored.
        assert!(matches!(parse_version_query("version=abc"), Bad));
        assert!(matches!(parse_version_query("version=-1"), Bad));
        assert!(matches!(parse_version_query("version="), Bad));
        assert!(matches!(parse_version_query("version=1&versions"), Bad));
    }
}
