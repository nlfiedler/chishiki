//! WebDAV/HTTP server binary.
//!
//! A thin axum router in front of `dav-server`'s `DavHandler`, backed by our
//! [`vfs::DavFs`] (SQLite metadata + content-addressable blob store). Phase 2
//! stands up a working WebDAV class-1/2 server; Phase 3 adds auto-versioning with
//! a version-history HTTP surface here in the router (`dav-server` does not route
//! Delta-V methods). Phase 4 adds the browser layer: our own directory index,
//! per-file version pages with revert/prune, and Markdown rendering by content
//! negotiation. SEARCH (Phase 5) is layered on later. See
//! `docs/specs/0001-initial-build-plan.md` and `docs/specs/0002-web-interface.md`.
//!
//! Content mutations (upload/move/delete) remain WebDAV-only; the browser gets
//! read-only browsing plus the explicit version-management writes (revert/prune).

mod web;

use std::net::SocketAddr;
use std::time::{SystemTime, UNIX_EPOCH};

use axum::extract::Request;
use axum::http::{Method, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::any;
use axum::{Extension, Router};
use dav_server::DavHandler;
use dav_server::davpath::DavPath;
use dav_server::memls::MemLs;
use vfs::{DavFs, DirEntryInfo, VersionInfo};

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
        // The DavHandler owns one clone of the filesystem; the browser/version
        // endpoints share the same store via a second clone (see the Extension).
        .filesystem(Box::new(fs.clone()))
        // In-process lock system; enough LOCK/UNLOCK support for macOS Finder
        // and Windows Explorer to mount read/write.
        .locksystem(MemLs::new())
        // We serve our own version-aware directory index (see the router), so the
        // built-in autoindex stays off.
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
    // On Ctrl-C / SIGTERM, stop accepting connections and let in-flight requests
    // finish before exiting. Completed writes are already durable (fsync'd blobs +
    // committed SQLite transactions); this drains in-progress ones and lets temp
    // files be cleaned up.
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;
    Ok(())
}

/// Resolve once the process receives an interrupt (Ctrl-C) or, on Unix, `SIGTERM`.
async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl-C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        () = ctrl_c => {},
        () = terminate => {},
    }
    println!("\nshutting down; waiting for in-flight requests to finish…");
}

/// Dispatch a request. Browser `GET`s (directory index, version pages, rendered
/// Markdown) and version-mutating `POST`s are handled here; everything else —
/// including WebDAV methods and raw file `GET`s — is passed to the `DavHandler`.
async fn handle(
    Extension(dav): Extension<DavHandler>,
    Extension(fs): Extension<DavFs>,
    req: Request,
) -> Response {
    let method = req.method();
    if method == Method::GET {
        // Extract the request pieces up front so nothing borrowed from `req`
        // (which isn't `Sync`) is held across an `.await`.
        let accept_html = wants_html(&req);
        let query = req.uri().query().unwrap_or("").to_string();
        let path_str = req.uri().path().to_string();
        if let Some(resp) = handle_browser_get(&fs, &path_str, &query, accept_html).await {
            return resp;
        }
    } else if method == Method::POST
        && let Some(resp) = handle_post_action(&fs, &req)
    {
        return resp;
    }
    dav.handle(req).await.into_response()
}

/// Handle a browser `GET`. Returns `None` to fall through to the `DavHandler`
/// (raw file bytes, a 404, etc.).
async fn handle_browser_get(
    fs: &DavFs,
    path_str: &str,
    query: &str,
    accept_html: bool,
) -> Option<Response> {
    match parse_version_query(query) {
        VersionRequest::Bad => Some(bad_request("invalid version selector")),
        VersionRequest::List => Some(version_listing(fs, path_str, accept_html)),
        VersionRequest::Version(number) => Some(match DavPath::new(path_str) {
            Ok(path) => serve_version(fs.clone(), path, number).await,
            Err(_) => bad_request("bad path"),
        }),
        VersionRequest::None => {
            let path = DavPath::new(path_str).ok()?;
            match fs.is_dir(&path) {
                // A collection: our index is its only GET representation, so serve
                // it to every client (not just browsers). Redirect to a trailing
                // slash first so relative entry links resolve; the Location is
                // relative (the last path segment) to stay correct behind a proxy.
                Ok(true) => Some(if let Some(seg) = needs_trailing_slash(path_str) {
                    redirect(StatusCode::FOUND, &format!("{seg}/"))
                } else {
                    match fs.list_dir(&path) {
                        Ok(entries) => directory_response(&path, entries),
                        Err(_) => return None,
                    }
                }),
                // A file: render Markdown for browsers; otherwise fall through to
                // the DavHandler (raw bytes with content-type).
                Ok(false) if accept_html && is_markdown(path_str) => {
                    render_markdown(fs, path).await
                }
                // A file for a non-HTML client, or a missing path → let the
                // DavHandler serve the bytes / a 404.
                _ => None,
            }
        }
    }
}

/// Handle a version-mutating `POST` (`?revert=N` / `?prune=N`). Returns `None` for
/// a `POST` without a recognized action (falls through — `DavHandler` will 405).
fn handle_post_action(fs: &DavFs, req: &Request) -> Option<Response> {
    let action = parse_action_query(req.uri().query().unwrap_or(""))?;
    // These are state-changing writes with no auth; refuse a browser-initiated
    // cross-origin POST (CSRF). A same-origin form (the version page) matches;
    // a non-browser tool sends no Origin and is allowed.
    if is_cross_origin(req) {
        return Some((StatusCode::FORBIDDEN, "cross-origin request refused").into_response());
    }
    let path_str = req.uri().path();
    let Ok(path) = DavPath::new(path_str) else {
        return Some(bad_request("bad path"));
    };
    let result = match action {
        Action::Revert(n) => fs.revert_to_version(&path, n),
        Action::Prune(n) => fs.prune_version(&path, n),
        Action::Bad => return Some(bad_request("invalid action")),
    };
    Some(match result {
        // POST-redirect-GET back to the (now-updated) version page.
        Ok(()) => redirect(StatusCode::SEE_OTHER, &format!("{path_str}?versions")),
        Err(e) if e.is_not_found() => not_found(),
        // e.g. "cannot delete the current version".
        Err(e) => (StatusCode::BAD_REQUEST, e.to_string()).into_response(),
    })
}

/// `GET /path?versions` → the version history (HTML page for browsers, JSON otherwise).
fn version_listing(fs: &DavFs, path_str: &str, accept_html: bool) -> Response {
    let Ok(path) = DavPath::new(path_str) else {
        return bad_request("bad path");
    };
    match fs.list_versions(&path) {
        Ok(versions) if accept_html => {
            html_response(web::version_page(&basename(&path), &versions))
        }
        Ok(versions) => json_response(versions_to_json(&versions)),
        Err(e) if e.is_not_found() => not_found(),
        Err(_) => bad_request("not a versioned file"),
    }
}

/// `GET /dir/` → our HTML directory index.
fn directory_response(path: &DavPath, entries: Vec<DirEntryInfo>) -> Response {
    let display = {
        let d = String::from_utf8_lossy(path.as_bytes());
        if d.is_empty() {
            "/".to_string()
        } else {
            d.into_owned()
        }
    };
    // The root has no parent to link to.
    let has_parent = path.as_bytes().iter().any(|&b| b != b'/');
    html_response(web::directory_index(&display, has_parent, &entries))
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

/// Largest Markdown file we'll render in memory. A document is tiny; the cap
/// bounds memory on this unauthenticated, per-request path (independent of the
/// much larger version-history cap).
const MAX_MARKDOWN_BYTES: u64 = 8 * 1024 * 1024;

/// Render the current content of a Markdown file to an HTML page.
///
/// Returns `None` — so the caller falls through to the `DavHandler` (raw bytes /
/// 404) — when the path isn't a renderable file (missing or a read error). An
/// oversized file yields 413 and a render panic yields 500, rather than silently
/// serving raw bytes.
async fn render_markdown(fs: &DavFs, path: DavPath) -> Option<Response> {
    let title = basename(&path);
    let fs = fs.clone();
    // Reading blobs + rendering is CPU/IO work; keep it off the async worker.
    let rendered = tokio::task::spawn_blocking(move || {
        fs.read_current(&path, MAX_MARKDOWN_BYTES).map(|bytes| {
            let markdown = String::from_utf8_lossy(&bytes);
            web::markdown_page(&title, &markdown)
        })
    })
    .await;
    match rendered {
        Ok(Ok(html)) => Some(html_response(html)),
        Ok(Err(e)) if e.is_too_large() => Some(
            (
                StatusCode::PAYLOAD_TOO_LARGE,
                "markdown file too large to render",
            )
                .into_response(),
        ),
        // Missing file or an IO error → let the DavHandler serve it.
        Ok(Err(_)) => None,
        // The render task panicked.
        Err(_) => Some((StatusCode::INTERNAL_SERVER_ERROR, "render failed").into_response()),
    }
}

/// What a `GET`'s query string requests, version-wise.
enum VersionRequest {
    /// No version selector — continue with normal browser/DavHandler handling.
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

/// A version-mutating action from a `POST` query string.
enum Action {
    Revert(u64),
    Prune(u64),
    /// An action key was present but its value didn't parse.
    Bad,
}

/// Parse a `POST` query for `revert=N` / `prune=N`. `None` if neither is present.
fn parse_action_query(query: &str) -> Option<Action> {
    for pair in query.split('&').filter(|s| !s.is_empty()) {
        let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
        match key {
            "revert" => return Some(value.parse().map(Action::Revert).unwrap_or(Action::Bad)),
            "prune" => return Some(value.parse().map(Action::Prune).unwrap_or(Action::Bad)),
            _ => {}
        }
    }
    None
}

/// If `path_str` lacks a trailing slash, the last path segment to redirect to
/// (as a relative `Location`, so it stays correct behind a mount prefix); `None`
/// if it already ends with `/`.
fn needs_trailing_slash(path_str: &str) -> Option<&str> {
    if path_str.ends_with('/') {
        return None;
    }
    Some(path_str.rsplit('/').find(|s| !s.is_empty()).unwrap_or(""))
}

/// Whether a request is a browser cross-origin request (CSRF guard for POSTs).
///
/// Modern browsers send `Origin` on form POSTs; if present and its authority
/// differs from `Host`, the request came from another site. An absent `Origin`
/// (non-browser client) is treated as same-origin.
fn is_cross_origin(req: &Request) -> bool {
    let Some(origin) = req
        .headers()
        .get(header::ORIGIN)
        .and_then(|v| v.to_str().ok())
    else {
        return false;
    };
    let host = req
        .headers()
        .get(header::HOST)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    // Compare the origin's authority (after "scheme://") to the Host header.
    origin.split_once("://").map(|(_, auth)| auth) != Some(host)
}

/// Whether the client accepts HTML (i.e. is a browser, not a WebDAV client).
fn wants_html(req: &Request) -> bool {
    req.headers()
        .get(header::ACCEPT)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|accept| accept.contains("text/html"))
}

/// Whether a request path names a Markdown file.
fn is_markdown(path: &str) -> bool {
    let lower = path.to_ascii_lowercase();
    lower.ends_with(".md") || lower.ends_with(".markdown")
}

/// Last path segment (decoded), for page titles and links.
fn basename(path: &DavPath) -> String {
    let last = path
        .as_bytes()
        .split(|&b| b == b'/')
        .rfind(|s| !s.is_empty())
        .unwrap_or(b"");
    String::from_utf8_lossy(last).into_owned()
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

fn html_response(html: String) -> Response {
    ([(header::CONTENT_TYPE, "text/html; charset=utf-8")], html).into_response()
}

fn json_response(json: String) -> Response {
    ([(header::CONTENT_TYPE, "application/json")], json).into_response()
}

fn redirect(status: StatusCode, location: &str) -> Response {
    (status, [(header::LOCATION, location)]).into_response()
}

fn bad_request(msg: &'static str) -> Response {
    (StatusCode::BAD_REQUEST, msg).into_response()
}

fn not_found() -> Response {
    (StatusCode::NOT_FOUND, "not found").into_response()
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

    #[test]
    fn action_query_parsing() {
        assert!(matches!(
            parse_action_query("revert=2"),
            Some(Action::Revert(2))
        ));
        assert!(matches!(
            parse_action_query("prune=5"),
            Some(Action::Prune(5))
        ));
        assert!(matches!(parse_action_query("revert=x"), Some(Action::Bad)));
        assert!(parse_action_query("").is_none());
        assert!(parse_action_query("other=1").is_none());
    }

    #[test]
    fn trailing_slash_target() {
        assert_eq!(needs_trailing_slash("/"), None);
        assert_eq!(needs_trailing_slash("/dir/"), None);
        assert_eq!(needs_trailing_slash("/dir"), Some("dir"));
        assert_eq!(needs_trailing_slash("/a/b/c"), Some("c"));
    }

    #[test]
    fn cross_origin_detection() {
        fn req(origin: Option<&str>, host: Option<&str>) -> Request {
            let mut b = axum::http::Request::builder()
                .method("POST")
                .uri("/x?prune=1");
            if let Some(o) = origin {
                b = b.header("origin", o);
            }
            if let Some(h) = host {
                b = b.header("host", h);
            }
            b.body(axum::body::Body::empty()).unwrap()
        }
        // No Origin (non-browser tool) → treated as same-origin.
        assert!(!is_cross_origin(&req(None, Some("host:1"))));
        // Origin authority matches Host → same-origin.
        assert!(!is_cross_origin(&req(
            Some("http://host:1"),
            Some("host:1")
        )));
        // Different host, or an opaque "null" origin → cross-origin.
        assert!(is_cross_origin(&req(
            Some("http://evil.example"),
            Some("host:1")
        )));
        assert!(is_cross_origin(&req(Some("null"), Some("host:1"))));
    }
}
