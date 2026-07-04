//! WebDAV/HTTP server binary.
//!
//! A thin axum router in front of `dav-server`'s `DavHandler`, backed by our
//! [`vfs::DavFs`] (SQLite metadata + content-addressable blob store). Phase 2
//! stands up a working WebDAV class-1/2 server; Phase 3 adds auto-versioning with
//! a version-history HTTP surface here in the router (`dav-server` does not route
//! Delta-V methods). Phase 4 adds the browser layer: a two-pane web UI (sidebar +
//! main pane, styled with Bulma) with directory browsing, file rendering (Markdown
//! /image/video via the `?raw` view/bytes split), and per-file version pages with
//! revert/prune. SEARCH (Phase 5) is layered on later. See the specs under
//! `docs/specs/` (0001 build plan, 0002 web interface, 0003 web UI).
//!
//! Content mutations (upload/move/delete) remain WebDAV-only; the browser gets
//! read-only browsing plus the explicit version-management writes (revert/prune).

mod web;

use std::net::SocketAddr;
use std::time::{SystemTime, UNIX_EPOCH};

use axum::extract::Request;
use axum::http::{Method, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{any, get};
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
        // Reserved prefix for embedded server assets (takes precedence over the
        // namespace wildcard). Shadows a real node literally named `_assets`.
        .route("/_assets/bulma.css", get(serve_bulma))
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

/// Serve the embedded Bulma stylesheet (cached aggressively — it never changes).
async fn serve_bulma() -> Response {
    (
        [
            (header::CONTENT_TYPE, "text/css; charset=utf-8"),
            (header::CACHE_CONTROL, "public, max-age=31536000, immutable"),
        ],
        web::BULMA_CSS,
    )
        .into_response()
}

/// Dispatch a request. Browser `GET`s (directory / file / version pages) and
/// version-mutating `POST`s are handled here; everything else — including WebDAV
/// methods and raw file `GET`s — is passed to the `DavHandler`.
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
    // `?raw` serves the underlying bytes (embedded media / downloads) — let the
    // DavHandler stream them, with content-type and range support.
    if has_raw(query) {
        return None;
    }
    match parse_version_query(query) {
        VersionRequest::Bad => Some(bad_request("invalid version selector")),
        VersionRequest::List => Some(version_page_response(fs, path_str, accept_html).await),
        VersionRequest::Version(number) => Some(match DavPath::new(path_str) {
            Ok(path) => serve_version(fs.clone(), path, number).await,
            Err(_) => bad_request("bad path"),
        }),
        VersionRequest::None => {
            let path = DavPath::new(path_str).ok()?;
            match fs.is_dir(&path) {
                // A collection: its page is the only GET representation, so serve
                // it to any client. Redirect to a trailing slash first so relative
                // asset/links resolve; the Location is relative to stay correct
                // behind a mount prefix.
                Ok(true) => Some(match needs_trailing_slash(path_str) {
                    Some(seg) => redirect(StatusCode::FOUND, &format!("{seg}/")),
                    None => directory_page(fs, &path, path_str).await,
                }),
                // A file: the browser gets a view page; other clients fall through
                // to the DavHandler for raw bytes.
                Ok(false) if accept_html => Some(file_page(fs, &path, path_str).await),
                _ => None,
            }
        }
    }
}

/// `GET /dir/` → the directory page: sidebar plus a rendered README (if present)
/// or an index table in the main pane.
async fn directory_page(fs: &DavFs, path: &DavPath, path_str: &str) -> Response {
    let entries = match fs.list_dir(path) {
        Ok(entries) => entries,
        Err(_) => return not_found(),
    };
    let dir_segments = decoded_segments(path);
    let readme_html = match entries.iter().find(|e| !e.is_dir && is_readme(&e.name)) {
        Some(entry) => match child_path(path_str, &entry.name) {
            Some(readme) => read_inline(fs, readme, web::FileKind::Markdown).await,
            None => None,
        },
        None => None,
    };
    let display = display_path(path);
    let sidebar = web::sidebar(&dir_segments, &entries, None);
    let main = web::dir_main(&display, readme_html.as_deref(), &dir_segments, &entries);
    html_response(web::page(&display, &sidebar, &main))
}

/// `GET /file` (browser) → the file view page: sidebar of the parent directory
/// plus the file's content (rendered Markdown / embedded media / download).
async fn file_page(fs: &DavFs, path: &DavPath, path_str: &str) -> Response {
    let name = basename(path);
    let kind = web::file_kind(&name);
    let content = if kind.reads_text() {
        read_inline(fs, path.clone(), kind).await
    } else {
        None
    };
    let sidebar = parent_sidebar(fs, path_str, &name);
    let main = web::file_main(&name, kind, content.as_deref());
    html_response(web::page(&name, &sidebar, &main))
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

/// `GET /path?versions` → the version history: a page (in the shell) for browsers,
/// JSON otherwise.
async fn version_page_response(fs: &DavFs, path_str: &str, accept_html: bool) -> Response {
    let Ok(path) = DavPath::new(path_str) else {
        return bad_request("bad path");
    };
    match fs.list_versions(&path) {
        Ok(versions) if accept_html => {
            let name = basename(&path);
            let sidebar = parent_sidebar(fs, path_str, &name);
            let main = web::version_main(&name, &versions);
            html_response(web::page(&format!("Versions of {name}"), &sidebar, &main))
        }
        Ok(versions) => json_response(versions_to_json(&versions)),
        Err(e) if e.is_not_found() => not_found(),
        Err(_) => bad_request("not a versioned file"),
    }
}

/// Sidebar showing the parent directory of `path_str`, highlighting `current_file`.
fn parent_sidebar(fs: &DavFs, path_str: &str, current_file: &str) -> String {
    match DavPath::new(&parent_encoded(path_str)) {
        Ok(parent) => {
            let segments = decoded_segments(&parent);
            let entries = fs.list_dir(&parent).unwrap_or_default();
            web::sidebar(&segments, &entries, Some(current_file))
        }
        Err(_) => web::sidebar(&[], &[], Some(current_file)),
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

/// Largest file we'll read into memory to render inline (Markdown/text). A
/// document is tiny; the cap bounds memory on this unauthenticated, per-request
/// path (independent of the much larger version-history cap).
const MAX_PREVIEW_BYTES: u64 = 8 * 1024 * 1024;

/// Read a file's current content (capped) and produce the inline main-pane HTML
/// fragment: rendered Markdown, or the raw text for a [`web::FileKind::Text`] file
/// (which `file_main` escapes). `None` if it can't be read (missing, over
/// [`MAX_PREVIEW_BYTES`], IO error, or a render panic) — the caller shows a
/// download fallback instead.
async fn read_inline(fs: &DavFs, path: DavPath, kind: web::FileKind) -> Option<String> {
    let fs = fs.clone();
    // Reading blobs + rendering is CPU/IO work; keep it off the async worker.
    let rendered = tokio::task::spawn_blocking(move || {
        fs.read_current(&path, MAX_PREVIEW_BYTES).map(|bytes| {
            let text = String::from_utf8_lossy(&bytes);
            if kind == web::FileKind::Markdown {
                web::markdown_to_html(&text)
            } else {
                text.into_owned()
            }
        })
    })
    .await;
    match rendered {
        Ok(Ok(html)) => Some(html),
        _ => None,
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

/// Whether the query string carries the `raw` selector (serve underlying bytes).
fn has_raw(query: &str) -> bool {
    query
        .split('&')
        .any(|p| p == "raw" || p.starts_with("raw="))
}

/// Whether a name is a directory's index document (rendered into the main pane).
fn is_readme(name: &str) -> bool {
    matches!(name.to_ascii_lowercase().as_str(), "readme.md" | "index.md")
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

/// The decoded, non-empty path segments of a `DavPath` (e.g. `["docs", "a"]`).
fn decoded_segments(path: &DavPath) -> Vec<String> {
    path.as_bytes()
        .split(|&b| b == b'/')
        .filter(|s| !s.is_empty())
        .map(|s| String::from_utf8_lossy(s).into_owned())
        .collect()
}

/// A file's decoded path for display (e.g. `/docs/a/`), defaulting to `/`.
fn display_path(path: &DavPath) -> String {
    let d = String::from_utf8_lossy(path.as_bytes());
    if d.is_empty() {
        "/".into()
    } else {
        d.into_owned()
    }
}

/// The parent directory of an (already percent-encoded) request path, with a
/// trailing slash (e.g. `/a/b/note.md` → `/a/b/`).
fn parent_encoded(path_str: &str) -> String {
    let trimmed = path_str.trim_end_matches('/');
    match trimmed.rfind('/') {
        Some(i) => path_str[..=i].to_string(),
        None => "/".to_string(),
    }
}

/// Build a child `DavPath` under an (encoded) parent path by appending an encoded
/// segment for `name`.
fn child_path(parent_encoded: &str, name: &str) -> Option<DavPath> {
    let sep = if parent_encoded.ends_with('/') {
        ""
    } else {
        "/"
    };
    DavPath::new(&format!(
        "{parent_encoded}{sep}{}",
        web::encode_segment(name)
    ))
    .ok()
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
