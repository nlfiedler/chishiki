//! WebDAV/HTTP server binary.
//!
//! A thin axum router in front of `dav-server`'s `DavHandler`, backed by our
//! [`vfs::DavFs`] (SQLite metadata + content-addressable blob store). Phase 2
//! stands up a working WebDAV class-1/2 server; Phase 3 adds auto-versioning with
//! a version-history HTTP surface here in the router (`dav-server` does not route
//! Delta-V methods). Phase 4 adds the browser layer: a two-pane web UI (sidebar +
//! main pane, styled with Bulma) with directory browsing, file rendering (Markdown
//! /image/video via the `?raw` view/bytes split), and per-file version pages with
//! revert/prune. Phase 5 adds full-text search: a browser `GET …?q=…` surface and
//! an RFC 5323 `SEARCH`-method interceptor over the `vfs`/`index` reverse index.
//! See the specs under `docs/specs/` (0001 build plan, 0002 web interface, 0003
//! web UI, 0004 search).
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
use vfs::{DavFs, GcStats, SearchResult, VersionInfo};

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
    // Cheap method checks up front; the booleans don't borrow `req` across await.
    let is_get = req.method() == Method::GET;
    let is_post = req.method() == Method::POST;
    let is_search = req.method().as_str() == "SEARCH";
    let is_options = req.method() == Method::OPTIONS;
    if is_get {
        // Extract the request pieces up front so nothing borrowed from `req`
        // (which isn't `Sync`) is held across an `.await`.
        let accept_html = wants_html(&req);
        let query = req.uri().query().unwrap_or("").to_string();
        let path_str = req.uri().path().to_string();
        if let Some(resp) = handle_browser_get(&fs, &path_str, &query, accept_html).await {
            return resp;
        }
    } else if is_post {
        // Extract owned pieces up front so no `&Request` (not `Sync`) is held
        // across the action's `.await`.
        let query = req.uri().query().unwrap_or("").to_string();
        let path_str = req.uri().path().to_string();
        let cross_origin = is_cross_origin(&req);
        if let Some(resp) = handle_post_action(&fs, &query, &path_str, cross_origin).await {
            return resp;
        }
    } else if is_search {
        // RFC 5323 SEARCH: the router handles it (dav-server does not).
        return handle_search_method(&fs, req).await;
    }
    let mut resp = dav.handle(req).await.into_response();
    if is_options {
        // Advertise the supported DASL query grammar (RFC 5323 §2) so clients can
        // discover that this server answers the SEARCH method.
        resp.headers_mut().insert(
            axum::http::HeaderName::from_static("dasl"),
            axum::http::HeaderValue::from_static("<DAV:basicsearch>"),
        );
    }
    resp
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
    // `?q=…` is a full-text search scoped to this path's subtree (root = global).
    if let Some(q) = parse_search_query(query) {
        return Some(search_response(fs, path_str, &q, accept_html).await);
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

/// Handle a state-changing `POST`: `?revert=N` / `?prune=N` (per-file) or `?gc`
/// (store-wide). Returns `None` for a `POST` without a recognized action (falls
/// through — `DavHandler` will 405).
///
/// The request pieces are passed by value (`query`, `path_str`, `cross_origin`)
/// rather than as `&Request`, so nothing un-`Sync` is held across the `.await`.
async fn handle_post_action(
    fs: &DavFs,
    query: &str,
    path_str: &str,
    cross_origin: bool,
) -> Option<Response> {
    let action = parse_action_query(query)?;
    // These are state-changing writes with no auth; refuse a browser-initiated
    // cross-origin POST (CSRF). A same-origin form (the version page) matches;
    // a non-browser tool sends no Origin and is allowed.
    if cross_origin {
        return Some((StatusCode::FORBIDDEN, "cross-origin request refused").into_response());
    }
    Some(match action {
        Action::Bad => bad_request("invalid action"),
        Action::Gc => {
            // Store-wide, path-independent: only honor it at the root so a
            // scoped-looking `POST /file?gc` can't trigger a global sweep.
            if path_str != "/" {
                return Some(bad_request("gc is only available at POST /?gc"));
            }
            let fs_owned = fs.clone();
            // GC scans/deletes blobs (disk I/O under an exclusive lock); off-thread.
            match tokio::task::spawn_blocking(move || fs_owned.gc()).await {
                Ok(Ok(stats)) => json_response(gc_stats_json(&stats)),
                _ => (StatusCode::INTERNAL_SERVER_ERROR, "gc failed").into_response(),
            }
        }
        Action::Revert(number) | Action::Prune(number) => {
            let Ok(path) = DavPath::new(path_str) else {
                return Some(bad_request("bad path"));
            };
            let revert = matches!(action, Action::Revert(_));
            // revert reconstructs content and commits the search index (fsync),
            // and both touch SQLite; run the mutation off the async worker.
            let fs_owned = fs.clone();
            let result = tokio::task::spawn_blocking(move || {
                if revert {
                    fs_owned.revert_to_version(&path, number)
                } else {
                    fs_owned.prune_version(&path, number)
                }
            })
            .await;
            match result {
                // POST-redirect-GET back to the (now-updated) version page.
                Ok(Ok(())) => redirect(StatusCode::SEE_OTHER, &format!("{path_str}?versions")),
                Ok(Err(e)) if e.is_not_found() => not_found(),
                // e.g. "cannot delete the current version".
                Ok(Err(e)) => (StatusCode::BAD_REQUEST, e.to_string()).into_response(),
                Err(_) => (StatusCode::INTERNAL_SERVER_ERROR, "action failed").into_response(),
            }
        }
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

/// Maximum number of search hits returned by any surface (browser or SEARCH).
const SEARCH_LIMIT: usize = 50;

/// Cap on a SEARCH request body — the query XML is tiny; reject anything larger.
const MAX_SEARCH_BODY: usize = 64 * 1024;

/// `GET /path?q=…` → full-text search scoped to `path_str`'s subtree. A browser
/// gets a results page in the shell; other clients get a JSON array.
async fn search_response(fs: &DavFs, path_str: &str, query: &str, accept_html: bool) -> Response {
    let Ok(scope) = DavPath::new(path_str) else {
        return bad_request("bad path");
    };
    let fs_owned = fs.clone();
    let q = query.to_string();
    // tantivy search does blocking segment reads; keep it off the async worker.
    let result =
        tokio::task::spawn_blocking(move || fs_owned.search(&q, SEARCH_LIMIT, &scope)).await;
    match result {
        Ok(Ok(hits)) if accept_html => {
            let sidebar = scope_sidebar(fs, path_str);
            let main = web::search_main(query, &hits);
            html_response(web::page(&format!("Search: {query}"), &sidebar, &main))
        }
        Ok(Ok(hits)) => json_response(search_to_json(&hits)),
        Ok(Err(e)) if e.is_not_found() => not_found(),
        Ok(Err(e)) if e.is_bad_query() => bad_request("invalid search query"),
        Ok(Err(_)) => (StatusCode::INTERNAL_SERVER_ERROR, "search failed").into_response(),
        Err(_) => (StatusCode::INTERNAL_SERVER_ERROR, "search failed").into_response(),
    }
}

/// Sidebar for a search-results page: the scope directory's listing (root by
/// default) so navigation stays available beside the results.
fn scope_sidebar(fs: &DavFs, path_str: &str) -> String {
    match DavPath::new(path_str) {
        Ok(path) => {
            let segments = decoded_segments(&path);
            let entries = fs.list_dir(&path).unwrap_or_default();
            web::sidebar(&segments, &entries, None)
        }
        Err(_) => web::sidebar(&[], &[], None),
    }
}

/// Render search hits as a JSON array of `{path, score, snippet}`.
fn search_to_json(hits: &[SearchResult]) -> String {
    let items: Vec<String> = hits
        .iter()
        .map(|h| {
            let snippet = match &h.snippet {
                Some(s) => json_string(s),
                None => "null".to_string(),
            };
            format!(
                r#"{{"path":{},"score":{},"snippet":{}}}"#,
                json_string(&h.path),
                h.score,
                snippet
            )
        })
        .collect();
    format!("[{}]", items.join(","))
}

/// Render a garbage-collection summary as JSON (all fields numeric).
fn gc_stats_json(stats: &GcStats) -> String {
    format!(
        r#"{{"scanned":{},"removed":{},"reclaimed":{},"failed":{}}}"#,
        stats.blobs_scanned, stats.blobs_removed, stats.bytes_reclaimed, stats.blobs_failed
    )
}

/// Encode a string as a JSON string literal (with surrounding quotes).
fn json_string(s: &str) -> String {
    use std::fmt::Write as _;
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => {
                let _ = write!(out, "\\u{:04x}", c as u32);
            }
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

/// Handle an RFC 5323 `SEARCH` request: parse the `DAV:basicsearch` body for a
/// free-text query (`DAV:contains`, or a `DAV:like` literal) and an optional
/// scope, run the search, and return a `207 Multi-Status` of matching resources.
///
/// This implements the useful free-text subset of the DASL grammar (which is all
/// our full-text index can answer); property/comparison predicates are not
/// supported. The scope comes from the body's first `DAV:scope/DAV:href`, falling
/// back to the request path.
async fn handle_search_method(fs: &DavFs, req: Request) -> Response {
    let request_path = req.uri().path().to_string();
    let body = match axum::body::to_bytes(req.into_body(), MAX_SEARCH_BODY).await {
        Ok(b) => b,
        Err(_) => return bad_request("search request body too large or unreadable"),
    };
    let parsed = parse_search_request(&body);
    if parsed.query.trim().is_empty() {
        return bad_request("no supported search term (expected DAV:contains)");
    }
    // Scope: the body's scope href if given, else the request path.
    let scope_str = parsed
        .scope_href
        .as_deref()
        .and_then(href_to_path)
        .unwrap_or(request_path);
    let Ok(scope) = DavPath::new(&scope_str) else {
        return bad_request("bad search scope");
    };
    let fs_owned = fs.clone();
    let q = parsed.query;
    let result =
        tokio::task::spawn_blocking(move || fs_owned.search(&q, SEARCH_LIMIT, &scope)).await;
    match result {
        Ok(Ok(hits)) => multistatus_response(&hits),
        Ok(Err(e)) if e.is_not_found() => not_found(),
        Ok(Err(e)) if e.is_bad_query() => bad_request("invalid search query"),
        Ok(Err(_)) => (StatusCode::INTERNAL_SERVER_ERROR, "search failed").into_response(),
        Err(_) => (StatusCode::INTERNAL_SERVER_ERROR, "search failed").into_response(),
    }
}

/// The free-text query and optional scope extracted from a SEARCH body.
struct SearchRequest {
    query: String,
    scope_href: Option<String>,
}

/// Parse the free-text subset of a `DAV:basicsearch` request body: the text of a
/// `DAV:contains` element (preferred) or a `DAV:like` `DAV:literal` (with `%`/`_`
/// wildcards trimmed), plus the first `DAV:scope/DAV:href`. Namespace-prefix
/// agnostic (matches on local element names); malformed XML yields whatever was
/// collected before the error, so a partial body still does something sensible.
fn parse_search_request(body: &[u8]) -> SearchRequest {
    use quick_xml::events::Event;
    let mut reader = quick_xml::Reader::from_reader(body);
    reader.config_mut().trim_text(true);

    let (mut in_contains, mut in_literal, mut in_scope, mut in_href) = (false, false, false, false);
    let (mut contains, mut literal, mut scope_href) = (String::new(), String::new(), String::new());
    let mut buf = Vec::new();
    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Start(e)) => match local_name(e.name().as_ref()) {
                b"contains" => in_contains = true,
                b"literal" => in_literal = true,
                b"scope" => in_scope = true,
                b"href" if in_scope => in_href = true,
                _ => {}
            },
            Ok(Event::End(e)) => match local_name(e.name().as_ref()) {
                b"contains" => in_contains = false,
                b"literal" => in_literal = false,
                b"scope" => in_scope = false,
                b"href" => in_href = false,
                _ => {}
            },
            Ok(Event::Text(t)) => {
                // `decode()` yields the literal text; entity references arrive as
                // separate events (ignored — search terms rarely contain them).
                let text = t.decode().unwrap_or_default();
                if in_contains {
                    contains.push_str(&text);
                } else if in_literal {
                    literal.push_str(&text);
                } else if in_href {
                    scope_href.push_str(&text);
                }
            }
            Ok(Event::Eof) | Err(_) => break,
            _ => {}
        }
        buf.clear();
    }

    // Prefer the contains term; otherwise a LIKE literal with its wildcards trimmed.
    let query = if !contains.trim().is_empty() {
        contains.trim().to_string()
    } else {
        literal.trim().trim_matches(['%', '_']).trim().to_string()
    };
    let scope_href = {
        let s = scope_href.trim();
        (!s.is_empty()).then(|| s.to_string())
    };
    SearchRequest { query, scope_href }
}

/// The local part of an XML element name (drop any `prefix:`).
fn local_name(qname: &[u8]) -> &[u8] {
    match qname.iter().rposition(|&b| b == b':') {
        Some(i) => &qname[i + 1..],
        None => qname,
    }
}

/// Reduce a scope `href` (an absolute path or full URL) to a URL path for
/// [`DavPath::new`]. `None` for an empty href.
fn href_to_path(href: &str) -> Option<String> {
    let h = href.trim();
    if h.is_empty() {
        return None;
    }
    let path = match h.find("://") {
        // Full URL: keep the path after the authority (default root if none).
        Some(i) => {
            let rest = &h[i + 3..];
            match rest.find('/') {
                Some(j) => &rest[j..],
                None => "/",
            }
        }
        None => h,
    };
    Some(path.to_string())
}

/// Build a `207 Multi-Status` listing the search hits as WebDAV responses.
fn multistatus_response(hits: &[SearchResult]) -> Response {
    let mut body = String::from(
        "<?xml version=\"1.0\" encoding=\"utf-8\"?>\n<D:multistatus xmlns:D=\"DAV:\">",
    );
    for h in hits {
        let href = web::path_href(&h.path);
        let name = h.path.rsplit('/').find(|s| !s.is_empty()).unwrap_or("");
        let displayname = web::escape_html(name);
        body.push_str(&format!(
            "<D:response><D:href>{href}</D:href><D:propstat><D:prop>\
             <D:displayname>{displayname}</D:displayname></D:prop>\
             <D:status>HTTP/1.1 200 OK</D:status></D:propstat></D:response>"
        ));
    }
    body.push_str("</D:multistatus>");
    let status = StatusCode::from_u16(207).unwrap();
    (
        status,
        [(header::CONTENT_TYPE, "application/xml; charset=utf-8")],
        body,
    )
        .into_response()
}

/// Extract the `q=` value from a query string, percent-decoded (`+` as space) and
/// trimmed. `None` if absent or blank (a blank search falls through to normal
/// handling — e.g. an empty search box submit shows the page).
fn parse_search_query(query: &str) -> Option<String> {
    for pair in query.split('&').filter(|s| !s.is_empty()) {
        let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
        if key == "q" {
            let decoded = decode_query_value(value);
            let trimmed = decoded.trim();
            if trimmed.is_empty() {
                return None;
            }
            return Some(trimmed.to_string());
        }
    }
    None
}

/// Decode an `application/x-www-form-urlencoded` query value: `+` → space, then
/// percent-decoding (lossy UTF-8).
fn decode_query_value(value: &str) -> String {
    let plus_decoded = value.replace('+', " ");
    percent_encoding::percent_decode_str(&plus_decoded)
        .decode_utf8_lossy()
        .into_owned()
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

/// A state-changing action from a `POST` query string.
enum Action {
    Revert(u64),
    Prune(u64),
    /// Store-wide chunk garbage collection (`?gc`, no value).
    Gc,
    /// An action key was present but its value didn't parse.
    Bad,
}

/// Parse a `POST` query for `revert=N` / `prune=N` / `gc`. `None` if none present.
fn parse_action_query(query: &str) -> Option<Action> {
    for pair in query.split('&').filter(|s| !s.is_empty()) {
        let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
        match key {
            "revert" => return Some(value.parse().map(Action::Revert).unwrap_or(Action::Bad)),
            "prune" => return Some(value.parse().map(Action::Prune).unwrap_or(Action::Bad)),
            "gc" => return Some(Action::Gc),
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
    fn search_query_extraction() {
        assert_eq!(parse_search_query("q=hello").as_deref(), Some("hello"));
        // `+` and percent-encoding decode; surrounding whitespace is trimmed.
        assert_eq!(
            parse_search_query("q=two+words").as_deref(),
            Some("two words")
        );
        assert_eq!(
            parse_search_query("q=caf%C3%A9%20bar").as_deref(),
            Some("café bar")
        );
        assert_eq!(parse_search_query("foo=1&q=x").as_deref(), Some("x"));
        // Absent or blank queries fall through (None).
        assert_eq!(parse_search_query("other=1"), None);
        assert_eq!(parse_search_query("q="), None);
        assert_eq!(parse_search_query("q=+++"), None);
    }

    #[test]
    fn json_string_escaping() {
        assert_eq!(json_string("plain"), r#""plain""#);
        assert_eq!(json_string("a\"b\\c"), r#""a\"b\\c""#);
        assert_eq!(json_string("line\nbreak"), r#""line\nbreak""#);
    }

    #[test]
    fn href_to_path_extraction() {
        assert_eq!(href_to_path("/docs/").as_deref(), Some("/docs/"));
        assert_eq!(
            href_to_path("http://host:4918/docs/a").as_deref(),
            Some("/docs/a")
        );
        // A bare authority with no path defaults to root.
        assert_eq!(href_to_path("http://host").as_deref(), Some("/"));
        assert_eq!(href_to_path("   "), None);
    }

    #[test]
    fn local_name_drops_prefix() {
        assert_eq!(local_name(b"D:contains"), b"contains");
        assert_eq!(local_name(b"contains"), b"contains");
    }

    #[test]
    fn parse_search_request_extracts_term_and_scope() {
        let body = br#"<?xml version="1.0"?>
            <D:searchrequest xmlns:D="DAV:"><D:basicsearch>
              <D:from><D:scope><D:href>/docs/</D:href><D:depth>infinity</D:depth></D:scope></D:from>
              <D:where><D:contains>quarterly report</D:contains></D:where>
            </D:basicsearch></D:searchrequest>"#;
        let parsed = parse_search_request(body);
        assert_eq!(parsed.query, "quarterly report");
        assert_eq!(parsed.scope_href.as_deref(), Some("/docs/"));
    }

    #[test]
    fn parse_search_request_falls_back_to_like_literal() {
        // No DAV:contains; a DAV:like literal with SQL wildcards is used instead.
        let body = br#"<D:searchrequest xmlns:D="DAV:"><D:basicsearch><D:where>
              <D:like><D:prop><D:displayname/></D:prop><D:literal>%needle%</D:literal></D:like>
            </D:where></D:basicsearch></D:searchrequest>"#;
        let parsed = parse_search_request(body);
        assert_eq!(parsed.query, "needle");
        assert_eq!(parsed.scope_href, None);
    }

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
        // `gc` is a valueless action.
        assert!(matches!(parse_action_query("gc"), Some(Action::Gc)));
        assert!(matches!(parse_action_query("gc=1"), Some(Action::Gc)));
        assert!(parse_action_query("").is_none());
        assert!(parse_action_query("other=1").is_none());
    }

    #[test]
    fn gc_stats_json_shape() {
        let stats = GcStats {
            blobs_scanned: 12,
            blobs_removed: 3,
            bytes_reclaimed: 4096,
            blobs_failed: 1,
        };
        assert_eq!(
            gc_stats_json(&stats),
            r#"{"scanned":12,"removed":3,"reclaimed":4096,"failed":1}"#
        );
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
