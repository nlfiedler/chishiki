//! WebDAV/HTTP server binary.
//!
//! A thin axum router in front of `dav-server`'s `DavHandler`, backed by our
//! [`vfs::DavFs`] (SQLite metadata + content-addressable blob store). Phase 2
//! stands up a working WebDAV class-1/2 server; Phase 3 adds auto-versioning with
//! a version-history HTTP surface (`?versions`, `?version=N`) here in the router,
//! since `dav-server` does not route Delta-V methods. Phase 4a adds the browser
//! layer (dav-server autoindex + Markdown rendering by content negotiation);
//! SEARCH (Phase 5) is layered on later. See `docs/specs/0001-initial-build-plan.md`
//! and `docs/specs/0002-web-interface.md`.

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
        // Serve a built-in HTML directory listing on a browser GET of a
        // collection (WebDAV clients PROPFIND instead, so they're unaffected).
        .autoindex(true)
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
            VersionRequest::None => {
                // Browser content negotiation: render Markdown to HTML when the
                // client accepts HTML. Anything else (missing file, a directory,
                // a non-HTML client) falls through to the DavHandler, which serves
                // raw bytes / the autoindex / a 404.
                if wants_html(&req)
                    && is_markdown(req.uri().path())
                    && let Ok(path) = DavPath::new(req.uri().path())
                    && let Some(resp) = render_markdown(&fs, path).await
                {
                    return resp;
                }
            }
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

/// Whether the client accepts HTML (i.e. is a browser, not a WebDAV client).
fn wants_html(req: &axum::extract::Request) -> bool {
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

/// Largest Markdown file we'll render in memory. A document is tiny; the cap
/// bounds memory on this unauthenticated, per-request path (independent of the
/// much larger version-history cap).
const MAX_MARKDOWN_BYTES: u64 = 8 * 1024 * 1024;

/// Render the current content of a Markdown file to an HTML page.
///
/// Returns `None` — so the caller falls through to the `DavHandler` (raw bytes /
/// autoindex / 404) — when the path isn't a renderable file (missing, a directory,
/// or a read error). An oversized file yields 413 and a render panic yields 500,
/// rather than silently serving raw bytes.
async fn render_markdown(fs: &DavFs, path: DavPath) -> Option<Response> {
    let title = basename(&path);
    let fs = fs.clone();
    // Reading blobs + rendering is CPU/IO work; keep it off the async worker.
    let rendered = tokio::task::spawn_blocking(move || {
        fs.read_current(&path, MAX_MARKDOWN_BYTES).map(|bytes| {
            let markdown = String::from_utf8_lossy(&bytes);
            markdown_to_html_page(&title, &markdown)
        })
    })
    .await;
    match rendered {
        Ok(Ok(html)) => {
            Some(([(header::CONTENT_TYPE, "text/html; charset=utf-8")], html).into_response())
        }
        Ok(Err(e)) if e.is_too_large() => Some(
            (
                StatusCode::PAYLOAD_TOO_LARGE,
                "markdown file too large to render",
            )
                .into_response(),
        ),
        // Missing file, a directory, or an IO error → let the DavHandler serve it.
        Ok(Err(_)) => None,
        // The render task panicked.
        Err(_) => Some((StatusCode::INTERNAL_SERVER_ERROR, "render failed").into_response()),
    }
}

/// Last path segment (decoded), for the page `<title>`.
fn basename(path: &DavPath) -> String {
    let last = path
        .as_bytes()
        .split(|&b| b == b'/')
        .rfind(|s| !s.is_empty())
        .unwrap_or(b"");
    String::from_utf8_lossy(last).into_owned()
}

/// Wrap rendered Markdown in a minimal, self-contained HTML document.
///
/// NOTE: `pulldown-cmark` passes raw HTML embedded in the Markdown through
/// unsanitized. Fine for a single-user personal server serving your own content;
/// sanitize if this ever becomes multi-user.
fn markdown_to_html_page(title: &str, markdown: &str) -> String {
    use pulldown_cmark::{Options, Parser, html};

    let options = Options::ENABLE_TABLES
        | Options::ENABLE_STRIKETHROUGH
        | Options::ENABLE_TASKLISTS
        | Options::ENABLE_FOOTNOTES;
    let parser = Parser::new_ext(markdown, options);
    let mut body = String::new();
    html::push_html(&mut body, parser);

    let title = escape_html(title);
    format!(
        "<!doctype html>\n\
         <html lang=\"en\"><head><meta charset=\"utf-8\">\
         <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">\
         <title>{title}</title><style>{PAGE_CSS}</style></head>\
         <body><main class=\"markdown-body\">{body}</main></body></html>"
    )
}

/// Escape the five HTML-significant characters for interpolation into markup.
fn escape_html(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&#39;"),
            _ => out.push(c),
        }
    }
    out
}

/// Minimal readable stylesheet for rendered pages.
const PAGE_CSS: &str = "\
body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Helvetica,Arial,sans-serif;\
line-height:1.6;color:#1a1a1a;background:#fff;margin:0}\
main{max-width:44rem;margin:2rem auto;padding:0 1.25rem}\
h1,h2,h3{line-height:1.25}\
pre{background:#f5f5f5;padding:1rem;overflow:auto;border-radius:6px}\
code{background:#f5f5f5;padding:.15em .35em;border-radius:4px;font-size:.9em}\
pre code{background:none;padding:0}\
a{color:#0366d6}\
table{border-collapse:collapse}th,td{border:1px solid #ddd;padding:.4em .6em}\
img{max-width:100%}\
blockquote{margin:0;padding-left:1rem;border-left:4px solid #ddd;color:#555}";

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
