//! Server-generated HTML for the two-pane browser UI, styled with Bulma.
//!
//! Pure data→String functions; the router (`main.rs`) owns dispatch and I/O.
//! Every page is the same shell — a fixed left sidebar (breadcrumb + the current
//! directory's entries) and a scrolling main pane — so full-reload navigation
//! feels like fixed panes. Links are absolute and percent-encoded (a mount
//! sub-path behind a proxy is out of scope; see `docs/specs/0003-web-ui.md`).

use std::time::{SystemTime, UNIX_EPOCH};

use percent_encoding::{AsciiSet, NON_ALPHANUMERIC, utf8_percent_encode};
use vfs::{DirEntryInfo, SearchResult, VersionInfo};

/// Vendored Bulma, embedded so the server is self-contained (served at
/// `/_assets/bulma.css`).
pub(crate) const BULMA_CSS: &str = include_str!("../assets/bulma.min.css");

/// Percent-encoding set for one URL path segment (everything but RFC 3986
/// unreserved `A-Za-z0-9-._~`).
const SEGMENT: &AsciiSet = &NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'_')
    .remove(b'.')
    .remove(b'~');

/// The kind of a file, chosen by extension, for main-pane rendering.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum FileKind {
    Markdown,
    /// Plain text / source shown escaped in a `<pre>`.
    Text,
    Image,
    Video,
    Audio,
    Pdf,
    Other,
}

impl FileKind {
    /// Whether the router should read the file's text to render it inline.
    pub(crate) fn reads_text(self) -> bool {
        matches!(self, FileKind::Markdown | FileKind::Text)
    }
}

/// Classify a file name by extension.
///
/// Note: `.svg`/`.html` are deliberately *not* inline-viewable kinds — served
/// inline they can execute same-origin script (see `docs/specs/0003-web-ui.md`).
pub(crate) fn file_kind(name: &str) -> FileKind {
    let ext = name.rsplit('.').next().unwrap_or("").to_ascii_lowercase();
    match ext.as_str() {
        "md" | "markdown" => FileKind::Markdown,
        "png" | "jpg" | "jpeg" | "gif" | "webp" | "avif" | "bmp" | "ico" => FileKind::Image,
        "mp4" | "webm" | "mov" | "mkv" | "ogv" => FileKind::Video,
        "mp3" | "wav" | "ogg" | "flac" | "m4a" | "aac" => FileKind::Audio,
        "pdf" => FileKind::Pdf,
        "txt" | "text" | "log" | "json" | "jsonc" | "csv" | "tsv" | "toml" | "yaml" | "yml"
        | "xml" | "ini" | "conf" | "cfg" | "env" | "properties" | "diff" | "patch" | "rs"
        | "py" | "js" | "mjs" | "cjs" | "ts" | "tsx" | "jsx" | "c" | "h" | "cpp" | "cc" | "hpp"
        | "go" | "java" | "rb" | "sh" | "bash" | "zsh" | "sql" | "css" | "scss" | "lua" | "pl"
        | "php" | "kt" | "swift" | "r" | "jl" | "hs" | "ml" | "ex" | "exs" => FileKind::Text,
        _ => FileKind::Other,
    }
}

/// Render Markdown to an HTML fragment (no surrounding page).
///
/// NOTE: raw HTML embedded in the Markdown is passed through unsanitized — fine
/// for a single-user personal server; sanitize if this becomes multi-user.
pub(crate) fn markdown_to_html(markdown: &str) -> String {
    use pulldown_cmark::{Options, Parser, html};
    let options = Options::ENABLE_TABLES
        | Options::ENABLE_STRIKETHROUGH
        | Options::ENABLE_TASKLISTS
        | Options::ENABLE_FOOTNOTES;
    let parser = Parser::new_ext(markdown, options);
    let mut out = String::new();
    html::push_html(&mut out, parser);
    out
}

/// Assemble the full two-pane page from a prebuilt sidebar and main pane.
pub(crate) fn page(title: &str, sidebar: &str, main: &str) -> String {
    let title = escape_html(title);
    format!(
        "<!doctype html>\n\
         <html lang=\"en\"><head><meta charset=\"utf-8\">\
         <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">\
         <title>{title}</title>\
         <link rel=\"stylesheet\" href=\"/_assets/bulma.css\">\
         <style>{CUSTOM_CSS}</style></head>\
         <body><div class=\"app\">\
         <aside class=\"app-sidebar\">{sidebar}</aside>\
         <main class=\"app-main\">{main}</main>\
         </div></body></html>"
    )
}

/// The sidebar: a breadcrumb of `dir_segments` (decoded path of the current
/// directory) plus its `entries` (folders then files). `current_file`, if set,
/// highlights the open file.
pub(crate) fn sidebar(
    dir_segments: &[String],
    entries: &[DirEntryInfo],
    current_file: Option<&str>,
) -> String {
    let mut folders = String::new();
    let mut files = String::new();
    for entry in entries {
        let name = escape_html(&entry.name);
        if entry.is_dir {
            let href = format!("{}/", file_href(dir_segments, &entry.name));
            folders.push_str(&format!("<li><a href=\"{href}\">{name}/</a></li>"));
        } else {
            let href = file_href(dir_segments, &entry.name);
            let active = if current_file == Some(entry.name.as_str()) {
                " class=\"is-active\""
            } else {
                ""
            };
            files.push_str(&format!("<li><a{active} href=\"{href}\">{name}</a></li>"));
        }
    }

    let mut menu = search_box();
    menu.push_str(&breadcrumb(dir_segments));
    menu.push_str("<div class=\"menu\">");
    if !folders.is_empty() {
        menu.push_str(&format!(
            "<p class=\"menu-label\">Folders</p><ul class=\"menu-list\">{folders}</ul>"
        ));
    }
    if !files.is_empty() {
        menu.push_str(&format!(
            "<p class=\"menu-label\">Files</p><ul class=\"menu-list\">{files}</ul>"
        ));
    }
    if folders.is_empty() && files.is_empty() {
        menu.push_str("<p class=\"menu-label\">empty</p>");
    }
    menu.push_str("</div>");
    menu
}

/// The sidebar's global search box (a `GET /?q=…` form). Present on every page so
/// search is always reachable; searching from the root scopes to the whole store.
fn search_box() -> String {
    "<form class=\"field mb-4\" action=\"/\" method=\"get\" role=\"search\">\
     <div class=\"control\">\
     <input class=\"input is-small\" type=\"search\" name=\"q\" \
     placeholder=\"Search\u{2026}\" aria-label=\"Search\">\
     </div></form>"
        .to_string()
}

/// Search-results main pane: each hit links to its file, with a highlighted
/// snippet beneath. `query` is the raw user query (escaped here for display).
pub(crate) fn search_main(query: &str, results: &[SearchResult]) -> String {
    let q = escape_html(query);
    let count = results.len();
    let mut body = String::new();
    if results.is_empty() {
        body.push_str("<p class=\"notification\">No matches.</p>");
    } else {
        body.push_str("<ul class=\"search-results\">");
        for r in results {
            let href = path_href(&r.path);
            let path = escape_html(&r.path);
            // The snippet is already HTML (tantivy escapes the text and wraps
            // matched terms in <b>…</b>), so it is inlined without re-escaping.
            let snippet = r.snippet.as_deref().unwrap_or("");
            body.push_str(&format!(
                "<li><a href=\"{href}\">{path}</a>\
                 <p class=\"snippet is-size-7 has-text-grey\">{snippet}</p></li>"
            ));
        }
        body.push_str("</ul>");
    }
    format!(
        "<h1 class=\"title is-5\">Search results for \u{201c}{q}\u{201d}</h1>\
         <p class=\"subtitle is-6\">{count} match{es}</p>\
         <div class=\"content\">{body}</div>",
        es = if count == 1 { "" } else { "es" }
    )
}

/// Directory main pane: a rendered `README` if present, else an index table.
pub(crate) fn dir_main(
    display_path: &str,
    readme_html: Option<&str>,
    dir_segments: &[String],
    entries: &[DirEntryInfo],
) -> String {
    if let Some(html) = readme_html {
        return format!("<div class=\"content\">{html}</div>");
    }
    let mut rows = String::new();
    for entry in entries {
        let name = escape_html(&entry.name);
        let modified = escape_html(&format_time(entry.modified));
        if entry.is_dir {
            let href = format!("{}/", file_href(dir_segments, &entry.name));
            rows.push_str(&format!(
                "<tr><td><a href=\"{href}\">{name}/</a></td><td></td><td>{modified}</td><td></td></tr>"
            ));
        } else {
            let href = file_href(dir_segments, &entry.name);
            let size = format_size(entry.size);
            rows.push_str(&format!(
                "<tr><td><a href=\"{href}\">{name}</a></td><td>{size}</td><td>{modified}</td>\
                 <td><a href=\"{href}?versions\">history</a></td></tr>"
            ));
        }
    }
    let heading = escape_html(display_path);
    format!(
        "<h1 class=\"title is-5\">Index of {heading}</h1>\
         <table class=\"table is-fullwidth is-hoverable\"><thead><tr>\
         <th>Name</th><th>Size</th><th>Modified</th><th></th></tr></thead><tbody>{rows}</tbody></table>"
    )
}

/// File main pane: a header (name + Download/History) over the content, which is
/// rendered Markdown, escaped text, embedded media, or a download prompt.
///
/// For [`FileKind::Markdown`] `text` is the *rendered* HTML fragment; for
/// [`FileKind::Text`] it is the raw file content (escaped here). Other kinds
/// ignore `text`.
pub(crate) fn file_main(name: &str, kind: FileKind, text: Option<&str>) -> String {
    let body = match kind {
        FileKind::Markdown => match text {
            Some(html) => format!("<div class=\"content\">{html}</div>"),
            None => no_preview(),
        },
        FileKind::Text => match text {
            Some(content) => format!("<pre>{}</pre>", escape_html(content)),
            None => no_preview(),
        },
        FileKind::Image => {
            "<figure class=\"image\"><img src=\"?raw\" alt=\"\"></figure>".to_string()
        }
        FileKind::Video => {
            "<video controls style=\"max-width:100%\"><source src=\"?raw\"></video>".to_string()
        }
        FileKind::Audio => "<audio controls src=\"?raw\" style=\"width:100%\"></audio>".to_string(),
        FileKind::Pdf => {
            "<iframe src=\"?raw\" title=\"PDF\" style=\"width:100%;height:82vh;border:0\"></iframe>"
                .to_string()
        }
        FileKind::Other => no_preview(),
    };
    format!("{}{body}", file_header(name))
}

/// "No preview" notice with a nudge to the Download button.
fn no_preview() -> String {
    "<p class=\"notification\">No inline preview for this file — use Download above.</p>"
        .to_string()
}

/// Version-history main pane: the table with view/revert/delete controls.
pub(crate) fn version_main(name: &str, versions: &[VersionInfo]) -> String {
    let mut rows = String::new();
    for v in versions.iter().rev() {
        let created = escape_html(&format_time(v.created));
        let size = format_size(v.size);
        let marker = if v.is_current {
            " <span class=\"tag is-success is-light\">current</span>"
        } else {
            ""
        };
        let mut actions = format!(
            "<a class=\"button is-small\" href=\"?version={}\">view</a>",
            v.number
        );
        if !v.is_current {
            actions.push_str(&format!(
                " <form method=\"post\" action=\"?revert={n}\">\
                 <button class=\"button is-small\">revert to</button></form>\
                 <form method=\"post\" action=\"?prune={n}\">\
                 <button class=\"button is-small is-danger is-light\">delete</button></form>",
                n = v.number
            ));
        }
        rows.push_str(&format!(
            "<tr><td>{}{marker}</td><td>{size}</td><td>{created}</td>\
             <td class=\"actions\">{actions}</td></tr>",
            v.number
        ));
    }
    // A relative basename href drops the `?versions` query, landing on the file's
    // rendered view page (not the raw bytes).
    let view_href = encode_segment(name);
    let name = escape_html(name);
    format!(
        "<h1 class=\"title is-5\">Versions of {name}</h1>\
         <p class=\"buttons\"><a class=\"button is-small\" href=\"{view_href}\">view current</a></p>\
         <p class=\"notification is-warning is-light\">Deleting a version removes it from \
         history but does not reclaim disk space until garbage collection (a later phase).</p>\
         <table class=\"table is-fullwidth\"><thead><tr>\
         <th>Version</th><th>Size</th><th>Created</th><th>Actions</th></tr></thead>\
         <tbody>{rows}</tbody></table>"
    )
}

/// Header bar for a file's main pane: the name plus Download/History links.
fn file_header(name: &str) -> String {
    let name = escape_html(name);
    format!(
        "<div class=\"level\"><div class=\"level-left\">\
         <h1 class=\"title is-5\">{name}</h1></div>\
         <div class=\"level-right buttons\">\
         <a class=\"button is-small\" href=\"?raw\" download>Download</a>\
         <a class=\"button is-small\" href=\"?versions\">History</a></div></div>"
    )
}

/// Breadcrumb `<nav>` for the current directory's decoded segments.
fn breadcrumb(dir_segments: &[String]) -> String {
    let mut items = String::from("<li><a href=\"/\">home</a></li>");
    for i in 0..dir_segments.len() {
        let href = format!("{}/", dir_href(&dir_segments[..=i]));
        let label = escape_html(&dir_segments[i]);
        let active = if i + 1 == dir_segments.len() {
            " class=\"is-active\""
        } else {
            ""
        };
        items.push_str(&format!("<li{active}><a href=\"{href}\">{label}</a></li>"));
    }
    format!("<nav class=\"breadcrumb is-small\"><ul>{items}</ul></nav>")
}

/// Absolute, percent-encoded href for a directory given its decoded segments
/// (without a trailing slash — callers append `/` as needed).
fn dir_href(segments: &[String]) -> String {
    let mut href = String::new();
    for seg in segments {
        href.push('/');
        href.push_str(&encode_segment(seg));
    }
    if href.is_empty() {
        href.push('/');
    }
    href
}

/// Absolute, percent-encoded href for a file `name` within `dir_segments`.
fn file_href(dir_segments: &[String], name: &str) -> String {
    let base = dir_href(dir_segments);
    if base.ends_with('/') {
        format!("{base}{}", encode_segment(name))
    } else {
        format!("{base}/{}", encode_segment(name))
    }
}

/// Absolute, percent-encoded href for a full slash-separated virtual path
/// (e.g. `/docs/a b.md` → `/docs/a%20b.md`), for search-result links and the
/// SEARCH-method multistatus response.
pub(crate) fn path_href(path: &str) -> String {
    let segments: Vec<String> = path
        .split('/')
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .collect();
    dir_href(&segments)
}

pub(crate) fn encode_segment(name: &str) -> String {
    utf8_percent_encode(name, SEGMENT).to_string()
}

/// Escape the HTML-significant characters for interpolation into markup.
pub(crate) fn escape_html(s: &str) -> String {
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

/// Human-readable byte size (e.g. `1.5 KiB`).
fn format_size(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    if bytes < 1024 {
        return format!("{bytes} B");
    }
    let mut size = bytes as f64;
    let mut unit = 0;
    while size >= 1024.0 && unit < UNITS.len() - 1 {
        size /= 1024.0;
        unit += 1;
    }
    format!("{size:.1} {}", UNITS[unit])
}

/// Format a `SystemTime` as `YYYY-MM-DD HH:MM:SS UTC`.
fn format_time(t: SystemTime) -> String {
    let secs = t
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);
    let days = secs.div_euclid(86_400);
    let tod = secs.rem_euclid(86_400);
    let (h, m, s) = (tod / 3600, (tod % 3600) / 60, tod % 60);
    let (year, month, day) = civil_from_days(days);
    format!("{year:04}-{month:02}-{day:02} {h:02}:{m:02}:{s:02} UTC")
}

/// Days-since-Unix-epoch → (year, month, day). Howard Hinnant's `civil_from_days`
/// (public domain), proleptic Gregorian.
fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let year = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let month = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (if month <= 2 { year + 1 } else { year }, month, day)
}

/// Custom layout CSS appended to Bulma: fixed sidebar + scrolling main.
// Colors reference Bulma's scheme CSS variables so the layout follows Bulma's
// automatic light/dark mode (`prefers-color-scheme`) instead of hardcoding light
// values — otherwise the sidebar stays light while the rest goes dark.
const CUSTOM_CSS: &str = "\
html,body{height:100%}\
.app{display:flex;height:100vh}\
.app-sidebar{width:20rem;min-width:15rem;max-width:24rem;overflow:auto;\
padding:1rem 1rem 2rem;border-right:1px solid var(--bulma-border);\
background:var(--bulma-scheme-main-bis)}\
.app-main{flex:1;overflow:auto;padding:1.5rem 2rem;background:var(--bulma-scheme-main)}\
.app-sidebar .breadcrumb{margin-bottom:1rem}\
.app-sidebar .menu-label{margin-top:1rem}\
.app-main .actions form{display:inline;margin-left:.35rem}\
.app-main .search-results li{margin-bottom:.9rem}\
.app-main .search-results .snippet{margin-top:.15rem}\
.app-main figure.image img{max-width:100%;height:auto}\
@media(max-width:768px){.app{flex-direction:column;height:auto}\
.app-sidebar{width:auto;max-width:none;border-right:none;\
border-bottom:1px solid var(--bulma-border)}}";
