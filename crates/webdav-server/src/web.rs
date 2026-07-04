//! Server-generated HTML for the browser interface: directory index, per-file
//! version-management page, and rendered Markdown. Pure functions that turn data
//! into a self-contained HTML string; the router (`main.rs`) owns dispatch.
//!
//! Links use relative/query-only hrefs so pages work regardless of mount prefix:
//! a directory page (served at a trailing-slash URL) links to `name` / `name/`
//! and `../`; a version page (at `…/file`) links to `?version=N`, and its
//! revert/prune forms POST to `?revert=N` / `?prune=N`.

use std::time::{SystemTime, UNIX_EPOCH};

use percent_encoding::{AsciiSet, NON_ALPHANUMERIC, utf8_percent_encode};
use vfs::{DirEntryInfo, VersionInfo};

/// Percent-encoding set for a single URL path segment: everything that isn't an
/// unreserved character (RFC 3986 `A-Za-z0-9-._~`) is encoded, so a name is safe
/// as one segment (spaces, `/`, `?`, `#`, … all encoded).
const SEGMENT: &AsciiSet = &NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'_')
    .remove(b'.')
    .remove(b'~');

/// Wrap rendered Markdown in a minimal, self-contained HTML document.
///
/// NOTE: `pulldown-cmark` passes raw HTML embedded in the Markdown through
/// unsanitized. Fine for a single-user personal server serving your own content;
/// sanitize if this ever becomes multi-user.
pub(crate) fn markdown_page(title: &str, markdown: &str) -> String {
    use pulldown_cmark::{Options, Parser, html};

    let options = Options::ENABLE_TABLES
        | Options::ENABLE_STRIKETHROUGH
        | Options::ENABLE_TASKLISTS
        | Options::ENABLE_FOOTNOTES;
    let parser = Parser::new_ext(markdown, options);
    let mut body = String::new();
    html::push_html(&mut body, parser);

    page(
        title,
        &format!("<article class=\"markdown\">{body}</article>"),
    )
}

/// A directory listing. `display_path` is the decoded path for the heading;
/// `has_parent` controls the `../` link; `entries` are the children.
pub(crate) fn directory_index(
    display_path: &str,
    has_parent: bool,
    entries: &[DirEntryInfo],
) -> String {
    let mut rows = String::new();
    if has_parent {
        rows.push_str("<tr><td><a href=\"../\">../</a></td><td></td><td></td><td></td></tr>");
    }
    for entry in entries {
        let name = escape_html(&entry.name);
        let seg = encode_segment(&entry.name);
        let modified = escape_html(&format_time(entry.modified));
        if entry.is_dir {
            rows.push_str(&format!(
                "<tr><td><a href=\"{seg}/\">{name}/</a></td><td></td>\
                 <td>{modified}</td><td></td></tr>"
            ));
        } else {
            let size = format_size(entry.size);
            rows.push_str(&format!(
                "<tr><td><a href=\"{seg}\">{name}</a></td><td>{size}</td>\
                 <td>{modified}</td><td><a href=\"{seg}?versions\">history</a></td></tr>"
            ));
        }
    }
    let heading = escape_html(display_path);
    let body = format!(
        "<h1>Index of {heading}</h1>\
         <table><thead><tr><th>Name</th><th>Size</th><th>Modified</th><th></th></tr></thead>\
         <tbody>{rows}</tbody></table>"
    );
    page(&format!("Index of {display_path}"), &body)
}

/// A file's version-management page: history plus revert/prune controls.
///
/// `file_name` is the decoded basename (for display and the "view current" link).
/// `versions` is oldest-first; the page shows newest-first.
pub(crate) fn version_page(file_name: &str, versions: &[VersionInfo]) -> String {
    let name = escape_html(file_name);
    let seg = encode_segment(file_name);

    let mut rows = String::new();
    for v in versions.iter().rev() {
        let created = escape_html(&format_time(v.created));
        let size = format_size(v.size);
        let marker = if v.is_current {
            " <span class=\"tag\">current</span>"
        } else {
            ""
        };
        // Query-only hrefs/actions keep the same path (…/file).
        let mut actions = format!("<a href=\"?version={}\">view</a>", v.number);
        if !v.is_current {
            actions.push_str(&format!(
                " <form method=\"post\" action=\"?revert={n}\"><button>revert to</button></form>\
                 <form method=\"post\" action=\"?prune={n}\">\
                 <button class=\"danger\">delete</button></form>",
                n = v.number
            ));
        }
        rows.push_str(&format!(
            "<tr><td>{}{marker}</td><td>{size}</td><td>{created}</td>\
             <td class=\"actions\">{actions}</td></tr>",
            v.number
        ));
    }

    let body = format!(
        "<h1>Versions of {name}</h1>\
         <p class=\"links\"><a href=\"./\">↑ directory</a> · \
         <a href=\"{seg}\">view current</a></p>\
         <p class=\"note\">Deleting a version removes it from history but does not \
         reclaim disk space until garbage collection (a later phase).</p>\
         <table><thead><tr><th>Version</th><th>Size</th><th>Created</th><th>Actions</th></tr>\
         </thead><tbody>{rows}</tbody></table>"
    );
    page(&format!("Versions of {file_name}"), &body)
}

/// Wrap page `body` in a minimal HTML document with the shared stylesheet.
fn page(title: &str, body: &str) -> String {
    let title = escape_html(title);
    format!(
        "<!doctype html>\n\
         <html lang=\"en\"><head><meta charset=\"utf-8\">\
         <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">\
         <title>{title}</title><style>{PAGE_CSS}</style></head>\
         <body><main>{body}</main></body></html>"
    )
}

/// Percent-encode a name for use as one URL path segment.
fn encode_segment(name: &str) -> String {
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

/// Convert days-since-Unix-epoch to a (year, month, day) civil date.
///
/// Howard Hinnant's `civil_from_days` algorithm (public domain), valid for the
/// proleptic Gregorian calendar.
fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097; // [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365; // [0, 399]
    let year = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let day = (doy - (153 * mp + 2) / 5 + 1) as u32; // [1, 31]
    let month = if mp < 10 { mp + 3 } else { mp - 9 } as u32; // [1, 12]
    (if month <= 2 { year + 1 } else { year }, month, day)
}

/// Minimal readable stylesheet shared by all server-generated pages.
const PAGE_CSS: &str = "\
body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Helvetica,Arial,sans-serif;\
line-height:1.6;color:#1a1a1a;background:#fff;margin:0}\
main{max-width:52rem;margin:2rem auto;padding:0 1.25rem}\
h1{line-height:1.25;font-size:1.5rem}\
a{color:#0366d6;text-decoration:none}a:hover{text-decoration:underline}\
table{border-collapse:collapse;width:100%}\
th,td{text-align:left;padding:.35em .75em;border-bottom:1px solid #eee}\
th{border-bottom:2px solid #ddd;font-size:.85em;color:#555}\
td.actions{white-space:nowrap}\
form{display:inline;margin:0 0 0 .5em}\
button{font:inherit;cursor:pointer;background:#f5f5f5;border:1px solid #ccc;\
border-radius:5px;padding:.1em .6em}button:hover{background:#eaeaea}\
button.danger{color:#b00}\
.tag{font-size:.75em;background:#e6f4ea;color:#137333;border-radius:4px;padding:.05em .4em}\
.note{color:#555;font-size:.9em;background:#fffbe6;border:1px solid #f0e0a0;\
border-radius:6px;padding:.5em .75em}\
.links{color:#555}\
article.markdown pre{background:#f5f5f5;padding:1rem;overflow:auto;border-radius:6px}\
article.markdown code{background:#f5f5f5;padding:.15em .35em;border-radius:4px;font-size:.9em}\
article.markdown pre code{background:none;padding:0}\
article.markdown img{max-width:100%}\
article.markdown blockquote{margin:0;padding-left:1rem;border-left:4px solid #ddd;color:#555}";
