# 0003 — Two-Pane Web UI

Status: Accepted
Date: 2026-07-04

A slicker browser interface: a persistent left **sidebar** (breadcrumb "stack" +
the current directory's contents) and a **main pane** that shows the selected
file (rendered Markdown, image, video, …). Wiki.js-like. Extends the Phase 4a/4b
browser layer (`docs/specs/0002-web-interface.md`) — no `vfs`/storage changes,
just router routing and the `web` module.

## Layout

```
┌───────────────┬─────────────────────────────────┐
│  breadcrumb   │                                 │
│  / a / b      │                                 │
│               │        main content             │
│  ▸ folders    │   (rendered md / <img> /        │
│  ▸ files      │    <video> / download / etc.)   │
│  (this dir)   │                                 │
└───────────────┴─────────────────────────────────┘
```

- **Sidebar** — a breadcrumb of the current path's ancestors (click a crumb to
  jump up) plus a menu of the *current directory's* entries (folders then files).
  For a **file** URL the "current directory" is the file's parent, with the file
  highlighted; for a **directory** URL it's the directory itself.
- **Main** — for a **file**: its content by type. For a **directory**: its
  `README.md`/`readme.md`/`index.md` rendered if present, else an index listing.

Every browser page renders the same shell, so navigation (full page loads) feels
like fixed panes. No client-side framework; if the reload feel needs smoothing
later, htmx can swap just the main pane (progressive enhancement).

## The view/raw split (content negotiation)

A browser navigation to a file (`Accept: text/html`) gets the **view page** (the
shell). The actual bytes are served on the **same path** with a **`?raw`** query,
which the router lets fall through to the `DavHandler` (streamed, range-supported,
`Content-Type` by extension). So:

- `<img src="?raw">`, `<video src="?raw" controls>`, `<audio src="?raw" controls>`
  embed media in the main pane by re-requesting the same path with `?raw`.
- A **Download** link is `<a href="?raw" download>`.
- WebDAV clients and other non-`text/html` GETs already fall through to raw bytes,
  unchanged.

`?raw` streams via the existing `DavFile` path (no in-memory cap), so large media
is fine. Rendered Markdown and inline text are read into memory (no sub-request),
capped by `MAX_PREVIEW_BYTES`.

## File kinds (by extension)

Extension lists are the authoritative set in `web::file_kind`; the highlights:

- **Markdown** (`.md`, `.markdown`) → rendered inline.
- **Text/source** (`.txt .log .json .csv .toml .yaml .xml`, common source
  extensions, …) → the raw content, HTML-escaped, in a `<pre>`.
- **Image** (`.png .jpg .jpeg .gif .webp .avif .bmp .ico`) → `<img src="?raw">`.
- **Video** (`.mp4 .webm .mov .mkv .ogv`) → `<video>` over `?raw`.
- **Audio** (`.mp3 .wav .ogg .flac .m4a .aac`) → `<audio>` over `?raw`.
- **PDF** (`.pdf`) → `<iframe src="?raw">` (the browser's PDF viewer).
- **Other** (incl. deliberately-excluded `.svg`/`.html`, see below) → a Download link.

**Security — `.svg` / `.html` are *not* inline-viewable.** Served inline they run
same-origin script (an SVG can carry `<script>`). They are classed as *Other* (no
`<img>`/iframe embed) so the view page never auto-runs them. A direct `?raw`
navigation still serves them inline via the `DavHandler` — the same accepted
single-user "raw HTML executes" risk as embedded HTML in Markdown; a CSP or
`Content-Disposition: attachment` on `?raw` is the proper Phase-6 hardening.

## Styling: Bulma, embedded

Adopt **Bulma** (pure CSS; has the `menu`, `breadcrumb`, and `columns` components
this layout needs). It is **vendored and embedded in the binary** (`include_str!`
of `assets/bulma.min.css`), *not* loaded from a CDN, so the server is
self-contained and works offline / on a LAN. A small amount of custom CSS (fixed
sidebar, scroll regions) is appended.

- Served at **`GET /_assets/bulma.css`** (`text/css`, long `Cache-Control`; the
  browser fetches it once). Pages link it via `<link rel="stylesheet">`.
- The **`/_assets/` path prefix is reserved** for server assets (a real node
  named `_assets` at the root is shadowed). Documented; unlikely to collide.
- (Bulma's full CSS is ~660 KB uncompressed; response compression via a
  tower-http layer is a later optimization.)

## Version management

The per-file version history moves into the shell: a **History** link/tab on the
file view leads to `?versions` rendered inside the shell (sidebar unchanged; main
shows the version table with the existing revert/delete `POST` controls). JSON is
still returned for non-browser `?versions` requests. Behavior is unchanged from
Phase 4b — only the presentation.

## Decisions

1. **Server-rendered, full-reload navigation** — no SPA/JS framework for the MVP.
   The consistent shell provides the panes; htmx is a possible later enhancement.
2. **`?raw` for bytes** — one explicit mechanism for embedded media and downloads,
   independent of `Accept` sniffing; streams via `DavHandler` (no memory cap).
3. **Bulma embedded, not CDN** — self-contained/offline; served from `/_assets/`.
4. **Absolute links in the shell** (breadcrumb + entries), percent-encoded. Simple
   and unambiguous for breadcrumb jumps. **Limitation:** a reverse-proxy mount
   sub-path isn't supported yet (links assume the app is at the origin root); a
   configurable base path can be added later. Direct localhost/LAN use (the
   documented deployment) is unaffected.
5. **No `vfs` changes** — `list_dir` / `read_current` / `is_dir` already suffice.

## Out of scope (later)

- An expandable full directory *tree* in the sidebar (MVP shows the current level
  only, which matches the stack-and-contents model).
- Response compression, htmx no-reload swaps, a configurable mount prefix,
  in-browser upload/rename/delete (those stay WebDAV-only; version revert/prune
  remain the only browser writes).
