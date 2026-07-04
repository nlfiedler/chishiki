# 0002 — Browser Web Interface

Status: Accepted
Date: 2026-07-04

Design for a basic browser-facing web interface: FTP-style directory browsing,
in-browser rendering of Markdown/images/videos, and version operations
(view/revert/prune) from the browser. This elaborates **Phase 4** of
`docs/specs/0001-initial-build-plan.md`; it builds on the Phase 2–3 architecture
(axum router in front of `dav-server`'s `DavHandler`, backed by `vfs::DavFs`, with
auto-versioning and the `?versions`/`?version=N` GET endpoints).

## Goal

When visited from a browser, the server should behave like a browsable file site:

- **Directory** → an HTML listing; clicking an entry navigates in / downloads.
- **File** → renders in the browser when it can (Markdown → HTML, images, video
  with seeking), otherwise downloads.
- **Versions** → view a file's history, fetch an old version, revert to an old
  version, and (later) prune old versions.

WebDAV clients (Finder, `rclone`, …) are unaffected: they never `GET` a
collection and don't send `Accept: text/html`, so they keep seeing raw bytes.

## What `dav-server` already provides (no work)

Verified against `dav-server` 0.11:

- **Content-Type by extension** on `GET` (`.png → image/png`, `.mp4 → video/mp4`,
  `.md → text/markdown`), so browsers render images/video inline.
- **HTTP range requests** (`206 Partial Content`) via our `DavFile::seek`, so
  video seeking works.
- **A built-in HTML autoindex** for a `GET` on a collection (dir→dir/ redirect +
  clickable entries) — but it is **opt-in** (`DavHandler::builder().autoindex(true)`)
  and was not enabled, so `GET /` currently returns `405`. Enabling it is the
  cheapest path to directory browsing.

The one content-negotiation gap: a `.md` file is served as raw `text/markdown`,
so a browser shows source text rather than rendered HTML.

## Phase 4a — MVP (this change)

Smallest set that yields FTP-style browsing + rendering:

1. **Enable the built-in autoindex** — `.autoindex(true)` on the handler builder.
   Directory browsing works immediately. (Safe for WebDAV clients — only affects
   `GET` on a collection, which they never issue.)
2. **Markdown rendering by content negotiation** — the router intercepts a `GET`
   whose `Accept` includes `text/html` on a `*.md`/`*.markdown` path, reads the
   current content, renders it with **`pulldown-cmark`**, and returns a minimal
   HTML page. Any failure (missing file, a directory) falls through to the
   `DavHandler` (autoindex / raw / 404). Requests without `Accept: text/html`
   (WebDAV clients) fall through and get raw bytes.
3. **`DavFs::read_current(path)`** — read a file's current content into memory,
   capped at `MAX_IN_MEMORY_VERSION` (shares the capped-read helper with
   `read_version`). Used by the renderer.

Deliberately *not* in 4a: our own directory index (we lean on the built-in
autoindex), per-file "view" pages, and any version writes.

**XSS note:** `pulldown-cmark` passes raw HTML embedded in Markdown through
unsanitized. For a single-user personal server serving your own content this is
acceptable; if the server ever becomes multi-user, sanitize rendered output.

## Phase 4b — Version UX + richer pages (later, this doc's scope)

1. **Our own directory index + per-file "view" page.** Replaces/augments the
   built-in autoindex so listings and file pages can carry version controls and
   embed media (`<img>`/`<video>`/rendered Markdown). Needs a
   `DavFs::list_dir(path) -> Vec<{name, is_dir, size, modified}>` helper (a clean
   public listing API rather than consuming the `DavDirEntry` stream).
2. **Revert** — `DavFs::revert_to_version(path, n)`: **append a new version whose
   manifest is version N's**. This fits the append-only, chunk-shared model
   exactly: non-destructive (history preserved: …, vN, …, vM = "reverted to vN"),
   O(1) metadata, no bytes copied. Preferred over repointing
   `current_version_id`, which would break the "current == highest number"
   invariant and the stability of `?version=N` identities.
3. **Prune** — `DavFs::delete_versions(...)`: remove old `versions`/`version_chunks`
   rows (never the current version). See the space-reclaim decision below.

## Decisions

1. **Version-mutating operations use `POST`/`DELETE`, never `GET`.** The read
   surfaces (`?versions`, `?version=N`, rendered views) stay safe/idempotent
   `GET`s; a destructive prune behind a `GET` could be triggered by a browser
   prefetch or crawler. The browser UI drives revert/prune via forms/`fetch`.
2. **Prune reclaims space only after chunk GC (Phase 6).** Blobs are
   content-addressed and shared across versions, so deleting version metadata does
   **not** free disk until a GC pass collects unreferenced chunks. Phase 4b builds
   the metadata prune; **actual space reclamation is deferred to Phase 6 GC**, and
   the UI must say so rather than imply immediate savings.
3. **"Browsers are read-only" is softened to "read-only for content."** Browsing,
   rendering, and reading history are read-only `GET`s. Version *management*
   (revert/prune) is a browser-initiated write — an intentional, explicit
   exception, not general write access. Content upload/move/delete remains
   WebDAV-only.
4. **AuthN/AuthZ is Phase 6.** Until then the interface (including revert/prune)
   is unauthenticated — acceptable only on a trusted personal network. Version
   management should be gated behind auth when Phase 6 lands.
5. **No schema change.** The Phase-3 `versions`/`version_chunks` model already
   supports revert (append) and prune (delete rows).

## Open questions (revisit at 4b)

- Own index vs. keep leaning on the built-in autoindex once version controls are
  needed (leaning: build our own).
- Whether revert/prune live on a per-file view page or a dedicated history page.
