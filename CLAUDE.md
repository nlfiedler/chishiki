# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project status

**Rust** project, built as a **Cargo workspace**. Phases 0 (workspace &
toolchain), 1 (content-addressable blob store + FastCDC chunker), 2 (virtualized
filesystem + working WebDAV server), and 3 (auto-versioning) are complete, and
Phase 4 (browser web interface) has landed. See
`docs/specs/0001-initial-build-plan.md` for the phased plan and
`docs/specs/0002-web-interface.md` for the web-interface design.

Layout:

- `crates/blobstore` — content-addressable blob store; blake3-keyed blobs + FastCDC file manifests (Phase 1 ✓)
- `crates/chunker` — FastCDC content-defined chunking (Phase 1 ✓)
- `crates/vfs` — virtualized filesystem: SQLite (`rusqlite`) metadata store with immutable per-file version history, + the `dav-server` `DavFileSystem`/`DavFile`/`DavMetaData` traits over the blob store (Phases 2–3 ✓)
- `crates/index` — reverse index for search, `tantivy` (Phase 5)
- `crates/webdav-server` — the WebDAV/HTTP binary; axum router in front of `dav-server`'s `DavHandler` + `MemLs`, backed by `vfs::DavFs`; version-history endpoints and the browser layer (own directory index, per-file version pages with revert/prune, Markdown rendering) in the `web` module (Phases 2–4 ✓)

The library crates are WebDAV-agnostic so the storage engine ships without
dragging WebDAV along.

**`vfs` design notes:** `DavFs` is `Clone` and holds an `Arc<Inner>` (SQLite
`MetaStore` + `BlobStore` + chunker config). Each `DavFileSystem` method returns
`async move { … }.boxed()`; SQLite metadata work runs synchronously (no `.await`
while the connection mutex is held) so the futures stay `Send`, while the two
file-size-proportional operations — reconstructing prior content on a
non-truncating open, and chunking the temp file on `flush` — run on
`tokio::task::spawn_blocking` so they don't stall the runtime. `dav-server` is
used with `default-features = false` (no bundled `localfs`/`memfs`), so we map
`io::Error`/`MetaError` to `FsError` ourselves (`io_to_fs`/`meta_to_fs`). Writes
buffer to a temp file under `<data>/tmp` and are chunked into the blob store on
`flush`; reads stream by reconstructing from the manifest (Range/seek supported,
with the current chunk cached to keep sequential reads O(n)). Moving/copying a
collection into its own subtree is rejected (`MetaStore::is_ancestor_or_self`).

**Versioning (Phase 3):** every content write appends an immutable version
(`versions` + `version_chunks` tables; `nodes.current_version_id` points at the
live one), unless the new content is byte-identical to the current version (then
it's a no-op, so repeated no-op re-PUTs don't grow history). Overwriting a file
via **PUT** appends a version to that same node, preserving its history.
GET/PROPFIND serve the current version; history is
exposed by the router (not `dav-server`) as `GET /path?versions` (JSON list) and
`GET /path?version=N` (that version's bytes; a malformed selector is a 400). Full
RFC 3253 Delta-V protocol is a later phase.

Known Phase-3 limitations (deferred, not bugs):
- `DavFs::read_version` reconstructs a historical version **into memory**, capped
  at `MAX_IN_MEMORY_VERSION` (256 MiB → 413 above that). **TODO:** replace with an
  owned streaming reader shared with the live GET path so any-size history streams.
- Version history is reachable only via these `GET` endpoints, so a WebDAV-mounted
  client (Finder/rclone) can't browse it. A virtual `.versions/` namespace or
  Delta-V would close that gap.
- **COPY/MOVE onto an existing file loses that file's history.** dav-server's
  handler deletes the destination (cascading its versions) *before* calling
  `fs.copy`/`fs.rename`, so overwrite-via-COPY/MOVE can't preserve history the way
  PUT does. `DavFs::copy` already does the right thing for direct callers; closing
  the HTTP gap needs the router to intercept COPY/MOVE (like the version endpoints)
  or path-keyed history.

**Browser layer (Phase 4).** `dav-server` supplies the basics for free: it sets
`Content-Type` by extension (images/video render inline, with range/seek). On top,
the router does content negotiation for browser `GET`s (those whose `Accept`
includes `text/html`), with all HTML built in the `web` module:
- **Directory** → our own version-aware index (`DavFs::list_dir`; the built-in
  autoindex is off). A `GET` on a collection without a trailing slash 302-redirects
  to add one so relative entry links resolve.
- **`*.md`** → rendered to HTML via `pulldown-cmark` (`DavFs::read_current`, capped
  at `MAX_MARKDOWN_BYTES`; oversized → 413).
- **`?versions`** → an HTML version page with revert/delete controls for browsers,
  JSON otherwise. `?version=N` → that version's bytes.
- Anything else (missing file, non-HTML client, other file types) falls through to
  the `DavHandler` → raw bytes, so WebDAV clients are unaffected.

**Version management** is browser-initiated *write*, exposed only via **`POST`**
(never `GET`): `POST /file?revert=N` (`DavFs::revert_to_version` — appends version
N's manifest as a new version, non-destructive) and `POST /file?prune=N`
(`DavFs::prune_version` — deletes a non-current version's metadata; refuses the
current version). Both 303-redirect back to the version page. **Prune frees
version metadata only; blobs are reclaimed at chunk GC (Phase 6).** Content
upload/move/delete stays WebDAV-only. AuthN/AuthZ is Phase 6 — until then these
writes are unauthenticated (trusted-network assumption).

`read_current`/`read_version` reconstruct into memory (capped); the streaming
owned-reader is the deferred TODO.

Toolchain is pinned by `rust-toolchain.toml` (Rust **1.96.0**, edition **2024**,
with `rustfmt` + `clippy`).

### Commands

- Build: `cargo build --workspace`
- Run the server: `cargo run -p webdav-server` (env: `CHISHIKI_DATA` — data dir,
  default `./data`; `CHISHIKI_ADDR` — listen address, default `127.0.0.1:4918`).
  Mount from a WebDAV client (`rclone`, `cadaver`, Finder, Windows) or drive with
  `curl -X PROPFIND/PUT/GET/MKCOL/MOVE/COPY/DELETE`.
- Test (all): `cargo test --workspace`
- Test a single crate: `cargo test -p <crate>` (e.g. `cargo test -p blobstore`)
- Test a single test by name: `cargo test -p <crate> <test_name>`
- Format: `cargo fmt --all` (check-only: `cargo fmt --all --check`)
- Lint: `cargo clippy --workspace --all-targets -- -D warnings`

CI (`.github/workflows/ci.yml`) runs fmt-check, clippy (warnings denied), tests,
doc-tests, and a release build on every push/PR to `main`.

## What is being built

A personal WebDAV server for storing and serving Markdown, images, and videos, accessible both from a browser and from WebDAV clients. Core goals from `README.md`:

- **WebDAV** server implementing **Delta-V** versioning (RFC 3253) and **WebDAV Search** (RFC 5323). References: RFC 3253, RFC 5323, http://www.webdav.org.
- **Browser rendering**: when accessed via a browser, `.md` files are rendered to HTML by a Markdown renderer (WebDAV clients still see the raw file).
- **Versioning**: track the full version history of each file as it is modified.
- **Search**: maintain a reverse index so documents can be found by query terms.

## Intended architecture (design constraints)

These are the load-bearing design decisions from `README.md` — preserve them as implementation proceeds:

- **Content-addressable blob store.** Content is stored by hash in a blob store; the folder/file hierarchy the client sees is *virtualized* on top of it rather than mapped directly to disk paths. Two files with identical content share storage.
- **Chunking with FastCDC.** Larger files are split into content-defined chunks via FastCDC; only unique chunks are persisted in the blob store. A file is reconstructed from its ordered list of chunk references.
- **Version history** is layered on the blob store — a new version references the (mostly shared) chunks of the prior version plus whatever changed.

The interplay between these three (virtualized namespace ↔ content-addressed chunks ↔ version history) is the central complexity of the system; keep it coherent when adding features.

## Browser vs. WebDAV client

WebDAV is an extension of HTTP: a WebDAV server *is* an HTTP server that adds extra methods (`PROPFIND`, `PROPPATCH`, `MKCOL`, `COPY`, `MOVE`, `LOCK`, `UNLOCK`, and `SEARCH` from RFC 5323) on top of `GET`/`PUT`/`DELETE`. This project serves **two kinds of clients over the same URLs**, and the split shapes the request-handling design:

- **Browsers** only issue plain HTTP verbs (effectively `GET`). They have no UI for the WebDAV methods, so they are **read-only** here: they can view content and follow links, but cannot upload, move, or list collections via `PROPFIND`. They *can* read version history and (later) run search through server-provided `GET` endpoints (e.g. `?versions`, `?version=N`) — those are read-only `GET` surfaces the server adds precisely because browsers can't drive the WebDAV/Delta-V methods. Running a WebDAV `SEARCH` (the RFC 5323 method) remains WebDAV-only; a browser search box would likewise be a server `GET` endpoint.
- **WebDAV clients** (Finder, Windows "Map network drive", `cadaver`, `rclone`, etc.) speak the full method set and drive uploads, versioning, and search.

The mechanism that makes one namespace serve both is **content negotiation on `GET`** — inspect the request (e.g. `Accept: text/html`, which browsers send and WebDAV clients generally do not) and choose the representation:

- `GET` a `.md` file → **rendered HTML** for a browser, **raw markdown bytes** for a WebDAV client.
- `GET` a collection (folder) → **server-generated HTML index page** for a browser, since browsers cannot `PROPFIND`. Pure WebDAV does not define a `GET` response for collections, so this listing must be generated by the server.
- `GET` an image/video → the raw bytes either way; the browser renders them natively.

Keep this rule in mind when adding endpoints: the same URL may need two representations, selected by client type, and any "browsable in a browser" behavior (directory listings, Markdown rendering) must be provided by the server layer — it is not part of the WebDAV protocol. In the current implementation the server generates its own version-aware directory index (the `dav-server` built-in autoindex is off) and renders Markdown via router content negotiation; a `GET` on a collection is served the index for any client, while file rendering is browser-only (`Accept: text/html`). See `docs/specs/0002-web-interface.md`.
