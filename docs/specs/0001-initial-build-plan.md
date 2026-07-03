# 0001 — Initial Build Plan

Status: Accepted
Date: 2026-07-02

Phased plan for building the WebDAV server in Rust on top of the `dav-server`
crate. See `README.md` for project goals and `CLAUDE.md` for the standing
architecture notes.

## Critical finding: `dav-server` does not implement the two headline features

Per the crate docs (docs.rs/dav-server), it explicitly does **not** support
**RFC 3253 (Delta-V versioning)** or **RFC 5323 (WebDAV SEARCH)** — the two RFCs
the README calls out as the point of the project. It *is* a solid WebDAV
**class 1/2** server (`GET`/`PUT`/`PROPFIND`/`MKCOL`/`COPY`/`MOVE`/`LOCK`) with a
pluggable backend. Versioning and search are therefore ours to build.

What the crate does provide:

- **`DavFileSystem` + `DavFile` + `DavMetaData` + `DavDirEntry`** — traits for a
  fully custom backing store (the hook for the content-addressable blob store).
  Backends can optionally store DAV properties. `GuardedFileSystem` adds access
  control.
- **Lock systems**: `MemLs` (in-memory) and `FakeLs` (minimal, for macOS/Windows
  Finder compatibility).
- **Framework adapters**: native `hyper`/`http` types, plus `actix-web` and
  `warp`; `axum` works too (hyper + tower underneath).
- **Construction**: builder pattern —
  `DavHandler::builder().filesystem(...).locksystem(...).build_handler()` —
  processing `http::Request` → `http::Response`.

**Architectural consequence:** put a thin outer HTTP router (recommend **axum**)
in front of `DavHandler`. The router owns what the crate won't: it intercepts the
`SEARCH` method, serves browser `GET`s with content negotiation, optionally
handles Delta-V methods, and passes everything else through to `DavHandler`.
Versioning lives *inside* our `DavFileSystem` (every write is a new immutable
version, cheap thanks to chunk sharing).

## Reuse goal

Reusable, WebDAV-agnostic pieces — blob store, FastCDC chunker, versioned
metadata, reverse index — live in their own library crates in a Cargo workspace.
The WebDAV/HTTP binary is a thin consumer on top, so the storage engine ships
without dragging WebDAV along.

## Phases

### Phase 0 — Workspace & toolchain
- `git init`; Cargo **workspace** with library crates (`blobstore`, `chunker`,
  `vfs`, `index`) + a binary crate (`webdav-server`).
- rustfmt, clippy (deny warnings in CI), a test harness, and CI. Then replace the
  "Project status" section of `CLAUDE.md` with the real build/test/run commands.

### Phase 1 — Content-addressable blob store (lib: `blobstore` + `chunker`)
- Blob store keyed by content hash (**blake3**); `put`/`get`/`has`, streaming
  read/write.
- **FastCDC** chunking via the `fastcdc` crate; store only unique chunks. A file
  = an ordered manifest of chunk hashes. Reconstruct as a streaming reader.
- Deferred: chunk ref-counting / GC (revisit in Phase 6).

### Phase 2 — Virtualized filesystem + `DavFileSystem` (lib: `vfs`, bin: `webdav-server`)
- Metadata store mapping the virtual path namespace → file manifests + attributes.
  **Engine: SQLite via `rusqlite`** (see Open decisions #1).
- Implement `DavFileSystem`/`DavFile`/`DavMetaData`/`DavDirEntry` over blob store
  + metadata.
- Stand up `DavHandler` + `MemLs`, serve via axum. **Milestone:** mount from
  Finder / `rclone` / `cadaver` and read/write real files (first end-to-end
  checkpoint).

### Phase 3 — Versioning (in `vfs`)
- **Basic auto-versioning first** (see Open decisions #3): every `PUT`/modify
  creates a new immutable version automatically; near-free because unchanged
  chunks are shared. Model version history in the metadata store.
- Expose history pragmatically — a virtual `.versions/` namespace and/or an
  HTTP endpoint — since `dav-server` won't route Delta-V methods.
- **Full RFC 3253 protocol compliance (`VERSION-CONTROL`, `REPORT`,
  `CHECKOUT`/`CHECKIN`) is deferred to later phases**, layered onto the outer
  router incrementally once basic auto-versioning is solid.

### Phase 4 — Browser layer / content negotiation (in the router)
- Detect browser vs. WebDAV client by `Accept: text/html`. `GET` `.md` → HTML via
  **pulldown-cmark**; `GET` a collection → server-generated HTML index;
  images/videos → raw bytes with HTTP **range request** support (needed for video
  seeking). Realizes the "Browser vs. WebDAV client" section in `CLAUDE.md`.

### Phase 5 — Search / reverse index (lib: `index`)
- On write, tokenize text/markdown and update a reverse index. **Engine:
  `tantivy`** (see Open decisions #2).
- Two surfaces: browser-facing `GET /search?q=…`, and a `SEARCH`-method
  interceptor in the router for RFC 5323 clients.

### Phase 6 — Hardening
- AuthN/AuthZ (`GuardedFileSystem` for access control), chunk GC, locking/
  concurrency correctness, large-file streaming, integration tests against real
  WebDAV clients, and benchmarks.

## Resolved decisions
1. **Metadata engine** (Phase 2): **SQLite via `rusqlite`.** `sled` has had very
   slow development for several years; `redb` looks promising but its own README
   comparison shows a notably larger on-disk file size than SQLite. SQLite is a
   very reliable, battle-tested storage library, so it wins.
   _Resolved 2026-07-02._
2. **Search engine** (Phase 5): **`tantivy`.** Full-featured and promising; worth
   the dependency over a hand-rolled inverted index. _Resolved 2026-07-02._
3. **Versioning scope** (Phase 3): **Basic auto-versioning first**, with
   additional Delta-V (RFC 3253) protocol functionality delivered in later phases.
   _Resolved 2026-07-02._
