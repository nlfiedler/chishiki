# 0006 — Large-File Version Streaming (Phase 6)

Status: Accepted
Date: 2026-07-06

Serving a historical file version (`GET /path?version=N`) reconstructs the whole
version **into memory** before sending it: `DavFs::read_version` →
`reconstruct` → `Vec<u8>`, capped at `MAX_IN_MEMORY_VERSION` (256 MiB). Above the
cap the request returns **413 Payload Too Large**; below it, the entire version
is buffered on the heap for the duration of the response. For a server whose
purpose includes images and **videos**, that is both a hard ceiling and a memory
spike. This change streams historical versions chunk-by-chunk, so any-size
version is served with bounded memory and no 413.

## Background: the live path already streams; only versions don't

There are two read paths today:

- **Live `GET`** (current content, and `?raw`) flows through the `dav-server`
  `DavHandler` → our `FileHandle` (`crates/vfs/src/file.rs`), whose `read_bytes`
  walks the manifest and loads **one chunk at a time**, caching the current
  chunk. This already streams with Range/seek support. No change needed.
- **Historical `GET /path?version=N`** is served by the router's `serve_version`
  → `DavFs::read_version`, which buffers the whole version into a `Vec<u8>`.
  **This is the only gap.**

The blob store already has the right primitive: `ManifestReader` in
`crates/blobstore/src/manifest.rs` is a `Read + Seek` that fetches chunks lazily,
one at a time — but it is **borrowed** (`ManifestReader<'a>` holds `&'a
BlobStore` and `&'a Manifest`), so it can't outlive the call or move into a
streamed response body. The fix is an **owned** sibling.

## Design

### 1. An owned `ManifestReader` (`blobstore`)

`BlobStore` is `Clone` and cloning is cheap (it's just the root `PathBuf`), and
`Manifest` is owned metadata. The blob store already has a borrowed `Read + Seek`
`ManifestReader<'a>` (used by `FileHandle` and the buffered `reconstruct`), but a
streamed response needs something that borrows nothing. The natural unit for
streaming is **one chunk**, so rather than an owned `Read` (which forces a
caller-allocated buffer and an extra copy on every read), add a forward-only
owned chunk stream:

```rust
pub struct ChunkStream { store: BlobStore, manifest: Manifest, next: usize }
impl ChunkStream {
    pub fn total_size(&self) -> u64;
    pub fn next_chunk(&mut self) -> Option<io::Result<Vec<u8>>>;  // the blob read, no extra copy
}
```

`BlobStore::stream_chunks(manifest: Manifest) -> ChunkStream` clones the store
handle and takes the manifest by value. `ChunkStream` is `Send + 'static`, so it
moves into a `spawn_blocking` closure and backs a streamed response. Each
`next_chunk` yields the chunk's own buffer directly — no memset, no oversized
buffer, no second copy. `ManifestReader<'a>` (borrowed) is left exactly as it was
for the in-memory callers.

### 2. `DavFs::open_version` (`vfs`)

```rust
pub fn open_version(&self, path: &DavPath, number: u64) -> Result<VersionReader, VfsError>
```

Resolves the node, looks up version `N`, loads its manifest, and returns a
`VersionReader` — a thin public wrapper over `ChunkStream` that also carries the
version's `size()` (for `Content-Length`) and exposes `next_chunk()`. **No size
cap**: streaming has no in-memory ceiling.

`read_version` (the buffered, capped variant) is kept for programmatic/in-memory
callers and simply loads the version manifest and calls the existing
`reconstruct` (reject `size > MAX_IN_MEMORY_VERSION` with `VfsError::TooLarge`,
else buffer) — the same path `read_current` uses. `read_current` / `reconstruct`
(the small-capped browser-preview path) are unchanged.

### 3. Streaming `serve_version` (router)

`serve_version` now:

1. `spawn_blocking` to build the reader (`open_version` — SQLite metadata only,
   no blob reads yet). Error mapping: `is_not_found` → 404; `is_invalid_target`
   (e.g. asking for a version of a directory) → 400 "not a versioned file"; any
   other error (corrupt metadata, SQLite/IO, join failure) → **500** rather than
   masking a server fault as a client 400. The 413 branch is gone (no cap).
2. Streams the version's chunks as the body with **bounded memory**, sets
   `Content-Type: application/octet-stream` and `Content-Length: size`, and
   returns `200 OK`.

The reader does **blocking** file I/O (a chunk is `fs::read`), which must not run
on the async runtime. **One** `tokio::task::spawn_blocking` task drains the
`ChunkStream` into a bounded `tokio::sync::mpsc` channel (capacity 4); the
response body is a `futures_util::stream::unfold` over the receiver.
`blocking_send` parks the producer when the channel is full, so read-ahead is
bounded and paced by the client's read speed (backpressure). One blocking
dispatch covers the whole transfer (not one per buffer), and each chunk `Vec`
becomes a `Bytes` with no copy. `axum::body::Body::from_stream` wraps the
receiver stream; a mid-stream error (see below) ends the body, and the declared
`Content-Length` lets the client detect the truncation. (This needs tokio's
`sync` feature.)

## Concurrency: reader vs. GC (unchanged posture)

A streaming read holds **no** GC guard — the same posture as the live path and as
documented in `0005`. Holding the shared `gc_lock` for the whole of a
possibly-minutes-long download would block GC far worse than the write-stall it
already accepts, so we don't. The consequence is the already-accepted
**reader-vs-GC race**, now potentially longer-lived: if the version being
streamed is *concurrently pruned* **and** then garbage-collected, a
not-yet-read chunk can vanish and the stream fails mid-transfer. This is a
**failed read, never corruption**, and only against a client racing a delete of
the exact version it is reading. (The buffered path had the same race during its
reconstruct window; streaming widens it to the response duration.)

## Scope / non-goals

- **Range requests for `?version=N` are not added here.** The buffered version
  never supported Range either, so this is not a regression. Deferred to keep this
  change focused on removing the memory ceiling. `ChunkStream` is forward-only,
  but a later `Range` can start at the right chunk via the manifest's per-chunk
  offsets and trim the first/last chunk. The live/current path keeps its existing
  Range support via `DavHandler`.
- `read_current` and the browser Markdown/preview renderer keep their small
  intentional cap (`MAX_PREVIEW_BYTES`) — a rendered preview *should* be bounded.
- `MAX_IN_MEMORY_VERSION` / `VfsError::TooLarge` remain, now only reachable via
  the buffered `read_version` convenience, not the HTTP path.

## Decisions

1. **A forward-only owned `ChunkStream`, not an owned `Read`/`Seek` reader** —
   streaming's natural unit is one chunk, so yielding the chunk's own buffer is
   zero-copy and needs no caller buffer. The borrowed `ManifestReader<'a>` is
   untouched for in-memory callers; nothing needs a generic/owned `Read`.
2. **One blocking task + a bounded mpsc channel, not per-buffer `spawn_blocking`** —
   one blocking dispatch covers the whole transfer, and a small bounded channel
   gives read-ahead with `blocking_send` backpressure. (Costs enabling tokio's
   `sync` feature.) A per-buffer `try_unfold`+`spawn_blocking` was rejected in
   review: it dispatches a task and allocates+zeroes a fresh buffer per hop.
3. **No GC guard on reads** — consistent with `0005`; the reader-vs-GC race stays
   a failed read, never corruption.
4. **Keep `read_version` as a capped wrapper over `reconstruct`** — the buffered
   in-memory convenience shares the one manifest-buffering path with
   `read_current`; the HTTP surface uses `open_version`/`ChunkStream` instead.

## Out of scope (later)

- `Range`/`206` for `?version=N` (start-chunk via manifest offsets; see above).
- Unifying `read_current` onto a shared reader (it is intentionally capped).
- An `ETag`/`Last-Modified` for historical versions (would enable conditional
  GETs and caching of old versions).
