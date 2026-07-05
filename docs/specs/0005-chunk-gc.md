# 0005 — Chunk Garbage Collection (Phase 6)

Status: Accepted
Date: 2026-07-05

Blobs in the content-addressable store were, until now, only ever *added* — so
pruning a version or deleting a file freed its metadata but left the underlying
chunks on disk forever (unbounded growth). This adds **mark-and-sweep garbage
collection** to reclaim chunk blobs that no version of any file references.

## Why mark-and-sweep (not ref-counting)

With content-defined chunking and cross-file/version dedup, a blob is live iff
*any* `version_chunks` row references its hash. Reference counting would have to
increment/decrement on every version add, prune, delete, copy, and identical-
content dedup — fragile, and a miscount silently corrupts or leaks. Mark-and-
sweep derives liveness directly from the source of truth (the metadata store)
each run, so it is self-correcting: it also reclaims blobs orphaned by a write
that stored chunks but failed before recording them. A periodic batch GC (à la
`git gc`) fits a personal server better than per-operation bookkeeping.

## Algorithm

1. **Mark** — `MetaStore::referenced_chunk_hashes()` → `SELECT DISTINCT hash
   FROM version_chunks`, the *live set* of every chunk hash referenced by any
   version of any file.
2. **Sweep** — `BlobStore::list_hashes()` enumerates the `<root>/<xx>/<hex>`
   layout (skipping in-progress temp files at the root and any non-hash names);
   each blob whose hash is not in the live set is deleted via
   `BlobStore::remove()`, which reports the bytes freed.

`DavFs::gc()` orchestrates this and returns `GcStats { blobs_scanned,
blobs_removed, bytes_reclaimed }`. It is O(number of blobs) disk work, so callers
run it off the async runtime (`spawn_blocking`).

## Concurrency safety

The hazard is any window in which a write **adds a chunk reference** to a hash
that GC's mark phase saw as unreferenced: if GC then deletes that blob, the write
dangles. An in-process `RwLock` (`Inner::gc_lock`) coordinates:

- **Every path that adds a chunk reference** holds a **shared (read)** guard
  across confirming the chunk is present/referenced and recording the new
  reference. These are: `FileHandle::flush` (stores brand-new blobs, guard around
  `store_file` + `set_file_content`) and the reference-only writes `DavFs::copy`
  and `DavFs::revert_to_version` (via `Inner::reference_under_guard`, guard around
  loading the source/version manifest + `set_file_content`).
- `DavFs::gc()` holds the **exclusive (write)** guard across mark + sweep.

So GC never overlaps a reference-add. It is **not** enough to guard only
`store_file`: `copy`/`revert` re-reference chunks that are live *at the instant
they load the manifest*, but a concurrent `remove_file`/`prune` can drop the
source's last reference in the window before they record their own — after which
those chunks are collectible. Guarding the whole load→reference span closes that
window (GC can't sweep until the guard is released, by which point the new
reference exists). Reads add no references and need no guard.

GC is conservative under races: an unreference that lands mid-sweep just defers
that blob's reclamation to the next run; a blob it can't delete is counted in
`GcStats.blobs_failed` and skipped rather than aborting the run.

**Write-stall trade-off.** The exclusive guard is held across the *entire* sweep
(O(number of blobs) of `stat`/`unlink`), so all content writes block for the
duration of a GC run. This is acceptable for an operator-triggered, infrequent GC
at personal scale; bounding the stall with incremental GC (batched sweeps that
re-mark, or per-blob re-checks) is a deferred refinement.

**Reader race.** A reader streams a version by reading its blobs lazily and holds
no guard. For a *live* version this is safe (its chunks are in the live set). But
a read of a version that is concurrently pruned/deleted **and** then GC'd can hit
a deleted blob and fail mid-stream — a failed read, never corruption, and only
against a client racing a delete. (Before GC existed the blob would have
lingered.)

**This guard is in-process.** GC must run inside the server process, not a second
process pointed at the same live data directory (the lock wouldn't coordinate
across processes). Hence the trigger is an admin endpoint, not a separate CLI.

## Trigger: `POST /?gc`

Store-wide, path-independent. Runs `DavFs::gc()` on a blocking task and returns
`{"scanned":N,"removed":N,"reclaimed":BYTES}`. It sits alongside the existing
version-management writes (`?revert`/`?prune`) and shares their **CSRF Origin
check**; like them it is currently **unauthenticated** (trusted-network
assumption — auth is a separate Phase 6 item). The per-file version page's copy
now says pruned chunks are reclaimed "the next time garbage collection runs."

## Decisions

1. **Mark-and-sweep, not ref-counting** — self-correcting, reclaims orphans,
   matches a batch-GC model.
2. **In-process `RwLock` coordination** — *every* reference-adding write (`flush`,
   `copy`, `revert`) takes a shared guard over its confirm-then-reference span;
   GC takes the exclusive guard over mark + sweep.
3. **Admin endpoint, not a CLI** — GC must share the running process so the
   in-process guard is meaningful and it needs no downtime.
4. **`blobstore` stays policy-free** — it only enumerates and removes; the live
   set and orchestration live in `vfs`.

## Out of scope (later)

- Automatic/scheduled GC (a background timer or a post-prune trigger). For now
  it is operator-invoked.
- Incremental GC that bounds the write-stall window (batched sweeps or per-blob
  re-checks under a short lock); the current sweep holds the exclusive guard for
  its whole duration.
- A dry-run / per-blob audit log of exactly which hashes were deleted.
- Compaction of the search index or SQLite (`VACUUM`); this reclaims blob files
  only. Pruned index docs are already reconciled lazily (see 0004).
- Cross-process GC (a filesystem lock so an offline CLI could run safely).
- Auth on the GC endpoint (shares the Phase-6 auth story).
