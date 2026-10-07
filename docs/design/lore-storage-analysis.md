# Lore immutable storage: pros and cons for chishiki

An assessment of whether chishiki should replace its one-file-per-chunk blob
store with a pack-file model like the local immutable store in Epic Games' Lore
version control system. Written 2026-10-06.

**Conclusion: don't adopt it.** It is the same answer reached earlier for
RocksDB + BlobDB: the model pays off at many millions of blobs, and at personal
scale it costs the properties that make the current store safe and simple.

The description of Lore's store comes from `immutable-store-layout.md`, which
was derived from reading the Lore source, not from inspecting a store on disk.
The comparison is against chishiki's blob store as described in `CLAUDE.md`.

## The two models

### chishiki today

- Each chunk is one file at `<root>/<xx>/<hex>`, where `<hex>` is the BLAKE3
  hash of the content and `<xx>` is its first byte.
- The filesystem is the index: a blob's name is its address.
- A blob is written to a temp file and renamed into place, so each write is
  atomic and needs no coordination between writers.
- File manifests (the ordered chunk list of each version) live in SQLite
  (`version_chunks`).
- Garbage collection is mark-and-sweep: collect the referenced hashes from
  SQLite, enumerate the blob files, and `unlink` any that are unreferenced.

### Lore's local immutable store

- The first hash byte selects one of 256 **groups**. Each group has its own
  index and its own pack files.
- **Pack files** are payloads concatenated back to back, with no header,
  framing or internal index. They are capped at 3 GiB and written append-only.
- **Index bucket files** hold fixed 96-byte entries plus a sorted position
  array for binary search. An entry maps a hash to `(pack_file, pack_offset,
  size_payload)` and also carries a context, a partition, flags and a
  last-access timestamp.
- A group's bucket count grows along 1 → 32 → 64 → 128 → 256 as buckets pass
  1000 entries ("fan-out"), committed with a `level.pending` sentinel that is
  rolled forward on open.
- Payloads are appended to a pack immediately. The index is updated in memory
  and flushed by a background task about 5 seconds later.
- Space is reclaimed in two steps: eviction drops index entries by last-access
  time, and compaction later copies the still-referenced payloads into other
  packs and truncates the old one.

## Pros for chishiki

- **Far fewer files.** At the default 64 KiB average chunk size, 100 GB of
  non-media content is about 1.6 million blob files. Packs would turn that into
  a few dozen large files plus index buckets, which makes every tree walk
  cheaper: the GC's `list_hashes`, backups, `du`.
- **No per-blob filesystem overhead.** Small files stop costing an inode and a
  4 KiB block each. At 10,000 small Markdown notes this is only tens of MB.
- **Cheaper reads per chunk.** A `pread` on an already-open pack replaces an
  open/read/close per chunk, and an existence check hits an in-memory index
  instead of a `stat`.

One expected benefit does not materialise: **read locality**. Because packs are
sharded by the first hash byte, the chunks of a single file scatter across all
256 groups, so a sequential read of a file is not a sequential read of a pack.

## Cons for chishiki

- **A second index to keep crash-consistent.** The buckets, the fan-out ladder
  and the `level.pending` roll-forward exist because Lore has no database.
  chishiki already has SQLite, so that machinery would duplicate it.
- **Weaker durability.** A crash inside the ~5 second window between the pack
  append and the index flush leaves payload bytes that nothing points to. That
  is tolerable for Lore: its entries carry a "stored durably" flag and can be
  evicted, which suggests the local store is a cache of data also held
  elsewhere. chishiki's blob store is the only copy.
- **A lost index means lost data.** Packs are headerless and unframed, so a
  corrupt or missing bucket turns its payloads into unfindable bytes; the index
  cannot be rebuilt by scanning the packs. Today every blob is self-describing
  (its name is its hash), so a blob can be verified with `b3sum` and the whole
  store can be audited from the files alone.
- **GC becomes compaction.** Today's sweep is an `unlink` per dead blob.
  Compaction has to copy live payloads, rewrite index entries, truncate the old
  pack, keep free-space headroom for the copies, and persist a resume cursor.
  In chishiki it would run under the exclusive `gc_lock`, which already blocks
  all writes for the duration of a GC run.
- **Worse for backups.** Immutable one-per-blob files are ideal for rsync,
  restic or Time Machine: a file that exists never changes. Packs change on
  every append and are rewritten by compaction, so they are re-read or re-sent.
- **Write coordination.** Temp-file plus rename is atomic and lock-free.
  Appending to shared packs needs something like Lore's writable-set manager to
  hand out packs and offsets to concurrent writers.
- **Unused features.** Partition, context, last-access eviction, lazy hydration
  (entries with no local payload) and obliteration serve a multi-tenant client
  cache of a remote store. Most of each 96-byte entry would be dead weight for
  chishiki, and last-access tracking turns reads into index writes.
- **Worse at small scale.** As the layout analysis itself notes, a small store
  ends up with 256 groups, each holding a near-empty index, a level marker and
  a near-empty pack.

## If file count ever becomes a problem

The pain point would be the number of blob files, and I would not expect it
before roughly the 10-million-blob range (an estimate, not a measurement). Two
cheaper steps are available before a full pack store:

1. **Add a second fan-out level**: `<root>/<xx>/<yy>/<hex>`. This keeps every
   current property and only shrinks the directories.
2. **Pack only small blobs.** Keep `(hash → pack, offset, length)` in SQLite
   rather than in bespoke bucket files, and frame each record in the pack with
   its hash and length so the index can be rebuilt by scanning the packs. This
   gets most of the file-count benefit without the second index or the loss of
   recoverability.

## What carries over to another design

These points are not specific to chishiki and apply to any store choosing
between one-file-per-blob and packs:

- **Is the store the only copy, or a cache?** Lore's deferred index flush,
  eviction and payload-less entries all suit a store whose contents can be
  re-fetched. A sole-copy store needs the payload and its index entry made
  durable together, in a defined order.
- **Is there already a transactional database?** If so, put the blob location
  index in it. A hand-rolled index is only justified without one.
- **Can the index be rebuilt from the data?** Framed, self-describing pack
  records (as in git packs) make the index a derived structure. Unframed packs
  make the index a second source of truth.
- **How is space reclaimed?** Per-blob files reclaim with `unlink`. Packs need
  compaction, with its I/O cost, headroom requirement and crash-resume state.
- **How is the pack sharded?** Sharding by hash spreads writes evenly but
  destroys locality. Packing in arrival order keeps a file's chunks adjacent
  but needs a different way to pick the pack.
- **What does the backup tool see?** Immutable files are incremental for free;
  mutable packs are not.
- **What scale is realistic?** Count expected blobs (total bytes ÷ average
  chunk size) before paying for a design built for many millions of them.
