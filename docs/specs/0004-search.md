# 0004 — Full-Text Search (Phase 5)

Status: Accepted
Date: 2026-07-05

A reverse (inverted) index over stored text content, exposed on two surfaces: a
browser-facing `GET …?q=…` and an RFC 5323 `SEARCH` method for WebDAV clients.
Engine: **tantivy** (resolved in `0001-initial-build-plan.md`, decision #2).

## Where the pieces live

- **`index` crate** — a thin, storage-agnostic wrapper over tantivy. It knows
  only *node ids* and *text bodies*: `index_document(node_id, body)`,
  `remove_document(node_id)`, `commit()`, `search(query, limit) -> [Hit]`. A hit
  is `{node_id, score, snippet}`. WebDAV-agnostic, so the storage engine keeps
  shipping without the server.
- **`vfs`** — owns the *policy*: which files are indexable, size caps, and
  wiring index updates into the write path. `DavFs::search` runs a query and
  resolves each hit's node id to its current path.
- **`webdav-server`** — the two query surfaces (router only; no storage logic).

## Keying on the node id (not the path)

Each document is keyed by the metadata store's **stable node id**, not the
virtual path. Consequences:

- A **move/rename needs no reindex** — the id is unchanged and the body is
  unchanged; only the path (resolved at query time) differs.
- A **content change** re-tokenizes (delete-by-id then add — tantivy has no
  in-place update).
- A **delete** removes by id.
- A hit's path is resolved from its id **at query time** (`MetaStore::path_of`
  walks to the root). A hit whose node was since deleted simply fails to resolve
  and is **dropped** — the index is lazily reconciled, so a missed removal is a
  correctness non-issue (only wasted space). This also makes moved files show
  their up-to-date location for free.

## What is indexed, and when

- **Text only, by extension** (`vfs::is_indexable_name`): Markdown, plain text,
  common config/data formats, and common source extensions. Binary media
  (images, audio, video) is never tokenized.
- **Size cap** `MAX_INDEX_BYTES` (4 MiB): a larger (or mislabeled) file is not
  read into memory to index; any stale document for it is removed.
- **On every content write** (`FileHandle::flush`, `DavFs::copy`,
  `DavFs::revert_to_version`) the node is re-synced with the index. Best-effort:
  the metadata/blob store is the source of truth and the index is derived, so a
  failure is **logged, not propagated** — it must not fail a write whose bytes
  are already durably stored. Indexing runs on the same blocking thread as the
  chunking work (or `spawn_blocking` for copy/delete) so it never stalls the
  async runtime.
- The index commits after each mutation, so a search reflects the latest content
  immediately. (Batching commits is a possible Phase-6 optimization; per-write
  commit is fine at personal scale.)

## Query grammar

tantivy's query parser with **AND as the default operator**: a multi-word query
*narrows* (all bare terms must match), `-term` excludes, `"a phrase"` is a
phrase. A malformed query is a **400**, not a silent empty result.

## Surface 1 — browser `GET …?q=…`

`?q=` on any path is a search scoped to that path's subtree (root = global). It
slots in beside `?raw` / `?versions` in the router's `GET` handling.

- **Browser** (`Accept: text/html`) → a results page in the two-pane shell: each
  hit links to its file with a highlighted snippet (matched terms in `<b>` —
  tantivy escapes the surrounding text, so the snippet is safe to inline).
- **Other clients** → a JSON array of `{path, score, snippet}`.
- A **global search box** lives in the sidebar on every page (a `GET /?q=…`
  form), so search is always reachable.

The `q` value is form-decoded (`+` → space, then percent-decoding). A blank
query falls through to normal page handling.

## Surface 2 — the `SEARCH` method (RFC 5323)

The router intercepts the `SEARCH` method (dav-server does not route it) and
answers the **free-text subset** of the `DAV:basicsearch` grammar — all our
full-text index can meaningfully answer:

- The query term is taken from `DAV:contains` (preferred) or a `DAV:like`
  `DAV:literal` (SQL `%`/`_` wildcards trimmed). Property/comparison predicates
  are **not** supported.
- The scope is the first `DAV:scope/DAV:href` in the body, falling back to the
  request path.
- The response is a **`207 Multi-Status`** of matching resources (`DAV:href` +
  `DAV:displayname`). The body is parsed with `quick-xml`, namespace-prefix
  agnostically (matching on local element names).
- `OPTIONS` advertises `DASL: <DAV:basicsearch>` so clients can discover support.

## Decisions

1. **Node-id keying** — moves/renames are free; paths resolve at query time;
   deletes are lazily reconciled. (§ above.)
2. **Text-only, extension-gated, size-capped** indexing — never tokenize binary
   media or huge files.
3. **Best-effort, logged index updates** — the derived index never fails a
   durable content write.
4. **AND-by-default grammar**, malformed query → 400.
5. **Free-text `DAV:basicsearch` subset** for SEARCH — the useful part of DASL
   for a full-text engine; a 207 of hrefs. Full predicate evaluation is out of
   scope.
6. **Per-write commit** for immediate searchability; batching deferred.

## Out of scope (later / Phase 6)

- Indexing pre-existing content on startup (greenfield: files are indexed as
  they're written; a one-shot reindex/rebuild command can be added later).
- Batched/asynchronous commits; a background index-maintenance task.
- Ranking tuning, per-field boosts, stemming/language analyzers, faceting.
- Full DASL predicate grammar (property comparisons, `DAV:and`/`DAV:or` trees).
- Auth on the search surfaces (part of the standalone AuthN/AuthZ future work,
  not a phase — see `0001-initial-build-plan.md` → "Future work").
