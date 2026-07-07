# 0007 — Integration Tests against a Live Server (Phase 6)

Status: Accepted
Date: 2026-07-06

Until now the server is covered only by **in-process unit tests** (the `vfs`,
`blobstore`, `index` crates and the router's parsing helpers). Nothing exercises
the assembled binary over a real socket with the real WebDAV method set — so a
regression in request routing, content negotiation, the version/search/GC HTTP
surfaces, or the interaction between them would slip through. This adds a
**black-box integration harness**: spawn the actual `webdav-server` binary and
drive it over HTTP with the full method set, asserting real round-trips.

It also lays the groundwork for the **locking/concurrency correctness** item: the
concurrency section here (concurrent writes, writes racing GC) is where real
races surface as failing tests to fix, rather than a speculative code audit.

## What "real client" means here

There are two senses of "real WebDAV client":

1. **The real wire protocol over a real socket** — sending genuine
   `PROPFIND`/`MKCOL`/`MOVE`/`COPY`/`LOCK`/`SEARCH` requests to the running
   binary and asserting the responses. This is fully automatable and portable.
2. **A specific third-party client** (Finder, `rclone`, Windows, `davfs2`) with
   its own quirks.

This harness does **(1)**. It does **not** do (2): none of those clients is
installed in CI (`ubuntu-latest`) or on the dev box, so gating tests on them
would mean they never run. Testing a specific client stays a **manual**,
documented step (see "Out of scope"). The in-Rust client still sends the exact
protocol bytes, so it validates the server's WebDAV behavior — just not one
vendor's idiosyncrasies.

## Design

### Harness: spawn the real binary

Integration tests live in `crates/webdav-server/tests/`. Cargo provides the built
binary's path to same-package integration tests as `CARGO_BIN_EXE_webdav-server`,
so a small `TestServer` helper:

1. Spawns the binary with `CHISHIKI_DATA=<fresh tempdir>` and
   `CHISHIKI_ADDR=127.0.0.1:0` (OS-assigned ephemeral port), stdout piped.
2. Reads the startup line to learn the **actual** bound address (race-free — the
   port is whatever the OS gave, reported by the server itself), and treats that
   line as the readiness signal.
3. Exposes a base URL and, on `Drop`, kills the child and removes the tempdir.

This requires a one-line production improvement: the binary must print the bound
address from `listener.local_addr()` rather than the *requested* `CHISHIKI_ADDR`,
so `:0` reports the real port. That is a genuine ops improvement too (operators
who bind `:0` learn the chosen port), with no change for a fixed-port config.

Spawning the real binary (rather than serving the router in-process) tests
exactly what ships — env-var config, the assembled `DavHandler` + router + lock
system, graceful shutdown — with no refactor of the working `main`. (An
in-process alternative would require splitting the bin into lib+bin; deferred as
unnecessary for now.)

### Client

Tests use `reqwest` (a **dev-dependency**, `default-features = false` — plain
HTTP, no TLS backend) from `#[tokio::test]`s. `reqwest` sends arbitrary methods
(`Method::from_bytes(b"PROPFIND")`), sets headers/bodies, does not error on 4xx/5xx
(so status assertions are clean), and makes concurrency tests trivial
(`tokio::join!` / `join_all`). `tempfile` (already a workspace dep) is added as a
dev-dependency for the per-server data dir.

### Coverage (first cut)

- **Core WebDAV round-trips**: `MKCOL` → `PUT` → `GET` (raw bytes) → `HEAD`;
  `PROPFIND` Depth 1 lists children; `MOVE`, `COPY`, `DELETE`; `404` on a missing
  path; `OPTIONS` advertises the `DAV:` and `DASL: <DAV:basicsearch>` headers.
- **Locking**: `LOCK` returns a `Lock-Token`; `UNLOCK` with that token succeeds
  (the `MemLs` path Finder/Windows rely on).
- **Content negotiation**: `GET` a `.md` with `Accept: text/html` → rendered HTML;
  without it → raw markdown bytes; `GET` a collection with `Accept: text/html` →
  the server's directory index.
- **Versioning**: two `PUT`s → `?versions` lists both; `?version=1` **streams**
  the historical bytes (exercising 0006); `POST ?revert=N` and `POST ?prune=N`
  behave and 303-redirect.
- **Search**: `PUT` text, then `GET ?q=term` finds it (JSON); the `SEARCH` method
  returns `207 Multi-Status`. Both assert a **negative control** — a non-matching
  document is *absent* from the results — so a "return everything" bug fails.
- **GC**: `POST /?gc` returns the stats JSON.
- **Concurrency (bridge to the locking item)**: many concurrent `PUT`s to distinct
  paths all succeed; concurrent multi-chunk `PUT`s to the *same* path each append a
  version (verified count — no lost updates) and the current content reconstructs
  to exactly one whole body (no corruption); and the current version streams back
  intact while old versions are **pruned** (freeing real blobs) and `POST /?gc`
  sweeps concurrently — the genuine GC-vs-prune/read race, not a no-op sweep.

Assertions favor status codes, headers, and byte/`contains` checks over parsing
full XML/JSON, to keep the harness dependency-light and robust to formatting.

## Decisions

1. **Black-box, spawn the real binary** — highest fidelity (tests the shipped
   artifact and its config/shutdown), zero refactor risk to `main`. In-process
   serving (needs a lib/bin split) is deferred.
2. **`:0` + report `local_addr()`** — race-free ephemeral ports; also a real ops
   improvement. Beats picking a port in-test (TOCTOU) or a fixed port (collides).
3. **`reqwest` dev-dependency, no TLS** — ergonomic arbitrary-method client with
   clean 4xx handling and easy concurrency; dev-only, never shipped.
4. **In-Rust protocol client, not third-party clients** — portable and CI-safe;
   third-party-client testing is a documented manual step.

## Out of scope (later)

- Automated tests against specific third-party clients (`rclone`, `cadaver`,
  Finder, Windows). Documented as a manual checklist instead; could become an
  opt-in CI job on a runner that installs the tools.
- In-process serving of the router (a lib/bin split) for faster, finer-grained
  tests, if subprocess startup cost or control becomes a problem.
- Property/fuzz testing of the WebDAV surface; load/perf benchmarks (separate
  Phase 6 item).
- TLS / auth flows (auth is future work, see 0001).
