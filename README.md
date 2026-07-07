# Chishiki

Basic WebDAV server with file auto-versioning, WebDAV Search, and a simple web interface.

## Features

* When accessed with a browser, renders Markdown, displays images and videos.
* Records multiple versions of each file with controls for reverting.
* Maintains a reverse index for searching textual files by query terms.

## Objective

The purpose of this project is to build a personal storage server that is
accessible both from a browser and via WebDAV clients. Use cases include:

* A simplistic wiki in which Markdown files are rendered as HTML.
* A file store for organizing documents and small collections of images and videos.
* A convenient place to store documents, automatically retaining older copies through auto-versioning.

## Implementation

* Content is stored in a content-addressable blob store.
* Files are chunked using FastCDC, storing only unique chunks.
* Files are assembled from chunks and streamed to the client.
* File versions consist of a manifest of the chunks for each revision.

## How to Use

Run the server [from the source tree](#build-and-run) with a Rust toolchain, or
[with Docker](#run-with-docker) using the provided `Dockerfile`.

### Prerequisites

* A **Rust** toolchain. The version is pinned by `rust-toolchain.toml`, so
  [rustup](https://rustup.rs/) installs the right one automatically on first
  build — you don't need to pick a version.
* A **C compiler** (`cc`/`clang`, e.g. from Xcode command-line tools or
  `build-essential`). SQLite is compiled from bundled source.

### Build and run

```sh
# From the repository root:
cargo run --release -p webdav-server
```

On startup the server prints the address it is listening on and the directory
where it stores data, for example:

```
chishiki webdav-server listening on http://127.0.0.1:4918 (data dir: ./data)
```

The data directory is created if it doesn't exist and contains the blob store
(`blobs/`), the metadata database (`metadata.sqlite`), and an upload staging area
(`tmp/`). Stop the server with Ctrl-C.

### Configuration

Configuration is via environment variables:

| Variable         | Default            | Description                                    |
| ---------------- | ------------------ | ---------------------------------------------- |
| `CHISHIKI_ADDR`  | `127.0.0.1:4918`   | Address (host:port) to listen on.              |
| `CHISHIKI_DATA`  | `./data`           | Directory holding the blob store and metadata. |

```sh
CHISHIKI_ADDR=0.0.0.0:8080 CHISHIKI_DATA=/srv/chishiki cargo run --release -p webdav-server
```

> **Note:** there is no authentication yet. Bind to `127.0.0.1` (the default) or
> otherwise restrict access to a trusted network.

### Run with Docker

A multi-stage `Dockerfile` builds the server and produces a slim runtime image
that runs as a non-root user. No local Rust or C toolchain is required — the
build stage supplies both.

```sh
# Build the image:
docker build -t chishiki .

# Run it, publishing the port and persisting data in a named volume:
docker run -d --name chishiki -p 4918:4918 -v chishiki-data:/data chishiki
```

Then browse to <http://127.0.0.1:4918/>. The container defaults differ from the
from-source defaults so it works out of the box:

* `CHISHIKI_ADDR` defaults to `0.0.0.0:4918` (rather than `127.0.0.1:4918`) so
  the server is reachable through the published port. Access is still gated by
  Docker's port publishing — bind the host side to a trusted interface (e.g.
  `-p 127.0.0.1:4918:4918`) since there is no authentication yet.
* `CHISHIKI_DATA` defaults to `/data`, exposed as a volume so the blob store,
  metadata database, and search index survive container restarts.

Override either with `-e`, e.g. `docker run -e CHISHIKI_ADDR=0.0.0.0:8080 …`.

### Accessing the server

**From a browser** (read-only) — open the listen address (e.g.
<http://127.0.0.1:4918/>) to browse directories, read rendered Markdown, and view
images and videos inline. Each file lists a **history** link to view, revert, or
delete older versions.

**From a WebDAV client** (read/write) — uploads, edits, moves, and deletes require
a WebDAV client, since browsers only issue plain `GET`. For example:

```sh
# rclone (one-off; or configure a remote)
rclone copy ./notes.md :webdav:/ --webdav-url http://127.0.0.1:4918

# macOS Finder: Go → Connect to Server (⌘K) → http://127.0.0.1:4918/
# Linux: mount with davfs2, or use cadaver
```

Or drive it directly with `curl`:

```sh
curl -T notes.md http://127.0.0.1:4918/notes.md          # upload (PUT)
curl http://127.0.0.1:4918/notes.md                      # download raw
curl -X MKCOL http://127.0.0.1:4918/docs                 # make a collection
curl http://127.0.0.1:4918/notes.md?versions             # version history (JSON)
curl http://127.0.0.1:4918/notes.md?version=1            # fetch a specific version
curl 'http://127.0.0.1:4918/?q=search+terms'             # full-text search (JSON)
curl -X POST http://127.0.0.1:4918/?gc                   # reclaim unreferenced chunks
```

### Development

See `CLAUDE.md` for the build/test/lint commands and the design overview, and
`docs/specs/` for the phased build plan and design notes.

## References

* [WebDAV](http://www.webdav.org)
* [Delta-V](https://www.rfc-editor.org/rfc/rfc3253) — inspiration only; the RFC 3253 protocol is deliberately not implemented (no client ecosystem — see `docs/specs/0001-initial-build-plan.md`)
* [WebDAV Search](https://www.rfc-editor.org/rfc/rfc5323)
* [awesome-webdav](https://github.com/fstanis/awesome-webdav)
