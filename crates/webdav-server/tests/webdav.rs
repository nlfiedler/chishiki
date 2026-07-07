//! Black-box integration tests: spawn the real binary and drive it over HTTP
//! with the WebDAV method set. See `docs/specs/0007-integration-tests.md`.

mod common;

use common::TestServer;
use reqwest::StatusCode;

// ---------------------------------------------------------------------------
// Core WebDAV method round-trips
// ---------------------------------------------------------------------------

#[tokio::test]
async fn put_then_get_roundtrips() {
    let server = TestServer::start();
    server.put("/hello.txt", "hello world").await;

    let resp = server.get("/hello.txt").await;
    assert!(resp.status().is_success());
    assert_eq!(resp.text().await.unwrap(), "hello world");
}

#[tokio::test]
async fn head_returns_metadata_without_body() {
    let server = TestServer::start();
    server.put("/h.txt", "twelve chars").await; // 12 bytes

    let resp = server.request("HEAD", "/h.txt").send().await.unwrap();
    assert!(resp.status().is_success(), "HEAD -> {}", resp.status());
    assert_eq!(
        resp.headers()
            .get("content-length")
            .and_then(|v| v.to_str().ok()),
        Some("12"),
        "HEAD Content-Length should match the resource size"
    );
    // A HEAD response carries the headers but no body.
    assert!(
        resp.bytes().await.unwrap().is_empty(),
        "HEAD returned a body"
    );
}

#[tokio::test]
async fn mkcol_creates_a_collection_and_propfind_lists_it() {
    let server = TestServer::start();

    let resp = server.request("MKCOL", "/docs").send().await.unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);
    server.put("/docs/a.txt", "aaa").await;
    server.put("/docs/b.txt", "bbb").await;

    // PROPFIND Depth 1 should enumerate the collection's children.
    let resp = server
        .request("PROPFIND", "/docs")
        .header("Depth", "1")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::MULTI_STATUS); // 207
    let body = resp.text().await.unwrap();
    assert!(
        body.contains("a.txt"),
        "PROPFIND body missing a.txt:\n{body}"
    );
    assert!(
        body.contains("b.txt"),
        "PROPFIND body missing b.txt:\n{body}"
    );
}

#[tokio::test]
async fn move_and_copy_and_delete() {
    let server = TestServer::start();
    server.put("/src.txt", "payload").await;

    // MOVE /src.txt -> /moved.txt
    let resp = server
        .request("MOVE", "/src.txt")
        .header("Destination", server.url("/moved.txt"))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success(), "MOVE -> {}", resp.status());
    assert_eq!(
        server.get("/moved.txt").await.text().await.unwrap(),
        "payload"
    );
    assert_eq!(server.get("/src.txt").await.status(), StatusCode::NOT_FOUND);

    // COPY /moved.txt -> /copy.txt (original stays)
    let resp = server
        .request("COPY", "/moved.txt")
        .header("Destination", server.url("/copy.txt"))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success(), "COPY -> {}", resp.status());
    assert_eq!(
        server.get("/copy.txt").await.text().await.unwrap(),
        "payload"
    );
    assert_eq!(
        server.get("/moved.txt").await.text().await.unwrap(),
        "payload"
    );

    // DELETE /copy.txt
    let resp = server.request("DELETE", "/copy.txt").send().await.unwrap();
    assert!(resp.status().is_success(), "DELETE -> {}", resp.status());
    assert_eq!(
        server.get("/copy.txt").await.status(),
        StatusCode::NOT_FOUND
    );
}

#[tokio::test]
async fn missing_path_is_404() {
    let server = TestServer::start();
    assert_eq!(
        server.get("/nope.txt").await.status(),
        StatusCode::NOT_FOUND
    );
}

#[tokio::test]
async fn options_advertises_dav_and_dasl() {
    let server = TestServer::start();
    let resp = server.request("OPTIONS", "/").send().await.unwrap();
    assert!(resp.status().is_success());
    let headers = resp.headers();
    assert!(headers.contains_key("dav"), "OPTIONS missing DAV header");
    // The router advertises the RFC 5323 SEARCH grammar it supports.
    let dasl = headers
        .get("dasl")
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default();
    assert!(dasl.contains("basicsearch"), "DASL header was {dasl:?}");
}

#[tokio::test]
async fn lock_then_unlock() {
    let server = TestServer::start();
    server.put("/locked.txt", "content").await;

    let lock_body = r#"<?xml version="1.0" encoding="utf-8"?>
<D:lockinfo xmlns:D="DAV:">
  <D:lockscope><D:exclusive/></D:lockscope>
  <D:locktype><D:write/></D:locktype>
  <D:owner><D:href>test</D:href></D:owner>
</D:lockinfo>"#;
    let resp = server
        .request("LOCK", "/locked.txt")
        .header("Content-Type", "application/xml")
        .header("Timeout", "Second-3600")
        .body(lock_body)
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success(), "LOCK -> {}", resp.status());
    let token = resp
        .headers()
        .get("lock-token")
        .and_then(|v| v.to_str().ok())
        .expect("LOCK returned no Lock-Token")
        .to_string();
    // The token comes back bracketed as <urn:uuid:…>; UNLOCK wants it verbatim.
    let token = token.trim_matches(|c| c == '<' || c == '>').to_string();

    let resp = server
        .request("UNLOCK", "/locked.txt")
        .header("Lock-Token", format!("<{token}>"))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success(), "UNLOCK -> {}", resp.status());
}

// ---------------------------------------------------------------------------
// Content negotiation (browser vs. WebDAV client)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn markdown_negotiation_html_vs_raw() {
    let server = TestServer::start();
    server.put("/note.md", "# Title\n\nsome *text*").await;

    // A browser (Accept: text/html) gets rendered HTML.
    let resp = server
        .request("GET", "/note.md")
        .header("Accept", "text/html")
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());
    let ct = resp
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
        .to_string();
    let html = resp.text().await.unwrap();
    assert!(ct.contains("text/html"), "content-type was {ct:?}");
    assert!(
        html.contains("<h1") && html.contains("Title"),
        "expected rendered markdown, got:\n{html}"
    );

    // A WebDAV client (no text/html) gets the raw markdown bytes.
    let resp = server
        .request("GET", "/note.md")
        .header("Accept", "*/*")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.text().await.unwrap(), "# Title\n\nsome *text*");
}

#[tokio::test]
async fn collection_get_returns_index_page() {
    let server = TestServer::start();
    server.mkcol("/folder").await;
    server.put("/folder/inside.txt", "x").await;

    let resp = server
        .request("GET", "/folder/")
        .header("Accept", "text/html")
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());
    let body = resp.text().await.unwrap();
    assert!(
        body.contains("inside.txt"),
        "directory index missing entry:\n{body}"
    );
}

// ---------------------------------------------------------------------------
// Versioning (ties to 0006 streaming)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn versions_are_listed_and_old_bytes_stream_back() {
    let server = TestServer::start();
    server.put("/doc.md", "version one").await;
    server.put("/doc.md", "version two is longer").await;

    // ?versions lists both, newest current.
    let body = server.get("/doc.md?versions").await.text().await.unwrap();
    assert!(body.contains("\"number\":1"), "versions JSON: {body}");
    assert!(body.contains("\"number\":2"), "versions JSON: {body}");

    // ?version=1 streams the historical bytes; current GET serves version 2.
    assert_eq!(
        server.get("/doc.md?version=1").await.text().await.unwrap(),
        "version one"
    );
    assert_eq!(
        server.get("/doc.md").await.text().await.unwrap(),
        "version two is longer"
    );
}

#[tokio::test]
async fn large_version_streams_back_intact() {
    let server = TestServer::start();
    // ~3 MiB so it spans many chunks and exercises the streaming body.
    let big: Vec<u8> = (0..3_000_000u32)
        .map(|i| (i.wrapping_mul(2_654_435_761) >> 13) as u8)
        .collect();
    server.put("/big.bin", big.clone()).await;
    server.put("/big.bin", "small current").await;

    let resp = server.get("/big.bin?version=1").await;
    assert!(resp.status().is_success());
    assert_eq!(
        resp.headers()
            .get("content-length")
            .and_then(|v| v.to_str().ok()),
        Some(big.len().to_string().as_str())
    );
    let got = resp.bytes().await.unwrap();
    assert_eq!(got.len(), big.len());
    assert_eq!(got.as_ref(), big.as_slice());
}

#[tokio::test]
async fn revert_restores_old_content_as_new_version() {
    let server = TestServer::start();
    server.put("/r.txt", "first").await;
    server.put("/r.txt", "second").await;

    // POST ?revert=1 appends version 1's content as a new (current) version and
    // 303-redirects to the version page (asserted without following the redirect).
    let resp = server.post_admin("/r.txt?revert=1").await;
    assert_eq!(resp.status(), StatusCode::SEE_OTHER, "revert should 303");
    assert!(
        resp.headers().contains_key("location"),
        "revert 303 missing Location header"
    );
    assert_eq!(server.get("/r.txt").await.text().await.unwrap(), "first");
}

#[tokio::test]
async fn prune_removes_a_noncurrent_version() {
    let server = TestServer::start();
    server.put("/p.txt", "one").await;
    server.put("/p.txt", "two").await; // version 2 is current

    // POST ?prune=1 deletes the old, non-current version and 303-redirects.
    let resp = server.post_admin("/p.txt?prune=1").await;
    assert_eq!(resp.status(), StatusCode::SEE_OTHER, "prune should 303");

    // Version 1 is gone from history; the current content is untouched, and the
    // pruned version's bytes are no longer retrievable.
    let versions = server.get("/p.txt?versions").await.text().await.unwrap();
    assert!(
        !versions.contains("\"number\":1"),
        "pruned version still listed:\n{versions}"
    );
    assert!(
        versions.contains("\"number\":2"),
        "current version missing:\n{versions}"
    );
    assert_eq!(server.get("/p.txt").await.text().await.unwrap(), "two");
    assert_eq!(
        server.get("/p.txt?version=1").await.status(),
        StatusCode::NOT_FOUND
    );
}

// ---------------------------------------------------------------------------
// Search
// ---------------------------------------------------------------------------

#[tokio::test]
async fn full_text_search_finds_content() {
    let server = TestServer::start();
    server
        .put("/searchable.md", "the quick brown fox jumps")
        .await;
    server.put("/other.md", "nothing relevant here").await;

    // Browser GET ?q= returns JSON hits (non-text/html Accept -> JSON).
    let resp = server
        .request("GET", "/?q=brown")
        .header("Accept", "application/json")
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());
    let body = resp.text().await.unwrap();
    assert!(
        body.contains("searchable.md"),
        "search results missing the hit:\n{body}"
    );
    // Negative control: the non-matching file must NOT come back, or the query
    // isn't actually filtering (a "return everything" bug would pass otherwise).
    assert!(
        !body.contains("other.md"),
        "search returned a non-matching file:\n{body}"
    );
}

#[tokio::test]
async fn search_method_returns_multistatus() {
    let server = TestServer::start();
    server.put("/findme.md", "unicorn sighting reported").await;
    server
        .put("/decoy.md", "ordinary horse, nothing to see")
        .await;

    let search_body = r#"<?xml version="1.0" encoding="utf-8"?>
<D:searchrequest xmlns:D="DAV:">
  <D:basicsearch>
    <D:select><D:prop/></D:select>
    <D:from><D:scope><D:href>/</D:href><D:depth>infinity</D:depth></D:scope></D:from>
    <D:where><D:contains>unicorn</D:contains></D:where>
  </D:basicsearch>
</D:searchrequest>"#;
    let resp = server
        .request("SEARCH", "/")
        .header("Content-Type", "application/xml")
        .body(search_body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::MULTI_STATUS);
    let body = resp.text().await.unwrap();
    assert!(
        body.contains("findme.md"),
        "SEARCH body missing hit:\n{body}"
    );
    // Negative control: the non-matching document must be filtered out.
    assert!(
        !body.contains("decoy.md"),
        "SEARCH returned a non-matching file:\n{body}"
    );
}

// ---------------------------------------------------------------------------
// GC admin endpoint
// ---------------------------------------------------------------------------

#[tokio::test]
async fn gc_endpoint_returns_stats() {
    let server = TestServer::start();
    server.put("/g.txt", "some content to store").await;

    let resp = server
        .request("POST", "/?gc")
        .header("Origin", server.url(""))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success(), "gc -> {}", resp.status());
    let body = resp.text().await.unwrap();
    for key in ["scanned", "removed", "reclaimed"] {
        assert!(body.contains(key), "gc stats missing {key:?}:\n{body}");
    }
}

// ---------------------------------------------------------------------------
// Concurrency (bridge to the locking/concurrency-correctness item)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn concurrent_puts_to_distinct_paths_all_succeed() {
    let server = TestServer::start();
    server.mkcol("/c").await;
    let server = &server; // share one server across the concurrent futures
    let futures = (0..24).map(|i| {
        let path = format!("/c/file{i}.txt");
        let body = format!("content number {i}");
        async move {
            server.put(&path, body.clone()).await;
            assert_eq!(server.get(&path).await.text().await.unwrap(), body);
        }
    });
    futures_util::future::join_all(futures).await;
}

#[tokio::test]
async fn concurrent_puts_to_same_path_dont_corrupt() {
    let server = TestServer::start();
    let server = &server; // share one server across the concurrent futures
    server.put("/race.txt", "seed").await;

    // Distinct, multi-kilobyte bodies (so each spans several chunks and no write is
    // a byte-identical no-op). Hammer the same path concurrently.
    let bodies: Vec<Vec<u8>> = (0..16)
        .map(|i| {
            (0..80_000u32)
                .map(|j| (j.wrapping_mul(2_654_435_761).wrapping_add(i * 40_503) >> 13) as u8)
                .collect()
        })
        .collect();
    let writes = bodies.iter().cloned().map(|b| async move {
        server.put("/race.txt", b).await;
    });
    futures_util::future::join_all(writes).await;

    // The current content must reconstruct to exactly one *whole* written body —
    // not a torn mix of two (a real corruption bug would fail here).
    let final_body = server.get("/race.txt").await.bytes().await.unwrap();
    assert!(
        bodies.iter().any(|b| b.as_slice() == final_body.as_ref()),
        "final content is not any complete written body ({} bytes)",
        final_body.len()
    );
    // No lost updates: every distinct write appended a version (seed + 16 = 17).
    let versions = server.get("/race.txt?versions").await.text().await.unwrap();
    let count = versions.matches("\"number\":").count();
    assert_eq!(
        count,
        bodies.len() + 1,
        "expected every concurrent write to append a version:\n{versions}"
    );
}

#[tokio::test]
async fn current_version_survives_concurrent_prune_and_gc() {
    let server = TestServer::start();

    // Three distinct versions with distinct chunks. Pruning the older two frees
    // real (now-unreferenced) blobs for GC to actually reclaim — so this exercises
    // the genuine hazard (GC racing a prune) rather than a no-op sweep.
    let body = |seed: u64| -> Vec<u8> {
        (0..60_000u64)
            .map(|j| (j.wrapping_mul(2_654_435_761).wrapping_add(seed) >> 13) as u8)
            .collect()
    };
    let (v1, v2, current) = (body(1), body(2), body(3));
    server.put("/keep.bin", v1).await; // version 1
    server.put("/keep.bin", v2).await; // version 2
    server.put("/keep.bin", current.clone()).await; // version 3 (current)

    let server = &server;
    let current = &current;
    // Prune the two old versions (freeing their chunks) while GC sweeps and a
    // reader keeps pulling the current version. The current version is always
    // referenced, so its bytes must never be reclaimed out from under the reader.
    let prune = async {
        assert_eq!(
            server.post_admin("/keep.bin?prune=1").await.status(),
            StatusCode::SEE_OTHER
        );
        assert_eq!(
            server.post_admin("/keep.bin?prune=2").await.status(),
            StatusCode::SEE_OTHER
        );
    };
    let gc = async {
        for _ in 0..6 {
            let _ = server.post_admin("/?gc").await;
        }
    };
    let read = async {
        for _ in 0..6 {
            let got = server.get("/keep.bin").await.bytes().await.unwrap();
            assert_eq!(
                got.as_ref(),
                current.as_slice(),
                "current version corrupted mid-GC"
            );
        }
    };
    tokio::join!(prune, gc, read);

    // After the dust settles the current version is still fully intact.
    let got = server.get("/keep.bin").await.bytes().await.unwrap();
    assert_eq!(got.as_ref(), current.as_slice());
}
