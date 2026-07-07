//! Shared harness for the integration tests: spawn the real `webdav-server`
//! binary on an ephemeral port with a fresh data directory, and drive it over
//! HTTP. See `docs/specs/0007-integration-tests.md`.
//!
//! Each integration-test binary `mod common;`s this file, so a helper unused by
//! one of them is not dead overall.
#![allow(dead_code)]

use std::io::{BufRead, BufReader};
use std::process::{Child, Command, Stdio};
use std::sync::mpsc;
use std::time::Duration;

use reqwest::redirect::Policy;
use reqwest::{Client, Method, Response};
use tempfile::TempDir;

/// A running server instance backed by a throwaway data directory. Killed and
/// cleaned up on drop.
pub(crate) struct TestServer {
    child: Child,
    _data: TempDir,
    base: String,
    client: Client,
    /// A client that does *not* follow redirects, so a test can assert the exact
    /// 3xx + `Location` a browser-facing write returns (`reqwest` follows by
    /// default, which would otherwise hide the redirect behind its target).
    no_redirect: Client,
}

impl TestServer {
    /// Spawn the binary bound to `127.0.0.1:0` and wait until it reports the
    /// OS-assigned port on stdout.
    pub(crate) fn start() -> Self {
        let data = TempDir::new().expect("create temp data dir");
        let child = Command::new(env!("CARGO_BIN_EXE_webdav-server"))
            .env("CHISHIKI_DATA", data.path())
            .env("CHISHIKI_ADDR", "127.0.0.1:0")
            .stdout(Stdio::piped())
            // Let the server's stderr (error logs) surface in test output.
            .stderr(Stdio::inherit())
            .spawn()
            .expect("spawn webdav-server binary");

        // Construct `self` — which owns the child and kills it on drop — *before*
        // the readiness wait, so a panic in `await_listen_addr` (timeout, crashed
        // server) still reaps the process instead of leaking it (a std `Child`
        // does not kill on drop).
        let mut server = Self {
            child,
            _data: data,
            base: String::new(),
            client: Client::new(),
            no_redirect: Client::builder()
                .redirect(Policy::none())
                .build()
                .expect("build no-redirect client"),
        };
        let addr = server.await_listen_addr();
        server.base = format!("http://{addr}");
        server
    }

    /// Block until the child prints its "listening on http://ADDR" line and
    /// return `ADDR`. Distinguishes a slow start (timeout) from a server that
    /// died before listening (stdout EOF without the line).
    fn await_listen_addr(&mut self) -> String {
        let stdout = self.child.stdout.take().expect("child stdout piped");
        let (tx, rx) = mpsc::channel();
        // Read on a side thread so a hung start is bounded by recv_timeout rather
        // than blocking forever; scan lines so stray output before the banner
        // doesn't defeat discovery.
        std::thread::spawn(move || {
            for line in BufReader::new(stdout).lines() {
                let Ok(line) = line else { return };
                if let Some(addr) = parse_listen_addr(&line) {
                    let _ = tx.send(addr);
                    return;
                }
            }
            // Reached EOF without the banner: the server exited before listening.
            // Dropping `tx` here surfaces as `Disconnected` below.
        });
        match rx.recv_timeout(Duration::from_secs(20)) {
            Ok(addr) => addr,
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                panic!("webdav-server exited before it started listening (see stderr above)")
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {
                panic!("webdav-server did not report a listening address within 20s")
            }
        }
    }

    /// Absolute URL for a server path (e.g. `"/notes.md"`).
    pub(crate) fn url(&self, path: &str) -> String {
        format!("{}{}", self.base, path)
    }

    /// The server's origin (scheme + host), for the CSRF `Origin` header.
    pub(crate) fn origin(&self) -> &str {
        &self.base
    }

    /// Issue a request with an arbitrary method (including WebDAV verbs).
    pub(crate) fn request(&self, method: &str, path: &str) -> reqwest::RequestBuilder {
        let method = Method::from_bytes(method.as_bytes()).expect("valid HTTP method");
        self.client.request(method, self.url(path))
    }

    /// `PUT` `body` at `path`, asserting a 2xx result; returns the response.
    pub(crate) async fn put(&self, path: &str, body: impl Into<reqwest::Body>) -> Response {
        let resp = self
            .request("PUT", path)
            .body(body)
            .send()
            .await
            .expect("PUT send");
        assert!(
            resp.status().is_success(),
            "PUT {path} -> {}",
            resp.status()
        );
        resp
    }

    /// `GET` `path`; returns the response (no status assertion).
    pub(crate) async fn get(&self, path: &str) -> Response {
        self.request("GET", path).send().await.expect("GET send")
    }

    /// `MKCOL` `path`, asserting success. WebDAV requires a collection to exist
    /// before `PUT`ting a resource into it (a missing parent is a 409).
    pub(crate) async fn mkcol(&self, path: &str) {
        let resp = self
            .request("MKCOL", path)
            .send()
            .await
            .expect("MKCOL send");
        assert!(
            resp.status().is_success(),
            "MKCOL {path} -> {}",
            resp.status()
        );
    }

    /// `POST` a version-management / admin action (`?revert`, `?prune`, `?gc`)
    /// with the CSRF `Origin` header set, **without following redirects** — so a
    /// caller can assert the `303` and `Location` a browser write returns.
    pub(crate) async fn post_admin(&self, path: &str) -> Response {
        self.no_redirect
            .post(self.url(path))
            .header("Origin", self.origin())
            .send()
            .await
            .expect("admin POST send")
    }
}

/// Extract `ADDR` from a `"…listening on http://ADDR (data dir: …)"` banner line.
fn parse_listen_addr(line: &str) -> Option<String> {
    let rest = line.split("http://").nth(1)?;
    let addr = rest.split_whitespace().next()?;
    // Sanity-check it looks like host:port before trusting it.
    addr.contains(':').then(|| addr.to_string())
}

impl Drop for TestServer {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}
