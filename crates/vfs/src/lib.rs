//! Virtualized filesystem.
//!
//! Maps the virtual path namespace clients see onto file manifests in the blob
//! store, holds versioned metadata (SQLite via `rusqlite`), and implements the
//! `dav-server` filesystem traits. Implemented in Phases 2-3
//! (see `docs/specs/0001-initial-build-plan.md`).

#[cfg(test)]
mod tests {
    #[test]
    fn smoke() {
        // Placeholder test so the crate builds and the test harness runs until
        // Phase 2 fills in the real API.
    }
}
