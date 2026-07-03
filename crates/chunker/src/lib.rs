//! Content-defined chunking via FastCDC.
//!
//! Splits byte streams into content-defined chunks so that only unique chunks
//! are persisted in the blob store. Implemented in Phase 1
//! (see `docs/specs/0001-initial-build-plan.md`).

#[cfg(test)]
mod tests {
    #[test]
    fn smoke() {
        // Placeholder test so the crate builds and the test harness runs until
        // Phase 1 fills in the real API.
    }
}
