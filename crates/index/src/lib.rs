//! Reverse (inverted) index for full-text search.
//!
//! Tokenizes text/markdown on write and maintains a searchable index (tantivy)
//! so documents can be found by query terms. Implemented in Phase 5
//! (see `docs/specs/0001-initial-build-plan.md`).

#[cfg(test)]
mod tests {
    #[test]
    fn smoke() {
        // Placeholder test so the crate builds and the test harness runs until
        // Phase 5 fills in the real API.
    }
}
