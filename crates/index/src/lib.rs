//! Reverse (inverted) full-text index over stored documents.
//!
//! A thin, storage-agnostic wrapper around [`tantivy`]. Each indexed document is
//! keyed by a stable **node id** (the metadata store's row id) and carries the
//! document's tokenized text `body`. Keying on the node id — not the virtual path
//! — means a move/rename needs no reindex (the id is stable); only a *content*
//! change re-tokenizes, and a delete removes by id. The caller (the `vfs` crate)
//! owns the policy of *what* to index (which extensions, size caps) and resolves a
//! hit's node id back to its current path; this crate only knows ids and text.
//!
//! Writes are made searchable by an explicit [`commit`](SearchIndex::commit) —
//! callers commit after each mutation so a search reflects the latest content.
//! tantivy allows only a single writer, so one is held behind a mutex.

use std::path::Path;
use std::sync::Mutex;

use tantivy::collector::TopDocs;
use tantivy::query::QueryParser;
use tantivy::schema::{FAST, INDEXED, STORED, Schema, TEXT, Value};
use tantivy::snippet::SnippetGenerator;
use tantivy::{Index, IndexReader, IndexWriter, TantivyDocument, Term, doc};

/// Memory budget (bytes) for the single index writer's indexing arena.
const WRITER_HEAP_BYTES: usize = 50 * 1024 * 1024;

/// A full-text index of document bodies, keyed by node id.
pub struct SearchIndex {
    index: Index,
    reader: IndexReader,
    writer: Mutex<IndexWriter>,
    fields: Fields,
}

/// The schema fields, resolved once at open.
struct Fields {
    node_id: tantivy::schema::Field,
    body: tantivy::schema::Field,
}

impl std::fmt::Debug for SearchIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SearchIndex").finish_non_exhaustive()
    }
}

/// One search hit: the document's node id, its relevance score, and an optional
/// text snippet with the matched terms wrapped in `<b>…</b>` (HTML-escaped).
#[derive(Debug, Clone)]
pub struct Hit {
    /// The node id of the matching document.
    pub node_id: u64,
    /// Relevance score (higher is more relevant).
    pub score: f32,
    /// A highlighted excerpt, if one could be generated.
    pub snippet: Option<String>,
}

/// Errors from the search index.
#[derive(Debug)]
pub enum IndexError {
    /// An underlying tantivy error.
    Tantivy(tantivy::TantivyError),
    /// The user's query string could not be parsed.
    BadQuery(String),
}

impl std::fmt::Display for IndexError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Tantivy(e) => write!(f, "search index error: {e}"),
            Self::BadQuery(q) => write!(f, "invalid search query: {q}"),
        }
    }
}

impl std::error::Error for IndexError {}

impl From<tantivy::TantivyError> for IndexError {
    fn from(e: tantivy::TantivyError) -> Self {
        IndexError::Tantivy(e)
    }
}

fn build_schema() -> (Schema, Fields) {
    let mut builder = Schema::builder();
    // The node id is INDEXED (so we can delete-by-term for upserts/removals),
    // STORED (so a hit can report it), and FAST (cheap to read back).
    let node_id = builder.add_u64_field("node_id", INDEXED | STORED | FAST);
    // The body is tokenized for search and STORED so snippets can be generated.
    let body = builder.add_text_field("body", TEXT | STORED);
    (builder.build(), Fields { node_id, body })
}

impl SearchIndex {
    /// Open (creating if needed) a search index under `dir`.
    ///
    /// The directory is created if absent. If it already holds an index its
    /// schema is reused; otherwise a fresh index is created.
    pub fn open(dir: impl AsRef<Path>) -> Result<Self, IndexError> {
        let dir = dir.as_ref();
        std::fs::create_dir_all(dir).map_err(tantivy::TantivyError::from)?;
        let (schema, fields) = build_schema();
        let mmap =
            tantivy::directory::MmapDirectory::open(dir).map_err(tantivy::TantivyError::from)?;
        // Reuse an existing index in this directory, or create one with our schema.
        let index = Index::open_or_create(mmap, schema)?;
        let reader = index.reader()?;
        let writer = index.writer(WRITER_HEAP_BYTES)?;
        Ok(Self {
            index,
            reader,
            writer: Mutex::new(writer),
            fields,
        })
    }

    /// Open an ephemeral, in-memory index (used by tests).
    pub fn open_in_memory() -> Result<Self, IndexError> {
        let (schema, fields) = build_schema();
        let index = Index::create_in_ram(schema);
        let reader = index.reader()?;
        let writer = index.writer(WRITER_HEAP_BYTES)?;
        Ok(Self {
            index,
            reader,
            writer: Mutex::new(writer),
            fields,
        })
    }

    fn writer(&self) -> std::sync::MutexGuard<'_, IndexWriter> {
        self.writer.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Insert or replace the document for `node_id` with `body`.
    ///
    /// Idempotent: any existing document with the same id is removed first, so a
    /// rewrite re-tokenizes rather than duplicating. Not searchable until
    /// [`commit`](Self::commit).
    pub fn index_document(&self, node_id: u64, body: &str) -> Result<(), IndexError> {
        let writer = self.writer();
        writer.delete_term(Term::from_field_u64(self.fields.node_id, node_id));
        writer.add_document(doc!(
            self.fields.node_id => node_id,
            self.fields.body => body,
        ))?;
        Ok(())
    }

    /// Remove the document for `node_id`, if present. Not effective until
    /// [`commit`](Self::commit). Removing an absent id is a no-op.
    pub fn remove_document(&self, node_id: u64) -> Result<(), IndexError> {
        let writer = self.writer();
        writer.delete_term(Term::from_field_u64(self.fields.node_id, node_id));
        Ok(())
    }

    /// Make all pending inserts/removals durable and searchable.
    pub fn commit(&self) -> Result<(), IndexError> {
        self.writer().commit()?;
        // Refresh the reader so subsequent searches see the committed state
        // immediately (deterministic, rather than waiting on the reload policy).
        self.reader.reload()?;
        Ok(())
    }

    /// Run `query` against the body field, returning up to `limit` hits ordered
    /// by descending relevance.
    ///
    /// The query uses tantivy's query grammar with **AND** as the default
    /// operator (all bare terms must match), so multi-word queries narrow rather
    /// than broaden; `-term` excludes and `"a phrase"` matches a phrase. A query
    /// that fails to parse yields [`IndexError::BadQuery`].
    pub fn search(&self, query: &str, limit: usize) -> Result<Vec<Hit>, IndexError> {
        let searcher = self.reader.searcher();
        let mut parser = QueryParser::for_index(&self.index, vec![self.fields.body]);
        parser.set_conjunction_by_default(); // all terms must match (AND) — fewer, better hits
        let parsed = parser
            .parse_query(query)
            .map_err(|e| IndexError::BadQuery(e.to_string()))?;

        let top = searcher.search(&parsed, &TopDocs::with_limit(limit).order_by_score())?;
        let mut snippet_gen = SnippetGenerator::create(&searcher, &parsed, self.fields.body)?;
        snippet_gen.set_max_num_chars(200);

        let mut hits = Vec::with_capacity(top.len());
        for (score, addr) in top {
            let stored: TantivyDocument = searcher.doc(addr)?;
            let Some(node_id) = stored
                .get_first(self.fields.node_id)
                .and_then(|v| v.as_u64())
            else {
                continue; // malformed doc without a node id; skip defensively
            };
            let snippet = {
                let s = snippet_gen.snippet_from_doc(&stored);
                let html = s.to_html();
                if html.is_empty() { None } else { Some(html) }
            };
            hits.push(Hit {
                node_id,
                score,
                snippet,
            });
        }
        Ok(hits)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn index_search_and_update() {
        let idx = SearchIndex::open_in_memory().unwrap();
        idx.index_document(1, "the quick brown fox jumps").unwrap();
        idx.index_document(2, "a lazy dog sleeps all day").unwrap();
        idx.commit().unwrap();

        let hits = idx.search("fox", 10).unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].node_id, 1);

        // Conjunction (AND) semantics: both terms must be present.
        assert_eq!(idx.search("quick fox", 10).unwrap().len(), 1);
        assert_eq!(idx.search("quick dog", 10).unwrap().len(), 0);

        // Reindexing the same id replaces its content (no duplicate, new terms).
        idx.index_document(1, "now about cats instead").unwrap();
        idx.commit().unwrap();
        assert_eq!(idx.search("fox", 10).unwrap().len(), 0);
        assert_eq!(idx.search("cats", 10).unwrap()[0].node_id, 1);
    }

    #[test]
    fn remove_document_drops_it_from_results() {
        let idx = SearchIndex::open_in_memory().unwrap();
        idx.index_document(7, "findable content here").unwrap();
        idx.commit().unwrap();
        assert_eq!(idx.search("findable", 10).unwrap().len(), 1);

        idx.remove_document(7).unwrap();
        idx.commit().unwrap();
        assert!(idx.search("findable", 10).unwrap().is_empty());
    }

    #[test]
    fn snippet_highlights_the_match() {
        let idx = SearchIndex::open_in_memory().unwrap();
        idx.index_document(1, "the needle is buried in a large haystack of words")
            .unwrap();
        idx.commit().unwrap();
        let hits = idx.search("needle", 10).unwrap();
        let snippet = hits[0].snippet.as_deref().unwrap();
        assert!(snippet.contains("<b>needle</b>"), "snippet was: {snippet}");
    }

    #[test]
    fn bad_query_is_reported() {
        let idx = SearchIndex::open_in_memory().unwrap();
        // An unbalanced quote is a parse error.
        assert!(matches!(
            idx.search("\"unterminated", 10),
            Err(IndexError::BadQuery(_))
        ));
    }

    #[test]
    fn persists_across_reopen() {
        let dir = tempfile::tempdir().unwrap();
        {
            let idx = SearchIndex::open(dir.path()).unwrap();
            idx.index_document(1, "durable across reopen").unwrap();
            idx.commit().unwrap();
        }
        let idx = SearchIndex::open(dir.path()).unwrap();
        assert_eq!(idx.search("durable", 10).unwrap()[0].node_id, 1);
    }
}
