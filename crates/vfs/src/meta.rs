//! SQLite-backed metadata store for the virtual namespace.
//!
//! The store models the folder/file hierarchy the client sees as a tree of
//! [`Node`]s (each a file or a collection). File content is *not* stored here —
//! a file node points at an ordered list of chunk references (its manifest) whose
//! bytes live in the content-addressable blob store. This is the seam between the
//! virtualized namespace and content-addressed storage.

use std::path::Path;
use std::sync::{Mutex, MutexGuard};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use blobstore::{ChunkRef, Hash, Manifest};
use rusqlite::{Connection, ErrorCode, OptionalExtension, params};

/// Row id of the always-present root collection.
pub const ROOT_ID: i64 = 1;

const NODE_COLUMNS: &str = "id, parent_id, name, is_dir, size, created, modified";

/// A node in the virtual namespace: either a file or a collection (directory).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Node {
    /// Row id.
    pub id: i64,
    /// Parent node id; `None` only for the root.
    pub parent_id: Option<i64>,
    /// Base name (raw bytes, as they appear in the path).
    pub name: Vec<u8>,
    /// Whether this node is a collection (directory).
    pub is_dir: bool,
    /// File size in bytes (0 for collections).
    pub size: u64,
    /// Creation time.
    pub created: SystemTime,
    /// Last-modified time.
    pub modified: SystemTime,
}

/// Errors from the metadata store.
#[derive(Debug)]
pub enum MetaError {
    /// The underlying SQLite call failed.
    Sqlite(rusqlite::Error),
    /// The requested node does not exist.
    NotFound,
    /// A node with that name already exists in the target collection.
    Exists,
    /// A collection that must be empty still has children.
    NotEmpty,
    /// Expected a collection but found a file.
    NotADirectory,
    /// Expected a file but found a collection.
    IsADirectory,
    /// Stored data could not be decoded (e.g. a malformed chunk hash).
    Corrupt,
}

impl std::fmt::Display for MetaError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Sqlite(e) => write!(f, "sqlite error: {e}"),
            Self::NotFound => write!(f, "node not found"),
            Self::Exists => write!(f, "node already exists"),
            Self::NotEmpty => write!(f, "collection is not empty"),
            Self::NotADirectory => write!(f, "not a collection"),
            Self::IsADirectory => write!(f, "is a collection"),
            Self::Corrupt => write!(f, "corrupt metadata"),
        }
    }
}

impl std::error::Error for MetaError {}

impl From<rusqlite::Error> for MetaError {
    fn from(e: rusqlite::Error) -> Self {
        MetaError::Sqlite(e)
    }
}

/// Result type for metadata operations.
pub type Result<T> = std::result::Result<T, MetaError>;

/// The SQLite metadata store.
#[derive(Debug)]
pub struct MetaStore {
    conn: Mutex<Connection>,
}

impl MetaStore {
    /// Open (creating and migrating if needed) the store at `path`.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let conn = Connection::open(path)?;
        Self::from_connection(conn)
    }

    /// Open an ephemeral in-memory store (used by tests).
    pub fn open_in_memory() -> Result<Self> {
        let conn = Connection::open_in_memory()?;
        Self::from_connection(conn)
    }

    fn from_connection(conn: Connection) -> Result<Self> {
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "foreign_keys", "ON")?;
        conn.busy_timeout(Duration::from_secs(5))?;
        let store = Self {
            conn: Mutex::new(conn),
        };
        store.migrate()?;
        Ok(store)
    }

    fn lock(&self) -> MutexGuard<'_, Connection> {
        // The mutex is only poisoned if a prior holder panicked mid-operation;
        // the connection is still usable, so recover rather than propagate.
        self.conn.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn migrate(&self) -> Result<()> {
        let conn = self.lock();
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS nodes (
                 id        INTEGER PRIMARY KEY,
                 parent_id INTEGER REFERENCES nodes(id) ON DELETE CASCADE,
                 name      BLOB    NOT NULL,
                 is_dir    INTEGER NOT NULL,
                 size      INTEGER NOT NULL DEFAULT 0,
                 created   INTEGER NOT NULL,
                 modified  INTEGER NOT NULL
             );
             CREATE UNIQUE INDEX IF NOT EXISTS idx_nodes_parent_name
                 ON nodes(parent_id, name);
             CREATE TABLE IF NOT EXISTS file_chunks (
                 node_id INTEGER NOT NULL REFERENCES nodes(id) ON DELETE CASCADE,
                 seq     INTEGER NOT NULL,
                 hash    TEXT    NOT NULL,
                 offset  INTEGER NOT NULL,
                 length  INTEGER NOT NULL,
                 PRIMARY KEY (node_id, seq)
             );",
        )?;
        let now = now_millis();
        conn.execute(
            "INSERT OR IGNORE INTO nodes (id, parent_id, name, is_dir, size, created, modified)
             VALUES (?1, NULL, x'', 1, 0, ?2, ?2)",
            params![ROOT_ID, now],
        )?;
        Ok(())
    }

    /// Fetch a node by id.
    pub fn get_node(&self, id: i64) -> Result<Node> {
        let conn = self.lock();
        conn.query_row(
            &format!("SELECT {NODE_COLUMNS} FROM nodes WHERE id = ?1"),
            [id],
            row_to_node,
        )
        .optional()?
        .ok_or(MetaError::NotFound)
    }

    /// Find a direct child of `parent_id` by name, if present.
    pub fn lookup_child(&self, parent_id: i64, name: &[u8]) -> Result<Option<Node>> {
        let conn = self.lock();
        Ok(conn
            .query_row(
                &format!("SELECT {NODE_COLUMNS} FROM nodes WHERE parent_id = ?1 AND name = ?2"),
                params![parent_id, name],
                row_to_node,
            )
            .optional()?)
    }

    /// Resolve a path (a sequence of name segments from the root) to a node.
    ///
    /// An empty `segments` slice resolves to the root collection.
    pub fn lookup_path(&self, segments: &[&[u8]]) -> Result<Node> {
        let mut current = self.get_node(ROOT_ID)?;
        for seg in segments {
            current = self
                .lookup_child(current.id, seg)?
                .ok_or(MetaError::NotFound)?;
        }
        Ok(current)
    }

    /// List the direct children of a collection, ordered by name.
    pub fn children(&self, parent_id: i64) -> Result<Vec<Node>> {
        let conn = self.lock();
        let mut stmt = conn.prepare(&format!(
            "SELECT {NODE_COLUMNS} FROM nodes WHERE parent_id = ?1 ORDER BY name"
        ))?;
        let rows = stmt.query_map([parent_id], row_to_node)?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    }

    /// Create an empty collection under `parent_id`. Errors with [`MetaError::Exists`]
    /// if the name is taken.
    pub fn create_dir(&self, parent_id: i64, name: &[u8]) -> Result<Node> {
        self.insert_node(parent_id, name, true)
    }

    /// Create an empty file under `parent_id`. Errors with [`MetaError::Exists`]
    /// if the name is taken.
    pub fn create_file(&self, parent_id: i64, name: &[u8]) -> Result<Node> {
        self.insert_node(parent_id, name, false)
    }

    fn insert_node(&self, parent_id: i64, name: &[u8], is_dir: bool) -> Result<Node> {
        let now = now_millis();
        let conn = self.lock();
        conn.execute(
            "INSERT INTO nodes (parent_id, name, is_dir, size, created, modified)
             VALUES (?1, ?2, ?3, 0, ?4, ?4)",
            params![parent_id, name, is_dir as i64, now],
        )
        .map_err(constraint_error)?;
        Ok(Node {
            id: conn.last_insert_rowid(),
            parent_id: Some(parent_id),
            name: name.to_vec(),
            is_dir,
            size: 0,
            created: millis_to_time(now),
            modified: millis_to_time(now),
        })
    }

    /// Replace a file's content with `manifest`, updating its size and mtime.
    ///
    /// Returns the modified time that was written, so callers (e.g. an open file
    /// handle) can report a timestamp consistent with what is now stored.
    pub fn set_file_content(&self, id: i64, manifest: &Manifest) -> Result<SystemTime> {
        let now = now_millis();
        let mut conn = self.lock();
        let tx = conn.transaction()?;
        tx.execute("DELETE FROM file_chunks WHERE node_id = ?1", [id])?;
        {
            let mut stmt = tx.prepare(
                "INSERT INTO file_chunks (node_id, seq, hash, offset, length)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
            )?;
            for (seq, chunk) in manifest.chunks.iter().enumerate() {
                stmt.execute(params![
                    id,
                    seq as i64,
                    chunk.hash.to_hex(),
                    chunk.offset as i64,
                    i64::from(chunk.length),
                ])?;
            }
        }
        let updated = tx.execute(
            "UPDATE nodes SET size = ?2, modified = ?3 WHERE id = ?1",
            params![id, manifest.total_size as i64, now],
        )?;
        if updated == 0 {
            return Err(MetaError::NotFound);
        }
        tx.commit()?;
        Ok(millis_to_time(now))
    }

    /// Load a file's manifest (its ordered chunk references).
    pub fn load_manifest(&self, id: i64) -> Result<Manifest> {
        let conn = self.lock();
        let mut stmt = conn.prepare(
            "SELECT hash, offset, length FROM file_chunks WHERE node_id = ?1 ORDER BY seq",
        )?;
        let rows = stmt.query_map([id], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, i64>(1)? as u64,
                row.get::<_, i64>(2)? as u32,
            ))
        })?;
        let mut manifest = Manifest::default();
        for row in rows {
            let (hex, offset, length) = row?;
            let hash = hex.parse::<Hash>().map_err(|_| MetaError::Corrupt)?;
            manifest.total_size = manifest.total_size.max(offset + u64::from(length));
            manifest.chunks.push(ChunkRef {
                hash,
                offset,
                length,
            });
        }
        Ok(manifest)
    }

    /// Delete a file node (and its chunk references via cascade).
    pub fn remove_file(&self, id: i64) -> Result<()> {
        let node = self.get_node(id)?;
        if node.is_dir {
            return Err(MetaError::IsADirectory);
        }
        self.delete_node(id)
    }

    /// Delete an empty collection. Errors with [`MetaError::NotEmpty`] otherwise.
    pub fn remove_dir(&self, id: i64) -> Result<()> {
        let node = self.get_node(id)?;
        if !node.is_dir {
            return Err(MetaError::NotADirectory);
        }
        let conn = self.lock();
        // Atomic emptiness-check-and-delete in a single statement, so a child
        // inserted concurrently between a separate check and delete can never be
        // silently cascade-deleted.
        let removed = conn.execute(
            "DELETE FROM nodes WHERE id = ?1
                 AND NOT EXISTS (SELECT 1 FROM nodes WHERE parent_id = ?1)",
            [id],
        )?;
        if removed == 0 {
            return Err(MetaError::NotEmpty);
        }
        Ok(())
    }

    /// Whether `ancestor` is `node` itself or one of its ancestors.
    ///
    /// Used to reject moving/copying a collection into its own subtree, which
    /// would otherwise create an orphaned parent cycle.
    pub fn is_ancestor_or_self(&self, ancestor: i64, node: i64) -> Result<bool> {
        let mut current = node;
        // Bounded to guard against a pre-existing cycle in the data.
        for _ in 0..10_000 {
            if current == ancestor {
                return Ok(true);
            }
            match self.get_node(current)?.parent_id {
                Some(parent) => current = parent,
                None => return Ok(false),
            }
        }
        Err(MetaError::Corrupt)
    }

    fn delete_node(&self, id: i64) -> Result<()> {
        let conn = self.lock();
        let removed = conn.execute("DELETE FROM nodes WHERE id = ?1", [id])?;
        if removed == 0 {
            return Err(MetaError::NotFound);
        }
        Ok(())
    }

    /// Move/rename a node to `(new_parent_id, new_name)`.
    ///
    /// A move does not change the resource's content, so its `modified` time is
    /// deliberately left untouched (preserving Last-Modified/ETag across MOVE).
    pub fn rename(&self, id: i64, new_parent_id: i64, new_name: &[u8]) -> Result<()> {
        let conn = self.lock();
        let updated = conn
            .execute(
                "UPDATE nodes SET parent_id = ?2, name = ?3 WHERE id = ?1",
                params![id, new_parent_id, new_name],
            )
            .map_err(constraint_error)?;
        if updated == 0 {
            return Err(MetaError::NotFound);
        }
        Ok(())
    }

    /// Set a node's last-modified time.
    pub fn set_modified(&self, id: i64, tm: SystemTime) -> Result<()> {
        let conn = self.lock();
        let updated = conn.execute(
            "UPDATE nodes SET modified = ?2 WHERE id = ?1",
            params![id, time_to_millis(tm)],
        )?;
        if updated == 0 {
            return Err(MetaError::NotFound);
        }
        Ok(())
    }
}

fn row_to_node(row: &rusqlite::Row<'_>) -> rusqlite::Result<Node> {
    Ok(Node {
        id: row.get(0)?,
        parent_id: row.get(1)?,
        name: row.get(2)?,
        is_dir: row.get::<_, i64>(3)? != 0,
        size: row.get::<_, i64>(4)? as u64,
        created: millis_to_time(row.get(5)?),
        modified: millis_to_time(row.get(6)?),
    })
}

/// Classify a SQLite constraint failure.
///
/// A UNIQUE/PRIMARY KEY violation means the name is taken ([`MetaError::Exists`]);
/// a FOREIGN KEY violation means the parent row vanished ([`MetaError::NotFound`]).
/// `ErrorCode::ConstraintViolation` covers both, so we discriminate on the
/// extended code rather than mapping every constraint failure to "exists".
fn constraint_error(e: rusqlite::Error) -> MetaError {
    if let rusqlite::Error::SqliteFailure(err, _) = &e
        && err.code == ErrorCode::ConstraintViolation
    {
        return match err.extended_code {
            rusqlite::ffi::SQLITE_CONSTRAINT_UNIQUE
            | rusqlite::ffi::SQLITE_CONSTRAINT_PRIMARYKEY => MetaError::Exists,
            rusqlite::ffi::SQLITE_CONSTRAINT_FOREIGNKEY => MetaError::NotFound,
            _ => MetaError::Sqlite(e),
        };
    }
    MetaError::Sqlite(e)
}

fn now_millis() -> i64 {
    time_to_millis(SystemTime::now())
}

fn time_to_millis(t: SystemTime) -> i64 {
    match t.duration_since(UNIX_EPOCH) {
        Ok(d) => d.as_millis() as i64,
        Err(e) => -(e.duration().as_millis() as i64),
    }
}

fn millis_to_time(ms: i64) -> SystemTime {
    if ms >= 0 {
        UNIX_EPOCH + Duration::from_millis(ms as u64)
    } else {
        UNIX_EPOCH - Duration::from_millis(ms.unsigned_abs())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn chunk(data: &[u8], offset: u64) -> ChunkRef {
        ChunkRef {
            hash: Hash::of(data),
            offset,
            length: data.len() as u32,
        }
    }

    #[test]
    fn root_exists_and_is_a_collection() {
        let store = MetaStore::open_in_memory().unwrap();
        let root = store.get_node(ROOT_ID).unwrap();
        assert!(root.is_dir);
        assert_eq!(root.parent_id, None);
        // The empty path resolves to root.
        assert_eq!(store.lookup_path(&[]).unwrap(), root);
    }

    #[test]
    fn create_and_resolve_nested_paths() {
        let store = MetaStore::open_in_memory().unwrap();
        let docs = store.create_dir(ROOT_ID, b"docs").unwrap();
        let sub = store.create_dir(docs.id, b"notes").unwrap();
        let file = store.create_file(sub.id, b"a.md").unwrap();

        assert_eq!(
            store.lookup_path(&[b"docs", b"notes", b"a.md"]).unwrap().id,
            file.id
        );
        assert!(matches!(
            store.lookup_path(&[b"docs", b"missing"]),
            Err(MetaError::NotFound)
        ));
    }

    #[test]
    fn duplicate_name_is_rejected() {
        let store = MetaStore::open_in_memory().unwrap();
        store.create_dir(ROOT_ID, b"x").unwrap();
        assert!(matches!(
            store.create_file(ROOT_ID, b"x"),
            Err(MetaError::Exists)
        ));
    }

    #[test]
    fn children_are_listed_sorted() {
        let store = MetaStore::open_in_memory().unwrap();
        store.create_file(ROOT_ID, b"b").unwrap();
        store.create_file(ROOT_ID, b"a").unwrap();
        store.create_dir(ROOT_ID, b"c").unwrap();
        let names: Vec<Vec<u8>> = store
            .children(ROOT_ID)
            .unwrap()
            .into_iter()
            .map(|n| n.name)
            .collect();
        assert_eq!(names, vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]);
    }

    #[test]
    fn manifest_roundtrip_updates_size() {
        let store = MetaStore::open_in_memory().unwrap();
        let file = store.create_file(ROOT_ID, b"data.bin").unwrap();

        let manifest = Manifest {
            total_size: 7,
            chunks: vec![chunk(b"abc", 0), chunk(b"defg", 3)],
        };
        store.set_file_content(file.id, &manifest).unwrap();

        let loaded = store.load_manifest(file.id).unwrap();
        assert_eq!(loaded, manifest);
        assert_eq!(store.get_node(file.id).unwrap().size, 7);
    }

    #[test]
    fn remove_dir_requires_empty() {
        let store = MetaStore::open_in_memory().unwrap();
        let dir = store.create_dir(ROOT_ID, b"d").unwrap();
        let file = store.create_file(dir.id, b"f").unwrap();
        assert!(matches!(store.remove_dir(dir.id), Err(MetaError::NotEmpty)));

        store.remove_file(file.id).unwrap();
        store.remove_dir(dir.id).unwrap();
        assert!(matches!(store.get_node(dir.id), Err(MetaError::NotFound)));
    }

    #[test]
    fn removing_a_file_cascades_its_chunks() {
        let store = MetaStore::open_in_memory().unwrap();
        let file = store.create_file(ROOT_ID, b"f").unwrap();
        store
            .set_file_content(
                file.id,
                &Manifest {
                    total_size: 3,
                    chunks: vec![chunk(b"abc", 0)],
                },
            )
            .unwrap();
        store.remove_file(file.id).unwrap();
        // The manifest is gone along with the node.
        assert!(store.load_manifest(file.id).unwrap().chunks.is_empty());
    }

    #[test]
    fn rename_moves_between_collections() {
        let store = MetaStore::open_in_memory().unwrap();
        let a = store.create_dir(ROOT_ID, b"a").unwrap();
        let b = store.create_dir(ROOT_ID, b"b").unwrap();
        let file = store.create_file(a.id, b"f").unwrap();

        store.rename(file.id, b.id, b"g").unwrap();
        assert!(matches!(
            store.lookup_path(&[b"a", b"f"]),
            Err(MetaError::NotFound)
        ));
        assert_eq!(store.lookup_path(&[b"b", b"g"]).unwrap().id, file.id);
    }

    #[test]
    fn rename_onto_existing_name_conflicts() {
        let store = MetaStore::open_in_memory().unwrap();
        let a = store.create_file(ROOT_ID, b"a").unwrap();
        store.create_file(ROOT_ID, b"b").unwrap();
        assert!(matches!(
            store.rename(a.id, ROOT_ID, b"b"),
            Err(MetaError::Exists)
        ));
    }

    #[test]
    fn is_ancestor_or_self_walks_up_the_tree() {
        let store = MetaStore::open_in_memory().unwrap();
        let a = store.create_dir(ROOT_ID, b"a").unwrap();
        let b = store.create_dir(a.id, b"b").unwrap();
        let c = store.create_dir(b.id, b"c").unwrap();

        assert!(store.is_ancestor_or_self(a.id, c.id).unwrap()); // a is above c
        assert!(store.is_ancestor_or_self(c.id, c.id).unwrap()); // self
        assert!(store.is_ancestor_or_self(ROOT_ID, c.id).unwrap()); // root above all
        assert!(!store.is_ancestor_or_self(c.id, a.id).unwrap()); // c is not above a
        assert!(!store.is_ancestor_or_self(b.id, a.id).unwrap());
    }

    #[test]
    fn rename_preserves_modified_time() {
        let store = MetaStore::open_in_memory().unwrap();
        let dir = store.create_dir(ROOT_ID, b"d").unwrap();
        let file = store.create_file(ROOT_ID, b"f").unwrap();
        let before = store.get_node(file.id).unwrap().modified;

        store.rename(file.id, dir.id, b"f").unwrap();
        assert_eq!(store.get_node(file.id).unwrap().modified, before);
    }
}
