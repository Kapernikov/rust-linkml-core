//! Disk-backed RDF store implementing the [`TripleSource`] trait.
//!
//! Triples are stored in two fjall keyspaces (fjall's term for what other
//! engines call "column families" or "partitions"): `spo` (keyed by
//! `[sid, pid, oid]` big-endian) and `pos` (keyed by `[pid, oid, sid]`).
//! A monotonic-u64 term dictionary lives in RAM for the lifetime of the
//! store; refs handed back through the `TripleSource` trait point into it.
//!
//! Behind the `disk_graph` Cargo feature.

#![cfg(feature = "disk_graph")]
#![allow(unused_imports)]
// Many imports below are introduced in this scaffold and used by later
// tasks (term interning, TripleSource impl, tracking variant).

use std::cell::RefCell;
use std::collections::HashSet;
use std::io::Read;
use std::path::Path;

use fjall::{Database, Keyspace, KeyspaceCreateOptions, PersistMode};
use oxrdf::{NamedNode, NamedOrBlankNode, NamedOrBlankNodeRef, Term, TermRef, TripleRef};

use crate::triple_source::TripleSource;
use crate::turtle_import::ImportError;

/// Errors specific to constructing or operating a `DiskRdfImportStore`.
#[derive(Debug)]
pub enum DiskStoreError {
    Fjall(fjall::Error),
    Io(std::io::Error),
    Parse(String),
}

impl std::fmt::Display for DiskStoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DiskStoreError::Fjall(e) => write!(f, "fjall error: {e}"),
            DiskStoreError::Io(e) => write!(f, "io error: {e}"),
            DiskStoreError::Parse(s) => write!(f, "parse error: {s}"),
        }
    }
}

impl std::error::Error for DiskStoreError {}

impl From<fjall::Error> for DiskStoreError {
    fn from(e: fjall::Error) -> Self {
        DiskStoreError::Fjall(e)
    }
}

impl From<std::io::Error> for DiskStoreError {
    fn from(e: std::io::Error) -> Self {
        DiskStoreError::Io(e)
    }
}

impl From<DiskStoreError> for ImportError {
    fn from(e: DiskStoreError) -> Self {
        ImportError::Parse(e.to_string())
    }
}

/// Disk-backed RDF store. Term dictionary lives in RAM; triples live in fjall.
pub struct DiskRdfImportStore {
    #[allow(dead_code)]
    db: Database,
    spo: Keyspace,
    pos: Keyspace,
    meta: Keyspace,
    /// Term dictionary, ID -> Term. Stable for the lifetime of the store
    /// (never mutated after load completes).
    term_by_id: Vec<Term>,
    /// Inverse dictionary, Term -> ID. Used for external-term-to-ID lookup
    /// at read time as well as deduplication during load.
    id_by_term: std::collections::HashMap<Term, u64>,
    /// Cached triple count. Set at end of load.
    triple_count: usize,
}

impl DiskRdfImportStore {
    /// Open a new (empty) disk store at `path`. Any existing contents in
    /// the three keyspaces are wiped — v1 always treats the path as
    /// ours-to-overwrite.
    pub fn open_empty(path: &Path) -> Result<Self, DiskStoreError> {
        std::fs::create_dir_all(path)?;
        let db = Database::builder(path).open()?;
        let spo = db.keyspace("spo", KeyspaceCreateOptions::default)?;
        let pos = db.keyspace("pos", KeyspaceCreateOptions::default)?;
        let meta = db.keyspace("meta", KeyspaceCreateOptions::default)?;
        // Wipe any pre-existing contents so the path is genuinely "ours".
        // fjall iterator yields Guard; we ask for just the key.
        let spo_keys: Vec<_> = spo.iter().filter_map(|g| g.key().ok()).collect();
        for k in spo_keys {
            spo.remove(k)?;
        }
        let pos_keys: Vec<_> = pos.iter().filter_map(|g| g.key().ok()).collect();
        for k in pos_keys {
            pos.remove(k)?;
        }
        let meta_keys: Vec<_> = meta.iter().filter_map(|g| g.key().ok()).collect();
        for k in meta_keys {
            meta.remove(k)?;
        }
        Ok(Self {
            db,
            spo,
            pos,
            meta,
            term_by_id: Vec::new(),
            id_by_term: std::collections::HashMap::new(),
            triple_count: 0,
        })
    }
}

// `TripleSource` impl, term interning, and tracking variant arrive in
// subsequent tasks. Imports above are pre-declared so this file's
// dependency surface lands in one commit.
