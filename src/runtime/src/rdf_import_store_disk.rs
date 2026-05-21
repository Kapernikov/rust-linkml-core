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

// ── Key packing ─────────────────────────────────────────────────────────────

#[inline]
fn pack_spo(s: u64, p: u64, o: u64) -> [u8; 24] {
    let mut k = [0u8; 24];
    k[0..8].copy_from_slice(&s.to_be_bytes());
    k[8..16].copy_from_slice(&p.to_be_bytes());
    k[16..24].copy_from_slice(&o.to_be_bytes());
    k
}

#[inline]
fn pack_pos(p: u64, o: u64, s: u64) -> [u8; 24] {
    let mut k = [0u8; 24];
    k[0..8].copy_from_slice(&p.to_be_bytes());
    k[8..16].copy_from_slice(&o.to_be_bytes());
    k[16..24].copy_from_slice(&s.to_be_bytes());
    k
}

#[inline]
fn unpack_u64(slice: &[u8], offset: usize) -> u64 {
    let mut buf = [0u8; 8];
    buf.copy_from_slice(&slice[offset..offset + 8]);
    u64::from_be_bytes(buf)
}

// ── Load helpers (private) ──────────────────────────────────────────────────

impl DiskRdfImportStore {
    /// Get-or-insert a term, returning its u64 ID.
    fn intern(&mut self, term: Term) -> u64 {
        if let Some(&id) = self.id_by_term.get(&term) {
            return id;
        }
        let id = self.term_by_id.len() as u64;
        self.term_by_id.push(term.clone());
        self.id_by_term.insert(term, id);
        id
    }

    /// Insert one parsed triple into both index keyspaces.
    fn insert_triple(
        &mut self,
        subject: NamedOrBlankNode,
        predicate: NamedNode,
        object: Term,
    ) -> Result<(), DiskStoreError> {
        let subj_term: Term = match subject {
            NamedOrBlankNode::NamedNode(n) => Term::NamedNode(n),
            NamedOrBlankNode::BlankNode(b) => Term::BlankNode(b),
        };
        let pred_term: Term = Term::NamedNode(predicate);
        let sid = self.intern(subj_term);
        let pid = self.intern(pred_term);
        let oid = self.intern(object);
        self.spo.insert(pack_spo(sid, pid, oid), b"")?;
        self.pos.insert(pack_pos(pid, oid, sid), b"")?;
        self.triple_count += 1;
        Ok(())
    }

    /// Finalize the load: shrink the dictionary, write metadata, persist.
    fn finalize_load(&mut self) -> Result<(), DiskStoreError> {
        self.term_by_id.shrink_to_fit();
        let count_bytes = (self.triple_count as u64).to_be_bytes();
        self.meta.insert(b"len", &count_bytes)?;
        self.meta.insert(b"format_version", &1u32.to_be_bytes())?;
        self.db.persist(PersistMode::SyncAll)?;
        Ok(())
    }
}

// ── Public constructors ─────────────────────────────────────────────────────

use oxttl::NTriplesParser;

impl DiskRdfImportStore {
    /// Build a disk store by streaming an N-Triples document into fjall.
    /// `path` is the directory where the fjall database lives.
    pub fn from_ntriples(reader: impl Read, path: &Path) -> Result<Self, DiskStoreError> {
        let mut store = Self::open_empty(path)?;
        let parser = NTriplesParser::new().for_reader(reader);
        for result in parser {
            let triple = result.map_err(|e| DiskStoreError::Parse(e.to_string()))?;
            store.insert_triple(triple.subject, triple.predicate, triple.object)?;
        }
        store.finalize_load()?;
        Ok(store)
    }

    /// Read-only accessor for the cached triple count. Stable after load.
    pub fn triple_count(&self) -> usize {
        self.triple_count
    }
}
