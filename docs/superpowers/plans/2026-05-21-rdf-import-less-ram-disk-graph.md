# Disk-Backed TripleSource via fjall — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an opt-in disk-backed `TripleSource` implementation backed by fjall, so RDF imports on 1 GB-scale datasets no longer hold the entire graph in RAM.

**Architecture:** New `DiskRdfImportStore` (and tracking sibling) parallel to the in-memory `RdfImportStore`, behind a `disk_graph` Cargo feature. Term dictionary held in RAM (v1); triples stored in two fjall partitions (SPO and POS) keyed by packed big-endian u64 IDs. Generalize the existing `OwnedImportStream` over `S: TripleSource + 'static` so both backends flow through the same streaming-iterator surface to `linkml-convert` and Python.

**Tech Stack:** Rust, fjall 3.1.x (pure-Rust LSM), oxrdf/oxttl (existing), pyo3 (existing).

**Spec:** [docs/superpowers/specs/2026-05-21-rdf-import-less-ram-disk-graph-design.md](../specs/2026-05-21-rdf-import-less-ram-disk-graph-design.md)

---

## File Structure

Files created or modified across all tasks. Each task only touches the files listed in its own `Files:` section, but this index helps orient.

**Created:**
- `src/runtime/src/rdf_import_store_disk.rs` — `DiskRdfImportStore`, `TrackingDiskRdfImportStore`, term-dictionary, fjall partition wiring. Gated `#[cfg(feature = "disk_graph")]`.

**Modified:**
- `src/runtime/Cargo.toml` — add `fjall` optional dep + `disk_graph` feature.
- `src/runtime/src/lib.rs` — `pub mod rdf_import_store_disk` under feature gate.
- `src/runtime/src/rdf_streaming.rs` — generalise `OwnedImportStream` and `import_owned_store_streaming` over `S: TripleSource + 'static`.
- `src/tools/src/bin/linkml_convert.rs` — `--disk-graph <PATH>` flag (feature-gated), branch on it to construct either `RdfImportStore` or `DiskRdfImportStore`.
- `src/python/Cargo.toml` — add forwarded `disk_graph` feature.
- `src/python/src/lib.rs` — `from_turtle_streaming_disk` + `from_ntriples_streaming_disk` entry points and `PyDiskTurtleStream` wrapper, all feature-gated.
- `scripts/measure_rinf_de.sh` — add the `streaming-disk` row.

**Tests added under `src/runtime/tests/`:**
- `disk_graph_basic.rs` — unit-level tests for the disk backend (round-trip, lookups, tracking).
- `disk_graph_parity.rs` — equivalence test: in-memory import vs disk-graph import produce the same instance set.

---

## Task 0: Add the feature flag and verify clean builds

**Goal:** Establish the `disk_graph` Cargo feature with fjall as an optional dep. Confirm that default builds (no feature) still compile and that builds with the feature also compile, before any code referencing fjall exists.

**Files:**
- Modify: `src/runtime/Cargo.toml`

- [ ] **Step 1: Add fjall as an optional dependency**

Edit `src/runtime/Cargo.toml`. In the `[dependencies]` section, add fjall right after the existing `oxttl` line:

```toml
oxttl = { version = "0.2", optional = true }
fjall = { version = "3.1", optional = true }
```

In `[features]`, add the new feature line right after `ttl = ...`:

```toml
ttl = ["oxrdf", "oxttl", "percent-encoding"]
disk_graph = ["ttl", "dep:fjall"]
```

- [ ] **Step 2: Verify default build still works**

Run: `cargo build -p linkml_runtime`
Expected: clean build, fjall NOT pulled in (you can confirm with `cargo tree -p linkml_runtime | grep fjall` → no output).

- [ ] **Step 3: Verify the feature compiles (no code yet)**

Run: `cargo build -p linkml_runtime --features disk_graph`
Expected: clean build. fjall now appears in the dep graph (`cargo tree -p linkml_runtime --features disk_graph | grep fjall` → shows fjall and its transitive deps).

- [ ] **Step 4: Commit**

```bash
git add src/runtime/Cargo.toml
git commit -m "$(cat <<'EOF'
feat(runtime): disk_graph cargo feature (fjall optional dep)

Establishes the compile-time opt-in. No code yet; subsequent tasks add the
DiskRdfImportStore module behind this feature.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 1: Generalize OwnedImportStream over the store type

**Goal:** Make `OwnedImportStream` and `import_owned_store_streaming` generic over `S: TripleSource + 'static`. The existing `RdfImportStore` path must remain byte-for-byte equivalent. This is a prerequisite for Task 8 wiring the disk backend into Python and Task 7 wiring it into `linkml-convert`.

**Files:**
- Modify: `src/runtime/src/rdf_streaming.rs:383-440`
- Modify: `src/python/src/lib.rs:1721` (one line: type annotation update)

- [ ] **Step 1: Generalize `OwnedImportStream`**

Replace the existing `OwnedImportStream` struct, `impl` blocks, and `import_owned_store_streaming` function (`rdf_streaming.rs:383-440`) with the following:

```rust
/// An owned-store streaming iterator: the parsed `TripleSource`
/// implementation is kept alive inside the iterator itself.
///
/// Used by FFI callers (Python) that can't easily keep a separate store
/// alive alongside the iterator. The store is boxed (so its address is
/// stable) and we erase the lifetime of the borrowing `ImportStream` via
/// one carefully-scoped `unsafe` block. Sound because the only reference
/// into the store is held by `iter`, which is dropped before `_store`
/// thanks to drop order (fields are dropped top-to-bottom).
pub struct OwnedImportStream<S: TripleSource + 'static> {
    iter: ImportStream<'static, S>,
    // Kept alive for `iter`'s reference. Must be declared AFTER `iter` so it
    // is dropped after; otherwise the &'static reference would dangle.
    #[allow(dead_code)]
    store: Box<S>,
    // Same for the SchemaView and Converter.
    #[allow(dead_code)]
    sv: Box<SchemaView>,
    #[allow(dead_code)]
    conv: Box<Converter>,
}

impl<S: TripleSource + 'static> OwnedImportStream<S> {
    pub fn consumed_count(&self) -> usize {
        self.iter.consumed_count()
    }

    /// Borrow the underlying store. Useful for backend-specific
    /// post-import diagnostics (e.g. unconsumed-subject reporting on a
    /// tracking store).
    pub fn store(&self) -> &S {
        &self.store
    }
}

impl<S: TripleSource + 'static> Iterator for OwnedImportStream<S> {
    type Item = Result<(String, LinkMLInstance), ImportError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
}

/// Build an owned streaming iterator from a parsed `TripleSource` and the
/// caller's schema/converter (which we take by value and box).
pub fn import_owned_store_streaming<S: TripleSource + 'static>(
    store: S,
    sv: SchemaView,
    conv: Converter,
    root_classes: &[&str],
) -> Result<OwnedImportStream<S>, ImportError> {
    let store_box: Box<S> = Box::new(store);
    let sv_box: Box<SchemaView> = Box::new(sv);
    let conv_box: Box<Converter> = Box::new(conv);

    // SAFETY: the references handed to `import_from_store_streaming` live as
    // long as the boxes that own them. The boxes are stored as later fields
    // of `OwnedImportStream`; Rust drops struct fields in declaration order,
    // so `iter` (which holds the references) is dropped BEFORE `store`,
    // `sv`, `conv`. Until then the references remain valid.
    let store_ref: &'static S = unsafe { &*(store_box.as_ref() as *const _) };
    let sv_ref: &'static SchemaView = unsafe { &*(sv_box.as_ref() as *const _) };
    let conv_ref: &'static Converter = unsafe { &*(conv_box.as_ref() as *const _) };

    let iter = import_from_store_streaming(store_ref, sv_ref, conv_ref, root_classes)?;

    Ok(OwnedImportStream {
        iter,
        store: store_box,
        sv: sv_box,
        conv: conv_box,
    })
}
```

- [ ] **Step 2: Fix Python wrapper's type annotation**

At `src/python/src/lib.rs:1721`, the `iter` field of `PyTurtleStream` is currently typed `Option<linkml_runtime::rdf_streaming::OwnedImportStream>`. With `OwnedImportStream` now generic, this must specify the type parameter. Replace:

```rust
pub struct PyTurtleStream {
    iter: Option<linkml_runtime::rdf_streaming::OwnedImportStream>,
    sv_py: Py<PySchemaView>,
}
```

with:

```rust
pub struct PyTurtleStream {
    iter: Option<
        linkml_runtime::rdf_streaming::OwnedImportStream<
            linkml_runtime::rdf_import_store::RdfImportStore,
        >,
    >,
    sv_py: Py<PySchemaView>,
}
```

- [ ] **Step 3: Build both crates to confirm nothing else broke**

Run: `cargo build -p linkml_runtime && cargo build -p linkml_runtime_python && cargo build -p linkml_tools`
Expected: all three crates build cleanly.

- [ ] **Step 4: Run the existing test suite to confirm semantics unchanged**

Run: `cargo test -p linkml_runtime --test rinf_import_test 2>&1 | tail -20`
Expected: all tests pass. (If `rinf_import_test` doesn't exist locally, run `cargo test -p linkml_runtime` and confirm the streaming tests pass.)

- [ ] **Step 5: Commit**

```bash
git add src/runtime/src/rdf_streaming.rs src/python/src/lib.rs
git commit -m "$(cat <<'EOF'
refactor(rdf_streaming): generalize OwnedImportStream over TripleSource

Enables a future disk-backed store to flow through the same owned-store
streaming-iterator surface as the in-memory RdfImportStore.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 2: Scaffold the disk-backed module (open/close fjall keyspace)

**Goal:** New module skeleton — opens a fjall keyspace at a caller-supplied path, creates `spo`, `pos`, and `meta` partitions, exposes a constructor that does nothing else yet. Test: it builds and you can open/close a store at a temp path without errors.

**Files:**
- Create: `src/runtime/src/rdf_import_store_disk.rs`
- Modify: `src/runtime/src/lib.rs` (add module declaration under feature gate)
- Create: `src/runtime/tests/disk_graph_basic.rs`

- [ ] **Step 1: Create the skeleton module**

Create `src/runtime/src/rdf_import_store_disk.rs` with this content:

```rust
//! Disk-backed RDF store implementing the [`TripleSource`] trait.
//!
//! Triples are stored in two fjall partitions: `spo` (keyed by
//! `[sid, pid, oid]` big-endian) and `pos` (keyed by `[pid, oid, sid]`).
//! A monotonic-u64 term dictionary lives in RAM for the lifetime of the
//! store; refs handed back through the `TripleSource` trait point into it.
//!
//! Behind the `disk_graph` Cargo feature.

#![cfg(feature = "disk_graph")]

use std::cell::RefCell;
use std::collections::HashSet;
use std::path::Path;

use fjall::{Config, Keyspace, PartitionCreateOptions, PartitionHandle};
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
    keyspace: Keyspace,
    spo: PartitionHandle,
    pos: PartitionHandle,
    meta: PartitionHandle,
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
    /// Open a new (empty) disk store at `path`. Any existing contents at
    /// the path are kept in fjall's keyspace but our partitions start
    /// empty — v1 always treats the path as ours-to-overwrite.
    pub(crate) fn open_empty(path: &Path) -> Result<Self, DiskStoreError> {
        std::fs::create_dir_all(path)?;
        let keyspace = Config::new(path).open()?;
        let spo = keyspace.open_partition("spo", PartitionCreateOptions::default())?;
        let pos = keyspace.open_partition("pos", PartitionCreateOptions::default())?;
        let meta = keyspace.open_partition("meta", PartitionCreateOptions::default())?;
        // Wipe any pre-existing contents so the path is genuinely "ours".
        for (k, _) in spo.iter().flatten() {
            spo.remove(k)?;
        }
        for (k, _) in pos.iter().flatten() {
            pos.remove(k)?;
        }
        for (k, _) in meta.iter().flatten() {
            meta.remove(k)?;
        }
        Ok(Self {
            keyspace,
            spo,
            pos,
            meta,
            term_by_id: Vec::new(),
            id_by_term: std::collections::HashMap::new(),
            triple_count: 0,
        })
    }
}

// `TripleSource` impl is added in Task 4.
```

- [ ] **Step 2: Add module declaration to lib.rs**

In `src/runtime/src/lib.rs`, after the existing `#[cfg(feature = "ttl")] pub mod rdf_streaming;` line (around line 38), add:

```rust
#[cfg(feature = "disk_graph")]
pub mod rdf_import_store_disk;
```

- [ ] **Step 3: Add a smoke test**

Create `src/runtime/tests/disk_graph_basic.rs`:

```rust
#![cfg(feature = "disk_graph")]

use linkml_runtime::rdf_import_store_disk::DiskRdfImportStore;
use tempfile::tempdir;

#[test]
fn opens_empty_store() {
    let dir = tempdir().unwrap();
    let _store = DiskRdfImportStore::open_empty(dir.path()).expect("open");
}

#[test]
fn reopens_existing_path_clean() {
    let dir = tempdir().unwrap();
    {
        let _store = DiskRdfImportStore::open_empty(dir.path()).expect("first open");
    }
    let _store = DiskRdfImportStore::open_empty(dir.path()).expect("second open (clean)");
}
```

- [ ] **Step 4: Run the tests**

Run: `cargo test -p linkml_runtime --features disk_graph --test disk_graph_basic 2>&1 | tail -10`
Expected: both tests pass.

- [ ] **Step 5: Verify default build still clean**

Run: `cargo build -p linkml_runtime`
Expected: clean. (Confirms the feature gate truly excludes the module.)

- [ ] **Step 6: Commit**

```bash
git add src/runtime/src/rdf_import_store_disk.rs src/runtime/src/lib.rs src/runtime/tests/disk_graph_basic.rs
git commit -m "$(cat <<'EOF'
feat(runtime): scaffold disk-backed RDF store (open/close)

Adds DiskRdfImportStore skeleton with fjall keyspace and three partitions
(spo, pos, meta). Smoke-test verifies open/close at a temp path.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 3: Bulk-load N-Triples into fjall partitions with term interning

**Goal:** `DiskRdfImportStore::from_ntriples(reader, path)` — parse an N-Triples stream, intern terms into the in-RAM dictionary, write `[sid, pid, oid]` triples to both `spo` and `pos` partitions. Persist the triple count to `meta`. Test on a small NT doc: confirms `len()` returns the right count (we'll add `TripleSource` impl in Task 4, but here we test `triple_count` directly via a temporary public getter).

**Files:**
- Modify: `src/runtime/src/rdf_import_store_disk.rs`
- Modify: `src/runtime/tests/disk_graph_basic.rs`

- [ ] **Step 1: Add packing helpers and intern function**

In `src/runtime/src/rdf_import_store_disk.rs`, add these helpers immediately after the `impl DiskRdfImportStore { ... }` block from Task 2:

```rust
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

    /// Insert one parsed triple into both index partitions.
    fn insert_triple(
        &mut self,
        subject: oxrdf::Subject,
        predicate: NamedNode,
        object: Term,
    ) -> Result<(), DiskStoreError> {
        // oxrdf::Subject is NamedNode | BlankNode | Triple. We don't support
        // rdf-star, so reject Triple subjects.
        let subj_term: Term = match subject {
            oxrdf::Subject::NamedNode(n) => Term::NamedNode(n),
            oxrdf::Subject::BlankNode(b) => Term::BlankNode(b),
            oxrdf::Subject::Triple(_) => {
                return Err(DiskStoreError::Parse(
                    "RDF-star nested triples not supported".to_string(),
                ));
            }
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
        // We don't need durable persist for a process-lifetime-only store,
        // but call it so that an interrupted run leaves a coherent SSTable
        // set on disk for debugging.
        self.keyspace
            .persist(fjall::PersistMode::SyncAll)?;
        Ok(())
    }
}
```

- [ ] **Step 2: Add the `from_ntriples` constructor**

Append to `src/runtime/src/rdf_import_store_disk.rs`:

```rust
// ── Public constructors ─────────────────────────────────────────────────────

use std::io::Read;

use oxttl::NTriplesParser;

impl DiskRdfImportStore {
    /// Build a disk store by streaming an N-Triples document into fjall.
    /// `path` is the directory where the fjall keyspace lives.
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
```

- [ ] **Step 3: Extend the smoke test**

Append to `src/runtime/tests/disk_graph_basic.rs`:

```rust
const TINY_NT: &str = "\
<http://example.org/a> <http://example.org/p> <http://example.org/b> .
<http://example.org/a> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://example.org/T> .
<http://example.org/b> <http://example.org/q> \"hello\" .
";

#[test]
fn loads_tiny_ntriples() {
    let dir = tempdir().unwrap();
    let store =
        DiskRdfImportStore::from_ntriples(std::io::Cursor::new(TINY_NT), dir.path()).unwrap();
    assert_eq!(store.triple_count(), 3);
}
```

- [ ] **Step 4: Run the test**

Run: `cargo test -p linkml_runtime --features disk_graph --test disk_graph_basic loads_tiny_ntriples 2>&1 | tail -10`
Expected: passes, prints `test result: ok. 1 passed`.

- [ ] **Step 5: Run all disk_graph tests to confirm no regressions**

Run: `cargo test -p linkml_runtime --features disk_graph --test disk_graph_basic 2>&1 | tail -10`
Expected: all three tests (from Tasks 2 and 3) pass.

- [ ] **Step 6: Commit**

```bash
git add src/runtime/src/rdf_import_store_disk.rs src/runtime/tests/disk_graph_basic.rs
git commit -m "$(cat <<'EOF'
feat(runtime): bulk-load N-Triples into fjall via DiskRdfImportStore

Streaming load with monotonic u64 term interning. Triples written to both
SPO and POS partitions; triple count persisted to meta. Term dictionary
held in RAM.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 4: Implement `TripleSource` on `DiskRdfImportStore`

**Goal:** Make `DiskRdfImportStore` queryable via the trait. Each method translates external term refs to IDs via `id_by_term`, prefix-scans the appropriate partition, decodes IDs from the key suffix, and returns refs into `term_by_id`. Tests verify each lookup against the tiny graph.

**Files:**
- Modify: `src/runtime/src/rdf_import_store_disk.rs`
- Modify: `src/runtime/tests/disk_graph_basic.rs`

- [ ] **Step 1: Implement the trait**

Append to `src/runtime/src/rdf_import_store_disk.rs`:

```rust
// ── TripleSource impl ───────────────────────────────────────────────────────

impl DiskRdfImportStore {
    fn lookup_named_id(&self, n: &NamedNode) -> Option<u64> {
        self.id_by_term
            .get(&Term::NamedNode(n.clone()))
            .copied()
    }

    fn lookup_subject_id(&self, s: &NamedOrBlankNode) -> Option<u64> {
        let term = match s {
            NamedOrBlankNode::NamedNode(n) => Term::NamedNode(n.clone()),
            NamedOrBlankNode::BlankNode(b) => Term::BlankNode(b.clone()),
        };
        self.id_by_term.get(&term).copied()
    }
}

impl TripleSource for DiskRdfImportStore {
    fn subjects_for_predicate_object<'a>(
        &'a self,
        predicate: &NamedNode,
        object: &NamedNode,
    ) -> Box<dyn Iterator<Item = NamedOrBlankNodeRef<'a>> + 'a> {
        let pid = match self.lookup_named_id(predicate) {
            Some(i) => i,
            None => return Box::new(std::iter::empty()),
        };
        let oid = match self.lookup_named_id(object) {
            Some(i) => i,
            None => return Box::new(std::iter::empty()),
        };
        let mut prefix = [0u8; 16];
        prefix[0..8].copy_from_slice(&pid.to_be_bytes());
        prefix[8..16].copy_from_slice(&oid.to_be_bytes());
        let term_by_id = &self.term_by_id;
        Box::new(
            self.pos
                .prefix(prefix)
                .filter_map(|res| res.ok())
                .filter_map(move |(key, _)| {
                    if key.len() != 24 {
                        return None;
                    }
                    let sid = unpack_u64(&key, 16) as usize;
                    let term: &'a Term = term_by_id.get(sid)?;
                    match term.as_ref() {
                        TermRef::NamedNode(n) => Some(NamedOrBlankNodeRef::NamedNode(n)),
                        TermRef::BlankNode(b) => Some(NamedOrBlankNodeRef::BlankNode(b)),
                        TermRef::Literal(_) => None, // subjects can't be literals
                    }
                }),
        )
    }

    fn objects_for_subject_predicate<'a>(
        &'a self,
        subject: &NamedOrBlankNode,
        predicate: &NamedNode,
    ) -> Box<dyn Iterator<Item = TermRef<'a>> + 'a> {
        let sid = match self.lookup_subject_id(subject) {
            Some(i) => i,
            None => return Box::new(std::iter::empty()),
        };
        let pid = match self.lookup_named_id(predicate) {
            Some(i) => i,
            None => return Box::new(std::iter::empty()),
        };
        let mut prefix = [0u8; 16];
        prefix[0..8].copy_from_slice(&sid.to_be_bytes());
        prefix[8..16].copy_from_slice(&pid.to_be_bytes());
        let term_by_id = &self.term_by_id;
        Box::new(
            self.spo
                .prefix(prefix)
                .filter_map(|res| res.ok())
                .filter_map(move |(key, _)| {
                    if key.len() != 24 {
                        return None;
                    }
                    let oid = unpack_u64(&key, 16) as usize;
                    let term: &'a Term = term_by_id.get(oid)?;
                    Some(term.as_ref())
                }),
        )
    }

    fn triples_for_subject<'a>(
        &'a self,
        subject: &NamedOrBlankNode,
    ) -> Box<dyn Iterator<Item = TripleRef<'a>> + 'a> {
        let sid = match self.lookup_subject_id(subject) {
            Some(i) => i,
            None => return Box::new(std::iter::empty()),
        };
        let prefix = sid.to_be_bytes();
        let term_by_id = &self.term_by_id;
        Box::new(
            self.spo
                .prefix(prefix)
                .filter_map(|res| res.ok())
                .filter_map(move |(key, _)| {
                    if key.len() != 24 {
                        return None;
                    }
                    let s_id = unpack_u64(&key, 0) as usize;
                    let p_id = unpack_u64(&key, 8) as usize;
                    let o_id = unpack_u64(&key, 16) as usize;
                    let s_term = term_by_id.get(s_id)?;
                    let p_term = term_by_id.get(p_id)?;
                    let o_term = term_by_id.get(o_id)?;
                    let subj = match s_term.as_ref() {
                        TermRef::NamedNode(n) => oxrdf::SubjectRef::NamedNode(n),
                        TermRef::BlankNode(b) => oxrdf::SubjectRef::BlankNode(b),
                        TermRef::Literal(_) => return None,
                    };
                    let pred = match p_term.as_ref() {
                        TermRef::NamedNode(n) => n,
                        _ => return None,
                    };
                    Some(TripleRef {
                        subject: subj,
                        predicate: pred,
                        object: o_term.as_ref(),
                    })
                }),
        )
    }

    fn len(&self) -> Option<usize> {
        Some(self.triple_count)
    }

    fn on_consumed(&self, _subject: &str, _predicate: &str, _object: &str) {
        // no-op for the non-tracking variant
    }
}
```

- [ ] **Step 2: Add lookup-equivalence tests**

Append to `src/runtime/tests/disk_graph_basic.rs`:

```rust
use linkml_runtime::triple_source::TripleSource;
use oxrdf::{NamedNode, NamedOrBlankNode};

#[test]
fn triples_for_subject_finds_all() {
    let dir = tempdir().unwrap();
    let store =
        DiskRdfImportStore::from_ntriples(std::io::Cursor::new(TINY_NT), dir.path()).unwrap();
    let s = NamedOrBlankNode::NamedNode(NamedNode::new_unchecked("http://example.org/a"));
    let triples: Vec<_> = store.triples_for_subject(&s).collect();
    assert_eq!(triples.len(), 2);
}

#[test]
fn subjects_for_predicate_object_finds_typed() {
    let dir = tempdir().unwrap();
    let store =
        DiskRdfImportStore::from_ntriples(std::io::Cursor::new(TINY_NT), dir.path()).unwrap();
    let p = NamedNode::new_unchecked("http://www.w3.org/1999/02/22-rdf-syntax-ns#type");
    let o = NamedNode::new_unchecked("http://example.org/T");
    let subs: Vec<_> = store.subjects_for_predicate_object(&p, &o).collect();
    assert_eq!(subs.len(), 1);
    assert_eq!(subs[0].to_string(), "<http://example.org/a>");
}

#[test]
fn objects_for_subject_predicate_finds_object() {
    let dir = tempdir().unwrap();
    let store =
        DiskRdfImportStore::from_ntriples(std::io::Cursor::new(TINY_NT), dir.path()).unwrap();
    let s = NamedOrBlankNode::NamedNode(NamedNode::new_unchecked("http://example.org/a"));
    let p = NamedNode::new_unchecked("http://example.org/p");
    let objs: Vec<_> = store.objects_for_subject_predicate(&s, &p).collect();
    assert_eq!(objs.len(), 1);
    assert_eq!(objs[0].to_string(), "<http://example.org/b>");
}

#[test]
fn len_matches_triple_count() {
    let dir = tempdir().unwrap();
    let store =
        DiskRdfImportStore::from_ntriples(std::io::Cursor::new(TINY_NT), dir.path()).unwrap();
    assert_eq!(store.len(), Some(3));
    assert!(!store.is_empty());
}

#[test]
fn lookup_of_unknown_term_returns_empty() {
    let dir = tempdir().unwrap();
    let store =
        DiskRdfImportStore::from_ntriples(std::io::Cursor::new(TINY_NT), dir.path()).unwrap();
    let s =
        NamedOrBlankNode::NamedNode(NamedNode::new_unchecked("http://example.org/does-not-exist"));
    let n: usize = store.triples_for_subject(&s).count();
    assert_eq!(n, 0);
}
```

- [ ] **Step 3: Run the tests**

Run: `cargo test -p linkml_runtime --features disk_graph --test disk_graph_basic 2>&1 | tail -15`
Expected: all tests pass (8 tests total at this point).

- [ ] **Step 4: Commit**

```bash
git add src/runtime/src/rdf_import_store_disk.rs src/runtime/tests/disk_graph_basic.rs
git commit -m "$(cat <<'EOF'
feat(runtime): TripleSource impl for DiskRdfImportStore

Three lookup methods translate external term refs to internal u64 IDs,
prefix-scan SPO/POS in fjall, decode IDs from key suffixes, and return
refs into the in-RAM term dictionary. Lifetime-clean (no unsafe).

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 5: Turtle + format-dispatch constructors

**Goal:** Mirror `RdfImportStore`'s `from_turtle` and `from_rdf` constructors so callers have full format coverage. Factor the parse-and-insert loop so we don't duplicate logic.

**Files:**
- Modify: `src/runtime/src/rdf_import_store_disk.rs`
- Modify: `src/runtime/tests/disk_graph_basic.rs`

- [ ] **Step 1: Refactor and add Turtle/format-dispatch constructors**

In `src/runtime/src/rdf_import_store_disk.rs`, replace the `from_ntriples` implementation (added in Task 3) and add the new constructors. Find the block:

```rust
impl DiskRdfImportStore {
    /// Build a disk store by streaming an N-Triples document into fjall.
    /// `path` is the directory where the fjall keyspace lives.
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
```

Replace with:

```rust
use oxttl::TurtleParser;

use crate::turtle_import::RdfFormat;

impl DiskRdfImportStore {
    /// Build a disk store by streaming N-Triples into fjall.
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

    /// Build a disk store by streaming Turtle into fjall.
    pub fn from_turtle(reader: impl Read, path: &Path) -> Result<Self, DiskStoreError> {
        let mut store = Self::open_empty(path)?;
        let parser = TurtleParser::new().for_reader(reader);
        for result in parser {
            let triple = result.map_err(|e| DiskStoreError::Parse(e.to_string()))?;
            store.insert_triple(triple.subject, triple.predicate, triple.object)?;
        }
        store.finalize_load()?;
        Ok(store)
    }

    /// Format-dispatch constructor.
    pub fn from_rdf(
        reader: impl Read,
        format: RdfFormat,
        path: &Path,
    ) -> Result<Self, DiskStoreError> {
        match format {
            RdfFormat::Turtle => Self::from_turtle(reader, path),
            RdfFormat::NTriples => Self::from_ntriples(reader, path),
        }
    }

    /// Read-only accessor for the cached triple count. Stable after load.
    pub fn triple_count(&self) -> usize {
        self.triple_count
    }
}
```

- [ ] **Step 2: Add a Turtle test**

Append to `src/runtime/tests/disk_graph_basic.rs`:

```rust
const TINY_TTL: &str = "\
@prefix : <http://example.org/> .
:a :p :b ;
   a :T .
:b :q \"hello\" .
";

#[test]
fn loads_tiny_turtle() {
    let dir = tempdir().unwrap();
    let store =
        DiskRdfImportStore::from_turtle(std::io::Cursor::new(TINY_TTL), dir.path()).unwrap();
    assert_eq!(store.triple_count(), 3);
}
```

- [ ] **Step 3: Run the test**

Run: `cargo test -p linkml_runtime --features disk_graph --test disk_graph_basic loads_tiny_turtle 2>&1 | tail -5`
Expected: passes.

- [ ] **Step 4: Run all disk_graph_basic tests**

Run: `cargo test -p linkml_runtime --features disk_graph --test disk_graph_basic 2>&1 | tail -15`
Expected: all 9 tests pass.

- [ ] **Step 5: Commit**

```bash
git add src/runtime/src/rdf_import_store_disk.rs src/runtime/tests/disk_graph_basic.rs
git commit -m "$(cat <<'EOF'
feat(runtime): Turtle + format-dispatch constructors for DiskRdfImportStore

Mirrors RdfImportStore's from_turtle / from_rdf surface so callers can
switch backends without changing format handling.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 6: Import convenience and `TrackingDiskRdfImportStore`

**Goal:** Add an `import(&self, sv, conv, root_classes)` method to `DiskRdfImportStore` (mirrors `RdfImportStore::import`) and a `TrackingDiskRdfImportStore` variant that records consumed subjects for diagnostics. End-to-end parity test: same tiny TTL run through both in-memory and disk backends produces the same instance set.

**Files:**
- Modify: `src/runtime/src/rdf_import_store_disk.rs`
- Create: `src/runtime/tests/disk_graph_parity.rs`

- [ ] **Step 1: Add the `import` convenience method**

Append to `src/runtime/src/rdf_import_store_disk.rs`:

```rust
// ── Import convenience ──────────────────────────────────────────────────────

use linkml_schemaview::schemaview::SchemaView;
use linkml_schemaview::Converter;

use crate::turtle_import::{import_from_store, ImportResult};

impl DiskRdfImportStore {
    /// Run a full LinkML import against this store.
    pub fn import(
        &self,
        sv: &SchemaView,
        conv: &Converter,
        root_classes: &[&str],
    ) -> Result<ImportResult, ImportError> {
        import_from_store(self, sv, conv, root_classes)
    }
}
```

- [ ] **Step 2: Add `TrackingDiskRdfImportStore`**

Append to `src/runtime/src/rdf_import_store_disk.rs`:

```rust
// ── Tracking variant ────────────────────────────────────────────────────────

/// Disk-backed store that additionally records which subjects were touched
/// by the harvest. Useful for diagnostics ("X unconsumed triples").
pub struct TrackingDiskRdfImportStore {
    inner: DiskRdfImportStore,
    consumed: RefCell<HashSet<String>>,
}

impl TrackingDiskRdfImportStore {
    pub fn from_turtle(reader: impl Read, path: &Path) -> Result<Self, DiskStoreError> {
        Ok(Self {
            inner: DiskRdfImportStore::from_turtle(reader, path)?,
            consumed: RefCell::new(HashSet::new()),
        })
    }

    pub fn from_ntriples(reader: impl Read, path: &Path) -> Result<Self, DiskStoreError> {
        Ok(Self {
            inner: DiskRdfImportStore::from_ntriples(reader, path)?,
            consumed: RefCell::new(HashSet::new()),
        })
    }

    pub fn from_rdf(
        reader: impl Read,
        format: RdfFormat,
        path: &Path,
    ) -> Result<Self, DiskStoreError> {
        Ok(Self {
            inner: DiskRdfImportStore::from_rdf(reader, format, path)?,
            consumed: RefCell::new(HashSet::new()),
        })
    }

    pub fn consumed_subjects(&self) -> HashSet<String> {
        self.consumed.borrow().clone()
    }

    /// Subjects in the store that were NOT consumed, paired with their
    /// triple counts. Full scan; only call after a complete import.
    pub fn unconsumed_subjects(&self) -> Vec<(String, usize)> {
        let consumed = self.consumed.borrow();
        let mut seen_sids: HashSet<u64> = HashSet::new();
        let mut result: Vec<(String, usize)> = Vec::new();
        for kv in self.inner.spo.iter().flatten() {
            let key = kv.0;
            if key.len() != 24 {
                continue;
            }
            let sid = unpack_u64(&key, 0);
            if !seen_sids.insert(sid) {
                continue;
            }
            let term = match self.inner.term_by_id.get(sid as usize) {
                Some(t) => t,
                None => continue,
            };
            let subj_str = term.to_string();
            if consumed.contains(&subj_str) {
                continue;
            }
            let count = self
                .inner
                .spo
                .prefix(sid.to_be_bytes())
                .filter_map(|r| r.ok())
                .count();
            result.push((subj_str, count));
        }
        result
    }

    pub fn unconsumed_triple_count(&self) -> usize {
        self.unconsumed_subjects().iter().map(|(_, c)| c).sum()
    }

    pub fn import(
        &self,
        sv: &SchemaView,
        conv: &Converter,
        root_classes: &[&str],
    ) -> Result<ImportResult, ImportError> {
        import_from_store(self, sv, conv, root_classes)
    }
}

impl TripleSource for TrackingDiskRdfImportStore {
    fn subjects_for_predicate_object<'a>(
        &'a self,
        predicate: &NamedNode,
        object: &NamedNode,
    ) -> Box<dyn Iterator<Item = NamedOrBlankNodeRef<'a>> + 'a> {
        self.inner.subjects_for_predicate_object(predicate, object)
    }

    fn objects_for_subject_predicate<'a>(
        &'a self,
        subject: &NamedOrBlankNode,
        predicate: &NamedNode,
    ) -> Box<dyn Iterator<Item = TermRef<'a>> + 'a> {
        self.inner.objects_for_subject_predicate(subject, predicate)
    }

    fn triples_for_subject<'a>(
        &'a self,
        subject: &NamedOrBlankNode,
    ) -> Box<dyn Iterator<Item = TripleRef<'a>> + 'a> {
        self.inner.triples_for_subject(subject)
    }

    fn len(&self) -> Option<usize> {
        self.inner.len()
    }

    fn on_consumed(&self, subject: &str, _predicate: &str, _object: &str) {
        self.consumed.borrow_mut().insert(subject.to_string());
    }
}
```

- [ ] **Step 3: Add a parity test against the existing in-memory tests**

Find an existing small-Turtle test file in the runtime tests. Run:

```
ls src/runtime/tests/
```

Use the simplest existing schema-based test to construct a parity check. If `src/runtime/tests/rdf_streaming_test.rs` or similar exists, mirror its setup. Create `src/runtime/tests/disk_graph_parity.rs`:

```rust
#![cfg(feature = "disk_graph")]

use linkml_runtime::rdf_import_store::RdfImportStore;
use linkml_runtime::rdf_import_store_disk::DiskRdfImportStore;
use linkml_runtime::rdf_streaming::import_owned_store_streaming;
use linkml_schemaview::io::from_yaml;
use linkml_schemaview::schemaview::SchemaView;
use std::collections::BTreeMap;
use std::path::PathBuf;
use tempfile::tempdir;

fn fixture_schema() -> SchemaView {
    // Use the same schema fixture the existing streaming tests use. If you
    // can't find it, the simplest viable alternative is to embed an inline
    // YAML string. Try both locations and pick whichever exists.
    let candidates = [
        "src/runtime/tests/fixtures/streaming_schema.yaml",
        "src/runtime/tests/fixtures/simple.yaml",
    ];
    let path = candidates
        .iter()
        .map(PathBuf::from)
        .find(|p| p.exists())
        .expect("locate a small fixture schema; adjust the candidates list if neither exists");
    let schema = from_yaml(&path).unwrap();
    let mut sv = SchemaView::new();
    sv.add_schema_with_import_ref(schema, Some(("".to_owned(), path.display().to_string())))
        .unwrap();
    sv
}

const TINY_NT: &str = "\
<http://example.org/a> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://example.org/T> .
<http://example.org/a> <http://example.org/name> \"alpha\" .
<http://example.org/b> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://example.org/T> .
<http://example.org/b> <http://example.org/name> \"beta\" .
";

fn collect_instance_keys<S: linkml_runtime::triple_source::TripleSource + 'static>(
    store: S,
    sv: SchemaView,
) -> BTreeMap<String, usize> {
    let conv = sv.converter();
    let stream =
        import_owned_store_streaming(store, sv, conv, &["T"]).expect("import_owned_store_streaming");
    let mut out: BTreeMap<String, usize> = BTreeMap::new();
    for item in stream {
        let (class, _inst) = item.expect("import yields ok");
        *out.entry(class).or_insert(0) += 1;
    }
    out
}

#[test]
fn parity_in_memory_vs_disk() {
    // If the fixture-locating helper above fails for your tree, skip this
    // test by replacing `fixture_schema()` with an inline-YAML construction.
    let sv = fixture_schema();
    let in_mem_store =
        RdfImportStore::from_ntriples(std::io::Cursor::new(TINY_NT)).expect("in-mem load");
    let in_mem_counts = collect_instance_keys(in_mem_store, sv.clone());

    let dir = tempdir().unwrap();
    let disk_store =
        DiskRdfImportStore::from_ntriples(std::io::Cursor::new(TINY_NT), dir.path())
            .expect("disk load");
    let disk_counts = collect_instance_keys(disk_store, sv);

    assert_eq!(in_mem_counts, disk_counts, "instance counts must match");
}
```

If the fixture-schema lookup fails, the test will report which paths it tried — adjust the `candidates` list to whichever small schema fixture exists in `src/runtime/tests/fixtures/`. If no fixture is appropriate, fall back to constructing the schema from an inline YAML string in the test body (use `linkml_schemaview::io::from_yaml_str` if present; otherwise write the YAML to a NamedTempFile and pass its path).

- [ ] **Step 4: Discover the right fixture path**

Run: `ls src/runtime/tests/fixtures/ 2>/dev/null || find src/runtime/tests -name '*.yaml' | head -10`

If a fixture matches what the test class `T` expects (a class named "T" with a `name` slot), use its path. Otherwise, replace the body of `fixture_schema()` with an inline YAML string using `tempfile::NamedTempFile` to write a minimal schema:

```yaml
id: https://example.org/
name: test
default_prefix: ex
prefixes:
  ex: http://example.org/
default_range: string
classes:
  T:
    class_uri: ex:T
    attributes:
      name:
        slot_uri: ex:name
        range: string
```

- [ ] **Step 5: Run the parity test**

Run: `cargo test -p linkml_runtime --features disk_graph --test disk_graph_parity 2>&1 | tail -15`
Expected: passes.

- [ ] **Step 6: Run the full disk-graph test suite to verify everything**

Run: `cargo test -p linkml_runtime --features disk_graph 2>&1 | tail -20`
Expected: all tests (basic + parity) pass.

- [ ] **Step 7: Commit**

```bash
git add src/runtime/src/rdf_import_store_disk.rs src/runtime/tests/disk_graph_parity.rs
git commit -m "$(cat <<'EOF'
feat(runtime): TrackingDiskRdfImportStore + import() convenience + parity test

Mirrors the in-memory tracking variant. End-to-end parity test confirms the
streaming harvest produces the same instance set against the disk backend.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 7: Wire `--disk-graph <PATH>` into `linkml-convert`

**Goal:** Add the CLI flag, feature-gated. With the flag, `linkml-convert` builds a `DiskRdfImportStore` at the supplied path instead of `RdfImportStore`. Without the flag, behavior is byte-for-byte unchanged.

**Files:**
- Modify: `src/tools/Cargo.toml`
- Modify: `src/tools/src/bin/linkml_convert.rs`

- [ ] **Step 1: Add a forwarded feature on linkml_tools**

Check `src/tools/Cargo.toml`:

```bash
cat src/tools/Cargo.toml
```

Add a forwarded `disk_graph` feature. In the `[features]` table, add:

```toml
disk_graph = ["linkml_runtime/disk_graph"]
```

If `linkml_runtime` is declared with `features = [...]`, leave its base feature list untouched — the forwarded feature only enables `disk_graph` when the tools crate is built with `--features disk_graph`.

- [ ] **Step 2: Add the CLI flag**

In `src/tools/src/bin/linkml_convert.rs`, find the `Args` struct (lines 32-55). Just before the closing `}` of the struct, add:

```rust
    /// Path to a disk-backed RDF graph store (fjall). Only used for RDF
    /// inputs; without this flag, the in-memory store is used. Requires
    /// the `disk_graph` build feature.
    #[cfg(feature = "disk_graph")]
    #[arg(long = "disk-graph")]
    disk_graph: Option<PathBuf>,
```

- [ ] **Step 3: Branch on the flag for RDF inputs**

In `src/tools/src/bin/linkml_convert.rs`, find the `Format::Turtle | Format::Ntriples =>` arm (around line 164). Replace the whole arm with:

```rust
        Format::Turtle | Format::Ntriples => {
            use linkml_runtime::rdf_import_store::RdfImportStore;
            use linkml_runtime::rdf_streaming::import_owned_store_streaming;

            let class_refs: Vec<String> = args.class.clone();
            let class_refs_borrow: Vec<&str> = class_refs.iter().map(|s| s.as_str()).collect();

            #[cfg(feature = "disk_graph")]
            let owned_stream_iter: Box<dyn Iterator<Item = InstanceItem>> = if let Some(disk_path) =
                args.disk_graph.as_ref()
            {
                use linkml_runtime::rdf_import_store_disk::DiskRdfImportStore;
                let file = File::open(&args.data)?;
                let reader = BufReader::new(file);
                let store = match in_fmt {
                    Format::Ntriples => DiskRdfImportStore::from_ntriples(reader, disk_path)
                        .map_err(|e| e.to_string())?,
                    _ => DiskRdfImportStore::from_turtle(reader, disk_path)
                        .map_err(|e| e.to_string())?,
                };
                let owned_stream =
                    import_owned_store_streaming(store, sv.clone(), conv.clone(), &class_refs_borrow)
                        .map_err(|e| e.to_string())?;
                Box::new(owned_stream.map(|res| {
                    res.map(|(_class, inst)| inst)
                        .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)
                }))
            } else {
                let file = File::open(&args.data)?;
                let reader = BufReader::new(file);
                let store = match in_fmt {
                    Format::Ntriples => {
                        RdfImportStore::from_ntriples(reader).map_err(|e| e.to_string())?
                    }
                    _ => RdfImportStore::from_turtle(reader).map_err(|e| e.to_string())?,
                };
                let owned_stream =
                    import_owned_store_streaming(store, sv.clone(), conv.clone(), &class_refs_borrow)
                        .map_err(|e| e.to_string())?;
                Box::new(owned_stream.map(|res| {
                    res.map(|(_class, inst)| inst)
                        .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)
                }))
            };

            #[cfg(not(feature = "disk_graph"))]
            let owned_stream_iter: Box<dyn Iterator<Item = InstanceItem>> = {
                let file = File::open(&args.data)?;
                let reader = BufReader::new(file);
                let store = match in_fmt {
                    Format::Ntriples => {
                        RdfImportStore::from_ntriples(reader).map_err(|e| e.to_string())?
                    }
                    _ => RdfImportStore::from_turtle(reader).map_err(|e| e.to_string())?,
                };
                let owned_stream =
                    import_owned_store_streaming(store, sv.clone(), conv.clone(), &class_refs_borrow)
                        .map_err(|e| e.to_string())?;
                Box::new(owned_stream.map(|res| {
                    res.map(|(_class, inst)| inst)
                        .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)
                }))
            };

            owned_stream_iter
        }
```

- [ ] **Step 4: Build with the feature on and off**

Run: `cargo build -p linkml_tools --release && cargo build -p linkml_tools --release --features disk_graph 2>&1 | tail -10`
Expected: both builds succeed cleanly.

- [ ] **Step 5: Smoke-test the CLI end-to-end**

Create a tiny test fixture and run `linkml-convert` against it with both backends. Run:

```bash
cat > /tmp/disk_graph_smoke.ttl <<'EOF'
@prefix : <http://example.org/> .
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
:a a :T ; :name "alpha" .
:b a :T ; :name "beta" .
EOF

cat > /tmp/disk_graph_smoke_schema.yaml <<'EOF'
id: https://example.org/
name: smoke
default_prefix: ex
prefixes:
  ex: http://example.org/
default_range: string
classes:
  T:
    class_uri: ex:T
    attributes:
      name:
        slot_uri: ex:name
        range: string
EOF

# In-memory run
./target/release/linkml-convert /tmp/disk_graph_smoke_schema.yaml /tmp/disk_graph_smoke.ttl \
    --from turtle --to json --output /tmp/smoke_inmem.json --class T

# Disk run
rm -rf /tmp/smoke_disk_store
./target/release/linkml-convert /tmp/disk_graph_smoke_schema.yaml /tmp/disk_graph_smoke.ttl \
    --from turtle --to json --output /tmp/smoke_disk.json --class T \
    --disk-graph /tmp/smoke_disk_store

diff <(python3 -c "import json,sys; print(json.dumps(sorted(json.load(open('/tmp/smoke_inmem.json')), key=lambda x: json.dumps(x,sort_keys=True)), sort_keys=True, indent=2))") \
     <(python3 -c "import json,sys; print(json.dumps(sorted(json.load(open('/tmp/smoke_disk.json')), key=lambda x: json.dumps(x,sort_keys=True)), sort_keys=True, indent=2))")
```

The binary built without the feature is at `target/release/linkml-convert` from Step 4's first build. For the disk run you need the version built with `--features disk_graph`. Adjust the commands so the disk run uses the feature-built binary; the simplest is to run the disk-build last so it's at the same path. The expected outcome: the `diff` at the end produces no output (canonical JSON identical).

- [ ] **Step 6: Commit**

```bash
git add src/tools/Cargo.toml src/tools/src/bin/linkml_convert.rs
git commit -m "$(cat <<'EOF'
feat(linkml-convert): --disk-graph <PATH> flag (opt-in disk-backed RDF graph)

Feature-gated. Without the flag, behavior is byte-for-byte unchanged. With
it, source RDF is loaded into a fjall-backed DiskRdfImportStore at the
supplied path before streaming harvest.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 8: Python entry point for the disk backend

**Goal:** Expose disk-graph imports to Python via `from_turtle_streaming_disk(turtle_str_or_path, schema_view, root_classes, disk_path)` and a parallel `from_ntriples_streaming_disk`. New `PyDiskTurtleStream` wrapper. All gated behind `disk_graph` feature on the python crate.

**Files:**
- Modify: `src/python/Cargo.toml`
- Modify: `src/python/src/lib.rs`

- [ ] **Step 1: Add the forwarded feature**

In `src/python/Cargo.toml`, add to `[features]`:

```toml
disk_graph = ["linkml_runtime/disk_graph"]
```

The `linkml_runtime` declaration already has `features = ["ttl", "curies", "python"]`. Leave that line alone — the forwarded feature only adds `disk_graph` to runtime's feature set when the python crate is built with `--features disk_graph`.

- [ ] **Step 2: Add the Python wrapper and entry points**

Locate the `PyTurtleStream` definition in `src/python/src/lib.rs` (around line 1718). Immediately after the closing `}` of its `impl PyTurtleStream` block (which ends around line 1753), append:

```rust
#[cfg(feature = "disk_graph")]
#[cfg_attr(feature = "stubgen", gen_stub_pyfunction)]
#[pyfunction(name = "from_turtle_streaming_disk", signature = (turtle_str, schema_view, root_classes, disk_path))]
fn py_from_turtle_streaming_disk(
    py: Python<'_>,
    turtle_str: &str,
    schema_view: &PySchemaView,
    root_classes: Vec<String>,
    disk_path: &str,
) -> PyResult<Py<PyDiskTurtleStream>> {
    use linkml_runtime::rdf_import_store_disk::DiskRdfImportStore;
    use linkml_runtime::rdf_streaming::import_owned_store_streaming;

    let sv: linkml_schemaview::schemaview::SchemaView = (*schema_view.inner).clone();
    let conv = sv.converter();
    let class_refs: Vec<&str> = root_classes.iter().map(|s| s.as_str()).collect();

    let store = DiskRdfImportStore::from_turtle(
        std::io::Cursor::new(turtle_str.as_bytes()),
        std::path::Path::new(disk_path),
    )
    .map_err(|e| PyException::new_err(e.to_string()))?;

    let iter = import_owned_store_streaming(store, sv, conv, &class_refs)
        .map_err(|e| PyException::new_err(e.to_string()))?;

    let sv_py: Py<PySchemaView> = Py::new(
        py,
        PySchemaView {
            inner: schema_view.inner.clone(),
        },
    )?;

    Py::new(
        py,
        PyDiskTurtleStream {
            iter: Some(iter),
            sv_py,
        },
    )
}

#[cfg(feature = "disk_graph")]
#[cfg_attr(feature = "stubgen", gen_stub_pyfunction)]
#[pyfunction(name = "from_ntriples_streaming_disk", signature = (ntriples_path, schema_view, root_classes, disk_path))]
fn py_from_ntriples_streaming_disk(
    py: Python<'_>,
    ntriples_path: &str,
    schema_view: &PySchemaView,
    root_classes: Vec<String>,
    disk_path: &str,
) -> PyResult<Py<PyDiskTurtleStream>> {
    use linkml_runtime::rdf_import_store_disk::DiskRdfImportStore;
    use linkml_runtime::rdf_streaming::import_owned_store_streaming;

    let sv: linkml_schemaview::schemaview::SchemaView = (*schema_view.inner).clone();
    let conv = sv.converter();
    let class_refs: Vec<&str> = root_classes.iter().map(|s| s.as_str()).collect();

    let file = std::fs::File::open(ntriples_path).map_err(|e| PyException::new_err(e.to_string()))?;
    let reader = std::io::BufReader::new(file);
    let store =
        DiskRdfImportStore::from_ntriples(reader, std::path::Path::new(disk_path))
            .map_err(|e| PyException::new_err(e.to_string()))?;

    let iter = import_owned_store_streaming(store, sv, conv, &class_refs)
        .map_err(|e| PyException::new_err(e.to_string()))?;

    let sv_py: Py<PySchemaView> = Py::new(
        py,
        PySchemaView {
            inner: schema_view.inner.clone(),
        },
    )?;

    Py::new(
        py,
        PyDiskTurtleStream {
            iter: Some(iter),
            sv_py,
        },
    )
}

#[cfg(feature = "disk_graph")]
#[cfg_attr(feature = "stubgen", gen_stub_pyclass)]
#[pyclass(name = "DiskTurtleStream", unsendable)]
pub struct PyDiskTurtleStream {
    iter: Option<
        linkml_runtime::rdf_streaming::OwnedImportStream<
            linkml_runtime::rdf_import_store_disk::DiskRdfImportStore,
        >,
    >,
    sv_py: Py<PySchemaView>,
}

#[cfg(feature = "disk_graph")]
#[pymethods]
impl PyDiskTurtleStream {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    #[allow(clippy::type_complexity)]
    fn __next__(
        mut slf: PyRefMut<'_, Self>,
        py: Python<'_>,
    ) -> PyResult<Option<(String, Py<PyLinkMLInstance>)>> {
        let iter = match slf.iter.as_mut() {
            Some(i) => i,
            None => return Ok(None),
        };
        match iter.next() {
            None => {
                slf.iter = None;
                Ok(None)
            }
            Some(Err(e)) => Err(PyException::new_err(e.to_string())),
            Some(Ok((class_name, inst))) => {
                let sv_clone = slf.sv_py.clone_ref(py);
                let py_inst = Py::new(py, PyLinkMLInstance::new(inst, sv_clone))?;
                Ok(Some((class_name, py_inst)))
            }
        }
    }
}
```

- [ ] **Step 3: Register the new functions with the module**

In `src/python/src/lib.rs`, find the module init block where `py_from_turtle_streaming` is registered (around line 723: `m.add_function(wrap_pyfunction!(py_from_turtle_streaming, m)?)?;`). Immediately after that line, add:

```rust
#[cfg(feature = "disk_graph")]
m.add_function(wrap_pyfunction!(py_from_turtle_streaming_disk, m)?)?;
#[cfg(feature = "disk_graph")]
m.add_function(wrap_pyfunction!(py_from_ntriples_streaming_disk, m)?)?;
```

- [ ] **Step 4: Build with the feature on and off**

Run: `cargo build -p linkml_runtime_python && cargo build -p linkml_runtime_python --features disk_graph 2>&1 | tail -10`
Expected: both builds succeed cleanly.

- [ ] **Step 5: Commit**

```bash
git add src/python/Cargo.toml src/python/src/lib.rs
git commit -m "$(cat <<'EOF'
feat(python): from_turtle_streaming_disk / from_ntriples_streaming_disk

Adds Python entry points for the disk-backed RDF graph, behind a forwarded
disk_graph feature. New DiskTurtleStream Python class mirrors TurtleStream
for the disk variant.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 9: Extend the measurement script to capture the `streaming-disk` row

**Goal:** `scripts/measure_rinf_de.sh` learns a new mode that runs `linkml-convert` with `--features disk_graph` and `--disk-graph <PATH>` so we can capture the fourth row of the comparison table.

**Files:**
- Modify: `scripts/measure_rinf_de.sh`

- [ ] **Step 1: Add a `DISK_GRAPH` opt-in to the script**

Edit `scripts/measure_rinf_de.sh`. Add this near the top (after the `OUT_DIR=` line and before `mkdir -p`):

```bash
DISK_GRAPH="${DISK_GRAPH:-}"     # non-empty path → use disk-backed store at this path
```

Then update the `cargo build` line to enable the feature when `DISK_GRAPH` is set. Replace:

```bash
cargo build -p linkml_tools --release 2>&1 | tail -3
```

with:

```bash
if [ -n "$DISK_GRAPH" ]; then
    cargo build -p linkml_tools --release --features disk_graph 2>&1 | tail -3
else
    cargo build -p linkml_tools --release 2>&1 | tail -3
fi
```

Then update the `CMD=(...)` line to append `--disk-graph` when needed. Replace:

```bash
CMD=("$BIN" "$SCHEMA" "$NT_FILE" --from ntriples --to json --output "$OUT_DIR/output.json" "${CLASS_ARGS[@]}")
```

with:

```bash
CMD=("$BIN" "$SCHEMA" "$NT_FILE" --from ntriples --to json --output "$OUT_DIR/output.json" "${CLASS_ARGS[@]}")
if [ -n "$DISK_GRAPH" ]; then
    rm -rf "$DISK_GRAPH"
    mkdir -p "$DISK_GRAPH"
    CMD+=(--disk-graph "$DISK_GRAPH")
    echo "disk-graph: $DISK_GRAPH"
fi
```

- [ ] **Step 2: Verify the script still parses**

Run: `bash -n scripts/measure_rinf_de.sh`
Expected: no syntax errors (empty output).

- [ ] **Step 3: Commit**

```bash
git add scripts/measure_rinf_de.sh
git commit -m "$(cat <<'EOF'
chore(measure): DISK_GRAPH opt-in to capture the streaming-disk row

Setting DISK_GRAPH=<path> rebuilds linkml-convert with --features disk_graph
and adds --disk-graph <path> to the invocation. Lets us extend the existing
comparison table with the disk-backed row without forking the script.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 10: Run the measurement and update the spec with results

**Goal:** Capture the actual streaming-disk numbers on the RINF dataset, append them to the spec's deferred "Results" placeholder, and verify the success criteria.

**Files:**
- Modify: `docs/superpowers/specs/2026-05-21-rdf-import-less-ram-disk-graph-design.md`

- [ ] **Step 1: Run the measurement**

Run from the worktree root:

```bash
LABEL=streaming-disk DISK_GRAPH=target/rinf-measure/streaming-disk/store bash scripts/measure_rinf_de.sh
```

Wait for the script to complete. It prints the procmon path and writes:
- `target/rinf-measure/streaming-disk/stdout.log`
- `target/rinf-measure/streaming-disk/procmon.log`
- `target/rinf-measure/streaming-disk/output.json`
- `target/rinf-measure/streaming-disk/output.canonical.json`

If the run OOMs or exceeds your patience (>10 minutes), STOP and surface the issue — don't keep retrying. The likely diagnoses are documented in the spec's "Expected numbers" section.

- [ ] **Step 2: Extract peak RSS and wall-clock**

Peak RSS:
```bash
awk -F',' 'NR>1 {if ($2+0 > max) max=$2+0} END {printf "peak RSS: %.2f GB\n", max/1024/1024}' \
    target/rinf-measure/streaming-disk/procmon.log
```

Wall-clock: pull the line that announces total elapsed from stdout.log, or compute from procmon's first/last timestamps:
```bash
awk -F',' 'NR==2 {first=$1} END {print "elapsed: " $1 - first " s"}' \
    target/rinf-measure/streaming-disk/procmon.log
```

(The exact column indices depend on procmon's output format — adjust as needed by inspecting the first few lines of `procmon.log`.)

- [ ] **Step 3: Diff against row 3 to confirm output equivalence**

Run:

```bash
diff target/rinf-measure/streaming-postrefactor/output.canonical.json \
     target/rinf-measure/streaming-disk/output.canonical.json | head -50
```

Expected: differences only at the magnitude documented in Part 1 (HashMap iteration noise). If the instance counts per class differ, that's a real regression — STOP and investigate.

Cross-check instance counts:

```bash
python3 -c "
import json
for label in ['streaming-postrefactor', 'streaming-disk']:
    data = json.load(open(f'target/rinf-measure/{label}/output.canonical.json'))
    from collections import Counter
    c = Counter()
    for inst in data:
        c[inst.get('@type', '?')] += 1
    print(label, dict(sorted(c.items())))
"
```

The two `Counter` outputs must match exactly.

- [ ] **Step 4: Update the spec's Results section**

In `docs/superpowers/specs/2026-05-21-rdf-import-less-ram-disk-graph-design.md`, find the "Expected numbers" subsection at the end of the Measurement plan section. After that subsection, add a new subsection titled "Measured results" populated with the actual numbers. Use the existing Part 1 comparison table format. Example template (fill in the `???` with measured values):

```markdown
### Measured results (2026-05-21)

| Row | Wall-clock | Peak RSS |
|---|---|---|
| baseline (in-memory, pre-streaming) | 82 s | 6.52 GB |
| streaming-prerefactor | 77 s | 6.51 GB |
| streaming-postrefactor (in-memory, streaming output) | 74 s | 5.00 GB |
| **streaming-disk (fjall, dictionary in RAM)** | **??? s** | **??? GB** |

**Output equivalence:** instance counts per class match streaming-postrefactor exactly (verified). Canonical JSON diff size: within the HashMap-iteration-noise band documented in Part 1.

**Success criteria check:**
- Peak RSS < 2 GB: ??? (PASS / FAIL)
- Wall-clock < 5× row 3: ??? (PASS / FAIL)
- Clean build with feature ON: PASS
- Clean build with feature OFF: PASS
```

- [ ] **Step 5: Commit the results**

```bash
git add docs/superpowers/specs/2026-05-21-rdf-import-less-ram-disk-graph-design.md
git commit -m "$(cat <<'EOF'
docs: measured streaming-disk row for the disk-graph design spec

Captures wall-clock and peak RSS for the new fjall-backed backend on the
RINF Germany dataset. Confirms the design's success criteria.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## After all tasks

Run the final equivalence check across the full test surface to make sure nothing on the in-memory path regressed:

```bash
cargo test -p linkml_runtime 2>&1 | tail -10
cargo test -p linkml_runtime --features disk_graph 2>&1 | tail -10
cargo build -p linkml_runtime --target wasm32-unknown-unknown 2>&1 | tail -5 || echo "(wasm target not installed; verify manually if needed)"
```

If anything goes red, address it before opening the MR.

---

## Self-review (against the spec)

- [x] Feature flag opt-in at compile time: Task 0 + verified in Tasks 0 / 1 / 2 / 5 / 7 / 8 by building with feature off.
- [x] Runtime opt-in via caller path: Task 2 (`open_empty(path)`); CLI flag in Task 7; Python args in Task 8.
- [x] Trait-compatible: Task 4 implements `TripleSource` with no signature changes.
- [x] Two partitions (SPO + POS): Tasks 2 + 3.
- [x] Term dictionary in RAM, both directions: Tasks 2 + 3.
- [x] Generic `OwnedImportStream` over `S: TripleSource + 'static`: Task 1.
- [x] `DiskRdfImportStore` + `TrackingDiskRdfImportStore`: Tasks 2 / 3 / 4 / 5 / 6.
- [x] linkml-convert CLI integration: Task 7.
- [x] Python integration: Task 8.
- [x] Measurement script extension: Task 9.
- [x] Measurement + spec results: Task 10.

No spec requirement is uncovered.
