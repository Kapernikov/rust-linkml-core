# Python API Redesign — Implementation Plan (Phase 3)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the entire RDF-related Python API surface with a streaming-friendly, diagnostic-rich design. File-like I/O on both directions; `pop_warnings()` API; `disk_path` kwarg replacing `_disk` suffix variants; full removal of the old API.

**Architecture:** Rust core takes `impl Read` / `impl Write` for native speed; PyO3 layer wraps Python `IO[bytes]` in a chunked-read bridge to amortize GIL cost. One public `RdfStream` type (enum-backed over the two backends) replaces `OwnedImportStream<S>` in the public surface. Warning sink is `Rc<RefCell<Vec<ValidationResult>>>` shared between `RdfStream` (drain side) and `HarvestContext` (push side).

**Tech Stack:** Rust, PyO3 0.25, oxrdf/oxttl, fjall (behind feature gate).

**Spec:** [docs/superpowers/specs/2026-05-21-python-api-streaming-and-diagnostics-design.md](../specs/2026-05-21-python-api-streaming-and-diagnostics-design.md)

---

## File Structure

**Created:**
- `src/runtime/src/rdf_import.rs` — new top-level public API. `import_turtle`, `import_ntriples`, `RdfStream`, `ImportOptions`.
- `src/runtime/src/rdf_export.rs` — new export API. `export_turtle`, `export_ntriples`, `_many` variants, `ExportOptions`.
- `src/python/src/io_bridge.rs` — PyO3 reader wrapping a Python file-like into Rust `Read`. Chunked.
- `src/runtime/tests/warning_emission.rs` — unit tests for the three warning categories + strict mode.
- `python/linkml_runtime/__init__.py` (or wherever the Python-side package lives) — convenience helpers.

**Modified (substantial):**
- `src/runtime/src/turtle_import.rs` — add warning sink to `HarvestContext`, instrument three warning points, remove `ImportResult` and `import_from_store`.
- `src/runtime/src/rdf_streaming.rs` — `OwnedImportStream<S>` becomes `pub(crate)`. `import_owned_store_streaming` becomes `pub(crate)`.
- `src/runtime/src/rdf_import_store.rs` — merge `TrackingRdfImportStore` into `RdfImportStore` (always tracks consumed subjects).
- `src/runtime/src/rdf_import_store_disk.rs` — drop `TrackingDiskRdfImportStore` and `unconsumed_subjects` machinery.
- `src/runtime/src/lib.rs` — wire new modules, drop old exports.
- `src/python/src/lib.rs` — replace `py_from_turtle*` / `py_to_turtle` with `py_import_turtle*` / `py_export_turtle*` and `PyRdfStream`.
- `src/tools/src/bin/linkml_convert.rs` — migrate to new API; add `--strict`.
- `scripts/measure_rinf_de.sh` — no changes expected; the `DISK_GRAPH` opt-in still works.

**Removed:**
- `ImportResult` struct.
- `import_from_store` function.
- `RdfImportStore::import` / `DiskRdfImportStore::import` convenience methods.
- `TrackingRdfImportStore` (merged into `RdfImportStore`).
- `TrackingDiskRdfImportStore` type.
- `OwnedImportStream<S>` from public surface (demoted to `pub(crate)`).
- Python: `py_from_turtle`, `py_from_turtle_tracked`, `py_from_turtle_streaming`, `py_from_turtle_streaming_disk`, `py_from_ntriples_streaming_disk`, `py_to_turtle`, `PyTurtleStream`, `PyDiskTurtleStream`.

---

## Task 0: Add warning sink to HarvestContext and instrument the three categories

**Why first:** every later step assumes the warning sink exists. Standalone tests verify each category emits exactly once on engineered inputs.

**Files:**
- Modify: `src/runtime/src/turtle_import.rs:272` (`HarvestContext` struct) + the three emission sites identified in the spec.
- Modify: `src/runtime/src/rdf_streaming.rs` (`Materializer` and `ImportStream` thread the sink through).

**Key signature change:**

```rust
pub struct HarvestContext<'a, T: TripleSource> {
    /* existing fields */
    pub warnings: std::rc::Rc<std::cell::RefCell<Vec<crate::ValidationResult>>>,
    pub strict: bool,   // if true, emit() returns Err instead of pushing
}

impl<'a, T: TripleSource> HarvestContext<'a, T> {
    pub fn emit_warning(&self, vr: crate::ValidationResult) -> Result<(), ImportError>;
    pub fn new(/* existing args */) -> Self;  // default warnings = empty, strict = false
    pub fn with_warnings_and_strict(/* args */, warnings: Rc<...>, strict: bool) -> Self;
}
```

**Steps:**

- [ ] Add `warnings` and `strict` fields to `HarvestContext`. Default constructor leaves `warnings` empty and `strict` false.
- [ ] Add `emit_warning` helper: in strict mode returns `Err(ImportError::ValidationFailure(vr))`; otherwise pushes to the buffer.
- [ ] Add `ImportError::ValidationFailure(ValidationResult)` variant.
- [ ] Instrument site 1: `turtle_import.rs:547` `SingleValue` arm — when `items.len() > 1`, build a `ValidationResult::warning(MaxCountViolation, path, format!("slot {} declared single-value but RDF has {} objects; first is kept, rest dropped", slot.name, items.len()))` and call `emit_warning`.
- [ ] Instrument site 2: per-triple loop in `harvest_subject` — when a predicate doesn't match any slot of the subject's class, emit `UndeclaredSlot` warning.
- [ ] Instrument site 3: `literal_to_json` — when conversion to declared range fails, emit `SlotRangeViolation`. Currently `literal_to_json` doesn't know about the slot; needs a slot param or a wrapper.
- [ ] Make `ImportStream` and `Materializer` carry references to the same `Rc<RefCell<...>>` (already shared through `HarvestContext`).
- [ ] **Build:** `cargo build -p linkml_runtime`. Expected: clean.
- [ ] **Commit:** `feat(runtime): warning sink in HarvestContext + 3 emission sites`

---

## Task 1: Merge TrackingRdfImportStore into RdfImportStore; drop TrackingDiskRdfImportStore

**Files:**
- Modify: `src/runtime/src/rdf_import_store.rs` — `RdfImportStore` always tracks consumed subjects.
- Modify: `src/runtime/src/rdf_import_store_disk.rs` — remove `TrackingDiskRdfImportStore` entirely.
- Modify any callers/tests referring to the old tracking type names.

**Key shape:**

```rust
pub struct RdfImportStore {
    graph: Graph,
    consumed: RefCell<HashSet<String>>,   // always-on now
}

impl RdfImportStore {
    pub fn consumed_subjects(&self) -> HashSet<String>;
    pub fn unconsumed_subjects(&self) -> Vec<(String, usize)>;
}
```

`TripleSource::on_consumed` for `RdfImportStore` inserts into `consumed`. For `DiskRdfImportStore` stays no-op.

**Steps:**

- [ ] Merge `TrackingRdfImportStore` into `RdfImportStore` (move methods, drop the separate type).
- [ ] Update all callers/tests in `src/runtime/tests/` (`streaming_import.rs`, `inline_structure.rs`, `rinf_herefoss.rs`, etc.) to use the unified type.
- [ ] Remove `TrackingDiskRdfImportStore` from `rdf_import_store_disk.rs` and the imports section.
- [ ] **Build + tests:** `cargo build -p linkml_runtime && cargo build -p linkml_runtime --features disk_graph && cargo test -p linkml_runtime --features disk_graph`. Expected: clean (existing tests may need touchups for renamed methods).
- [ ] **Commit:** `refactor(runtime): merge TrackingRdfImportStore into RdfImportStore; drop tracking on disk`

---

## Task 2: Build the public `RdfStream` type

**Files:**
- Create: `src/runtime/src/rdf_import.rs`.
- Modify: `src/runtime/src/rdf_streaming.rs` — demote `OwnedImportStream` and `import_owned_store_streaming` to `pub(crate)`.
- Modify: `src/runtime/src/lib.rs` — wire the new module.

**Key shape:** (matches the spec's Rust section)

```rust
pub struct RdfStream {
    inner: RdfStreamInner,
    warnings: Rc<RefCell<Vec<ValidationResult>>>,
}

enum RdfStreamInner {
    Memory(OwnedImportStream<RdfImportStore>),
    #[cfg(feature = "disk_graph")]
    Disk(OwnedImportStream<DiskRdfImportStore>),
}

impl RdfStream {
    pub fn pop_warnings(&mut self) -> Vec<ValidationResult>;
    pub fn warning_count(&self) -> usize;
    pub fn unconsumed_subjects(&self) -> Option<Vec<(String, usize)>>;
}

impl Iterator for RdfStream {
    type Item = Result<(String, LinkMLInstance), ImportError>;
    fn next(&mut self) -> Option<Self::Item>;
}
```

`unconsumed_subjects` returns `None` after iteration completes if the backend is disk-graph or iteration hasn't finished yet, otherwise `Some(...)` from `RdfImportStore::unconsumed_subjects()`.

**Steps:**

- [ ] Add `RdfStream`, `RdfStreamInner`, and the public methods in the new module.
- [ ] Demote `OwnedImportStream` and `import_owned_store_streaming` to `pub(crate)`.
- [ ] Add `pub mod rdf_import` to `lib.rs`.
- [ ] **Build:** `cargo build -p linkml_runtime --features disk_graph`. Expected: clean.
- [ ] **Commit:** `feat(runtime): public RdfStream type wraps both backends`

---

## Task 3: Top-level entry points `import_turtle` / `import_ntriples`

**Files:**
- Modify: `src/runtime/src/rdf_import.rs` (add the functions).

**Key signatures:**

```rust
#[derive(Debug, Default, Clone)]
pub struct ImportOptions {
    pub disk_path: Option<PathBuf>,
    pub strict: bool,
}

pub fn import_turtle<R: Read>(
    reader: R,
    sv: SchemaView,
    conv: Converter,
    root_classes: &[&str],
    options: ImportOptions,
) -> Result<RdfStream, ImportError>;

pub fn import_ntriples<R: Read>( /* same */ ) -> Result<RdfStream, ImportError>;
```

Behaviour: if `options.disk_path` is `Some(_)`:
- With `disk_graph` feature: build `DiskRdfImportStore::from_turtle(reader, path)`.
- Without `disk_graph` feature: return `ImportError::Parse("disk_path requires the disk_graph cargo feature")`.

If `None`: build `RdfImportStore::from_turtle(reader)`.

Either way, pass `strict` and a freshly-created `Rc<RefCell<Vec<...>>>` warnings sink through. Wrap the resulting `OwnedImportStream<...>` in the appropriate `RdfStreamInner` variant.

**Steps:**

- [ ] Add `ImportOptions`.
- [ ] Add `import_turtle` and `import_ntriples` dispatching on `disk_path`.
- [ ] Add a small integration test: build an `import_turtle` against a `Cursor<&[u8]>`, iterate, check counts.
- [ ] **Build + test:** `cargo test -p linkml_runtime --features disk_graph rdf_import`. Expected: clean.
- [ ] **Commit:** `feat(runtime): import_turtle / import_ntriples top-level entry points`

---

## Task 4: Export side — `export_turtle*` / `export_ntriples*`

**Files:**
- Create: `src/runtime/src/rdf_export.rs`.
- Modify: `src/runtime/src/lib.rs`.

**Key signatures:**

```rust
#[derive(Debug, Default, Clone)]
pub struct ExportOptions {
    pub skolem: bool,
}

pub fn export_turtle<W: Write>(
    instance: &LinkMLInstance,
    sv: &SchemaView,
    schema: &SchemaDefinition,
    conv: &Converter,
    writer: &mut W,
    options: ExportOptions,
) -> Result<(), ExportError>;

pub fn export_turtle_many<W, I>(
    instances: I,
    sv: &SchemaView,
    schema: &SchemaDefinition,
    conv: &Converter,
    writer: &mut W,
    options: ExportOptions,
) -> Result<(), ExportError>
where
    W: Write,
    I: IntoIterator<Item = LinkMLInstance>;

pub fn export_ntriples<W: Write>( /* same */ );
pub fn export_ntriples_many<W, I>( /* same */ );
```

Internally these wrap the existing `write_turtle` / `write_ntriples` functions in `src/runtime/src/turtle/`. The `_many` variants emit prefix declarations once and instance bodies in a loop.

**Steps:**

- [ ] Create the file with the four functions as thin shims over `write_turtle` / `write_ntriples`.
- [ ] Verify the existing `write_turtle` re-emits prefixes per call (and that's not a regression for the `_many` case). If yes, factor out the prefix-write step for the `_many` loop.
- [ ] Add a round-trip test: import_turtle → export_turtle_many → re-parse, check equivalence.
- [ ] **Build + test.**
- [ ] **Commit:** `feat(runtime): export_turtle / export_ntriples entry points`

---

## Task 5: Remove old public surface

**Files:**
- Modify: `src/runtime/src/turtle_import.rs` — drop `ImportResult`, `import_from_store`.
- Modify: `src/runtime/src/rdf_import_store.rs` — drop `RdfImportStore::import`.
- Modify: `src/runtime/src/rdf_import_store_disk.rs` — drop `DiskRdfImportStore::import`.
- Modify: `src/runtime/src/lib.rs` — drop public re-exports for the above.
- Modify: any tests that called those.

**Steps:**

- [ ] Drop `ImportResult`, `import_from_store`, `import_turtle` (old in `turtle_import.rs`), `import_ntriples` (old), `import_rdf` (old) from `turtle_import.rs`. The new `import_turtle` / `import_ntriples` live in `rdf_import.rs`.
- [ ] Drop the `import()` convenience methods from `RdfImportStore` and `DiskRdfImportStore`.
- [ ] Fix any test failures by porting calls to the new API.
- [ ] **Build:** `cargo build -p linkml_runtime --features disk_graph`. Expected: clean once tests are ported.
- [ ] **Commit:** `refactor(runtime): remove ImportResult, import_from_store, store-level import() methods`

---

## Task 6: PyO3 IO bridge — Python file-like → Rust `Read`

**Files:**
- Create: `src/python/src/io_bridge.rs`.
- Modify: `src/python/src/lib.rs` — declare the module.

**Key shape:**

```rust
pub struct PyReader {
    obj: PyObject,                // the Python file-like
    chunk_size: usize,            // default 256 * 1024
    buf: Vec<u8>,                 // unread bytes from the last .read()
    buf_pos: usize,
}

impl PyReader {
    pub fn new(py: Python<'_>, obj: PyObject) -> Self;
}

impl std::io::Read for PyReader {
    fn read(&mut self, dst: &mut [u8]) -> std::io::Result<usize> {
        // 1. drain self.buf first
        // 2. if dst still wants more, acquire GIL and call .read(chunk_size)
        // 3. copy into dst, save remainder in self.buf
    }
}
```

The `Read` impl is the only thing public. It batches calls to Python's `read()` to amortize the GIL.

**Steps:**

- [ ] Implement `PyReader`. Handle EOF (empty bytes returned from `.read()`).
- [ ] Handle errors: Python exceptions from `.read()` get converted to `std::io::Error::new(ErrorKind::Other, ...)`.
- [ ] Unit test: feed a `io.BytesIO(b"hello world")` through it, verify the bytes come out.
- [ ] **Build:** `cargo build -p linkml_runtime_python`.
- [ ] **Commit:** `feat(python): PyReader bridge for IO[bytes] -> impl Read`

---

## Task 7: New Python entry points + `PyRdfStream`

**Files:**
- Modify: `src/python/src/lib.rs`.

**Key shape:**

```rust
#[pyclass(name = "RdfStream", unsendable)]
pub struct PyRdfStream {
    inner: Option<linkml_runtime::rdf_import::RdfStream>,
    sv_py: Py<PySchemaView>,
}

#[pymethods]
impl PyRdfStream {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self>;
    fn __next__(mut slf: PyRefMut<'_, Self>, py: Python<'_>)
        -> PyResult<Option<(String, Py<PyLinkMLInstance>)>>;
    fn pop_warnings(&mut self) -> Vec<PyValidationResult>;
    fn warning_count(&self) -> usize;
    fn unconsumed_subjects(&self) -> Option<Vec<(String, usize)>>;
}

#[pyfunction(name = "import_turtle", signature = (reader, schema_view, root_classes, *, disk_path=None, strict=false))]
fn py_import_turtle(py: Python<'_>, reader: PyObject, schema_view: &PySchemaView, root_classes: Vec<String>, disk_path: Option<&str>, strict: bool) -> PyResult<Py<PyRdfStream>>;

// parallel: py_import_ntriples, py_export_turtle, py_export_ntriples
```

Remove ALL old `py_from_turtle*` / `py_to_turtle` / `PyTurtleStream` / `PyDiskTurtleStream`.

**Steps:**

- [ ] Add `PyRdfStream` class.
- [ ] Add `py_import_turtle` and `py_import_ntriples` using the `PyReader` bridge.
- [ ] Add `py_export_turtle` and `py_export_ntriples` accepting either a single `PyLinkMLInstance` or an iterable.
- [ ] Wire the new functions and class in the `#[pymodule]` init block.
- [ ] Remove ALL old `py_from_turtle*`, `py_from_ntriples*`, `py_to_turtle`, `PyTurtleStream`, `PyDiskTurtleStream`.
- [ ] **Build:** `cargo build -p linkml_runtime_python && cargo build -p linkml_runtime_python --features disk_graph`. Expected: clean both ways.
- [ ] **Commit:** `feat(python): new import_turtle/export_turtle entry points + PyRdfStream; drop old API`

---

## Task 8: Python convenience helpers

**Files:**
- Modify: `python/linkml_runtime/__init__.py` (or wherever the Python package lives — check `src/python/` for a `python/` subdir or similar).

**Key shape:**

```python
import io
import os
from typing import IO, Iterable, Union

from linkml_runtime_python import (
    import_turtle as _import_turtle,
    import_ntriples as _import_ntriples,
    export_turtle as _export_turtle,
    export_ntriples as _export_ntriples,
    RdfStream,
)

def import_turtle_path(path: Union[str, os.PathLike], schema_view, root_classes, **kwargs) -> RdfStream:
    with open(path, "rb") as f:
        return _import_turtle(f, schema_view, root_classes, **kwargs)
    # NB: file is closed after construction; the parser drains synchronously

def import_turtle_str(text: str, schema_view, root_classes, **kwargs) -> RdfStream:
    return _import_turtle(io.BytesIO(text.encode("utf-8")), schema_view, root_classes, **kwargs)

# parallel for import_ntriples_*, export_turtle_*, export_ntriples_*
```

**Steps:**

- [ ] Locate the Python package directory (likely `src/python/python/` or `src/python/python_pkg/` — check the existing structure).
- [ ] Add convenience helpers.
- [ ] Re-export the Rust extension's symbols (`import_turtle`, etc.) so callers can `from linkml_runtime import import_turtle`.
- [ ] **Commit:** `feat(python): convenience wrappers import_turtle_path / import_turtle_str etc.`

---

## Task 9: `linkml-convert` migration

**Files:**
- Modify: `src/tools/src/bin/linkml_convert.rs`.

**Steps:**

- [ ] Replace the RDF input branch's use of `import_owned_store_streaming` with the new `import_turtle` / `import_ntriples` entry points and `ImportOptions { disk_path, strict }`.
- [ ] Replace the RDF output branch's use of `write_turtle` / `write_ntriples` with `export_turtle` / `export_ntriples`.
- [ ] Add `--strict` flag.
- [ ] At end of import, drain `stream.pop_warnings()` and print a count to stderr; if a new `-v` / `--show-warnings` is convenient, dump each one.
- [ ] Smoke-test on the same `/tmp/disk_graph_smoke.ttl` fixture: in-memory and disk-graph runs produce identical JSON.
- [ ] **Commit:** `refactor(linkml-convert): migrate to new import_*/export_* API; add --strict`

---

## Task 10: Tests for warning categories + strict mode

**Files:**
- Create: `src/runtime/tests/warning_emission.rs`.

**Test cases:**

1. **MaxCountViolation**: schema with `T.name` single-value; TTL with two `name` triples for one subject. Assert one warning of type `MaxCountViolation`, severity `Warning`, mentioning `name`.
2. **UndeclaredSlot**: schema with `T.name`; TTL with a `T` subject having `name "x"` and `foo "y"`. Assert one `UndeclaredSlot` warning for `foo`.
3. **SlotRangeViolation**: schema with `T.age` ranged on `xsd:integer`; TTL with `age "abc"^^xsd:string`. Assert one `SlotRangeViolation`.
4. **Clean input**: schema and data match → zero warnings.
5. **Strict mode**: same input as case 1, but `strict=true`. Assert `next()` returns `Err(ImportError::ValidationFailure(...))` and subsequent calls return `None` (StopIteration on Python).
6. **`pop_warnings` drain semantics**: harvest with N warnings; pop, assert N returned; pop again, assert empty.

**Steps:**

- [ ] Write test cases 1-6 using the `import_turtle` API.
- [ ] **Run:** `cargo test -p linkml_runtime --features disk_graph --test warning_emission`. Expected: all pass.
- [ ] **Commit:** `test(runtime): warning emission for the three categories + strict mode + pop drain`

---

## Task 11: Multiset-parity test (in-memory vs disk)

**Files:**
- Modify: `src/runtime/tests/disk_graph_parity.rs` — replace with a stronger version using deep-canonicalized multisets.

**Steps:**

- [ ] Rewrite the parity test to compare a `HashSet` of canonicalized instance JSONs (with sorted lists, sorted keys) between the in-memory and disk runs.
- [ ] Use the existing `two_trains_sharing_operator` fixture from `streaming_import.rs` (or a similar small graph with shared inlined subjects).
- [ ] **Run:** `cargo test -p linkml_runtime --features disk_graph --test disk_graph_parity`. Expected: pass.
- [ ] **Commit:** `test(runtime): deep-canonical multiset parity between in-mem and disk`

---

## Task 12: Re-run measurement and update spec

**Files:**
- Modify: `docs/superpowers/specs/2026-05-21-python-api-streaming-and-diagnostics-design.md`.

**Steps:**

- [ ] Run: `LABEL=phase3-disk DISK_GRAPH=target/rinf-measure/phase3-disk/store bash scripts/measure_rinf_de.sh`.
- [ ] Extract peak RSS and wall-clock from procmon.log.
- [ ] Verify peak RSS still < 2 GB (Phase 2 was 0.90 GB; Phase 3 has the warning sink overhead — expected ~1.0-1.2 GB, still well under target).
- [ ] Verify wall-clock change is within ~10% of Phase 2 (84.9 s baseline).
- [ ] Add a "Phase 3 measurement" subsection to the spec with the new numbers.
- [ ] **Commit:** `docs: Phase 3 measured results`

---

## After all tasks

- Run the full test surface under both feature configurations:
  ```bash
  cargo test -p linkml_runtime
  cargo test -p linkml_runtime --features disk_graph
  cargo test -p linkml_runtime_python --features disk_graph
  cargo test -p linkml_tools
  ```
- Smoke-test `linkml-convert` on the RINF dataset end-to-end (already part of Task 12 measurement).
- Self-review: every spec success criterion has a covering task.

---

## Self-review (against the spec)

- [x] File-like input on both directions (Tasks 3, 4, 6, 7)
- [x] `disk_path` kwarg (Tasks 3, 7)
- [x] `pop_warnings` / `warning_count` (Tasks 0, 2, 7)
- [x] Three warning categories (Task 0)
- [x] Strict mode (Tasks 0, 7, 10)
- [x] `unconsumed_subjects` in-memory only (Tasks 1, 2)
- [x] All old API removed (Tasks 5, 7)
- [x] CLI migrated (Task 9)
- [x] Tests for warnings + strict + parity (Tasks 10, 11)
- [x] Measurement (Task 12)

No spec requirement is uncovered.
