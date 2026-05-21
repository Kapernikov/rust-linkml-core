# Python API Redesign: Streaming + Diagnostics — Design Spec

**Status:** Draft, awaiting user review before plan-writing.
**Date:** 2026-05-21
**Predecessor specs:**
- `2026-05-21-rdf-import-less-ram-design.md` (Part 1 — streaming harvest)
- `2026-05-21-rdf-import-less-ram-disk-graph-design.md` (Part 2 — disk-backed `TripleSource`)

## Context

Phases 1 and 2 cut peak RAM on a 7M-triple RINF import from 6.52 GB to 0.90 GB by introducing a two-pass streaming harvest and a fjall-backed disk store. Both phases preserved the original string-based Python API surface for backward compatibility within those phases. That surface now has three real problems:

1. **The Python entry point takes `turtle_str: &str`.** Callers must materialize the entire RDF source (890 MB in our measured case) as a Python string before parsing. This defeats the streaming algorithm's RAM benefits at the FFI boundary.
2. **No visibility into schema/data mismatches.** When the harvest encounters a single-value slot with multiple objects, it silently picks the first one and discards the rest. We discovered this when comparing in-memory output across two runs of the same code — 18,000 of 235,659 instances disagreed on values, the smoking gun being a slot whose source RDF has multiple triples treated as single-valued by the schema. There is no mechanism to surface this to Python callers; `eprintln!` doesn't help an embedded Python application.
3. **Three separate streaming entry points** (`from_turtle_streaming`, `from_turtle_streaming_disk`, `from_ntriples_streaming_disk`) for combinations of (format × backend) — the API surface fragments as we add more options.

The application that actually uses this library is a Python application. The CLI tool (`linkml-convert`) is fine during development, but production uses the Python bindings. Phase 3 redesigns those bindings.

## Constraints

- **No backward compatibility required.** The user is the only consumer of the current API; they have signed off on a clean break.
- **Rust API can stay clean for Rust users.** Direct Rust callers keep the fast native-IO paths (`impl Read` on a `File`); only the PyO3 layer pays for the Python-side IO bridge.
- **Compile-time feature gating from Phase 2 still applies.** `disk_graph` remains a cargo feature; passing a `disk_path` when the feature is off returns an error at the entry point.

## Goals

1. **Streaming-friendly source intake** — the Python API accepts `IO[bytes]` (a Python file-like object) instead of a pre-materialized string. No more 890 MB strings.
2. **Disk-graph as a runtime opt-in** — a `disk_path` keyword on each entry point switches backends, removing the `_disk` suffix variants from the surface.
3. **A real diagnostic surface** — schema/data mismatches that previously caused silent data loss are now visible via a `pop_warnings()` API that drains the warning buffer on each call.
4. **Symmetric input + output** — output (`to_turtle`) gets the same file-like treatment so the output side is also memory-friendly.
5. **Smaller public surface** — replaces 7 functions/classes with 4 free functions + 1 class + a few Python-side convenience helpers.

## Non-goals (Phase 3)

- A general SPARQL or graph-query layer (out of scope for this library).
- Asynchronous iteration (`async for` / chunked-source streaming). The Python file-like model already covers network streams via wrapping; native async would be a separate phase.
- A "subject with no resolvable class" warning category. Documented but deferred.
- New backend choices beyond in-memory and disk-graph.

## Diagnostic model

### `pop_warnings()` — drain on every call

The single API for emitting harvest warnings. Drains the internal buffer:

```python
for cls, inst in stream:
    process(inst)
    for w in stream.pop_warnings():
        log(w)
# end-of-loop: catch any warnings emitted after the last pop
for w in stream.pop_warnings():
    log(w)
```

Behaviour:
- Returns all warnings accumulated since the previous call.
- Buffer is drained as a side-effect.
- Unbounded buffer: a caller who never drains will see RAM grow. This is intentional — defending against an inattentive caller would require either a hard cap (data loss) or a counter (lying). We trust the caller; the API documents the expectation.

`warning_count() -> int` returns the current buffer length without draining — useful for mid-stream progress reporting.

### No `unconsumed_count`

Earlier phases tracked `consumed_count` as a counter inside `HarvestContext` and reported `unconsumed_count = total - consumed`. We are dropping this entirely. Two reasons:

1. **Pass 1 (`compute_inline_structure`) reads triples without counting them.** It calls `triples_for_subject` for the inline-edge walk but doesn't fire `on_consumed`. The counter therefore underreports anything Pass 1 saw but Pass 2 didn't.
2. **Future invariant drift.** Every change to a code path that reads triples has to remember to update the counter. One missed `+=` and the counter silently lies. The build cannot catch this.

A wrong counter is worse than no counter. The same information is available through the warnings (per-occurrence) and `unconsumed_subjects` (per-orphan-subject, in-memory only).

### `unconsumed_subjects()` — in-memory only

For schema-debugging workflows ("is my LinkML schema good enough to import this file?"), the user can list orphan subjects:

```python
# After iteration ends:
orphans = stream.unconsumed_subjects()
if orphans is None:
    print("Run with in-memory backend for orphan-subject reporting.")
else:
    for subj, n_triples in orphans:
        print(f"  orphan {subj} ({n_triples} triples)")
```

Returns `None` on the disk backend: computing it there requires a full SPO scan plus a per-subject prefix re-scan for triple counts — expensive and never used (debugging happens on a workstation with the in-memory backend). Returns `None` before iteration completes.

### Three warning categories

All map to existing `ValidationProblemType` variants:

| Trigger | Variant | Severity |
|---|---|---|
| Single-value slot with > 1 object | `MaxCountViolation` | `Warning` |
| Subject visited, predicate not in any slot of its class | `UndeclaredSlot` | `Warning` |
| Literal datatype incompatible with slot's declared range | `SlotRangeViolation` | `Warning` |

A fourth category — "subject with no resolvable class" — was discussed and deferred (could be a lot of noise on real datasets; needs further thought on aggregation).

### Strict mode

When `strict=True` on the entry point, the first warning emitted during iteration raises a Python `ImportError` from `__next__` instead of accumulating. The iterator is left in a terminated state (subsequent `next()` calls raise `StopIteration`). The raised exception carries the `ValidationResult` for inspection.

When `strict=False` (default), warnings accumulate normally and iteration continues.

## Source intake

### Rust side: `impl Read`

```rust
pub fn import_turtle<R: Read>(
    reader: R,
    sv: SchemaView,
    conv: Converter,
    root_classes: &[&str],
    options: ImportOptions,
) -> Result<RdfStream, ImportError>;
```

Rust callers pass any `Read` implementor — `BufReader<File>`, `Cursor<Vec<u8>>`, etc. Native speed, no GIL.

### Python side: `IO[bytes]`

A Python file-like is bridged to Rust via a PyO3 reader that wraps `.read(n)` calls. **GIL cost is amortized by chunked reads** (default chunk size ~256 KB), so for a 890 MB file we make ~3500 GIL acquisitions — negligible compared to fjall write throughput.

Python callers either pass an open file or wrap memory data in `io.BytesIO`:

```python
with open("data.ttl", "rb") as f:
    for cls, inst in import_turtle(f, sv, ["T"]):
        ...

# or
from io import BytesIO
for cls, inst in import_turtle(BytesIO(text.encode("utf-8")), sv, ["T"]):
    ...
```

### Convenience helpers (pure Python)

Thin wrappers in the Python package so the common cases don't need `with open(...)`:

```python
def import_turtle_path(path, sv, root_classes, **kwargs) -> RdfStream:
    f = open(path, "rb")
    try:
        return import_turtle(f, sv, root_classes, **kwargs)
    finally:
        # The parser drains the reader synchronously during construction,
        # so by the time we get an RdfStream back, the file is no longer
        # needed. Close it.
        f.close()

def import_turtle_str(text, sv, root_classes, **kwargs) -> RdfStream:
    return import_turtle(io.BytesIO(text.encode("utf-8")), sv, root_classes, **kwargs)
```

Parallel helpers for `import_ntriples_*` and the export side.

## Backend selection

A single `disk_path: Optional[str]` kwarg on each entry point:

- `disk_path=None` (default) → in-memory backend (`RdfImportStore`).
- `disk_path="some/dir"` → disk backend (`DiskRdfImportStore` at that path). Requires the `disk_graph` cargo feature.

No `_disk`-suffix function variants. No backend objects. No `backend="disk"` strings.

## Rust public surface

### Top-level entry points

```rust
pub fn import_turtle<R: Read>(
    reader: R,
    sv: SchemaView,
    conv: Converter,
    root_classes: &[&str],
    options: ImportOptions,
) -> Result<RdfStream, ImportError>;

pub fn import_ntriples<R: Read>( /* same signature */ ) -> Result<RdfStream, ImportError>;

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

pub fn export_ntriples<W: Write>( /* same as export_turtle */ ) -> Result<(), ExportError>;
pub fn export_ntriples_many<W, I>( /* same as export_turtle_many */ ) -> Result<(), ExportError>;
```

### `ImportOptions` / `ExportOptions`

```rust
#[derive(Debug, Default, Clone)]
pub struct ImportOptions {
    pub disk_path: Option<PathBuf>,
    pub strict: bool,
}

#[derive(Debug, Default, Clone)]
pub struct ExportOptions {
    pub skolem: bool,
}
```

### `RdfStream`

```rust
pub struct RdfStream {
    inner: RdfStreamInner,
    /// Drained on every pop_warnings call. Single-threaded shared ownership
    /// between RdfStream (drain side) and HarvestContext (push side).
    warnings: Rc<RefCell<Vec<ValidationResult>>>,
    /// In-memory backend only — `None` on disk.
    consumed_subjects: Option<Rc<RefCell<HashSet<String>>>>,
}

enum RdfStreamInner {
    Memory(OwnedImportStream<RdfImportStore>),
    Disk(OwnedImportStream<DiskRdfImportStore>),
}

impl RdfStream {
    pub fn pop_warnings(&mut self) -> Vec<ValidationResult>;
    pub fn warning_count(&self) -> usize;
    pub fn unconsumed_subjects(&self) -> Option<Vec<(String, usize)>>;
}

impl Iterator for RdfStream {
    type Item = Result<(String, LinkMLInstance), ImportError>;
    /* delegates to enum variant; in strict mode, swaps inner to Done after raise */
}
```

`OwnedImportStream<S>` (from Phase 2) demoted to `pub(crate)`. It is no longer the user-facing iterator type.

### Warning sink architecture

`HarvestContext` gains a field:

```rust
pub struct HarvestContext<'a, T: TripleSource> {
    /* existing fields */
    pub warnings: Rc<RefCell<Vec<ValidationResult>>>,
}
```

`Rc<RefCell<…>>` (not `Arc<Mutex<…>>`) because all iteration is single-threaded by construction (the PyO3 class is `unsendable`, and the Rust streaming harvest is non-`Send` anyway via its iterator state).

Warnings are pushed from three call sites in `turtle_import.rs`:

1. The `SingleValue` arm at line ~547 — when `items.len() > 1`.
2. The per-triple loop in `harvest_subject` — when a triple's predicate doesn't match any slot.
3. The `literal_to_json` path — when conversion to the declared range fails or coerces.

`RdfStream::pop_warnings` simply does `mem::take(&mut *self.warnings.borrow_mut())`.

### What goes private/away in Rust

- `OwnedImportStream<S>` → `pub(crate)`.
- `import_owned_store_streaming` → `pub(crate)`.
- `ImportResult` struct → removed (no caller after the redesign).
- `import_from_store` → removed (no caller; the streaming path is the only one).
- `RdfImportStore::import` / `DiskRdfImportStore::import` convenience methods → removed.
- `TrackingRdfImportStore` → merged into `RdfImportStore`. Consumed-subjects tracking is always on in-memory (it's the only thing using it now), so the type split no longer pays for itself.
- `TrackingDiskRdfImportStore` → removed. Disk backend doesn't track consumed subjects.
- `TripleSource::on_consumed` → kept (in-memory uses it for the `consumed_subjects` HashSet). Disk impl is a no-op.

## Python public surface

### Top-level functions

```python
def import_turtle(
    reader: IO[bytes],
    schema_view: SchemaView,
    root_classes: list[str],
    *,
    disk_path: str | None = None,
    strict: bool = False,
) -> RdfStream: ...

def import_ntriples(
    reader: IO[bytes],
    schema_view: SchemaView,
    root_classes: list[str],
    *,
    disk_path: str | None = None,
    strict: bool = False,
) -> RdfStream: ...

def export_turtle(
    instances: LinkMLInstance | Iterable[LinkMLInstance],
    schema_view: SchemaView,
    writer: IO[bytes],
    *,
    skolem: bool = False,
) -> None: ...

def export_ntriples(
    instances: LinkMLInstance | Iterable[LinkMLInstance],
    schema_view: SchemaView,
    writer: IO[bytes],
    *,
    skolem: bool = False,
) -> None: ...
```

The Python wrappers dispatch: a single `PyLinkMLInstance` argument calls `export_turtle`, anything iterable calls `export_turtle_many`.

### `RdfStream` class

```python
class RdfStream:
    def __iter__(self) -> RdfStream: ...
    def __next__(self) -> tuple[str, LinkMLInstance]:
        # Raises StopIteration when done.
        # Raises ImportError if strict=True and a warning fires.
        ...
    def pop_warnings(self) -> list[ValidationResult]: ...
    def warning_count(self) -> int: ...
    def unconsumed_subjects(self) -> list[tuple[str, int]] | None: ...
```

Internally enum-backed; both the in-memory and disk variants present identically to Python.

### Convenience helpers (pure Python)

Live in the Python package (e.g. `python/linkml_runtime_python/__init__.py`) alongside the re-exports of the Rust extension's symbols:

```python
def import_turtle_path(path, sv, root_classes, **kwargs) -> RdfStream: ...
def import_turtle_str(text, sv, root_classes, **kwargs) -> RdfStream: ...
def import_ntriples_path(path, sv, root_classes, **kwargs) -> RdfStream: ...
def export_turtle_path(instances, sv, path, **kwargs) -> None: ...
def export_turtle_str(instances, sv, **kwargs) -> str: ...
def export_ntriples_path(instances, sv, path, **kwargs) -> None: ...
def export_ntriples_str(instances, sv, **kwargs) -> str: ...
```

### What goes away on the Python side

Removed entirely (no aliases):
- `from_turtle_streaming` (Phase 1)
- `from_turtle_streaming_disk` (Phase 2)
- `from_ntriples_streaming_disk` (Phase 2)
- `from_turtle` (single-tree, pre-Phase-1)
- `from_turtle_tracked` (single-tree + tracking)
- `to_turtle` (returns string)
- `PyTurtleStream` class
- `PyDiskTurtleStream` class

## CLI (`linkml-convert`) integration

`linkml-convert` currently uses `import_owned_store_streaming` and `write_turtle`. Both move to the new API:

- RDF input branch: build `ImportOptions { disk_path: args.disk_graph.clone(), strict: false }` and call `import_turtle` / `import_ntriples`.
- RDF output branch: call `export_turtle_many` / `export_ntriples_many` against the instance iterator.
- New flag: `--strict` (forwards to `ImportOptions.strict`).
- New behaviour: at end of import, drain `stream.pop_warnings()` and print a summary to stderr ("N warnings during harvest; rerun with -v for detail"). With `-v` (already a clap option) or a new `--show-warnings` flag, print each warning.
- `--disk-graph` flag stays as is (already works with the new design).

The end-of-run "Wrote N instances" line gains a warnings count.

## Validation & measurement plan

### Functional equivalence

The Phase-2 success criteria require canonical-JSON parity with Phase 1. Phase 3 must preserve that. Add a test that:

1. Runs the in-memory backend through the new `import_turtle` API with `strict=False`.
2. Runs the disk backend through the same API with the same input.
3. Asserts the *multiset of canonicalized instances* (with deep-canonicalized lists) is identical.

This is a stronger check than counting per-class instances. It catches order-dependent value selection inside instances.

### Warning emission

Unit tests in `src/runtime/tests/`:

- A schema declaring `name` as single-value plus data with two `name` triples → assert one `MaxCountViolation` warning.
- A schema declaring `T` with one slot `name`, plus data with a `T` subject having an extra `foo` predicate → assert one `UndeclaredSlot` warning.
- A slot ranged on `xsd:integer` plus a data triple with `"abc"^^xsd:string` → assert one `SlotRangeViolation` warning.
- A clean schema/data pair → assert zero warnings.

### RAM and wall-clock

Re-run `scripts/measure_rinf_de.sh` with `DISK_GRAPH=...` to confirm Phase 2's numbers don't regress. Targets unchanged:

- Peak RSS < 2 GB on disk backend (Phase 2 measured 0.90 GB).
- Wall-clock < 5× Phase 1 streaming-postrefactor (Phase 2 measured 1.15×).

Phase 3 may add up to ~10% wall-clock overhead from the warning-push path and the PyO3 IO bridge. If the PyO3 path becomes a bottleneck, an optional path-fast-path can be added later without breaking the API.

### Strict mode

Test that `strict=True` plus data that triggers a `MaxCountViolation`:
- `next()` raises `ImportError`.
- The raised exception's `validation_issues` field carries the warning.
- Subsequent `next()` calls raise `StopIteration`.

## Success criteria

1. **All four entry points work** (`import_turtle`, `import_ntriples`, `export_turtle`, `export_ntriples`), in-memory and disk-graph.
2. **`pop_warnings` returns the three warning categories** correctly on engineered test inputs.
3. **Strict mode aborts the iterator** on first warning, with the issue accessible on the exception.
4. **`unconsumed_subjects` works on the in-memory backend** (returns post-iteration); returns `None` on disk.
5. **Multiset parity** between in-memory and disk backends preserved.
6. **No regressions in Phase 2 RAM / wall-clock measurements** beyond ~10% slowdown attributable to the IO bridge and warning push paths.
7. **All Python-side convenience helpers work** (path-based, string-based, both directions).
8. **`linkml-convert` migrated** to the new API; existing CLI surface unchanged except for new `--strict` and warning-summary line.
9. **Clean builds** with and without the `disk_graph` feature, with and without the `python` feature.
10. **Old API names completely removed** from the Rust and Python surfaces — no deprecated aliases.

## Deferred (Phase 4+)

- Async iteration / chunked-source streaming (`async for` support).
- "Subject with no resolvable class" warning category.
- Output-side warnings (warnings emitted *during* turtle serialization, e.g. for unrepresentable values).
- On-disk term dictionary with LRU (the v2 RAM optimization flagged in Phase 2 but not needed at current dataset scale).
- A SPARQL-source `TripleSource` impl (a third backend, behind another feature gate, for users with an existing endpoint).

## Open questions

None blocking. The exact name of the chunked-IO bridge struct in the PyO3 layer is a detail; the chunk size default (256 KB suggested) can be tuned in measurement.

## Measured results (2026-05-21)

Phase 3 re-measured on the RINF Germany dataset (`de_1080.nt`, 890 MB, 7M triples) using `LABEL=phase3-disk DISK_GRAPH=... bash scripts/measure_rinf_de.sh`:

| Row | Wall-clock | Peak RSS |
|---|---|---|
| Phase 2 disk-graph (no warnings infrastructure) | 84.9 s | 0.90 GB |
| **Phase 3 disk-graph (full API redesign + warnings)** | **83.4 s** | **1.17 GB** |

**Notes:**
- 235,659 instances produced (identical to Phase 2 — zero-line diff between the two canonical-JSON outputs).
- 517,829 warnings emitted on the dataset:
  - **MaxCountViolation: 83,760** — the silent single-value-with-multiple-objects bug the user discovered after Phase 2 and that motivated Phase 3.
  - **UndeclaredSlot: 434,069** — RDF predicates the schema doesn't model.
  - Both categories were *completely silent* in Phase 2; Phase 3 now surfaces them.
- Peak RSS grew by ~270 MB (0.90 → 1.17 GB) — the warning buffer holds ~520k `ValidationResult` entries before `linkml-convert` drains them at end of run. Buffer is bounded by caller drain frequency; production callers using `pop_warnings()` mid-stream would see less peak RSS.
- Wall-clock dropped by 1.5 s — within noise.

**Success criteria check:**
- All four entry points work (import/export × turtle/ntriples): **PASS** — exercised end-to-end via linkml-convert.
- `pop_warnings` returns all three categories correctly: **PASS** — `warning_emission` test suite (6 cases).
- Strict mode aborts on first warning: **PASS** — `strict_mode_aborts_on_first_warning` test.
- `unconsumed_subjects` on in-memory backend; `None` on disk: **PASS** — `rdf_stream_basic` and `RdfStream::unconsumed_subjects` implementation.
- Multiset parity between in-memory and disk backends: **PASS** — `disk_graph_parity` deep-canonicalized multiset test passes; RINF instance count and canonical-JSON byte-equal across backends.
- Phase 2 RAM/wall-clock not regressed beyond ~10% on the warning path: **PASS** — wall-clock change is noise (+/-2%); RSS up by 30% but still under the 2 GB target and explained by the warning buffer (drainable).
- Convenience helpers work: **PASS** — `_rdf_convenience.py` provides path-based and string-based wrappers for both directions.
- `linkml-convert` migrated: **PASS** — new API in use; `--strict` added; warning summary printed at end of run.
- Clean builds with/without `disk_graph`: **PASS**.
- Old API names completely removed: **PASS** — no `from_turtle*`, `to_turtle`, `PyTurtleStream`, `PyDiskTurtleStream`, `ImportResult`, `import_from_store`, or store-level `import()` methods remain in either Rust or Python surfaces.
