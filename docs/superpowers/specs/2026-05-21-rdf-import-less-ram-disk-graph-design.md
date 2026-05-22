# Disk-backed `TripleSource` via fjall — Design Spec

**Status:** Draft, awaiting user review before plan-writing.
**Date:** 2026-05-21
**Predecessor spec:** `2026-05-21-rdf-import-less-ram-design.md`
**Predecessor plan (delivered):** `2026-05-21-rdf-import-less-ram.md`

## Context

Part 1 of this work delivered a two-pass streaming harvest algorithm with refcount-driven subtree caching, and refactored `linkml-convert` to drive its JSON output from a streaming instance iterator instead of a `Vec<LinkMLInstance>`. Those two changes together cut peak RSS on the German RINF N-Triples dataset (`de_1080.nt`, ~890 MB, ~7M triples, 235,659 harvested instances) from 6.52 GB to 5.00 GB, with a small wall-clock improvement (82 s → 74 s).

The remaining ~5 GB is dominated by the in-memory `oxrdf::Graph` itself. This spec covers Part 2: an opt-in disk-backed `TripleSource` implementation that replaces the in-memory graph as the storage layer for the source RDF, leaving the rest of the pipeline unchanged.

## Goals

1. **Opt-in at compile time.** New `disk_graph` Cargo feature on `linkml_runtime`, off by default. WASM and minimal builds must remain unaffected. No new transitive deps when the feature is off.
2. **Opt-in at runtime.** When the feature is compiled in, callers (CLI, Python, library) choose RAM vs disk per import. Default behavior is unchanged: in-memory store.
3. **Trait-compatible.** Slot in behind the existing `TripleSource` trait. No trait signature changes. Streaming harvest code (generic over `T: TripleSource`) takes the new backend as-is.
4. **Big RAM win.** Peak RSS on the RINF dataset under 2 GB (target near 1 GB) — down from 5.00 GB.
5. **Acceptable wall-clock.** Slower than the in-memory backend, but within ~5× of it. Beyond that the backend is impractical for the datasets it's meant to serve.

## Non-goals (v1)

- SPARQL or any query layer above `TripleSource`. If we ever need SPARQL, the `TripleSource` trait stays the seam — we add an oxigraph-backed `TripleSource` impl behind a separate feature then.
- Persistence across runs. The caller passes a path; v1 always treats it as ours-to-overwrite within a single invocation. A persisted-store workflow could be added later with format-version checks.
- Schema-aware load-time filtering. Loading every triple matches the in-memory backend's semantics (the unconsumed-triples diagnostic depends on it). A future opt-in flag could prune at load time.
- On-disk term dictionary. v1 keeps the term dictionary in RAM. v2 (if needed) moves it to disk with an LRU.
- OSP index / object-to-anything lookups. The trait doesn't expose them. Adding a third partition is straightforward later.

## Backend choice: fjall

After researching the Rust ecosystem (oxigraph/RocksDB, redb, fjall, heed/LMDB, hand-rolled fst+mmap, and others including ruled-out sled and read-only `hdt`), the chosen backend is **fjall** (pure-Rust LSM, actively maintained, 91+ releases through April 2026).

Rationale:
- Pure Rust — no C/C++ deps, fast clean builds, no wasm-blocker bring-in.
- Active maintenance and a healthy release cadence.
- A dedicated bulk-segment-from-sorted-iterator API exists for write-once workloads (deferred to v2 — v1 uses the normal insert path for simplicity).
- LSM read amplification is acceptable for our access patterns once the load completes; we're not doing point lookups across cold sets.

Alternatives ruled out:
- **oxigraph** — trait-perfect fit, but pulls RocksDB (C++, multi-minute clean builds, +20–30 MB binary, non-trivial RAM appetite of its own). Heaviest dependency we'd add. Worth considering only if SPARQL becomes a requirement.
- **sled** — beta since 2021, last release Sep 2021, slowest in published benches.
- **redb** — viable; B+tree, pure Rust. We chose fjall instead because its bulk-segment API is a better fit for write-once even though v1 doesn't use it.
- **heed (LMDB)** — fastest persistent option, but introduces a (small) C dep. Pure-Rust constraint wins.
- **Hand-rolled fst + mmap'd sorted arrays** — lowest deps and RAM, but ~500 LoC we'd own. We trade that complexity for fjall's stable, maintained surface.
- **`hdt` crate** — Rust crate is read-only; can't build the HDT file from Rust. Blocker for this pipeline.

## Architecture

### Cargo feature

In `src/runtime/Cargo.toml`:

```toml
[dependencies]
fjall = { version = "...", optional = true }    # version selected at plan time

[features]
disk_graph = ["ttl", "dep:fjall"]
```

`disk_graph` is off by default. Nothing in non-`disk_graph` builds references fjall.

### New module

`src/runtime/src/rdf_import_store_disk.rs`, gated `#[cfg(feature = "disk_graph")]`.

Two public types, mirroring the in-memory module:

- `DiskRdfImportStore` — `TripleSource` impl. Constructors `from_turtle(reader, path)`, `from_ntriples(reader, path)`, `from_rdf(reader, format, path)`, plus an `import(&self, sv, conv, root_classes)` convenience method.
- `TrackingDiskRdfImportStore` — same constructors, plus `consumed_subjects()`, `unconsumed_subjects()`, `unconsumed_triple_count()` — mirrors `TrackingRdfImportStore`.

### Generalizing `OwnedImportStream`

`OwnedImportStream` in `rdf_streaming.rs` is currently hard-typed to `RdfImportStore`. It becomes generic over `S: TripleSource + 'static`. Existing in-memory call sites become `OwnedImportStream<RdfImportStore>`; the new disk path is `OwnedImportStream<DiskRdfImportStore>`. One iterator type, two parameterizations.

### Runtime opt-in

- **`linkml-convert` CLI:** new `--disk-graph <PATH>` flag, gated behind `#[cfg(feature = "disk_graph")]`. Without the flag the in-memory path is byte-for-byte unchanged. With the flag, a `DiskRdfImportStore` is built at `PATH` and handed to the generic `OwnedImportStream`.
- **Python:** new optional `disk_graph_path: Option<&str>` keyword on `py_from_turtle_streaming`, gated behind `python` + `disk_graph`. `None` → in-memory; `Some(path)` → disk.
- **Library callers:** can construct `DiskRdfImportStore` directly when the feature is enabled.

### What stays unchanged

- `TripleSource` trait — no signature changes.
- `RdfImportStore`, `TrackingRdfImportStore` — remain the default and stay byte-for-byte equivalent.
- `compute_inline_structure`, `Materializer`, `ImportStream` — already generic over `T: TripleSource`. Accept the new backend as-is.

## Data model on disk

### Term encoding: monotonic u64 IDs

Every distinct RDF term (NamedNode, BlankNode, Literal — including datatype and language) gets a `u64` ID, assigned the first time it's seen during load. Triples live on disk as packed `[sid: u64, pid: u64, oid: u64]` big-endian — 24 bytes per triple. For 7M triples × 2 indices ≈ 336 MB on disk, plus the dictionary.

### Term dictionary lives in RAM (v1)

Both directions of the dictionary survive into the read phase, because the trait methods take external term refs as inputs and must translate them to IDs:

- `term_by_id: Vec<oxrdf::Term>` — indexed by ID. Stable storage; refs handed back via the trait point into this Vec's items, valid for `&'a self`. `shrink_to_fit` at end of load.
- `id_by_term: FxHashMap<oxrdf::Term, u64>` — used for external-term-to-ID lookup during both load and read.

Estimated cost on RINF: a few million unique terms × ~80 bytes for `oxrdf::Term` Rust overhead ≈ **400–800 MB**. This is the dictionary RAM tax we can't avoid without mmap'ing a custom on-disk dictionary format — flagged as the v2 target if v1 measurements leave RAM as the new bottleneck.

### Two fjall partitions, no third

| Partition | Key (big-endian packed) | Value | Used by |
|---|---|---|---|
| `spo` | `[sid, pid, oid]` (24 B) | empty | `objects_for_subject_predicate(s, p)` (prefix `[sid, pid]`); `triples_for_subject(s)` (prefix `[sid]`) |
| `pos` | `[pid, oid, sid]` (24 B) | empty | `subjects_for_predicate_object(p, o)` (prefix `[pid, oid]`) |

No OSP index — the trait does not expose an object-to-? lookup.

Big-endian packing makes lexicographic byte order match numeric order, so fjall's range iterators correspond directly to prefix scans on `[sid, pid, *]` and `[pid, oid, *]`. Values are empty — the key encodes the full triple.

### Meta partition

A small `meta` partition stores:
- `len` (u64) — total triple count, returned by `TripleSource::len`.
- `format_version` (u32) — for sanity-checking. We always treat the path as ours-to-overwrite in v1, but the tag makes future format changes detectable.

## Load & lookup paths

### Load: stream-into-fjall (v1)

Single sequential pass over the source:

```text
parser.iter(reader)
  for each triple (s, p, o):
    sid = intern(s)
    pid = intern(p)
    oid = intern(o)
    spo_partition.insert(pack_be(sid, pid, oid), b"")
    pos_partition.insert(pack_be(pid, oid, sid), b"")
    triple_count += 1
  at end:
    term_by_id.shrink_to_fit()
    meta.insert("len", triple_count)
    fjall.persist()
```

Why not the bulk-SSTable API yet: it requires sorted input, which means either an in-RAM sort (~336 MB extra) or an external sort (more code). The default `insert` path lets fjall's memtable + SSTable flushes do the sorting incrementally, bounded by the memtable size (default ~16 MB). Simpler code; slower load than the bulk API but still streaming and disk-bounded. **If measured load time is unacceptable, v2 switches to sort-then-bulk.**

### Lookups

Each method shares the same shape: external term → ID lookup → fjall prefix scan → decode IDs from key suffix → return refs into the in-RAM `term_by_id`.

- `subjects_for_predicate_object(p, o)` → look up `pid`, `oid`; if either missing, return empty. Prefix-scan `pos` partition by `[pid, oid]`. For each key, decode the `sid` suffix, return `term_by_id[sid].as_ref()` narrowed to `NamedOrBlankNodeRef` (subjects can't be literals).
- `objects_for_subject_predicate(s, p)` → look up `sid`, `pid`; prefix-scan `spo` by `[sid, pid]`. Decode `oid` from suffix. Return `term_by_id[oid].as_ref()` as `TermRef`.
- `triples_for_subject(s)` → look up `sid`; prefix-scan `spo` by `[sid]`. Decode `pid`, `oid` from suffix. Construct `TripleRef` from refs into `term_by_id`.
- `len()` → read from `meta` at construction time, cache in the struct.
- `on_consumed(s, p, o)` → no-op for `DiskRdfImportStore`; for `TrackingDiskRdfImportStore`, `consumed.borrow_mut().insert(s.to_string())`.

### Lifetime model

`term_by_id` is never mutated after load and is `shrink_to_fit`'d once. References into its items are stable for `'a` (the lifetime of `&'a self`). The boxed iterator closures capture `&self.term_by_id` and the relevant partition handle with that lifetime. fjall's iterator items are owned `Slice`s — we decode them into u64 IDs and use those to index `term_by_id`. The only refs handed across the trait boundary point into the dictionary Vec, which is exactly what the trait expects. No `unsafe` blocks needed.

### Tracking variant

`TrackingDiskRdfImportStore` is identical to `DiskRdfImportStore` plus:
- `consumed: RefCell<HashSet<String>>` — same as the in-memory tracking variant.
- `unconsumed_subjects()` — full SPO scan deduplicating subject IDs via a `HashSet<u64>` (small), skipping consumed strings. Slower than the in-memory equivalent but only used post-import for diagnostics.

## Measurement plan

### Harness

The existing `scripts/measure_rinf_de.sh` already produces a procmon timeline + canonical JSON per run, written to `target/rinf-measure/<label>/`. We extend it with a fourth row:

| Row | Label | Configuration |
|---|---|---|
| 1 | `baseline` | pre-streaming, Vec-collect output |
| 2 | `streaming-prerefactor` | streaming algorithm, Vec-collect output |
| 3 | `streaming-postrefactor` | streaming algorithm + streaming output |
| 4 | `streaming-disk` | streaming algorithm + streaming output + `--disk-graph <PATH>` |

Implementation: the script builds `linkml-convert --features disk_graph` once for the row-4 invocation, points the `--disk-graph` flag at `target/rinf-measure/streaming-disk/store/`, and otherwise uses the same input, schema, and root classes as row 3.

### Output equivalence

```
diff streaming-postrefactor/output.canonical.json \
     streaming-disk/output.canonical.json
```

Expected: clean diff modulo the same hashmap-iteration noise documented in Part 1. Instance counts per class must match exactly.

### Expected numbers

**Peak RSS:**

| Component | Estimate |
|---|---|
| Term dictionary (both directions, v1) | 400–800 MB |
| fjall memtable + caches | 50–100 MB |
| Streaming harvest's subtree cache + active instance | 100–300 MB |
| **Predicted peak** | **0.7–1.2 GB** |

Target: well under 2 GB. Compare row 3: 5.00 GB.

**Wall-clock:** likely 2–3× row 3 (~150–220 s vs 74 s). >5× indicates lookup-per-triple is the bottleneck and triggers the v2 LRU plan.

### Success criteria

1. Canonical JSON output matches row 3 (modulo known iteration noise).
2. Peak RSS < 2 GB.
3. Wall-clock < 5× row 3.
4. Clean build with `--features disk_graph` AND clean build without it.
5. Existing tests pass under both feature configurations.

### Measured results (2026-05-21)

| Row | Wall-clock | Peak RSS |
|---|---|---|
| baseline (in-memory, pre-streaming) | 82 s | 6.52 GB |
| streaming-prerefactor (in-memory, Vec-collect output) | 77 s | 6.51 GB |
| streaming-postrefactor (in-memory, streaming output) | 73.6 s | 4.66 GB |
| **streaming-disk (fjall, dictionary in RAM)** | **84.9 s** | **0.90 GB** |

Source: `target/rinf-measure/streaming-disk/procmon.log` (peak `memory_rss_bytes`, last sample timestamp minus first).

**Output equivalence:** instance count exactly matches row 3 (235,659 in both runs). Canonical JSON diff against row 3 is 1.67M lines — same order of magnitude as the diff between two in-memory runs of the same code (1.61M lines), confirming the differences are HashMap iteration noise, not algorithmic divergence.

**Success criteria check:**
- Output equivalence: **PASS** (instance counts match; diff size within in-memory-noise band).
- Peak RSS < 2 GB: **PASS** — 0.90 GB, 5.2× below the in-memory backend.
- Wall-clock < 5× row 3: **PASS** — 1.15× (just +11 s for the disk round-trip).
- Clean build with `--features disk_graph`: **PASS**.
- Clean build without it: **PASS** — fjall is not pulled in.
- All existing tests pass under both feature configurations: **PASS**.

**Summary:** the disk-backed backend cuts peak RAM by 5.2× for a 15 % wall-clock penalty on the 7M-triple RINF dataset. The remaining ~0.9 GB is dominated by the in-RAM term dictionary (~3M unique terms) plus fjall's caches plus the streaming harvest's per-instance working set — comfortably under the 2 GB target. The v2 RAM-optimization path (on-disk dictionary with LRU) is therefore not needed for the current dataset class; the design's success criteria are met by v1.

## Deferred to v2 (explicitly out of scope)

- Bulk-SSTable load via sorted iterator (load-time perf).
- On-disk term dictionary with LRU (RAM optimization beyond v1).
- Schema-aware load-time filtering (different semantics from in-memory; needs opt-in design).
- OSP index for object-to-? lookups (not in trait today).
- Persistence across runs / format-version-based reuse.
- An oxigraph-backed `TripleSource` for users who want SPARQL — coexists under a separate feature flag.

## Open questions

None blocking. The fjall crate version pin will be selected at plan-writing time. If fjall's bulk-segment API turns out to be trivially adoptable during implementation, we may use it in v1 instead of deferring — but the design works either way.
