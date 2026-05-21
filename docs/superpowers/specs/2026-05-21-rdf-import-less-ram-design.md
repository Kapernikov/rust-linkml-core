# RDF Import — less RAM, support 1 GB inputs

Date: 2026-05-21
Branch: `rdf-import-less-ram`

## Problem

`linkml_runtime::turtle_import::import_from_store` works correctly today but its peak RAM usage grows linearly with input size in three independent places:

1. `RdfImportStore::from_turtle` parses the entire input into an in-memory `oxrdf::Graph`. For 1 GB of Turtle this is several GB of RAM.
2. `import_from_store` collects every harvested top-level instance tree into a `Vec` before filtering, then materializes the full `HashMap<class, Vec<LinkMLInstance>>` as the return value. The caller cannot start processing until everything is built.
3. The Python entry point takes the whole Turtle as a `str`, so the file is held by Python in addition to being parsed by Rust.

This branch addresses (1) and (2). The Python entry point change is independent and can ship separately.

## Constraints — no functional compromises

The harvest must continue to produce byte-identical output to today on existing test fixtures (`rinf_import_test`, `turtle_import_roundtrip`, `rinf_herefoss`).

In particular:

- A subject inlined via multiple inline-mode edges is **denormalized** in the output — one independent inlined subtree per occurrence. (Example: two `Train` roots sharing the same `Operator` → the `Operator` appears inlined under each `Train`, as two independent subtrees.)
- A subject that has been inlined anywhere is suppressed from the top-level result lists (the `claimed` semantics). It still appears inlined wherever the inline edges point at it.
- Each emitted `LinkMLInstance` carries its own unique `node_id` (no shared identity between denormalized copies).
- Cycle detection (`InlinedCycle`) and unknown-fields collection are preserved.

## Goals (in scope for this MR)

- Yield top-level root instances **one at a time** to the caller, so a 1 GB-worth of LinkML instances never has to fit in RAM at once.
- Stop building instance trees only to throw them away (today's "harvest everything, filter by `claimed`" pattern).
- Avoid rebuilding the same shared inlined subtree across multiple consumers when it can be cached safely; bound the cache so it never holds a subtree past its last consumer.
- Add an on-disk-backed `TripleSource` implementation (oxigraph) so the graph itself does not have to fit in RAM.

## Non-goals (deferred)

- Changing the Python entry point to accept a file path or binary reader (separate small change, doesn't need this design).
- Spilling Pass 1's edge map to disk (only relevant for very large inline closures; defer until profiling demands it).
- Per-subject manual cache policy tuning (the default policy is enough for v1).
- Cross-MR concerns: Wasm, schema view APIs.

## Algorithm — two-pass streaming harvest

The existing `harvest_subject` (`turtle_import.rs:298-584`) is preserved as-is for the actual instance construction. We change only the driver in `import_from_store` and add a small in-process cache.

### Pass 1 — structure analysis (no instance trees built)

`SubjectKey` is `String`, matching today's `subject.to_string()` keying in `claimed`.

Walks the **inline subgraph** of the data, starting from each root candidate. Produces three values:

- `claimed: HashSet<SubjectKey>` — subjects that appear as the target of any inline edge from any reachable root. These are not yielded standalone at the top level.
- `inline_edges: HashMap<SubjectKey, Vec<SubjectKey>>` — the inline subgraph, used to propagate materialization counts. (Predicates not needed for the count math — multiplicity matters, identity doesn't.)
- `materializations: HashMap<SubjectKey, usize>` — for each subject reached by any root, the exact number of times its instance tree will be produced in Pass 2.

What counts as an "inline edge" — must match `harvest_subject`'s actual behavior, not just inline-mode slots:

- A **named-node** object on a slot whose `inline_mode == Inline` → inline edge.
- A **blank-node** object on **any** slot of the schema → inline edge. (Today's `harvest_subject` auto-inlines blank-node objects unconditionally; see the `TermRef::BlankNode` branch in `turtle_import.rs:496-503`.)
- A named-node object on a `Reference` or IRI-typed slot → **not** an inline edge.

Walk algorithm:

```
visited:      HashSet<SubjectKey>
claimed:      HashSet<SubjectKey>
inline_edges: HashMap<SubjectKey, Vec<SubjectKey>>

fn walk(s):
  if s in visited: return
  visited.insert(s)
  let class = resolve_class(s)                 // reads s's rdf:type
  let slot_modes = { slot.canonical_uri() : slot.inline_mode for slot in class.slots() }

  // ONE store query per subject (cheap on disk, no worse than today in RAM)
  for triple in store.triples_for_subject(s):
    let p = triple.predicate
    let o = triple.object
    if p == rdf:type: continue
    let inline = match (o, slot_modes.get(p)):
      (BlankNode, _)                 => true   // blank nodes always inlined
      (NamedNode, Some(Inline))      => true
      (NamedNode, Some(Reference))   => false
      (NamedNode, Some(_) | None)    => false  // unknown predicates / IRI-typed
      (Literal, _)                   => false
    if inline:
      let n = subject_key_of(o)
      claimed.insert(n)
      inline_edges[s].push(n)
      walk(n)

for r in store.subjects_for_predicate_object(rdf:type, root_class_uri) for each root class:
  walk(r)
```

Cycle detection: same as today (`InlinedCycle` error via a visit stack), but lifted to the structural walk so it fires before any instance is built.

Pass 1's store access pattern: one `triples_for_subject(s)` per visited subject, plus one `subjects_for_predicate_object(rdf:type, root_class_uri)` per root class. It still reads every triple of every visited subject (so it isn't free), but it never builds instance trees, never converts literals, never resolves enums, and never touches subjects that aren't reachable as roots or via inline edges. On a disk-backed store, this is one sequential index seek per visited subject — much less I/O than Pass 2.

Materialization counts: computed by topological propagation over `inline_edges` after the walk completes.

```
materializations: HashMap<SubjectKey, usize>

// Initialize: unclaimed roots start at 1, everyone else (including claimed roots) at 0.
for r in all_root_candidates:
  materializations[r] = if claimed.contains(r) { 0 } else { 1 }
for s in visited but not yet in materializations:
  materializations[s] = 0

// Propagate.
for s in topological order of inline_edges:
  for child in inline_edges[s]:
    materializations[child] += materializations[s]
```

A claimed root candidate (e.g. `t2` in the worked example) starts at `0` and accumulates count from incoming inline edges. That's why it correctly ends at `1` rather than being missed.

### Pass 2 — yield-and-drop with refcount-driven cache

Iterates root candidates a second time. For each unclaimed root, calls `harvest_subject` and yields the resulting tree. The harvest internally still descends recursively through inline edges, but each call to `harvest_subject(S)` for a shared subject `S` is routed through the cache:

```
cache:     HashMap<SubjectKey, LinkMLInstance>
remaining: HashMap<SubjectKey, usize> = materializations  // owned, decremented as we go

fn materialise(s):
  if materializations[s] < 2:
    return harvest_subject(s)            // single-use, never cached

  if s not in cache:
    cache.insert(s, harvest_subject(s))  // first time: build and store original
  remaining[s] -= 1
  if remaining[s] == 0:
    return cache.remove(s)               // last consumer: move out, no clone needed
  return cache[s].clone_with_fresh_node_ids()  // earlier consumer: clone for unique node_ids
```

Each materialise call produces a `LinkMLInstance` with unique `node_id`s across the import, because the original tree (built once per cached subject) is handed to exactly the last consumer and every earlier consumer gets a fresh clone.

When `harvest_subject` itself encounters an inline-mode named-node object, the recursive call goes through `materialise` instead of straight into `harvest_subject`. That is the **only** point of contact between the cache and the existing harvest code.

Top-level driver:

```
for r in store.subjects_for_predicate_object(rdf:type, class_uri) for each root class:
  if r in claimed: continue
  let tree = materialise(r)
  yield (class_name, tree)
```

After yielding each root, the caller holds the tree; we drop our reference. The cache holds only subtrees with `remaining[s] > 0`, so peak cache size is bounded by `Σ size(s) for s with remaining[s] > 0 at this moment`, which strictly decreases as Pass 2 progresses and never exceeds `Σ size(s) for s where materializations[s] ≥ 2`.

### `clone_with_fresh_node_ids`

A new helper on `LinkMLInstance`. Recursively clones the tree (values + structure) but allocates fresh `node_id`s via `new_node_id()` at every node. Reuses the same `class` and `sv` references (those are `Arc`-like already). Preserves the property that every emitted instance has a unique identity for blame/diff.

### Public API shape

The existing entry points (`import_turtle`, `import_ntriples`, `import_rdf`, `import_turtle_from_string`, `RdfImportStore::import`, `TrackingRdfImportStore::import`) keep their current return type (`ImportResult`) for backwards compatibility — they internally collect the streaming harvest into the same `HashMap` and return as today.

Add a new streaming entry point:

```rust
/// Streaming harvest. Yields (class_name, LinkMLInstance) one at a time.
/// Caller decides what to do with each instance (process and drop, or collect).
pub fn import_from_store_streaming<'a, T: TripleSource>(
    store: &'a T,
    sv: &'a SchemaView,
    conv: &'a Converter,
    root_classes: &[&str],
) -> Result<impl Iterator<Item = Result<(String, LinkMLInstance), ImportError>> + 'a, ImportError>;
```

Pass 1 runs eagerly when the iterator is constructed (so any structural errors surface immediately and the caller has the full `claimed` set before consuming anything). Pass 2 is lazy in the returned iterator.

The Python binding gets a corresponding `from_turtle_streaming` function returning a Python iterator. Each `next()` produces a single `(class_name, PyLinkMLInstance)` pair.

## Subordinate refactor — unknown-fields in the slot loop

`harvest_subject` currently does:

1. Loop over slots, calling `objects_for_subject_predicate(s, pred)` for each, marking `consumed_predicates`.
2. At the end, call `triples_for_subject(s)` and collect any triple whose predicate is not in `consumed_predicates` into `unknown_fields`.

On an in-memory `oxrdf::Graph` this re-scan is free. On a disk-backed store it doubles I/O per subject.

Refactor: collect unknown fields in the slot loop instead. Concretely:

- Inside the slot loop, keep a `HashSet<&str>` of predicate IRIs we've handled.
- After the slot loop, make a **single** `triples_for_subject(s)` pass; for each triple whose predicate is not in the handled set, add to `unknown_fields`.

That removes the duplicated `triples_for_subject` work. Same call count as today (one per subject) but only one scan instead of slot-loop + scan.

In scope for this session (the cleanup makes the disk-backed step in the second session usable).

## Disk-backed `TripleSource` — in scope for the MR, not for this session

A second `impl TripleSource` backed by an oxigraph on-disk store will be added in a follow-up session. No design changes required:

- Implements the same trait surface.
- Same algorithm (Pass 1 + Pass 2 + cache) runs unchanged.
- The `cache iff materializations ≥ 2` policy gives a bigger win on disk than in RAM because each rebuild costs disk I/O.
- The `triples_for_subject` refactor above is a prerequisite for tolerable disk performance.

Implications for *this session's* algorithm: none. The trait abstraction was designed for exactly this case.

## Implementation order (this session)

1. Implement `clone_with_fresh_node_ids` on `LinkMLInstance`. Unit test on a few-level tree.
2. Refactor unknown-fields collection in `harvest_subject` into the slot loop. Verify all existing tests pass byte-for-byte.
3. Implement Pass 1 (structural walk + materialization counts) as a new function `compute_inline_structure(store, sv, conv, root_classes) -> Result<InlineStructure>`. Unit-test it on hand-built tiny graphs.
4. Implement Pass 2 driver with the refcount-driven cache, exposed as `import_from_store_streaming`. Unit-test.
5. Rewrite the existing `import_from_store` to internally drive `import_from_store_streaming` and collect into the `HashMap`. Verify every existing test still passes.
6. Add Python `from_turtle_streaming` binding. Smoke-test from Python.

## Testing strategy

### Regression — existing tests must pass byte-for-byte

`rinf_import_test`, `turtle_import_roundtrip`, `rinf_herefoss`, and the `from_turtle` tests in Python must produce identical output to today. The collect-into-`HashMap` entry points return the same shape as today, so these tests are unchanged.

### New unit tests

- Pass 1 on hand-built graphs: assert `claimed`, `inline_edges`, `materializations` directly.
  - Two trains sharing an operator (`materializations[op1] = 2`).
  - Diamond (A→B→D, A→C→D): `materializations[D] = 2`.
  - Mixed Inline + Reference slots: only Inline named-node objects are claimed; blank-node objects on any slot are claimed.
  - Cycle through inline edges → `InlinedCycle` from Pass 1.
- `clone_with_fresh_node_ids`: structural equality, all `node_id`s differ from the original tree.
- Streaming entry point on small fixtures: shared subtree yields the expected number of inlined copies and zero standalone copies.
- Cache instrumentation: a test `TripleSource` wraps `RdfImportStore` and counts `objects_for_subject_predicate` / `triples_for_subject` calls. Assert that a deeply-shared subject is fetched from the store the expected number of times (1 if `materializations ≥ 2`, otherwise N).

### Real-data integration — the RINF German railroad network

The German RINF network is a ~890 MB N-Triples file at `/home/kervel/projects/asset360/lml/de_1080.nt` (public data), against the RINF LinkML schema at `/home/kervel/projects/asset360/consolidator-server/components/py/asset360-model/asset360_model/schemas/rinf/repository/v1.0.0` (`rinf_subset.yaml` + supporting files). This is the headline test case for the 1 GB scale we're designing for. The dev machine has enough RAM to run the import to completion with the *current* code, which lets us produce a baseline result file we can diff against.

**Procedure — measure-baseline-then-fix:**

1. **Baseline run** with the current `import_turtle` + serialize-to-JSON (existing path). Capture:
   - wall-clock time
   - peak RSS (and the time series, for shape)
   - output JSON file → `baseline.json`
2. **Streaming run with in-memory `RdfImportStore`** — same input, new algorithm, iterator collected to JSON. Capture the same three metrics → `streaming-ram.json`. Diff against `baseline.json`: must be identical (allowing for stable key ordering).
3. **Streaming run with disk-backed `TripleSource`** (added in the second session of this MR) — same input, oxigraph store, new algorithm, iterator collected to JSON. Same three metrics → `streaming-disk.json`. Diff against `baseline.json`: must be identical.

Each run is launched under [`procmon`](https://gitlab.pp.kapernikov.com/frank/procmon) to record RSS-over-time and total wall-clock. Procmon outputs go in a dated subdirectory under the test artifacts dir so we can keep historical comparisons.

Pass criteria:
- Output equality: `streaming-ram.json` and `streaming-disk.json` are byte-identical to `baseline.json` (after canonical JSON normalisation if needed for key order).
- RAM goal: `streaming-ram.json` run has materially lower peak RSS than baseline (target: at least the size of the final HashMap of instance trees worth of savings — `streaming` should be O(graph) instead of O(graph + all-trees)). `streaming-disk.json` run has peak RSS far below the input file size (target: under 1 GB regardless of dataset).
- Time: `streaming-ram` should be within ±20% of baseline (algorithm change shouldn't materially regress in-RAM speed). `streaming-disk` will be slower; record the actual factor for the PR description.

These integration tests are gated behind a `--features rinf-large-dataset` flag (or env var) so CI without the data file skips them. The procmon-collected metrics and the diff outputs are attached to the PR.

## Open questions

None blocking. The two questions that came up during brainstorming:

- *Per-subject cache policy tuning* — deferred. Default `≥ 2` is enough for v1.
- *Edge-map spillover to disk* — deferred. Will revisit if profiling shows the inline closure is too large to keep in RAM.
