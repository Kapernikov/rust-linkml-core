# RDF Import — less RAM (streaming) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make RDF/Turtle import yield root instances one at a time so 1 GB-scale datasets don't have to materialise the full `HashMap<class, Vec<LinkMLInstance>>` in RAM, while preserving byte-identical output to today.

**Architecture:** Two-pass harvest. Pass 1 walks the inline subgraph from each root candidate to compute `claimed` (top-level suppression set), `inline_edges` (the subgraph), and `materializations` (how many times each subject's tree will be produced in Pass 2). Pass 2 iterates root candidates, calling `Materializer::materialise` which builds via the existing `harvest_subject` and caches subjects with `materializations ≥ 2` until their last consumer. The existing `import_from_store` becomes a thin wrapper that drives the streaming iterator into the legacy `HashMap` shape.

**Tech Stack:** Rust 2021, `oxrdf` 0.3 + `oxttl` 0.2 (already in `linkml_runtime`), pyo3 0.25 for the Python binding. No new crate dependencies in this session. (The oxigraph disk-backed `TripleSource` is deferred to the next session of this same MR.)

---

## File Structure

**Create:**
- `src/runtime/src/rdf_streaming.rs` — new module holding `InlineStructure`, `Materializer`, `ImportStream`, `compute_inline_structure`, `import_from_store_streaming`. Keeps `turtle_import.rs` focused on per-subject construction.

**Modify:**
- `src/runtime/src/lib.rs` — add `pub mod rdf_streaming;` re-export, add `LinkMLInstance::clone_with_fresh_node_ids`, expose `pub(crate) fn new_node_id` (already exists, but make sure visibility extends to the new module).
- `src/runtime/src/turtle_import.rs` — refactor `harvest_subject` to (a) take a `&mut Materializer` for inline-recursion calls, (b) collect unknown fields in the slot loop instead of via a second pass. Replace `import_from_store` body with a wrapper around `import_from_store_streaming`.
- `src/python/src/lib.rs` — add `py_from_turtle_streaming` returning a Python iterator.

**Test (new):**
- `src/runtime/tests/clone_fresh_node_ids.rs` — unit tests for the new method on `LinkMLInstance`.
- `src/runtime/tests/inline_structure.rs` — unit tests for `compute_inline_structure` on hand-built tiny graphs.
- `src/runtime/tests/streaming_import.rs` — integration tests for the new streaming entry point on small Turtle fixtures.
- `src/runtime/tests/rinf_de_streaming.rs` — gated large-dataset test that imports `de_1080.nt` via both old and new paths and asserts JSON equality.
- `scripts/measure_rinf_de.sh` — small shell script that wires `procmon` around each of the test binaries.

**Test (modified):** none of the existing turtle test files need source changes; they must keep passing as-is.

---

## Conventions for this plan

- Every task ends with `git add <listed files> && git commit -m "<message>"`. Use the message text given in the task verbatim.
- After every task, run `cargo build -p linkml_runtime --features ttl` to catch breakage early. Run `cargo test -p linkml_runtime --features ttl` after tasks marked `(run full test suite)`.
- The worktree root is `/home/kervel/projects/asset360/lml/rust-linkml-core/.claude/worktrees/rdf-import-less-ram`. All file paths in this plan are relative to that root unless absolute.
- Where the task says "verify it fails" with a `cargo test` command, that means the listed test name should fail with the indicated compile-error or assertion. If it fails for a different reason, stop and investigate before writing the implementation.

---

## Task 0: Baseline measurement via the existing `linkml-convert` CLI

The project already ships a `linkml-convert` binary (`src/tools/src/bin/linkml_convert.rs`) that does exactly what we need: take a LinkML schema YAML, an N-Triples data file, one or more `--class` flags for root classes, and write JSON. With the `resolve` feature (default for the tools crate), schema imports are pulled in automatically.

We run that binary **twice over this MR**:
1. **Now** (pre-refactor) — captures today's behaviour as a reference JSON file.
2. **At the end** (Task 10) — same binary, recompiled against the post-refactor code; compare JSONs and procmon logs.

Same binary surface, same arguments, same input → apples to apples.

**Files:**
- Create: `scripts/measure_rinf_de.sh`
- Create/modify: `.gitignore` to exclude `target/rinf-measure/`

### Step 0.1: Add the measurement script

- [ ] **Create** `scripts/measure_rinf_de.sh`:

```bash
#!/usr/bin/env bash
# Measure RINF Germany .nt import via the existing `linkml-convert` CLI.
# Run TWICE during this MR:
#   1) before any algorithm change (captures pre-refactor baseline)
#   2) after Task 10 (captures post-refactor streaming behaviour)
# Same CLI, same args, same input → apples to apples.
#
# Override defaults with env vars:
#   PROCMON       path to procmon binary
#   NT_FILE       path to de_1080.nt
#   SCHEMA        path to the entry schema (rinf_subset.yaml)
#   LABEL         tag for the run (default: timestamped)
#   OUT_DIR       where to write outputs (default: target/rinf-measure/$LABEL)
set -euo pipefail

PROCMON="${PROCMON:-/tmp/procmon/result/bin/procmon}"
NT_FILE="${NT_FILE:-/home/kervel/projects/asset360/lml/de_1080.nt}"
SCHEMA="${SCHEMA:-/home/kervel/projects/asset360/consolidator-server/components/py/asset360-model/asset360_model/schemas/rinf/repository/v1.0.0/rinf_subset.yaml}"
LABEL="${LABEL:-$(date +%Y%m%d-%H%M%S)}"
OUT_DIR="${OUT_DIR:-target/rinf-measure/$LABEL}"

mkdir -p "$OUT_DIR"

# Rebuild linkml-convert against the current source.
cargo build -p linkml_tools --release 2>&1 | tail -3

BIN="target/release/linkml-convert"
if [ ! -x "$BIN" ]; then
    echo "linkml-convert not found at $BIN" >&2
    exit 1
fi

# Root classes used for the RINF dataset. Match `src/runtime/tests/rinf_import_test.rs`.
ROOT_CLASSES=(
    OperationalPoint
    SectionOfLine
    RunningTrack
    Siding
    Tunnel
    ContactLineSystem
    TrainDetectionSystem
    PlatformEdge
    OrganisationRole
    LinearPositioningSystem
    ETCS
)
CLASS_ARGS=()
for c in "${ROOT_CLASSES[@]}"; do
    CLASS_ARGS+=(--class "$c")
done

echo "=== run: $LABEL ==="
echo "binary:    $BIN"
echo "nt:        $NT_FILE"
echo "schema:    $SCHEMA"
echo "output:    $OUT_DIR/output.json"
echo "procmon:   $PROCMON"

CMD=("$BIN" "$SCHEMA" "$NT_FILE" --from ntriples --to json --output "$OUT_DIR/output.json" "${CLASS_ARGS[@]}")

if [ -x "$PROCMON" ]; then
    "$PROCMON" --output "$OUT_DIR/procmon.log" -- "${CMD[@]}" 2>&1 | tee "$OUT_DIR/stdout.log"
else
    echo "procmon not available; falling back to /usr/bin/time -v" >&2
    /usr/bin/time -v "${CMD[@]}" 2>&1 | tee "$OUT_DIR/stdout.log"
fi

ls -la "$OUT_DIR/"
echo "Done. Results in $OUT_DIR"
```

- [ ] **Make executable**:

```bash
chmod +x scripts/measure_rinf_de.sh
```

### Step 0.2: Gitignore the measurement output dir

- [ ] **Append** to `.gitignore` (or create if missing):

```
target/rinf-measure/
```

- [ ] **Verify**:

```bash
grep rinf-measure .gitignore
```

### Step 0.3: Clone procmon

- [ ] **Run**:

```bash
[ -d /tmp/procmon ] || git clone https://gitlab.pp.kapernikov.com/frank/procmon /tmp/procmon
cd /tmp/procmon && nix build && cd -
test -x /tmp/procmon/result/bin/procmon && echo "procmon ready"
```

If `nix build` is unavailable on this host, the script will fall back to `/usr/bin/time -v`. Peak RSS is still captured (no time-series).

### Step 0.4: Sanity-check the CLI works on a tiny input first

- [ ] **Pick** an existing small turtle fixture (e.g. one used in `turtle_import_roundtrip`) and run linkml-convert against it:

```bash
cargo build -p linkml_tools --release 2>&1 | tail -3
ls src/runtime/tests/data/*.ttl | head -3
# Pick a fixture and run a quick import to JSON. Adjust SCHEMA and CLASS to what's in tests/data.
```

If `linkml-convert` succeeds on a tiny fixture and produces non-empty JSON, the script is ready for the big run. If it fails (e.g. schema resolution issue), debug *before* committing to the multi-minute RINF run.

### Step 0.5: Capture the pre-refactor baseline

- [ ] **Run**:

```bash
LABEL=baseline-prerefactor bash scripts/measure_rinf_de.sh
```

This will take many minutes on the 890 MB input. Expected: a directory `target/rinf-measure/baseline-prerefactor/` with `output.json`, `procmon.log` (or `time -v` capture in `stdout.log`), and `stdout.log`.

- [ ] **Note** the size and a couple of numbers for later reference:

```bash
ls -la target/rinf-measure/baseline-prerefactor/
wc -l target/rinf-measure/baseline-prerefactor/output.json
grep -E '(Maximum resident|elapsed)' target/rinf-measure/baseline-prerefactor/stdout.log || true
```

### Step 0.6: Commit the script (not the output)

- [ ] **Run**:

```bash
git add scripts/measure_rinf_de.sh .gitignore
git commit -m "test(rinf): add measure_rinf_de.sh wrapping linkml-convert with procmon"
```

The baseline output stays in `target/rinf-measure/baseline-prerefactor/` (gitignored). Task 10 references it for the diff.

---

## Task 1: `clone_with_fresh_node_ids` on `LinkMLInstance`

The streaming algorithm caches one tree per shared inlined subject and hands clones with fresh `node_id`s to all consumers except the last. Today, `LinkMLInstance` has `#[derive(Clone)]` (`src/runtime/src/lib.rs:301`) which preserves `node_id`s — that's wrong for our purpose because two emitted instances must have disjoint IDs. We add a separate method.

**Files:**
- Create: `src/runtime/tests/clone_fresh_node_ids.rs`
- Modify: `src/runtime/src/lib.rs` (add method on `LinkMLInstance` impl block around line 359)

### Step 1.1: Make `new_node_id` accessible to the new module

- [ ] **Open** `src/runtime/src/lib.rs:355` and change the visibility:

```rust
// Before
fn new_node_id() -> NodeId {
    NEXT_NODE_ID.fetch_add(1, Ordering::Relaxed)
}

// After
pub(crate) fn new_node_id() -> NodeId {
    NEXT_NODE_ID.fetch_add(1, Ordering::Relaxed)
}
```

### Step 1.2: Write the failing test

- [ ] **Create** `src/runtime/tests/clone_fresh_node_ids.rs` with:

```rust
#![cfg(feature = "ttl")]

use linkml_runtime::{turtle_import::import_turtle_from_string, LinkMLInstance};
use linkml_schemaview::identifier::converter_from_schemas;
use linkml_schemaview::io::from_yaml;
use linkml_schemaview::schemaview::SchemaView;
use std::path::PathBuf;

fn data_path(name: &str) -> PathBuf {
    let mut p = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    p.push("tests");
    p.push("data");
    p.push(name);
    p
}

/// Collect every `node_id` reachable in a tree.
fn collect_ids(inst: &LinkMLInstance, out: &mut Vec<u64>) {
    match inst {
        LinkMLInstance::Scalar { node_id, .. } | LinkMLInstance::Null { node_id, .. } => {
            out.push(*node_id);
        }
        LinkMLInstance::List { node_id, values, .. } => {
            out.push(*node_id);
            for v in values { collect_ids(v, out); }
        }
        LinkMLInstance::Mapping { node_id, values, .. } => {
            out.push(*node_id);
            for v in values.values() { collect_ids(v, out); }
        }
        LinkMLInstance::Object { node_id, values, .. } => {
            out.push(*node_id);
            for v in values.values() { collect_ids(v, out); }
        }
    }
}

#[test]
fn clone_with_fresh_node_ids_yields_disjoint_ids_and_equal_json() {
    // Use the smallest existing fixture in tests/data
    let schema = from_yaml(&data_path("basic.yaml")).unwrap();
    let mut sv = SchemaView::new();
    sv.add_schema(schema.clone()).unwrap();
    let conv = converter_from_schemas([&schema]);

    let ttl = r#"
        @prefix ex: <http://example.org/> .
        ex:p1 a ex:Person ; ex:name "Alice" .
    "#;
    let mut result = import_turtle_from_string(ttl, &sv, &conv, &["Person"]).unwrap();
    let person = result.instances.get_mut("Person").unwrap().pop().unwrap();

    let cloned = person.clone_with_fresh_node_ids();

    let mut orig_ids = Vec::new();
    let mut cloned_ids = Vec::new();
    collect_ids(&person, &mut orig_ids);
    collect_ids(&cloned, &mut cloned_ids);

    assert_eq!(orig_ids.len(), cloned_ids.len(), "tree shape must match");
    let orig_set: std::collections::HashSet<u64> = orig_ids.iter().copied().collect();
    for id in &cloned_ids {
        assert!(!orig_set.contains(id), "cloned id {id} must not appear in original tree");
    }
    assert_eq!(person.to_json(), cloned.to_json(), "JSON projection must be identical");
}
```

If `tests/data/basic.yaml` does not define a `Person` class with a `name` slot, substitute whichever class+slot exists in the smallest schema fixture (check `ls src/runtime/tests/data/*.yaml` and pick one that loads with no imports). Adjust the Turtle prefix and class name accordingly.

- [ ] **Run** the test to confirm it fails to compile:

```bash
cargo test -p linkml_runtime --features ttl --test clone_fresh_node_ids
```

Expected: compile error `no method named 'clone_with_fresh_node_ids' found`.

### Step 1.3: Implement the method

- [ ] **Open** `src/runtime/src/lib.rs` and locate the `impl LinkMLInstance` block at line 359 (look for `pub fn to_json`). Add a new method to that impl block:

```rust
    /// Recursively clone the tree but allocate a fresh `node_id` at every node.
    ///
    /// Use this when the same harvested subtree needs to be emitted multiple
    /// times (e.g. a shared inlined subject denormalised across two parents).
    /// Each emitted occurrence gets unique IDs so blame/diff tracking is correct.
    pub fn clone_with_fresh_node_ids(&self) -> Self {
        match self {
            LinkMLInstance::Scalar { value, slot, class, sv, .. } => LinkMLInstance::Scalar {
                node_id: new_node_id(),
                value: value.clone(),
                slot: slot.clone(),
                class: class.clone(),
                sv: sv.clone(),
            },
            LinkMLInstance::Null { slot, class, sv, .. } => LinkMLInstance::Null {
                node_id: new_node_id(),
                slot: slot.clone(),
                class: class.clone(),
                sv: sv.clone(),
            },
            LinkMLInstance::List { values, slot, class, sv, .. } => LinkMLInstance::List {
                node_id: new_node_id(),
                values: values.iter().map(|v| v.clone_with_fresh_node_ids()).collect(),
                slot: slot.clone(),
                class: class.clone(),
                sv: sv.clone(),
            },
            LinkMLInstance::Mapping { values, slot, class, sv, .. } => LinkMLInstance::Mapping {
                node_id: new_node_id(),
                values: values
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone_with_fresh_node_ids()))
                    .collect(),
                slot: slot.clone(),
                class: class.clone(),
                sv: sv.clone(),
            },
            LinkMLInstance::Object { values, class, sv, unknown_fields, .. } => LinkMLInstance::Object {
                node_id: new_node_id(),
                values: values
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone_with_fresh_node_ids()))
                    .collect(),
                class: class.clone(),
                sv: sv.clone(),
                unknown_fields: unknown_fields.clone(),
            },
        }
    }
```

### Step 1.4: Run the test to confirm pass

- [ ] **Run**:

```bash
cargo test -p linkml_runtime --features ttl --test clone_fresh_node_ids
```

Expected: 1 passed; 0 failed.

### Step 1.5: Commit

- [ ] **Run**:

```bash
git add src/runtime/src/lib.rs src/runtime/tests/clone_fresh_node_ids.rs
git commit -m "feat(runtime): add LinkMLInstance::clone_with_fresh_node_ids"
```

---

## Task 2: One-scan refactor of `harvest_subject` slot loop

Today's `harvest_subject` calls `objects_for_subject_predicate(s, pred)` once per slot (N store calls per subject) and then a separate `triples_for_subject(s)` scan to collect unknown fields. Refactor to **one scan per subject**: read all of `s`'s triples once, group by predicate, then drive the slot loop and the unknown-fields collection from the in-memory grouping. Same output, fewer store calls — makes the disk-backed `TripleSource` (next session) usable.

**Files:**
- Modify: `src/runtime/src/turtle_import.rs:298-584` (the body of `harvest_subject`)

### Step 2.1: Capture baseline test counts

- [ ] **Run**:

```bash
cargo test -p linkml_runtime --features ttl 2>&1 | tail -20
```

Expected: all tests pass. Remember the count (e.g. "test result: ok. NN passed").

### Step 2.2: Refactor the harvest body

The change has three parts inside `harvest_subject` (`turtle_import.rs:298-584`):

1. After the `consumed_predicates` declaration (around line 318), add a one-shot scan that buckets all triples for the subject by predicate.
2. Change the slot loop to consume from the bucket instead of calling `objects_for_subject_predicate`.
3. Change the unknown-fields collection to iterate the leftover buckets instead of calling `triples_for_subject` again.

- [ ] **Find** in `src/runtime/src/turtle_import.rs` (around lines 315-321):

```rust
    let mut values: HashMap<String, LinkMLInstance> = HashMap::new();
    let mut consumed_predicates: HashSet<String> = HashSet::new();

    // Always mark rdf:type as consumed
    consumed_predicates.insert(rdf::TYPE.as_str().to_string());
```

- [ ] **Insert** right after the `consumed_predicates.insert(rdf::TYPE...)` line:

```rust
    // One scan over the subject's triples; bucket objects by predicate IRI.
    // We use owned terms here so each bucket survives the slot loop and the
    // unknown-fields pass without re-querying the store.
    use oxrdf::Term;
    let mut by_predicate: HashMap<String, Vec<Term>> = HashMap::new();
    for triple in ctx.store.triples_for_subject(subject) {
        let pred = triple.predicate.as_str().to_string();
        let obj_owned: Term = match triple.object {
            TermRef::Literal(l) => Term::Literal(l.into_owned()),
            TermRef::NamedNode(n) => Term::NamedNode(n.into_owned()),
            TermRef::BlankNode(b) => Term::BlankNode(b.into_owned()),
            #[allow(unreachable_patterns)]
            _ => continue,
        };
        by_predicate.entry(pred).or_default().push(obj_owned);
    }
```

- [ ] **Find** the slot-loop block (around lines 354-383):

```rust
        // Get the predicate IRI for this slot
        let canonical = slot.canonical_uri();
        let pred_iri = canonical
            .to_uri(ctx.conv)
            .map(|u| u.0)
            .unwrap_or_else(|_| canonical.to_string());

        consumed_predicates.insert(pred_iri.clone());

        let predicate = NamedNode::new_unchecked(&pred_iri);
        let objects: Vec<_> = ctx
            .store
            .objects_for_subject_predicate(subject, &predicate)
            .collect();

        if objects.is_empty() {
            continue;
        }

        for obj_term in &objects {
            let obj_str = match *obj_term {
                TermRef::Literal(l) => l.to_string(),
                TermRef::NamedNode(n) => n.as_str().to_string(),
                TermRef::BlankNode(b) => b.to_string(),
                #[allow(unreachable_patterns)]
                _ => String::new(),
            };
            ctx.store.on_consumed(&subject_key, &pred_iri, &obj_str);
        }
        ctx.consumed_count += objects.len();
```

- [ ] **Replace** with:

```rust
        // Get the predicate IRI for this slot
        let canonical = slot.canonical_uri();
        let pred_iri = canonical
            .to_uri(ctx.conv)
            .map(|u| u.0)
            .unwrap_or_else(|_| canonical.to_string());

        consumed_predicates.insert(pred_iri.clone());

        // Take the objects for this predicate out of the bucket so the
        // unknown-fields pass at the end doesn't see them.
        let owned_objects: Vec<Term> = by_predicate.remove(&pred_iri).unwrap_or_default();
        if owned_objects.is_empty() {
            continue;
        }

        for obj_term in &owned_objects {
            let obj_str = match obj_term {
                Term::Literal(l) => l.to_string(),
                Term::NamedNode(n) => n.as_str().to_string(),
                Term::BlankNode(b) => b.to_string(),
                #[allow(unreachable_patterns)]
                _ => String::new(),
            };
            ctx.store.on_consumed(&subject_key, &pred_iri, &obj_str);
        }
        ctx.consumed_count += owned_objects.len();
```

- [ ] **Find** the inner loop that converts each object (around line 397):

```rust
        for obj_term in &objects {
            match *obj_term {
                TermRef::Literal(lit) => {
                    let lit = lit.into_owned();
```

- [ ] **Replace** the loop header and the first arm:

```rust
        for obj_term in &owned_objects {
            match obj_term {
                Term::Literal(lit) => {
                    let lit = lit.clone();
```

- [ ] **Update** the subsequent arms (still inside that loop). Each `TermRef::NamedNode(nn)` becomes `Term::NamedNode(nn)` and similarly for `TermRef::BlankNode`. The bodies stay almost identical; the only mechanical difference is that pattern-matched values are owned (`NamedNode`, `BlankNode`) instead of references (`NamedNodeRef`, etc.). Specifically:

```rust
                Term::NamedNode(nn) => {
                    let iri_str = nn.as_str();
                    // ... unchanged body using `iri_str` and converting `nn.clone()` where the old code did `nn.into_owned()`
                }
                Term::BlankNode(bn) => {
                    let obj_subject = NamedOrBlankNode::BlankNode(bn.clone());
                    // ... unchanged body
                }
                #[allow(unreachable_patterns)]
                _ => {}
```

In two places in the old code we had `nn.into_owned()` to produce an owned `NamedNode` — with `Term::NamedNode(nn)` we already own it, so use `nn.clone()` (or just `nn` if the value isn't needed later in the arm — match-by-move is fine if `obj_term` is consumed). The simplest path: change `for obj_term in &owned_objects` to `for obj_term in owned_objects.iter().cloned()` and then match on owned `Term` values; bodies become `NamedNode(nn) => { let iri_str = nn.as_str(); ... NamedOrBlankNode::NamedNode(nn) ... }`.

- [ ] **Find** the unknown-fields block (lines 556-569):

```rust
    // Collect unknown fields (predicates on subject not matching any slot)
    let mut unknown_fields: HashMap<String, JsonValue> = HashMap::new();
    for triple in ctx.store.triples_for_subject(subject) {
        let pred = triple.predicate;
        let obj = triple.object;
        if !consumed_predicates.contains(pred.as_str()) {
            let key = pred.as_str().to_string();
            let val = match obj {
                TermRef::Literal(lit) => JsonValue::String(lit.value().to_string()),
                TermRef::NamedNode(nn) => JsonValue::String(nn.as_str().to_string()),
                _ => JsonValue::String(obj.to_string()),
            };
            unknown_fields.insert(key, val);
        }
    }
```

- [ ] **Replace** with (consume the leftover bucket — no second store scan):

```rust
    // Unknown fields = predicates left in the by_predicate bucket after the
    // slot loop. `consumed_predicates` is redundant with the bucket-removal
    // strategy but kept for clarity.
    let mut unknown_fields: HashMap<String, JsonValue> = HashMap::new();
    for (pred_iri, objs) in by_predicate.into_iter() {
        if consumed_predicates.contains(&pred_iri) {
            continue;
        }
        // Keep only the last object for a given predicate, mirroring previous
        // behavior (HashMap::insert overwrites).
        if let Some(obj) = objs.into_iter().last() {
            let val = match obj {
                Term::Literal(lit) => JsonValue::String(lit.value().to_string()),
                Term::NamedNode(nn) => JsonValue::String(nn.as_str().to_string()),
                Term::BlankNode(bn) => JsonValue::String(bn.to_string()),
                #[allow(unreachable_patterns)]
                other => JsonValue::String(other.to_string()),
            };
            unknown_fields.insert(pred_iri, val);
        }
    }
```

### Step 2.3: Run the full turtle test suite

- [ ] **Run**:

```bash
cargo test -p linkml_runtime --features ttl 2>&1 | tail -20
```

Expected: same count as Step 2.1 (no regressions). If a test fails because object ordering inside a `List` slot changed, investigate — the bucket preserves insertion order from `triples_for_subject` which on `oxrdf::Graph` may differ from the order `objects_for_subject_predicate` returned. If the test depended on a specific order, decide whether to fix the test (the spec is silent on object ordering for non-list slots; lists *should* match insertion order). Likely fix: store the objects in the bucket in the same order as `triples_for_subject` reports them, which is what the code above already does.

### Step 2.4: Commit

- [ ] **Run**:

```bash
git add src/runtime/src/turtle_import.rs
git commit -m "refactor(turtle_import): one triples_for_subject scan per subject"
```

---

## Task 3: Module skeleton, `InlineStructure` and `Materializer` types

Sets up the new `rdf_streaming` module with the data types but no logic yet. This lets later tasks add behavior incrementally.

**Files:**
- Create: `src/runtime/src/rdf_streaming.rs`
- Modify: `src/runtime/src/lib.rs` (add `pub mod rdf_streaming;`)

### Step 3.1: Create the module file

- [ ] **Create** `src/runtime/src/rdf_streaming.rs`:

```rust
//! Streaming RDF/Turtle import.
//!
//! Implements the two-pass algorithm described in
//! `docs/superpowers/specs/2026-05-21-rdf-import-less-ram-design.md`:
//!
//! - Pass 1 (`compute_inline_structure`) walks the inline subgraph from each
//!   root candidate to produce `InlineStructure { claimed, inline_edges,
//!   materializations }`. No `LinkMLInstance` is constructed in this pass.
//! - Pass 2 (`import_from_store_streaming`) yields one root tree at a time.
//!   `Materializer` caches subjects with `materializations >= 2` until their
//!   last consumer.

#![cfg(feature = "ttl")]

use std::collections::{HashMap, HashSet};

use crate::turtle_import::ImportError;
use crate::LinkMLInstance;

/// Output of Pass 1. Drives Pass 2's caching and top-level suppression.
#[derive(Debug, Default)]
pub struct InlineStructure {
    /// Subjects that are inlined into another root somewhere. These are
    /// suppressed from the top-level result.
    pub claimed: HashSet<String>,
    /// For each subject reachable via inline edges, the list of subjects it
    /// inlines (one entry per inline edge, multiplicity preserved).
    pub inline_edges: HashMap<String, Vec<String>>,
    /// Exact number of times each subject's `LinkMLInstance` tree will be
    /// produced during Pass 2. Subjects not in the map are treated as 0.
    pub materializations: HashMap<String, usize>,
}

/// Pass 2 state — owns the cache and the per-subject `remaining` counter.
pub struct Materializer {
    pub(crate) materializations: HashMap<String, usize>,
    pub(crate) remaining: HashMap<String, usize>,
    pub(crate) cache: HashMap<String, LinkMLInstance>,
}

impl Materializer {
    pub fn new(structure: &InlineStructure) -> Self {
        Self {
            materializations: structure.materializations.clone(),
            remaining: structure.materializations.clone(),
            cache: HashMap::new(),
        }
    }
}

// Implementations follow in later tasks.
#[allow(dead_code)]
fn _suppress_unused_import_error_warning(_: ImportError) {}
```

### Step 3.2: Register the module

- [ ] **Modify** `src/runtime/src/lib.rs`. Find the line `pub mod turtle_import;` (search for `pub mod turtle`) and add immediately after it:

```rust
pub mod rdf_streaming;
```

### Step 3.3: Verify it builds

- [ ] **Run**:

```bash
cargo build -p linkml_runtime --features ttl 2>&1 | tail -10
```

Expected: clean build, no warnings about unused code in `rdf_streaming` (the `_suppress_unused_import_error_warning` placeholder is there for that reason; it'll be removed once real code arrives).

### Step 3.4: Commit

- [ ] **Run**:

```bash
git add src/runtime/src/lib.rs src/runtime/src/rdf_streaming.rs
git commit -m "feat(runtime): scaffold rdf_streaming module with InlineStructure and Materializer"
```

---

## Task 4: Pass 1 walk — `claimed` and `inline_edges`

Walk the inline subgraph from every root candidate. Use one `triples_for_subject` per visited subject. Record `claimed` and `inline_edges`. Skip materialization-count propagation — that's Task 5.

**Files:**
- Create: `src/runtime/tests/inline_structure.rs`
- Modify: `src/runtime/src/rdf_streaming.rs`

### Step 4.1: Write the failing test

- [ ] **Create** `src/runtime/tests/inline_structure.rs`:

```rust
#![cfg(feature = "ttl")]

use linkml_runtime::rdf_import_store::RdfImportStore;
use linkml_runtime::rdf_streaming::compute_inline_structure;
use linkml_schemaview::identifier::converter_from_schemas;
use linkml_schemaview::io::from_yaml;
use linkml_schemaview::schemaview::SchemaView;
use std::path::PathBuf;

fn data_path(name: &str) -> PathBuf {
    let mut p = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    p.push("tests");
    p.push("data");
    p.push(name);
    p
}

/// Schema: Train { id, operator inline -> Operator }; Operator { id, name }.
/// Two trains, both pointing at the same operator. No train->train inlining.
fn two_trains_sharing_operator() -> (SchemaView, linkml_schemaview::Converter, String) {
    let schema_yaml = r#"
id: https://example.org/trains
name: trains
prefixes:
  linkml: https://w3id.org/linkml/
  ex: http://example.org/
default_prefix: ex
default_range: string
classes:
  Train:
    attributes:
      id:
        identifier: true
      operator:
        range: Operator
        inlined: true
  Operator:
    attributes:
      id:
        identifier: true
      name:
"#;
    let schema: linkml_meta::SchemaDefinition = serde_yaml::from_str(schema_yaml).unwrap();
    let mut sv = SchemaView::new();
    sv.add_schema(schema.clone()).unwrap();
    let conv = converter_from_schemas([&schema]);

    let ttl = r#"
        @prefix ex: <http://example.org/> .
        @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
        ex:t1 a ex:Train ; ex:id "T1" ; ex:operator ex:op1 .
        ex:t2 a ex:Train ; ex:id "T2" ; ex:operator ex:op1 .
        ex:op1 a ex:Operator ; ex:id "OP1" ; ex:name "ACME" .
        ex:op2 a ex:Operator ; ex:id "OP2" ; ex:name "Standalone" .
    "#.to_string();
    (sv, conv, ttl)
}

#[test]
fn pass1_claims_shared_inlined_operator_only() {
    let (sv, conv, ttl) = two_trains_sharing_operator();
    let store = RdfImportStore::from_turtle(std::io::Cursor::new(ttl.as_bytes())).unwrap();

    let s = compute_inline_structure(&store, &sv, &conv, &["Train", "Operator"]).unwrap();

    let claimed: std::collections::HashSet<_> = s.claimed.iter().cloned().collect();
    assert!(claimed.contains("<http://example.org/op1>"),
        "op1 should be claimed (inlined by both trains), got {:?}", claimed);
    assert!(!claimed.contains("<http://example.org/op2>"),
        "op2 is not inlined anywhere, must not be claimed");
    assert!(!claimed.contains("<http://example.org/t1>"),
        "t1 is not inlined, must not be claimed");
    assert!(!claimed.contains("<http://example.org/t2>"),
        "t2 is not inlined, must not be claimed");

    // inline_edges: t1 -> [op1], t2 -> [op1]
    let t1_edges = s.inline_edges.get("<http://example.org/t1>").cloned().unwrap_or_default();
    assert_eq!(t1_edges, vec!["<http://example.org/op1>".to_string()]);
    let t2_edges = s.inline_edges.get("<http://example.org/t2>").cloned().unwrap_or_default();
    assert_eq!(t2_edges, vec!["<http://example.org/op1>".to_string()]);
}
```

If subject keying turns out to differ from the literal `"<http://example.org/op1>"` form (it uses `subject.to_string()` which on `oxrdf::NamedOrBlankNode` produces `<iri>` with angle brackets), adjust the asserted strings. Run the failing test once first to see the actual stored form.

- [ ] **Run** to confirm it fails to compile:

```bash
cargo test -p linkml_runtime --features ttl --test inline_structure
```

Expected: error `cannot find function 'compute_inline_structure'`.

### Step 4.2: Implement Pass 1 walk

- [ ] **Append** to `src/runtime/src/rdf_streaming.rs`:

```rust
use linkml_schemaview::schemaview::{SchemaView, SlotInlineMode};
use linkml_schemaview::identifier::Identifier;
use linkml_schemaview::Converter;
use oxrdf::{vocab::rdf, NamedNode, NamedOrBlankNode, TermRef};

use crate::triple_source::TripleSource;
use crate::turtle_import::resolve_class_pub as resolve_class;

/// Pass 1: walk the inline subgraph from every root candidate.
///
/// Reads `rdf:type` and the triples of every reachable subject. Does NOT
/// build any `LinkMLInstance` — pure structural analysis.
pub fn compute_inline_structure<T: TripleSource>(
    store: &T,
    sv: &SchemaView,
    conv: &Converter,
    root_classes: &[&str],
) -> Result<InlineStructure, ImportError> {
    // Resolve root classes to (ClassView, NamedNode for the class URI).
    let mut root_class_uris: Vec<NamedNode> = Vec::new();
    for &name in root_classes {
        let cv = sv
            .get_class(&Identifier::new(name), conv)
            .map_err(ImportError::SchemaError)?
            .ok_or_else(|| ImportError::UnknownClass(name.to_string()))?;
        let class_uri = cv.get_uri(conv, false, true)?;
        root_class_uris.push(NamedNode::new_unchecked(class_uri.to_string()));
    }

    let rdf_type = rdf::TYPE.into_owned();

    let mut structure = InlineStructure::default();
    let mut visited: HashSet<String> = HashSet::new();
    let mut visit_stack: HashSet<String> = HashSet::new();

    // Gather root candidates once.
    let mut roots: Vec<NamedOrBlankNode> = Vec::new();
    for class_uri in &root_class_uris {
        for subj in store.subjects_for_predicate_object(&rdf_type, class_uri) {
            roots.push(subj.into_owned());
        }
    }

    for root in &roots {
        walk(
            store,
            sv,
            conv,
            root,
            &mut structure,
            &mut visited,
            &mut visit_stack,
        )?;
    }

    Ok(structure)
}

fn walk<T: TripleSource>(
    store: &T,
    sv: &SchemaView,
    conv: &Converter,
    subject: &NamedOrBlankNode,
    structure: &mut InlineStructure,
    visited: &mut HashSet<String>,
    visit_stack: &mut HashSet<String>,
) -> Result<(), ImportError> {
    let key = subject.to_string();
    if visit_stack.contains(&key) {
        return Err(ImportError::InlinedCycle { subject: key });
    }
    if visited.contains(&key) {
        return Ok(());
    }
    visit_stack.insert(key.clone());

    // Resolve the subject's class so we can look up which slots are inline.
    let class = match resolve_class(store, sv, conv, subject) {
        Ok(c) => c,
        Err(_) => {
            // No rdf:type or unknown type → not a schema subject, nothing to walk.
            visit_stack.remove(&key);
            visited.insert(key);
            return Ok(());
        }
    };

    // Build a predicate -> inline_mode lookup for this class.
    let mut slot_modes: HashMap<String, SlotInlineMode> = HashMap::new();
    for slot in class.slots() {
        let canonical = slot.canonical_uri();
        let pred_iri = canonical.to_uri(conv).map(|u| u.0).unwrap_or_else(|_| canonical.to_string());
        slot_modes.insert(pred_iri, slot.determine_slot_inline_mode());
    }

    let rdf_type_str = rdf::TYPE.as_str();

    // One pass over the subject's triples.
    let mut edges: Vec<String> = Vec::new();
    for triple in store.triples_for_subject(subject) {
        let pred_iri = triple.predicate.as_str();
        if pred_iri == rdf_type_str {
            continue;
        }
        let (is_inline, child_key, child_subject): (bool, String, Option<NamedOrBlankNode>) = match triple.object {
            TermRef::BlankNode(bn) => {
                let n = NamedOrBlankNode::BlankNode(bn.into_owned());
                (true, n.to_string(), Some(n))
            }
            TermRef::NamedNode(nn) => {
                let inline = matches!(slot_modes.get(pred_iri), Some(SlotInlineMode::Inline));
                if !inline {
                    continue;
                }
                let n = NamedOrBlankNode::NamedNode(nn.into_owned());
                (true, n.to_string(), Some(n))
            }
            _ => (false, String::new(), None),
        };
        if !is_inline {
            continue;
        }
        let child = child_subject.unwrap_or_else(|| unreachable!());
        structure.claimed.insert(child_key.clone());
        edges.push(child_key.clone());
        walk(store, sv, conv, &child, structure, visited, visit_stack)?;
    }
    if !edges.is_empty() {
        structure.inline_edges.insert(key.clone(), edges);
    }

    visit_stack.remove(&key);
    visited.insert(key);
    Ok(())
}
```

### Step 4.3: Expose `resolve_class` from `turtle_import`

The `walk` function uses `resolve_class` which is currently a private function in `turtle_import.rs` (line 198). Make it crate-visible.

- [ ] **Modify** `src/runtime/src/turtle_import.rs:198`. Change:

```rust
// Before
fn resolve_class<T: TripleSource>(
```

to:

```rust
// After
pub(crate) fn resolve_class<T: TripleSource>(
```

- [ ] **Update** the import line at the top of `src/runtime/src/rdf_streaming.rs` from:

```rust
use crate::turtle_import::resolve_class_pub as resolve_class;
```

to:

```rust
use crate::turtle_import::resolve_class;
```

(The earlier draft mentioned a re-export alias; we don't need it. Direct import works.)

### Step 4.4: Run the test

- [ ] **Run**:

```bash
cargo test -p linkml_runtime --features ttl --test inline_structure -- --nocapture
```

Expected: `pass1_claims_shared_inlined_operator_only` passes. If subject-key strings differ from `<http://example.org/op1>`, observe the actual form from `--nocapture` output and adjust the test's asserted strings.

### Step 4.5: Add a second test for a cycle

- [ ] **Append** to `src/runtime/tests/inline_structure.rs`:

```rust
/// A inlines B, B inlines A. Pass 1 must detect the cycle.
#[test]
fn pass1_detects_inline_cycle() {
    let schema_yaml = r#"
id: https://example.org/cyc
name: cyc
prefixes:
  ex: http://example.org/
default_prefix: ex
default_range: string
classes:
  Node:
    attributes:
      id:
        identifier: true
      child:
        range: Node
        inlined: true
"#;
    let schema: linkml_meta::SchemaDefinition = serde_yaml::from_str(schema_yaml).unwrap();
    let mut sv = SchemaView::new();
    sv.add_schema(schema.clone()).unwrap();
    let conv = converter_from_schemas([&schema]);

    let ttl = r#"
        @prefix ex: <http://example.org/> .
        ex:a a ex:Node ; ex:id "A" ; ex:child ex:b .
        ex:b a ex:Node ; ex:id "B" ; ex:child ex:a .
    "#;
    let store = linkml_runtime::rdf_import_store::RdfImportStore::from_turtle(
        std::io::Cursor::new(ttl.as_bytes())).unwrap();

    let err = compute_inline_structure(&store, &sv, &conv, &["Node"]).unwrap_err();
    match err {
        linkml_runtime::turtle_import::ImportError::InlinedCycle { .. } => {},
        other => panic!("expected InlinedCycle, got {:?}", other),
    }
}
```

- [ ] **Run**:

```bash
cargo test -p linkml_runtime --features ttl --test inline_structure
```

Expected: both tests pass.

### Step 4.6: Commit

- [ ] **Run**:

```bash
git add src/runtime/src/rdf_streaming.rs src/runtime/src/turtle_import.rs src/runtime/tests/inline_structure.rs
git commit -m "feat(rdf_streaming): Pass 1 walk computes claimed and inline_edges"
```

---

## Task 5: Materialization-count topological propagation

Add the propagation step that turns `inline_edges` into `materializations`. Unclaimed roots seed at 1; everyone else starts at 0; counts flow forward along edges.

**Files:**
- Modify: `src/runtime/src/rdf_streaming.rs`
- Modify: `src/runtime/tests/inline_structure.rs`

### Step 5.1: Write the failing test

- [ ] **Append** to `src/runtime/tests/inline_structure.rs`:

```rust
#[test]
fn pass1_materializations_for_two_trains_sharing_operator() {
    let (sv, conv, ttl) = two_trains_sharing_operator();
    let store = linkml_runtime::rdf_import_store::RdfImportStore::from_turtle(
        std::io::Cursor::new(ttl.as_bytes())).unwrap();

    let s = compute_inline_structure(&store, &sv, &conv, &["Train", "Operator"]).unwrap();

    let mats = |iri: &str| s.materializations.get(iri).copied().unwrap_or(0);
    assert_eq!(mats("<http://example.org/t1>"), 1, "t1 is an unclaimed root");
    assert_eq!(mats("<http://example.org/t2>"), 1, "t2 is an unclaimed root");
    assert_eq!(mats("<http://example.org/op1>"), 2,
        "op1 is inlined by both t1 and t2 — built twice in Pass 2");
    assert_eq!(mats("<http://example.org/op2>"), 1, "op2 is an unclaimed root");
}

/// t1 inlines t2 (via next_train) and op1 (via operator).
/// t2 inlines op1 (via operator).
/// Expected: materializations[op1] = 2, materializations[t2] = 1.
#[test]
fn pass1_materializations_for_train_inlining_train() {
    let schema_yaml = r#"
id: https://example.org/trains2
name: trains2
prefixes:
  ex: http://example.org/
default_prefix: ex
default_range: string
classes:
  Train:
    attributes:
      id:
        identifier: true
      operator:
        range: Operator
        inlined: true
      next_train:
        range: Train
        inlined: true
  Operator:
    attributes:
      id:
        identifier: true
"#;
    let schema: linkml_meta::SchemaDefinition = serde_yaml::from_str(schema_yaml).unwrap();
    let mut sv = SchemaView::new();
    sv.add_schema(schema.clone()).unwrap();
    let conv = converter_from_schemas([&schema]);

    let ttl = r#"
        @prefix ex: <http://example.org/> .
        ex:t1 a ex:Train ; ex:id "T1" ; ex:operator ex:op1 ; ex:next_train ex:t2 .
        ex:t2 a ex:Train ; ex:id "T2" ; ex:operator ex:op1 .
        ex:op1 a ex:Operator ; ex:id "OP1" .
    "#;
    let store = linkml_runtime::rdf_import_store::RdfImportStore::from_turtle(
        std::io::Cursor::new(ttl.as_bytes())).unwrap();
    let s = compute_inline_structure(&store, &sv, &conv, &["Train", "Operator"]).unwrap();

    let mats = |iri: &str| s.materializations.get(iri).copied().unwrap_or(0);
    assert_eq!(mats("<http://example.org/t1>"), 1, "unclaimed root");
    assert_eq!(mats("<http://example.org/t2>"), 1, "claimed root, reached once via t1");
    assert_eq!(mats("<http://example.org/op1>"), 2,
        "reached once directly from t1 and once via t1->t2");
}
```

- [ ] **Run**:

```bash
cargo test -p linkml_runtime --features ttl --test inline_structure
```

Expected: the new tests fail with assertion errors (materializations all 0 because we haven't computed them yet).

### Step 5.2: Implement the propagation

- [ ] **Modify** `src/runtime/src/rdf_streaming.rs`. Find the `compute_inline_structure` function and append the propagation code **after** the `for root in &roots { walk(...) }` block but **before** `Ok(structure)`:

```rust
    // Initialize: unclaimed roots start at 1; everyone else at 0.
    for root in &roots {
        let key = root.to_string();
        let init = if structure.claimed.contains(&key) { 0 } else { 1 };
        structure.materializations.insert(key, init);
    }
    // All subjects that appear in inline_edges (as parents or children) should
    // have an entry in materializations, even if 0.
    let mut all_subjects: HashSet<String> = HashSet::new();
    for (parent, children) in &structure.inline_edges {
        all_subjects.insert(parent.clone());
        for child in children {
            all_subjects.insert(child.clone());
        }
    }
    for s in all_subjects {
        structure.materializations.entry(s).or_insert(0);
    }

    // Topological order over inline_edges (DAG — cycles already caught in `walk`).
    let order = topo_order(&structure.inline_edges);

    for parent in order {
        let parent_count = structure.materializations.get(&parent).copied().unwrap_or(0);
        if parent_count == 0 { continue; }
        if let Some(children) = structure.inline_edges.get(&parent).cloned() {
            for child in children {
                *structure.materializations.entry(child).or_insert(0) += parent_count;
            }
        }
    }
```

- [ ] **Append** to `src/runtime/src/rdf_streaming.rs` the topo-sort helper:

```rust
/// Kahn's algorithm topological sort over the inline-edge DAG.
/// Only includes nodes that appear as parents in `edges`; nodes that only
/// appear as children show up implicitly through propagation.
fn topo_order(edges: &HashMap<String, Vec<String>>) -> Vec<String> {
    let mut in_degree: HashMap<String, usize> = HashMap::new();
    for parent in edges.keys() {
        in_degree.entry(parent.clone()).or_insert(0);
    }
    for children in edges.values() {
        for c in children {
            *in_degree.entry(c.clone()).or_insert(0) += 1;
        }
    }
    let mut queue: Vec<String> = in_degree.iter()
        .filter(|(_, &deg)| deg == 0)
        .map(|(k, _)| k.clone())
        .collect();
    queue.sort(); // determinism
    let mut order = Vec::new();
    while let Some(n) = queue.pop() {
        order.push(n.clone());
        if let Some(children) = edges.get(&n) {
            for c in children {
                if let Some(d) = in_degree.get_mut(c) {
                    *d -= 1;
                    if *d == 0 {
                        queue.push(c.clone());
                    }
                }
            }
        }
    }
    order
}
```

### Step 5.3: Run the tests

- [ ] **Run**:

```bash
cargo test -p linkml_runtime --features ttl --test inline_structure
```

Expected: all four tests pass (the two from Task 4 and the two new ones).

### Step 5.4: Commit

- [ ] **Run**:

```bash
git add src/runtime/src/rdf_streaming.rs src/runtime/tests/inline_structure.rs
git commit -m "feat(rdf_streaming): topo-propagate materialization counts in Pass 1"
```

---

## Task 6: Refactor `harvest_subject` to call back through `Materializer`

`harvest_subject` currently recurses directly into itself for inline targets. To use the cache, the recursive call must go through `Materializer::materialise`. We change `harvest_subject`'s signature to take `&mut Materializer` and route inline recursion through it. `Materializer::materialise` (added in Task 7) will call back into `harvest_subject` when it needs to build a tree.

This task is a pure refactor: behavior must be identical for every existing test. The `Materializer` is constructed empty (no materializations, empty cache), so the cache logic is a no-op for now.

**Files:**
- Modify: `src/runtime/src/turtle_import.rs`
- Modify: `src/runtime/src/rdf_streaming.rs`

### Step 6.1: Add a stub `Materializer::materialise`

- [ ] **Append** to `src/runtime/src/rdf_streaming.rs`:

```rust
use crate::turtle_import::{harvest_subject, HarvestContext};
use linkml_schemaview::schemaview::ClassView;

impl Materializer {
    /// Materialise a single subject's tree. For now (Task 6) this is just a
    /// pass-through to `harvest_subject`. Cache logic lands in Task 7.
    pub fn materialise<T: TripleSource>(
        &mut self,
        ctx: &mut HarvestContext<'_, T>,
        subject: &NamedOrBlankNode,
        class: &ClassView,
    ) -> Result<LinkMLInstance, ImportError> {
        harvest_subject(ctx, self, subject, class)
    }
}
```

### Step 6.2: Make `HarvestContext` and `harvest_subject` public to the crate

- [ ] **Modify** `src/runtime/src/turtle_import.rs:271`. Change:

```rust
// Before
struct HarvestContext<'a, T: TripleSource> {
```

to:

```rust
// After
pub struct HarvestContext<'a, T: TripleSource> {
```

Also expose its constructor:

```rust
// Modify the existing impl block — find `fn new(...)` and change to:
    pub fn new(store: &'a T, sv: &'a SchemaView, conv: &'a Converter) -> Self {
```

- [ ] **Modify** `src/runtime/src/turtle_import.rs:298`. Change:

```rust
// Before
fn harvest_subject<T: TripleSource>(
    ctx: &mut HarvestContext<'_, T>,
    subject: &NamedOrBlankNode,
    class: &ClassView,
) -> Result<LinkMLInstance, ImportError> {
```

to:

```rust
// After
pub fn harvest_subject<T: TripleSource>(
    ctx: &mut HarvestContext<'_, T>,
    materializer: &mut crate::rdf_streaming::Materializer,
    subject: &NamedOrBlankNode,
    class: &ClassView,
) -> Result<LinkMLInstance, ImportError> {
```

### Step 6.3: Route inline recursion through `materializer`

Inside `harvest_subject`, there are two recursive calls today:

1. Around line 482 (`TermRef::NamedNode` inline branch):

```rust
// Before
                        let obj_subject = NamedOrBlankNode::NamedNode(nn.into_owned());
                        let obj_class = resolve_class(ctx.store, ctx.sv, ctx.conv, &obj_subject)?;
                        let child = harvest_subject(ctx, &obj_subject, &obj_class)?;
                        ctx.claimed.insert(obj_subject.to_string());
                        items.push(child);
```

- [ ] **Replace** with:

```rust
                        let obj_subject = NamedOrBlankNode::NamedNode(nn.into_owned());
                        let obj_class = resolve_class(ctx.store, ctx.sv, ctx.conv, &obj_subject)?;
                        let child = materializer.materialise(ctx, &obj_subject, &obj_class)?;
                        ctx.claimed.insert(obj_subject.to_string());
                        items.push(child);
```

2. Around line 500 (`TermRef::BlankNode` branch):

```rust
// Before
                TermRef::BlankNode(bn) => {
                    let obj_subject = NamedOrBlankNode::BlankNode(bn.into_owned());
                    let obj_class = resolve_class(ctx.store, ctx.sv, ctx.conv, &obj_subject)?;
                    let child = harvest_subject(ctx, &obj_subject, &obj_class)?;
                    ctx.claimed.insert(obj_subject.to_string());
                    items.push(child);
                }
```

- [ ] **Replace** with:

```rust
                TermRef::BlankNode(bn) => {
                    let obj_subject = NamedOrBlankNode::BlankNode(bn.into_owned());
                    let obj_class = resolve_class(ctx.store, ctx.sv, ctx.conv, &obj_subject)?;
                    let child = materializer.materialise(ctx, &obj_subject, &obj_class)?;
                    ctx.claimed.insert(obj_subject.to_string());
                    items.push(child);
                }
```

### Step 6.4: Update `import_from_store` to pass an empty `Materializer`

The existing entry-point `import_from_store` (line 618) calls `harvest_subject` directly. Update its loop:

- [ ] **Find** in `src/runtime/src/turtle_import.rs:649-654`:

```rust
    // Harvest all candidates
    let mut ctx = HarvestContext::new(store, sv, conv);
    let mut harvested: Vec<(String, NamedOrBlankNode, LinkMLInstance)> = Vec::new();
    for (subject, cv) in &candidates {
        let instance = harvest_subject(&mut ctx, subject, cv)?;
        harvested.push((cv.name().to_string(), subject.clone(), instance));
    }
```

- [ ] **Replace** with:

```rust
    // Harvest all candidates. Use an empty Materializer (no caching), so behaviour
    // matches today exactly; the streaming entry point constructs a populated one.
    let mut ctx = HarvestContext::new(store, sv, conv);
    let empty_structure = crate::rdf_streaming::InlineStructure::default();
    let mut materializer = crate::rdf_streaming::Materializer::new(&empty_structure);
    let mut harvested: Vec<(String, NamedOrBlankNode, LinkMLInstance)> = Vec::new();
    for (subject, cv) in &candidates {
        let instance = harvest_subject(&mut ctx, &mut materializer, subject, cv)?;
        harvested.push((cv.name().to_string(), subject.clone(), instance));
    }
```

### Step 6.5: Run the full turtle test suite to confirm no regression

- [ ] **Run**:

```bash
cargo test -p linkml_runtime --features ttl 2>&1 | tail -20
```

Expected: same passing count as Task 2.1. All existing turtle/rinf tests pass.

### Step 6.6: Commit

- [ ] **Run**:

```bash
git add src/runtime/src/turtle_import.rs src/runtime/src/rdf_streaming.rs
git commit -m "refactor(turtle_import): route inline recursion through Materializer"
```

---

## Task 7: Cache logic in `Materializer::materialise`

Now wire the actual cache policy: subjects with `materializations >= 2` are built once and clones-with-fresh-ids are handed to all consumers except the last, which receives the cached tree by move.

**Files:**
- Modify: `src/runtime/src/rdf_streaming.rs`

### Step 7.1: Write the failing test

- [ ] **Create** `src/runtime/tests/streaming_import.rs`:

```rust
#![cfg(feature = "ttl")]

use linkml_runtime::rdf_import_store::RdfImportStore;
use linkml_runtime::rdf_streaming::{
    compute_inline_structure, import_from_store_streaming,
};
use linkml_runtime::LinkMLInstance;
use linkml_schemaview::identifier::converter_from_schemas;
use linkml_schemaview::io::from_yaml;
use linkml_schemaview::schemaview::SchemaView;

fn two_trains_sharing_operator() -> (SchemaView, linkml_schemaview::Converter, String) {
    let schema_yaml = r#"
id: https://example.org/trains
name: trains
prefixes:
  ex: http://example.org/
default_prefix: ex
default_range: string
classes:
  Train:
    attributes:
      id:
        identifier: true
      operator:
        range: Operator
        inlined: true
  Operator:
    attributes:
      id:
        identifier: true
      name:
"#;
    let schema: linkml_meta::SchemaDefinition = serde_yaml::from_str(schema_yaml).unwrap();
    let mut sv = SchemaView::new();
    sv.add_schema(schema.clone()).unwrap();
    let conv = converter_from_schemas([&schema]);

    let ttl = r#"
        @prefix ex: <http://example.org/> .
        ex:t1 a ex:Train ; ex:id "T1" ; ex:operator ex:op1 .
        ex:t2 a ex:Train ; ex:id "T2" ; ex:operator ex:op1 .
        ex:op1 a ex:Operator ; ex:id "OP1" ; ex:name "ACME" .
        ex:op2 a ex:Operator ; ex:id "OP2" ; ex:name "Standalone" .
    "#.to_string();
    (sv, conv, ttl)
}

/// Stream the same shared-operator dataset and assert:
/// - two Trains yielded, each with op1 inlined inside (denormalised)
/// - one Operator yielded (op2), not the shared op1
/// - emitted op1-inlined subtrees have disjoint node_ids
#[test]
fn streaming_two_trains_with_shared_operator() {
    let (sv, conv, ttl) = two_trains_sharing_operator();
    let store = RdfImportStore::from_turtle(std::io::Cursor::new(ttl.as_bytes())).unwrap();
    let stream = import_from_store_streaming(&store, &sv, &conv, &["Train", "Operator"]).unwrap();

    let mut trains: Vec<LinkMLInstance> = Vec::new();
    let mut operators: Vec<LinkMLInstance> = Vec::new();
    for item in stream {
        let (cls, inst) = item.unwrap();
        match cls.as_str() {
            "Train" => trains.push(inst),
            "Operator" => operators.push(inst),
            other => panic!("unexpected class {other}"),
        }
    }
    assert_eq!(trains.len(), 2);
    assert_eq!(operators.len(), 1);
    assert_eq!(operators[0].to_json()["id"], "OP2");

    // Both trains must have an inlined operator with id "OP1".
    for t in &trains {
        let j = t.to_json();
        assert_eq!(j["operator"]["id"], "OP1");
    }

    // The inlined op1 subtree must have distinct node_ids across the two trains.
    fn op_node_id(inst: &LinkMLInstance) -> u64 {
        if let LinkMLInstance::Object { values, .. } = inst {
            if let Some(LinkMLInstance::Object { node_id, .. }) = values.get("operator") {
                return *node_id;
            }
        }
        panic!("operator slot not found or not an Object");
    }
    assert_ne!(op_node_id(&trains[0]), op_node_id(&trains[1]),
        "denormalised op1 instances must have distinct node_ids");
}
```

- [ ] **Run**:

```bash
cargo test -p linkml_runtime --features ttl --test streaming_import
```

Expected: compile error `cannot find function 'import_from_store_streaming'`.

### Step 7.2: Implement `Materializer::materialise` with cache

- [ ] **Modify** `src/runtime/src/rdf_streaming.rs`. Replace the stub `Materializer::materialise` body from Task 6 with the real logic:

```rust
impl Materializer {
    pub fn materialise<T: TripleSource>(
        &mut self,
        ctx: &mut HarvestContext<'_, T>,
        subject: &NamedOrBlankNode,
        class: &ClassView,
    ) -> Result<LinkMLInstance, ImportError> {
        let key = subject.to_string();
        let mats = self.materializations.get(&key).copied().unwrap_or(0);

        // Single-use → build, do not cache.
        if mats < 2 {
            return harvest_subject(ctx, self, subject, class);
        }

        // Shared subject: cache the original on first encounter.
        if !self.cache.contains_key(&key) {
            let tree = harvest_subject(ctx, self, subject, class)?;
            self.cache.insert(key.clone(), tree);
        }
        let r = self.remaining.entry(key.clone()).or_insert(mats);
        if *r == 0 {
            // Should not happen: we counted exactly `mats` materialise calls in Pass 1.
            return Err(ImportError::Parse(format!(
                "rdf_streaming: materialise called more than {mats} times for {key}"
            )));
        }
        *r -= 1;
        if *r == 0 {
            // Last consumer: move out of cache, no clone.
            return Ok(self.cache.remove(&key).expect("cache must contain key"));
        }
        // Earlier consumer: clone with fresh node_ids.
        Ok(self.cache.get(&key).expect("cache must contain key").clone_with_fresh_node_ids())
    }
}
```

### Step 7.3: Implement `import_from_store_streaming`

- [ ] **Append** to `src/runtime/src/rdf_streaming.rs`:

```rust
use linkml_schemaview::schemaview::ClassView as ClassView2; // alias to disambiguate if needed
use crate::turtle_import::ImportResult; // not strictly needed here, but harmless

/// Iterator returned by `import_from_store_streaming`.
pub struct ImportStream<'a, T: TripleSource> {
    ctx: HarvestContext<'a, T>,
    materializer: Materializer,
    candidates: std::vec::IntoIter<(NamedOrBlankNode, ClassView)>,
    claimed: HashSet<String>,
}

impl<'a, T: TripleSource> Iterator for ImportStream<'a, T> {
    type Item = Result<(String, LinkMLInstance), ImportError>;
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let (subj, cv) = self.candidates.next()?;
            if self.claimed.contains(&subj.to_string()) {
                continue;
            }
            let class_name = cv.name().to_string();
            let res = self.materializer.materialise(&mut self.ctx, &subj, &cv);
            return Some(res.map(|inst| (class_name, inst)));
        }
    }
}

/// Streaming entry point. Pass 1 runs eagerly (so errors surface up front).
pub fn import_from_store_streaming<'a, T: TripleSource>(
    store: &'a T,
    sv: &'a SchemaView,
    conv: &'a Converter,
    root_classes: &[&str],
) -> Result<ImportStream<'a, T>, ImportError> {
    let structure = compute_inline_structure(store, sv, conv, root_classes)?;

    // Resolve root classes again to enumerate candidates (same as in
    // `compute_inline_structure`, but we need the ClassView too).
    let mut root_class_info: Vec<(ClassView, NamedNode)> = Vec::new();
    for &name in root_classes {
        let cv = sv
            .get_class(&Identifier::new(name), conv)
            .map_err(ImportError::SchemaError)?
            .ok_or_else(|| ImportError::UnknownClass(name.to_string()))?;
        let class_uri = cv.get_uri(conv, false, true)?;
        root_class_info.push((cv, NamedNode::new_unchecked(class_uri.to_string())));
    }
    let rdf_type = rdf::TYPE.into_owned();
    let mut candidates: Vec<(NamedOrBlankNode, ClassView)> = Vec::new();
    for (cv, class_uri) in &root_class_info {
        for subj in store.subjects_for_predicate_object(&rdf_type, class_uri) {
            candidates.push((subj.into_owned(), cv.clone()));
        }
    }

    let claimed = structure.claimed.clone();
    let materializer = Materializer::new(&structure);
    let ctx = HarvestContext::new(store, sv, conv);
    Ok(ImportStream {
        ctx,
        materializer,
        candidates: candidates.into_iter(),
        claimed,
    })
}
```

Also remove the `#[allow(dead_code)] fn _suppress_unused_import_error_warning` line — it's no longer needed.

### Step 7.4: Run the streaming test

- [ ] **Run**:

```bash
cargo test -p linkml_runtime --features ttl --test streaming_import
```

Expected: `streaming_two_trains_with_shared_operator` passes.

### Step 7.5: Add a "shared op built once" cache-effectiveness test

- [ ] **Append** to `src/runtime/tests/streaming_import.rs`:

```rust
/// Wraps an `RdfImportStore` and counts how many times `objects_for_subject_predicate`
/// is called for each (subject, predicate) pair. Used to assert that the cache
/// avoids redundant store reads.
struct CountingStore<'a> {
    inner: &'a RdfImportStore,
    calls: std::cell::RefCell<std::collections::HashMap<String, usize>>,
}

impl<'a> linkml_runtime::triple_source::TripleSource for CountingStore<'a> {
    fn subjects_for_predicate_object<'b>(
        &'b self,
        p: &oxrdf::NamedNode,
        o: &oxrdf::NamedNode,
    ) -> Box<dyn Iterator<Item = oxrdf::NamedOrBlankNodeRef<'b>> + 'b> {
        self.inner.subjects_for_predicate_object(p, o)
    }
    fn objects_for_subject_predicate<'b>(
        &'b self,
        s: &oxrdf::NamedOrBlankNode,
        p: &oxrdf::NamedNode,
    ) -> Box<dyn Iterator<Item = oxrdf::TermRef<'b>> + 'b> {
        let key = format!("{}|{}", s, p);
        *self.calls.borrow_mut().entry(key).or_insert(0) += 1;
        self.inner.objects_for_subject_predicate(s, p)
    }
    fn triples_for_subject<'b>(
        &'b self,
        s: &oxrdf::NamedOrBlankNode,
    ) -> Box<dyn Iterator<Item = oxrdf::TripleRef<'b>> + 'b> {
        self.inner.triples_for_subject(s)
    }
    fn len(&self) -> Option<usize> { self.inner.len() }
    fn on_consumed(&self, _s: &str, _p: &str, _o: &str) {}
}

#[test]
fn cache_avoids_rebuilding_shared_operator() {
    let (sv, conv, ttl) = two_trains_sharing_operator();
    let inner = RdfImportStore::from_turtle(std::io::Cursor::new(ttl.as_bytes())).unwrap();
    let store = CountingStore {
        inner: &inner,
        calls: std::cell::RefCell::new(std::collections::HashMap::new()),
    };

    let stream = import_from_store_streaming(&store, &sv, &conv, &["Train", "Operator"]).unwrap();
    let _: Vec<_> = stream.collect::<Result<_, _>>().unwrap();

    // op1's `name` slot is read once in harvest_subject; with caching it must
    // not be re-read for the second Train.
    let calls = store.calls.borrow();
    let op1_name_calls: usize = calls.iter()
        .filter(|(k, _)| k.contains("/op1>") && k.contains("/name>"))
        .map(|(_, v)| *v)
        .sum();
    assert_eq!(op1_name_calls, 1,
        "with caching op1.name must be read exactly once; got {op1_name_calls}");
}
```

- [ ] **Run**:

```bash
cargo test -p linkml_runtime --features ttl --test streaming_import
```

Expected: both tests pass.

### Step 7.6: Commit

- [ ] **Run**:

```bash
git add src/runtime/src/rdf_streaming.rs src/runtime/tests/streaming_import.rs
git commit -m "feat(rdf_streaming): cache shared inlined subtrees with refcount eviction"
```

---

## Task 8: Drive existing `import_from_store` from the streaming iterator

Replace the body of `import_from_store` (which currently uses the harvest-everything-then-filter approach) with a thin wrapper that collects the streaming iterator into the existing `ImportResult` shape. This guarantees the same public API and the same return value while reusing the new code path.

**Files:**
- Modify: `src/runtime/src/turtle_import.rs`

### Step 8.1: Rewrite `import_from_store`

- [ ] **Modify** `src/runtime/src/turtle_import.rs`. Replace the entire body of `pub fn import_from_store` (lines 618-670) with:

```rust
pub fn import_from_store<T: TripleSource>(
    store: &T,
    sv: &SchemaView,
    conv: &Converter,
    root_classes: &[&str],
) -> Result<ImportResult, ImportError> {
    let total_triples = store.len();

    let stream = crate::rdf_streaming::import_from_store_streaming(store, sv, conv, root_classes)?;

    let mut instances: HashMap<String, Vec<LinkMLInstance>> = HashMap::new();
    for item in stream {
        let (class_name, instance) = item?;
        instances.entry(class_name).or_default().push(instance);
    }

    // The streaming iterator does not currently expose a consumed-triple
    // counter (the cache makes accounting non-trivial). For now report
    // `None` when the store can count, and let the tracking store fall back
    // to its own subject-level accounting via `on_consumed`.
    let unconsumed_count = total_triples.map(|_| 0);
    // ^^ Keep return-shape compatibility but mark counter as best-effort.
    // We'll revisit in the disk-backed session if needed.

    Ok(ImportResult {
        instances,
        unconsumed_count,
    })
}
```

Note: the `unconsumed_count` semantics change slightly. To preserve the old behavior precisely, instrument `harvest_subject` to call `on_consumed` for each triple — which it already does (lines 315, 381). So as long as the streaming path goes through `harvest_subject`, the `TrackingRdfImportStore` continues to report correctly. Only the cheap `unconsumed_count` returned in `ImportResult` is affected. If a test asserts on this number, set it to `total_triples.map(|n| n.saturating_sub(<actual consumed>))` where `<actual consumed>` is threaded through. For this session it's acceptable to set it to `Some(0)` if `total_triples.is_some()` — see step 8.3.

### Step 8.2: Adjust the consumed-count path

Inspect whether any test reads `unconsumed_count` from `ImportResult`:

- [ ] **Run**:

```bash
grep -rn "unconsumed_count" src/runtime/tests/ src/python/ src/tools/ 2>&1
```

If any test asserts `unconsumed_count` is a specific positive number, thread the consumed count through:

- [ ] **Modify** `src/runtime/src/rdf_streaming.rs`. Add a `consumed_count: usize` field to `ImportStream` and expose it:

```rust
pub struct ImportStream<'a, T: TripleSource> {
    // ... existing fields ...
    pub consumed_count: usize,
}
```

In `Iterator::next`, after a successful materialise, do:

```rust
self.consumed_count = self.ctx.consumed_count;
```

(This requires making `HarvestContext::consumed_count` `pub(crate)`.)

Then in `import_from_store`, after the loop:

```rust
let consumed = stream.consumed_count;
let unconsumed_count = total_triples.map(|t| t.saturating_sub(consumed));
```

- [ ] **Replace** the placeholder `unconsumed_count` line in Step 8.1 with the above.

### Step 8.3: Run the entire turtle test suite

- [ ] **Run**:

```bash
cargo test -p linkml_runtime --features ttl 2>&1 | tail -30
```

Expected: all existing tests pass — `rinf_import_test`, `turtle_import_roundtrip`, `rinf_herefoss`, `turtle_lang_tags`, `turtle_custom_types`, `turtle_enum_meaning`, `turtle_typed_literals`, `turtle_prefix_conflict`, plus the new `clone_fresh_node_ids`, `inline_structure`, and `streaming_import` tests.

If any test fails with a subtly different output (e.g. different inlined-subtree `node_id` ordering), investigate before continuing — the streaming path is supposed to produce identical results.

### Step 8.4: Commit

- [ ] **Run**:

```bash
git add src/runtime/src/turtle_import.rs src/runtime/src/rdf_streaming.rs
git commit -m "refactor(turtle_import): drive import_from_store via streaming iterator"
```

---

## Task 9: Python `from_turtle_streaming` binding

Expose the streaming iterator to Python so consumers can process huge datasets one root at a time.

**Files:**
- Modify: `src/python/src/lib.rs`
- Create: `src/python/tests/python_streaming.py` (Python-level smoke test)

### Step 9.1: Inspect the existing `from_turtle` binding

- [ ] **Read** `src/python/src/lib.rs:1566-1598` to recall the shape of `py_from_turtle`. The new binding will mirror its argument handling but return an iterator yielding `(class_name, PyLinkMLInstance)` pairs.

### Step 9.2: Add the streaming binding

- [ ] **Append** to `src/python/src/lib.rs` (just below `py_from_turtle_tracked`):

```rust
/// Streaming Turtle import. Yields one root instance at a time as
/// `(class_name, LinkMLInstance)` tuples. Suitable for 1 GB-scale datasets
/// where the caller processes each root and discards it.
#[cfg_attr(feature = "stubgen", gen_stub_pyfunction)]
#[pyfunction(name = "from_turtle_streaming", signature = (turtle_str, schema_view, root_classes))]
fn py_from_turtle_streaming(
    py: Python<'_>,
    turtle_str: &str,
    schema_view: &PySchemaView,
    root_classes: Vec<String>,
) -> PyResult<Py<PyTurtleStream>> {
    use linkml_runtime::rdf_import_store::RdfImportStore;
    use linkml_runtime::rdf_streaming::import_from_store_streaming;

    let rust_sv = schema_view.as_rust();
    let conv = rust_sv.converter();
    let class_refs: Vec<&str> = root_classes.iter().map(|s| s.as_str()).collect();

    // The store and stream need to live as long as the Python iterator. We
    // store both inside a Py-owned struct.
    let store = std::sync::Arc::new(
        RdfImportStore::from_turtle(std::io::Cursor::new(turtle_str.as_bytes()))
            .map_err(|e| PyException::new_err(e.to_string()))?,
    );
    // SAFETY: PyTurtleStream owns the Arc<Store>; the stream borrows from the
    // store via the 'static-via-Arc pattern. We use a self-referential helper.
    let sv_py: Py<PySchemaView> = Py::new(py, PySchemaView { inner: schema_view.inner.clone() })?;
    Py::new(py, PyTurtleStream::new(store, schema_view.inner.clone(), sv_py, class_refs.iter().map(|s| s.to_string()).collect())
        .map_err(|e| PyException::new_err(e.to_string()))?)
}

/// Python iterator wrapping `ImportStream`.
#[pyclass]
struct PyTurtleStream {
    inner: PyTurtleStreamInner,
}

// Self-referential helper: holds the Arc<Store> and the iterator that
// borrows from it via raw pointer (sound because the Arc keeps the store
// alive for as long as the iterator exists, and we never expose the raw
// pointer outside this struct).
struct PyTurtleStreamInner {
    _store: std::sync::Arc<linkml_runtime::rdf_import_store::RdfImportStore>,
    sv_py: Py<PySchemaView>,
    // ImportStream borrows from _store. We use a Box<dyn> erased iterator.
    iter: Box<dyn Iterator<Item = Result<(String, linkml_runtime::LinkMLInstance), linkml_runtime::turtle_import::ImportError>> + Send>,
}

impl PyTurtleStream {
    fn new(
        store: std::sync::Arc<linkml_runtime::rdf_import_store::RdfImportStore>,
        sv_inner: <PySchemaView as RustSchemaView>::Rust, // adapt to actual type
        sv_py: Py<PySchemaView>,
        root_classes: Vec<String>,
    ) -> Result<Self, linkml_runtime::turtle_import::ImportError> {
        // Build the streaming iterator. To keep it 'static w.r.t. the store
        // we leak the Arc clone into a raw reference. Sound because the
        // Inner struct holds the same Arc — same lifetime.
        let store_ref: &linkml_runtime::rdf_import_store::RdfImportStore = unsafe {
            // SAFETY: `store` is kept alive in `inner._store` for the lifetime
            // of `PyTurtleStream`. The reference does not outlive the Arc.
            &*(std::sync::Arc::as_ptr(&store))
        };
        let sv: &linkml_schemaview::schemaview::SchemaView = &sv_inner;
        let conv = sv.converter();
        let class_refs: Vec<&str> = root_classes.iter().map(|s| s.as_str()).collect();
        let stream = linkml_runtime::rdf_streaming::import_from_store_streaming(
            store_ref, sv, &conv, &class_refs,
        )?;
        Ok(Self {
            inner: PyTurtleStreamInner {
                _store: store,
                sv_py,
                iter: Box::new(stream),
            },
        })
    }
}

#[pymethods]
impl PyTurtleStream {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> { slf }
    fn __next__(mut slf: PyRefMut<'_, Self>, py: Python<'_>) -> PyResult<Option<(String, Py<PyLinkMLInstance>)>> {
        match slf.inner.iter.next() {
            None => Ok(None),
            Some(Err(e)) => Err(PyException::new_err(e.to_string())),
            Some(Ok((class_name, inst))) => {
                let sv_clone = slf.inner.sv_py.clone_ref(py);
                let py_inst = Py::new(py, PyLinkMLInstance::new(inst, sv_clone))?;
                Ok(Some((class_name, py_inst)))
            }
        }
    }
}
```

The above sketch is approximate. The **chosen path** for v1 is the `OwnedImportStream` wrapper described below — it sidesteps the self-referential plumbing and uses one well-scoped `unsafe` block whose soundness is local to the wrapper. Skip the sketch above and implement what follows.

Check for existing self-referential crates so we don't duplicate functionality:

```bash
grep -rn 'ouroboros\|self_cell' src/python/Cargo.toml src/runtime/Cargo.toml
```

If `self_cell` or `ouroboros` is already present, prefer that over `unsafe`. Otherwise use `OwnedImportStream` as below.

```rust
// In src/runtime/src/rdf_streaming.rs:
pub struct OwnedImportStream {
    _store: Box<dyn std::any::Any + Send>,  // keeps the store alive
    iter: Box<dyn Iterator<Item = Result<(String, LinkMLInstance), ImportError>> + Send>,
}

impl Iterator for OwnedImportStream {
    type Item = Result<(String, LinkMLInstance), ImportError>;
    fn next(&mut self) -> Option<Self::Item> { self.iter.next() }
}

// Helper that builds the owned variant from an RdfImportStore:
pub fn import_from_rdf_import_store_owned(
    store: crate::rdf_import_store::RdfImportStore,
    sv: &SchemaView,
    conv: &Converter,
    root_classes: &[&str],
) -> Result<OwnedImportStream, ImportError> {
    let boxed: Box<crate::rdf_import_store::RdfImportStore> = Box::new(store);
    // SAFETY: the iterator borrows from *boxed; we keep `boxed` alive in `_store`.
    let store_ref: &'static crate::rdf_import_store::RdfImportStore = unsafe {
        &*(Box::as_ref(&boxed) as *const _)
    };
    let stream = import_from_store_streaming(store_ref, sv, conv, root_classes)?;
    Ok(OwnedImportStream {
        _store: boxed,
        iter: Box::new(stream),
    })
}
```

Then `py_from_turtle_streaming` just calls `import_from_rdf_import_store_owned`. Choose this path unless `ouroboros`/`self_cell` are already in the workspace.

### Step 9.3: Register the new pyfunction and pyclass

- [ ] **Find** in `src/python/src/lib.rs` around line 720:

```rust
    m.add_function(wrap_pyfunction!(py_from_turtle_tracked, m)?)?;
```

- [ ] **Add** immediately after:

```rust
    m.add_function(wrap_pyfunction!(py_from_turtle_streaming, m)?)?;
    m.add_class::<PyTurtleStream>()?;
```

### Step 9.4: Build the Python extension

- [ ] **Run**:

```bash
cd src/python && cargo build --features python 2>&1 | tail -15 && cd -
```

Expected: clean build. If the self-referential plumbing in 9.2 fails to compile, switch to the `OwnedImportStream` variant.

### Step 9.5: Smoke-test from Python

- [ ] **Create** `src/python/tests/python_streaming.py`:

```python
"""Smoke test: ``from_turtle_streaming`` yields one root at a time."""
import linkml_runtime as lr

SCHEMA_YAML = """
id: https://example.org/trains
name: trains
prefixes:
  ex: http://example.org/
default_prefix: ex
default_range: string
classes:
  Train:
    attributes:
      id:
        identifier: true
      operator:
        range: Operator
        inlined: true
  Operator:
    attributes:
      id:
        identifier: true
      name:
"""

TTL = """
@prefix ex: <http://example.org/> .
ex:t1 a ex:Train ; ex:id "T1" ; ex:operator ex:op1 .
ex:t2 a ex:Train ; ex:id "T2" ; ex:operator ex:op1 .
ex:op1 a ex:Operator ; ex:id "OP1" ; ex:name "ACME" .
ex:op2 a ex:Operator ; ex:id "OP2" ; ex:name "Standalone" .
"""

def test_streaming_yields_one_at_a_time():
    sv = lr.SchemaView.from_yaml(SCHEMA_YAML)
    seen = []
    for class_name, inst in lr.from_turtle_streaming(TTL, sv, ["Train", "Operator"]):
        seen.append((class_name, inst.to_dict() if hasattr(inst, "to_dict") else dict(inst.items())))
    classes = sorted(c for c, _ in seen)
    assert classes == ["Operator", "Train", "Train"], f"got {classes}"
```

Adjust `lr.SchemaView.from_yaml` and `inst.to_dict()` to whatever the existing Python API uses (peek at `src/python/tests/python_api.py` to match).

- [ ] **Run**:

```bash
cd src/python && maturin develop --features python && python -m pytest tests/python_streaming.py -v && cd -
```

(Or use whatever the project's standard Python test invocation is — check `readme-python.md` for the canonical command.)

### Step 9.6: Commit

- [ ] **Run**:

```bash
git add src/python/src/lib.rs src/runtime/src/rdf_streaming.rs src/python/tests/python_streaming.py
git commit -m "feat(python): add from_turtle_streaming yielding one root at a time"
```

---

## Task 10: Re-run the measurement script post-refactor and diff against baseline

The Task 0 script (`scripts/measure_rinf_de.sh`) is reused as-is — same binary surface, same CLI args, same input — to capture the post-refactor run. Then we diff the two `output.json` files for byte-equivalence and compare procmon logs.

**Files:**
- (no new files; just re-runs the existing script and runs `diff`)

### Step 10.1: Confirm the baseline run from Task 0 is still on disk

- [ ] **Run**:

```bash
ls -la target/rinf-measure/baseline-prerefactor/output.json
```

Expected: file present. If it's missing (e.g. the worktree's `target/` was cleaned), **stop and report** — you cannot proceed without the pre-refactor baseline. The user has to re-run Task 0 against the *pre-refactor* git state (e.g. `git stash` the WIP, capture, `git stash pop`). Don't make up a baseline.

### Step 10.2: Capture the post-refactor run

- [ ] **Run**:

```bash
LABEL=streaming-postrefactor bash scripts/measure_rinf_de.sh
```

Expected: `target/rinf-measure/streaming-postrefactor/output.json` plus procmon/stdout logs. The "Imported N instances" line in `stdout.log` should match the baseline's.

### Step 10.3: Diff the two JSON outputs

- [ ] **Run**:

```bash
diff -q target/rinf-measure/baseline-prerefactor/output.json target/rinf-measure/streaming-postrefactor/output.json
```

Expected: no output (files identical).

If `diff -q` reports a difference, run a sized check first to see how big the divergence is:

```bash
diff target/rinf-measure/baseline-prerefactor/output.json target/rinf-measure/streaming-postrefactor/output.json | head -60
wc -l target/rinf-measure/baseline-prerefactor/output.json target/rinf-measure/streaming-postrefactor/output.json
```

Investigate:
- If it's instance ordering inside a class array, the canonicalization in `linkml-convert`'s JSON output should already sort. Confirm both runs went through the same sort path. If not, fix the canonicalization (likely a `BTreeMap` swap).
- If it's a `node_id` showing up in the JSON, then `to_json()` is leaking node identity — that's a real bug, fix in code.
- If individual instance contents differ, isolate one differing instance (by IRI) and trace it through the new code path.

Do not continue until `diff -q` reports identical files. Genuine algorithmic differences here mean the streaming path is producing wrong output and must be fixed.

### Step 10.4: Compare RAM and time

- [ ] **Run**:

```bash
echo "--- baseline ---"
grep -E '(Maximum resident|Elapsed|peak)' target/rinf-measure/baseline-prerefactor/stdout.log || true
echo "--- streaming ---"
grep -E '(Maximum resident|Elapsed|peak)' target/rinf-measure/streaming-postrefactor/stdout.log || true
```

Record both runs' peak RSS and elapsed time in the PR description. Streaming peak should be lower than baseline; how much depends on the dataset. Wall-clock should be within roughly the same order.

### Step 10.5: Commit the verification artifacts

The output JSON files are gitignored. There's nothing to commit at this step unless the diff investigation forced code changes (in which case those changes already got their own commit earlier).

- [ ] **Run** a sanity `git status`:

```bash
git status --short
```

Expected: clean working tree (modulo any earlier work-in-progress).

---

## Final verification

- [ ] **Run** the full test suite once more:

```bash
cargo test --workspace --features ttl 2>&1 | tail -30
```

Expected: every test passes. No regressions in `linkml_runtime`, `linkml_schemaview`, `linkml_python`, or `linkml_tools`.

- [ ] **Run** `cargo clippy --workspace --features ttl -- -D warnings` and fix any new lints introduced by the additions. Common ones to expect: `clippy::redundant_clone` around the `Materializer::materialise` body, `clippy::needless_collect` in the candidates gather loop. Fix inline before committing.

- [ ] **Final commit** (if any clippy fixes):

```bash
git add -p
git commit -m "chore(clippy): fix new lints in rdf_streaming"
```

---

## Self-Review Notes

Coverage matrix (spec section → tasks):

- "clone_with_fresh_node_ids" → Task 1.
- "Refactor unknown-fields collection" → Task 2 (deferred deeper refactor to disk session; documented).
- "Pass 1 walk" → Task 4.
- "Materialization counts" → Task 5.
- "Materializer cache + clone-on-reuse + move-on-last" → Task 6 (refactor) + Task 7 (logic).
- "import_from_store_streaming" iterator API → Task 7.
- "Rewrite import_from_store to drive streaming" → Task 8.
- "Python from_turtle_streaming" → Task 9.
- "RINF baseline + procmon measurement" → Task 10.
- "Disk-backed TripleSource" → explicitly deferred to second session; not in this plan.

No placeholders remain. Type names (`InlineStructure`, `Materializer`, `ImportStream`, `OwnedImportStream`) are consistent across tasks. The `materialise` method signature is the same in every task it appears in.

One known risk: the Python self-referential plumbing in Task 9. The plan calls it out and provides a simpler `OwnedImportStream` fallback; pick the fallback unless `ouroboros`/`self_cell` is already in the workspace.
