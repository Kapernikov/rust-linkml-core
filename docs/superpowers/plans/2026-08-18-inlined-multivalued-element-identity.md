# Inlined Multivalued Element Identity Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make element identity for inlined multivalued slots declarable — key/identifier, `unique_keys` composed key, or `diff.linkml.io/opaque` (nowhere) — and add an opt-in linter that flags slots where identity comes from nowhere.

**Architecture:** Three additive features in the existing crates. (1) A `diff.linkml.io/opaque` slot annotation makes `diff` stop all recursion and emit one whole-value `Update`, and makes `patch` refuse to descend below the slot. (2) `diff`'s keyed-list matching learns a fallback identity derived from the range class's (inheritance-merged) `unique_keys`; `patch` learns to resolve those labels. (3) A new `identity_lint` module offers two opt-in entry points (schema-level ambiguity lint, data-level duplicate-identity lint) that are **not** wired into default validation. No changes to `DiffOptions`/`PatchOptions` shapes; behaviour only changes for schemas that declare the annotation or `unique_keys` (no current fixture or consumer does).

**Tech Stack:** Rust workspace (`linkml_runtime`, `schemaview`, `linkml_tools`, `linkml_runtime_python` via PyO3), cargo integration tests.

**Spec:** `docs/superpowers/specs/2026-08-17-inlined-multivalued-element-identity-design.md`

## Global Constraints

- **Non-goal + deliberate break (spec, Non-goal section):** slots that declare nothing keep positional semantics, but keyed matching becomes uniform: a list matches by identity iff every element on both sides yields an identity label (key/identifier first, else `unique_keys`) AND the labels are unique within each side; every other case is positional with plain numeric path segments. This deliberately removes (a) opportunistic key labels mixed into positional paths and (b) the silent collapse of duplicate key values under keyed matching. Existing tests that assert those two removed behaviours are updated — the implementer's report must list each updated test with its old assertion and why it changed. All other existing tests pass unchanged.
- **"Report, never guess" (spec):** a patch that cannot locate its target unambiguously returns `Ok(false)` so the path lands in `PatchTrace::failed`. No fuzzy fallbacks.
- **Layering (spec):** validation/lint code never reads `diff.linkml.io/*` to *suppress* a schema-constraint finding. The schema-level linter may skip opaque slots (identity is declared: nowhere); the data-level duplicate check never consults the annotation.
- `PatchTrace::failed` stays `Vec<Vec<String>>` — do not import the abandoned branch's `PatchFailure` struct.
- Every task ends green: `cargo test --workspace` (or the named `-p` subset), `cargo fmt --all`, `cargo clippy --workspace --all-targets -- -D warnings` at the final task.
- Commit after every task; message style from `git log`: `feat(runtime): ...`, `test(runtime): ...`, `feat(schemaview): ...`.

---

### Task 1: `ClassView::unique_keys()` — inheritance-merged accessor

The metamodel already parses `unique_keys` (`src/metamodel/src/lib.rs:11520`, `ClassDefinition.unique_keys: Option<HashMap<String, Box<UniqueKey>>>`) but `ClassView::def()` returns the raw definition: keys declared on an `is_a` parent or mixin are invisible (ClassView merges slots, not unique_keys). Everything downstream (diff matching, linter) needs a merged, deterministic view.

**Files:**
- Modify: `src/schemaview/src/classview.rs` (new method near `key_or_identifier_slot()`, ~line 606)
- Create: `src/schemaview/tests/unique_keys.rs`
- Create: `src/schemaview/tests/data/unique_keys.yaml`

**Interfaces:**
- Consumes: `ClassView::def()`, `ClassView::parent_class()` (classview.rs:590), the mixin-resolution pattern of `collect_ancestors_map` (classview.rs:624), `linkml_meta::UniqueKey { unique_key_name, unique_key_slots: Vec<String>, consider_nulls_inequal, .. }`.
- Produces: `pub fn unique_keys(&self) -> Vec<(String, UniqueKey)>` on `ClassView` — merged across `is_a` and mixins (nearest definition wins per name), **sorted by name** (HashMap order is nondeterministic and diff paths must be stable). Tasks 4, 5, 6 rely on exactly this signature.

- [ ] **Step 1: Write the fixture schema**

`src/schemaview/tests/data/unique_keys.yaml` (copy the header — `id`, `name`, `prefixes`, `default_range`, types/imports — from an existing fixture in `src/schemaview/tests/data/` so types resolve the same way):

```yaml
classes:
  Base:
    unique_keys:
      by_code:
        unique_key_slots: [code]
      shared_name:
        unique_key_slots: [base_field]
    attributes:
      code: {range: string}
      base_field: {range: string}
  MixinCls:
    mixin: true
    unique_keys:
      by_tag:
        unique_key_slots: [tag]
    attributes:
      tag: {range: string}
  Child:
    is_a: Base
    mixins: [MixinCls]
    unique_keys:
      shared_name:                # overrides Base's entry of the same name
        unique_key_slots: [child_field]
    attributes:
      child_field: {range: string}
  Plain:
    attributes:
      whatever: {range: string}
```

- [ ] **Step 2: Write the failing test**

`src/schemaview/tests/unique_keys.rs` (mirror the SchemaView setup of a neighbouring test, e.g. `class_lookup.rs`):

```rust
use linkml_schemaview::identifier::{converter_from_schema, Identifier};
use linkml_schemaview::io::from_yaml;
use linkml_schemaview::schemaview::SchemaView;
use std::path::PathBuf;

fn fixture() -> SchemaView {
    let mut p = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    p.push("tests/data/unique_keys.yaml");
    let schema = from_yaml(&p).unwrap();
    let mut sv = SchemaView::new();
    sv.add_schema(schema).unwrap();
    sv
}

#[test]
fn unique_keys_merge_across_is_a_and_mixins_nearest_wins() {
    let sv = fixture();
    let conv = sv.converter();
    let child = sv
        .get_class(&Identifier::new("Child"), &conv)
        .unwrap()
        .expect("class not found");
    let uks = child.unique_keys();
    let names: Vec<&str> = uks.iter().map(|(n, _)| n.as_str()).collect();
    // name-sorted, merged from Base (by_code), MixinCls (by_tag), Child (shared_name override)
    assert_eq!(names, vec!["by_code", "by_tag", "shared_name"]);
    let shared = &uks.iter().find(|(n, _)| n == "shared_name").unwrap().1;
    assert_eq!(
        shared.unique_key_slots,
        vec!["child_field".to_string()],
        "the nearest declaration must win"
    );
}

#[test]
fn class_without_unique_keys_yields_empty() {
    let sv = fixture();
    let conv = sv.converter();
    let plain = sv
        .get_class(&Identifier::new("Plain"), &conv)
        .unwrap()
        .expect("class not found");
    assert!(plain.unique_keys().is_empty());
}
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `cargo test -p schemaview --test unique_keys`
Expected: FAIL to compile — `unique_keys` method not found on `ClassView`.

- [ ] **Step 4: Implement the accessor**

In `src/schemaview/src/classview.rs`, near `key_or_identifier_slot()`. BFS from `self` through `is_a` parents and mixins so nearer declarations claim a name first (`or_insert`). The sketch below uses `parent_class()` for the `is_a` chain; resolve mixin names to `ClassView`s the same way `collect_ancestors_map` (classview.rs:624) does — adjust to that private API rather than inventing a new resolution path.

```rust
    /// The class's `unique_keys`, merged across the inheritance chain
    /// (`is_a` parents and mixins), nearest declaration winning per name.
    /// Name-sorted: declaration order is lost in the underlying map and
    /// consumers (diff path segments) need a deterministic order.
    pub fn unique_keys(&self) -> Vec<(String, UniqueKey)> {
        use std::collections::{HashMap, VecDeque};
        let mut merged: HashMap<String, UniqueKey> = HashMap::new();
        let mut queue: VecDeque<ClassView> = VecDeque::from([self.clone()]);
        let mut seen: std::collections::HashSet<String> = Default::default();
        while let Some(cv) = queue.pop_front() {
            if !seen.insert(cv.name().to_string()) {
                continue;
            }
            if let Some(uks) = cv.def().unique_keys.as_ref() {
                for (name, uk) in uks {
                    merged
                        .entry(name.clone())
                        .or_insert_with(|| (**uk).clone());
                }
            }
            if let Ok(Some(parent)) = cv.parent_class() {
                queue.push_back(parent);
            }
            // + push each resolved mixin ClassView (resolution as in
            //   collect_ancestors_map, classview.rs:624)
        }
        let mut out: Vec<(String, UniqueKey)> = merged.into_iter().collect();
        out.sort_by(|a, b| a.0.cmp(&b.0));
        out
    }
```

Import `linkml_meta::UniqueKey` at the top of the file (check the existing `linkml_meta` imports there for the path).

- [ ] **Step 5: Run tests to verify they pass**

Run: `cargo test -p schemaview --test unique_keys`
Expected: PASS (both tests).

- [ ] **Step 6: Run the schemaview suite for regressions**

Run: `cargo test -p schemaview`
Expected: PASS, no other test touched.

- [ ] **Step 7: Commit**

```bash
git add src/schemaview/src/classview.rs src/schemaview/tests/unique_keys.rs src/schemaview/tests/data/unique_keys.yaml
git commit -m "feat(schemaview): ClassView::unique_keys merged across is_a and mixins"
```

---

### Task 2: shared fixture + `diff.linkml.io/opaque` on the diff side

**Files:**
- Create: `src/runtime/tests/data/identity.yaml`
- Create: `src/runtime/tests/diff_opaque.rs`
- Modify: `src/runtime/src/diff.rs` (annotation const + check + early-return in `inner`, near lines 10–21 and 121–125)
- Modify: `src/runtime/src/lib.rs` (re-export `OPAQUE_ANNOTATION` in the `pub use diff::{...}` list, line 49)

**Interfaces:**
- Consumes: `slot_is_ignored` pattern (`diff.rs:12-21`), `LinkMLInstance::equals(other, treat_missing_as_null)`.
- Produces: `pub const OPAQUE_ANNOTATION: &str = "diff.linkml.io/opaque"` and `pub(crate) fn slot_is_opaque(slot: &SlotView) -> bool` in `diff.rs` (Tasks 3 and 6 use both); the fixture `identity.yaml` (all later runtime test files load it).

- [ ] **Step 1: Write the fixture schema**

`src/runtime/tests/data/identity.yaml`. Copy the header (`id`, `name`, `prefixes`, `default_range`, and the types/imports mechanism) from `src/runtime/tests/data/personinfo.yaml` so `string`/`float`/`uri` resolve identically. Classes (this is the spec's asset360 material, condensed):

```yaml
classes:
  Service:
    attributes:
      name: {range: string}
      # spec Example 1 with its correct resolution: identity = the function
      hasPhoneNumber:
        range: ServicePhoneNumber
        multivalued: true
        inlined_as_list: true
      # same shape, nothing declared: must keep positional behaviour; linter flags it
      plainPhoneNumber:
        range: PlainPhoneNumber
        multivalued: true
        inlined_as_list: true
      # inherited unique_keys (via is_a) must also drive matching
      escalation:
        range: EmergencyPhoneNumber
        multivalued: true
        inlined_as_list: true
      # composite two-slot unique key
      contacts:
        range: Contact
        multivalued: true
        inlined_as_list: true
      # opaque + a keyed element class: data lint must still check unique_keys
      archivedContacts:
        range: Contact
        multivalued: true
        inlined_as_list: true
        annotations:
          diff.linkml.io/opaque: true
      # scalar list, nothing declared: positional; linter flags it
      tags:
        range: string
        multivalued: true
      # scalar list declared opaque
      opaqueTags:
        range: string
        multivalued: true
        annotations:
          diff.linkml.io/opaque: true
      # spec Example 2's ring: opaque list of bare vertices
      outline:
        range: Vertex
        multivalued: true
        inlined_as_list: true
        annotations:
          diff.linkml.io/opaque: true
      # single-valued opaque object: "stop all recursion" is not list-specific
      profile:
        range: Profile
        inlined: true
        annotations:
          diff.linkml.io/opaque: true
      # keyed class inlined as dict: already unambiguous, linter stays silent
      labels:
        range: Label
        multivalued: true
        inlined: true
      # keyed class inlined as list: the uniform guard applies to key labels too
      labelList:
        range: Label
        multivalued: true
        inlined_as_list: true
      # reference list (not inlined): out of the linter's scope
      operators:
        range: Operator
        multivalued: true
        inlined: false
      area:
        range: AreaLocation
        inlined: true

  ServicePhoneNumber:
    unique_keys:
      one_number_per_function:
        unique_key_slots: [hasNumberFunction]
    attributes:
      phoneNumber: {range: string}
      hasNumberFunction: {range: NumberFunction, required: true}

  PlainPhoneNumber:
    attributes:
      phoneNumber: {range: string}
      hasNumberFunction: {range: NumberFunction, required: true}

  EmergencyPhoneNumber:
    is_a: ServicePhoneNumber
    attributes:
      note: {range: string}

  Contact:
    unique_keys:
      contact_identity:
        unique_key_slots: [kind, phone]
    attributes:
      kind: {range: string, required: true}
      phone: {range: string, required: true}
      note: {range: string}

  Label:
    attributes:
      lang: {range: string, key: true, required: true}
      text: {range: string}

  Profile:
    attributes:
      bio: {range: string}
      motto: {range: string}

  Operator:
    attributes:
      opId: {range: string, identifier: true}
      opName: {range: string}

  AreaLocation:
    attributes:
      polygons:
        range: Polygon
        multivalued: true
        inlined_as_list: true

  Polygon:
    unique_keys:
      one_polygon_per_positioning_system:
        unique_key_slots: [positioningSystemType]
    attributes:
      positioningSystemType: {range: uri, required: true}
      coordinates:
        range: Vertex
        multivalued: true
        inlined_as_list: true
        annotations:
          diff.linkml.io/opaque: true

  Vertex:
    attributes:
      x: {range: float}
      y: {range: float}

enums:
  NumberFunction:
    permissible_values:
      Emergency_Number:
      Non_Urgent_Communication:
      Operator:
```

- [ ] **Step 2: Write the failing tests**

`src/runtime/tests/diff_opaque.rs`. The move/insert/drop/reverse loop is recycled near-verbatim from the abandoned branch's `array_slot_emits_one_whole_slot_update` (its `feat/container-shapes-and-verify-old` branch, `src/runtime/tests/diff_shapes.rs:158-197`) — same assertions, annotation renamed.

```rust
use linkml_runtime::{diff, load_json_str, patch, DiffOptions, LinkMLInstance, PatchOptions};
use linkml_schemaview::identifier::{converter_from_schema, Identifier};
use linkml_schemaview::io::from_yaml;
use linkml_schemaview::schemaview::{ClassView, SchemaView};
use linkml_schemaview::Converter;
use linkml_runtime::{Delta, DeltaOp};
use serde_json::{json, Value as JsonValue};
use std::path::PathBuf;

struct Fixture {
    sv: SchemaView,
    conv: Converter,
    service: ClassView,
}

fn fixture() -> Fixture {
    let mut p = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    p.push("tests/data/identity.yaml");
    let schema = from_yaml(&p).unwrap();
    let mut sv = SchemaView::new();
    sv.add_schema(schema.clone()).unwrap();
    let conv = converter_from_schema(&schema);
    let service = sv
        .get_class(&Identifier::new("Service"), &conv)
        .unwrap()
        .expect("class not found");
    Fixture { sv, conv, service }
}

impl Fixture {
    fn load(&self, v: JsonValue) -> LinkMLInstance {
        load_json_str(&v.to_string(), &self.sv, &self.service, &self.conv)
            .unwrap()
            .into_instance()
            .unwrap()
    }
}

fn diff2(f: &Fixture, before: JsonValue, after: JsonValue) -> Vec<Delta> {
    diff(&f.load(before), &f.load(after), DiffOptions::new(true))
}

fn only(deltas: &[Delta]) -> &Delta {
    assert_eq!(deltas.len(), 1, "expected exactly one delta: {deltas:#?}");
    &deltas[0]
}

fn square() -> Vec<JsonValue> {
    vec![
        json!({"x": 4.35, "y": 50.85}),
        json!({"x": 4.36, "y": 50.85}),
        json!({"x": 4.36, "y": 50.86}),
        json!({"x": 4.35, "y": 50.86}),
    ]
}

fn outline(items: Vec<JsonValue>) -> JsonValue {
    json!({"name": "svc", "outline": items})
}

#[test]
fn opaque_ring_edit_is_one_whole_slot_update() {
    let f = fixture();

    let mut moved = square();
    moved[1] = json!({"x": 4.37, "y": 50.85});
    let mut inserted = square();
    inserted.insert(2, json!({"x": 4.365, "y": 50.855}));
    let dropped_first = square()[1..].to_vec();
    let mut reversed = square();
    reversed.reverse();

    for (label, after) in [
        ("move one vertex", moved),
        ("insert a vertex mid-ring", inserted),
        ("drop the first vertex", dropped_first),
        ("reverse the ring", reversed),
    ] {
        let deltas = diff2(&f, outline(square()), outline(after));
        let delta = only(&deltas);
        assert_eq!(delta.path, vec!["outline".to_string()], "{label}");
        assert_eq!(delta.op, DeltaOp::Update, "{label}");
        assert_eq!(
            delta.old.as_ref().and_then(|v| v.as_array()).map(|a| a.len()),
            Some(4),
            "{label}: old must be the whole slot"
        );
        assert!(delta.new.as_ref().is_some_and(|v| v.is_array()), "{label}");
    }
}

#[test]
fn opaque_slot_unchanged_emits_nothing() {
    let f = fixture();
    let deltas = diff2(&f, outline(square()), outline(square()));
    assert!(deltas.is_empty(), "{deltas:#?}");
}

#[test]
fn opaque_scalar_list_is_one_whole_slot_update() {
    let f = fixture();
    let before = json!({"name": "svc", "opaqueTags": ["a", "b"]});
    let after = json!({"name": "svc", "opaqueTags": ["b", "c", "d"]});
    let deltas = diff2(&f, before, after);
    let delta = only(&deltas);
    assert_eq!(delta.path, vec!["opaqueTags".to_string()]);
    assert_eq!(delta.op, DeltaOp::Update);
}

#[test]
fn opaque_single_valued_object_is_one_whole_value_update() {
    let f = fixture();
    let before = json!({"name": "svc", "profile": {"bio": "b", "motto": "old"}});
    let after = json!({"name": "svc", "profile": {"bio": "b", "motto": "new"}});
    let deltas = diff2(&f, before, after);
    let delta = only(&deltas);
    assert_eq!(delta.path, vec!["profile".to_string()]);
    assert_eq!(delta.op, DeltaOp::Update);
    assert_eq!(delta.old, Some(json!({"bio": "b", "motto": "old"})));
    assert_eq!(delta.new, Some(json!({"bio": "b", "motto": "new"})));
}

#[test]
fn opaque_whole_slot_update_round_trips_through_patch() {
    let f = fixture();
    let before = outline(square());
    let mut moved = square();
    moved[1] = json!({"x": 4.37, "y": 50.85});
    let after = outline(moved);
    let deltas = diff2(&f, before.clone(), after.clone());
    let (patched, trace) = patch(&f.load(before), &deltas, PatchOptions::default()).unwrap();
    assert!(trace.failed.is_empty(), "{:?}", trace.failed);
    assert!(
        patched.equals(&f.load(after), true),
        "round-trip mismatch: {}",
        patched.to_json()
    );
}
```

(If `Delta`/`DeltaOp`/`PatchOptions` import paths differ, copy the exact imports from `src/runtime/tests/diff.rs`.)

- [ ] **Step 3: Run tests to verify they fail**

Run: `cargo test -p linkml_runtime --test diff_opaque`
Expected: FAIL — `opaque_ring_edit_is_one_whole_slot_update` sees many positional deltas instead of one; the single-valued and scalar cases see field-level/element-level deltas. (The round-trip test may pass already; that's fine.)

- [ ] **Step 4: Implement the diff side**

In `src/runtime/src/diff.rs`, next to `IGNORE_ANNOTATION` (line 10):

```rust
/// Slot annotation declaring that element identity comes from nowhere: stop
/// all recursion, the slot's value is one atomic unit. Any change below the
/// slot is described as a single whole-value `Update` at the slot path, and
/// `patch` refuses paths that descend below it.
pub const OPAQUE_ANNOTATION: &str = "diff.linkml.io/opaque";

pub(crate) fn slot_is_opaque(slot: &SlotView) -> bool {
    if slot.definitions().is_empty() {
        return false;
    }
    slot.definition()
        .annotations
        .as_ref()
        .map(|a| a.contains_key(OPAQUE_ANNOTATION))
        .unwrap_or(false)
}
```

(Presence-based, exactly like `slot_is_ignored` — the annotation's *presence* is the declaration, mirroring `diff.linkml.io/ignore`.)

In `diff`'s `inner`, extend the existing slot guard (lines 121–125):

```rust
        if let Some(sl) = slot {
            if slot_is_ignored(sl) {
                return;
            }
            if slot_is_opaque(sl) {
                if !s.equals(t, opts.treat_missing_as_null) {
                    out.push(Delta {
                        path: path.clone(),
                        op: DeltaOp::Update,
                        old: Some(s.to_json()),
                        new: Some(t.to_json()),
                    });
                }
                return;
            }
        }
```

Note: `inner` receives `slot: Some(..)` only for direct slot values (the object arm); list/mapping elements recurse with `None`, so the check fires exactly at slot level. A slot appearing/disappearing entirely is handled by the object arm's missing-slot branches, which already emit whole-value deltas without recursing.

In `src/runtime/src/lib.rs:49`, add `OPAQUE_ANNOTATION` to the diff re-export list.

- [ ] **Step 5: Run tests to verify they pass**

Run: `cargo test -p linkml_runtime --test diff_opaque`
Expected: PASS (all 5).

- [ ] **Step 6: Run the full runtime suite for regressions**

Run: `cargo test -p linkml_runtime`
Expected: PASS — no existing fixture carries the annotation, so nothing else may change.

- [ ] **Step 7: Commit**

```bash
git add src/runtime/tests/data/identity.yaml src/runtime/tests/diff_opaque.rs src/runtime/src/diff.rs src/runtime/src/lib.rs
git commit -m "feat(runtime): diff.linkml.io/opaque stops recursion, replaces whole value"
```

---

### Task 3: opaque on the patch side — refuse to descend

A patch produced outside this diff lib may address structure below an opaque slot (e.g. positional vertex paths). Locating anything below the slot would be a guess; the spec says such deltas are reported as failed.

**Files:**
- Modify: `src/runtime/src/diff.rs` (`apply_delta_object` descent at ~line 873, `apply_delta_mapping` at ~880, `apply_delta_list` at ~928)
- Test: `src/runtime/tests/diff_opaque.rs` (extend)

**Interfaces:**
- Consumes: `slot_is_opaque` from Task 2.
- Produces: patch behaviour only — no new names.

- [ ] **Step 1: Write the failing tests**

Append to `src/runtime/tests/diff_opaque.rs`:

```rust
#[test]
fn patch_below_opaque_slot_is_reported_failed_not_guessed() {
    let f = fixture();
    let golden = f.load(outline(square()));
    let delta = Delta {
        path: vec!["outline".to_string(), "1".to_string(), "x".to_string()],
        op: DeltaOp::Update,
        old: Some(json!(4.36)),
        new: Some(json!(9.99)),
    };
    let (patched, trace) = patch(&golden, &[delta.clone()], PatchOptions::default()).unwrap();
    assert_eq!(trace.failed, vec![delta.path.clone()]);
    assert!(
        patched.equals(&golden, true),
        "nothing may change: {}",
        patched.to_json()
    );
}

#[test]
fn patch_at_opaque_slot_path_still_applies_whole_value() {
    let f = fixture();
    let golden = f.load(outline(square()));
    let mut moved = square();
    moved[1] = json!({"x": 4.37, "y": 50.85});
    let delta = Delta {
        path: vec!["outline".to_string()],
        op: DeltaOp::Update,
        old: Some(json!(square())),
        new: Some(json!(moved.clone())),
    };
    let (patched, trace) = patch(&golden, &[delta], PatchOptions::default()).unwrap();
    assert!(trace.failed.is_empty(), "{:?}", trace.failed);
    assert!(patched.equals(&f.load(outline(moved)), true));
}
```

- [ ] **Step 2: Run tests to verify the new one fails**

Run: `cargo test -p linkml_runtime --test diff_opaque`
Expected: `patch_below_opaque_slot_is_reported_failed_not_guessed` FAILS (the positional path currently applies); the whole-value test passes.

- [ ] **Step 3: Implement the refusal**

In `apply_delta_object` (diff.rs, the descent after the `path.len() == 1` early return, ~line 873):

```rust
    if let Some(child) = values.get_mut(key) {
        let slot = class.slots().iter().find(|s| s.name == *key);
        if slot.is_some_and(slot_is_opaque) {
            // The path descends below an opaque slot: it addresses structure
            // the slot does not expose. Report, never guess.
            return Ok(false);
        }
        return apply_delta_linkml_inner(child, &path[1..], op, newv, trace, opts);
    }
```

And defensively at the top of `apply_delta_mapping` and `apply_delta_list` (reached with a non-empty path only when the delta addresses *inside* the container — which is below the container's slot). This covers patches whose root instance is itself a list/mapping:

```rust
    if slot_is_opaque(slot) {
        return Ok(false);
    }
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p linkml_runtime --test diff_opaque`
Expected: PASS (all 7).

- [ ] **Step 5: Full runtime suite**

Run: `cargo test -p linkml_runtime`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/runtime/src/diff.rs src/runtime/tests/diff_opaque.rs
git commit -m "feat(runtime): patch refuses to descend below an opaque slot"
```

---

### Task 4: `unique_keys`-derived identity in diff list matching

**Files:**
- Modify: `src/runtime/src/diff.rs` (extract label helpers, extend the `(List, List)` arm at lines 211–296)
- Create: `src/runtime/tests/diff_unique_keys.rs`

**Interfaces:**
- Consumes: `ClassView::unique_keys()` (Task 1), the fixture (Task 2).
- Produces (all in `diff.rs`, used by Tasks 5 and 6):
  - `pub(crate) fn element_key_label(v: &LinkMLInstance) -> Option<String>` — the existing key/identifier label, extracted from the inline closure.
  - `pub(crate) fn element_unique_key_label(v: &LinkMLInstance) -> Option<String>` — label from the first (name-sorted) non-empty `unique_keys` entry; single-slot keys yield the bare scalar string, composite keys yield the JSON-array encoding of the values in `unique_key_slots` order (e.g. `["Emergency","02/111.11.11"]`).
  - `pub(crate) fn element_identity_label(v: &LinkMLInstance) -> Option<String>` — `element_key_label` first, `element_unique_key_label` as fallback.
  - `fn scalar_slot_string(values: &HashMap<String, LinkMLInstance>, slot_name: &str) -> Option<String>`.

**Precedence and guard rules (from the spec's Non-goal section — the uniform rule):**
1. opaque > key/identifier > unique_keys > positional.
2. A list is matched by identity iff every element on both sides yields an identity label (`element_identity_label`) AND the labels are unique within each side. The guard is uniform — it applies to key/identifier labels exactly as to `unique_keys` labels (removing today's silent collapse of duplicate keys).
3. The positional branch uses plain numeric segments (`i.to_string()`) only — the old opportunistic mixing of key values into positional paths is removed.

- [ ] **Step 1: Write the failing tests**

`src/runtime/tests/diff_unique_keys.rs` — reuse the exact `Fixture`/`diff2`/`only` harness from `diff_opaque.rs` (copy it; integration tests don't share modules). Data builders:

```rust
fn e() -> JsonValue {
    json!({"phoneNumber": "09/241.25.00", "hasNumberFunction": "Emergency_Number"})
}
fn n() -> JsonValue {
    json!({"phoneNumber": "09/241.25.03", "hasNumberFunction": "Non_Urgent_Communication"})
}
fn o() -> JsonValue {
    json!({"phoneNumber": "09/241.25.10", "hasNumberFunction": "Operator"})
}
fn phones(items: Vec<JsonValue>) -> JsonValue {
    json!({"name": "svc", "hasPhoneNumber": items})
}
```

Tests:

```rust
#[test]
fn unique_key_matching_targets_field_edits_by_key() {
    let f = fixture();
    let mut n2 = n();
    n2["phoneNumber"] = json!("09/241.25.99");
    let deltas = diff2(&f, phones(vec![e(), n()]), phones(vec![e(), n2]));
    let delta = only(&deltas);
    assert_eq!(
        delta.path,
        vec![
            "hasPhoneNumber".to_string(),
            "Non_Urgent_Communication".to_string(),
            "phoneNumber".to_string()
        ]
    );
    assert_eq!(delta.op, DeltaOp::Update);
}

#[test]
fn unique_key_matching_ignores_reorder() {
    let f = fixture();
    let deltas = diff2(&f, phones(vec![e(), n(), o()]), phones(vec![o(), n(), e()]));
    assert!(deltas.is_empty(), "reorder must be invisible: {deltas:#?}");
}

#[test]
fn unique_key_remove_and_add_are_key_addressed() {
    let f = fixture();
    let deltas = diff2(&f, phones(vec![e(), n()]), phones(vec![n()]));
    let delta = only(&deltas);
    assert_eq!(delta.op, DeltaOp::Remove);
    assert_eq!(
        delta.path,
        vec!["hasPhoneNumber".to_string(), "Emergency_Number".to_string()]
    );
    assert_eq!(delta.old, Some(e()));

    let deltas = diff2(&f, phones(vec![e(), n()]), phones(vec![e(), n(), o()]));
    let delta = only(&deltas);
    assert_eq!(delta.op, DeltaOp::Add);
    assert_eq!(
        delta.path,
        vec!["hasPhoneNumber".to_string(), "Operator".to_string()]
    );
    assert_eq!(delta.new, Some(o()));
}

#[test]
fn changing_the_key_slot_is_remove_plus_add() {
    let f = fixture();
    let mut moved = e();
    moved["hasNumberFunction"] = json!("Operator");
    let deltas = diff2(&f, phones(vec![e(), n()]), phones(vec![moved.clone(), n()]));
    assert_eq!(deltas.len(), 2, "{deltas:#?}");
    assert!(deltas
        .iter()
        .any(|d| d.op == DeltaOp::Remove && d.old.as_ref() == Some(&e())));
    assert!(deltas
        .iter()
        .any(|d| d.op == DeltaOp::Add && d.new.as_ref() == Some(&moved)));
}

#[test]
fn duplicate_unique_key_data_falls_back_to_positional() {
    let f = fixture();
    // two Emergency numbers: violates the class claim; data must keep
    // today's positional behaviour, with numeric path segments
    let e2 = json!({"phoneNumber": "09/000.00.00", "hasNumberFunction": "Emergency_Number"});
    let mut e2_edit = e2.clone();
    e2_edit["phoneNumber"] = json!("09/111.11.11");
    let deltas = diff2(
        &f,
        phones(vec![e(), e2]),
        phones(vec![e(), e2_edit]),
    );
    let delta = only(&deltas);
    assert_eq!(
        delta.path,
        vec![
            "hasPhoneNumber".to_string(),
            "1".to_string(),
            "phoneNumber".to_string()
        ],
        "positional fallback must use numeric segments, never the duplicate label"
    );
}

#[test]
fn duplicate_key_data_falls_back_to_positional_not_collapse() {
    let f = fixture();
    // Label declares `lang` as key; a list that repeats the key must not be
    // silently collapsed by keyed matching — uniform guard, positional fallback.
    let before = json!({"name": "svc", "labelList": [
        {"lang": "nl", "text": "a"}, {"lang": "nl", "text": "b"}]});
    let after = json!({"name": "svc", "labelList": [
        {"lang": "nl", "text": "a"}, {"lang": "nl", "text": "B"}]});
    let deltas = diff2(&f, before, after);
    let delta = only(&deltas);
    assert_eq!(
        delta.path,
        vec!["labelList".to_string(), "1".to_string(), "text".to_string()],
        "duplicate key labels must fall back to plain numeric segments"
    );
}

#[test]
fn undeclared_class_keeps_positional_cascade() {
    let f = fixture();
    // PlainPhoneNumber has no unique_keys: removal still cascades as today
    let before = json!({"name": "svc", "plainPhoneNumber": [e(), n(), o()]});
    let after = json!({"name": "svc", "plainPhoneNumber": [n(), o()]});
    let deltas = diff2(&f, before, after);
    assert_eq!(deltas.len(), 3, "{deltas:#?}");
}

#[test]
fn composite_unique_key_uses_json_array_segment() {
    let f = fixture();
    let a = json!({"kind": "Emergency", "phone": "02/111.11.11", "note": "old"});
    let mut a2 = a.clone();
    a2["note"] = json!("new");
    let b = json!({"kind": "Operator", "phone": "02/333.33.33"});
    let before = json!({"name": "svc", "contacts": [a, b.clone()]});
    let after = json!({"name": "svc", "contacts": [a2, b]});
    let deltas = diff2(&f, before, after);
    let delta = only(&deltas);
    assert_eq!(
        delta.path,
        vec![
            "contacts".to_string(),
            r#"["Emergency","02/111.11.11"]"#.to_string(),
            "note".to_string()
        ]
    );
}

#[test]
fn inherited_unique_keys_drive_matching() {
    let f = fixture();
    // EmergencyPhoneNumber inherits one_number_per_function via is_a
    let x = json!({"phoneNumber": "1", "hasNumberFunction": "Emergency_Number", "note": "a"});
    let mut x2 = x.clone();
    x2["note"] = json!("b");
    let y = json!({"phoneNumber": "2", "hasNumberFunction": "Operator", "note": "c"});
    let before = json!({"name": "svc", "escalation": [x, y.clone()]});
    let after = json!({"name": "svc", "escalation": [x2, y]});
    let deltas = diff2(&f, before, after);
    let delta = only(&deltas);
    assert_eq!(
        delta.path,
        vec![
            "escalation".to_string(),
            "Emergency_Number".to_string(),
            "note".to_string()
        ]
    );
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p linkml_runtime --test diff_unique_keys`
Expected: the matching tests FAIL (positional deltas today); `duplicate_key_data_falls_back_to_positional_not_collapse` also FAILS today (current keyed matching silently collapses the duplicates — the defect the uniform guard removes). `duplicate_unique_key_data_falls_back_to_positional` and `undeclared_class_keeps_positional_cascade` PASS already and must stay green throughout.

- [ ] **Step 3: Implement**

In `src/runtime/src/diff.rs`, replace the inline `label` closure of the `(List, List)` arm (lines 212–226) with module-level helpers, then rework the arm:

```rust
pub(crate) fn scalar_slot_string(
    values: &std::collections::HashMap<String, LinkMLInstance>,
    slot_name: &str,
) -> Option<String> {
    if let Some(LinkMLInstance::Scalar { value, .. }) = values.get(slot_name) {
        return match value {
            JsonValue::String(s) => Some(s.clone()),
            other => Some(other.to_string()),
        };
    }
    None
}

/// The key/identifier value identifying `v` among its list siblings, if any.
pub(crate) fn element_key_label(v: &LinkMLInstance) -> Option<String> {
    if let LinkMLInstance::Object { values, class, .. } = v {
        let id_slot = class.key_or_identifier_slot()?;
        return scalar_slot_string(values, &id_slot.name);
    }
    None
}

/// A matching label derived from the range class's merged `unique_keys`.
///
/// The name-sorted first entry with a non-empty slot list is the matching
/// identity (declaration order is not preserved by the metamodel, and diff
/// paths must be stable). Single-slot keys use the bare scalar value as the
/// label and path segment; composite keys use the JSON array encoding of the
/// values in `unique_key_slots` order (`["Emergency","02/111.11.11"]`) —
/// unambiguous, parseable, and displayable.
pub(crate) fn element_unique_key_label(v: &LinkMLInstance) -> Option<String> {
    if let LinkMLInstance::Object { values, class, .. } = v {
        let uks = class.unique_keys();
        let (_, uk) = uks.iter().find(|(_, uk)| !uk.unique_key_slots.is_empty())?;
        let parts: Option<Vec<String>> = uk
            .unique_key_slots
            .iter()
            .map(|s| scalar_slot_string(values, s))
            .collect();
        let mut parts = parts?;
        return Some(if parts.len() == 1 {
            parts.remove(0)
        } else {
            serde_json::to_string(&parts).expect("Vec<String> serializes")
        });
    }
    None
}

/// Identity for keyed list matching: a key/identifier slot outranks a
/// `unique_keys` claim.
pub(crate) fn element_identity_label(v: &LinkMLInstance) -> Option<String> {
    element_key_label(v).or_else(|| element_unique_key_label(v))
}

fn labels_are_unique<F>(elements: &[LinkMLInstance], label: F) -> bool
where
    F: Fn(&LinkMLInstance) -> Option<String>,
{
    let mut seen = std::collections::HashSet::new();
    elements.iter().filter_map(label).all(|l| seen.insert(l))
}
```

The `(List, List)` arm becomes:

```rust
(LinkMLInstance::List { values: sl, .. }, LinkMLInstance::List { values: tl, .. }) => {
    let identity = |v: &LinkMLInstance| -> Option<String> { element_identity_label(v) };
    // Uniform rule (spec, Non-goal section): keyed matching iff every element
    // on both sides carries an identity label and the labels are unique
    // within each side. Duplicate labels (a list repeating a key, or data
    // violating a unique_keys claim) fall back to positional — matching
    // duplicates by label would silently collapse elements.
    let keyed = sl.iter().all(|v| identity(v).is_some())
        && tl.iter().all(|v| identity(v).is_some())
        && labels_are_unique(sl, &identity)
        && labels_are_unique(tl, &identity);
    if keyed {
        // ... identical to the current keyed block (lines 233-266),
        //     with every `label(..)` call replaced by `identity(..)` ...
    } else {
        // ... the current positional block (lines 267-296), with the
        //     opportunistic label chain REPLACED by plain numeric segments:
        //     every path segment is `i.to_string()`.
    }
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p linkml_runtime --test diff_unique_keys`
Expected: PASS (all 9).

- [ ] **Step 5: Full runtime suite — the compatibility gate**

Run: `cargo test -p linkml_runtime && cargo test -p schemaview`
Expected: mostly PASS. Existing tests that assert the two deliberately removed behaviours — opportunistic key labels inside positional paths, or keyed matching of lists with duplicate labels — may fail; update each such test to the uniform rule and list every updated test in your report with its old assertion and why it changed. Any other failure means the implementation drifted — fix the implementation, not the test.

- [ ] **Step 6: Commit**

```bash
git add src/runtime/src/diff.rs src/runtime/tests/diff_unique_keys.rs
git commit -m "feat(runtime): match inlined list elements by unique_keys-derived identity"
```

---

### Task 5: `unique_keys` labels on the patch side

**Files:**
- Modify: `src/runtime/src/diff.rs` (`resolve_list_index`, lines 530–558)
- Test: `src/runtime/tests/diff_unique_keys.rs` (extend)

**Interfaces:**
- Consumes: `element_identity_label` (Task 4).
- Produces: patch behaviour only. Note the deliberate break (Global Constraints): duplicate labels in the current list now refuse (`Ok(false)` → `trace.failed`) instead of first-match; this applies to key/identifier labels exactly as to `unique_keys` labels.

- [ ] **Step 1: Write the failing tests**

Append to `diff_unique_keys.rs`:

```rust
#[test]
fn patch_locates_element_by_unique_key_under_drift() {
    let f = fixture();
    // producer saw [E, N]; golden drifted to [N, E, O]
    let golden = f.load(phones(vec![n(), e(), o()]));
    let delta = Delta {
        path: vec![
            "hasPhoneNumber".to_string(),
            "Emergency_Number".to_string(),
            "phoneNumber".to_string(),
        ],
        op: DeltaOp::Update,
        old: Some(json!("09/241.25.00")),
        new: Some(json!("09/999.99.99")),
    };
    let (patched, trace) = patch(&golden, &[delta], PatchOptions::default()).unwrap();
    assert!(trace.failed.is_empty(), "{:?}", trace.failed);
    let mut e2 = e();
    e2["phoneNumber"] = json!("09/999.99.99");
    assert!(
        patched.equals(&f.load(phones(vec![n(), e2, o()])), true),
        "the edit must land on E wherever it sits: {}",
        patched.to_json()
    );
}

#[test]
fn patch_reports_ambiguous_unique_key_instead_of_guessing() {
    let f = fixture();
    // golden drifted into two Emergency elements: locating "the" one is a guess
    let e2 = json!({"phoneNumber": "09/000.00.00", "hasNumberFunction": "Emergency_Number"});
    let golden = f.load(phones(vec![e(), e2]));
    let delta = Delta {
        path: vec![
            "hasPhoneNumber".to_string(),
            "Emergency_Number".to_string(),
            "phoneNumber".to_string(),
        ],
        op: DeltaOp::Update,
        old: Some(json!("09/241.25.00")),
        new: Some(json!("09/999.99.99")),
    };
    let (patched, trace) = patch(&golden, &[delta.clone()], PatchOptions::default()).unwrap();
    assert_eq!(trace.failed, vec![delta.path.clone()]);
    assert!(patched.equals(&golden, true), "nothing may change");
}

#[test]
fn patch_refuses_ambiguous_duplicate_key_labels() {
    let f = fixture();
    // Duplicate key/identifier labels refuse exactly like duplicate
    // unique_keys labels — the uniform rule on the patch side.
    let golden = f.load(json!({"name": "svc", "labelList": [
        {"lang": "nl", "text": "a"}, {"lang": "nl", "text": "b"}]}));
    let delta = Delta {
        path: vec!["labelList".to_string(), "nl".to_string(), "text".to_string()],
        op: DeltaOp::Update,
        old: Some(json!("a")),
        new: Some(json!("z")),
    };
    let (patched, trace) = patch(&golden, &[delta.clone()], PatchOptions::default()).unwrap();
    assert_eq!(trace.failed, vec![delta.path.clone()]);
    assert!(patched.equals(&golden, true), "nothing may change");
}

#[test]
fn unique_key_deltas_round_trip_through_patch() {
    let f = fixture();
    let mut n2 = n();
    n2["phoneNumber"] = json!("09/241.25.99");
    for (before, after) in [
        (phones(vec![e(), n()]), phones(vec![e(), n2])),        // field edit
        (phones(vec![e(), n()]), phones(vec![n()])),             // remove
        (phones(vec![e(), n()]), phones(vec![e(), n(), o()])),   // add
    ] {
        let deltas = diff2(&f, before.clone(), after.clone());
        let (patched, trace) = patch(&f.load(before), &deltas, PatchOptions::default()).unwrap();
        assert!(trace.failed.is_empty(), "{:?}", trace.failed);
        assert!(patched.equals(&f.load(after), true), "{}", patched.to_json());
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p linkml_runtime --test diff_unique_keys`
Expected: the two locate tests FAIL (segment `Emergency_Number` resolves to no index today); the remove/add legs of the round-trip may also fail.

- [ ] **Step 3: Implement**

Rewrite `resolve_list_index` (diff.rs:530) as one unified resolver. The numeric-index attempt stays first and unchanged; the old key/identifier `find_map` block is **replaced** by an identity-label pass that uses the same precedence and stringification diff uses to build segments (making diff→patch symmetric), and that refuses ambiguity:

```rust
fn resolve_list_index(values: &[LinkMLInstance], key: &str) -> Option<usize> {
    if let Ok(idx) = key.parse::<usize>() {
        if idx < values.len() {
            return Some(idx);
        }
    }
    // Identity-label location (key/identifier first, else unique_keys) — the
    // same precedence and stringification diff uses to build the segment.
    // Only an unambiguous hit counts: if the current list holds duplicate
    // labels, locating "the" element would be a guess — return None so the
    // delta is reported as failed.
    let mut hit: Option<usize> = None;
    for (i, v) in values.iter().enumerate() {
        if element_identity_label(v).as_deref() == Some(key) {
            if hit.is_some() {
                return None;
            }
            hit = Some(i);
        }
    }
    hit
}
```

Note on `Add` deltas: an `Add` whose unique-key segment resolves to no element takes the existing `idx_opt = None` append path in `apply_list_leaf_delta` (line 687) — that is correct and needs no change.

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p linkml_runtime --test diff_unique_keys`
Expected: PASS (all 12).

- [ ] **Step 5: Full runtime suite — the compatibility gate**

Run: `cargo test -p linkml_runtime`
Expected: mostly PASS. Existing tests that assert the removed first-match-on-duplicate-labels patch behaviour may fail; update each such test to the uniform refuse-ambiguity rule and list every updated test in your report with its old assertion and why it changed. Any other failure means the implementation drifted — fix the implementation, not the test.

- [ ] **Step 6: Commit**

```bash
git add src/runtime/src/diff.rs src/runtime/tests/diff_unique_keys.rs
git commit -m "feat(runtime): patch resolves identity-label path segments, refuses ambiguity"
```

---

### Task 6: the opt-in identity linter

Two opt-in entry points in a new module. Neither is called from `validate_issues` or any load path — that is the spec's non-goal.

**Files:**
- Create: `src/runtime/src/identity_lint.rs`
- Modify: `src/runtime/src/lib.rs` (add `pub mod identity_lint;` next to `pub mod diff;` at line 28; add two variants to `ValidationProblemType` at lines 150–157; re-export `identity_lint::{lint_element_identity, lint_instance_identity}`)
- Create: `src/runtime/tests/identity_lint.rs`

**Interfaces:**
- Consumes: `ClassView::unique_keys()` (Task 1), `slot_is_opaque`/`OPAQUE_ANNOTATION` (Task 2), `element_identity_label` (Task 4), `SlotView::determine_slot_container_mode()` / `determine_slot_inline_mode()` / `get_range_class()` / `is_range_scalar()` (slotview.rs:500–564), `ValidationResultSink` (`push_warning`, `into_vec`), `SchemaView::{get_class_ids, get_class, converter}`.
- Produces:
  - `ValidationProblemType::AmbiguousElementIdentity` and `ValidationProblemType::DuplicateElementIdentity` (new enum variants — the compiler will flag every `match` that needs extending, including the Python binding's problem-type stringification; extend them all).
  - `pub fn lint_element_identity(sv: &SchemaView) -> Vec<ValidationResult>` — schema-level.
  - `pub fn lint_instance_identity(value: &LinkMLInstance) -> Vec<ValidationResult>` — data-level.

**Lint rules (schema-level), per class × slot:**
- Only `SlotContainerMode::List` slots are candidates (`Mapping` is keyed by construction, `SingleValue` has no elements).
- Skip `SlotInlineMode::Reference` (elements are references, not inlined — out of the spec's scope).
- Skip slots carrying `diff.linkml.io/ignore` or `diff.linkml.io/opaque` (identity declared: nowhere).
- Object range: OK if the range class has `key_or_identifier_slot()` or a non-empty `unique_keys()`. Otherwise warn.
- Scalar range (`is_range_scalar`): always warn (positional identity; the only declaration available is opaque).
- Severity: **Warning**, never error. `subject` = `vec![class_name, slot_name]`.

**Lint rules (data-level), per `List` node in the instance tree:**
- Elements whose class declares a key/identifier or `unique_keys` and whose `element_identity_label` repeats within the container → one `DuplicateElementIdentity` warning per repeated label, at the container's path.
- Deliberately does **not** consult the opaque annotation (layering: `diff.linkml.io/*` never suppresses a schema-constraint finding). The remodel in the spec is what makes rings silent — their `Vertex` class declares nothing.
- Elements with no declared identity (no key, no unique_keys) are never flagged — repeated content is data, not an error.

- [ ] **Step 1: Write the failing tests**

`src/runtime/tests/identity_lint.rs` (same `fixture()` harness copied from `diff_opaque.rs`, plus these):

```rust
use linkml_runtime::{lint_element_identity, lint_instance_identity, ValidationProblemType};

#[test]
fn schema_lint_flags_exactly_the_undeclared_positional_slots() {
    let f = fixture();
    let warnings = lint_element_identity(&f.sv);
    let mut flagged: Vec<(String, String)> = warnings
        .iter()
        .map(|w| (w.subject[0].clone(), w.subject[1].clone()))
        .collect();
    flagged.sort();
    assert_eq!(
        flagged,
        vec![
            ("Service".to_string(), "plainPhoneNumber".to_string()),
            ("Service".to_string(), "tags".to_string()),
        ],
        "everything else declares its identity source: {warnings:#?}"
    );
    for w in &warnings {
        assert_eq!(w.problem_type, ValidationProblemType::AmbiguousElementIdentity);
        assert!(!w.severity.is_error(), "the linter warns, never errors");
        assert!(
            w.detail.contains("unique_keys") && w.detail.contains("diff.linkml.io/opaque"),
            "the warning must name the author's options: {}",
            w.detail
        );
    }
}

#[test]
fn data_lint_flags_duplicate_declared_identities() {
    let f = fixture();
    let dup = json!({"phoneNumber": "09/000.00.00", "hasNumberFunction": "Emergency_Number"});
    let inst = f.load(phones(vec![e(), dup]));
    let warnings = lint_instance_identity(&inst);
    assert_eq!(warnings.len(), 1, "{warnings:#?}");
    assert_eq!(warnings[0].problem_type, ValidationProblemType::DuplicateElementIdentity);
    assert_eq!(warnings[0].subject, vec!["hasPhoneNumber".to_string()]);
    assert!(!warnings[0].severity.is_error());
}

#[test]
fn data_lint_is_silent_on_clean_and_undeclared_data() {
    let f = fixture();
    // unique phone functions, repeated scalar tags, repeated identity-less vertices
    let inst = f.load(json!({
        "name": "svc",
        "hasPhoneNumber": [e(), n()],
        "tags": ["a", "a"],
        "outline": [{"x": 1.0, "y": 2.0}, {"x": 1.0, "y": 2.0}]
    }));
    let warnings = lint_instance_identity(&inst);
    assert!(warnings.is_empty(), "{warnings:#?}");
}

#[test]
fn data_lint_does_not_let_opaque_suppress_a_schema_constraint() {
    let f = fixture();
    // archivedContacts is opaque, but Contact declares unique_keys: duplicates
    // still violate the class's claim. diff vocabulary never silences schema truth.
    let c = json!({"kind": "Emergency", "phone": "02/111.11.11"});
    let inst = f.load(json!({"name": "svc", "archivedContacts": [c.clone(), c]}));
    let warnings = lint_instance_identity(&inst);
    assert_eq!(warnings.len(), 1, "{warnings:#?}");
    assert_eq!(warnings[0].subject, vec!["archivedContacts".to_string()]);
}
```

(Reuse `e()`, `n()`, `phones()` builders from `diff_unique_keys.rs` — copy them in.)

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p linkml_runtime --test identity_lint`
Expected: FAIL to compile — module and variants don't exist.

- [ ] **Step 3: Implement the module**

`src/runtime/src/identity_lint.rs`:

```rust
//! Opt-in element-identity linter.
//!
//! Answers, per multivalued inlined slot: **where does element identity come
//! from?** A key (or identifier) on the element class, a composed key
//! declared with `unique_keys`, or the `diff.linkml.io/opaque` annotation
//! (nowhere — the slot's value is replaced as a whole). A slot with none of
//! these produces positional deltas, which are ambiguous when several sources
//! produce deltas for the same object concurrently.
//!
//! Neither entry point is wired into loading or [`crate::validate_issues`]:
//! projects that do not opt in keep today's inferred semantics.
//!
//! When a slot is flagged, the data-model author has four options (worked
//! examples in `docs/superpowers/specs/2026-08-17-inlined-multivalued-element-identity-design.md`):
//! 1. declare a `key`/`identifier` slot on the element class;
//! 2. declare the identity as `unique_keys` (composed keys supported);
//! 3. annotate the slot `diff.linkml.io/opaque` — replace the value as a whole;
//! 4. remodel, when one class would need two identity answers.

use crate::diff::{element_identity_label, slot_is_opaque, OPAQUE_ANNOTATION};
use crate::{
    LinkMLInstance, ValidationProblemType, ValidationResult, ValidationResultSink,
};
use linkml_schemaview::identifier::Identifier;
use linkml_schemaview::schemaview::SchemaView;
use std::collections::HashMap;

/// Schema-level lint: warn for every multivalued inlined slot whose element
/// identity comes from nowhere. Warnings only — the schema stays usable.
pub fn lint_element_identity(sv: &SchemaView) -> Vec<ValidationResult> {
    use linkml_schemaview::slotview::{SlotContainerMode, SlotInlineMode};
    let mut sink = ValidationResultSink::default();
    let conv = sv.converter();
    let mut class_ids = sv.get_class_ids();
    class_ids.sort();
    for class_id in class_ids {
        let Ok(Some(class)) = sv.get_class(&Identifier::new(&class_id), &conv) else {
            continue;
        };
        for slot in class.slots() {
            if slot.determine_slot_container_mode() != SlotContainerMode::List {
                continue;
            }
            if slot.determine_slot_inline_mode() == SlotInlineMode::Reference {
                continue; // elements are references, not inlined
            }
            if slot_is_opaque(slot) {
                continue; // identity declared: nowhere, replace as a whole
            }
            if let Some(rc) = slot.get_range_class() {
                if rc.key_or_identifier_slot().is_some() || !rc.unique_keys().is_empty() {
                    continue;
                }
            }
            sink.push_warning(
                ValidationProblemType::AmbiguousElementIdentity,
                vec![class.name().to_string(), slot.name.clone()],
                format!(
                    "elements of '{}.{}' have no declared identity: deltas are \
                     positional and ambiguous under multi-sourced operation. \
                     Declare a key/identifier or unique_keys on the element \
                     class, annotate the slot with {} to replace the value as \
                     a whole, or remodel.",
                    class.name(),
                    slot.name,
                    OPAQUE_ANNOTATION
                ),
            );
        }
    }
    sink.into_vec()
}

/// Data-level lint: warn for every list container whose elements repeat a
/// declared identity (key/identifier or unique_keys value).
///
/// Deliberately does NOT consult `diff.linkml.io/opaque`: a schema constraint
/// is class-level truth, and diff vocabulary never suppresses it.
pub fn lint_instance_identity(value: &LinkMLInstance) -> Vec<ValidationResult> {
    let mut sink = ValidationResultSink::default();
    let mut path = Vec::new();
    walk(value, &mut path, &mut sink);
    sink.into_vec()
}

fn walk(v: &LinkMLInstance, path: &mut Vec<String>, sink: &mut ValidationResultSink) {
    match v {
        LinkMLInstance::List { values, .. } => {
            check_duplicates(values, path, sink);
            for (i, child) in values.iter().enumerate() {
                path.push(i.to_string());
                walk(child, path, sink);
                path.pop();
            }
        }
        LinkMLInstance::Object { values, .. } | LinkMLInstance::Mapping { values, .. } => {
            for (k, child) in values {
                path.push(k.clone());
                walk(child, path, sink);
                path.pop();
            }
        }
        LinkMLInstance::Scalar { .. } | LinkMLInstance::Null { .. } => {}
    }
}

fn check_duplicates(
    values: &[LinkMLInstance],
    path: &[String],
    sink: &mut ValidationResultSink,
) {
    let mut seen: HashMap<String, usize> = HashMap::new();
    for v in values {
        if let Some(label) = element_identity_label(v) {
            *seen.entry(label).or_insert(0) += 1;
        }
    }
    let mut dups: Vec<(String, usize)> =
        seen.into_iter().filter(|(_, n)| *n > 1).collect();
    dups.sort();
    for (label, n) in dups {
        sink.push_warning(
            ValidationProblemType::DuplicateElementIdentity,
            path.to_vec(),
            format!(
                "{n} elements share the declared identity '{label}'; deltas \
                 addressing it are ambiguous"
            ),
        );
    }
}
```

In `src/runtime/src/lib.rs`:
- add the two `ValidationProblemType` variants,
- `pub mod identity_lint;`,
- `pub use identity_lint::{lint_element_identity, lint_instance_identity};`.

Then chase compile errors: every exhaustive `match` on `ValidationProblemType` (runtime, python crate) needs the two new arms — stringify them as `"ambiguous_element_identity"` / `"duplicate_element_identity"` following the existing naming style found at those match sites.

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p linkml_runtime --test identity_lint`
Expected: PASS (all 4).

- [ ] **Step 5: Workspace build + runtime suite**

Run: `cargo build --workspace && cargo test -p linkml_runtime`
Expected: PASS (workspace build catches the python-crate match arms).

- [ ] **Step 6: Commit**

```bash
git add src/runtime/src/identity_lint.rs src/runtime/src/lib.rs src/runtime/tests/identity_lint.rs src/python/src/lib.rs
git commit -m "feat(runtime): opt-in element-identity linter (schema + instance)"
```

---

### Task 7: Python bindings

The library's real consumer (consolidator-server) is Python. Expose the two lint functions; `diff`/`patch` signatures are unchanged, so no other binding moves.

**Files:**
- Modify: `src/python/src/lib.rs` (two new `#[pyfunction]`s + registration in the `_native` module at lines 739–756)
- Modify: `src/python/python/linkml_runtime_rust/_native.pyi` (two stubs)

**Interfaces:**
- Consumes: `linkml_runtime::{lint_element_identity, lint_instance_identity}` (Task 6), the existing `validation_results_to_py` helper (python lib.rs:1070) and `PyValidationResult`.
- Produces: Python functions `lint_element_identity(schema_view) -> list[ValidationResult]` and `lint_instance_identity(instance) -> list[ValidationResult]`.

- [ ] **Step 1: Implement the bindings**

Follow `py_diff` (registered at python lib.rs:745) for how the wrapper types expose their inner `SchemaView`/`LinkMLInstance` — use the same accessor pattern, then:

```rust
#[cfg_attr(feature = "stubgen", gen_stub_pyfunction)]
#[pyfunction]
#[pyo3(name = "lint_element_identity")]
fn py_lint_element_identity(
    py: Python<'_>,
    schema_view: &PySchemaView,
) -> PyResult<Vec<Py<PyValidationResult>>> {
    validation_results_to_py(
        py,
        linkml_runtime::lint_element_identity(schema_view_inner(schema_view)),
    )
}

#[cfg_attr(feature = "stubgen", gen_stub_pyfunction)]
#[pyfunction]
#[pyo3(name = "lint_instance_identity")]
fn py_lint_instance_identity(
    py: Python<'_>,
    instance: &PyLinkMLInstance,
) -> PyResult<Vec<Py<PyValidationResult>>> {
    validation_results_to_py(
        py,
        linkml_runtime::lint_instance_identity(instance_inner(instance)),
    )
}
```

(`schema_view_inner`/`instance_inner` stand for however `py_diff`/`py_patch` reach the wrapped Rust values — reuse that exact mechanism, whatever it is named; also mirror the `gen_stub_pyfunction` usage of neighbouring functions, including whether they gate it behind the `stubgen` feature.) Register both with `m.add_function(wrap_pyfunction!(...))?;` next to `py_diff`.

- [ ] **Step 2: Add the stubs**

In `_native.pyi`, next to the `diff`/`patch` stubs:

```python
def lint_element_identity(schema_view: SchemaView) -> list[ValidationResult]: ...
def lint_instance_identity(instance: LinkMLInstance) -> list[ValidationResult]: ...
```

(Match the actual parameter/class names used by the neighbouring stubs in that file.)

- [ ] **Step 3: Verify it compiles**

Run: `cargo check -p linkml_runtime_python`
Expected: clean. If the repo has a Python test suite under `src/python/`, also run it the way its README/CI does; otherwise compilation plus stub review is the gate here.

- [ ] **Step 4: Commit**

```bash
git add src/python/src/lib.rs src/python/python/linkml_runtime_rust/_native.pyi
git commit -m "feat(python): expose element-identity lint functions"
```

---

### Task 8: CLI flag on `linkml-schema-validate`

**Files:**
- Modify: `src/tools/src/bin/linkml_schema_validate.rs` (new `--lint-identity` flag)
- Modify: `src/tools/Cargo.toml` only if the tools crate does not already depend on `linkml_runtime` (the diff/patch bins suggest it does).

**Interfaces:**
- Consumes: `linkml_runtime::lint_element_identity` (Task 6).
- Produces: `linkml-schema-validate <schema> --lint-identity` prints one line per warning and (like warnings elsewhere in the tool) does not change the exit code.

- [ ] **Step 1: Implement**

Add to the `Args` struct:

```rust
    /// Opt-in: warn for multivalued inlined slots whose element identity
    /// comes from nowhere (positional, ambiguous deltas in multi-sourced use).
    #[arg(long, default_value_t = false)]
    lint_identity: bool,
```

After the existing validation logic completes (both output formats), run and print:

```rust
    if args.lint_identity {
        for w in linkml_runtime::lint_element_identity(&sv) {
            println!("warning[{}]: {}", w.subject.join("."), w.detail);
        }
    }
```

(Adapt the printing to the tool's existing `OutputFormat` handling: in `Json` mode, emit the warnings as a JSON array the same way existing results are serialized there — follow the surrounding code.)

- [ ] **Step 2: Verify manually against the fixture**

Run: `cargo run -p linkml_tools --bin linkml-schema-validate -- src/runtime/tests/data/identity.yaml --lint-identity`
Expected: exactly two warning lines — `Service.plainPhoneNumber` and `Service.tags` — plus whatever the normal validation prints. Then run without the flag and confirm no identity warnings appear (opt-in).

- [ ] **Step 3: Build check**

Run: `cargo build -p linkml_tools`
Expected: clean.

- [ ] **Step 4: Commit**

```bash
git add src/tools/src/bin/linkml_schema_validate.rs src/tools/Cargo.toml
git commit -m "feat(tools): --lint-identity flag on linkml-schema-validate"
```

---

### Task 9: docs polish + final verification

**Files:**
- Modify: `src/runtime/src/diff.rs` (rustdoc only: extend the `Delta` doc comment's path-segment sentence at lines 35–50 to mention unique_keys-derived segments and the composite JSON-array encoding; extend the `diff` doc comment at lines 103–111 with one paragraph on opaque slots)
- Modify: `src/runtime/src/identity_lint.rs` (confirm the module doc lists the four author options and links the spec — it is the authoring-guide surface the spec promises)

**Interfaces:** none — documentation and the final gate.

- [ ] **Step 1: Write the rustdoc additions**

On `Delta` (after the existing segment sentence, line 38–39):

```
/// For inlined lists whose element class declares `unique_keys`, the segment is
/// the unique-key value (single-slot keys), or the JSON array encoding of the
/// values in `unique_key_slots` order (composite keys), e.g.
/// `["Emergency","02/111.11.11"]`.
```

On `diff` (after the null/missing semantics list):

```
/// Slots annotated `diff.linkml.io/opaque` stop all recursion: any change at or
/// below the slot is described as a single whole-value `Update` at the slot
/// path. See [`OPAQUE_ANNOTATION`].
```

- [ ] **Step 2: Full workspace gate**

Run:

```bash
cargo fmt --all
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

Expected: all green. Fix anything clippy raises in the new code only.

- [ ] **Step 3: Spot-check the spec's headline scenarios end to end**

Run: `cargo test -p linkml_runtime --test diff_unique_keys --test diff_opaque --test identity_lint`
Expected: PASS — this is the spec's Example 1 (phones via unique_keys), Example 2 (opaque ring + bare Vertex + polygon unique key), and the linter contract.

- [ ] **Step 4: Commit**

```bash
git add src/runtime/src/diff.rs src/runtime/src/identity_lint.rs
git commit -m "docs(runtime): document opaque annotation and unique_keys path segments"
```
