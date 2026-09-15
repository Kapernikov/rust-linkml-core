# A delta does not record whether its list segments mean key or position

**Date:** 2026-09-14
**Reported as:** `#400` (pepibru GitLab issue, `asset360/consolidator-server`) — "diff() switches between positional and key addressing on a keyed list, with no signal in the delta"
**Reported against:** `asset360-rust` 0.8.8
**Branch:** `fix/keyed-list-addressing-is-schema-shaped`
**Status:** implemented in `79e1e2f` — *fix(runtime): a list whose class declares an identity never gets positional segments*

---

## This reverses a decision, it does not fix an oversight

The behaviour below was chosen deliberately and was asserted by three passing tests with written rationale. Those tests encode the "Non-goal" section of
`docs/superpowers/specs/2026-08-17-inlined-multivalued-element-identity-design.md`:

> *"[keyed matching is uniform:] a list is matched by element identity only when every element on both sides yields an identity label ... and the labels are unique within each side. ... In every other case matching is positional and path segments are plain numeric indices. This removes two behaviours of the old fallback: key values were opportunistically mixed into positional paths, and **duplicate key values within a "keyed" list were silently collapsed by the matcher**."*

That was right about **collapsing**. Field evidence said it was wrong about **falling back to positional**: the fallback produced a delta that was silently re-interpreted against a different base. The change keeps the anti-collapse guarantee and removes the positional fallback for classes that declare an identity.

## Goal

Make the answer to *"is this list addressed by key or by position?"* a function of **the schema** (shared by everyone) instead of a function of **the data in the base in front of you** (different for every caller). A stored delta then means one thing everywhere it is replayed.

## The defect in one table

`list_is_keyed_shaped` (`src/runtime/src/diff.rs:243`) is the single predicate every consumer shares — `diff` emission, `resolve_list_segment` for `patch`, `navigate_path`, `list_path_segments` for the blame walker. It is purely data-driven:

```rust
pub(crate) fn list_is_keyed_shaped(values: &[LinkMLInstance]) -> bool {
    !values.is_empty()
        && values.iter().all(|v| element_identity_label(v).is_some())
        && labels_are_unique(values, element_identity_label)   // <- data, not schema
}
```

| range class declares identity | data honours it | before | after |
|---|---|---|---|
| no | — | positional | positional (unchanged — stable, the schema is shared) |
| yes | yes | keyed | keyed (unchanged) |
| **yes** | **no — duplicate labels** | **silently positional** | **one whole-slot `Update`** |

The third row was the whole bug. Nothing in the emitted delta recorded which of the two addressings was meant, so the delta was only replayable against the exact base it was computed from.

### Both directions were silently wrong

`resolve_list_segment` (`src/runtime/src/diff.rs:1014`):

- **keyed-shaped base**: label only. Segment `"0"` *exactly equals* the label of the element keyed `0`, so it lands on it and reports success.
- **non-keyed-shaped base**: `key.parse::<usize>()` is tried **first**. A key segment `"6"` on a 7+-element list with duplicate labels lands on **position 6** and reports success.

The engine already documented this hazard verbatim, on `list_path_segment` (`:1105`):

> *"position space and label space overlap: a list of rows keyed by a sequence number counting from 1 answers position `"k"` with the row labelled `k`, one place earlier, and reports it as a successful update."*

It had been solved for **walkers that name elements** (`list_path_segments`). It was never solved for **stored deltas**, because when that design was written nothing in the model said a stored delta was a thing. It now is: consolidator-server harvests deltas against a source snapshot and replays them against golden records weeks later.

### Field evidence

`#399` (pepibru GitLab issue, the `Picture.uri` UNC-backslash fix). `Picture` is keyed on `sequence`, a small integer — so label space and position space are *the same strings*. Deltas were harvested against a snapshot whose `sequence` repeated on 1308 parents, so they came out positional. Replayed against golden records that had since been renumbered, 456 of 1309 changes produced wrong picture lists — elements duplicated and dropped — with **no error anywhere**:

```
want: [1, 2, 3a, 4b, 5c, 6d]
got:  [6d, 5c, 4b, 4b, 5c, 6d]
```

Worked around by discarding the stored deltas and rewriting each to a single whole-slot `update ['pictures']` — which is what the engine now does by itself.

Original reproduction from the issue, for provenance (asset360 schema `v1.0.0`, class `CivilEngineeringAsset`):

```python
import json, asset360_rust as lr
from asset360_model_api.schema import get_latest_schema
sv = get_latest_schema()
cv = sv.get_class_view("https://data.infrabel.be/asset360/CivilEngineeringAsset")

def pic(seq, name):
    return {"uri": "\\\\HOST\\share\\%s.jpg" % name, "sequence": seq,
            "isMain": False, "isOriginal": True, "description": "d"}
def box(p):
    return lr.load_json(json.dumps({"id": "http://example/CEAsset/1", "pictures": p}), sv, cv)[0]
def show(b):
    return [(p["sequence"], p["uri"][-5:]) for p in b.as_python()["pictures"]]

A = box([pic(0, "e"), pic(0, "y"), pic(5, "z")])   # base repeats sequence 0
B = box([pic(0, "y"), pic(1, "e"), pic(2, "z")])
d = lr.diff(A, B, treat_missing_as_null=True, treat_changed_identifier_as_new_object=True)
C = box([pic(0, "e"), pic(1, "y"), pic(2, "z")])   # a DIFFERENT base, keys unique
res = lr.patch(C, d)
print([(x.op, x.path) for x in d])
print(show(C), "->", show(res.value), "failed:", list(res.trace.failed))
```

`src/runtime/tests/data/identity.yaml` reproduces it without asset360 — see the tests below.

---

## What changed

One surgical change, no signature change, no delta-format change, no data migration.

**`diff` stops emitting positional segments into an identity-declaring list.** The existing whole-slot-`Update` fallback at `diff.rs:659` widened from "the *source data* is keyed-shaped" to "the *source data* is keyed-shaped **or the slot's range class declares an element identity**":

```rust
} else if list_is_keyed_shaped(sl) || slot_declares_element_identity(s_slot) {
```

The `(List, List)` arm binds the **source** side's own `slot` field for this; `inner`'s `slot: Option<&SlotView>` parameter is `None` when the patched root is itself a list and cannot be used.

The new predicate, next to `list_is_keyed_shaped` at `diff.rs:273`:

```rust
pub(crate) fn slot_declares_element_identity(slot: &SlotView) -> bool {
    crate::identity_lint::slot_addresses_elements_by_position_or_label(slot)
        && slot.get_range_class().is_some_and(|rc| {
            identity_key_slot(&rc).is_some() || rc.has_any_unique_key()
        })
}
```

`slot_addresses_elements_by_position_or_label` (`identity_lint.rs:135`, promoted to `pub(crate)`) is what keeps reference lists, dict-shaped slots and slots annotated `diff.linkml.io/opaque` or `diff.linkml.io/ignore` out. The `&&` matters: `slot_lacks_element_identity` returns `false` for those, so a plain negation would wrongly include them.

Exactly one branch moved:

| source | target | branch before | branch after |
|---|---|---|---|
| keyed, unique | keyed, unique | 1 (keyed) | 1 — unchanged |
| keyed, unique | duplicates | 2 (whole-slot) | 2 — unchanged |
| duplicates | keyed, unique | **3 (positional)** | **2 (whole-slot)** ← the fix |
| no declared identity | no declared identity | 3 (positional) | 3 — unchanged |

Empty lists are unaffected: `list_is_keyed_shaped` is `false` for empty, but `keyed` at `diff.rs:621` is vacuously true when one side is empty and the other is labelled+unique, so branch 1 already owned the empty cases and an empty source into an identity-declaring slot still emits label-addressed `Add`s.

### `ClassView::has_any_unique_key`

The predicate is O(1) per list node, but it asks the range class a question that walked the `is_a`/mixin graph on every call: +0.3µs per positional list, ~+29% on the smallest such diff. It is memoised on the view as `ClassView::has_any_unique_key` (`src/schemaview/src/classview.rs:691`), which removes that entirely — the unchanged paths are neutral and the branch that moved (declared identity, duplicate labels) is ~16% faster, since it no longer walks the list at all. Covered by `src/schemaview/tests/unique_keys.rs` (`has_any_unique_key_agrees_with_unique_keys`, including a cached second call).

## Tests

All in `src/runtime/tests/diff_unique_keys.rs`.

Flipped — each keeps its scenario and now asserts one whole-slot `Update` at the slot path plus a `patch(a, diff(a,b))` round-trip with an empty `trace.failed`:

- `duplicate_unique_key_data_falls_back_to_positional` asserted `["hasPhoneNumber", "1", "phoneNumber"]` with *"positional fallback must use numeric segments, never the duplicate label"* → `duplicate_unique_key_data_is_one_whole_slot_update` (`:126`).
- `duplicate_key_data_falls_back_to_positional_not_collapse` (`labelList`, `Label` keyed on `lang`) asserted `["labelList", "1", "text"]` → `duplicate_key_data_is_one_whole_slot_update_not_collapse` (`:159`). The anti-collapse point is pinned by an explicit assertion that both elements survive: two elements sharing a label are still never merged, they are simply not addressed individually.
- `duplicated_source_to_keyed_target_stays_positional_and_round_trips` — issue #400's exact scenario, previously asserted as intended — → `duplicated_source_to_keyed_target_is_one_whole_slot_update` (`:501`). Its old comment (*"the source is NOT keyed-shaped, so numeric segments are exactly what patch resolves against it"*) is sound for `patch(a, diff(a,b))` and false for `patch(c, diff(a,b))`, which is what production does; the comment now says so.

Unchanged, and the guard that this did not swallow the legitimate positional case:

- `undeclared_class_keeps_positional_cascade` (`:195`) — `PlainPhoneNumber` declares no `unique_keys`.
- `designator_keyed_class_without_unique_keys_is_positional` (`:772`) and `polymorphic_designator_keyed_list_is_positional` (`:797`) — a type designator is not element identity (`identity_key_slot` skips it), so `slot_declares_element_identity` answers `false` for those.
- The keyed-base refusals `patch_refuses_positional_segment_into_identity_addressed_list` (`:313`), `patch_refuses_ambiguous_duplicate_key_labels` (`:336`) and `patch_refuses_positional_update_into_identity_addressed_list` (`:384`).

New: `duplicate_labels_delta_replayed_against_a_renumbered_base` (`:549`) — the cross-base regression the suite never had, `A (duplicate labels) --diff--> B` then `patch(C, that delta)` where `C` holds the same elements with unique labels. Every other round-trip test patches against its own diff base. Against the previous commit it "passed" by quietly editing position 0 instead of position 1, with an empty `trace.failed`.

## Design constraints this respected

- **"Report, never guess"** (spec): a patch that cannot locate its target unambiguously returns `Ok(false)` so the path lands in `PatchTrace::failed`. No fuzzy fallbacks.
- **The signature of `diff` did not change.** It is `pub fn diff(source, target, opts) -> Vec<Delta>` (`diff.rs:448`). A diagnostics channel would ripple through PyO3, wasm, the `linkml-diff` CLI and every Python caller — and the plan that introduced this subsystem states the identity lints are deliberately **not wired into default validation**. The data-side lint already exists and is already exposed: `lint_instance_identity` → `check_duplicates` (`identity_lint.rs:836`, `:931`) pushes `ValidationProblemType::DuplicateElementIdentity` with *"N elements share the declared identity 'X'; deltas addressing it are ambiguous"*.
- **The on-the-wire `Delta` shape did not change.** `Delta { path, op, old, new }` is persisted as JSONB in consolidator-server and read back through a `@dataclass` whose `from_dict` is `Delta(**dct)` — an unknown key raises. `PatchTrace::failed` stays `Vec<Vec<String>>`.

## Known limitation

`slot_declares_element_identity` inspects the slot's **declared** range class only, not its descendants. A polymorphic list ranged on a class that declares nothing, holding descendants that each declare `unique_keys`, answers "no declared identity" and stays positional. That mirrors `slot_lacks_element_identity`, which inspects the same thing, and keeps the two consistent. `identity_lint.rs:195` has `identity_class_family` (`rc` plus `get_descendants(true, false)`) for the widened question; using it is more correct and strictly widens the blast radius, so it is a separate decision for the schema authors.

## Rejected alternative: mark the addressing mode in the delta

Issue #400's first suggestion: *"make the addressing mode explicit in the delta"*. Rejected, because:

- A path is a *sequence* of segments crossing several lists, so it needs per-segment marking — a parallel `addressing: ["slot","key","slot"]` array, not a single flag. (Encoding it into the segment string instead, e.g. `"#3"` for position, breaks every string-matching consumer: consolidator-server's `remove_rejected_deltas` compares paths by list equality, the Angular form matches them by prefix, the blame `path_map` is keyed on them.)
- It buys **honesty, not recoverability**. A delta saying *"I meant position 2 of a list that no longer looks like that"* is still unappliable. It converts a wrong answer into an honest failure — which the change above also does, earlier and more cheaply, by never writing the bad delta in the first place.
- Every delta already in production is unmarked, so a default interpretation must be picked for the legacy corpus — and the default is wrong for exactly the rows that are broken.

Revisit only if a case turns up where positional addressing of an identity-bearing list is genuinely *wanted*. None is known.

## File map

| what | where |
|---|---|
| the data-side predicate | `src/runtime/src/diff.rs:243` `list_is_keyed_shaped`, `:290` `list_is_keyed_shaped_from_labels` |
| the schema-side predicate | `src/runtime/src/diff.rs:273` `slot_declares_element_identity` |
| the edit site | `src/runtime/src/diff.rs:659` |
| the list arm | `src/runtime/src/diff.rs:604`–`718` |
| segment resolution | `src/runtime/src/diff.rs:1014` `resolve_list_segment` |
| emission twin | `src/runtime/src/diff.rs:1105` `list_path_segment`, `:1122` `list_path_segments` |
| schema-side helpers | `src/runtime/src/identity_lint.rs:135`, `:150`, `:195` |
| the memoised existence check | `src/schemaview/src/classview.rs:691` `has_any_unique_key` |
| the existing data lint | `src/runtime/src/identity_lint.rs:836` `lint_instance_identity`, `:931` `check_duplicates` |
| tests | `src/runtime/tests/diff_unique_keys.rs` (fixture at `:18`, schema `src/runtime/tests/data/identity.yaml`), `src/schemaview/tests/unique_keys.rs` |
| the design being amended | `docs/superpowers/specs/2026-08-17-inlined-multivalued-element-identity-design.md`, "Non-goal" |
| the plan that built it | `docs/superpowers/plans/2026-08-18-inlined-multivalued-element-identity.md` |
