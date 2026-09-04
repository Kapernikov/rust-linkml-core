use crate::{LResult, LinkMLInstance, NodeId, ValidationResultSink};
use linkml_schemaview::{
    converter::Converter,
    identifier::Identifier,
    schemaview::{ClassView, SchemaView, SlotView},
};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::collections::hash_map::Entry;

const IGNORE_ANNOTATION: &str = "diff.linkml.io/ignore";

pub(crate) fn slot_is_ignored(slot: &SlotView) -> bool {
    if slot.definitions().is_empty() {
        return false;
    }
    slot.definition()
        .annotations
        .as_ref()
        .map(|a| a.contains_key(IGNORE_ANNOTATION))
        .unwrap_or(false)
}

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

/// One component of an element's identity label, canonicalised so that
/// identity compares meaning and not spelling (spec addendum rule 2, D6).
///
/// A slot whose range descends from `uri`/`uriorcurie` holds an IRI, and `ex:WGS84`
/// and `https://example.org/canon/WGS84` are the same IRI. Compared as raw
/// strings they are two identities: the same element re-spelled diffs as a
/// Remove + Add, `navigate_path` by the expansion misses the CURIE-spelled
/// element, and the instance lint calls a genuine duplicate unique.
///
/// The expansion lives here, in the one function every identity label is built
/// from, so all four resolve sites move together: diff emission (via
/// [`element_key_label`] / [`element_unique_key_label`]), [`resolve_list_segment`]
/// for `patch` and `navigate_path`, and the instance lint. That is what keeps
/// "the segments diff emits are exactly what the resolver computes" true by
/// construction rather than by three coincidences.
///
/// A bare name (no `:` at all) and a CURIE with an unregistered prefix are left
/// verbatim: `Identifier::to_uri` refuses them, and inventing an expansion
/// against the default prefix would rename identities the schema never claimed
/// were IRIs.
pub(crate) fn canonical_identity_component(raw: &str, slot: &SlotView) -> String {
    if !slot.is_range_iri() {
        return raw.to_string();
    }
    let Some(conv) = slot.sv.converter_for_schema(slot.schema_id()) else {
        return raw.to_string();
    };
    match Identifier::new(raw).to_uri(&conv) {
        Ok(uri) => uri.0,
        Err(_) => raw.to_string(),
    }
}

pub(crate) fn scalar_slot_string(
    values: &std::collections::HashMap<String, LinkMLInstance>,
    slot_name: &str,
) -> Option<String> {
    if let Some(LinkMLInstance::Scalar { value, slot, .. }) = values.get(slot_name) {
        return match value {
            JsonValue::String(s) => Some(canonical_identity_component(s, slot)),
            other => Some(other.to_string()),
        };
    }
    None
}

/// Are these two `ClassView`s the same class?
///
/// Schema-qualified name, not pointer equality: a `ClassView` is a derived,
/// freshly built view, so two views of one class are routinely distinct values,
/// and a name alone is only unique within its schema. This is the question
/// "did diff pair two objects of different classes?" (spec addendum rule 3),
/// which is about the *declaration* the element instantiates — deliberately
/// finer than [`crate::LinkMLInstance::equals`]'s canonical-URI comparison,
/// which two classes sharing a `class_uri` (spike D8) also satisfy.
fn class_identity_equal(a: &ClassView, b: &ClassView) -> bool {
    a.name() == b.name() && a.schema_id() == b.schema_id()
}

/// The class's key/identifier slot, when it can identify an *element*.
///
/// A key (or identifier) that is also the class's type designator is skipped: a
/// designator's value is a function of the element's *class*, not of the
/// element, so it is constant across any homogeneous list by construction and
/// says "one element per subtype" across a polymorphic one. Neither is element
/// identity. Identity then falls through to `unique_keys` — which such a class
/// may well declare, and which the designator key used to shadow — else the
/// list is positional. [`crate::lint_element_identity`] is the author-facing
/// voice for the same shape; this is the engine agreeing with it.
///
/// This governs *labelling* — how an element is addressed among its siblings —
/// and so is used by every resolve site (diff emission, `resolve_list_segment`
/// for patch and navigate, the instance lint). It is deliberately **not** used
/// by `diff`'s changed-key check, which asks a different question: see the
/// comment at `treat_changed_identifier_as_new_object`.
pub(crate) fn identity_key_slot(class: &ClassView) -> Option<&SlotView> {
    let slot = class.key_or_identifier_slot()?;
    if slot.definition().designates_type == Some(true) {
        return None;
    }
    Some(slot)
}

/// The key/identifier value identifying `v` among its list siblings, if any.
pub(crate) fn element_key_label(v: &LinkMLInstance) -> Option<String> {
    if let LinkMLInstance::Object { values, class, .. } = v {
        let id_slot = identity_key_slot(class)?;
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
            // Infallible JSON array encoding (the crate denies `expect`).
            JsonValue::Array(parts.into_iter().map(JsonValue::String).collect()).to_string()
        });
    }
    None
}

/// Identity for keyed list matching: a key/identifier slot outranks a
/// `unique_keys` claim — unless that key is the class's type designator, which
/// is never element identity (see [`identity_key_slot`]).
pub(crate) fn element_identity_label(v: &LinkMLInstance) -> Option<String> {
    element_key_label(v).or_else(|| element_unique_key_label(v))
}

/// The single slot `v`'s identity label was read from, when the label *is* one
/// scalar: a key/identifier, or a one-slot `unique_keys` entry. A composite
/// `unique_keys` entry encodes a JSON array and has no single source slot.
///
/// Mirrors [`element_identity_label`]'s precedence exactly, including its
/// fall-through when the key slot carries no value — the point is to name the
/// slot that produced the label, so that a path segment can be normalised the
/// same way the label was.
fn identity_label_slot(v: &LinkMLInstance) -> Option<&SlotView> {
    let LinkMLInstance::Object { values, class, .. } = v else {
        return None;
    };
    if let Some(slot) = identity_key_slot(class) {
        if scalar_slot_string(values, &slot.name).is_some() {
            return Some(slot);
        }
    }
    let uks = class.unique_keys();
    let (_, uk) = uks.iter().find(|(_, uk)| !uk.unique_key_slots.is_empty())?;
    let [only] = uk.unique_key_slots.as_slice() else {
        return None;
    };
    class.slots().iter().find(|s| s.name == *only)
}

/// Does the path segment `key` address the element `v`, whose identity label is
/// `label`?
///
/// The other half of spec addendum rule 2's "a curie and its expansion are one
/// identity". Labels are already IRI-expanded on the way out
/// ([`canonical_identity_component`]); a segment arriving from outside — a
/// stored delta, a hand-written patch, a caller's `navigate_path` — has been
/// through no such thing. Normalising it through the *same* slot the label came
/// from makes the comparison symmetric, so `ex:WGS84` addresses an element
/// whose label expanded to `https://example.org/canon/WGS84` and vice versa.
///
/// Segments diff itself emits already equal the label outright and never reach
/// the expansion.
fn segment_matches_label(v: &LinkMLInstance, label: Option<&str>, key: &str) -> bool {
    let Some(label) = label else {
        return false;
    };
    if label == key {
        return true;
    }
    match identity_label_slot(v) {
        Some(slot) => canonical_identity_component(key, slot) == label,
        None => false,
    }
}

fn labels_are_unique<F>(elements: &[LinkMLInstance], label: F) -> bool
where
    F: Fn(&LinkMLInstance) -> Option<String>,
{
    let mut seen = std::collections::HashSet::new();
    elements.iter().filter_map(label).all(|l| seen.insert(l))
}

/// Whether this one list is addressed by identity label: it is non-empty,
/// every element carries an identity label, and the labels are unique.
///
/// This is the predicate that decides how a list is *addressed*, asked of a
/// single list. `diff` needs the same answer of both sides at once (a keyed
/// match needs identity on both), but every consumer that has only one list in
/// front of it — `patch`'s segment resolver, `navigate_path`, and diff's
/// keyed-source fallback — must agree, or a path one of them emits is a path
/// another cannot resolve.
///
/// Derives labels lazily and stops at the first element that has none — the
/// answer for an unlabelled list is settled by its first bare element, however
/// long the list is. [`list_is_keyed_shaped_from_labels`] is the variant for a
/// caller that has already paid for the labels.
pub(crate) fn list_is_keyed_shaped(values: &[LinkMLInstance]) -> bool {
    !values.is_empty()
        && values.iter().all(|v| element_identity_label(v).is_some())
        && labels_are_unique(values, element_identity_label)
}

/// [`list_is_keyed_shaped`] for a caller that already has the labels.
///
/// The predicate itself, over labels rather than elements. Deriving a label is
/// not free — it walks the class's merged `unique_keys` and IRI-expands the
/// components — and [`resolve_list_segment`] needs both the labels and this
/// answer, so it must not pay for them twice.
fn list_is_keyed_shaped_from_labels(labels: &[Option<String>]) -> bool {
    let mut seen = std::collections::HashSet::new();
    !labels.is_empty()
        && labels
            .iter()
            .all(|l| matches!(l, Some(l) if seen.insert(l.as_str())))
}

/// Operation applied by a [`Delta`].
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DeltaOp {
    /// Insert a new value at the given path.
    Add,
    /// Remove the value at the given path (produces a missing entry).
    Remove,
    /// Update an existing value, including transitions to `null`.
    Update,
}

/// Semantic delta emitted by [`diff`] and consumed by [`patch`].
///
/// The `path` identifies the location within the instance tree. Each segment is a
/// slot name, mapping key, list index, or — for inlined objects in lists matched
/// by identity — the element's identity label: its identifier/key slot value, or
/// failing that a value derived from the range class's `unique_keys`. Lists whose
/// elements do not all carry a *unique* identity label are addressed by numeric
/// index instead. A key/identifier that is the class's type designator does not
/// count: it labels the class, not the element, so identity falls through to
/// `unique_keys` or to the index (see [`identity_key_slot`]).
///
/// For a `unique_keys`-derived segment, a single-slot key contributes the bare
/// value of that slot, while a composite key contributes the JSON array encoding
/// of the values in `unique_key_slots` order, e.g. `["Emergency","02/111.11.11"]`.
/// When the range class declares several `unique_keys`, the identity comes from
/// the name-sorted first entry with a non-empty slot list — the metamodel does
/// not preserve declaration order, and paths have to be stable. The hazard is
/// schema evolution: adding an earlier-sorting entry re-addresses every path for
/// that slot, so [`crate::lint_element_identity`] warns about any such class.
///
/// Operations are expressed jointly via [`Delta::op`], `old`, and `new`:
///
/// | `op` | `old` | `new` | Description |
/// | --- | --- | --- | --- |
/// | `Add` | `None` | `Some(value)` | Insert `value` into a list/mapping/object slot |
/// | `Remove` | `Some(value)` | `None` | Remove the addressed entry (value becomes missing) |
/// | `Update` | `Some(before)` | `Some(after)` | Replace an existing value; `after` may be `JsonValue::Null` |
///
/// Consumers that need additional semantics (e.g. fuzzy patching) can rely on the
/// explicit `op` instead of inferring behaviour from the optional payloads.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Delta {
    pub path: Vec<String>,
    pub op: DeltaOp,
    pub old: Option<JsonValue>,
    pub new: Option<JsonValue>,
}

#[derive(Clone, Copy, Debug)]
pub struct DiffOptions {
    /// When `false` (the default), entries present in the source but absent in
    /// the target are silently ignored for **object slots** and **mapping keys**.
    /// This supports partial-update semantics where the target only supplies the
    /// fields / keys it cares about.
    ///
    /// When `true`, every absent entry is treated as an explicit removal:
    /// - Object slots produce an `Update` delta with `new = null`.
    /// - Mapping keys produce a `Remove` delta.
    ///
    /// **Lists** are always treated as complete regardless of this flag: a
    /// shorter target list produces `Remove` deltas for the trailing source
    /// elements.
    ///
    /// **Note:** detecting a mapping key *rename* (delete old key + add new key)
    /// requires `treat_missing_as_null = true`, because the old key is absent
    /// from the target and would otherwise be silently ignored.
    pub treat_missing_as_null: bool,
    pub treat_changed_identifier_as_new_object: bool,
}

impl DiffOptions {
    /// Construct with an explicit value for [`DiffOptions::treat_missing_as_null`].
    ///
    /// This flag is intentionally required: its effect (partial-update vs
    /// target-is-authoritative) is easy to misread and was too important to
    /// hide behind a `Default`. [`DiffOptions::treat_changed_identifier_as_new_object`]
    /// is set to `true` — override via struct update syntax when needed:
    ///
    /// ```ignore
    /// DiffOptions {
    ///     treat_changed_identifier_as_new_object: false,
    ///     ..DiffOptions::new(true)
    /// }
    /// ```
    pub fn new(treat_missing_as_null: bool) -> Self {
        Self {
            treat_missing_as_null,
            treat_changed_identifier_as_new_object: true,
        }
    }
}

/// Compute a semantic diff between two LinkMLInstance trees.
///
/// Semantics of nulls and missing values:
/// - X → null: `Update` (old = X, new = null).
/// - null → X: `Update` (old = null, new = X).
/// - missing → X: `Add` (old = None, new = X).
/// - X → missing (object slot): ignored by default; `Update` to null when `treat_missing_as_null`.
/// - X → missing (mapping key): ignored by default; `Remove` when `treat_missing_as_null`.
/// - X → missing (list element): always `Remove` (lists are positional/complete).
///
/// Slots annotated `diff.linkml.io/opaque` stop all recursion: any change at or
/// below the slot is described as a single whole-value `Update` at the slot
/// path. See [`OPAQUE_ANNOTATION`].
///
/// Two paired objects of *different classes* are likewise one whole-element
/// `Update`, never a field-by-field recursion across the two class definitions:
/// the element did not change, it was replaced. "Different class" is the
/// schema-qualified name, but the `Update` is still suppressed when the two
/// objects compare equal, so two classes sharing a `class_uri` with identical
/// content emit nothing.
///
/// The qualification is by schema *id*, not by `SchemaView` instance: two views
/// built separately over the same schema qualify their classes identically and
/// diff as finely as ever. Where it bites is a genuinely cross-schema pairing —
/// one tree typed by `…/schema/v1`, the other by `…/schema/v2`, or by two
/// schemas that merely declare the same class name. Then no paired object is
/// ever "the same class" and every one of them coarsens to a whole-element
/// `Update`: correct, and patchable where the two schemas still agree about the
/// element's shape. Where they genuinely disagree, the `Update`'s payload will
/// not build against the source schema and lands in [`PatchTrace::failed`] —
/// still an improvement on the field-level recursion this replaced, which
/// produced deltas that could not apply *and* had no single path to report.
///
/// Lists are matched by element identity when both sides carry unique identity
/// labels, and positionally otherwise — with one exception: when the *source*
/// list alone is label-addressed (the target repeats or lacks a label), the
/// change is described as a single whole-value `Update` at the list's path.
/// `patch` addresses a label-addressed list by label only, so positional
/// segments aimed at one could never be applied.
pub fn diff(source: &LinkMLInstance, target: &LinkMLInstance, opts: DiffOptions) -> Vec<Delta> {
    fn inner(
        path: &mut Vec<String>,
        slot: Option<&SlotView>,
        s: &LinkMLInstance,
        t: &LinkMLInstance,
        opts: DiffOptions,
        out: &mut Vec<Delta>,
    ) {
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
        match (s, t) {
            (
                LinkMLInstance::Object {
                    values: sm,
                    class: sc,
                    ..
                },
                LinkMLInstance::Object {
                    values: tm,
                    class: tc,
                    ..
                },
            ) => {
                // Spec addendum rule 3 (D2a): the two sides are different
                // *kinds* of thing, so the change is one whole-element
                // replacement — never field recursion across two classes.
                // Recursing produced deltas describing one class's slots on
                // the other's element (`thread` on a `Nut`), which no builder
                // can apply: the diff was unpatchable by construction.
                //
                // Guarded by `equals` so the invariant "objects the crate
                // considers equal produce no delta" survives: `equals` compares
                // class identity by canonical URI, so two *differently named*
                // classes sharing a `class_uri` (spike D8) with matching
                // assignments stay silent, exactly as before.
                //
                // Not governed by `treat_changed_identifier_as_new_object`:
                // that flag chooses how to describe a changed *key* within one
                // class, while a cross-class recursion is unpatchable whatever
                // the caller prefers.
                if !class_identity_equal(sc, tc) {
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
                // If objects have an identifier or key slot and it changed, treat as whole-object replacement
                // This applies for single-valued and list-valued inlined objects.
                if opts.treat_changed_identifier_as_new_object {
                    // Deliberately the metamodel's key, not [`identity_key_slot`]:
                    // this asks "is this still the same thing?", not "how is it
                    // labelled among its siblings".
                    //
                    // Rule 3 above now owns the *designator* case: a designator
                    // value is canonicalised at load (rule 2), so a changed
                    // designator means a changed class and the whole-element
                    // Update has already been emitted. What is left for this
                    // branch — and why it stays — is a changed key value within
                    // ONE class: same class, different element.
                    //
                    // The comparison is the canonical one (rule 2), through
                    // [`scalar_slot_string`] — the very function identity
                    // labels are built from. Raw strings were the rule's last
                    // unconverted site: a `uri`-ranged key respelled
                    // curie↔uri makes ONE identity label, so the list above
                    // matched the two elements as one element, and this branch
                    // then called that element replaced — a whole-element
                    // `Update` at a path that addresses it by the identity the
                    // branch had just declared changed. What the two questions
                    // disagree about is *which slot* to read (see above), never
                    // what makes two values of it the same value.
                    let key_slot_name = sc
                        .key_or_identifier_slot()
                        .or_else(|| tc.key_or_identifier_slot())
                        .map(|s| s.name.clone());
                    if let Some(ks) = key_slot_name {
                        if let (Some(s_id), Some(t_id)) =
                            (scalar_slot_string(sm, &ks), scalar_slot_string(tm, &ks))
                        {
                            if s_id != t_id {
                                out.push(Delta {
                                    path: path.clone(),
                                    op: DeltaOp::Update,
                                    old: Some(s.to_json()),
                                    new: Some(t.to_json()),
                                });
                                return;
                            }
                        }
                    }
                }
                for (k, sv) in sm {
                    let slot_view = sc
                        .slots()
                        .iter()
                        .find(|s| s.name == *k)
                        .or_else(|| tc.slots().iter().find(|s| s.name == *k));
                    path.push(k.clone());
                    match tm.get(k) {
                        Some(tv) => inner(path, slot_view, sv, tv, opts, out),
                        None => {
                            if !slot_view.is_some_and(slot_is_ignored) {
                                // Missing target slot: either ignore (default) or treat as update to null
                                if opts.treat_missing_as_null {
                                    out.push(Delta {
                                        path: path.clone(),
                                        op: DeltaOp::Update,
                                        old: Some(sv.to_json()),
                                        new: Some(JsonValue::Null),
                                    });
                                }
                            }
                        }
                    }
                    path.pop();
                }
                for (k, tv) in tm {
                    if !sm.contains_key(k) {
                        let slot_view = sc
                            .slots()
                            .iter()
                            .find(|s| s.name == *k)
                            .or_else(|| tc.slots().iter().find(|s| s.name == *k));
                        if !slot_view.is_some_and(slot_is_ignored) {
                            path.push(k.clone());
                            out.push(Delta {
                                path: path.clone(),
                                op: DeltaOp::Add,
                                old: None,
                                new: Some(tv.to_json()),
                            });
                            path.pop();
                        }
                    }
                }
            }
            (LinkMLInstance::List { values: sl, .. }, LinkMLInstance::List { values: tl, .. }) => {
                let identity = |v: &LinkMLInstance| -> Option<String> { element_identity_label(v) };
                // Uniform rule (spec, Non-goal section): keyed matching iff every
                // element on both sides carries an identity label and the labels
                // are unique within each side. Positional diff corrupts mid-list
                // removes/inserts into shifted Updates, and on patch the label
                // resolver can remove the wrong duplicate. Duplicate labels (a
                // list repeating a key, or data violating a unique_keys claim)
                // fall back to positional — matching duplicates by label would
                // silently collapse elements.
                let keyed = sl.iter().all(|v| identity(v).is_some())
                    && tl.iter().all(|v| identity(v).is_some())
                    && labels_are_unique(sl, identity)
                    && labels_are_unique(tl, identity);
                if keyed {
                    use std::collections::HashSet;
                    let src_ids: HashSet<String> = sl.iter().filter_map(&identity).collect();
                    let tgt_by_id: std::collections::HashMap<String, &LinkMLInstance> = tl
                        .iter()
                        .filter_map(|v| identity(v).map(|id| (id, v)))
                        .collect();
                    for sv in sl {
                        let Some(id) = identity(sv) else { continue };
                        path.push(id.clone());
                        match tgt_by_id.get(&id) {
                            Some(tv) => inner(path, None, sv, tv, opts, out),
                            None => out.push(Delta {
                                path: path.clone(),
                                op: DeltaOp::Remove,
                                old: Some(sv.to_json()),
                                new: None,
                            }),
                        }
                        path.pop();
                    }
                    for tv in tl {
                        let Some(id) = identity(tv) else { continue };
                        if !src_ids.contains(&id) {
                            path.push(id);
                            out.push(Delta {
                                path: path.clone(),
                                op: DeltaOp::Add,
                                old: None,
                                new: Some(tv.to_json()),
                            });
                            path.pop();
                        }
                    }
                } else if list_is_keyed_shaped(sl) {
                    // The source alone is keyed-shaped: `patch` resolves such a
                    // list by label ONLY, so positional segments aimed at it are
                    // unappliable by design and `patch(a, diff(a, b))` would
                    // refuse the very deltas we just emitted. What actually
                    // happened is honestly a whole-value change — this list
                    // stopped having coherent element identity — so say that,
                    // once, at the slot.
                    if !s.equals(t, opts.treat_missing_as_null) {
                        out.push(Delta {
                            path: path.clone(),
                            op: DeltaOp::Update,
                            old: Some(s.to_json()),
                            new: Some(t.to_json()),
                        });
                    }
                } else {
                    let max_len = std::cmp::max(sl.len(), tl.len());
                    for i in 0..max_len {
                        // Plain numeric segments only: a label that failed the
                        // keyed guard cannot address an element unambiguously.
                        path.push(i.to_string());
                        match (sl.get(i), tl.get(i)) {
                            (Some(sv), Some(tv)) => inner(path, None, sv, tv, opts, out),
                            (Some(sv), None) => out.push(Delta {
                                path: path.clone(),
                                op: DeltaOp::Remove,
                                old: Some(sv.to_json()),
                                new: None,
                            }),
                            (None, Some(tv)) => out.push(Delta {
                                path: path.clone(),
                                op: DeltaOp::Add,
                                old: None,
                                new: Some(tv.to_json()),
                            }),
                            (None, None) => {}
                        }
                        path.pop();
                    }
                }
            }
            (
                LinkMLInstance::Mapping { values: sm, .. },
                LinkMLInstance::Mapping { values: tm, .. },
            ) => {
                use std::collections::BTreeSet;
                let keys: BTreeSet<_> = sm.keys().chain(tm.keys()).cloned().collect();
                for k in keys {
                    path.push(k.clone());
                    match (sm.get(&k), tm.get(&k)) {
                        (Some(sv), Some(tv)) => inner(path, None, sv, tv, opts, out),
                        (Some(sv), None) => {
                            if opts.treat_missing_as_null {
                                out.push(Delta {
                                    path: path.clone(),
                                    op: DeltaOp::Remove,
                                    old: Some(sv.to_json()),
                                    new: None,
                                });
                            }
                        }
                        (None, Some(tv)) => out.push(Delta {
                            path: path.clone(),
                            op: DeltaOp::Add,
                            old: None,
                            new: Some(tv.to_json()),
                        }),
                        (None, None) => {}
                    }
                    path.pop();
                }
            }
            (LinkMLInstance::Null { .. }, LinkMLInstance::Null { .. }) => {}
            (LinkMLInstance::Null { .. }, tv) => {
                out.push(Delta {
                    path: path.clone(),
                    op: DeltaOp::Update,
                    old: Some(JsonValue::Null),
                    new: Some(tv.to_json()),
                });
            }
            (sv, LinkMLInstance::Null { .. }) => {
                out.push(Delta {
                    path: path.clone(),
                    op: DeltaOp::Update,
                    old: Some(sv.to_json()),
                    new: Some(JsonValue::Null),
                });
            }
            (sv, tv) => {
                let sj = sv.to_json();
                let tj = tv.to_json();
                if sj != tj {
                    out.push(Delta {
                        path: path.clone(),
                        op: DeltaOp::Update,
                        old: Some(sj),
                        new: Some(tj),
                    });
                }
            }
        }
    }
    let mut out = Vec::new();
    inner(&mut Vec::new(), None, source, target, opts, &mut out);
    out
}

#[derive(Debug, Clone, Default)]
pub struct PatchTrace {
    /// Node IDs of subtrees that were newly created by the patch.
    ///
    /// See [`crate::NodeId`] for semantics: these are internal, ephemeral IDs
    /// that are useful for tooling and provenance, not object identifiers.
    pub added: Vec<NodeId>,
    /// Node IDs of subtrees that were removed by the patch.
    pub deleted: Vec<NodeId>,
    /// Node IDs of nodes that were directly updated (e.g., parent containers, scalars).
    pub updated: Vec<NodeId>,
    /// Paths of deltas that could not be applied: an address that resolves to
    /// nothing or resolves ambiguously, a path descending below an opaque slot,
    /// or a payload that cannot be built at the location it addresses (spec
    /// addendum rule 4). Each such delta leaves the tree untouched and the rest
    /// of the batch still applies.
    pub failed: Vec<Vec<String>>,
}

#[derive(Clone, Copy, Debug)]
pub struct PatchOptions {
    pub ignore_no_ops: bool,
    pub treat_missing_as_null: bool,
}

impl Default for PatchOptions {
    fn default() -> Self {
        Self {
            ignore_no_ops: true,
            treat_missing_as_null: true,
        }
    }
}

/// Apply `deltas` to a clone of `source`, returning the result and a
/// [`PatchTrace`]. A delta whose path cannot be resolved is reported in
/// [`PatchTrace::failed`] rather than guessed at.
///
/// One bad delta never voids the batch (spec addendum rule 4): a delta whose
/// payload cannot be built at the location it addresses — a scalar where the
/// range is a class, a slot the resolved element's class does not declare — is
/// reported the same way, with the tree untouched, and the remaining deltas
/// still apply. Callers wanting "all or nothing" check `trace.failed` and
/// discard the result themselves.
///
/// With that, `patch` no longer fails: every way a delta can go wrong is a
/// `trace.failed` entry, and `LinkMLError` carries validation problems, not
/// infrastructure ones. The `LResult` return is retained for API stability —
/// treat an `Err` as unreachable rather than as the place to look for a
/// rejected delta.
///
/// **List segments are resolved against the list's CURRENT state**, as the
/// deltas are applied in order — not against a snapshot of the list the deltas
/// were produced from. On a list whose elements carry *duplicate* identity
/// labels this is observable: such a list is addressed numerically, but a delta
/// that removes the duplication flips it to identity-addressed mid-sequence,
/// and every numeric segment still queued in the same patch then resolves to
/// nothing and is reported in `failed`. The patch stops short; it never lands
/// an edit on a guessed element.
///
/// This is confined to lists whose element identity is already degenerate —
/// exactly what [`crate::lint_element_identity`] (the schema declares no
/// identity) and [`crate::lint_instance_identity`] (the data repeats one) exist
/// to flag. Declaring an identity, or `diff.linkml.io/opaque`, removes the
/// situation rather than working around it.
pub fn patch(
    source: &LinkMLInstance,
    deltas: &[Delta],
    opts: PatchOptions,
) -> LResult<(LinkMLInstance, PatchTrace)> {
    let mut out = source.clone();
    let mut trace = PatchTrace::default();
    for i in apply_order(deltas) {
        let d = &deltas[i];
        let applied = apply_delta_linkml(&mut out, d, &mut trace, opts)?;
        if !applied {
            trace.failed.push(d.path.clone());
        }
    }
    Ok((out, trace))
}

/// Return the indices of `deltas` in the order they should be applied.
///
/// The only reordering we perform is: `Remove` deltas that address a list
/// element by numeric index and share the same parent path are applied in
/// descending index order. This prevents earlier removes from shifting the
/// indices of later ones. All other deltas keep their original relative
/// position.
fn apply_order(deltas: &[Delta]) -> Vec<usize> {
    let mut order: Vec<usize> = (0..deltas.len()).collect();
    // Entries carry the numeric leaf so we never re-parse (and never unwrap).
    let mut groups: std::collections::HashMap<&[String], Vec<(usize, usize)>> = Default::default();
    for (pos, d) in deltas.iter().enumerate() {
        if d.op != DeltaOp::Remove {
            continue;
        }
        let Some(leaf) = d.path.last() else { continue };
        let Ok(idx) = leaf.parse::<usize>() else {
            continue;
        };
        let parent = &d.path[..d.path.len() - 1];
        groups.entry(parent).or_default().push((pos, idx));
    }
    for entries in groups.values() {
        if entries.len() < 2 {
            continue;
        }
        let mut reordered: Vec<(usize, usize)> = entries.clone();
        reordered.sort_by_key(|&(_, idx)| std::cmp::Reverse(idx));
        for ((slot, _), (new_delta_idx, _)) in entries.iter().zip(reordered) {
            order[*slot] = new_delta_idx;
        }
    }
    order
}

fn collect_all_ids(value: &LinkMLInstance, ids: &mut Vec<NodeId>) {
    ids.push(value.node_id());
    match value {
        LinkMLInstance::Scalar { .. } => {}
        LinkMLInstance::Null { .. } => {}
        LinkMLInstance::List { values, .. } => {
            for v in values {
                collect_all_ids(v, ids);
            }
        }
        LinkMLInstance::Mapping { values, .. } | LinkMLInstance::Object { values, .. } => {
            for v in values.values() {
                collect_all_ids(v, ids);
            }
        }
    }
}

fn mark_added_subtree(v: &LinkMLInstance, trace: &mut PatchTrace) {
    collect_all_ids(v, &mut trace.added);
}

fn mark_deleted_subtree(v: &LinkMLInstance, trace: &mut PatchTrace) {
    collect_all_ids(v, &mut trace.deleted);
}

fn with_converter<F>(
    schema_view: &SchemaView,
    value: JsonValue,
    builder: F,
) -> LResult<LinkMLInstance>
where
    F: FnOnce(JsonValue, &SchemaView, &Converter) -> LResult<LinkMLInstance>,
{
    let conv = schema_view.converter();
    builder(value, schema_view, &conv)
}

/// Build a delta's replacement value, turning a build failure into "this delta
/// did not apply" (spec addendum rule 4, D2b).
///
/// A delta carries a JSON payload that may be nonsense at the location it
/// addresses: a scalar where the slot's range is a class, a slot the resolved
/// element's class does not declare, an enum value outside the permissible set.
/// Propagating the builder's `Err` voided the entire batch — one stale or
/// hand-written delta and every other delta in the same patch was lost, with no
/// record of which one was at fault. The delta's path goes to
/// [`PatchTrace::failed`] instead and the tree is left untouched; `Err` from
/// `patch` is reserved for infrastructure failure.
///
/// Called before any mutation at every apply site, so "failed" always means
/// "nothing happened", never "half happened".
fn build_or_fail<F>(build: F) -> Option<LinkMLInstance>
where
    F: FnOnce() -> LResult<LinkMLInstance>,
{
    build().ok()
}

fn current_class_and_slot(current: &LinkMLInstance) -> (Option<ClassView>, Option<SlotView>) {
    match current {
        LinkMLInstance::Object { class, .. } => (Some(class.clone()), None),
        LinkMLInstance::List { class, slot, .. }
        | LinkMLInstance::Mapping { class, slot, .. }
        | LinkMLInstance::Scalar { class, slot, .. }
        | LinkMLInstance::Null { class, slot, .. } => (class.clone(), Some(slot.clone())),
    }
}

fn should_skip_update(
    old: &LinkMLInstance,
    new_child: &LinkMLInstance,
    opts: PatchOptions,
) -> bool {
    opts.ignore_no_ops && old.equals(new_child, opts.treat_missing_as_null)
}

fn should_skip_add_null(new_child: &LinkMLInstance, opts: PatchOptions) -> bool {
    opts.ignore_no_ops
        && opts.treat_missing_as_null
        && matches!(new_child, LinkMLInstance::Null { .. })
}

fn should_skip_remove_null(old_child: &LinkMLInstance, opts: PatchOptions) -> bool {
    opts.ignore_no_ops
        && opts.treat_missing_as_null
        && matches!(old_child, LinkMLInstance::Null { .. })
}

fn replace_child_subtree(
    target: &mut LinkMLInstance,
    new_child: LinkMLInstance,
    parent_id: NodeId,
    trace: &mut PatchTrace,
    mark_parent: bool,
) {
    let old_snapshot = std::mem::replace(target, new_child);
    mark_deleted_subtree(&old_snapshot, trace);
    mark_added_subtree(target, trace);
    if mark_parent {
        trace.updated.push(parent_id);
    }
}

/// Resolve one path segment against a list, to the index of the element it
/// addresses.
///
/// The single rule every consumer of a delta path shares — `patch` when it
/// applies one, [`crate::LinkMLInstance::navigate_path`] when it follows one.
/// Keeping it in one function is what makes "diff emits it, patch applies it,
/// navigate finds it" true by construction rather than by three coincidences.
pub(crate) fn resolve_list_segment(values: &[LinkMLInstance], key: &str) -> Option<usize> {
    // A list whose elements all carry unique identity labels is addressed by
    // label ONLY: diff emits label segments for exactly these lists, and a
    // numeric segment aimed at one (a stale positional patch) would be a
    // guess — report, never guess. This also keeps integer-valued identity
    // labels (e.g. a year as unique key) unambiguous: they resolve as
    // labels, never as positions.
    let labels: Vec<Option<String>> = values.iter().map(element_identity_label).collect();
    // Exact label equality first, everywhere. Two reasons, one line:
    // *correctness* — a segment that IS an element's label must address that
    // element, never a sibling whose differently-spelled label happens to
    // normalise to the same string (only reachable in a heterogeneous list,
    // where siblings draw their labels from different slots); and *cost* — the
    // normalising comparison re-derives the label slot per element, walking the
    // class's merged `unique_keys`, so the common case (segments diff emitted,
    // which equal the labels outright) should never pay for it.
    let exact = |from: usize| {
        labels[from..]
            .iter()
            .position(|l| l.as_deref() == Some(key))
            .map(|i| i + from)
    };
    let matches = |i: usize| segment_matches_label(&values[i], labels[i].as_deref(), key);
    // The one normalising hit, or nothing: two elements the segment could mean
    // is a question, not an answer, and this function reports rather than
    // guesses. Both branches below share it, so neither can drift into
    // resolving an ambiguity the other refuses.
    //
    // In the keyed branch the second hit is barely reachable: labels are unique
    // there, and two of them can only normalise to one string if they were
    // normalised by *different converters* — a heterogeneous list whose element
    // classes come from schemas that disagree about a prefix. It is left as a
    // refusal rather than an assertion precisely because it is reachable at all:
    // a `debug_assert` would turn "I cannot tell which element you mean" into a
    // crash.
    let unique_normalised = || {
        let mut hit: Option<usize> = None;
        for i in 0..values.len() {
            if matches(i) {
                if hit.is_some() {
                    return None;
                }
                hit = Some(i);
            }
        }
        hit
    };
    if list_is_keyed_shaped_from_labels(&labels) {
        // Labels are unique here, so an exact hit is the only exact hit.
        return exact(0).or_else(unique_normalised);
    }
    // Positional list: numeric index first (the segments diff produces for
    // these lists), then a single unambiguous label hit for drift tolerance.
    if let Ok(idx) = key.parse::<usize>() {
        if idx < values.len() {
            return Some(idx);
        }
    }
    if let Some(first) = exact(0) {
        // Ambiguity is refused, not guessed at — but only exact hits compete
        // with an exact hit.
        return match exact(first + 1) {
            Some(_) => None,
            None => Some(first),
        };
    }
    unique_normalised()
}

fn try_update_scalar_in_place(
    existing: &mut LinkMLInstance,
    new_child: &LinkMLInstance,
    trace: &mut PatchTrace,
) -> bool {
    if let LinkMLInstance::Scalar {
        value: old_value,
        node_id,
        ..
    } = existing
    {
        if let LinkMLInstance::Scalar {
            value: new_value, ..
        } = new_child
        {
            *old_value = new_value.clone();
            trace.updated.push(*node_id);
            return true;
        }
    }
    false
}

#[derive(Clone, Copy)]
struct HashmapDeltaConfig {
    allow_scalar_in_place: bool,
    skip_add_null: bool,
    skip_remove_null: bool,
}

const OBJECT_DELTA_CONFIG: HashmapDeltaConfig = HashmapDeltaConfig {
    allow_scalar_in_place: true,
    skip_add_null: true,
    skip_remove_null: false,
};

const MAPPING_DELTA_CONFIG: HashmapDeltaConfig = HashmapDeltaConfig {
    allow_scalar_in_place: false,
    skip_add_null: false,
    skip_remove_null: true,
};

#[allow(clippy::too_many_arguments)]
fn apply_hashmap_leaf_delta<F>(
    values: &mut std::collections::HashMap<String, LinkMLInstance>,
    key: &str,
    owner_id: NodeId,
    trace: &mut PatchTrace,
    opts: PatchOptions,
    op: &DeltaOp,
    build_child: F,
    config: HashmapDeltaConfig,
) -> LResult<bool>
where
    F: FnOnce() -> LResult<LinkMLInstance>,
{
    match op {
        DeltaOp::Add | DeltaOp::Update => {
            let Some(new_child) = build_or_fail(build_child) else {
                return Ok(false);
            };
            match values.entry(key.to_string()) {
                Entry::Occupied(mut entry) => {
                    let existing = entry.get_mut();
                    if should_skip_update(existing, &new_child, opts) {
                        return Ok(true);
                    }
                    if config.allow_scalar_in_place
                        && try_update_scalar_in_place(existing, &new_child, trace)
                    {
                        return Ok(true);
                    }
                    replace_child_subtree(existing, new_child, owner_id, trace, false);
                    Ok(true)
                }
                Entry::Vacant(entry) => {
                    if config.skip_add_null && should_skip_add_null(&new_child, opts) {
                        return Ok(true);
                    }
                    mark_added_subtree(&new_child, trace);
                    entry.insert(new_child);
                    trace.updated.push(owner_id);
                    Ok(true)
                }
            }
        }
        DeltaOp::Remove => {
            if let Some(old_child) = values.get(key) {
                if config.skip_remove_null && should_skip_remove_null(old_child, opts) {
                    return Ok(true);
                }
            }
            if let Some(old_child) = values.remove(key) {
                mark_deleted_subtree(&old_child, trace);
                trace.updated.push(owner_id);
                Ok(true)
            } else {
                Ok(false)
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn apply_list_leaf_delta<F>(
    values: &mut Vec<LinkMLInstance>,
    idx_opt: Option<usize>,
    key: &str,
    owner_id: NodeId,
    trace: &mut PatchTrace,
    opts: PatchOptions,
    op: &DeltaOp,
    build_child: F,
) -> LResult<bool>
where
    F: FnOnce() -> LResult<LinkMLInstance>,
{
    match op {
        DeltaOp::Add | DeltaOp::Update => {
            // An `Update` whose address resolves to no element is either a
            // source re-asserting an element some other source dropped — the
            // multi-source case, where appending is the intended merge — or a
            // stale address, where appending would invent an element. The
            // payload's own identity decides which: it must name the element
            // the path addresses. `Add` always appends.
            if idx_opt.is_none() && matches!(op, DeltaOp::Update) {
                // Build failure means the value could never be applied
                // anyway: report the path instead of erroring the patch.
                let Some(new_child) = build_or_fail(build_child) else {
                    return Ok(false);
                };
                let allowed = match element_identity_label(&new_child) {
                    // Identity present: only the address that names it may
                    // append. Blocks stale positional Updates into a keyed
                    // list, which used to overwrite the wrong element. The
                    // comparison is the resolver's, so an address that WOULD
                    // have resolved to this element had it still been there
                    // is the address that may re-add it — a segment spelled
                    // as a CURIE against an IRI-expanded label included.
                    Some(label) => segment_matches_label(&new_child, Some(&label), key),
                    // No identity to check (scalar element, unkeyed range):
                    // only a positional or emptied list may grow this way.
                    None => !list_is_keyed_shaped(values),
                };
                if !allowed {
                    return Ok(false);
                }
                mark_added_subtree(&new_child, trace);
                values.push(new_child);
                trace.updated.push(owner_id);
                return Ok(true);
            }
            let Some(new_child) = build_or_fail(build_child) else {
                return Ok(false);
            };
            if let Some(idx) = idx_opt {
                let existing = &mut values[idx];
                if should_skip_update(existing, &new_child, opts) {
                    return Ok(true);
                }
                if try_update_scalar_in_place(existing, &new_child, trace) {
                    return Ok(true);
                }
                replace_child_subtree(existing, new_child, owner_id, trace, false);
                Ok(true)
            } else {
                mark_added_subtree(&new_child, trace);
                values.push(new_child);
                trace.updated.push(owner_id);
                Ok(true)
            }
        }
        DeltaOp::Remove => {
            if let Some(idx) = idx_opt {
                let old_child = values.remove(idx);
                mark_deleted_subtree(&old_child, trace);
                trace.updated.push(owner_id);
                Ok(true)
            } else {
                Ok(false)
            }
        }
    }
}

fn apply_delta_linkml(
    current: &mut LinkMLInstance,
    delta: &Delta,
    trace: &mut PatchTrace,
    opts: PatchOptions,
) -> LResult<bool> {
    apply_delta_linkml_inner(
        current,
        &delta.path,
        &delta.op,
        delta.new.as_ref(),
        trace,
        opts,
    )
}

fn apply_delta_linkml_inner(
    current: &mut LinkMLInstance,
    path: &[String],
    op: &DeltaOp,
    newv: Option<&JsonValue>,
    trace: &mut PatchTrace,
    opts: PatchOptions,
) -> LResult<bool> {
    let schema_view = current.schema_view().clone();
    if path.is_empty() {
        return apply_delta_root(current, op, newv, trace, opts, &schema_view);
    }

    match current {
        LinkMLInstance::Object {
            values,
            class,
            node_id,
            ..
        } => apply_delta_object(
            values,
            class,
            *node_id,
            &schema_view,
            path,
            op,
            newv,
            trace,
            opts,
        ),
        LinkMLInstance::Mapping {
            values,
            slot,
            node_id,
            ..
        } => apply_delta_mapping(
            values,
            slot,
            *node_id,
            &schema_view,
            path,
            op,
            newv,
            trace,
            opts,
        ),
        LinkMLInstance::List {
            values,
            slot,
            class,
            node_id,
            ..
        } => apply_delta_list(
            values,
            slot,
            class,
            *node_id,
            &schema_view,
            path,
            op,
            newv,
            trace,
            opts,
        ),
        LinkMLInstance::Scalar { .. } | LinkMLInstance::Null { .. } => Ok(false),
    }
}

fn apply_delta_root(
    current: &mut LinkMLInstance,
    op: &DeltaOp,
    newv: Option<&JsonValue>,
    trace: &mut PatchTrace,
    opts: PatchOptions,
    schema_view: &SchemaView,
) -> LResult<bool> {
    match op {
        DeltaOp::Add => {
            let v = newv.cloned().unwrap_or(JsonValue::Null);
            let (class_opt, slot_opt) = current_class_and_slot(current);
            if let Some(cls) = class_opt {
                let slot_clone = slot_opt.clone();
                let Some(new_node) = build_or_fail(|| {
                    with_converter(schema_view, v, move |value, sv, conv| {
                        LinkMLInstance::from_json(value, cls, slot_clone, sv, conv, false)
                            .into_instance_tolerate_errors()
                    })
                }) else {
                    return Ok(false);
                };
                mark_added_subtree(&new_node, trace);
                *current = new_node;
                Ok(true)
            } else {
                Ok(false)
            }
        }
        DeltaOp::Remove => Ok(false),
        DeltaOp::Update => {
            if let Some(v) = newv.cloned() {
                let (class_opt, slot_opt) = current_class_and_slot(current);
                if let Some(cls) = class_opt {
                    let slot_clone = slot_opt.clone();
                    let Some(new_node) = build_or_fail(|| {
                        with_converter(schema_view, v, move |value, sv, conv| {
                            LinkMLInstance::from_json(value, cls, slot_clone, sv, conv, false)
                                .into_instance_tolerate_errors()
                        })
                    }) else {
                        return Ok(false);
                    };
                    if should_skip_update(current, &new_node, opts) {
                        return Ok(true);
                    }
                    mark_deleted_subtree(current, trace);
                    mark_added_subtree(&new_node, trace);
                    *current = new_node;
                    return Ok(true);
                }
            }
            Ok(false)
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn apply_delta_object(
    values: &mut std::collections::HashMap<String, LinkMLInstance>,
    class: &ClassView,
    owner_id: NodeId,
    schema_view: &SchemaView,
    path: &[String],
    op: &DeltaOp,
    newv: Option<&JsonValue>,
    trace: &mut PatchTrace,
    opts: PatchOptions,
) -> LResult<bool> {
    let key = &path[0];
    if path.len() == 1 {
        let value = newv.cloned().unwrap_or(JsonValue::Null);
        let slot = class.slots().iter().find(|s| s.name == *key).cloned();
        let class_clone = class.clone();
        let slot_clone = slot.clone();
        return apply_hashmap_leaf_delta(
            values,
            key,
            owner_id,
            trace,
            opts,
            op,
            || {
                with_converter(schema_view, value, move |val, sv, conv| {
                    LinkMLInstance::from_json(val, class_clone, slot_clone, sv, conv, false)
                        .into_instance_tolerate_errors()
                })
            },
            OBJECT_DELTA_CONFIG,
        );
    }
    if let Some(child) = values.get_mut(key) {
        let slot = class.slots().iter().find(|s| s.name == *key);
        if slot.is_some_and(slot_is_opaque) {
            // The path descends below an opaque slot: it addresses structure
            // the slot does not expose. Report, never guess.
            return Ok(false);
        }
        return apply_delta_linkml_inner(child, &path[1..], op, newv, trace, opts);
    }
    Ok(false)
}

#[allow(clippy::too_many_arguments)]
fn apply_delta_mapping(
    values: &mut std::collections::HashMap<String, LinkMLInstance>,
    slot: &SlotView,
    owner_id: NodeId,
    schema_view: &SchemaView,
    path: &[String],
    op: &DeltaOp,
    newv: Option<&JsonValue>,
    trace: &mut PatchTrace,
    opts: PatchOptions,
) -> LResult<bool> {
    // A non-empty path here addresses *inside* the mapping, which is below the
    // mapping's own slot. Reached when the patched root is itself a mapping.
    if slot_is_opaque(slot) {
        return Ok(false);
    }
    let key = &path[0];
    if path.len() == 1 {
        let value = newv.cloned().unwrap_or(JsonValue::Null);
        let slot_clone = slot.clone();
        // The delta's own path segment is the entry's dict key, and rule 5
        // makes that key the element's key-slot value: a delta that adds an
        // entry must build the same object the loader would have built for the
        // same key.
        let key_clone = key.clone();
        return apply_hashmap_leaf_delta(
            values,
            key,
            owner_id,
            trace,
            opts,
            op,
            || {
                with_converter(schema_view, value, move |val, sv, conv| {
                    let mut diags = ValidationResultSink::default();
                    let value = LinkMLInstance::build_mapping_entry_for_slot(
                        &slot_clone,
                        &key_clone,
                        val,
                        sv,
                        conv,
                        Vec::new(),
                        &mut diags,
                    )?;
                    // Diagnostics recorded in `diags` are intentionally ignored here so patching
                    // can proceed even if validation failures were observed.
                    Ok(value)
                })
            },
            MAPPING_DELTA_CONFIG,
        );
    }
    if let Some(child) = values.get_mut(key) {
        return apply_delta_linkml_inner(child, &path[1..], op, newv, trace, opts);
    }
    Ok(false)
}

#[allow(clippy::too_many_arguments)]
fn apply_delta_list(
    values: &mut Vec<LinkMLInstance>,
    slot: &SlotView,
    class: &Option<ClassView>,
    owner_id: NodeId,
    schema_view: &SchemaView,
    path: &[String],
    op: &DeltaOp,
    newv: Option<&JsonValue>,
    trace: &mut PatchTrace,
    opts: PatchOptions,
) -> LResult<bool> {
    // A non-empty path here addresses *inside* the list, which is below the
    // list's own slot. Reached when the patched root is itself a list.
    if slot_is_opaque(slot) {
        return Ok(false);
    }
    let key = &path[0];
    let idx_opt = resolve_list_segment(values, key);
    if path.len() == 1 {
        let value = newv.cloned().unwrap_or(JsonValue::Null);
        let slot_clone = slot.clone();
        let class_clone = class.clone();
        return apply_list_leaf_delta(values, idx_opt, key, owner_id, trace, opts, op, || {
            with_converter(schema_view, value, move |val, sv, conv| {
                let mut diags = ValidationResultSink::default();
                let value = LinkMLInstance::build_list_item_for_slot(
                    &slot_clone,
                    class_clone.as_ref(),
                    val,
                    sv,
                    conv,
                    Vec::new(),
                    &mut diags,
                )?;
                // Diagnostics recorded in `diags` are intentionally ignored here so patching
                // can proceed even if validation failures were observed.
                Ok(value)
            })
        });
    }
    if let Some(idx) = idx_opt {
        return apply_delta_linkml_inner(&mut values[idx], &path[1..], op, newv, trace, opts);
    }
    Ok(false)
}
