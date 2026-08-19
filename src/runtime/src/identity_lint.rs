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
//!
//! A slot annotated `diff.linkml.io/ignore` is never flagged either, for a
//! different reason: diff skips such a slot entirely, so it produces no deltas
//! and has no element identity to declare. `ignore` silences the lint by
//! removing the slot from diff's scope; `opaque` silences it by answering the
//! question with "nowhere — replace the value as a whole".
//!
//! A second, narrower question is asked of slots that pass: **which** of the
//! range class's `unique_keys` provides the identity? Only the name-sorted
//! first entry does (declaration order is not preserved by the metamodel), so a
//! class declaring several has an alphabetically-decided identity and adding an
//! earlier-sorting entry silently re-addresses every delta path for every slot
//! ranged on it. That is warned about too, naming the load-bearing entry.
//!
//! A third rule catches a declared identity that cannot discriminate: a list
//! whose element class is keyed by its own type designator, whose value
//! describes the class rather than the element (the dict form of the same
//! class, meaning at-most-one-per-subtype, is left alone). The engine ignores
//! such a key entirely, so the slot also matches the first rule's shape (no
//! other identity is declared) or the second's (the `unique_keys` the key used
//! to shadow); this rule is the sharpest diagnosis and is asked first, so a
//! designator-keyed slot yields exactly one warning — this one.
//!
//! Warnings are reported at the class that **introduces** the slot: a flagged
//! slot inherited unchanged by a descendant is not repeated there, since the
//! declaration the author would edit lives on the ancestor.

use crate::diff::{
    element_identity_label, identity_key_slot, slot_is_ignored, slot_is_opaque, OPAQUE_ANNOTATION,
};
use crate::{LinkMLInstance, ValidationProblemType, ValidationResult, ValidationResultSink};
use linkml_schemaview::identifier::Identifier;
use linkml_schemaview::schemaview::{ClassView, SchemaView};
use linkml_schemaview::slotview::SlotView;
use std::collections::{BTreeMap, HashMap, HashSet};

/// Whether this is a multivalued inlined slot whose element identity comes from
/// nowhere — the engine's answer, from which the reporting loop subtracts the
/// designator case (see [`slot_is_identity_less_only`]).
///
/// Split out from the reporting loop because the same question has to be asked
/// of an inherited slot on its parent class, to decide which class introduced
/// the problem.
fn slot_lacks_element_identity(slot: &SlotView) -> bool {
    use linkml_schemaview::slotview::{SlotContainerMode, SlotInlineMode};
    if slot.determine_slot_container_mode() != SlotContainerMode::List {
        return false;
    }
    if slot.determine_slot_inline_mode() == SlotInlineMode::Reference {
        return false; // elements are references, not inlined
    }
    if slot_is_opaque(slot) {
        return false; // identity declared: nowhere, replace the value as a whole
    }
    if slot_is_ignored(slot) {
        return false; // outside diff's scope entirely: no deltas, no identity
    }
    if let Some(rc) = slot.get_range_class() {
        // The engine's notion of a key, not the metamodel's: a key that is the
        // class's type designator identifies the class, never the element, and
        // `element_identity_label` looks straight past it (spec addendum rule
        // 1). Such a class does lack element identity — but the designator rule
        // below diagnoses it more precisely and is its only voice, so the
        // reporting loop asks that question first.
        if identity_key_slot(&rc).is_some() || !rc.unique_keys().is_empty() {
            return false;
        }
    }
    true
}

/// The `unique_keys` entries a class offers as element identity, name-sorted.
///
/// An entry with no slots names nothing and can never be load-bearing, so it is
/// not a candidate and does not make an otherwise-single-entry class ambiguous.
fn identity_unique_key_names(rc: &ClassView) -> Vec<String> {
    rc.unique_keys()
        .into_iter()
        .filter(|(_, uk)| !uk.unique_key_slots.is_empty())
        .map(|(name, _)| name)
        .collect()
}

/// Whether this slot's element identity is *ambiguous* rather than absent: its
/// range class offers several `unique_keys` entries to derive it from, and only
/// the name-sorted first is load-bearing.
///
/// Returns the range class name and the candidate entry names (sorted, so the
/// load-bearing one is first). This flags slots the identity-less rule passes:
/// the identity exists, but which of the declarations provides it was decided
/// alphabetically rather than by the author.
///
/// A class with a `key`/`identifier` slot is not ambiguous however many
/// `unique_keys` it declares: the key outranks them all, so none of them is
/// load-bearing and adding one changes nothing. A key that is the class's type
/// designator outranks nothing — the engine looks past it — so such a class is
/// judged on its `unique_keys` like any other; the designator rule speaks for
/// it first regardless, since that is the defect worth reporting.
fn slot_has_ambiguous_unique_keys(slot: &SlotView) -> Option<(String, Vec<String>)> {
    use linkml_schemaview::slotview::{SlotContainerMode, SlotInlineMode};
    if slot.determine_slot_container_mode() != SlotContainerMode::List {
        return None;
    }
    if slot.determine_slot_inline_mode() == SlotInlineMode::Reference {
        return None; // elements are references, not inlined
    }
    if slot_is_opaque(slot) || slot_is_ignored(slot) {
        return None; // no per-element delta paths to re-address
    }
    let rc = slot.get_range_class()?;
    if identity_key_slot(&rc).is_some() {
        return None; // the key outranks unique_keys entirely
    }
    let names = identity_unique_key_names(&rc);
    if names.len() < 2 {
        return None;
    }
    Some((rc.name().to_string(), names))
}

/// Whether this slot's element identity, though declared, cannot discriminate
/// between the elements of a list: the range class's key (or identifier) is its
/// type designator, whose value is fixed per class.
///
/// Returns the range class name and the designator slot's name.
///
/// Only asked of the list form. The dict form of the same class is a different,
/// legitimate model — a mapping keyed by the designator says at-most-one
/// element per subtype — so it is deliberately left alone.
///
/// This is the sharpest of the three rules and speaks first: the engine ignores
/// a designator key outright, so the same slot also matches the identity-less
/// shape (when the class declares no `unique_keys`) or the several-`unique_keys`
/// shape (the entries the key used to shadow), and only this rule names the
/// declaration the author would actually edit.
fn slot_identity_is_type_designator(slot: &SlotView) -> Option<(String, String)> {
    use linkml_schemaview::slotview::{SlotContainerMode, SlotInlineMode};
    if slot.determine_slot_container_mode() != SlotContainerMode::List {
        return None; // the dict form keyed by the designator is legitimate
    }
    if slot.determine_slot_inline_mode() == SlotInlineMode::Reference {
        return None; // elements are references, not inlined
    }
    if slot_is_opaque(slot) || slot_is_ignored(slot) {
        return None; // no per-element identity is being claimed
    }
    let rc = slot.get_range_class()?;
    // Asked of the metamodel's key, not the engine's: this rule exists to
    // report the key `element_identity_label` deliberately looks past.
    let key = rc.key_or_identifier_slot()?;
    if key.definition().designates_type != Some(true) {
        return None;
    }
    Some((rc.name().to_string(), key.name.clone()))
}

/// The identity-less rule as the reporting loop applies it.
///
/// A designator-keyed class declaring no `unique_keys` does lack element
/// identity, but the designator rule diagnoses it precisely and is its only
/// voice. Subtracting that case here keeps the "flagged for the same reason"
/// contract of [`introduces_flagged_slot`] honest: a subclass whose `slot_usage`
/// swaps a designator-keyed range for a bare one is still judged on its own
/// merits.
fn slot_is_identity_less_only(slot: &SlotView) -> bool {
    slot_lacks_element_identity(slot) && slot_identity_is_type_designator(slot).is_none()
}

/// The warning text for a list whose identity is its element class's type
/// designator.
fn type_designator_identity_detail(
    class_name: &str,
    slot_name: &str,
    range_class: &str,
    designator: &str,
) -> String {
    format!(
        "elements of '{class_name}.{slot_name}' declare their identity as \
         '{range_class}.{designator}', which is the type designator \
         (designates_type). Its value is a function of the element's class, not \
         of the element: constant across a homogeneous list, and one value per \
         subtype across a polymorphic one. It is therefore never element \
         identity, and the diff engine ignores the key outright — the list is \
         addressed by the element class's unique_keys if it declares any and \
         those labels are unique within the list, positionally otherwise. \
         Declare an identity that varies per element: a \
         discriminating key/identifier or unique_keys on '{range_class}', or \
         range the list on a bare element class that does not declare the \
         designator as its key — or use the dict form instead, if \
         at-most-one-element-per-subtype is what the key really means."
    )
}

/// Whether `class` is where a flagged slot should be reported, rather than an
/// ancestor it merely inherits the problem from.
///
/// Answers no only when the direct `is_a` parent carries a slot of the same
/// name that is flagged *for the same reason*. Applied at every level this
/// leaves exactly the topmost flagged declarer, and it keeps a subclass whose
/// `slot_usage` changes the answer — narrowing the range to a keyed class, or
/// widening it away from one — judged on its own merits in both directions.
///
/// Only the `is_a` chain is walked. A slot arriving from a mixin is reported on
/// the class using the mixin as well as on the mixin itself: a mixin can be
/// applied to unrelated classes, so there is no single owning declaration to
/// point at, and chasing one is not worth the complexity.
fn introduces_flagged_slot<F>(class: &ClassView, slot_name: &str, flagged: F) -> bool
where
    F: Fn(&SlotView) -> bool,
{
    let Ok(Some(parent)) = class.parent_class() else {
        return true; // no is_a parent: this class is the declarer
    };
    let Some(parent_slot) = parent.slot(&Identifier::new(slot_name)) else {
        return true; // the parent does not have it: introduced here
    };
    !flagged(&parent_slot)
}

/// The warning text for a range class offering several `unique_keys`.
///
/// `names` is name-sorted, so `names[0]` is the load-bearing entry.
fn ambiguous_unique_keys_detail(
    class_name: &str,
    slot_name: &str,
    range_class: &str,
    names: &[String],
) -> String {
    let quoted: Vec<String> = names.iter().map(|n| format!("'{n}'")).collect();
    format!(
        "elements of '{}.{}' take their identity from the unique_keys of \
         element class '{}', which declares {}: {}. Only {} is load-bearing — \
         the metamodel does not preserve declaration order, so the name-sorted \
         first entry is used, and every delta path for this slot is addressed \
         by it. Adding an earlier-sorting entry silently re-addresses them all. \
         Keep one entry, or rename deliberately.",
        class_name,
        slot_name,
        range_class,
        names.len(),
        quoted.join(", "),
        quoted.first().map(String::as_str).unwrap_or("none"),
    )
}

/// Schema-level lint: warn for every multivalued inlined slot whose element
/// identity comes from nowhere; for every list whose identity is the range
/// class's type designator, which cannot tell the elements of a homogeneous
/// list apart; and for every one whose identity is derived from a class
/// offering more than one `unique_keys` entry to derive it from.
/// Warnings only — the schema stays usable.
pub fn lint_element_identity(sv: &SchemaView) -> Vec<ValidationResult> {
    let mut sink = ValidationResultSink::default();
    let conv = sv.converter();
    let mut class_ids = sv.get_class_ids();
    class_ids.sort();
    let mut seen: HashSet<(String, String)> = HashSet::new();
    for class_id in class_ids {
        let Ok(Some(class)) = sv.get_class(&Identifier::new(&class_id), &conv) else {
            continue;
        };
        // `get_class_ids` yields one id per class *URI*: a class declaring an
        // explicit `class_uri` is indexed under both that and its default URI,
        // so walking the ids naively reports each of its slots twice.
        //
        // Key the seen-set on (schema, name), which is unique per class by
        // construction. Keying on the class URI would be wrong in the other
        // direction: LinkML lets distinct classes declare the same `class_uri`
        // (meta.yaml's `Anything` and extensions.yaml's `AnyValue` both declare
        // `linkml:Any`), and that would silently drop the second class's
        // warnings — a false negative, worse in a lint than a duplicate.
        if !seen.insert((class.schema_id().to_string(), class.name().to_string())) {
            continue;
        }
        for slot in class.slots() {
            // Report at the class that introduces the slot: repeating an
            // inherited warning on every descendant buries the one declaration
            // the author would actually edit. Applies to all three rules below.
            // The designator rule speaks first, and alone. Since the engine
            // stopped accepting a designator as element identity, such a slot
            // also matches the identity-less shape or the several-unique_keys
            // shape, and one warning per slot means the sharpest one wins.
            if let Some((rc_name, designator)) = slot_identity_is_type_designator(slot) {
                if introduces_flagged_slot(&class, &slot.name, |s| {
                    slot_identity_is_type_designator(s).is_some()
                }) {
                    sink.push_warning(
                        ValidationProblemType::AmbiguousElementIdentity,
                        vec![class.name().to_string(), slot.name.clone()],
                        type_designator_identity_detail(
                            class.name(),
                            &slot.name,
                            &rc_name,
                            &designator,
                        ),
                    );
                }
                continue;
            }
            if !slot_is_identity_less_only(slot) {
                if let Some((rc_name, names)) = slot_has_ambiguous_unique_keys(slot) {
                    if introduces_flagged_slot(&class, &slot.name, |s| {
                        slot_has_ambiguous_unique_keys(s).is_some()
                    }) {
                        sink.push_warning(
                            ValidationProblemType::AmbiguousElementIdentity,
                            vec![class.name().to_string(), slot.name.clone()],
                            ambiguous_unique_keys_detail(
                                class.name(),
                                &slot.name,
                                &rc_name,
                                &names,
                            ),
                        );
                    }
                }
                continue;
            }
            if !introduces_flagged_slot(&class, &slot.name, slot_is_identity_less_only) {
                continue;
            }
            let range_class = slot.get_range_class();
            // The advice has to fit the range: a scalar- or enum-ranged slot has
            // no element class on which a key could be declared.
            let detail = match &range_class {
                Some(rc) => format!(
                    "elements of '{}.{}' have no declared identity: deltas are \
                     positional and ambiguous under multi-sourced operation. \
                     Declare a key/identifier or unique_keys on the element \
                     class '{}', annotate the slot with {} to replace the value \
                     as a whole, or remodel.",
                    class.name(),
                    slot.name,
                    rc.name(),
                    OPAQUE_ANNOTATION
                ),
                None => format!(
                    "elements of '{}.{}' have no declared identity: the range is \
                     not a class, so deltas can only be positional, and they are \
                     ambiguous under multi-sourced operation. Annotate the slot \
                     with {} to replace the value as a whole, or remodel the \
                     range into a class that declares a key/identifier or \
                     unique_keys.",
                    class.name(),
                    slot.name,
                    OPAQUE_ANNOTATION
                ),
            };
            sink.push_warning(
                ValidationProblemType::AmbiguousElementIdentity,
                vec![class.name().to_string(), slot.name.clone()],
                detail,
            );
        }
    }
    let mut warnings = sink.into_vec();
    // The classes are visited in sorted id order, but a class's own slots come
    // from `ClassView::slots()`, which is HashMap-backed, so the warnings for a
    // single class arrive in an order that varies between runs. Sort here, once,
    // so every consumer inherits a stable, diffable order.
    warnings.sort_by(|a, b| a.subject.cmp(&b.subject));
    warnings
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
            // Name-sorted, so the warning order is stable across runs: the
            // values are a `HashMap`, whose iteration order is not.
            let ordered: BTreeMap<&String, &LinkMLInstance> = values.iter().collect();
            for (k, child) in ordered {
                path.push(k.clone());
                walk(child, path, sink);
                path.pop();
            }
        }
        LinkMLInstance::Scalar { .. } | LinkMLInstance::Null { .. } => {}
    }
}

fn check_duplicates(values: &[LinkMLInstance], path: &[String], sink: &mut ValidationResultSink) {
    let mut seen: HashMap<String, usize> = HashMap::new();
    for v in values {
        if let Some(label) = element_identity_label(v) {
            *seen.entry(label).or_insert(0) += 1;
        }
    }
    let mut dups: Vec<(String, usize)> = seen.into_iter().filter(|(_, n)| *n > 1).collect();
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
