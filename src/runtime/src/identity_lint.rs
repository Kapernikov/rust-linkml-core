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
//! Warnings are reported at the class that **introduces** the slot: a flagged
//! slot inherited unchanged by a descendant is not repeated there, since the
//! declaration the author would edit lives on the ancestor.

use crate::diff::{element_identity_label, slot_is_ignored, slot_is_opaque, OPAQUE_ANNOTATION};
use crate::{LinkMLInstance, ValidationProblemType, ValidationResult, ValidationResultSink};
use linkml_schemaview::identifier::Identifier;
use linkml_schemaview::schemaview::{ClassView, SchemaView};
use linkml_schemaview::slotview::SlotView;
use std::collections::{BTreeMap, HashMap, HashSet};

/// Whether this slot is one the lint flags: a multivalued inlined slot whose
/// element identity comes from nowhere.
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
        if rc.key_or_identifier_slot().is_some() || !rc.unique_keys().is_empty() {
            return false;
        }
    }
    true
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
fn introduces_flagged_slot(class: &ClassView, slot_name: &str) -> bool {
    let Ok(Some(parent)) = class.parent_class() else {
        return true; // no is_a parent: this class is the declarer
    };
    let Some(parent_slot) = parent.slot(&Identifier::new(slot_name)) else {
        return true; // the parent does not have it: introduced here
    };
    !slot_lacks_element_identity(&parent_slot)
}

/// Schema-level lint: warn for every multivalued inlined slot whose element
/// identity comes from nowhere. Warnings only — the schema stays usable.
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
            if !slot_lacks_element_identity(slot) {
                continue;
            }
            // Report at the class that introduces the slot: repeating an
            // inherited warning on every descendant buries the one declaration
            // the author would actually edit.
            if !introduces_flagged_slot(&class, &slot.name) {
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
