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
//! # The rules
//!
//! [`lint_element_identity`] asks five questions of a schema, and
//! [`lint_instance_identity`] two of a loaded instance. All seven warn; none
//! errors, and none changes what the engine does.
//!
//! ## Schema rules
//!
//! 1. **No declared identity.** A multivalued inlined list whose element class
//!    declares no key/identifier and no `unique_keys` (or whose range is not a
//!    class at all). Its deltas are positional, and positional deltas are
//!    ambiguous when several sources produce deltas for one object
//!    concurrently. This is the rule the four options above answer.
//! 2. **The identity is the type designator.** The element class's key (or
//!    identifier) is its own `designates_type` slot, whose value describes the
//!    class and not the element: constant across a homogeneous list, one value
//!    per subtype across a polymorphic one. The engine ignores such a key
//!    outright (spec addendum rule 1), so the slot also matches rule 1's shape
//!    (nothing else is declared) or rule 3's (the `unique_keys` the key used to
//!    shadow). This is the sharpest diagnosis and is asked first, so a
//!    designator-keyed slot yields exactly one warning — this one. The dict
//!    form of the same class is left alone: a mapping keyed by the designator
//!    legitimately says at-most-one-element-per-subtype.
//! 3. **Several `unique_keys` to choose from.** Which of them provides the
//!    identity? Only the name-sorted first does (declaration order is not
//!    preserved by the metamodel), so a class offering several has an
//!    alphabetically-decided identity, and adding an earlier-sorting entry
//!    silently re-addresses every delta path for every slot ranged on it. The
//!    candidates are counted across the range class **and every class
//!    descending from it**, since a list ranged on a class holds elements of
//!    all of them.
//! 4. **A split label space.** Those same classes are labelled *different
//!    ways*: `Gadget` elements by one `unique_keys` entry and
//!    `Widget is_a Gadget` elements by another, in one list — or one of them by
//!    a `key` and the other by an entry, which splits the space just as
//!    thoroughly. A path written against one label space cannot address an
//!    element of the other, and two elements labelled different ways can carry
//!    the same label without violating either class's uniqueness constraint.
//!    Rule 3 and this one ask different questions of the same family: rule 3 is
//!    about which *entry* is chosen, so a key-labelled class is outside it,
//!    while this one is about which *declaration* labels each element, so a
//!    key-labelled class is one of the groups.
//! 5. **A shared `class_uri` under a designator.** Two classes of one `is_a`
//!    hierarchy declare the same `class_uri` while the hierarchy designates its
//!    type. A designator value is a class URI, so it names both classes at
//!    once and the loader resolves it to one of them — stably, but by an
//!    ordering the schema does not state. Warning only: the loader's choice is
//!    deliberately unchanged.
//!
//! Slot warnings are reported at the class that **introduces** the slot: a
//! flagged slot inherited unchanged by a descendant is not repeated there,
//! since the declaration the author would edit lives on the ancestor. Each rule
//! gates on its own predicate, so a subclass whose `slot_usage` changes one
//! rule's answer is judged on its own merits for that rule — and each of rules
//! 1, 3 and 4 subtracts rule 2's cases from its gate, because a designator-keyed
//! range class matches all three raw shapes while rule 2 is the slot's only
//! voice: a parent that emitted no warning must not suppress a subclass's.
//! Rule 5 is class-level and has no slot to attribute, so it is emitted once per
//! (hierarchy, shared URI) instead.
//!
//! ## Instance rules
//!
//! 6. **Repeated identity.** Two elements of one list carry the same identity
//!    label: a delta addressing it cannot say which is meant.
//! 7. **Positional despite a declared identity.** Some element of an inlined
//!    list yields no label — the element class declares an identity, and the
//!    data leaves the slot it names empty — so the list is not keyed-shaped and
//!    is addressed positionally after all. Nothing here is invalid (a `key`
//!    that is not `required` may legitimately be absent), so no schema rule and
//!    no validation can see it; only the data can.
//!
//! Rule 6 does not consult `diff.linkml.io/opaque` or `ignore`: a repeated
//! identity contradicts the element class's own constraint, which is
//! class-level truth that diff vocabulary on the slot cannot suppress. Rule 7
//! does honour both, and skips reference lists, because it claims only that the
//! list is addressed positionally — a claim those slots make false rather than
//! excused.
//!
//! # Sharp edge, deliberately not its own rule
//!
//! A subclass may switch the designator off with
//! `slot_usage: { theSlot: { designates_type: false } }` while the slot stays
//! (or becomes) the class's `key`. The override is respected, so the designator
//! machinery stops filling the slot — and nothing else fills it either. The
//! class then declares a key that no element ever carries a value for, which is
//! not a schema defect the schema can be read for: the declaration is
//! well-formed, and only the data shows that the key is always empty. It
//! collapses into instance rule 7, which is where it is reported.

use crate::diff::{
    element_identity_label, identity_key_slot, slot_is_ignored, slot_is_opaque, OPAQUE_ANNOTATION,
};
use crate::{LinkMLInstance, ValidationProblemType, ValidationResult, ValidationResultSink};
use linkml_schemaview::identifier::Identifier;
use linkml_schemaview::schemaview::{ClassView, SchemaView};
use linkml_schemaview::slotview::SlotView;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

/// The shape every schema rule below is asked of: an inlined list whose
/// per-element delta paths are diff's to address, and which therefore has to
/// answer "where does element identity come from?".
///
/// A dict is addressed by its keys, a reference list by the referents' own
/// identifiers, an `opaque` slot by nothing (the value is replaced whole) and
/// an `ignore`d slot not at all. None of them can be mis-addressed by a
/// mis-declared element identity, so none of them is any rule's business.
fn slot_addresses_elements_by_position_or_label(slot: &SlotView) -> bool {
    use linkml_schemaview::slotview::{SlotContainerMode, SlotInlineMode};
    slot.determine_slot_container_mode() == SlotContainerMode::List
        && slot.determine_slot_inline_mode() != SlotInlineMode::Reference
        && !slot_is_opaque(slot)
        && !slot_is_ignored(slot)
}

/// Whether this is a multivalued inlined slot whose element identity comes from
/// nowhere — the engine's answer, from which the reporting loop subtracts the
/// designator case (see [`slot_is_identity_less_only`]).
///
/// Split out from the reporting loop because the same question has to be asked
/// of an inherited slot on its parent class, to decide which class introduced
/// the problem.
fn slot_lacks_element_identity(slot: &SlotView) -> bool {
    if !slot_addresses_elements_by_position_or_label(slot) {
        return false;
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

/// The classes whose `unique_keys` can label an element of a list ranged on
/// `rc`: `rc` itself, and every class descending from it.
///
/// A list ranged on a class holds elements of every class descending from it,
/// and each element is labelled by its **own** merged `unique_keys` — so both
/// `unique_keys` rules below are questions about the whole family, not about
/// the one declaration the slot's range happens to name (spike D4d).
///
/// Mixin users are excluded (`include_mixins: false`): applying a mixin does
/// not make a class an instance of it, so it cannot put that class into a list
/// ranged on the mixin.
///
/// A failure to resolve the descendants degrades to "no descendants" rather
/// than to a panic or a swallowed rule: the family always contains `rc`, so the
/// rules keep the pre-D4d answer instead of disappearing.
fn identity_class_family(rc: &ClassView) -> Vec<ClassView> {
    let mut family = vec![rc.clone()];
    if let Ok(descendants) = rc.get_descendants(true, false) {
        family.extend(descendants);
    }
    family
}

/// The `unique_keys` entry this class's elements are actually labelled by:
/// the name-sorted first entry that names any slots, mirroring
/// `crate::diff::element_unique_key_label`.
///
/// `None` when the class's identity does not come from `unique_keys` at all —
/// either it has a (non-designator) key/identifier, which outranks every entry,
/// or it declares no usable entry and its elements go unlabelled.
fn load_bearing_unique_key(rc: &ClassView) -> Option<String> {
    if identity_key_slot(rc).is_some() {
        return None; // the key outranks unique_keys entirely
    }
    identity_unique_key_names(rc).into_iter().next()
}

/// Whether this slot's element identity is *ambiguous* rather than absent:
/// the range class and its descendants offer several `unique_keys` entries to
/// derive it from, and only the name-sorted first is load-bearing.
///
/// Returns the range class name, the name-sorted candidate entries, and the one
/// the range class *itself* resolves to — which is the first candidate unless a
/// descendant declares an earlier-sorting entry, the split case the divergence
/// rule is the voice for. This flags slots the identity-less rule passes: the
/// identity exists, but which of the declarations provides it was decided
/// alphabetically rather than by the author.
///
/// The candidates are unioned over [`identity_class_family`], because an entry
/// a descendant adds is an entry some element of the list is really labelled by
/// (spike D4d). Family members whose identity is a key are skipped: their
/// entries are never load-bearing, so adding one to them re-addresses nothing.
///
/// A range class with a `key`/`identifier` slot is not ambiguous however many
/// `unique_keys` it declares, for the same reason. A key that is the class's
/// type designator outranks nothing — the engine looks past it — so such a
/// class is judged on its `unique_keys` like any other; the designator rule
/// speaks for it first regardless, since that is the defect worth reporting.
fn slot_has_ambiguous_unique_keys(slot: &SlotView) -> Option<(String, Vec<String>, String)> {
    if !slot_addresses_elements_by_position_or_label(slot) {
        return None;
    }
    let rc = slot.get_range_class()?;
    // The range class's own answer. `None` means its elements carry no label at
    // all, which is the identity-less rule's business, not this one's.
    let own = load_bearing_unique_key(&rc)?;
    let mut names: BTreeSet<String> = BTreeSet::new();
    for cv in identity_class_family(&rc) {
        if identity_key_slot(&cv).is_some() {
            continue;
        }
        names.extend(identity_unique_key_names(&cv));
    }
    if names.len() < 2 {
        return None;
    }
    Some((rc.name().to_string(), names.into_iter().collect(), own))
}

/// How one class labels its elements, as the divergence rule compares it: the
/// rendered description of the declaration `element_identity_label` reads.
///
/// `None` for a class whose elements carry no label at all — that is the
/// identity-less rule's business, and an unlabelled class does not occupy a
/// label space to be split from anyone.
///
/// A key and a `unique_keys` entry are *different* labellings even when they
/// read the same slot, so they are rendered differently and never collide as
/// group names. This mirrors `element_identity_label`'s precedence exactly: the
/// key first, the name-sorted first entry otherwise.
fn identity_labelling(rc: &ClassView) -> Option<String> {
    if let Some(key) = identity_key_slot(rc) {
        let kind = if key.definition().identifier == Some(true) {
            "identifier"
        } else {
            "key"
        };
        return Some(format!("{kind} '{}'", key.name));
    }
    let entry = identity_unique_key_names(rc).into_iter().next()?;
    Some(format!("unique_keys entry '{entry}'"))
}

/// Per distinct identity labelling, the classes of one range class's family
/// that resolve to it. Labelling-sorted, each class list name-sorted.
type LabelSpaceGroups = Vec<(String, Vec<String>)>;

/// Whether the classes a list ranged on this slot can hold resolve **different**
/// identity labellings: one list, two label spaces (spike D4b/d).
///
/// Returns the range class name and, per distinct labelling, the name-sorted
/// classes resolving to it.
///
/// This is a sharper defect than the several-entries one and is reported *in
/// addition* to it. `Gadget{gadget_identity:[code]}` with
/// `Widget is_a Gadget{aaa_widget_identity:[serial]}` labels its `Gadget`
/// elements by `code` and its `Widget` elements by `serial`, in one list:
/// navigating by a base-class label never finds a `Widget`, and a `Widget`
/// serial colliding with a `Gadget` code produces two elements with one label
/// while violating neither class's constraint. Namespacing labels by the entry
/// that produced them is the deep fix and is out of scope for this branch, so
/// the author is told instead.
///
/// A family member whose identity is a `key` is counted as its own group, not
/// skipped. Its `unique_keys` entries are indeed never load-bearing — which is
/// why the several-entries rule ignores them — but the key itself labels its
/// elements, and it splits the list's label space exactly as a second entry
/// would: `Plate` elements addressed by a `plate_identity` value and
/// `StampedPlate` elements by a `stampId` collide across the two spaces without
/// breaking either class's constraint. The two rules ask different questions of
/// the same family, and only one of them is about entry *choice*.
fn slot_has_split_identity_label_space(slot: &SlotView) -> Option<(String, LabelSpaceGroups)> {
    if !slot_addresses_elements_by_position_or_label(slot) {
        return None;
    }
    let rc = slot.get_range_class()?;
    let mut by_labelling: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for cv in identity_class_family(&rc) {
        if let Some(labelling) = identity_labelling(&cv) {
            by_labelling
                .entry(labelling)
                .or_default()
                .push(cv.name().to_string());
        }
    }
    if by_labelling.len() < 2 {
        return None;
    }
    let mut groups: LabelSpaceGroups = by_labelling.into_iter().collect();
    for (_, classes) in groups.iter_mut() {
        classes.sort();
        classes.dedup();
    }
    Some((rc.name().to_string(), groups))
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
    // Includes "not the dict form": a mapping keyed by the designator is
    // legitimate, and says at-most-one-element-per-subtype.
    if !slot_addresses_elements_by_position_or_label(slot) {
        return None;
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

/// The several-entries rule as the reporting loop applies it.
///
/// Same subtraction, same reason as [`slot_is_identity_less_only`], and needed
/// for the same reason it is: the designator key stopped shadowing its class's
/// `unique_keys` (spec addendum rule 1), so a designator-keyed range class with
/// several entries satisfies the raw shape while the designator rule remains
/// the slot's only voice.
///
/// Used only as [`introduces_flagged_slot`]'s predicate, where the omission
/// loses a real warning: a parent whose slot fires the designator rule would
/// otherwise suppress a subclass that `slot_usage`-retargets the slot onto a
/// keyless multi-entry class, and the ambiguity would be reported nowhere. The
/// reporting loop's own call site does not need it — a designator-keyed slot
/// has already `continue`d by then.
fn slot_has_ambiguous_unique_keys_only(slot: &SlotView) -> bool {
    slot_has_ambiguous_unique_keys(slot).is_some()
        && slot_identity_is_type_designator(slot).is_none()
}

/// The divergence rule as the reporting loop applies it, subtracted exactly as
/// [`slot_has_ambiguous_unique_keys_only`] is and for the same reason.
fn slot_has_split_identity_label_space_only(slot: &SlotView) -> bool {
    slot_has_split_identity_label_space(slot).is_some()
        && slot_identity_is_type_designator(slot).is_none()
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

/// The warning text for a range class family offering several `unique_keys`.
///
/// `names` is the name-sorted union over the family; `own` is the entry the
/// range class itself resolves to, which is `names[0]` unless a descendant
/// declares an earlier-sorting entry — the split case, which the divergence
/// warning is the voice for.
fn ambiguous_unique_keys_detail(
    class_name: &str,
    slot_name: &str,
    range_class: &str,
    names: &[String],
    own: &str,
) -> String {
    let quoted: Vec<String> = names.iter().map(|n| format!("'{n}'")).collect();
    format!(
        "elements of '{}.{}' take their identity from the unique_keys of \
         element class '{}' and of the classes descending from it, which \
         between them declare {} candidate entries: {}. Only one of them can be \
         load-bearing for a given element — the metamodel does not preserve \
         declaration order, so the name-sorted first entry that element's own \
         class offers is used, which for '{}' itself is '{}'. Every delta path \
         for this slot is addressed by it, and adding an earlier-sorting entry \
         anywhere in the family silently re-addresses them. Keep one entry, or \
         rename deliberately.",
        class_name,
        slot_name,
        range_class,
        names.len(),
        quoted.join(", "),
        range_class,
        own,
    )
}

/// The warning text for a range class family whose members resolve different
/// identity labellings.
///
/// `groups` is labelling-sorted, and each group's classes are name-sorted, so
/// the text is stable across runs.
fn split_label_space_detail(
    class_name: &str,
    slot_name: &str,
    range_class: &str,
    groups: &[(String, Vec<String>)],
) -> String {
    let described: Vec<String> = groups
        .iter()
        .map(|(labelling, classes)| {
            // Bounded: a wide hierarchy must not turn one warning into a wall.
            // Three names name the split; the rest are counted.
            let shown: Vec<String> = classes.iter().take(3).map(|c| format!("'{c}'")).collect();
            let rest = classes.len().saturating_sub(shown.len());
            let more = if rest > 0 {
                format!(" and {rest} more")
            } else {
                String::new()
            };
            format!("{labelling} ({}{})", shown.join(", "), more)
        })
        .collect();
    format!(
        "elements of '{}.{}' do not share one identity label space: a list \
         ranged on '{}' holds elements of every class descending from it, and \
         they are labelled {} different ways — {}. Each element is labelled by \
         the declaration its own class resolves to, so a delta path written \
         against one of them cannot address an element of another, and two \
         elements labelled different ways can produce the same label without \
         either class's uniqueness constraint being violated. Declare the \
         identity once, on '{}', so every element of the list is labelled the \
         same way.",
        class_name,
        slot_name,
        range_class,
        groups.len(),
        described.join("; "),
        range_class,
    )
}

/// How deep the linter walks an `is_a` chain looking for a hierarchy root.
///
/// A valid schema's `is_a` graph is a tree, so the bound is never reached; it
/// exists so that a cyclic one fails schema validation rather than hanging the
/// linter that was asked to explain it.
const MAX_IS_A_DEPTH: usize = 100;

/// The topmost `is_a` ancestor of `class` — the class that names its hierarchy.
fn hierarchy_root(class: &ClassView) -> ClassView {
    let mut current = class.clone();
    for _ in 0..MAX_IS_A_DEPTH {
        match current.parent_class() {
            Ok(Some(parent)) => current = parent,
            _ => break,
        }
    }
    current
}

/// The warning text for two classes of one hierarchy sharing a `class_uri`.
fn shared_class_uri_detail(root: &str, uri: &str, classes: &[String], designator: &str) -> String {
    let quoted: Vec<String> = classes.iter().map(|c| format!("'{c}'")).collect();
    format!(
        "{} declare the same class_uri '{}' and belong to one is_a hierarchy, \
         rooted at '{}', which designates its type through '{}' \
         (designates_type). A designator value is a class URI, so '{}' names \
         every one of them at once: the loader resolves it to a single class, \
         stably but by an ordering the schema does not state and nothing \
         promises to keep. Instances meaning any of the others load as the \
         winner without a diagnostic, and diff then pairs elements of different \
         classes as if they were one. Give each class a distinct class_uri. The \
         loader's choice is deliberately unchanged — this is a warning about \
         the declarations, not a behavioural fix.",
        quoted.join(" and "),
        uri,
        root,
        designator,
        uri,
    )
}

/// The warning text for a list addressed positionally although its element
/// class declares an identity: some element leaves the identity slot empty.
fn missing_labels_detail(
    missing: usize,
    total: usize,
    range_class: &str,
    identity: &str,
) -> String {
    format!(
        "{missing} of {total} elements of this list carry no identity label, \
         although their element class '{range_class}' declares one ({identity}). \
         A list is addressed by identity only when every element yields a label, \
         so this one is addressed positionally — ambiguous under multi-sourced \
         operation, and silently so, since an optional identity slot left empty \
         is valid data. Fill the identity slot on every element, make it \
         required, or declare the slot {OPAQUE_ANNOTATION} if the value is meant \
         to be replaced as a whole."
    )
}

/// How the range class declares element identity, for the warning above.
///
/// The same question [`identity_labelling`] answers for the divergence rule —
/// "which declaration does `element_identity_label` read?" — so it is the same
/// function, and `None` means the same thing in both: no identity is declared.
fn declared_identity_description(rc: &ClassView) -> Option<String> {
    Some(format!("its {}", identity_labelling(rc)?))
}

/// Schema-level, class-level rule: two classes of one `is_a` hierarchy declare
/// the same `class_uri`, and the hierarchy carries a type designator (spike D8).
///
/// Unlike the slot rules, this one has no "introducing class" to report at:
/// there is no slot, and the defect belongs to the pair of declarations rather
/// than to either of them. Emitting once per (hierarchy, shared URI) buys the
/// same thing [`introduces_flagged_slot`] buys the others — one warning per
/// thing the author would edit — so no gate predicate is needed.
///
/// The URI compared is the class's canonical one, which is the `class_uri` when
/// declared and the schema-derived default otherwise. Two defaults can never
/// collide (they are derived from the class name), so a collision always means
/// at least one explicit declaration.
fn lint_shared_class_uris(classes: &[ClassView], sink: &mut ValidationResultSink) {
    let mut hierarchies: BTreeMap<(String, String), Vec<&ClassView>> = BTreeMap::new();
    for class in classes {
        let root = hierarchy_root(class);
        hierarchies
            .entry((root.schema_id().to_string(), root.name().to_string()))
            .or_default()
            .push(class);
    }
    for ((_, root_name), mut members) in hierarchies {
        members.sort_by_key(|c| c.name().to_string());
        // The designator is declared once and inherited, so the first member
        // carrying one names it for the whole hierarchy.
        let Some(designator) = members
            .iter()
            .find_map(|c| c.get_type_designator_slot())
            .map(|d| d.name.clone())
        else {
            continue; // nothing dispatches on a class URI here: not this defect
        };
        let mut by_uri: BTreeMap<String, Vec<String>> = BTreeMap::new();
        for member in &members {
            by_uri
                .entry(member.canonical_uri().to_string())
                .or_default()
                .push(member.name().to_string());
        }
        for (uri, mut names) in by_uri {
            names.dedup();
            if names.len() < 2 {
                continue;
            }
            sink.push_warning(
                ValidationProblemType::AmbiguousElementIdentity,
                names.clone(),
                shared_class_uri_detail(&root_name, &uri, &names, &designator),
            );
        }
    }
}

/// Schema-level lint: the module's rules 1–5 — an element identity that comes
/// from nowhere, one that is the range class's type designator and so cannot
/// tell the elements of a homogeneous list apart, one derived from a family of
/// classes offering more than one `unique_keys` entry, one where that family
/// resolves *different* entries, and two classes of a designator-carrying
/// hierarchy answering to the same `class_uri`.
///
/// Warnings only — the schema stays usable.
pub fn lint_element_identity(sv: &SchemaView) -> Vec<ValidationResult> {
    let mut sink = ValidationResultSink::default();
    let conv = sv.converter();
    let mut class_ids = sv.get_class_ids();
    class_ids.sort();
    let mut seen: HashSet<(String, String)> = HashSet::new();
    // Kept for the class-level rule below, which is not about any one slot and
    // so cannot be answered inside the slot loop.
    let mut visited: Vec<ClassView> = Vec::new();
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
                if let Some((rc_name, names, own)) = slot_has_ambiguous_unique_keys(slot) {
                    if introduces_flagged_slot(
                        &class,
                        &slot.name,
                        slot_has_ambiguous_unique_keys_only,
                    ) {
                        sink.push_warning(
                            ValidationProblemType::AmbiguousElementIdentity,
                            vec![class.name().to_string(), slot.name.clone()],
                            ambiguous_unique_keys_detail(
                                class.name(),
                                &slot.name,
                                &rc_name,
                                &names,
                                &own,
                            ),
                        );
                    }
                }
                // A split label space is a sharper defect than an ambiguous
                // candidate set, and an additional one: both warnings fire, and
                // each gets its own introduces-gate predicate, because a
                // subclass can narrow the range to a non-diverging class while
                // leaving the candidate set as wide as it was (and the other
                // way round).
                if let Some((rc_name, groups)) = slot_has_split_identity_label_space(slot) {
                    if introduces_flagged_slot(
                        &class,
                        &slot.name,
                        slot_has_split_identity_label_space_only,
                    ) {
                        sink.push_warning(
                            ValidationProblemType::AmbiguousElementIdentity,
                            vec![class.name().to_string(), slot.name.clone()],
                            split_label_space_detail(class.name(), &slot.name, &rc_name, &groups),
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
        visited.push(class);
    }
    lint_shared_class_uris(&visited, &mut sink);
    let mut warnings = sink.into_vec();
    // The classes are visited in sorted id order, but a class's own slots come
    // from `ClassView::slots()`, which is HashMap-backed, so the warnings for a
    // single class arrive in an order that varies between runs. Sort here, once,
    // so every consumer inherits a stable, diffable order.
    warnings.sort_by(|a, b| a.subject.cmp(&b.subject));
    warnings
}

/// Data-level lint: the module's rules 6 and 7 — a list whose elements repeat a
/// declared identity (key/identifier or `unique_keys` value), and one addressed
/// positionally although its element class declares an identity, because some
/// element leaves the slot that identity names empty.
///
/// Both are things only the data can show: repeated and absent values are
/// alike valid against a schema that declares a non-`required` identity.
///
/// The duplicate rule deliberately does NOT consult `diff.linkml.io/opaque`:
/// a schema constraint is class-level truth, and diff vocabulary never
/// suppresses it. The positional rule does honour it, for the reason spelled
/// out at [`check_missing_labels`].
pub fn lint_instance_identity(value: &LinkMLInstance) -> Vec<ValidationResult> {
    let mut sink = ValidationResultSink::default();
    let mut path = Vec::new();
    walk(value, &mut path, &mut sink);
    sink.into_vec()
}

fn walk(v: &LinkMLInstance, path: &mut Vec<String>, sink: &mut ValidationResultSink) {
    match v {
        LinkMLInstance::List { values, slot, .. } => {
            check_duplicates(values, path, sink);
            check_missing_labels(values, slot, path, sink);
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

/// Warns when this list is addressed positionally *although* its element class
/// declares an identity: some element leaves the identity slot empty and yields
/// no label, so the keyed shape fails (spike D7).
///
/// The duplicate rule above is the other half of the same question. Together
/// they cover both ways a declared identity fails to address a list — repeated
/// labels and missing ones — and neither is visible in the schema: a `key` that
/// is not `required` may legitimately be absent, so only the data can show it.
/// The half-labelled list is the same defect at a smaller scale and is counted
/// the same way.
///
/// Silent when nothing is declared: a class with no identity is *meant* to be
/// positional, the schema lint has already said so, and repeating it once per
/// container in the data would bury the rule that matters. Silent for a range
/// that is not a class, for the same reason. A key that is the class's type
/// designator does not count as a declaration either — the engine looks past it
/// (spec addendum rule 1), so the class declares no element identity at all and
/// the schema-level designator rule is its voice.
///
/// Unlike the duplicate rule, this one **does** honour `opaque` and `ignore`,
/// and skips reference lists — it asks
/// [`slot_addresses_elements_by_position_or_label`] exactly as the schema rules
/// do. The two rules differ because they claim different things. A repeated
/// `unique_keys` value contradicts the class's own constraint whatever the slot
/// is annotated with, so diff vocabulary cannot silence it. This rule claims
/// only that the list *is addressed positionally*, and for a slot answering
/// "replaced as a whole" or "outside diff's scope" that claim is simply false.
/// A reference list is the sharper case: its elements are identifier strings
/// and can never carry an inlined element's identity label, so the rule would
/// fire on every such slot of every document — the shipped downstream corpus
/// has exactly one list matching it, and it is a reference list.
fn check_missing_labels(
    values: &[LinkMLInstance],
    slot: &SlotView,
    path: &[String],
    sink: &mut ValidationResultSink,
) {
    if values.is_empty() {
        return; // nothing to label, nothing to address
    }
    if !slot_addresses_elements_by_position_or_label(slot) {
        return; // references, dicts, opaque and ignored slots: see above
    }
    let Some(rc) = slot.get_range_class() else {
        return; // scalars and enums have no class to declare an identity on
    };
    let Some(identity) = declared_identity_description(&rc) else {
        return; // no identity declared: positional is what was asked for
    };
    let missing = values
        .iter()
        .filter(|v| element_identity_label(v).is_none())
        .count();
    if missing == 0 {
        return;
    }
    sink.push_warning(
        ValidationProblemType::AmbiguousElementIdentity,
        path.to_vec(),
        missing_labels_detail(missing, values.len(), rc.name(), &identity),
    );
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
