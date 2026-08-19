use linkml_runtime::{
    lint_element_identity, lint_instance_identity, load_json_str, LinkMLInstance,
    ValidationProblemType,
};
use linkml_schemaview::identifier::{converter_from_schema, Identifier};
use linkml_schemaview::io::from_yaml;
use linkml_schemaview::schemaview::{ClassView, SchemaView};
use linkml_schemaview::Converter;
use serde_json::{json, Value as JsonValue};
use std::path::PathBuf;

struct Fixture {
    sv: SchemaView,
    conv: Converter,
    service: ClassView,
}

/// Loads a schema from `tests/data` into its own `SchemaView`.
fn schema_view(file: &str) -> SchemaView {
    let mut p = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    p.push("tests/data");
    p.push(file);
    let schema = from_yaml(&p).unwrap();
    let mut sv = SchemaView::new();
    sv.add_schema(schema).unwrap();
    sv
}

/// Loads `data` as an instance of `class` against a schema from `tests/data`.
///
/// The shared [`Fixture`] is pinned to `identity.yaml`'s `Service`; the rules
/// below each get their own schema, so they need the same service generically.
fn load_into(file: &str, class: &str, data: JsonValue) -> LinkMLInstance {
    let mut p = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    p.push("tests/data");
    p.push(file);
    let schema = from_yaml(&p).unwrap();
    let mut sv = SchemaView::new();
    sv.add_schema(schema.clone()).unwrap();
    let conv = converter_from_schema(&schema);
    let cv = sv
        .get_class(&Identifier::new(class), &conv)
        .unwrap()
        .expect("class not found");
    load_json_str(&data.to_string(), &sv, &cv, &conv)
        .unwrap()
        .into_instance()
        .unwrap()
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

fn e() -> JsonValue {
    json!({"phoneNumber": "09/241.25.00", "hasNumberFunction": "Emergency_Number"})
}
fn n() -> JsonValue {
    json!({"phoneNumber": "09/241.25.03", "hasNumberFunction": "Non_Urgent_Communication"})
}
fn phones(items: Vec<JsonValue>) -> JsonValue {
    json!({"name": "svc", "hasPhoneNumber": items})
}

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
        assert_eq!(
            w.problem_type,
            ValidationProblemType::AmbiguousElementIdentity
        );
        assert!(!w.severity.is_error(), "the linter warns, never errors");
        assert!(
            w.detail.contains("unique_keys") && w.detail.contains("diff.linkml.io/opaque"),
            "the warning must name the author's options: {}",
            w.detail
        );
    }

    let detail = |slot: &str| {
        warnings
            .iter()
            .find(|w| w.subject[1] == slot)
            .map(|w| w.detail.clone())
            .unwrap_or_default()
    };
    assert!(
        detail("plainPhoneNumber").contains("element class 'PlainPhoneNumber'"),
        "an object range must be told which class to declare the identity on: {}",
        detail("plainPhoneNumber")
    );
    assert!(
        !detail("tags").contains("element class"),
        "a scalar range has no element class to declare keys on: {}",
        detail("tags")
    );
}

#[test]
fn schema_lint_visits_a_class_with_an_explicit_class_uri_exactly_once() {
    // `Service` declares `class_uri: identity:ServiceEndpoint`, so schemaview
    // indexes it under both that URI and its default one and `get_class_ids()`
    // yields it twice. Walking those ids naively reports every one of the
    // class's slots twice.
    let f = fixture();
    let warnings = lint_element_identity(&f.sv);
    let mut subjects: Vec<Vec<String>> = warnings.iter().map(|w| w.subject.clone()).collect();
    subjects.sort();
    let mut unique = subjects.clone();
    unique.dedup();
    assert_eq!(
        subjects, unique,
        "each slot must be reported once, not once per class URI: {warnings:#?}"
    );
}

#[test]
fn schema_lint_reports_both_classes_that_share_one_class_uri() {
    // LinkML lets distinct classes declare the same `class_uri` (meta.yaml's
    // `Anything` and extensions.yaml's `AnyValue` both use `linkml:Any`).
    // Deduping visits by class_uri makes the second class look already-seen and
    // silently drops every one of its warnings — a false negative, which is
    // worse in a lint than the duplicate reporting it was meant to fix.
    //
    // The fixture pulls in both directions at once: `SharedUriA`/`SharedUriB`
    // share one `class_uri` and must BOTH be reported, while `TwoUris` is
    // indexed under two URIs of its own and must be reported only once.
    let sv = schema_view("identity_shared_class_uri.yaml");
    let subjects: Vec<Vec<String>> = lint_element_identity(&sv)
        .iter()
        .map(|w| w.subject.clone())
        .collect();
    assert_eq!(
        subjects,
        vec![
            vec!["SharedUriA".to_string(), "itemsA".to_string()],
            vec!["SharedUriB".to_string(), "itemsB".to_string()],
            vec!["TwoUris".to_string(), "itemsC".to_string()],
        ],
        "classes sharing a class_uri must each be linted, and a class indexed \
         under two URIs must be linted once"
    );
}

#[test]
fn schema_lint_reports_an_inherited_slot_only_where_it_is_introduced() {
    // A flagged slot is inherited by every descendant, so reporting it per
    // class buries the one place the author can fix it. Only the topmost
    // flagged declarer is reported — but a subclass whose `slot_usage` changes
    // the identity answer is judged on its own merits, in both directions.
    let sv = schema_view("identity_inheritance.yaml");
    let subjects: Vec<Vec<String>> = lint_element_identity(&sv)
        .iter()
        .map(|w| w.subject.clone())
        .collect();
    assert_eq!(
        subjects,
        vec![
            // introduces `outline`; Middle, Leaf and Broken inherit it unchanged
            vec!["Base".to_string(), "outline".to_string()],
            // widens an inherited clean slot into an ambiguous one: introduced here
            vec!["Broken".to_string(), "keyed".to_string()],
            // mixin-provided, reported both on the mixin and on its user
            vec!["HasTagsMixin".to_string(), "tags".to_string()],
            vec!["Tagged".to_string(), "tags".to_string()],
        ],
        "an inherited flagged slot must be reported once, at its introducing class"
    );
    // Spelled out, because these are the cases a naive fix gets wrong:
    for silent in ["Middle", "Leaf", "Fixed"] {
        assert!(
            !subjects.iter().any(|s| s[0] == silent),
            "{silent} must stay silent, got {subjects:?}"
        );
    }
}

#[test]
fn schema_lint_warning_order_is_deterministic() {
    // A class's slots come from `ClassView::slots()`, which is HashMap-backed,
    // so the per-class warning order is process-dependent. Each fresh
    // `SchemaView` gets its own hash seed, so an unsorted walk eventually
    // disagrees with itself.
    let expected = vec![
        vec!["Service".to_string(), "plainPhoneNumber".to_string()],
        vec!["Service".to_string(), "tags".to_string()],
    ];
    for _ in 0..20 {
        let warnings = lint_element_identity(&fixture().sv);
        let subjects: Vec<Vec<String>> = warnings.iter().map(|w| w.subject.clone()).collect();
        assert_eq!(subjects, expected, "{warnings:#?}");
    }
}

#[test]
fn schema_lint_flags_a_list_whose_identity_is_the_type_designator() {
    // A key that is also the type designator is constant across a homogeneous
    // list — every vertex of a ring carries the same value — and one value per
    // subtype across a polymorphic one. Neither is element identity, so the
    // engine looks past such a key entirely (spec addendum rule 1) and the list
    // falls back to the class's unique_keys, or to positional addressing. This
    // rule is the author-facing voice for exactly that shape, and the only one:
    // it is asked first and the other unique_keys rules never see the slot.
    let sv = schema_view("identity_type_designator_key.yaml");
    let warnings = lint_element_identity(&sv);
    let subjects: Vec<Vec<String>> = warnings.iter().map(|w| w.subject.clone()).collect();
    assert_eq!(
        subjects,
        vec![
            // the wild shape: the designator is declared on a base class and
            // the subclass promotes it to the key with `slot_usage`, so the
            // rule sees it only through `SlotView::definition()`'s chain merge
            vec!["Ring".to_string(), "inheritedVertices".to_string()],
            // `Marker` declares two `unique_keys` the designator key used to
            // shadow and no longer does, so the several-entries rule matches it
            // too; the designator rule is asked first and is its sole voice, so
            // `markers` is flagged once, by this rule
            vec!["Ring".to_string(), "markers".to_string()],
            vec!["Ring".to_string(), "vertices".to_string()],
        ],
        "only the designator-keyed list slots are flagged, once each, at their \
         introducing class: {warnings:#?}"
    );
    let w = warnings
        .iter()
        .find(|w| w.subject[1] == "vertices")
        .expect("the ring slot must be flagged");
    assert_eq!(
        w.problem_type,
        ValidationProblemType::AmbiguousElementIdentity
    );
    assert!(!w.severity.is_error(), "the linter warns, never errors");
    assert!(
        w.detail.contains("typeURI") && w.detail.contains("Coordinate"),
        "the warning must name the designator slot and its class: {}",
        w.detail
    );
    assert!(
        w.detail.contains("designates_type"),
        "the warning must say why the key is not discriminating: {}",
        w.detail
    );
    assert!(
        w.detail.contains("positional"),
        "the warning must say what the diff engine actually does: {}",
        w.detail
    );
    let inherited = warnings
        .iter()
        .find(|w| w.subject[1] == "inheritedVertices")
        .expect("a designator promoted to key by slot_usage must be flagged too");
    assert!(
        inherited.detail.contains("'KeyedTypedThing.typeURI'"),
        "the warning must name the subclass that declares the key, and the \
         inherited slot it declares it on: {}",
        inherited.detail
    );
}

#[test]
fn schema_lint_fires_once_per_designator_keyed_slot_and_names_the_designator() {
    // The engine no longer accepts a designator as element identity (spec
    // addendum rule 1), so a designator-keyed class declaring no `unique_keys`
    // is *also* the shape the identity-less rule looks for, and one whose real
    // `unique_keys` were shadowed is the shape the several-unique_keys rule
    // looks for. The designator rule is the sharper diagnosis of both and
    // stays their only voice: exactly one warning per slot, naming the
    // designator that cannot discriminate.
    let sv = schema_view("identity_type_designator_key.yaml");
    let warnings = lint_element_identity(&sv);
    for (slot, why) in [
        ("vertices", "a designator-keyed class with no unique_keys"),
        (
            "markers",
            "a designator-keyed class whose unique_keys it shadowed",
        ),
    ] {
        let fired: Vec<_> = warnings
            .iter()
            .filter(|w| w.subject == vec!["Ring".to_string(), slot.to_string()])
            .collect();
        assert_eq!(
            fired.len(),
            1,
            "{why} must produce exactly one warning for '{slot}': {warnings:#?}"
        );
        assert!(
            fired[0].detail.contains("designates_type") && fired[0].detail.contains("typeURI"),
            "the designator rule must be the voice for '{slot}': {}",
            fired[0].detail
        );
    }
}

#[test]
fn schema_lint_leaves_designator_dicts_and_ordinary_keys_alone() {
    // Guard rails for the designator rule: the dict form keyed by the designator
    // means at-most-one-element-per-subtype and is legitimate; an ordinary key
    // discriminates per element; `opaque` / `ignore` answer the question already;
    // and an unchanged inherited slot belongs to its introducing class.
    let sv = schema_view("identity_type_designator_key.yaml");
    let warnings = lint_element_identity(&sv);
    let flagged: Vec<String> = warnings.iter().map(|w| w.subject[1].clone()).collect();
    for silent in ["byType", "points", "archivedVertices", "draftVertices"] {
        assert!(
            !flagged.contains(&silent.to_string()),
            "{silent} must stay silent, got {warnings:#?}"
        );
    }
    assert!(
        !warnings.iter().any(|w| w.subject[0] == "SubRing"),
        "an unchanged inherited slot is reported at Ring only: {warnings:#?}"
    );
}

#[test]
fn data_lint_flags_duplicate_declared_identities() {
    let f = fixture();
    let dup = json!({"phoneNumber": "09/000.00.00", "hasNumberFunction": "Emergency_Number"});
    let inst = f.load(phones(vec![e(), dup]));
    let warnings = lint_instance_identity(&inst);
    assert_eq!(warnings.len(), 1, "{warnings:#?}");
    assert_eq!(
        warnings[0].problem_type,
        ValidationProblemType::DuplicateElementIdentity
    );
    assert_eq!(warnings[0].subject, vec!["hasPhoneNumber".to_string()]);
    assert!(!warnings[0].severity.is_error());
}

#[test]
fn data_lint_warning_order_is_deterministic_across_sibling_containers() {
    let f = fixture();
    // Three sibling containers, each with a duplicated declared identity. The
    // instance's slots live in a HashMap, so the warnings must be emitted in a
    // stable (name-sorted) order rather than in process-dependent hash order.
    let c = json!({"kind": "Emergency", "phone": "02/111.11.11"});
    let dup = json!({"phoneNumber": "09/000.00.00", "hasNumberFunction": "Emergency_Number"});
    let data = json!({
        "name": "svc",
        "archivedContacts": [c.clone(), c.clone()],
        "contacts": [c.clone(), c],
        "hasPhoneNumber": [e(), dup],
    });
    let expected = vec![
        vec!["archivedContacts".to_string()],
        vec!["contacts".to_string()],
        vec!["hasPhoneNumber".to_string()],
    ];
    // Repeated on freshly built instances: each `HashMap` gets its own hash
    // seed, so an unordered walk would eventually disagree with itself.
    for _ in 0..10 {
        let warnings = lint_instance_identity(&f.load(data.clone()));
        let subjects: Vec<Vec<String>> = warnings.iter().map(|w| w.subject.clone()).collect();
        assert_eq!(subjects, expected, "{warnings:#?}");
    }
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

#[test]
fn schema_lint_names_the_load_bearing_unique_key_when_a_class_declares_several() {
    // Declaration order is not preserved by the metamodel, so element identity
    // is derived from the name-sorted first `unique_keys` entry. A class with
    // two entries therefore has a silent, alphabetically-decided identity:
    // adding an earlier-sorting entry re-addresses every delta path for every
    // slot ranged on it, with nothing to notice it by.
    let sv = schema_view("identity_multiple_unique_keys.yaml");
    let warnings = lint_element_identity(&sv);
    let subjects: Vec<Vec<String>> = warnings.iter().map(|w| w.subject.clone()).collect();
    assert_eq!(
        subjects,
        vec![vec!["Catalog".to_string(), "badges".to_string()]],
        "only the ambiguous slot is flagged, once, at its introducing class: \
         {warnings:#?}"
    );
    let w = &warnings[0];
    assert_eq!(
        w.problem_type,
        ValidationProblemType::AmbiguousElementIdentity
    );
    assert!(!w.severity.is_error(), "the linter warns, never errors");
    assert!(
        w.detail.contains("'by_code'") && w.detail.contains("'zz_by_label'"),
        "the warning must name every candidate: {}",
        w.detail
    );
    assert!(
        w.detail.contains("Badge"),
        "the warning must name the element class the entries live on: {}",
        w.detail
    );
    let winner = w.detail.find("'by_code'").unwrap_or(usize::MAX);
    let other = w.detail.find("'zz_by_label'").unwrap_or(0);
    assert!(
        winner < other,
        "the load-bearing entry must be named first, and identified as such: {}",
        w.detail
    );
}

#[test]
fn schema_lint_leaves_single_entry_and_keyed_classes_alone() {
    // Guard rails for the ambiguity rule: one entry is unambiguous, a key slot
    // outranks `unique_keys` so the entries are not load-bearing at all, and
    // `opaque` / `ignore` mean there are no per-element paths to re-address.
    let sv = schema_view("identity_multiple_unique_keys.yaml");
    let flagged: Vec<String> = lint_element_identity(&sv)
        .iter()
        .map(|w| w.subject[1].clone())
        .collect();
    for silent in ["tickets", "seats", "archivedBadges", "draftBadges"] {
        assert!(
            !flagged.contains(&silent.to_string()),
            "{silent} must stay silent, got {flagged:?}"
        );
    }
}

#[test]
fn data_lint_flags_a_list_addressed_positionally_despite_a_declared_identity() {
    // The element class declares an identity, but the data leaves the slot it
    // names empty: the element yields no label, the list stops being
    // keyed-shaped, and every delta addressing it goes back to being positional
    // — silently, because nothing here is invalid. A `key` that is not
    // `required` may legitimately be absent, so only the data can show it.
    let inst = load_into(
        "identity_missing_labels.yaml",
        "Registry",
        json!({
            "name": "reg",
            // all elements unlabelled: the absent-optional-key case
            "entries": [{"note": "a"}, {"note": "b"}],
            // half-labelled: one element carries the unique_keys slot, one does not
            "records": [{"serial": "S1"}, {"note": "b"}],
            // D9: the designator override left the key with nothing to fill it
            "unfillable": [{"note": "a"}, {"note": "b"}],
        }),
    );
    let warnings = lint_instance_identity(&inst);
    let subjects: Vec<Vec<String>> = warnings.iter().map(|w| w.subject.clone()).collect();
    assert_eq!(
        subjects,
        vec![
            vec!["entries".to_string()],
            vec!["records".to_string()],
            vec!["unfillable".to_string()],
        ],
        "one warning per container, at the container's path: {warnings:#?}"
    );
    for w in &warnings {
        assert_eq!(
            w.problem_type,
            ValidationProblemType::AmbiguousElementIdentity
        );
        assert!(!w.severity.is_error(), "the linter warns, never errors");
        assert!(
            w.detail.contains("positional"),
            "the warning must say what the engine falls back to: {}",
            w.detail
        );
    }
    let detail = |slot: &str| {
        warnings
            .iter()
            .find(|w| w.subject[0] == slot)
            .map(|w| w.detail.clone())
            .unwrap_or_default()
    };
    assert!(
        detail("entries").contains("2 of 2") && detail("entries").contains("'OptKey'"),
        "the warning must count the unlabelled elements and name the class \
         whose identity is unfilled: {}",
        detail("entries")
    );
    assert!(
        detail("records").contains("1 of 2"),
        "a half-labelled list is the same defect, and its count says so: {}",
        detail("records")
    );
}

#[test]
fn data_lint_positional_despite_identity_only_speaks_for_inlined_addressable_lists() {
    // Guard rails, each for a different reason.
    //
    // A class that declares no identity is *meant* to be positional: the schema
    // lint already speaks for it, and repeating that once per container in the
    // data would drown the rule that matters. Scalars have no element class at
    // all. `opaque` and `ignore` answer the addressing question themselves, so
    // "this list is addressed positionally" is not true of them — the duplicate
    // rule ignores both annotations because a repeated identity contradicts the
    // class's own constraint, which is a different claim.
    //
    // The reference list is the one that matters in practice: its elements are
    // identifier strings, which can never carry an inlined element's identity
    // label, so without this guard the rule fires on every reference list ranged
    // on a keyed class. asset360's `NetElement.ports` is that shape, and it is
    // the ONLY list in the downstream corpus the unguarded rule fired on.
    let inst = load_into(
        "identity_missing_labels.yaml",
        "Registry",
        json!({
            "name": "reg",
            "vertices": [{"x": 1.0, "y": 2.0}, {"x": 1.0, "y": 2.0}],
            "tags": ["a", "b"],
            "archivedEntries": [{"note": "a"}],
            "draftEntries": [{"note": "a"}],
            "references": ["urn:a", "urn:b"],
        }),
    );
    let warnings = lint_instance_identity(&inst);
    assert!(warnings.is_empty(), "{warnings:#?}");
}

#[test]
fn data_lint_positional_despite_identity_is_silent_on_fully_labelled_lists() {
    // The rule must not fire on the lists it was written to leave alone: every
    // element labelled is exactly the keyed shape.
    let inst = load_into(
        "identity_missing_labels.yaml",
        "Registry",
        json!({
            "name": "reg",
            "entries": [{"code": "a"}, {"code": "b"}],
            "records": [{"serial": "S1"}, {"serial": "S2"}],
            // an empty list has no unlabelled element to complain about
            "unfillable": [],
        }),
    );
    let warnings = lint_instance_identity(&inst);
    assert!(warnings.is_empty(), "{warnings:#?}");
}

#[test]
fn schema_lint_counts_unique_keys_entries_across_the_range_class_descendants() {
    // A list ranged on a class holds elements of every class descending from it,
    // and each element is labelled by its OWN merged `unique_keys`. Inspecting
    // only the range class misses a descendant's entry entirely: `Box` alone
    // declares one entry and looks unambiguous, while `BigBox` adds a second
    // that any element of the list may be labelled by.
    let sv = schema_view("identity_descendant_unique_keys.yaml");
    let warnings = lint_element_identity(&sv);
    let boxes: Vec<_> = warnings
        .iter()
        .filter(|w| w.subject == vec!["Depot".to_string(), "boxes".to_string()])
        .collect();
    assert_eq!(
        boxes.len(),
        1,
        "a later-sorting descendant entry widens the candidate set without \
         splitting the label space: one warning: {warnings:#?}"
    );
    assert!(
        boxes[0].detail.contains("'by_id'") && boxes[0].detail.contains("'zz_by_volume'"),
        "the warning must name every candidate, wherever it is declared: {}",
        boxes[0].detail
    );
    // guard rail: a descendant declaring nothing of its own leaves the range
    // class as unambiguous as it was
    let flagged: Vec<String> = warnings.iter().map(|w| w.subject[1].clone()).collect();
    assert!(
        !flagged.contains(&"crates".to_string()),
        "crates must stay silent, got {warnings:#?}"
    );
    // `StampedPlate`'s key outranks its own `unique_keys`, so its entries never
    // widen the candidate set: this rule has nothing to say about `keyed`. (The
    // divergence rule does — the key is its own label space.)
    let keyed_candidates: Vec<_> = warnings
        .iter()
        .filter(|w| w.subject[1] == "keyed")
        .filter(|w| w.detail.contains("candidate entries"))
        .collect();
    assert!(
        keyed_candidates.is_empty(),
        "a key-labelled descendant does not widen the candidate set: \
         {keyed_candidates:#?}"
    );
}

#[test]
fn schema_lint_splits_a_label_space_between_a_key_and_a_unique_keys_entry() {
    // A descendant that declares a `key` does not widen the candidate set — its
    // own `unique_keys` stop being load-bearing — but it very much occupies its
    // own label space: `Plate` elements are labelled by `plate_identity` and
    // `StampedPlate` elements by `stampId`, in one list. A path written against
    // one cannot address an element of the other, and a `stampId` colliding
    // with a `plate_identity` value breaks neither class's constraint. Counting
    // a key-labelled class as its own group is what sees that.
    let sv = schema_view("identity_descendant_unique_keys.yaml");
    let warnings = lint_element_identity(&sv);
    let keyed: Vec<_> = warnings
        .iter()
        .filter(|w| w.subject == vec!["Depot".to_string(), "keyed".to_string()])
        .collect();
    assert_eq!(
        keyed.len(),
        1,
        "the divergence warning, and only it, speaks for `keyed`: {warnings:#?}"
    );
    for needle in ["'Plate'", "'plate_identity'", "'StampedPlate'", "'stampId'"] {
        assert!(
            keyed[0].detail.contains(needle),
            "the warning must name both labellings and the class each belongs \
             to; missing {needle}: {}",
            keyed[0].detail
        );
    }
    assert!(
        keyed[0].detail.contains("key 'stampId'"),
        "the warning must say that one of the two labellings is a key, not a \
         unique_keys entry: {}",
        keyed[0].detail
    );
}

#[test]
fn schema_lint_reports_a_retargeted_slot_the_parent_never_warned_about() {
    // The introduces-gate asks "is the parent's slot flagged for the same
    // reason?". For the two unique_keys rules that question has to subtract the
    // designator case, exactly as the identity-less rule's gate does: a
    // designator-keyed range class satisfies both raw shapes (the key no longer
    // shadows the entries), but the designator rule is asked first and is the
    // slot's ONLY voice, so the parent never emitted either warning. Without
    // the subtraction a subclass that `slot_usage`-retargets the slot onto a
    // class those rules really do speak for is suppressed by a parent warning
    // that does not exist, and the ambiguity is reported nowhere.
    let sv = schema_view("identity_descendant_unique_keys.yaml");
    let warnings = lint_element_identity(&sv);
    let subject = |c: &str| vec![c.to_string(), "stamps".to_string()];

    let base: Vec<_> = warnings
        .iter()
        .filter(|w| w.subject == subject("Base"))
        .collect();
    assert_eq!(
        base.len(),
        1,
        "the designator rule speaks alone: {warnings:#?}"
    );
    assert!(
        base[0].detail.contains("designates_type"),
        "and it is the designator rule: {}",
        base[0].detail
    );

    let retargeted: Vec<_> = warnings
        .iter()
        .filter(|w| w.subject == subject("Retargeted"))
        .collect();
    assert_eq!(
        retargeted.len(),
        1,
        "a retarget onto a keyless two-entry class must be reported here: \
         {warnings:#?}"
    );
    assert!(
        retargeted[0].detail.contains("'by_alpha'")
            && retargeted[0].detail.contains("'zz_by_beta'"),
        "and by the several-entries rule: {}",
        retargeted[0].detail
    );

    let split: Vec<_> = warnings
        .iter()
        .filter(|w| w.subject == subject("SplitRetargeted"))
        .collect();
    assert_eq!(
        split.len(),
        2,
        "a retarget onto a split family must get both warnings here: \
         {warnings:#?}"
    );
    assert!(
        split.iter().any(|w| w.detail.contains("label space")),
        "one of them being the divergence rule: {split:#?}"
    );
}

#[test]
fn schema_lint_flags_descendants_that_resolve_different_load_bearing_entries() {
    // The split label space: `Gadget` elements are labelled by `gadget_identity`
    // and `Widget` elements by the earlier-sorting `aaa_widget_identity` it
    // adds. One list, two label spaces — navigating by a base label misses a
    // Widget, and a Widget serial colliding with a Gadget code violates neither
    // class's constraint. This is an ADDITIONAL warning: the several-entries
    // rule still fires, because the candidate set is ambiguous too.
    let sv = schema_view("identity_descendant_unique_keys.yaml");
    let warnings = lint_element_identity(&sv);
    let gadgets: Vec<_> = warnings
        .iter()
        .filter(|w| w.subject == vec!["Depot".to_string(), "gadgets".to_string()])
        .collect();
    assert_eq!(
        gadgets.len(),
        2,
        "the split gets its own warning on top of the several-entries one: \
         {warnings:#?}"
    );
    let split = gadgets
        .iter()
        .find(|w| w.detail.contains("Widget"))
        .expect("one of the two must name the diverging descendant");
    assert_eq!(
        split.problem_type,
        ValidationProblemType::AmbiguousElementIdentity
    );
    assert!(!split.severity.is_error(), "the linter warns, never errors");
    for needle in [
        "'Gadget'",
        "'gadget_identity'",
        "'Widget'",
        "'aaa_widget_identity'",
    ] {
        assert!(
            split.detail.contains(needle),
            "the warning must name each diverging class and the entry it \
             resolves to; missing {needle}: {}",
            split.detail
        );
    }
    // `boxes` agrees on `by_id` throughout, so it must NOT get this warning
    assert_eq!(
        warnings
            .iter()
            .filter(|w| w.subject[1] == "boxes")
            .filter(|w| w.detail.contains("zz_by_volume") && w.detail.contains("'BigBox'"))
            .count(),
        0,
        "a widened candidate set is not a split label space: {warnings:#?}"
    );
}

#[test]
fn schema_lint_flags_a_shared_class_uri_within_a_designator_hierarchy() {
    // Two classes of one hierarchy answering to one `class_uri`, in a hierarchy
    // whose designator is dispatched by exactly that URI: the loader picks one
    // of them, stably but by an ordering the schema never states.
    let sv = schema_view("identity_shared_uri_designator.yaml");
    let warnings = lint_element_identity(&sv);
    let subjects: Vec<Vec<String>> = warnings.iter().map(|w| w.subject.clone()).collect();
    // The classes that share the URI, behind a discriminator. Rules 1-4 are
    // per-slot and subject `[class, slot]`; both CLIs render a subject by
    // joining it with `.`, so a bare `[Alpha, Beta]` printed as `Alpha.Beta` —
    // indistinguishable from a slot warning about `Beta` on class `Alpha`, and
    // just as indistinguishable to anything grouping findings by subject. The
    // leading segment says which rule spoke, and no `[class, slot]` subject can
    // collide with it: it is not a class name in any schema being linted, since
    // it is not a class name at all.
    assert_eq!(
        subjects,
        vec![vec![
            "shared_class_uri".to_string(),
            "Alpha".to_string(),
            "Beta".to_string()
        ]],
        "the sharing classes are the subject, behind the rule's discriminator, \
         and the controls stay silent: {warnings:#?}"
    );
    let w = &warnings[0];
    assert_eq!(
        w.problem_type,
        ValidationProblemType::AmbiguousElementIdentity
    );
    assert!(!w.severity.is_error(), "the linter warns, never errors");
    assert!(
        w.detail.contains("Alpha") && w.detail.contains("Beta"),
        "the warning must name both classes: {}",
        w.detail
    );
    assert!(
        w.detail.contains("designates_type") && w.detail.contains("class_uri"),
        "the warning must name both halves of the condition: {}",
        w.detail
    );
    assert!(
        w.detail.contains("typeURI"),
        "the warning must name the designator the URI is dispatched by: {}",
        w.detail
    );
}
