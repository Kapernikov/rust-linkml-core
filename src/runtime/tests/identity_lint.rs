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
    // list: every vertex of a ring carries the same value, so keyed matching
    // would collapse an N-vertex ring to one element. The class "has a key", so
    // the identity-less rule passes it — this rule is what sees it.
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
            // the designator key outranks `unique_keys`, so `markers` is flagged
            // by this rule, once, and not by the several-unique_keys rule
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
