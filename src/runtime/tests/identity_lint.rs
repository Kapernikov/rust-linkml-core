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
