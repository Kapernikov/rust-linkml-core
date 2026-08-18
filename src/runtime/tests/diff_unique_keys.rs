use linkml_runtime::{
    diff, load_json_str, patch, Delta, DeltaOp, DiffOptions, LinkMLInstance, PatchOptions,
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
    let deltas = diff2(&f, phones(vec![e(), e2]), phones(vec![e(), e2_edit]));
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
    // PlainPhoneNumber has no unique_keys: removal still cascades as today.
    // Both surviving elements shift up, and each differs from the element that
    // used to sit at its index in both slots: 2 x 2 shifted-slot Updates plus
    // the trailing Remove.
    let before = json!({"name": "svc", "plainPhoneNumber": [e(), n(), o()]});
    let after = json!({"name": "svc", "plainPhoneNumber": [n(), o()]});
    let deltas = diff2(&f, before, after);
    assert_eq!(deltas.len(), 5, "{deltas:#?}");
    assert!(
        deltas.iter().all(|d| d.path[1].parse::<usize>().is_ok()),
        "positional cascade must use numeric segments: {deltas:#?}"
    );
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
    let (patched, trace) = patch(
        &golden,
        std::slice::from_ref(&delta),
        PatchOptions::default(),
    )
    .unwrap();
    assert_eq!(trace.failed, vec![delta.path.clone()]);
    assert!(patched.equals(&golden, true), "nothing may change");
}

#[test]
fn patch_refuses_positional_segment_into_identity_addressed_list() {
    let f = fixture();
    let golden = f.load(phones(vec![e(), n()]));
    // A stale positional patch aimed at a list whose elements all carry
    // unique identity labels: applying "index 0" would be a guess, and for
    // numeric-valued identity labels it would silently hit the wrong element.
    let delta = Delta {
        path: vec!["hasPhoneNumber".to_string(), "0".to_string()],
        op: DeltaOp::Remove,
        old: Some(e()),
        new: None,
    };
    let (patched, trace) = patch(
        &golden,
        std::slice::from_ref(&delta),
        PatchOptions::default(),
    )
    .unwrap();
    assert_eq!(trace.failed, vec![delta.path.clone()]);
    assert!(patched.equals(&golden, true), "nothing may be removed");
}

#[test]
fn patch_refuses_ambiguous_duplicate_key_labels() {
    let f = fixture();
    // Duplicate key/identifier labels refuse exactly like duplicate
    // unique_keys labels — the uniform rule on the patch side.
    let golden = f.load(json!({"name": "svc", "labelList": [
        {"lang": "nl", "text": "a"}, {"lang": "nl", "text": "b"}]}));
    let delta = Delta {
        path: vec![
            "labelList".to_string(),
            "nl".to_string(),
            "text".to_string(),
        ],
        op: DeltaOp::Update,
        old: Some(json!("a")),
        new: Some(json!("z")),
    };
    let (patched, trace) = patch(
        &golden,
        std::slice::from_ref(&delta),
        PatchOptions::default(),
    )
    .unwrap();
    assert_eq!(trace.failed, vec![delta.path.clone()]);
    assert!(patched.equals(&golden, true), "nothing may change");
}

#[test]
fn unique_key_deltas_round_trip_through_patch() {
    let f = fixture();
    let mut n2 = n();
    n2["phoneNumber"] = json!("09/241.25.99");
    for (before, after) in [
        (phones(vec![e(), n()]), phones(vec![e(), n2])), // field edit
        (phones(vec![e(), n()]), phones(vec![n()])),     // remove
        (phones(vec![e(), n()]), phones(vec![e(), n(), o()])), // add
    ] {
        let deltas = diff2(&f, before.clone(), after.clone());
        let (patched, trace) = patch(&f.load(before), &deltas, PatchOptions::default()).unwrap();
        assert!(trace.failed.is_empty(), "{:?}", trace.failed);
        assert!(
            patched.equals(&f.load(after), true),
            "{}",
            patched.to_json()
        );
    }
}

#[test]
fn patch_refuses_positional_update_into_identity_addressed_list() {
    let f = fixture();
    let golden = f.load(phones(vec![e(), n()]));
    // A stale positional Update against a keyed-shaped list resolves to no
    // element. It must report, not append: an unresolved address is never an
    // invitation to grow the list.
    let mut e2 = e();
    e2["phoneNumber"] = json!("09/999.99.99");
    let delta = Delta {
        path: vec!["hasPhoneNumber".to_string(), "0".to_string()],
        op: DeltaOp::Update,
        old: Some(e()),
        new: Some(e2),
    };
    let (patched, trace) = patch(
        &golden,
        std::slice::from_ref(&delta),
        PatchOptions::default(),
    )
    .unwrap();
    assert_eq!(trace.failed, vec![delta.path.clone()]);
    assert!(patched.equals(&golden, true), "nothing may be appended");
}

#[test]
fn patch_refuses_update_whose_label_matches_nothing() {
    let f = fixture();
    let golden = f.load(phones(vec![e(), n()]));
    // An Update addressing an element that is not there: the producer meant to
    // edit an existing Operator entry, and the golden has none. Reporting is
    // the only honest answer — appending would invent an edit as a creation.
    let delta = Delta {
        path: vec!["hasPhoneNumber".to_string(), "Operator".to_string()],
        op: DeltaOp::Update,
        old: Some(o()),
        new: Some(o()),
    };
    let (patched, trace) = patch(
        &golden,
        std::slice::from_ref(&delta),
        PatchOptions::default(),
    )
    .unwrap();
    assert_eq!(trace.failed, vec![delta.path.clone()]);
    assert!(patched.equals(&golden, true), "nothing may be appended");
}
