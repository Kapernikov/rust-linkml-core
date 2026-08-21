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
    // element, and its payload's identity ("Emergency_Number") does not name
    // the address ("0"). It must report: appending would invent an element,
    // and on `main` this address overwrote whichever element sat at index 0.
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
fn update_whose_payload_names_its_address_is_re_added() {
    let f = fixture();
    let golden = f.load(phones(vec![e(), n()]));
    // Multi-source: one source dropped the Operator entry, another still has
    // it and describes it as an Update (its delta was computed against an
    // older golden). The payload's identity names exactly the element the path
    // addresses, so the Update re-adds it — the field comes back rather than
    // being reported as a failure and silently lost.
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
    assert!(trace.failed.is_empty(), "failed: {:?}", trace.failed);
    assert!(
        patched.equals(&f.load(phones(vec![e(), n(), o()])), true),
        "{}",
        patched.to_json()
    );
    // And the round trip is clean: re-diffing the result against the intent
    // yields nothing.
    assert!(
        diff2(&f, patched.to_json(), phones(vec![e(), n(), o()])).is_empty(),
        "re-added element must be indistinguishable from an Add"
    );
}

#[test]
fn patch_refuses_update_whose_payload_contradicts_its_address() {
    let f = fixture();
    let golden = f.load(phones(vec![e(), n()]));
    // A label address that resolves to nothing AND a payload naming a
    // different element: the address is stale, not a re-add. Appending would
    // duplicate the Non_Urgent_Communication entry the list already carries
    // and knock the whole list off keyed matching.
    let mut n2 = n();
    n2["phoneNumber"] = json!("09/999.99.99");
    let delta = Delta {
        path: vec!["hasPhoneNumber".to_string(), "Operator".to_string()],
        op: DeltaOp::Update,
        old: Some(o()),
        new: Some(n2),
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

/// A second element carrying the same identity label as [`e`].
fn e2() -> JsonValue {
    json!({"phoneNumber": "09/000.00.00", "hasNumberFunction": "Emergency_Number"})
}

#[test]
fn keyed_source_to_duplicated_target_is_one_whole_slot_update() {
    let f = fixture();
    // The source list is keyed-shaped, the target repeats a label. Positional
    // segments against a keyed-shaped source are unappliable by design (patch
    // resolves such a list by label only), so the honest description is: this
    // list stopped having coherent element identity — one whole-slot Update.
    let before = phones(vec![e(), n()]);
    let after = phones(vec![e(), e2()]);
    let deltas = diff2(&f, before.clone(), after.clone());
    let delta = only(&deltas);
    assert_eq!(delta.path, vec!["hasPhoneNumber".to_string()]);
    assert_eq!(delta.op, DeltaOp::Update);
    assert_eq!(delta.old, Some(json!([e(), n()])));
    assert_eq!(delta.new, Some(json!([e(), e2()])));

    let (patched, trace) = patch(&f.load(before), &deltas, PatchOptions::default()).unwrap();
    assert!(trace.failed.is_empty(), "{:?}", trace.failed);
    assert!(
        patched.equals(&f.load(after), true),
        "patch(a, diff(a,b)) must equal b: {}",
        patched.to_json()
    );
}

#[test]
fn duplicated_source_to_keyed_target_stays_positional_and_round_trips() {
    let f = fixture();
    // The mirror image: the source is NOT keyed-shaped, so numeric segments
    // are exactly what patch resolves against it. Keep positional deltas.
    //
    // The second element differs from its target only in the key-bearing slot,
    // so the whole edit is one delta. A multi-delta variant is order-dependent
    // for a reason unrelated to the source-keyed fallback, documented on
    // `patch`: once the key edit lands the list becomes keyed-shaped, and any
    // numeric segment still queued is reported failed rather than guessed.
    let dup = json!({"phoneNumber": "09/241.25.03", "hasNumberFunction": "Emergency_Number"});
    let before = phones(vec![e(), dup]);
    let after = phones(vec![e(), n()]);
    let deltas = diff2(&f, before.clone(), after.clone());
    assert!(!deltas.is_empty(), "expected positional deltas");
    for d in &deltas {
        assert_eq!(
            d.path[0], "hasPhoneNumber",
            "unexpected delta path: {:?}",
            d.path
        );
        assert!(
            d.path.len() > 1 && d.path[1].parse::<usize>().is_ok(),
            "a non-keyed-shaped source keeps numeric segments: {:?}",
            d.path
        );
    }
    let (patched, trace) = patch(&f.load(before), &deltas, PatchOptions::default()).unwrap();
    assert!(trace.failed.is_empty(), "{:?}", trace.failed);
    assert!(
        patched.equals(&f.load(after), true),
        "patch(a, diff(a,b)) must equal b: {}",
        patched.to_json()
    );
}

// ---------------------------------------------------------------------------
// D3 — a type designator is never an element identity (spec addendum rule 1).
//
// A designator's value is a function of the element's class, so it cannot tell
// two elements of one class apart. The engine skips a designator key entirely:
// identity falls through to `unique_keys`, else the list is positional.

struct DesignatorFixture {
    sv: SchemaView,
    conv: Converter,
    ring: ClassView,
}

fn designator_fixture() -> DesignatorFixture {
    let mut p = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    p.push("tests/data/identity_type_designator_key.yaml");
    let schema = from_yaml(&p).unwrap();
    let mut sv = SchemaView::new();
    sv.add_schema(schema.clone()).unwrap();
    let conv = converter_from_schema(&schema);
    let ring = sv
        .get_class(&Identifier::new("Ring"), &conv)
        .unwrap()
        .expect("class not found");
    DesignatorFixture { sv, conv, ring }
}

impl DesignatorFixture {
    fn load(&self, v: JsonValue) -> LinkMLInstance {
        load_json_str(&v.to_string(), &self.sv, &self.ring, &self.conv)
            .unwrap()
            .into_instance()
            .unwrap()
    }

    fn diff2(&self, before: JsonValue, after: JsonValue) -> Vec<Delta> {
        diff(
            &self.load(before),
            &self.load(after),
            DiffOptions::new(true),
        )
    }
}

fn m1() -> JsonValue {
    json!({"code": "M1", "label": "one"})
}
fn m2() -> JsonValue {
    json!({"code": "M2", "label": "two"})
}

#[test]
fn designator_key_does_not_shadow_unique_keys() {
    // `Marker` declares BOTH a designator key (`typeURI`) and `unique_keys`.
    // The designator labels every marker alike; the real identity is `by_code`
    // (the name-sorted first entry). Reordering the list must be a no-op.
    let f = designator_fixture();
    let deltas = f.diff2(
        json!({"markers": [m1(), m2()]}),
        json!({"markers": [m2(), m1()]}),
    );
    assert!(
        deltas.is_empty(),
        "a reorder of unique_keys-identified markers is not a change: {deltas:#?}"
    );
}

#[test]
fn designator_key_shadow_field_edit_is_addressed_by_the_unique_key_label() {
    let f = designator_fixture();
    let mut edited = m2();
    edited["label"] = json!("TWO");
    let before = json!({"markers": [m1(), m2()]});
    let after = json!({"markers": [m1(), edited]});
    let deltas = f.diff2(before.clone(), after.clone());
    let delta = only(&deltas);
    assert_eq!(
        delta.path,
        vec!["markers".to_string(), "M2".to_string(), "label".to_string()],
        "the unique_keys label addresses the edit, not the constant designator"
    );
    assert_eq!(delta.op, DeltaOp::Update);

    // diff ↔ patch ↔ navigate stay symmetric: the emitted label resolves
    // through `resolve_list_segment` for both consumers.
    let loaded = f.load(before);
    assert!(
        loaded
            .navigate_path(["markers", "M2", "label"])
            .is_some_and(|v| v.to_json() == json!("two")),
        "navigate must resolve the label diff emitted"
    );
    let (patched, trace) = patch(&loaded, &deltas, PatchOptions::default()).unwrap();
    assert!(trace.failed.is_empty(), "{:?}", trace.failed);
    assert!(
        patched.equals(&f.load(after), true),
        "patch(a, diff(a,b)) must equal b: {}",
        patched.to_json()
    );
}

#[test]
fn designator_keyed_class_without_unique_keys_is_positional() {
    // `Coordinate` is keyed by its designator and declares no `unique_keys`, so
    // it has no element identity at all: even a one-element list — where the
    // constant designator would pass the uniqueness guard — is positional.
    let f = designator_fixture();
    let before = json!({"vertices": [{"x": 1.0, "y": 2.0}]});
    let after = json!({"vertices": [{"x": 1.0, "y": 9.0}]});
    let deltas = f.diff2(before.clone(), after.clone());
    let delta = only(&deltas);
    assert_eq!(
        delta.path,
        vec!["vertices".to_string(), "0".to_string(), "y".to_string()],
        "a designator key yields no label, so the list is addressed by index"
    );

    let (patched, trace) = patch(&f.load(before), &deltas, PatchOptions::default()).unwrap();
    assert!(trace.failed.is_empty(), "{:?}", trace.failed);
    assert!(
        patched.equals(&f.load(after), true),
        "{}",
        patched.to_json()
    );
}

#[test]
fn polymorphic_designator_keyed_list_is_positional() {
    // One element per subtype: the designator values differ, so they used to
    // pass as a per-element identity ("at most one element per subtype").
    // A designator still describes the class, never the element — positional.
    let f = designator_fixture();
    let base = json!({"typeURI": "identity:KeyedTypedThing", "label": "base"});
    let special = json!({"typeURI": "identity:SpecialKeyedTypedThing", "label": "special"});
    let mut edited = base.clone();
    edited["label"] = json!("BASE");
    let before = json!({"inheritedVertices": [base, special.clone()]});
    let after = json!({"inheritedVertices": [edited, special]});
    let deltas = f.diff2(before.clone(), after.clone());
    let delta = only(&deltas);
    assert_eq!(
        delta.path,
        vec![
            "inheritedVertices".to_string(),
            "0".to_string(),
            "label".to_string()
        ],
        "a polymorphic designator-keyed list is addressed by index too"
    );

    let (patched, trace) = patch(&f.load(before), &deltas, PatchOptions::default()).unwrap();
    assert!(trace.failed.is_empty(), "{:?}", trace.failed);
    assert!(
        patched.equals(&f.load(after), true),
        "{}",
        patched.to_json()
    );
}
