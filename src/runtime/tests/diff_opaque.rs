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

fn square() -> Vec<JsonValue> {
    vec![
        json!({"x": 4.35, "y": 50.85}),
        json!({"x": 4.36, "y": 50.85}),
        json!({"x": 4.36, "y": 50.86}),
        json!({"x": 4.35, "y": 50.86}),
    ]
}

fn outline(items: Vec<JsonValue>) -> JsonValue {
    json!({"name": "svc", "outline": items})
}

#[test]
fn opaque_ring_edit_is_one_whole_slot_update() {
    let f = fixture();

    let mut moved = square();
    moved[1] = json!({"x": 4.37, "y": 50.85});
    let mut inserted = square();
    inserted.insert(2, json!({"x": 4.365, "y": 50.855}));
    let dropped_first = square()[1..].to_vec();
    let mut reversed = square();
    reversed.reverse();

    for (label, after) in [
        ("move one vertex", moved),
        ("insert a vertex mid-ring", inserted),
        ("drop the first vertex", dropped_first),
        ("reverse the ring", reversed),
    ] {
        let deltas = diff2(&f, outline(square()), outline(after));
        let delta = only(&deltas);
        assert_eq!(delta.path, vec!["outline".to_string()], "{label}");
        assert_eq!(delta.op, DeltaOp::Update, "{label}");
        assert_eq!(
            delta
                .old
                .as_ref()
                .and_then(|v| v.as_array())
                .map(|a| a.len()),
            Some(4),
            "{label}: old must be the whole slot"
        );
        assert!(delta.new.as_ref().is_some_and(|v| v.is_array()), "{label}");
    }
}

#[test]
fn opaque_slot_unchanged_emits_nothing() {
    let f = fixture();
    let deltas = diff2(&f, outline(square()), outline(square()));
    assert!(deltas.is_empty(), "{deltas:#?}");
}

#[test]
fn opaque_scalar_list_is_one_whole_slot_update() {
    let f = fixture();
    let before = json!({"name": "svc", "opaqueTags": ["a", "b"]});
    let after = json!({"name": "svc", "opaqueTags": ["b", "c", "d"]});
    let deltas = diff2(&f, before, after);
    let delta = only(&deltas);
    assert_eq!(delta.path, vec!["opaqueTags".to_string()]);
    assert_eq!(delta.op, DeltaOp::Update);
}

#[test]
fn opaque_single_valued_object_is_one_whole_value_update() {
    let f = fixture();
    let before = json!({"name": "svc", "profile": {"bio": "b", "motto": "old"}});
    let after = json!({"name": "svc", "profile": {"bio": "b", "motto": "new"}});
    let deltas = diff2(&f, before, after);
    let delta = only(&deltas);
    assert_eq!(delta.path, vec!["profile".to_string()]);
    assert_eq!(delta.op, DeltaOp::Update);
    assert_eq!(delta.old, Some(json!({"bio": "b", "motto": "old"})));
    assert_eq!(delta.new, Some(json!({"bio": "b", "motto": "new"})));
}

#[test]
fn opaque_whole_slot_update_round_trips_through_patch() {
    let f = fixture();
    let before = outline(square());
    let mut moved = square();
    moved[1] = json!({"x": 4.37, "y": 50.85});
    let after = outline(moved);
    let deltas = diff2(&f, before.clone(), after.clone());
    let (patched, trace) = patch(&f.load(before), &deltas, PatchOptions::default()).unwrap();
    assert!(trace.failed.is_empty(), "{:?}", trace.failed);
    assert!(
        patched.equals(&f.load(after), true),
        "round-trip mismatch: {}",
        patched.to_json()
    );
}
