//! Spec addendum rules 3 and 4 (spike findings D2).
//!
//! Rule 3: when `diff` pairs two objects whose classes differ, the change is a
//! whole-element `Update` — never field-level recursion across two classes.
//! Rule 4: a delta whose value cannot be built or applied at its resolved
//! location records its path in `PatchTrace::failed`; it never voids the batch.

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
    inventory: ClassView,
}

fn fixture() -> Fixture {
    let mut p = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    p.push("tests/data/identity_class_change.yaml");
    let schema = from_yaml(&p).unwrap();
    let mut sv = SchemaView::new();
    sv.add_schema(schema.clone()).unwrap();
    let conv = converter_from_schema(&schema);
    let inventory = sv
        .get_class(&Identifier::new("Inventory"), &conv)
        .unwrap()
        .expect("class not found");
    Fixture {
        sv,
        conv,
        inventory,
    }
}

impl Fixture {
    fn load(&self, v: JsonValue) -> LinkMLInstance {
        load_json_str(&v.to_string(), &self.sv, &self.inventory, &self.conv)
            .unwrap()
            .into_instance()
            .unwrap()
    }
}

fn bolt() -> JsonValue {
    json!({"typeURI": "part:Bolt", "code": "B1", "thread": "M8"})
}
fn nut() -> JsonValue {
    json!({"typeURI": "part:Nut", "code": "B1", "pitch": "1.25"})
}

fn class_name_at(v: &LinkMLInstance, path: &[&str]) -> String {
    let segs: Vec<String> = path.iter().map(|s| s.to_string()).collect();
    match v.navigate_path(&segs).expect("path must resolve") {
        LinkMLInstance::Object { class, .. } => class.name().to_string(),
        other => panic!("expected an object, got {}", other.to_json()),
    }
}

/// Rule 3, both directions: same identity label, different class.
#[test]
fn class_change_is_one_whole_element_update() {
    let f = fixture();
    for (before, after, want_class) in [(bolt(), nut(), "Nut"), (nut(), bolt(), "Bolt")] {
        let src = f.load(json!({"parts": [before.clone()]}));
        let tgt = f.load(json!({"parts": [after.clone()]}));
        let deltas = diff(&src, &tgt, DiffOptions::new(true));
        assert_eq!(
            deltas.len(),
            1,
            "a class change is ONE delta, not field recursion: {deltas:#?}"
        );
        let d = &deltas[0];
        assert_eq!(d.path, vec!["parts".to_string(), "B1".to_string()]);
        assert_eq!(d.op, DeltaOp::Update);
        assert_eq!(
            d.new.as_ref().and_then(|v| v.get("code")),
            Some(&json!("B1"))
        );

        // Rule 3 round trip: patch rebuilds the element as the NEW class,
        // polymorphically, through the slot.
        let (patched, trace) = patch(&src, &deltas, PatchOptions::default()).unwrap();
        assert!(trace.failed.is_empty(), "{:?}", trace.failed);
        assert!(patched.equals(&tgt, true), "patched: {}", patched.to_json());
        assert_eq!(class_name_at(&patched, &["parts", "B1"]), want_class);
    }
}

/// Rule 3 is not list-specific: a single-valued inlined object changing class
/// is the same whole-element replacement.
#[test]
fn class_change_of_single_valued_object_is_one_update() {
    let f = fixture();
    let src = f.load(json!({"featured": bolt()}));
    let tgt = f.load(json!({"featured": nut()}));
    let deltas = diff(&src, &tgt, DiffOptions::new(true));
    assert_eq!(deltas.len(), 1, "{deltas:#?}");
    assert_eq!(deltas[0].path, vec!["featured".to_string()]);
    assert_eq!(deltas[0].op, DeltaOp::Update);
    let (patched, trace) = patch(&src, &deltas, PatchOptions::default()).unwrap();
    assert!(trace.failed.is_empty(), "{:?}", trace.failed);
    assert!(patched.equals(&tgt, true), "{}", patched.to_json());
    assert_eq!(class_name_at(&patched, &["featured"]), "Nut");
}

/// Same class, changed key value: still a whole-element replacement, and the
/// key-value branch that produces it is untouched by rule 3.
#[test]
fn same_class_key_change_still_replaces_whole_element() {
    let f = fixture();
    let src = f.load(json!({"owner": {"oid": "o1", "oname": "Ada"}}));
    let tgt = f.load(json!({"owner": {"oid": "o2", "oname": "Ada"}}));
    let deltas = diff(
        &src,
        &tgt,
        DiffOptions {
            treat_changed_identifier_as_new_object: true,
            ..DiffOptions::new(true)
        },
    );
    assert_eq!(deltas.len(), 1, "{deltas:#?}");
    assert_eq!(deltas[0].path, vec!["owner".to_string()]);
    assert_eq!(deltas[0].op, DeltaOp::Update);
}

/// Rule 4: a hand-built delta whose value cannot be built at its resolved
/// location (`thread` is a `Bolt` slot; the element is a `Nut`) fails soft —
/// `Ok`, path in `trace.failed`, tree untouched — and the OTHER deltas in the
/// same batch still apply. This is the spike's exact probe.
#[test]
fn unbuildable_delta_fails_soft_without_voiding_the_batch() {
    let f = fixture();
    let golden = f.load(json!({"parts": [nut()]}));
    let bad = Delta {
        path: vec!["parts".to_string(), "B1".to_string(), "thread".to_string()],
        op: DeltaOp::Update,
        old: Some(json!("M8")),
        new: Some(json!("M10")),
    };
    let good = Delta {
        path: vec!["parts".to_string(), "B1".to_string(), "note".to_string()],
        op: DeltaOp::Add,
        old: None,
        new: Some(json!("stocked")),
    };
    let (patched, trace) = patch(
        &golden,
        &[bad.clone(), good.clone()],
        PatchOptions::default(),
    )
    .unwrap();
    assert_eq!(trace.failed, vec![bad.path.clone()]);
    assert!(
        patched.equals(
            &f.load(json!({"parts": [{"typeURI": "part:Nut", "code": "B1",
                                      "pitch": "1.25", "note": "stocked"}]})),
            true
        ),
        "the good delta must still land, the bad one must change nothing: {}",
        patched.to_json()
    );
    assert!(
        patched
            .navigate_path(&["parts".to_string(), "B1".to_string(), "thread".to_string()])
            .is_none(),
        "the unbuildable delta must leave no trace in the tree"
    );
}

/// Rule 4 at the whole-element level: an `Update` carrying a payload no class
/// in the slot's range can accept fails soft too.
#[test]
fn unbuildable_whole_element_update_fails_soft() {
    let f = fixture();
    let golden = f.load(json!({"parts": [nut()]}));
    let bad = Delta {
        path: vec!["parts".to_string(), "B1".to_string()],
        op: DeltaOp::Update,
        old: Some(nut()),
        // a scalar where the slot's range is a class: unbuildable
        new: Some(json!("not-an-object")),
    };
    let (patched, trace) = patch(&golden, std::slice::from_ref(&bad), PatchOptions::default())
        .expect("a bad delta must not void the batch");
    assert_eq!(trace.failed, vec![bad.path.clone()]);
    assert!(patched.equals(&golden, true), "nothing may change");
}
