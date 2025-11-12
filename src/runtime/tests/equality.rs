use linkml_runtime::{load_json_str, load_yaml_str, LinkMLInstance};
use linkml_schemaview::identifier::converter_from_schema;
use linkml_schemaview::io::from_yaml;
use linkml_schemaview::schemaview::SchemaView;
use std::path::Path;

fn data_path(name: &str) -> std::path::PathBuf {
    let mut p = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    p.push("tests");
    p.push("data");
    p.push(name);
    p
}

fn load_yaml_doc(
    doc: &str,
    sv: &SchemaView,
    class: &linkml_schemaview::schemaview::ClassView,
    conv: &linkml_schemaview::Converter,
) -> LinkMLInstance {
    load_yaml_str(doc, sv, class, conv)
        .unwrap()
        .into_instance()
        .unwrap()
}

fn load_json_doc(
    doc: &str,
    sv: &SchemaView,
    class: &linkml_schemaview::schemaview::ClassView,
    conv: &linkml_schemaview::Converter,
) -> LinkMLInstance {
    load_json_str(doc, sv, class, conv)
        .unwrap()
        .into_instance()
        .unwrap()
}

#[test]
fn object_equality_ignores_null_assignments() {
    // Load personinfo schema and Container class
    let schema = from_yaml(Path::new(&data_path("personinfo.yaml"))).unwrap();
    let mut sv = SchemaView::new();
    sv.add_schema(schema.clone()).unwrap();
    let conv = converter_from_schema(&schema);
    let container = sv
        .get_class(
            &linkml_schemaview::identifier::Identifier::new("Container"),
            &conv,
        )
        .unwrap()
        .expect("class not found");

    let doc_with_null = r#"
objects:
  - objecttype: personinfo:Person
    id: "P:1"
    name: "Alice"
    current_address: null
"#;
    let doc_without_slot = r#"
objects:
  - objecttype: personinfo:Person
    id: "P:1"
    name: "Alice"
"#;
    let v1 = load_yaml_doc(doc_with_null, &sv, &container, &conv);
    let v2 = load_yaml_doc(doc_without_slot, &sv, &container, &conv);
    let p1 = v1.navigate_path(["objects", "0"]).unwrap();
    let p2 = v2.navigate_path(["objects", "0"]).unwrap();
    assert!(
        p1.equals(p2, true),
        "Person with null assignment should equal omission"
    );
}

#[test]
fn list_identity_is_order_sensitive() {
    let schema = from_yaml(Path::new(&data_path("personinfo.yaml"))).unwrap();
    let mut sv = SchemaView::new();
    sv.add_schema(schema.clone()).unwrap();
    let conv = converter_from_schema(&schema);
    let container = sv
        .get_class(
            &linkml_schemaview::identifier::Identifier::new("Container"),
            &conv,
        )
        .unwrap()
        .expect("class not found");

    let doc_a = r#"
objects:
  - objecttype: personinfo:Person
    id: "P:1"
    name: "Alice"
    has_employment_history:
      - started_at_time: 2019-01-01
        is_current: true
      - started_at_time: 2020-01-01
        is_current: false
"#;
    let doc_b = r#"
objects:
  - objecttype: personinfo:Person
    id: "P:1"
    name: "Alice"
    has_employment_history:
      - started_at_time: 2020-01-01
        is_current: false
      - started_at_time: 2019-01-01
        is_current: true
"#;
    let v1 = load_yaml_doc(doc_a, &sv, &container, &conv);
    let v2 = load_yaml_doc(doc_b, &sv, &container, &conv);
    let p1 = v1.navigate_path(["objects", "0"]).unwrap();
    let p2 = v2.navigate_path(["objects", "0"]).unwrap();
    assert!(matches!(p1, LinkMLInstance::Object { .. }));
    assert!(matches!(p2, LinkMLInstance::Object { .. }));
    assert!(!p1.equals(p2, true), "List order must affect equality");
}

#[test]
fn mapping_equality_is_key_based_not_ordered() {
    // Load mapping schema and Bag class
    let schema = from_yaml(Path::new(&data_path("mapping_schema.yaml"))).unwrap();
    let mut sv = SchemaView::new();
    sv.add_schema(schema.clone()).unwrap();
    let conv = converter_from_schema(&schema);
    let bag = sv
        .get_class(
            &linkml_schemaview::identifier::Identifier::new("Bag"),
            &conv,
        )
        .unwrap()
        .expect("class not found");

    let doc1 = r#"{
  "things": {
    "alpha": {"typeURI": "ThingA", "a_only": "foo", "common": "shared"},
    "beta":  {"typeURI": "ThingB", "b_only": true,   "common": "shared"}
  }
}"#;
    let doc2 = r#"{
  "things": {
    "beta":  {"typeURI": "ThingB", "b_only": true,   "common": "shared"},
    "alpha": {"typeURI": "ThingA", "a_only": "foo", "common": "shared"}
  }
}"#;
    let v1 = load_json_doc(doc1, &sv, &bag, &conv);
    let v2 = load_json_doc(doc2, &sv, &bag, &conv);
    let m1 = v1.navigate_path(["things"]).unwrap();
    let m2 = v2.navigate_path(["things"]).unwrap();
    assert!(matches!(m1, LinkMLInstance::Mapping { .. }));
    assert!(matches!(m2, LinkMLInstance::Mapping { .. }));
    assert!(
        m1.equals(m2, true),
        "Mapping equality should ignore key order"
    );
}

#[test]
fn enum_scalar_equality_respects_value_and_range() {
    let schema = from_yaml(Path::new(&data_path("personinfo.yaml"))).unwrap();
    let mut sv = SchemaView::new();
    sv.add_schema(schema.clone()).unwrap();
    let conv = converter_from_schema(&schema);
    let container = sv
        .get_class(
            &linkml_schemaview::identifier::Identifier::new("Container"),
            &conv,
        )
        .unwrap()
        .expect("class not found");

    let doc1 = r#"
objects:
  - objecttype: personinfo:Person
    id: "P:1"
    name: "Alice"
    gender: "cisgender man"
"#;
    let doc2 = r#"
objects:
  - objecttype: personinfo:Person
    id: "P:2"
    name: "Bob"
    gender: "cisgender man"
"#;
    let doc3 = r#"
objects:
  - objecttype: personinfo:Person
    id: "P:3"
    name: "Carol"
    gender: "cisgender woman"
"#;
    let v1 = load_yaml_doc(doc1, &sv, &container, &conv);
    let v2 = load_yaml_doc(doc2, &sv, &container, &conv);
    let v3 = load_yaml_doc(doc3, &sv, &container, &conv);
    let g1 = v1.navigate_path(["objects", "0", "gender"]).unwrap();
    let g2 = v2.navigate_path(["objects", "0", "gender"]).unwrap();
    let g3 = v3.navigate_path(["objects", "0", "gender"]).unwrap();
    assert!(g1.equals(g2, true));
    assert!(!g1.equals(g3, true));
}
