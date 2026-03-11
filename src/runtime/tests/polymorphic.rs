use linkml_runtime::LinkMLInstance;
use linkml_runtime::{load_yaml_file, validate};
use linkml_schemaview::identifier::{converter_from_schema, Identifier};
use linkml_schemaview::io::from_yaml;
use linkml_schemaview::schemaview::SchemaView;
use serde_json::Value as JsonValue;
use std::path::{Path, PathBuf};

fn data_path(name: &str) -> PathBuf {
    let mut p = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    p.push("tests");
    p.push("data");
    p.push(name);
    p
}

#[test]
fn polymorphism_with_type() {
    let schema = from_yaml(Path::new(&data_path("poly_schema.yaml"))).unwrap();
    let mut sv = SchemaView::new();
    sv.add_schema(schema.clone()).unwrap();
    let conv = converter_from_schema(&schema);
    let class = sv
        .get_class(&Identifier::new("Container"), &conv)
        .unwrap()
        .expect("class not found");
    let v = load_yaml_file(
        Path::new(&data_path("poly_with_type.yaml")),
        &sv,
        &class,
        &conv,
    )
    .unwrap()
    .into_instance()
    .unwrap();
    assert!(validate(&v).is_ok());
}

#[test]
fn polymorphism_without_type() {
    let schema = from_yaml(Path::new(&data_path("poly_schema.yaml"))).unwrap();
    let mut sv = SchemaView::new();
    sv.add_schema(schema.clone()).unwrap();
    let conv = converter_from_schema(&schema);
    let class = sv
        .get_class(&Identifier::new("Container"), &conv)
        .unwrap()
        .expect("class not found");
    let v = load_yaml_file(
        Path::new(&data_path("poly_without_type.yaml")),
        &sv,
        &class,
        &conv,
    )
    .unwrap()
    .into_instance()
    .unwrap();
    assert!(validate(&v).is_ok());
}

#[test]
fn root_polymorphism_with_type() {
    let schema = from_yaml(Path::new(&data_path("poly_schema.yaml"))).unwrap();
    let mut sv = SchemaView::new();
    sv.add_schema(schema.clone()).unwrap();
    let conv = converter_from_schema(&schema);
    let class = sv
        .get_class(&Identifier::new("Parent"), &conv)
        .unwrap()
        .expect("class not found");
    let v = load_yaml_file(
        Path::new(&data_path("root_with_type.yaml")),
        &sv,
        &class,
        &conv,
    )
    .unwrap()
    .into_instance()
    .unwrap();
    assert!(validate(&v).is_ok());
}

#[test]
fn root_polymorphism_without_type() {
    let schema = from_yaml(Path::new(&data_path("poly_schema.yaml"))).unwrap();
    let mut sv = SchemaView::new();
    sv.add_schema(schema.clone()).unwrap();
    let conv = converter_from_schema(&schema);
    let class = sv
        .get_class(&Identifier::new("Parent"), &conv)
        .unwrap()
        .expect("class not found");
    let v = load_yaml_file(
        Path::new(&data_path("root_without_type.yaml")),
        &sv,
        &class,
        &conv,
    )
    .unwrap()
    .into_instance()
    .unwrap();
    assert!(validate(&v).is_ok());
}

#[test]
fn array_polymorphism() {
    let schema = from_yaml(Path::new(&data_path("poly_schema.yaml"))).unwrap();
    let mut sv = SchemaView::new();
    sv.add_schema(schema.clone()).unwrap();
    let conv = converter_from_schema(&schema);
    let class = sv
        .get_class(&Identifier::new("Container"), &conv)
        .unwrap()
        .expect("class not found");
    let v = load_yaml_file(Path::new(&data_path("poly_array.yaml")), &sv, &class, &conv)
        .unwrap()
        .into_instance()
        .unwrap();
    assert!(validate(&v).is_ok());
    if let LinkMLInstance::Object { values, .. } = v {
        let objs = values.get("objs").expect("objs not found");
        if let LinkMLInstance::List { values: arr, .. } = objs {
            assert_eq!(arr.len(), 3);
            match &arr[0] {
                LinkMLInstance::Object { class, .. } => assert_eq!(class.name(), "Child"),
                _ => panic!("expected map"),
            }
            match &arr[1] {
                LinkMLInstance::Object { class, .. } => assert_eq!(class.name(), "Child"),
                _ => panic!("expected map"),
            }
            match &arr[2] {
                LinkMLInstance::Object { class, .. } => assert_eq!(class.name(), "Parent"),
                _ => panic!("expected map"),
            }
        } else {
            panic!("expected list");
        }
    } else {
        panic!("expected map");
    }
}

/// When data is loaded without an explicit type designator value,
/// `to_json()` should auto-populate it based on the resolved class.
#[test]
fn to_json_populates_designates_type_when_missing() {
    let schema = from_yaml(Path::new(&data_path("poly_schema.yaml"))).unwrap();
    let mut sv = SchemaView::new();
    sv.add_schema(schema.clone()).unwrap();
    let conv = converter_from_schema(&schema);

    // poly_without_type.yaml has a Child object without a "type" field
    let class = sv
        .get_class(&Identifier::new("Container"), &conv)
        .unwrap()
        .expect("class not found");
    let v = load_yaml_file(
        Path::new(&data_path("poly_without_type.yaml")),
        &sv,
        &class,
        &conv,
    )
    .unwrap()
    .into_instance()
    .unwrap();

    let json = v.to_json();
    let obj = json.get("obj").expect("obj not found");
    assert_eq!(
        obj.get("type"),
        Some(&JsonValue::String("Child".to_string())),
        "designates_type slot should be auto-populated with the class name"
    );
}

/// When data already has an explicit type designator value,
/// `to_json()` should preserve it as-is rather than overwriting.
#[test]
fn to_json_preserves_existing_designates_type() {
    let schema = from_yaml(Path::new(&data_path("poly_schema.yaml"))).unwrap();
    let mut sv = SchemaView::new();
    sv.add_schema(schema.clone()).unwrap();
    let conv = converter_from_schema(&schema);

    // poly_with_type.yaml has a Child object with "type: Child" already set
    let class = sv
        .get_class(&Identifier::new("Container"), &conv)
        .unwrap()
        .expect("class not found");
    let v = load_yaml_file(
        Path::new(&data_path("poly_with_type.yaml")),
        &sv,
        &class,
        &conv,
    )
    .unwrap()
    .into_instance()
    .unwrap();

    let json = v.to_json();
    let obj = json.get("obj").expect("obj not found");
    assert_eq!(
        obj.get("type"),
        Some(&JsonValue::String("Child".to_string())),
        "existing designates_type value should be preserved"
    );
}

/// Round-trip: load without type → to_json (auto-populates type) → re-parse
/// should resolve back to the same class.
#[test]
fn to_json_designates_type_round_trip() {
    let schema = from_yaml(Path::new(&data_path("poly_schema.yaml"))).unwrap();
    let mut sv = SchemaView::new();
    sv.add_schema(schema.clone()).unwrap();
    let conv = converter_from_schema(&schema);

    // Load without type — resolved to Child via slot-based heuristics
    let class = sv
        .get_class(&Identifier::new("Container"), &conv)
        .unwrap()
        .expect("class not found");
    let v = load_yaml_file(
        Path::new(&data_path("poly_without_type.yaml")),
        &sv,
        &class,
        &conv,
    )
    .unwrap()
    .into_instance()
    .unwrap();

    // Dump to JSON (should now have "type": "Child")
    let json = v.to_json();

    // Re-parse from JSON
    let v2 = LinkMLInstance::from_json(json, class, None, &sv, &conv, false)
        .into_instance()
        .unwrap();
    if let LinkMLInstance::Object { values, .. } = &v2 {
        let inner = values.get("obj").expect("obj not found");
        if let LinkMLInstance::Object { class, .. } = inner {
            assert_eq!(class.name(), "Child", "round-trip should preserve class");
        } else {
            panic!("expected object");
        }
    } else {
        panic!("expected object");
    }
}
