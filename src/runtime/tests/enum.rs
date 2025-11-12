use linkml_runtime::{
    load_yaml_file,
    load_yaml_file_with_diagnostics,
    validate,
    DiagnosticCode,
    LinkMLInstance,
};
use linkml_schemaview::identifier::{converter_from_schema, Identifier};
use linkml_schemaview::io::from_yaml;
use linkml_schemaview::schemaview::SchemaView;
use std::path::{Path, PathBuf};

fn data_path(name: &str) -> PathBuf {
    let mut p = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    p.push("tests");
    p.push("data");
    p.push(name);
    p
}

// Schema has a class with an attribute whose range is an enum.
// Valid instance should accept a permissible enum value; invalid should be rejected.

#[test]
fn enum_valid_value() {
    let schema = from_yaml(Path::new(&data_path("enum_schema.yaml"))).unwrap();
    let mut sv = SchemaView::new();
    sv.add_schema(schema.clone()).unwrap();
    let conv = converter_from_schema(&schema);
    let class = sv
        .get_class(&Identifier::new("Item"), &conv)
        .unwrap()
        .expect("class not found");

    let v = load_yaml_file(Path::new(&data_path("enum_valid.yaml")), &sv, &class, &conv)
        .expect("failed to load valid enum instance");

    // Once enum support is implemented, validate should succeed.
    assert!(validate(&v).is_ok());

    // Sanity check parsed value shape
    if let LinkMLInstance::Object { values, .. } = v {
        let status = values.get("status").expect("status not found");
        match status {
            LinkMLInstance::Scalar { value, .. } => {
                assert_eq!(value.as_str(), Some("active"));
            }
            _ => panic!("expected scalar for status"),
        }
    } else {
        panic!("expected map root for Item");
    }
}

#[test]
fn enum_invalid_value() {
    let schema = from_yaml(Path::new(&data_path("enum_schema.yaml"))).unwrap();
    let mut sv = SchemaView::new();
    sv.add_schema(schema.clone()).unwrap();
    let conv = converter_from_schema(&schema);
    let class = sv
        .get_class(&Identifier::new("Item"), &conv)
        .unwrap()
        .expect("class not found");

    let outcome = load_yaml_file_with_diagnostics(
        Path::new(&data_path("enum_invalid.yaml")),
        &sv,
        &class,
        &conv,
    )
    .expect("failed to load invalid enum instance (parsing should still succeed)");

    assert!(outcome
        .diagnostics
        .iter()
        .any(|d| matches!(d.code, DiagnosticCode::InvalidEnumValue)));
}
