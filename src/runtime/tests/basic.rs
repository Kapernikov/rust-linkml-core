use linkml_runtime::{load_yaml_file, validate};
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

#[test]
fn validate_person_ok() {
    let schema = from_yaml(Path::new(&data_path("schema.yaml"))).unwrap();
    let mut sv = SchemaView::new();
    sv.add_schema(schema.clone()).unwrap();
    let conv = converter_from_schema(&schema);
    let class = sv
        .get_class(&Identifier::new("Person"), &conv)
        .unwrap()
        .expect("class not found");
    let v = load_yaml_file(
        Path::new(&data_path("person_valid.yaml")),
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
fn validate_person_fail() {
    let schema = from_yaml(Path::new(&data_path("schema.yaml"))).unwrap();
    let mut sv = SchemaView::new();
    sv.add_schema(schema.clone()).unwrap();
    let conv = converter_from_schema(&schema);
    let class = sv
        .get_class(&Identifier::new("Person"), &conv)
        .unwrap()
        .expect("class not found");
    let outcome = load_yaml_file(
        Path::new(&data_path("person_invalid.yaml")),
        &sv,
        &class,
        &conv,
    )
    .unwrap();
    assert!(outcome.has_errors());
}
