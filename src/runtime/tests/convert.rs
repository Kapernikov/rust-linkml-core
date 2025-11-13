#![cfg(feature = "ttl")]

use linkml_runtime::{
    load_yaml_file,
    turtle::{turtle_to_string, TurtleOptions},
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

#[test]
fn convert_person_to_ttl() {
    let schema = from_yaml(Path::new(&data_path("schema.yaml"))).unwrap();
    let mut sv = SchemaView::new();
    sv.add_schema(schema.clone()).unwrap();
    let conv = converter_from_schema(&schema);
    let class = sv
        .get_class(&Identifier::new("Person"), &conv)
        .unwrap()
        .unwrap();
    let v = load_yaml_file(
        Path::new(&data_path("person_valid.yaml")),
        &sv,
        &class,
        &conv,
    )
    .unwrap()
    .into_instance()
    .unwrap();
    let ttl = turtle_to_string(&v, &sv, &schema, &conv, TurtleOptions { skolem: false }).unwrap();
    assert!(ttl.contains("@prefix test: <https://example.com/test/> ."));
    assert!(ttl.contains("<test:name> \"Alice\""));
}

#[test]
fn suppress_objecttype_triple() {
    let schema = from_yaml(Path::new(&data_path("personinfo.yaml"))).unwrap();
    let mut sv = SchemaView::new();
    sv.add_schema(schema.clone()).unwrap();
    let conv = converter_from_schema(&schema);
    let container = sv
        .get_class(&Identifier::new("Container"), &conv)
        .unwrap()
        .expect("class not found");
    let v = load_yaml_file(
        Path::new(&data_path("example_personinfo_data.yaml")),
        &sv,
        &container,
        &conv,
    )
    .unwrap()
    .into_instance()
    .unwrap();
    let ttl = turtle_to_string(&v, &sv, &schema, &conv, TurtleOptions { skolem: false }).unwrap();
    assert!(!ttl.contains("<personinfo:objecttype>"));
}
