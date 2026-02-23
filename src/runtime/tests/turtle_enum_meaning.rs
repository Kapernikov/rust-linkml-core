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
fn turtle_enum_meaning_emits_named_node() {
    let schema = from_yaml(Path::new(&data_path("enum_meaning_schema.yaml"))).unwrap();
    let mut sv = SchemaView::new();
    sv.add_schema(schema.clone()).unwrap();
    let conv = converter_from_schema(&schema);
    let class = sv
        .get_class(&Identifier::new("Item"), &conv)
        .unwrap()
        .unwrap();
    let v = load_yaml_file(
        Path::new(&data_path("enum_meaning_data.yaml")),
        &sv,
        &class,
        &conv,
    )
    .unwrap()
    .into_instance()
    .unwrap();
    let ttl = turtle_to_string(&v, &sv, &schema, &conv, TurtleOptions { skolem: false }).unwrap();

    // Single-valued enum with meaning should be a named node, not a string literal
    assert!(
        ttl.contains("sstatus:Active") || ttl.contains("<https://example.com/status/Active>"),
        "Expected enum meaning URI (sstatus:Active) for single-valued slot. Got:\n{}",
        ttl
    );
    assert!(
        !ttl.contains("\"active\""),
        "Enum with meaning should NOT be a plain string literal. Got:\n{}",
        ttl
    );

    // Multivalued enum with meaning should also emit named nodes
    assert!(
        ttl.contains("sstatus:Retired") || ttl.contains("<https://example.com/status/Retired>"),
        "Expected enum meaning URI (sstatus:Retired) for multivalued slot. Got:\n{}",
        ttl
    );
    assert!(
        !ttl.contains("\"retired\""),
        "Multivalued enum with meaning should NOT be a plain string literal. Got:\n{}",
        ttl
    );
}

#[test]
fn turtle_enum_without_meaning_emits_literal() {
    // Enum values without a meaning should still be emitted as string literals
    let schema = from_yaml(Path::new(&data_path("enum_meaning_schema.yaml"))).unwrap();
    let mut sv = SchemaView::new();
    sv.add_schema(schema.clone()).unwrap();
    let conv = converter_from_schema(&schema);
    let class = sv
        .get_class(&Identifier::new("Item"), &conv)
        .unwrap()
        .unwrap();
    let v = load_yaml_file(
        Path::new(&data_path("enum_meaning_no_meaning_data.yaml")),
        &sv,
        &class,
        &conv,
    )
    .unwrap()
    .into_instance()
    .unwrap();
    let ttl = turtle_to_string(&v, &sv, &schema, &conv, TurtleOptions { skolem: false }).unwrap();

    // "unknown" has no meaning, should remain a string literal
    assert!(
        ttl.contains("\"unknown\""),
        "Enum without meaning should be a plain string literal. Got:\n{}",
        ttl
    );
}
