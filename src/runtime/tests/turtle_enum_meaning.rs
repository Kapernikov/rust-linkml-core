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
fn turtle_enum_without_meaning_emits_the_minted_concept_iri() {
    // A value nobody mapped to an ontology is still a concept: the slot's range
    // is a skos:ConceptScheme, and a bare string is not a member of one. It gets
    // gen-owl's `<enum_uri>#<code>`, the same IRI the schema graph describes.
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

    assert!(
        ttl.contains("<https://example.com/enum-meaning-test/StatusEnum#unknown>"),
        "Enum without meaning should be its minted concept IRI. Got:\n{}",
        ttl
    );
    assert!(
        !ttl.contains("\"unknown\""),
        "…and not also a plain string literal. Got:\n{}",
        ttl
    );
}
