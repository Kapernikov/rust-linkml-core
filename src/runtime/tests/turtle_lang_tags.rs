#![cfg(feature = "ttl")]

use linkml_runtime::{
    load_yaml_file,
    turtle::{turtle_to_string, TurtleOptions},
};
use linkml_schemaview::identifier::{converter_from_schemas, Identifier};
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

fn load_schema_with_types(
    schema_file: &str,
) -> (
    linkml_meta::SchemaDefinition,
    SchemaView,
    linkml_schemaview::Converter,
) {
    let schema = from_yaml(Path::new(&data_path(schema_file))).unwrap();
    let types_schema = from_yaml(Path::new(&data_path("types.yaml"))).unwrap();
    let mut sv = SchemaView::new();
    sv.add_schema(schema.clone()).unwrap();
    sv.add_schema_with_import_ref(
        types_schema.clone(),
        Some((schema.id.clone(), "linkml:types".to_string())),
    )
    .unwrap();
    let conv = converter_from_schemas([&schema, &types_schema]);
    (schema, sv, conv)
}

fn get_station_ttl() -> String {
    let (schema, sv, conv) = load_schema_with_types("lang_tag_schema.yaml");
    let class = sv
        .get_class(&Identifier::new("Station"), &conv)
        .unwrap()
        .unwrap();
    let v = load_yaml_file(
        Path::new(&data_path("lang_tag_data.yaml")),
        &sv,
        &class,
        &conv,
    )
    .unwrap()
    .into_instance()
    .unwrap();
    turtle_to_string(&v, &sv, &schema, &conv, TurtleOptions { skolem: false }).unwrap()
}

// ─── Feature 1: in_language on slot ───

#[test]
fn static_lang_tag_on_slot() {
    let ttl = get_station_ttl();
    // opName has in_language: en, should emit "Brussels North"@en
    assert!(
        ttl.contains("\"Brussels North\"@en"),
        "Expected language-tagged literal @en for opName. Got:\n{}",
        ttl
    );
}

#[test]
fn plain_string_without_in_language() {
    let ttl = get_station_ttl();
    // description has no in_language, should be plain string
    assert!(
        ttl.contains("\"A major station\""),
        "Expected plain string literal for description. Got:\n{}",
        ttl
    );
    // Should NOT have a language tag
    assert!(
        !ttl.contains("\"A major station\"@"),
        "description should not have a language tag. Got:\n{}",
        ttl
    );
}

// ─── Feature 2: lang-tag class collapse via implements ───

#[test]
fn lang_tag_class_collapses_to_tagged_literals() {
    let ttl = get_station_ttl();
    // labels should be collapsed from Object to language-tagged literals
    assert!(
        ttl.contains("\"Brussels North\"@en"),
        "Expected @en label. Got:\n{}",
        ttl
    );
    assert!(
        ttl.contains("\"Bruxelles-Nord\"@fr"),
        "Expected @fr label. Got:\n{}",
        ttl
    );
    assert!(
        ttl.contains("\"Brussel-Noord\"@nl"),
        "Expected @nl label. Got:\n{}",
        ttl
    );
}

#[test]
fn lang_tag_class_no_blank_nodes() {
    let ttl = get_station_ttl();
    // The LangLabel instances should NOT produce blank nodes or nested triples
    assert!(
        !ttl.contains("_:b"),
        "lang-tag class should not produce blank nodes. Got:\n{}",
        ttl
    );
}

#[test]
fn lang_tag_class_no_rdf_type_for_collapsed() {
    let ttl = get_station_ttl();
    // Collapsed lang-tag instances should NOT emit rdf:type for LangLabel
    assert!(
        !ttl.contains("LangLabel"),
        "Collapsed lang-tag class should not emit rdf:type. Got:\n{}",
        ttl
    );
}
