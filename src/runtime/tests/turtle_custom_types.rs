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

#[test]
fn turtle_wkt_literal_has_geosparql_datatype() {
    let (schema, sv, conv) = load_schema_with_types("custom_type_schema.yaml");
    let class = sv
        .get_class(&Identifier::new("Place"), &conv)
        .unwrap()
        .unwrap();
    let v = load_yaml_file(
        Path::new(&data_path("custom_type_data.yaml")),
        &sv,
        &class,
        &conv,
    )
    .unwrap()
    .into_instance()
    .unwrap();
    let ttl = turtle_to_string(&v, &sv, &schema, &conv, TurtleOptions { skolem: false }).unwrap();

    // wktLiteral should carry ^^geosparql:wktLiteral or the full IRI
    assert!(
        ttl.contains("^^geosparql:wktLiteral")
            || ttl.contains("^^<http://www.opengis.net/ont/geosparql#wktLiteral>"),
        "Expected wktLiteral datatype annotation. Got:\n{}",
        ttl
    );
    // Should NOT be a plain untyped string (would end with just a quote, not ^^)
    assert!(
        !ttl.contains("\"POINT(4.3517 50.8503)\" "),
        "wktLiteral should not be a plain string literal without datatype. Got:\n{}",
        ttl
    );
}

#[test]
fn turtle_string_literal_has_no_datatype() {
    let (schema, sv, conv) = load_schema_with_types("custom_type_schema.yaml");
    let class = sv
        .get_class(&Identifier::new("Place"), &conv)
        .unwrap()
        .unwrap();
    let v = load_yaml_file(
        Path::new(&data_path("custom_type_data.yaml")),
        &sv,
        &class,
        &conv,
    )
    .unwrap()
    .into_instance()
    .unwrap();
    let ttl = turtle_to_string(&v, &sv, &schema, &conv, TurtleOptions { skolem: false }).unwrap();

    // Plain string should NOT have ^^xsd:string annotation
    assert!(
        !ttl.contains("\"Brussels\"^^"),
        "Plain string should not have datatype annotation. Got:\n{}",
        ttl
    );
    assert!(
        ttl.contains("\"Brussels\""),
        "Expected plain string literal. Got:\n{}",
        ttl
    );
}

#[test]
fn turtle_uri_range_emits_named_node() {
    let (schema, sv, conv) = load_schema_with_types("custom_type_schema.yaml");
    let class = sv
        .get_class(&Identifier::new("Place"), &conv)
        .unwrap()
        .unwrap();
    let v = load_yaml_file(
        Path::new(&data_path("custom_type_data.yaml")),
        &sv,
        &class,
        &conv,
    )
    .unwrap()
    .into_instance()
    .unwrap();
    let ttl = turtle_to_string(&v, &sv, &schema, &conv, TurtleOptions { skolem: false }).unwrap();

    // homepage (range: uri) should be a named node, not a string literal
    assert!(
        ttl.contains("<https://www.brussels.be>"),
        "URI range should emit a named node (IRI in angle brackets). Got:\n{}",
        ttl
    );
    assert!(
        !ttl.contains("\"https://www.brussels.be\""),
        "URI range should NOT be a string literal. Got:\n{}",
        ttl
    );
}

#[test]
fn turtle_uriorcurie_range_emits_named_node() {
    let (schema, sv, conv) = load_schema_with_types("custom_type_schema.yaml");
    let class = sv
        .get_class(&Identifier::new("Place"), &conv)
        .unwrap()
        .unwrap();
    let v = load_yaml_file(
        Path::new(&data_path("custom_type_data.yaml")),
        &sv,
        &class,
        &conv,
    )
    .unwrap()
    .into_instance()
    .unwrap();
    let ttl = turtle_to_string(&v, &sv, &schema, &conv, TurtleOptions { skolem: false }).unwrap();

    // related_doc (range: uriorcurie) should be a named node, not a string literal
    assert!(
        ttl.contains("<https://example.com/docs/brussels>"),
        "uriorcurie range should emit a named node. Got:\n{}",
        ttl
    );
    assert!(
        !ttl.contains("\"https://example.com/docs/brussels\""),
        "uriorcurie range should NOT be a string literal. Got:\n{}",
        ttl
    );
}
