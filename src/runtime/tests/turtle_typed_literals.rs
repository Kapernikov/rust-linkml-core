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
fn turtle_integer_literal_has_xsd_type() {
    let schema = from_yaml(Path::new(&data_path("typed_literals_schema.yaml"))).unwrap();
    let mut sv = SchemaView::new();
    sv.add_schema(schema.clone()).unwrap();
    let conv = converter_from_schema(&schema);
    let class = sv
        .get_class(&Identifier::new("Thing"), &conv)
        .unwrap()
        .unwrap();
    let v = load_yaml_file(
        Path::new(&data_path("typed_literals_data.yaml")),
        &sv,
        &class,
        &conv,
    )
    .unwrap()
    .into_instance()
    .unwrap();
    let ttl = turtle_to_string(&v, &sv, &schema, &conv, TurtleOptions { skolem: false }).unwrap();

    // Integer should be typed - either native Turtle syntax (42) or explicit (\"42\"^^xsd:integer)
    // Native Turtle integers are valid and equivalent to xsd:integer
    let has_typed_int = ttl.contains("\"42\"^^xsd:integer")
        || ttl.contains("\"42\"^^<http://www.w3.org/2001/XMLSchema#integer>")
        || ttl.contains("typed:count 42"); // native Turtle integer syntax
    assert!(
        has_typed_int,
        "Expected integer literal (native or typed), got:\n{}",
        ttl
    );
    // Should NOT be a plain string
    assert!(
        !ttl.contains("typed:count \"42\""),
        "Integer should not be a plain string literal, got:\n{}",
        ttl
    );
}

#[test]
fn turtle_float_literal_has_xsd_type() {
    let schema = from_yaml(Path::new(&data_path("typed_literals_schema.yaml"))).unwrap();
    let mut sv = SchemaView::new();
    sv.add_schema(schema.clone()).unwrap();
    let conv = converter_from_schema(&schema);
    let class = sv
        .get_class(&Identifier::new("Thing"), &conv)
        .unwrap()
        .unwrap();
    let v = load_yaml_file(
        Path::new(&data_path("typed_literals_data.yaml")),
        &sv,
        &class,
        &conv,
    )
    .unwrap()
    .into_instance()
    .unwrap();
    let ttl = turtle_to_string(&v, &sv, &schema, &conv, TurtleOptions { skolem: false }).unwrap();

    // Float should have xsd:float type annotation
    assert!(
        ttl.contains("\"3.14\"^^xsd:float")
            || ttl.contains("\"3.14\"^^<http://www.w3.org/2001/XMLSchema#float>"),
        "Expected float literal with xsd:float type, got:\n{}",
        ttl
    );
}

#[test]
fn turtle_boolean_literal_has_xsd_type() {
    let schema = from_yaml(Path::new(&data_path("typed_literals_schema.yaml"))).unwrap();
    let mut sv = SchemaView::new();
    sv.add_schema(schema.clone()).unwrap();
    let conv = converter_from_schema(&schema);
    let class = sv
        .get_class(&Identifier::new("Thing"), &conv)
        .unwrap()
        .unwrap();
    let v = load_yaml_file(
        Path::new(&data_path("typed_literals_data.yaml")),
        &sv,
        &class,
        &conv,
    )
    .unwrap()
    .into_instance()
    .unwrap();
    let ttl = turtle_to_string(&v, &sv, &schema, &conv, TurtleOptions { skolem: false }).unwrap();

    // Boolean should be typed - either native Turtle syntax (true) or explicit (\"true\"^^xsd:boolean)
    // Native Turtle booleans are valid and equivalent to xsd:boolean
    let has_typed_bool = ttl.contains("\"true\"^^xsd:boolean")
        || ttl.contains("\"true\"^^<http://www.w3.org/2001/XMLSchema#boolean>")
        || ttl.contains("typed:active true"); // native Turtle boolean syntax
    assert!(
        has_typed_bool,
        "Expected boolean literal (native or typed), got:\n{}",
        ttl
    );
    // Should NOT be a plain string
    assert!(
        !ttl.contains("typed:active \"true\""),
        "Boolean should not be a plain string literal, got:\n{}",
        ttl
    );
}

#[test]
fn turtle_predicates_use_expanded_prefix() {
    let schema = from_yaml(Path::new(&data_path("typed_literals_schema.yaml"))).unwrap();
    let mut sv = SchemaView::new();
    sv.add_schema(schema.clone()).unwrap();
    let conv = converter_from_schema(&schema);
    let class = sv
        .get_class(&Identifier::new("Thing"), &conv)
        .unwrap()
        .unwrap();
    let v = load_yaml_file(
        Path::new(&data_path("typed_literals_data.yaml")),
        &sv,
        &class,
        &conv,
    )
    .unwrap()
    .into_instance()
    .unwrap();
    let ttl = turtle_to_string(&v, &sv, &schema, &conv, TurtleOptions { skolem: false }).unwrap();

    // Predicates should use the typed: prefix WITHOUT angle brackets
    // Current bug: outputs <typed:name> (CURIE as literal IRI in brackets)
    // Correct: typed:name (using declared prefix) or <https://example.com/typed/name> (expanded)

    // Should NOT have angle brackets around CURIEs like <typed:name>
    assert!(
        !ttl.contains("<typed:name>"),
        "Predicates should not be CURIEs in angle brackets like <typed:name>. Got:\n{}",
        ttl
    );

    // Should use proper prefix notation: typed:name (without angle brackets)
    assert!(
        ttl.contains(" typed:name ") || ttl.contains("\ttyped:name "),
        "Expected 'typed:name' predicate (without angle brackets). Got:\n{}",
        ttl
    );
}

#[test]
fn turtle_slot_uri_overrides_default_prefix() {
    // Test that slots with explicit slot_uri use that URI instead of the default prefix
    let schema = from_yaml(Path::new(&data_path("slot_uri_schema.yaml"))).unwrap();
    let mut sv = SchemaView::new();
    sv.add_schema(schema.clone()).unwrap();
    let conv = converter_from_schema(&schema);
    let class = sv
        .get_class(&Identifier::new("Thing"), &conv)
        .unwrap()
        .unwrap();
    let v = load_yaml_file(
        Path::new(&data_path("slot_uri_data.yaml")),
        &sv,
        &class,
        &conv,
    )
    .unwrap()
    .into_instance()
    .unwrap();
    let ttl = turtle_to_string(&v, &sv, &schema, &conv, TurtleOptions { skolem: false }).unwrap();

    // 'name' has no slot_uri, should use default prefix: sloturi:name
    assert!(
        ttl.contains("sloturi:name") || ttl.contains("<https://example.com/sloturi/name>"),
        "Expected 'sloturi:name' for slot without slot_uri. Got:\n{}",
        ttl
    );

    // 'label' has slot_uri: rdfs:label, should use that
    assert!(
        ttl.contains("rdfs:label") || ttl.contains("<http://www.w3.org/2000/01/rdf-schema#label>"),
        "Expected 'rdfs:label' for slot with slot_uri. Got:\n{}",
        ttl
    );

    // 'definition' has slot_uri: skos:definition, should use that
    assert!(
        ttl.contains("skos:definition")
            || ttl.contains("<http://www.w3.org/2004/02/skos/core#definition>"),
        "Expected 'skos:definition' for slot with slot_uri. Got:\n{}",
        ttl
    );

    // Should NOT use default prefix for slots with explicit slot_uri
    assert!(
        !ttl.contains("sloturi:label"),
        "Slot with slot_uri should NOT use default prefix. Got:\n{}",
        ttl
    );
}

#[test]
fn turtle_inherited_slots_use_originating_schema_prefix() {
    // Test that slots inherited from imported schemas use their originating schema's default_prefix
    // base.yaml has default_prefix: base, defines base_attr
    // derived.yaml has default_prefix: derived, imports base, defines derived_attr
    // DerivedClass inherits from BaseClass
    // Expected: base_attr uses base:, derived_attr uses derived:

    // Manually load both schemas with proper import relationship
    let base_schema = from_yaml(Path::new(&data_path("cross_schema_prefix_base.yaml"))).unwrap();
    let derived_schema =
        from_yaml(Path::new(&data_path("cross_schema_prefix_derived.yaml"))).unwrap();

    let mut sv = SchemaView::new();
    // Add derived first, then base with import reference
    sv.add_schema(derived_schema.clone()).unwrap();
    sv.add_schema_with_import_ref(
        base_schema,
        Some((
            derived_schema.id.clone(),
            "./cross_schema_prefix_base".to_string(),
        )),
    )
    .unwrap();

    let conv = converter_from_schema(&derived_schema);
    let class = sv
        .get_class(&Identifier::new("DerivedClass"), &conv)
        .unwrap()
        .unwrap();
    let v = load_yaml_file(
        Path::new(&data_path("cross_schema_prefix_data.yaml")),
        &sv,
        &class,
        &conv,
    )
    .unwrap()
    .into_instance()
    .unwrap();
    let ttl = turtle_to_string(
        &v,
        &sv,
        &derived_schema,
        &conv,
        TurtleOptions { skolem: false },
    )
    .unwrap();

    // derived_attr should use derived: prefix (defined in derived.yaml)
    assert!(
        ttl.contains("derived:derived_attr")
            || ttl.contains("<https://example.com/derived/derived_attr>"),
        "Slot defined in derived schema should use derived: prefix. Got:\n{}",
        ttl
    );

    // base_attr should use base: prefix (defined in base.yaml), NOT derived:
    assert!(
        ttl.contains("base:base_attr") || ttl.contains("<https://example.com/base/base_attr>"),
        "Slot inherited from base schema should use base: prefix. Got:\n{}",
        ttl
    );

    // Specifically, base_attr should NOT use derived: prefix
    assert!(
        !ttl.contains("derived:base_attr"),
        "Inherited slot should NOT use the importing schema's prefix. Got:\n{}",
        ttl
    );
}

#[test]
fn turtle_builtin_prefix_declared_when_used() {
    // Regression test: a slot with slot_uri set to the expanded rdfs:label URI
    // (http://www.w3.org/2000/01/rdf-schema#label) gets compressed to the CURIE
    // rdfs:label because the Converter has rdfs as a built-in prefix.  The schema
    // itself does NOT declare rdfs in its prefixes block.  The @prefix rdfs: header
    // must still be emitted.
    let schema = from_yaml(Path::new(&data_path("builtin_prefix_schema.yaml"))).unwrap();
    let mut sv = SchemaView::new();
    sv.add_schema(schema.clone()).unwrap();
    let conv = converter_from_schema(&schema);
    let class = sv
        .get_class(&Identifier::new("Thing"), &conv)
        .unwrap()
        .unwrap();
    let v = load_yaml_file(
        Path::new(&data_path("builtin_prefix_data.yaml")),
        &sv,
        &class,
        &conv,
    )
    .unwrap()
    .into_instance()
    .unwrap();
    let ttl = turtle_to_string(&v, &sv, &schema, &conv, TurtleOptions { skolem: false }).unwrap();

    // The predicate must be compressed to rdfs:label (not the full IRI)
    assert!(
        ttl.contains("rdfs:label"),
        "Expected rdfs:label CURIE in body. Got:\n{}",
        ttl
    );

    // The @prefix rdfs: declaration MUST be present
    assert!(
        ttl.contains("@prefix rdfs:"),
        "Expected @prefix rdfs: declaration for built-in prefix used via slot_uri. Got:\n{}",
        ttl
    );
}
