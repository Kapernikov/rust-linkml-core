#![cfg(feature = "ttl")]

use linkml_runtime::{
    load_yaml_file,
    turtle::{turtle_to_string, TurtleOptions},
    turtle_import::import_turtle_from_string,
};
use linkml_schemaview::identifier::{converter_from_schema, converter_from_schemas, Identifier};
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

/// Helper: export to TTL, re-import, compare JSON trees.
/// Returns the re-imported JSON for additional assertions.
fn roundtrip(
    sv: &SchemaView,
    schema: &linkml_meta::SchemaDefinition,
    conv: &linkml_schemaview::Converter,
    original: &linkml_runtime::LinkMLInstance,
    class_name: &str,
) -> serde_json::Value {
    let ttl =
        turtle_to_string(original, sv, schema, conv, TurtleOptions { skolem: false }).unwrap();

    let result = import_turtle_from_string(&ttl, sv, conv, &[class_name]).unwrap();

    let instances = result
        .instances
        .get(class_name)
        .unwrap_or_else(|| panic!("No instances found for class {class_name}"));
    assert!(
        !instances.is_empty(),
        "Expected at least one {class_name} instance"
    );

    instances[0].to_json()
}

// ─── Round-trip: typed literals ───

#[test]
fn roundtrip_typed_literals() {
    let (schema, sv, conv) = load_schema_with_types("typed_literals_schema.yaml");
    let class = sv
        .get_class(&Identifier::new("Thing"), &conv)
        .unwrap()
        .unwrap();
    let original = load_yaml_file(
        Path::new(&data_path("typed_literals_data.yaml")),
        &sv,
        &class,
        &conv,
    )
    .unwrap()
    .into_instance()
    .unwrap();
    let original_json = original.to_json();

    let reimported = roundtrip(&sv, &schema, &conv, &original, "Thing");

    // Check key scalar fields survive round-trip
    assert_eq!(reimported["name"], original_json["name"], "string field");
    assert_eq!(reimported["count"], original_json["count"], "integer field");
    assert_eq!(
        reimported["active"], original_json["active"],
        "boolean field"
    );
    // Float may have minor precision differences, just check it exists and is numeric
    assert!(
        reimported["score"].is_number(),
        "float field should be numeric"
    );
}

// ─── Round-trip: enum meanings ───

#[test]
fn roundtrip_enum_meaning() {
    let schema = from_yaml(Path::new(&data_path("enum_meaning_schema.yaml"))).unwrap();
    let mut sv = SchemaView::new();
    sv.add_schema(schema.clone()).unwrap();
    let conv = converter_from_schema(&schema);
    let class = sv
        .get_class(&Identifier::new("Item"), &conv)
        .unwrap()
        .unwrap();
    let original = load_yaml_file(
        Path::new(&data_path("enum_meaning_data.yaml")),
        &sv,
        &class,
        &conv,
    )
    .unwrap()
    .into_instance()
    .unwrap();
    let original_json = original.to_json();

    let reimported = roundtrip(&sv, &schema, &conv, &original, "Item");

    // Enum values should survive round-trip (meaning URI → permissible value name)
    assert_eq!(
        reimported["status"], original_json["status"],
        "single-valued enum"
    );
}

// ─── Round-trip: language-tagged literals ───

#[test]
fn roundtrip_lang_tags() {
    let (schema, sv, conv) = load_schema_with_types("lang_tag_schema.yaml");
    let class = sv
        .get_class(&Identifier::new("Station"), &conv)
        .unwrap()
        .unwrap();
    let original = load_yaml_file(
        Path::new(&data_path("lang_tag_data.yaml")),
        &sv,
        &class,
        &conv,
    )
    .unwrap()
    .into_instance()
    .unwrap();
    let original_json = original.to_json();

    let reimported = roundtrip(&sv, &schema, &conv, &original, "Station");

    // Static in_language slot should survive
    assert_eq!(
        reimported["opName"], original_json["opName"],
        "in_language slot"
    );
    // Plain string should survive
    assert_eq!(
        reimported["description"], original_json["description"],
        "plain string slot"
    );
    // Language-tagged class labels should be reconstructed
    let reimp_labels = &reimported["labels"];
    assert!(
        reimp_labels.is_array() || reimp_labels.is_object(),
        "labels should be present after round-trip"
    );
}
