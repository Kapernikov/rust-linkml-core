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

/// Helper: get the RangeInfo for a named attribute on a class.
fn range_info_for_slot(
    sv: &SchemaView,
    conv: &linkml_schemaview::Converter,
    class_name: &str,
    slot_name: &str,
) -> linkml_schemaview::slotview::RangeInfo {
    let class = sv
        .get_class(&Identifier::new(class_name), conv)
        .unwrap()
        .unwrap();
    let slot = class
        .slots()
        .iter()
        .find(|s| s.name == slot_name)
        .unwrap_or_else(|| panic!("slot '{}' not found on class '{}'", slot_name, class_name));
    slot.get_range_info()
        .first()
        .cloned()
        .unwrap_or_else(|| panic!("no RangeInfo for slot '{}'", slot_name))
}

#[test]
fn string_range_has_no_datatype() {
    let (_schema, sv, conv) = load_schema_with_types("rdf_type_schema.yaml");
    let ri = range_info_for_slot(&sv, &conv, "Thing", "name");
    assert_eq!(
        ri.rdf_datatype_iri, None,
        "string should suppress xsd:string"
    );
    assert!(!ri.is_range_iri);
}

#[test]
fn integer_range_has_xsd_integer() {
    let (_schema, sv, conv) = load_schema_with_types("rdf_type_schema.yaml");
    let ri = range_info_for_slot(&sv, &conv, "Thing", "count");
    assert_eq!(
        ri.rdf_datatype_iri.as_deref(),
        Some("http://www.w3.org/2001/XMLSchema#integer")
    );
    assert!(!ri.is_range_iri);
}

#[test]
fn float_range_has_xsd_float() {
    let (_schema, sv, conv) = load_schema_with_types("rdf_type_schema.yaml");
    let ri = range_info_for_slot(&sv, &conv, "Thing", "score");
    assert_eq!(
        ri.rdf_datatype_iri.as_deref(),
        Some("http://www.w3.org/2001/XMLSchema#float")
    );
    assert!(!ri.is_range_iri);
}

#[test]
fn double_range_has_xsd_double() {
    let (_schema, sv, conv) = load_schema_with_types("rdf_type_schema.yaml");
    let ri = range_info_for_slot(&sv, &conv, "Thing", "precise");
    assert_eq!(
        ri.rdf_datatype_iri.as_deref(),
        Some("http://www.w3.org/2001/XMLSchema#double")
    );
    assert!(!ri.is_range_iri);
}

#[test]
fn wkt_literal_has_geosparql_datatype() {
    let (_schema, sv, conv) = load_schema_with_types("rdf_type_schema.yaml");
    let ri = range_info_for_slot(&sv, &conv, "Thing", "location");
    assert_eq!(
        ri.rdf_datatype_iri.as_deref(),
        Some("http://www.opengis.net/ont/geosparql#wktLiteral")
    );
    assert!(!ri.is_range_iri);
}

#[test]
fn uri_range_is_iri() {
    let (_schema, sv, conv) = load_schema_with_types("rdf_type_schema.yaml");
    let ri = range_info_for_slot(&sv, &conv, "Thing", "homepage");
    assert_eq!(
        ri.rdf_datatype_iri, None,
        "IRI ranges should not have a datatype"
    );
    assert!(ri.is_range_iri, "uri range should be flagged as IRI");
}

#[test]
fn uriorcurie_range_is_iri() {
    let (_schema, sv, conv) = load_schema_with_types("rdf_type_schema.yaml");
    let ri = range_info_for_slot(&sv, &conv, "Thing", "see_also");
    assert_eq!(
        ri.rdf_datatype_iri, None,
        "IRI ranges should not have a datatype"
    );
    assert!(ri.is_range_iri, "uriorcurie range should be flagged as IRI");
}

/// Every XSD numeric datatype counts as numeric, not only the four that need
/// JSON canonicalisation (`is_integer` / `is_floating_point`).
#[test]
fn every_xsd_numeric_datatype_is_numeric() {
    let (_schema, sv, conv) = load_schema_with_types("rdf_type_schema.yaml");
    for slot in [
        "as_int",
        "as_long",
        "as_short",
        "as_byte",
        "as_unsigned_int",
        "as_unsigned_long",
        "as_unsigned_short",
        "as_unsigned_byte",
        "as_non_negative_integer",
        "as_positive_integer",
        "as_negative_integer",
        "as_non_positive_integer",
        "as_integer",
        "as_float",
        "as_double",
        "as_decimal",
    ] {
        let ri = range_info_for_slot(&sv, &conv, "Numbers", slot);
        assert!(
            ri.is_numeric(),
            "'{}' (datatype {:?}) should be numeric",
            slot,
            ri.rdf_datatype_iri
        );
    }
}

/// A schema-defined subtype carrying its own datatype IRI is numeric through
/// the IRI path — nothing about the name "trackLength" says it is a number.
#[test]
fn schema_defined_numeric_subtype_is_numeric_through_its_datatype() {
    let (_schema, sv, conv) = load_schema_with_types("rdf_type_schema.yaml");
    let ri = range_info_for_slot(&sv, &conv, "Numbers", "as_track_length");
    assert_eq!(
        ri.rdf_datatype_iri.as_deref(),
        Some("http://www.w3.org/2001/XMLSchema#int")
    );
    assert!(ri.is_numeric());
    // `is_integer` answers a narrower question — which JSON canonicalisation to
    // apply — and xsd:int is not xsd:integer, so it stays false.
    assert!(!ri.is_integer());
    assert!(!ri.is_floating_point());
}

#[test]
fn non_numeric_ranges_are_not_numeric() {
    let (_schema, sv, conv) = load_schema_with_types("rdf_type_schema.yaml");
    for slot in [
        "name",     // string
        "truthy",   // boolean
        "when",     // date
        "see_also", // uriorcurie
        "location", // a custom string subtype
        "status",   // an enum range
        "target",   // a class range
    ] {
        let ri = range_info_for_slot(&sv, &conv, "Thing", slot);
        assert!(!ri.is_numeric(), "'{}' should not be numeric", slot);
    }
}

/// The case a local list of datatype IRIs gets wrong: with no `types:` block
/// there is no datatype IRI to resolve, so detection has to fall back to the
/// builtin LinkML type names.
#[test]
fn builtin_range_is_numeric_without_a_types_block() {
    let schema = from_yaml(Path::new(&data_path("numeric_no_types.yaml"))).unwrap();
    let mut sv = SchemaView::new();
    sv.add_schema(schema.clone()).unwrap();
    let conv = converter_from_schemas([&schema]);

    for slot in ["count", "score"] {
        let ri = range_info_for_slot(&sv, &conv, "Thing", slot);
        assert_eq!(ri.rdf_datatype_iri, None, "no types block, no datatype IRI");
        assert!(ri.is_numeric(), "'{}' should be numeric by name", slot);
    }
    assert!(!range_info_for_slot(&sv, &conv, "Thing", "name").is_numeric());
}
