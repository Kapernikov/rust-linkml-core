//! LinkML spells the derived-type key `typeof` and the class flag `abstract`.
//! Both are Rust keywords, so the generated metamodel fields are `typeof_` and
//! `abstract_`; without a serde rename the key on the wire never matches the
//! field and the value is silently dropped, leaving derived types with no
//! parent at all.
//!
//! The generator now emits `#[serde(rename = "<linkml name>")]` for every field
//! whose Rust name was escaped, so both spellings line up and a derived type
//! inherits again. These ran red until the regenerated metamodel landed (#108).

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

fn load() -> (SchemaView, linkml_schemaview::Converter) {
    let schema = from_yaml(Path::new(&data_path("typeof_schema.yaml"))).unwrap();
    let types_schema = from_yaml(Path::new(&data_path("types.yaml"))).unwrap();
    let mut sv = SchemaView::new();
    sv.add_schema(schema.clone()).unwrap();
    sv.add_schema_with_import_ref(
        types_schema.clone(),
        Some((schema.id.clone(), "linkml:types".to_string())),
    )
    .unwrap();
    let conv = converter_from_schemas([&schema, &types_schema]);
    (sv, conv)
}

fn range_info(
    sv: &SchemaView,
    conv: &linkml_schemaview::Converter,
    class_name: &str,
    slot_name: &str,
) -> linkml_schemaview::slotview::RangeInfo {
    let class = sv
        .get_class(&Identifier::new(class_name), conv)
        .unwrap()
        .unwrap();
    class
        .slot(&Identifier::Name(slot_name.to_string()))
        .unwrap_or_else(|| panic!("slot '{}' not found on '{}'", slot_name, class_name))
        .get_range_info()
        .first()
        .cloned()
        .unwrap()
}

#[test]
fn typeof_survives_deserialization() {
    let schema = from_yaml(Path::new(&data_path("typeof_schema.yaml"))).unwrap();
    let types = schema.types.as_ref().expect("schema declares types");
    assert_eq!(
        types.get("trackLength").unwrap().typeof_.as_deref(),
        Some("integer"),
        "`typeof:` must not be dropped on the way in"
    );
}

#[test]
fn abstract_survives_deserialization() {
    let schema = from_yaml(Path::new(&data_path("typeof_schema.yaml"))).unwrap();
    let classes = schema.classes.as_ref().expect("schema declares classes");
    assert_eq!(
        classes.get("Base").unwrap().abstract_,
        Some(true),
        "`abstract:` must not be dropped on the way in"
    );
}

#[test]
fn type_ancestors_walks_the_whole_typeof_chain() {
    let (sv, conv) = load();
    let ancestors = sv
        .type_ancestors(&Identifier::new("mainTrackLength"), &conv)
        .unwrap();
    let names: Vec<String> = ancestors.iter().map(|a| a.to_string()).collect();
    assert_eq!(
        names,
        vec![
            "mainTrackLength".to_string(),
            "trackLength".to_string(),
            "integer".to_string()
        ],
        "a derived type inherits through every level of the chain"
    );
}

#[test]
fn derived_type_inherits_its_parents_datatype() {
    let (sv, conv) = load();
    for slot in ["length", "main_length"] {
        let ri = range_info(&sv, &conv, "Thing", slot);
        assert_eq!(
            ri.rdf_datatype_iri.as_deref(),
            Some("http://www.w3.org/2001/XMLSchema#integer"),
            "'{}' should inherit xsd:integer through its typeof chain",
            slot
        );
        assert!(
            ri.is_integer(),
            "'{}' should canonicalise as an integer",
            slot
        );
    }
}

/// The case with user-visible serialisation impact: a type derived from `uri`
/// is an IRI, so its values are named nodes rather than literals.
#[test]
fn type_derived_from_uri_is_an_iri_range() {
    let (sv, conv) = load();
    let ri = range_info(&sv, &conv, "Thing", "homepage");
    assert!(
        ri.is_range_iri,
        "a `typeof: uri` range must be flagged as an IRI"
    );
    assert_eq!(ri.rdf_datatype_iri, None);
}
