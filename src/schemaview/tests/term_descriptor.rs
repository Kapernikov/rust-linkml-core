//! How a slot's values become RDF terms, decided from the slot alone.
//!
//! One case per rule of the precedence chain, so the ordering is pinned rather
//! than implied. The turtle writer applies the same descriptor per value; its
//! own tests (`turtle_enum_meaning`, `turtle_lang_tags`, `turtle_typed_literals`,
//! `turtle_custom_types`) are the end-to-end half.

use linkml_schemaview::identifier::{converter_from_schemas, Identifier};
use linkml_schemaview::io::from_yaml;
use linkml_schemaview::schemaview::SchemaView;
use linkml_schemaview::slotview::{TermDescriptor, TermKind};
use std::path::{Path, PathBuf};

fn data_path(name: &str) -> PathBuf {
    let mut p = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    p.push("tests");
    p.push("data");
    p.push(name);
    p
}

fn load() -> (SchemaView, linkml_schemaview::Converter) {
    let schema = from_yaml(Path::new(&data_path("rdf_type_schema.yaml"))).unwrap();
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

/// The descriptor for `Thing.<slot>`, or `None` when the slot's values are not a
/// reproducible term.
fn descriptor_for(slot_name: &str) -> Option<TermDescriptor> {
    let (sv, conv) = load();
    let class = sv
        .get_class(&Identifier::new("Thing"), &conv)
        .unwrap()
        .unwrap();
    class
        .slot(&Identifier::Name(slot_name.to_string()))
        .unwrap_or_else(|| panic!("slot '{}' not found", slot_name))
        .term_descriptor(&conv)
}

fn describe(slot_name: &str) -> TermDescriptor {
    descriptor_for(slot_name).unwrap_or_else(|| panic!("no descriptor for '{}'", slot_name))
}

#[test]
fn plain_string_is_an_untyped_literal() {
    let d = describe("name");
    assert_eq!(d.kind, TermKind::Literal);
    assert_eq!(d.datatype, None);
    assert_eq!(d.lang, None);
    assert!(d.enum_map.is_empty());
}

#[test]
fn declared_type_carries_its_datatype() {
    let d = describe("count");
    assert_eq!(d.kind, TermKind::Literal);
    assert_eq!(
        d.datatype.as_deref(),
        Some("http://www.w3.org/2001/XMLSchema#integer")
    );
}

/// Rule 1: an enum whose permissible values carry `meaning` IRIs. The map is
/// expanded and sorted — it crosses into generated SQL downstream, where an
/// unstable order makes queries and tests flap.
#[test]
fn enum_with_meanings_maps_values_to_sorted_iris() {
    let d = describe("status");
    assert_eq!(d.kind, TermKind::EnumIri);
    assert_eq!(
        d.enum_map,
        vec![
            (
                "active".to_string(),
                "https://example.com/rdftype/Active".to_string()
            ),
            (
                "retired".to_string(),
                "https://example.com/rdftype/Retired".to_string()
            ),
        ]
    );
}

/// Nothing to map to, so the turtle writer emits the value as a literal and the
/// descriptor has to agree.
#[test]
fn enum_without_meanings_stays_a_literal() {
    let d = describe("flag");
    assert_eq!(d.kind, TermKind::Literal);
    assert!(d.enum_map.is_empty());
}

/// Rule 2.
#[test]
fn iri_range_is_a_named_node() {
    let d = describe("see_also");
    assert_eq!(d.kind, TermKind::Iri);
    assert_eq!(d.datatype, None);
    assert_eq!(d.lang, None);
}

/// Rule 3.
#[test]
fn language_tag_is_carried_when_there_is_no_datatype() {
    let d = describe("description");
    assert_eq!(d.kind, TermKind::Literal);
    assert_eq!(d.lang.as_deref(), Some("en"));
    assert_eq!(d.datatype, None);
}

/// Rules 3 and 4 collide: RDF allows a datatype or a language tag, not both, and
/// the turtle writer lets the datatype win. A slot declaring both must resolve
/// the same way here or the two would disagree.
#[test]
fn datatype_wins_over_a_language_tag() {
    let d = describe("counted_in_english");
    assert_eq!(
        d.datatype.as_deref(),
        Some("http://www.w3.org/2001/XMLSchema#integer")
    );
    assert_eq!(d.lang, None);
}

/// A reference stores the target's URI, which is exactly the term the writer
/// emits — so it is a value a consumer can reproduce.
#[test]
fn a_reference_is_the_target_iri() {
    let d = describe("target");
    assert_eq!(d.kind, TermKind::Iri);
}

/// An inlined structure serialises as a blank node whose label nothing can
/// reproduce, so there is no descriptor to give.
#[test]
fn an_inlined_structure_is_not_a_term() {
    assert!(descriptor_for("nested").is_none());
}
