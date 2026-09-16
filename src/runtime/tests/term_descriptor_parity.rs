#![cfg(feature = "ttl")]
//! `term_for` is the value-dependent half of `SlotView::term_descriptor`, and it
//! is what the turtle writer now uses at all three of its scalar call sites.
//!
//! These assert the exact `Term` produced per precedence rule, on the same
//! fixtures the golden turtle tests use — so a change here shows up as both a
//! wrong term and wrong turtle output.

use linkml_runtime::turtle::term_for;
use linkml_schemaview::identifier::{converter_from_schema, converter_from_schemas, Identifier};
use linkml_schemaview::io::from_yaml;
use linkml_schemaview::schemaview::SchemaView;
use linkml_schemaview::slotview::SlotView;
use oxrdf::{Literal, NamedNode, Term};
use serde_json::json;
use std::path::{Path, PathBuf};

fn data_path(name: &str) -> PathBuf {
    let mut p = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    p.push("tests");
    p.push("data");
    p.push(name);
    p
}

fn load_alone(schema_file: &str) -> (SchemaView, linkml_schemaview::Converter) {
    let schema = from_yaml(Path::new(&data_path(schema_file))).unwrap();
    let mut sv = SchemaView::new();
    sv.add_schema(schema.clone()).unwrap();
    let conv = converter_from_schema(&schema);
    (sv, conv)
}

fn load_with_types(schema_file: &str) -> (SchemaView, linkml_schemaview::Converter) {
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
    (sv, conv)
}

fn slot(
    sv: &SchemaView,
    conv: &linkml_schemaview::Converter,
    class_name: &str,
    slot_name: &str,
) -> SlotView {
    sv.get_class(&Identifier::new(class_name), conv)
        .unwrap()
        .unwrap()
        .slot(&Identifier::Name(slot_name.to_string()))
        .unwrap_or_else(|| panic!("slot '{}' not found on '{}'", slot_name, class_name))
}

/// Resolve the descriptor once, apply it to one value — the two-step the SQL
/// pushdown route needs and the turtle writer performs per value.
fn term(
    sv: &SchemaView,
    conv: &linkml_schemaview::Converter,
    class_name: &str,
    slot_name: &str,
    value: serde_json::Value,
) -> Term {
    let s = slot(sv, conv, class_name, slot_name);
    let d = s
        .term_descriptor(conv)
        .unwrap_or_else(|| panic!("no descriptor for '{}'", slot_name));
    term_for(&d, &value, conv)
}

#[test]
fn plain_string_is_a_simple_literal() {
    let (sv, conv) = load_with_types("custom_type_schema.yaml");
    assert_eq!(
        term(&sv, &conv, "Place", "name", json!("Brussels")),
        Term::Literal(Literal::new_simple_literal("Brussels"))
    );
}

#[test]
fn custom_type_is_a_typed_literal() {
    let (sv, conv) = load_with_types("custom_type_schema.yaml");
    assert_eq!(
        term(
            &sv,
            &conv,
            "Place",
            "location",
            json!("POINT(4.3517 50.8503)")
        ),
        Term::Literal(Literal::new_typed_literal(
            "POINT(4.3517 50.8503)",
            NamedNode::new_unchecked("http://www.opengis.net/ont/geosparql#wktLiteral")
        ))
    );
}

#[test]
fn iri_range_is_a_named_node() {
    let (sv, conv) = load_with_types("custom_type_schema.yaml");
    assert_eq!(
        term(
            &sv,
            &conv,
            "Place",
            "homepage",
            json!("https://www.brussels.be")
        ),
        Term::NamedNode(NamedNode::new_unchecked("https://www.brussels.be"))
    );
}

/// An IRI-ish value may be a CURIE, which is expanded with the converter — the
/// same converter the values are serialized with.
#[test]
fn iri_range_expands_a_curie() {
    let (sv, conv) = load_with_types("lang_tag_schema.yaml");
    assert_eq!(
        term(&sv, &conv, "Station", "id", json!("langtest:brussels")),
        Term::NamedNode(NamedNode::new_unchecked(
            "https://example.com/langtest/brussels"
        ))
    );
}

/// A number renders through its JSON representation, not through Rust's
/// `Display` for `serde_json::Value`.
#[test]
fn numbers_and_booleans_render_as_their_json_text() {
    let (sv, conv) = load_with_types("typed_literals_schema.yaml");
    assert_eq!(
        term(&sv, &conv, "Thing", "count", json!(42)),
        Term::Literal(Literal::new_typed_literal(
            "42",
            NamedNode::new_unchecked("http://www.w3.org/2001/XMLSchema#integer")
        ))
    );
}

#[test]
fn language_tag_is_applied_when_there_is_no_datatype() {
    let (sv, conv) = load_with_types("lang_tag_schema.yaml");
    assert_eq!(
        term(&sv, &conv, "Station", "opName", json!("Brussels North")),
        Term::Literal(Literal::new_language_tagged_literal("Brussels North", "en").unwrap())
    );
}

#[test]
fn enum_value_with_a_meaning_is_that_named_node() {
    let (sv, conv) = load_alone("enum_meaning_schema.yaml");
    assert_eq!(
        term(&sv, &conv, "Item", "status", json!("active")),
        Term::NamedNode(NamedNode::new_unchecked(
            "https://example.com/status/Active"
        ))
    );
}

/// The mixed enum: `unknown` carries no `meaning`, and is still a concept.
///
/// It used to fall back to a literal, which put a term in instance data that
/// the slot's own declared range — a `skos:ConceptScheme` — does not admit, and
/// made one enum answer two kinds of term depending on whether a value happened
/// to be mapped to an ontology.
#[test]
fn enum_value_without_a_meaning_is_the_minted_concept_iri() {
    let (sv, conv) = load_alone("enum_meaning_schema.yaml");
    assert_eq!(
        term(&sv, &conv, "Item", "status", json!("unknown")),
        Term::NamedNode(NamedNode::new_unchecked(
            "https://example.com/enum-meaning-test/StatusEnum#unknown"
        ))
    );
}

/// The drift guard. The instance term and the schema graph's concept subject
/// are two call sites of one spelling; if they ever disagree, a client joining
/// instance data to the schema graph silently loses the values where they do —
/// which is how this was found in the first place.
#[test]
fn every_instance_enum_term_is_a_concept_subject_in_the_schema_graph() {
    use linkml_runtime::schema_rdf::{schema_triples, SchemaRdfOptions, SKOS_NOTATION};

    let (sv, conv) = load_alone("enum_meaning_schema.yaml");

    let built = schema_triples(&sv, &SchemaRdfOptions::default());
    let concepts: Vec<String> = built
        .triples
        .iter()
        .filter(|t| t.predicate.as_str() == SKOS_NOTATION)
        .map(|t| t.subject.to_string().trim_matches(['<', '>']).to_owned())
        .collect();
    assert_eq!(concepts.len(), 3, "one concept per permissible value");

    for code in ["active", "retired", "unknown"] {
        let Term::NamedNode(node) = term(&sv, &conv, "Item", "status", json!(code)) else {
            panic!("{code} did not render as an IRI");
        };
        assert!(
            concepts.contains(&node.as_str().to_owned()),
            "instance term {node} for {code} names no concept in the schema graph: {concepts:?}"
        );
    }
}
