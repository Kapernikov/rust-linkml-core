//! Importer half of the RDF type inference: any writer inference's output
//! (`rdf:type` for ancestors/mixins plus `sesame:directType`) must import as
//! the same instance as the plain asserted output.
#![cfg(feature = "ttl")]

use linkml_runtime::rdf_import::{import_turtle, ImportOptions};
use linkml_runtime::turtle_import::ImportError;
use linkml_runtime::LinkMLInstance;
use linkml_schemaview::identifier::converter_from_schemas;
use linkml_schemaview::schemaview::SchemaView;

const PRE: &str = "@prefix ex: <http://example.org/> .\n\
    @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .\n\
    @prefix sesame: <http://www.openrdf.org/schema/sesame#> .\n";

fn import(body: &str, roots: &[&str]) -> Result<Vec<(String, LinkMLInstance)>, ImportError> {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/data/type_regimes_import_schema.yaml");
    let schema: linkml_meta::SchemaDefinition =
        yaml_serde::from_str(&std::fs::read_to_string(path).unwrap()).unwrap();
    let mut sv = SchemaView::new();
    sv.add_schema(schema.clone()).unwrap();
    let conv = converter_from_schemas([&schema]);
    let ttl = format!("{PRE}{body}");
    let stream = import_turtle(
        std::io::Cursor::new(ttl.into_bytes()),
        sv,
        conv,
        roots,
        ImportOptions::default(),
    )?;
    let mut out = Vec::new();
    for item in stream {
        out.push(item?);
    }
    Ok(out)
}

fn import_err(body: &str, roots: &[&str]) -> ImportError {
    match import(body, roots) {
        Err(e) => e,
        Ok(items) => panic!("expected an import error, got {} instance(s)", items.len()),
    }
}

fn one(body: &str, roots: &[&str]) -> (String, serde_json::Value) {
    let mut items = import(body, roots).unwrap();
    assert_eq!(items.len(), 1, "expected exactly one root instance");
    let (class, inst) = items.remove(0);
    (class, inst.to_json())
}

/// The Asserted-style reference output for the signal used below.
const PLAIN: &str = "ex:s1 a ex:Signal ; ex:name \"S\" ; ex:aspect \"green\" .";

#[test]
fn direct_type_with_ancestor_types_is_the_direct_class() {
    let (c0, j0) = one(PLAIN, &["Signal"]);
    let body = "ex:s1 a ex:Signal, ex:Asset, ex:Thing ; sesame:directType ex:Signal ; \
                ex:name \"S\" ; ex:aspect \"green\" .";
    // Root class is the ancestor only, the leaf only, or both: one Signal.
    for roots in [&["Signal"][..], &["Asset"], &["Signal", "Asset", "Thing"]] {
        let (c, j) = one(body, roots);
        assert_eq!(c, "Signal");
        assert_eq!(c, c0);
        assert_eq!(j, j0);
    }
    // Designator repopulated from the direct class, single-valued.
    assert_eq!(j0["typeURI"], serde_json::json!("ex:Signal"));
}

#[test]
fn chain_types_without_direct_type_resolve_to_most_specific() {
    let (c, j) = one(
        "ex:s1 a ex:Thing, ex:Asset, ex:Signal ; ex:name \"S\" ; ex:aspect \"green\" .",
        &["Thing", "Asset"],
    );
    assert_eq!(c, "Signal");
    assert_eq!(j, one(PLAIN, &["Signal"]).1);
}

#[test]
fn mixin_type_is_implied_not_a_candidate() {
    let (c, j) = one(
        "ex:b1 a ex:Beacon, ex:Tagged, ex:Asset, ex:Thing ; ex:name \"B\" .",
        &["Beacon"],
    );
    assert_eq!(c, "Beacon");
    assert_eq!(j["name"], "B");
    // With directType too.
    let (c, _) = one(
        "ex:b1 a ex:Beacon, ex:Tagged, ex:Asset, ex:Thing ; sesame:directType ex:Beacon ; \
         ex:name \"B\" .",
        &["Tagged", "Beacon"],
    );
    assert_eq!(c, "Beacon");
}

#[test]
fn direct_type_conflicting_with_rdf_type_is_a_type_conflict() {
    let err = import_err(
        "ex:s1 a ex:Signal, ex:Pump ; sesame:directType ex:Signal .",
        &["Signal"],
    );
    match &err {
        ImportError::TypeConflict {
            direct_type,
            type_iri,
            ..
        } => {
            assert_eq!(direct_type, "http://example.org/Signal");
            assert_eq!(type_iri, "http://example.org/Pump");
        }
        other => panic!("expected TypeConflict, got {other}"),
    }
}

#[test]
fn two_direct_types_are_conflicting() {
    let err = import_err(
        "ex:s1 a ex:Signal, ex:Asset ; sesame:directType ex:Signal, ex:Asset .",
        &["Signal"],
    );
    match &err {
        ImportError::ConflictingDirectTypes {
            subject,
            direct_types,
        } => {
            assert_eq!(subject, "<http://example.org/s1>");
            assert_eq!(
                direct_types,
                &vec![
                    "http://example.org/Asset".to_string(),
                    "http://example.org/Signal".to_string()
                ]
            );
        }
        other => panic!("expected ConflictingDirectTypes, got {other}"),
    }
    let msg = err.to_string();
    assert!(msg.contains("keep one sesame:directType"), "{msg}");
    assert!(!msg.contains("add a sesame:directType"), "{msg}");
}

#[test]
fn unknown_direct_type_is_unknown_type() {
    let err = import_err(
        "ex:s1 a ex:Signal ; sesame:directType ex:Nope .",
        &["Signal"],
    );
    assert!(matches!(err, ImportError::UnknownType { .. }), "{err}");
}

#[test]
fn unrelated_types_without_direct_type_are_ambiguous_with_readable_message() {
    let err = import_err("ex:x a ex:Signal, ex:Pump .", &["Signal"]);
    match &err {
        ImportError::AmbiguousType { candidates, .. } => assert_eq!(
            candidates,
            &vec![
                "http://example.org/Pump".to_string(),
                "http://example.org/Signal".to_string()
            ]
        ),
        other => panic!("expected AmbiguousType, got {other}"),
    }
    let msg = err.to_string();
    assert!(msg.contains("<http://example.org/x>"), "{msg}");
    assert!(msg.contains("<http://example.org/Pump>"), "{msg}");
    assert!(msg.contains("<http://example.org/Signal>"), "{msg}");
    assert!(msg.contains("sesame:directType"), "{msg}");
}

#[test]
fn inlined_blank_node_with_ancestor_types_and_direct_type() {
    let body = "ex:h1 a ex:Holder ; ex:part [ a ex:Signal, ex:Asset, ex:Thing ; \
                sesame:directType ex:Signal ; ex:name \"inner\" ; ex:aspect \"red\" ] .";
    let plain = "ex:h1 a ex:Holder ; ex:part [ a ex:Signal ; ex:name \"inner\" ; \
                 ex:aspect \"red\" ] .";
    let (c, j) = one(body, &["Holder"]);
    assert_eq!(c, "Holder");
    let (_, j_plain) = one(plain, &["Holder"]);
    assert_eq!(j, j_plain);
    assert_eq!(j["part"]["aspect"], "red");
    assert_eq!(j["part"]["typeURI"], serde_json::json!("ex:Signal"));
}

#[test]
fn extras_are_not_unknown_fields_or_warnings() {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/data/type_regimes_import_schema.yaml");
    let schema: linkml_meta::SchemaDefinition =
        yaml_serde::from_str(&std::fs::read_to_string(path).unwrap()).unwrap();
    let mut sv = SchemaView::new();
    sv.add_schema(schema.clone()).unwrap();
    let conv = converter_from_schemas([&schema]);
    let ttl = format!(
        "{PRE}ex:s1 a ex:Signal, ex:Asset, ex:Thing ; sesame:directType ex:Signal ; ex:name \"S\" ."
    );
    let mut stream = import_turtle(
        std::io::Cursor::new(ttl.into_bytes()),
        sv,
        conv,
        &["Signal"],
        ImportOptions {
            strict: true,
            ..Default::default()
        },
    )
    .unwrap();
    let (_, inst) = stream.next().unwrap().unwrap();
    assert!(stream.next().is_none());
    assert!(stream.pop_warnings().is_empty());
    match inst {
        LinkMLInstance::Object { unknown_fields, .. } => assert!(unknown_fields.is_empty()),
        _ => panic!("expected an object instance"),
    }
}

#[test]
fn self_mixin_class_does_not_loop() {
    let (c, _) = one(
        "ex:m1 a ex:SelfMix, ex:Thing ; sesame:directType ex:SelfMix ; ex:name \"M\" .",
        &["SelfMix"],
    );
    assert_eq!(c, "SelfMix");
    let (c, _) = one("ex:m2 a ex:SelfMix, ex:Thing ; ex:name \"M\" .", &["Thing"]);
    assert_eq!(c, "SelfMix");
}

/// Roots are chosen by the most concrete class, so asserted and inferred
/// files select the same subjects; a leaf selects only its own subjects.
#[test]
fn root_selection_is_independent_of_inferred_types() {
    let asserted = "ex:s1 a ex:Signal ; ex:name \"S\" ; ex:aspect \"green\" .\n\
        ex:b1 a ex:Beacon ; ex:name \"B\" .\n\
        ex:a1 a ex:Asset ; ex:name \"A\" .\n\
        ex:p1 a ex:Pump ; ex:name \"P\" .";
    let inferred = "ex:s1 a ex:Signal, ex:Asset, ex:Thing ; sesame:directType ex:Signal ; \
        ex:name \"S\" ; ex:aspect \"green\" .\n\
        ex:b1 a ex:Beacon, ex:Tagged, ex:Asset, ex:Thing ; sesame:directType ex:Beacon ; \
        ex:name \"B\" .\n\
        ex:a1 a ex:Asset, ex:Thing ; sesame:directType ex:Asset ; ex:name \"A\" .\n\
        ex:p1 a ex:Pump, ex:Thing ; sesame:directType ex:Pump ; ex:name \"P\" .";
    let summary = |body: &str, roots: &[&str]| -> Vec<(String, String)> {
        let mut v: Vec<(String, String)> = import(body, roots)
            .unwrap()
            .into_iter()
            .map(|(c, i)| (c, i.to_json().to_string()))
            .collect();
        v.sort();
        v
    };
    for (roots, classes) in [
        (&["Asset"][..], vec!["Asset", "Beacon", "Signal"]),
        (&["Signal"], vec!["Signal"]),
        (&["Beacon"], vec!["Beacon"]),
        (&["Signal", "Beacon"], vec!["Beacon", "Signal"]),
        (&["Thing"], vec!["Asset", "Beacon", "Pump", "Signal"]),
    ] {
        let from_asserted = summary(asserted, roots);
        assert_eq!(from_asserted, summary(inferred, roots), "{roots:?}");
        let got: Vec<&str> = from_asserted.iter().map(|(c, _)| c.as_str()).collect();
        assert_eq!(got, classes, "{roots:?}");
    }
}
