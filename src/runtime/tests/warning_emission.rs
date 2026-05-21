#![cfg(feature = "ttl")]

//! Unit tests for the three warning categories emitted by the RDF harvest:
//!   - MaxCountViolation: single-value slot with multiple objects
//!   - UndeclaredSlot: subject visited, predicate not in any slot
//!   - SlotRangeViolation: literal datatype incompatible with slot range
//!
//! Plus strict-mode tests: in strict mode, first warning aborts the import.

use linkml_runtime::rdf_import::{import_turtle, ImportOptions};
use linkml_runtime::{ValidationProblemType, ValidationSeverity};
use linkml_schemaview::identifier::converter_from_schemas;
use linkml_schemaview::schemaview::SchemaView;

fn schema_and_conv(yaml: &str) -> (SchemaView, linkml_schemaview::Converter) {
    let schema: linkml_meta::SchemaDefinition = serde_yaml::from_str(yaml).unwrap();
    let mut sv = SchemaView::new();
    sv.add_schema(schema.clone()).unwrap();
    let conv = converter_from_schemas([&schema]);
    (sv, conv)
}

fn drain(stream: &mut linkml_runtime::rdf_import::RdfStream) {
    while stream.next().is_some() {}
}

const SINGLE_VALUE_SCHEMA: &str = r#"
id: https://example.org/sv
name: sv
prefixes:
  ex: http://example.org/
default_prefix: ex
default_range: string
classes:
  T:
    attributes:
      id:
        identifier: true
      name:
        # default cardinality is single-valued
"#;

#[test]
fn max_count_violation_when_single_value_slot_has_multiple_triples() {
    let (sv, conv) = schema_and_conv(SINGLE_VALUE_SCHEMA);
    let ttl = r#"
        @prefix ex: <http://example.org/> .
        ex:a a ex:T ; ex:name "alpha" ; ex:name "beta" .
    "#;
    let mut stream = import_turtle(
        std::io::Cursor::new(ttl.as_bytes()),
        sv,
        conv,
        &["T"],
        ImportOptions::default(),
    )
    .unwrap();
    drain(&mut stream);
    let warnings = stream.pop_warnings();
    let max_count: Vec<_> = warnings
        .iter()
        .filter(|w| w.problem_type == ValidationProblemType::MaxCountViolation)
        .collect();
    assert_eq!(
        max_count.len(),
        1,
        "expected one MaxCountViolation, got: {warnings:?}"
    );
    assert_eq!(max_count[0].severity, ValidationSeverity::Warning);
    assert!(max_count[0].detail.contains("name"));
    // pop drains
    assert_eq!(stream.pop_warnings().len(), 0);
}

const UNDECLARED_SLOT_SCHEMA: &str = r#"
id: https://example.org/us
name: us
prefixes:
  ex: http://example.org/
default_prefix: ex
default_range: string
classes:
  T:
    attributes:
      id:
        identifier: true
      name:
"#;

#[test]
fn undeclared_slot_warning_for_unknown_predicate() {
    let (sv, conv) = schema_and_conv(UNDECLARED_SLOT_SCHEMA);
    let ttl = r#"
        @prefix ex: <http://example.org/> .
        ex:a a ex:T ; ex:name "alpha" ; ex:foo "extra" .
    "#;
    let mut stream = import_turtle(
        std::io::Cursor::new(ttl.as_bytes()),
        sv,
        conv,
        &["T"],
        ImportOptions::default(),
    )
    .unwrap();
    drain(&mut stream);
    let warnings = stream.pop_warnings();
    let unknown: Vec<_> = warnings
        .iter()
        .filter(|w| w.problem_type == ValidationProblemType::UndeclaredSlot)
        .collect();
    assert_eq!(
        unknown.len(),
        1,
        "expected one UndeclaredSlot warning for ex:foo, got: {warnings:?}"
    );
    assert!(unknown[0].detail.contains("foo"));
}

const RANGE_VIOLATION_SCHEMA: &str = r#"
id: https://example.org/rv
name: rv
prefixes:
  ex: http://example.org/
  xsd: http://www.w3.org/2001/XMLSchema#
default_prefix: ex
classes:
  T:
    attributes:
      id:
        identifier: true
        range: string
      age:
        range: integer
"#;

#[test]
fn slot_range_violation_when_literal_cannot_convert() {
    let (sv, conv) = schema_and_conv(RANGE_VIOLATION_SCHEMA);
    // age is xsd:string-typed but the slot's range is integer.
    // literal_to_json reads the datatype, sees xsd:string -> returns the
    // string. The slot's declared range (integer) doesn't constrain the
    // conversion path in literal_to_json today, so the warning fires only
    // when the LITERAL DATATYPE specifically declares an incompatible
    // numeric type. Use xsd:integer with a non-numeric value to trigger.
    let ttl = r#"
        @prefix ex: <http://example.org/> .
        @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
        ex:a a ex:T ; ex:age "not-a-number"^^xsd:integer .
    "#;
    let mut stream = import_turtle(
        std::io::Cursor::new(ttl.as_bytes()),
        sv,
        conv,
        &["T"],
        ImportOptions::default(),
    )
    .unwrap();
    drain(&mut stream);
    let warnings = stream.pop_warnings();
    let range: Vec<_> = warnings
        .iter()
        .filter(|w| w.problem_type == ValidationProblemType::SlotRangeViolation)
        .collect();
    assert_eq!(
        range.len(),
        1,
        "expected one SlotRangeViolation, got: {warnings:?}"
    );
}

#[test]
fn clean_input_emits_no_warnings() {
    let (sv, conv) = schema_and_conv(UNDECLARED_SLOT_SCHEMA);
    let ttl = r#"
        @prefix ex: <http://example.org/> .
        ex:a a ex:T ; ex:name "alpha" .
    "#;
    let mut stream = import_turtle(
        std::io::Cursor::new(ttl.as_bytes()),
        sv,
        conv,
        &["T"],
        ImportOptions::default(),
    )
    .unwrap();
    drain(&mut stream);
    let warnings = stream.pop_warnings();
    assert_eq!(
        warnings.len(),
        0,
        "expected no warnings on clean input, got: {warnings:?}"
    );
}

#[test]
fn strict_mode_aborts_on_first_warning() {
    let (sv, conv) = schema_and_conv(SINGLE_VALUE_SCHEMA);
    let ttl = r#"
        @prefix ex: <http://example.org/> .
        ex:a a ex:T ; ex:name "alpha" ; ex:name "beta" .
    "#;
    let mut stream = import_turtle(
        std::io::Cursor::new(ttl.as_bytes()),
        sv,
        conv,
        &["T"],
        ImportOptions {
            strict: true,
            ..Default::default()
        },
    )
    .unwrap();
    let first = stream.next();
    match first {
        Some(Err(linkml_runtime::turtle_import::ImportError::ValidationFailure(vr))) => {
            assert_eq!(vr.problem_type, ValidationProblemType::MaxCountViolation);
        }
        Some(Err(other)) => panic!("expected ValidationFailure error, got: {other}"),
        Some(Ok(_)) => panic!("expected error, got Ok"),
        None => panic!("expected error, got None"),
    }
}

#[test]
fn pop_warnings_drains_between_calls() {
    let (sv, conv) = schema_and_conv(SINGLE_VALUE_SCHEMA);
    let ttl = r#"
        @prefix ex: <http://example.org/> .
        ex:a a ex:T ; ex:name "alpha" ; ex:name "beta" .
        ex:b a ex:T ; ex:name "gamma" ; ex:name "delta" .
    "#;
    let mut stream = import_turtle(
        std::io::Cursor::new(ttl.as_bytes()),
        sv,
        conv,
        &["T"],
        ImportOptions::default(),
    )
    .unwrap();
    drain(&mut stream);
    let first = stream.pop_warnings();
    assert_eq!(first.len(), 2, "expected 2 warnings, got: {first:?}");
    let second = stream.pop_warnings();
    assert_eq!(second.len(), 0, "second pop should be empty");
}
