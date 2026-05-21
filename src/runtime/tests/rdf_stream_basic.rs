#![cfg(feature = "ttl")]

use linkml_runtime::rdf_import::{import_ntriples, import_turtle, ImportOptions};
use linkml_schemaview::identifier::converter_from_schemas;
use linkml_schemaview::schemaview::SchemaView;

fn fixture() -> (SchemaView, linkml_schemaview::Converter, String) {
    let schema_yaml = r#"
id: https://example.org/trains
name: trains
prefixes:
  ex: http://example.org/
default_prefix: ex
default_range: string
classes:
  Train:
    attributes:
      id:
        identifier: true
      operator:
        range: Operator
        inlined: true
  Operator:
    attributes:
      id:
        identifier: true
      name:
"#;
    let schema: linkml_meta::SchemaDefinition = serde_yaml::from_str(schema_yaml).unwrap();
    let mut sv = SchemaView::new();
    sv.add_schema(schema.clone()).unwrap();
    let conv = converter_from_schemas([&schema]);

    let ttl = r#"
        @prefix ex: <http://example.org/> .
        ex:t1 a ex:Train ; ex:id "T1" ; ex:operator ex:op1 .
        ex:op1 a ex:Operator ; ex:id "OP1" ; ex:name "ACME" .
    "#
    .to_string();
    (sv, conv, ttl)
}

#[test]
fn import_turtle_basic_iteration() {
    let (sv, conv, ttl) = fixture();
    let stream = import_turtle(
        std::io::Cursor::new(ttl.as_bytes()),
        sv,
        conv,
        &["Train", "Operator"],
        ImportOptions::default(),
    )
    .unwrap();

    let mut classes: Vec<String> = Vec::new();
    for item in stream {
        let (class, _inst) = item.unwrap();
        classes.push(class);
    }
    classes.sort();
    assert_eq!(classes, vec!["Train".to_string()]); // op1 is inlined into t1
}

#[test]
fn import_turtle_pop_warnings_when_clean() {
    let (sv, conv, ttl) = fixture();
    let mut stream = import_turtle(
        std::io::Cursor::new(ttl.as_bytes()),
        sv,
        conv,
        &["Train", "Operator"],
        ImportOptions::default(),
    )
    .unwrap();
    while stream.next().is_some() {}
    let warnings = stream.pop_warnings();
    for w in &warnings {
        eprintln!(
            "WARN: {:?} {} - {}",
            w.problem_type,
            w.subject.join("/"),
            w.detail
        );
    }
    assert_eq!(warnings.len(), 0);
}

#[test]
fn import_turtle_unconsumed_subjects_after_iteration() {
    let (sv, conv, ttl) = fixture();
    let mut stream = import_turtle(
        std::io::Cursor::new(ttl.as_bytes()),
        sv,
        conv,
        &["Train", "Operator"],
        ImportOptions::default(),
    )
    .unwrap();
    // Before iteration: returns None
    assert!(stream.unconsumed_subjects().is_none());
    while stream.next().is_some() {}
    // After: in-memory backend returns Some(empty) — all subjects visited
    let unconsumed = stream.unconsumed_subjects().unwrap();
    assert_eq!(unconsumed.len(), 0, "{unconsumed:?}");
}

#[test]
fn import_ntriples_basic() {
    let (sv, conv, _ttl) = fixture();
    let nt = r#"<http://example.org/t1> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://example.org/Train> .
<http://example.org/t1> <http://example.org/id> "T1" .
"#;
    let mut count = 0;
    let stream = import_ntriples(
        std::io::Cursor::new(nt.as_bytes()),
        sv,
        conv,
        &["Train"],
        ImportOptions::default(),
    )
    .unwrap();
    for item in stream {
        item.unwrap();
        count += 1;
    }
    assert_eq!(count, 1);
}
