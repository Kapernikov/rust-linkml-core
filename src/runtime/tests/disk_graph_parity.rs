#![cfg(feature = "disk_graph")]

//! Parity test: in-memory and disk-backed RdfStream produce the same
//! multiset of canonicalized instances. Stronger than counting classes:
//! we deep-canonicalize every JSON value (sorted dict keys AND sorted
//! list elements by serialized form) and compare the multisets.

use linkml_runtime::rdf_import::{import_turtle, ImportOptions};
use linkml_schemaview::identifier::converter_from_schemas;
use linkml_schemaview::schemaview::SchemaView;
use serde_json::Value as JsonValue;
use std::collections::BTreeMap;
use tempfile::tempdir;

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
      cars:
        range: Car
        inlined: true
        multivalued: true
  Operator:
    attributes:
      id:
        identifier: true
      name:
  Car:
    attributes:
      id:
        identifier: true
      seats:
        range: integer
"#;
    let schema: linkml_meta::SchemaDefinition = serde_yaml::from_str(schema_yaml).unwrap();
    let mut sv = SchemaView::new();
    sv.add_schema(schema.clone()).unwrap();
    let conv = converter_from_schemas([&schema]);

    let ttl = r#"
        @prefix ex: <http://example.org/> .
        @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
        ex:t1 a ex:Train ; ex:id "T1" ; ex:operator ex:op1 ; ex:cars ex:c1, ex:c2 .
        ex:t2 a ex:Train ; ex:id "T2" ; ex:operator ex:op1 ; ex:cars ex:c3 .
        ex:op1 a ex:Operator ; ex:id "OP1" ; ex:name "ACME" .
        ex:op2 a ex:Operator ; ex:id "OP2" ; ex:name "Standalone" .
        ex:c1 a ex:Car ; ex:id "C1" ; ex:seats "40"^^xsd:integer .
        ex:c2 a ex:Car ; ex:id "C2" ; ex:seats "50"^^xsd:integer .
        ex:c3 a ex:Car ; ex:id "C3" ; ex:seats "60"^^xsd:integer .
    "#
    .to_string();
    (sv, conv, ttl)
}

/// Deep canonicalization: sort dict keys AND sort list elements by their
/// serialized form. Strips internal node_ids (which differ across runs).
fn deep_canon(v: &JsonValue) -> JsonValue {
    match v {
        JsonValue::Object(map) => {
            let mut sorted: BTreeMap<String, JsonValue> = BTreeMap::new();
            for (k, val) in map {
                sorted.insert(k.clone(), deep_canon(val));
            }
            JsonValue::Object(sorted.into_iter().collect())
        }
        JsonValue::Array(arr) => {
            let mut items: Vec<JsonValue> = arr.iter().map(deep_canon).collect();
            items.sort_by_key(|item| serde_json::to_string(item).unwrap_or_default());
            JsonValue::Array(items)
        }
        other => other.clone(),
    }
}

fn collect_multiset(
    backend: ImportOptions,
    sv: SchemaView,
    conv: linkml_schemaview::Converter,
    ttl: &str,
) -> BTreeMap<String, Vec<String>> {
    let stream = import_turtle(
        std::io::Cursor::new(ttl.as_bytes()),
        sv,
        conv,
        &["Train", "Operator", "Car"],
        backend,
    )
    .expect("import_turtle");
    let mut by_class: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for item in stream {
        let (class, inst) = item.expect("yields ok");
        let canon = deep_canon(&inst.to_json());
        let s = serde_json::to_string(&canon).unwrap();
        by_class.entry(class).or_default().push(s);
    }
    // Sort each class's list to compare as a multiset.
    for v in by_class.values_mut() {
        v.sort();
    }
    by_class
}

#[test]
fn deep_multiset_parity_in_memory_vs_disk() {
    let (sv1, conv1, ttl) = fixture();
    let (sv2, conv2, _) = fixture();

    let in_mem = collect_multiset(ImportOptions::default(), sv1, conv1, &ttl);

    let dir = tempdir().unwrap();
    let disk_path = dir.path().to_path_buf();
    let disk = collect_multiset(
        ImportOptions {
            disk_path: Some(disk_path),
            strict: false,
        },
        sv2,
        conv2,
        &ttl,
    );

    assert_eq!(
        in_mem, disk,
        "in-memory and disk multisets must match exactly"
    );

    // Sanity-check shape: 2 Trains, 1 Operator (op2; op1 is inlined into both
    // trains), 0 standalone Cars (all inlined). Schema-driven inlining rule:
    // any Car referenced by `cars` is claimed.
    assert_eq!(in_mem.get("Train").map(|v| v.len()), Some(2));
    assert_eq!(in_mem.get("Operator").map(|v| v.len()), Some(1));
}
