#![cfg(feature = "disk_graph")]

//! Parity test: the same Turtle import produces the same instance set
//! through both the in-memory and disk-backed TripleSource impls.

use linkml_runtime::rdf_import_store::RdfImportStore;
use linkml_runtime::rdf_import_store_disk::DiskRdfImportStore;
use linkml_runtime::rdf_streaming::import_owned_store_streaming;
use linkml_runtime::triple_source::TripleSource;
use linkml_schemaview::identifier::converter_from_schemas;
use linkml_schemaview::schemaview::SchemaView;
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
        ex:t2 a ex:Train ; ex:id "T2" ; ex:operator ex:op1 .
        ex:op1 a ex:Operator ; ex:id "OP1" ; ex:name "ACME" .
        ex:op2 a ex:Operator ; ex:id "OP2" ; ex:name "Standalone" .
    "#
    .to_string();
    (sv, conv, ttl)
}

fn collect_counts<S: TripleSource + 'static>(
    store: S,
    sv: SchemaView,
    conv: linkml_schemaview::Converter,
) -> BTreeMap<String, usize> {
    let stream = import_owned_store_streaming(store, sv, conv, &["Train", "Operator"])
        .expect("import_owned_store_streaming");
    let mut out: BTreeMap<String, usize> = BTreeMap::new();
    for item in stream {
        let (class, _inst) = item.expect("import yields ok");
        *out.entry(class).or_insert(0) += 1;
    }
    out
}

#[test]
fn parity_in_memory_vs_disk() {
    let (sv1, conv1, ttl) = fixture();
    let (sv2, conv2, _) = fixture();

    let in_mem = RdfImportStore::from_turtle(std::io::Cursor::new(ttl.as_bytes())).unwrap();
    let in_mem_counts = collect_counts(in_mem, sv1, conv1);

    let dir = tempdir().unwrap();
    let disk =
        DiskRdfImportStore::from_turtle(std::io::Cursor::new(ttl.as_bytes()), dir.path()).unwrap();
    let disk_counts = collect_counts(disk, sv2, conv2);

    // Same class counts: 2 Trains + 1 standalone Operator (op2; op1 is
    // inlined into both trains).
    assert_eq!(in_mem_counts, disk_counts);
    assert_eq!(in_mem_counts.get("Train").copied(), Some(2));
    assert_eq!(in_mem_counts.get("Operator").copied(), Some(1));
}
