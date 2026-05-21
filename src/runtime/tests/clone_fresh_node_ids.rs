#![cfg(feature = "ttl")]

use linkml_runtime::{turtle_import::import_turtle_from_string, LinkMLInstance};
use linkml_schemaview::identifier::converter_from_schemas;
use linkml_schemaview::schemaview::SchemaView;

const SCHEMA_YAML: &str = r#"
id: https://example.org/clone-test
name: clone_test
prefixes:
  ex: http://example.org/
default_prefix: ex
default_range: string
classes:
  Person:
    attributes:
      id:
        identifier: true
      name:
"#;

fn load() -> (SchemaView, linkml_schemaview::Converter) {
    let schema: linkml_meta::SchemaDefinition = serde_yaml::from_str(SCHEMA_YAML).unwrap();
    let mut sv = SchemaView::new();
    sv.add_schema(schema.clone()).unwrap();
    let conv = converter_from_schemas([&schema]);
    (sv, conv)
}

/// Collect every node_id reachable in a tree.
fn collect_ids(inst: &LinkMLInstance, out: &mut Vec<u64>) {
    match inst {
        LinkMLInstance::Scalar { node_id, .. } | LinkMLInstance::Null { node_id, .. } => {
            out.push(*node_id);
        }
        LinkMLInstance::List {
            node_id, values, ..
        } => {
            out.push(*node_id);
            for v in values {
                collect_ids(v, out);
            }
        }
        LinkMLInstance::Mapping {
            node_id, values, ..
        } => {
            out.push(*node_id);
            for v in values.values() {
                collect_ids(v, out);
            }
        }
        LinkMLInstance::Object {
            node_id, values, ..
        } => {
            out.push(*node_id);
            for v in values.values() {
                collect_ids(v, out);
            }
        }
    }
}

#[test]
fn clone_with_fresh_node_ids_yields_disjoint_ids_and_equal_json() {
    let (sv, conv) = load();
    let ttl = r#"
        @prefix ex: <http://example.org/> .
        ex:p1 a ex:Person ; ex:id "P1" ; ex:name "Alice" .
    "#;
    let mut result = import_turtle_from_string(ttl, &sv, &conv, &["Person"]).unwrap();
    let person = result
        .instances
        .get_mut("Person")
        .expect("Person class in result")
        .pop()
        .expect("at least one Person");

    let cloned = person.clone_with_fresh_node_ids();

    let mut orig_ids = Vec::new();
    let mut cloned_ids = Vec::new();
    collect_ids(&person, &mut orig_ids);
    collect_ids(&cloned, &mut cloned_ids);

    assert_eq!(
        orig_ids.len(),
        cloned_ids.len(),
        "tree shape must match"
    );
    let orig_set: std::collections::HashSet<u64> = orig_ids.iter().copied().collect();
    for id in &cloned_ids {
        assert!(
            !orig_set.contains(id),
            "cloned id {id} must not appear in original tree"
        );
    }
    assert_eq!(
        person.to_json(),
        cloned.to_json(),
        "JSON projection must be identical"
    );
}
