use linkml_schemaview::identifier::Identifier;
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

#[test]
fn resolves_branching_paths() {
    let schema = from_yaml(Path::new(&data_path("path_traversal.yaml"))).unwrap();

    let mut sv = SchemaView::new();
    sv.add_schema(schema.clone()).unwrap();

    let matches = sv
        .slots_for_path(
            &Identifier::new("Person"),
            ["workplaces", "departments", "location", "address", "street"],
        )
        .unwrap();

    assert_eq!(matches.len(), 2, "expected both address variants");

    let mut owners: Vec<_> = matches
        .iter()
        .map(|slot| slot.definition().owner.clone())
        .collect();
    owners.sort();

    assert_eq!(
        owners,
        vec![Some("Address".to_string()), Some("AltAddress".to_string())]
    );
}

#[test]
fn resolves_branching_paths_with_import_ref() {
    let schema = from_yaml(Path::new(&data_path("path_traversal.yaml"))).unwrap();

    let mut sv = SchemaView::new();
    sv.add_schema_with_import_ref(schema.clone(), Some(("".to_string(), "dummy".to_string())))
        .unwrap();

    let matches = sv
        .slots_for_path(
            &Identifier::new("Person"),
            ["workplaces", "departments", "location", "address", "street"],
        )
        .unwrap();

    assert_eq!(matches.len(), 2, "expected both address variants");
}

#[test]
fn stops_when_segment_is_scalar() {
    let schema = from_yaml(Path::new(&data_path("path_traversal.yaml"))).unwrap();

    let mut sv = SchemaView::new();
    sv.add_schema(schema.clone()).unwrap();

    let matches = sv
        .slots_for_path(&Identifier::new("Person"), ["workplaces", "name", "street"])
        .unwrap();

    assert!(matches.is_empty(), "scalar segments should stop traversal");
}

#[test]
fn empty_path_is_noop() {
    let schema = from_yaml(Path::new(&data_path("path_traversal.yaml"))).unwrap();

    let mut sv = SchemaView::new();
    sv.add_schema(schema.clone()).unwrap();

    let matches = sv
        .slots_for_path(&Identifier::new("Person"), std::iter::empty::<&str>())
        .unwrap();

    assert!(matches.is_empty(), "empty paths should not resolve slots");
}

#[test]
fn resolves_branching_paths_with_string_vec_input() {
    let schema = from_yaml(Path::new(&data_path("path_traversal.yaml"))).unwrap();

    let mut sv = SchemaView::new();
    sv.add_schema(schema.clone()).unwrap();

    let path: Vec<String> = ["workplaces", "departments", "location", "address", "street"]
        .iter()
        .map(|segment| segment.to_string())
        .collect();

    let matches = sv
        .slots_for_path(&Identifier::new("Person"), path.iter().map(|s| s.as_str()))
        .unwrap();

    assert_eq!(matches.len(), 2, "string vec input should keep branches");
}

// ============================================================================
// Tests for list/dict index support in paths
// ============================================================================

fn load_indexed_schema() -> SchemaView {
    let schema = from_yaml(Path::new(&data_path("indexed_paths.yaml"))).unwrap();
    let mut sv = SchemaView::new();
    sv.add_schema(schema).unwrap();
    sv
}

#[test]
fn resolves_list_index_numeric() {
    let sv = load_indexed_schema();

    // Path: ["items", "0", "name"] - traverse through list index to nested slot
    let matches = sv
        .slots_for_path(&Identifier::new("Root"), ["items", "0", "name"])
        .unwrap();

    assert_eq!(matches.len(), 1, "should resolve to name slot on Item");
    assert_eq!(matches[0].name, "name");
}

#[test]
fn resolves_list_index_identifier() {
    let sv = load_indexed_schema();

    // Path: ["items", "some_id", "name"] - identifier-based index (any string)
    let matches = sv
        .slots_for_path(&Identifier::new("Root"), ["items", "some_id", "name"])
        .unwrap();

    assert_eq!(matches.len(), 1, "should resolve through identifier-based index");
    assert_eq!(matches[0].name, "name");
}

#[test]
fn resolves_mapping_key() {
    let sv = load_indexed_schema();

    // Path: ["things", "alpha", "value"] - traverse through mapping key to nested slot
    let matches = sv
        .slots_for_path(&Identifier::new("Root"), ["things", "alpha", "value"])
        .unwrap();

    assert_eq!(matches.len(), 1, "should resolve to value slot on Thing");
    assert_eq!(matches[0].name, "value");
}

#[test]
fn resolves_nested_list_in_mapping() {
    let sv = load_indexed_schema();

    // Path: ["things", "key1", "nested_list", "0", "label"]
    // mapping -> key -> list -> index -> slot
    let matches = sv
        .slots_for_path(
            &Identifier::new("Root"),
            ["things", "key1", "nested_list", "0", "label"],
        )
        .unwrap();

    assert_eq!(matches.len(), 1, "should resolve through nested containers");
    assert_eq!(matches[0].name, "label");
}

#[test]
fn resolves_nested_mapping_in_list() {
    let sv = load_indexed_schema();

    // Path: ["items", "0", "nested_map", "foo", "value"]
    // list -> index -> mapping -> key -> slot
    let matches = sv
        .slots_for_path(
            &Identifier::new("Root"),
            ["items", "0", "nested_map", "foo", "value"],
        )
        .unwrap();

    assert_eq!(matches.len(), 1, "should resolve through nested containers");
    assert_eq!(matches[0].name, "value");
}

#[test]
fn terminal_index_returns_slot() {
    let sv = load_indexed_schema();

    // Path: ["items", "0"] - terminal is the index, should return items slot
    let matches = sv
        .slots_for_path(&Identifier::new("Root"), ["items", "0"])
        .unwrap();

    assert_eq!(matches.len(), 1, "terminal index should return parent slot");
    assert_eq!(matches[0].name, "items");
}

#[test]
fn terminal_mapping_key_returns_slot() {
    let sv = load_indexed_schema();

    // Path: ["things", "some_key"] - terminal is the key, should return things slot
    let matches = sv
        .slots_for_path(&Identifier::new("Root"), ["things", "some_key"])
        .unwrap();

    assert_eq!(matches.len(), 1, "terminal key should return parent slot");
    assert_eq!(matches[0].name, "things");
}

#[test]
fn scalar_list_with_index() {
    let sv = load_indexed_schema();

    // Path: ["scalars", "0"] - scalars is multivalued string, index should work
    let matches = sv
        .slots_for_path(&Identifier::new("Root"), ["scalars", "0"])
        .unwrap();

    assert_eq!(matches.len(), 1, "scalar list should support index");
    assert_eq!(matches[0].name, "scalars");
}

#[test]
fn invalid_path_after_index() {
    let sv = load_indexed_schema();

    // Path: ["items", "0", "nonexistent"] - no such slot on Item
    let matches = sv
        .slots_for_path(&Identifier::new("Root"), ["items", "0", "nonexistent"])
        .unwrap();

    assert!(matches.is_empty(), "invalid slot after index should fail");
}
