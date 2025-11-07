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
