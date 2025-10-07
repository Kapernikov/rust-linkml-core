use linkml_schemaview::identifier::{converter_from_schemas, Identifier};
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
fn class_get_uri() {
    let person_schema = from_yaml(Path::new(&data_path("person.yaml"))).unwrap();
    let mut sv = SchemaView::new();
    sv.add_schema(person_schema.clone()).unwrap();
    let conv = converter_from_schemas([&person_schema]);
    let cv = sv
        .get_class(&Identifier::new("Person"), &conv)
        .unwrap()
        .unwrap();
    assert_eq!(
        cv.get_uri(&conv, false, false).unwrap().to_string(),
        "linkml:Person"
    );
    assert_eq!(
        cv.get_uri(&conv, true, false).unwrap().to_string(),
        "personinfo:Person"
    );
    assert_eq!(
        cv.canonical_uri().to_string(),
        "https://w3id.org/linkml/Person"
    );
}

#[test]
fn class_lookup_by_uri() {
    let person_schema = from_yaml(Path::new(&data_path("person.yaml"))).unwrap();
    let mut sv = SchemaView::new();
    sv.add_schema(person_schema.clone()).unwrap();

    let class_by_uri = sv
        .get_class_by_uri("https://w3id.org/linkml/Person")
        .unwrap()
        .expect("class not found by URI");
    assert_eq!(class_by_uri.name(), "Person");

    let class_by_curie = sv
        .get_class_by_uri("linkml:Person")
        .unwrap()
        .expect("class not found by CURIE");
    assert_eq!(class_by_curie.name(), "Person");
}
