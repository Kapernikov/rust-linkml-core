use schemaview::identifier::{converter_from_schemas, Identifier};
use schemaview::io::from_yaml;
use schemaview::schemaview::SchemaView;
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
    let container_schema = from_yaml(Path::new(&data_path("container.yaml"))).unwrap();

    let mut sv = SchemaView::new();
    sv.add_schema(container_schema.clone()).unwrap();
    sv.add_schema(person_schema.clone()).unwrap();

    let conv = converter_from_schemas([&person_schema, &container_schema]);

    let person = sv
        .get_class(&Identifier::new("personinfo:Person"), &conv)
        .unwrap()
        .unwrap();
    assert_eq!(
        person.get_uri(&conv, false, false).unwrap().to_string(),
        "linkml:Person"
    );
    assert_eq!(
        person.get_uri(&conv, true, false).unwrap().to_string(),
        "personinfo:Person"
    );
    assert_eq!(
        person.get_uri(&conv, false, true).unwrap().to_string(),
        "https://w3id.org/linkml/Person"
    );

    let container = sv
        .get_class(&Identifier::new("Container"), &conv)
        .unwrap()
        .unwrap();
    assert_eq!(
        container.get_uri(&conv, false, false).unwrap().to_string(),
        "container:Container"
    );
    assert_eq!(
        container.get_uri(&conv, true, false).unwrap().to_string(),
        "container:Container"
    );
}
