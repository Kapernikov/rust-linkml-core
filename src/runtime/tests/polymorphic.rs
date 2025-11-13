use linkml_runtime::LinkMLInstance;
use linkml_runtime::{load_yaml_file, validate};
use linkml_schemaview::identifier::{converter_from_schema, Identifier};
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
fn polymorphism_with_type() {
    let schema = from_yaml(Path::new(&data_path("poly_schema.yaml"))).unwrap();
    let mut sv = SchemaView::new();
    sv.add_schema(schema.clone()).unwrap();
    let conv = converter_from_schema(&schema);
    let class = sv
        .get_class(&Identifier::new("Container"), &conv)
        .unwrap()
        .expect("class not found");
    let v = load_yaml_file(
        Path::new(&data_path("poly_with_type.yaml")),
        &sv,
        &class,
        &conv,
    )
    .unwrap()
    .into_instance()
    .unwrap();
    assert!(validate(&v).is_ok());
}

#[test]
fn polymorphism_without_type() {
    let schema = from_yaml(Path::new(&data_path("poly_schema.yaml"))).unwrap();
    let mut sv = SchemaView::new();
    sv.add_schema(schema.clone()).unwrap();
    let conv = converter_from_schema(&schema);
    let class = sv
        .get_class(&Identifier::new("Container"), &conv)
        .unwrap()
        .expect("class not found");
    let v = load_yaml_file(
        Path::new(&data_path("poly_without_type.yaml")),
        &sv,
        &class,
        &conv,
    )
    .unwrap()
    .into_instance()
    .unwrap();
    assert!(validate(&v).is_ok());
}

#[test]
fn root_polymorphism_with_type() {
    let schema = from_yaml(Path::new(&data_path("poly_schema.yaml"))).unwrap();
    let mut sv = SchemaView::new();
    sv.add_schema(schema.clone()).unwrap();
    let conv = converter_from_schema(&schema);
    let class = sv
        .get_class(&Identifier::new("Parent"), &conv)
        .unwrap()
        .expect("class not found");
    let v = load_yaml_file(
        Path::new(&data_path("root_with_type.yaml")),
        &sv,
        &class,
        &conv,
    )
    .unwrap()
    .into_instance()
    .unwrap();
    assert!(validate(&v).is_ok());
}

#[test]
fn root_polymorphism_without_type() {
    let schema = from_yaml(Path::new(&data_path("poly_schema.yaml"))).unwrap();
    let mut sv = SchemaView::new();
    sv.add_schema(schema.clone()).unwrap();
    let conv = converter_from_schema(&schema);
    let class = sv
        .get_class(&Identifier::new("Parent"), &conv)
        .unwrap()
        .expect("class not found");
    let v = load_yaml_file(
        Path::new(&data_path("root_without_type.yaml")),
        &sv,
        &class,
        &conv,
    )
    .unwrap()
    .into_instance()
    .unwrap();
    assert!(validate(&v).is_ok());
}

#[test]
fn array_polymorphism() {
    let schema = from_yaml(Path::new(&data_path("poly_schema.yaml"))).unwrap();
    let mut sv = SchemaView::new();
    sv.add_schema(schema.clone()).unwrap();
    let conv = converter_from_schema(&schema);
    let class = sv
        .get_class(&Identifier::new("Container"), &conv)
        .unwrap()
        .expect("class not found");
    let v = load_yaml_file(Path::new(&data_path("poly_array.yaml")), &sv, &class, &conv)
        .unwrap()
        .into_instance()
        .unwrap();
    assert!(validate(&v).is_ok());
    if let LinkMLInstance::Object { values, .. } = v {
        let objs = values.get("objs").expect("objs not found");
        if let LinkMLInstance::List { values: arr, .. } = objs {
            assert_eq!(arr.len(), 3);
            match &arr[0] {
                LinkMLInstance::Object { class, .. } => assert_eq!(class.name(), "Child"),
                _ => panic!("expected map"),
            }
            match &arr[1] {
                LinkMLInstance::Object { class, .. } => assert_eq!(class.name(), "Child"),
                _ => panic!("expected map"),
            }
            match &arr[2] {
                LinkMLInstance::Object { class, .. } => assert_eq!(class.name(), "Parent"),
                _ => panic!("expected map"),
            }
        } else {
            panic!("expected list");
        }
    } else {
        panic!("expected map");
    }
}
