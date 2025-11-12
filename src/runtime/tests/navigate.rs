use linkml_runtime::load_yaml_file;
use linkml_schemaview::identifier::{converter_from_schema, Identifier};
use linkml_schemaview::io::from_yaml;
use linkml_schemaview::schemaview::SchemaView;
use std::path::{Path, PathBuf};

fn info_path(name: &str) -> PathBuf {
    let mut p = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    p.push("tests");
    p.push("data");
    p.push(name);
    p
}

#[test]
fn navigate_basic() {
    let schema = from_yaml(Path::new(&info_path("personinfo.yaml"))).unwrap();
    let mut sv = SchemaView::new();
    sv.add_schema(schema.clone()).unwrap();
    let conv = converter_from_schema(&schema);
    let container = sv
        .get_class(&Identifier::new("Container"), &conv)
        .unwrap()
        .expect("class not found");
    let v = load_yaml_file(
        Path::new(&info_path("example_personinfo_data.yaml")),
        &sv,
        &container,
        &conv,
    )
    .unwrap()
    .into_instance()
    .unwrap();
    // Map root should have key 'objects'
    match &v {
        linkml_runtime::LinkMLInstance::Object { values, .. } => {
            assert!(values.contains_key("objects"));
            let inner = v.navigate_path([
                "objects",
                "2",
                "has_medical_history",
                "0",
                "diagnosis",
                "name",
            ]);
            assert!(inner.is_some());
        }
        _ => panic!("expected map at root"),
    }
}
