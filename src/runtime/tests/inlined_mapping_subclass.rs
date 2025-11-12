use linkml_runtime::{load_json_file, validate, LinkMLInstance};
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
fn inlined_mapping_selects_subclass_by_typeuri() {
    // Load simple schema
    let schema = from_yaml(Path::new(&data_path("mapping_schema.yaml"))).unwrap();
    let mut sv = SchemaView::new();
    sv.add_schema(schema.clone()).unwrap();
    let conv = converter_from_schema(&schema);

    // Base container class
    let bag = sv
        .get_class(&Identifier::new("Bag"), &conv)
        .unwrap()
        .expect("class not found");

    // Load JSON data
    let v = load_json_file(Path::new(&data_path("mapping_data.json")), &sv, &bag, &conv)
        .unwrap()
        .into_instance()
        .unwrap();

    // Instance should validate
    assert!(validate(&v).is_ok());

    // Ensure inlined mapping children select subclasses based on typeURI
    match v {
        LinkMLInstance::Object { values, .. } => {
            let things = values.get("things").expect("things slot missing");
            match things {
                LinkMLInstance::Mapping { values, .. } => {
                    match values.get("alpha").expect("alpha missing") {
                        LinkMLInstance::Object { class, .. } => assert_eq!(class.name(), "ThingA"),
                        _ => panic!("alpha should be an object"),
                    }
                    match values.get("beta").expect("beta missing") {
                        LinkMLInstance::Object { class, .. } => assert_eq!(class.name(), "ThingB"),
                        _ => panic!("beta should be an object"),
                    }
                }
                _ => panic!("things should be a mapping"),
            }
        }
        _ => panic!("expected top-level object"),
    }
}
