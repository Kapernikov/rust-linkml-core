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
fn parse_alias_fields() {
    let schema = from_yaml(Path::new(&data_path("alias_schema.yaml"))).unwrap();
    let mut sv = SchemaView::new();
    sv.add_schema(schema.clone()).unwrap();
    let conv = converter_from_schema(&schema);
    let container = sv
        .get_class(&Identifier::new("Container"), &conv)
        .unwrap()
        .expect("class not found");
    let v = load_yaml_file(
        Path::new(&data_path("alias_data.yaml")),
        &sv,
        &container,
        &conv,
    )
    .unwrap()
    .into_instance()
    .unwrap();
    if let Err(e) = validate(&v) {
        println!("JSON: {:?}", v.to_json());
        panic!("validation failed: {}", e);
    }
    if let linkml_runtime::LinkMLInstance::Object { values, .. } = &v {
        let desc = values.get("description").expect("desc");
        if let linkml_runtime::LinkMLInstance::Object { values: item, .. } = desc {
            println!("json: {:?}", v.to_json());
            let desc_v = item.get("alt_description_text");
            assert!(desc_v.is_some(), "desc field missing");
            if let linkml_runtime::LinkMLInstance::Scalar { slot, .. } = desc_v.unwrap() {
                assert_eq!(slot.name, "alt_description_text");
            } else {
                panic!("wrong type for description");
            }
            let src_v = item.get("alt_description_source");
            assert!(src_v.is_some(), "src field missing");
            if let linkml_runtime::LinkMLInstance::Scalar { slot, .. } = src_v.unwrap() {
                assert_eq!(slot.name, "alt_description_source");
            } else {
                panic!("wrong type for source");
            }
        } else {
            panic!("wrong type for description slot");
        }
    } else {
        panic!("wrong root type");
    }
}
