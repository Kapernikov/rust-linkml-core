use linkml_runtime::load_json_str;
use linkml_schemaview::identifier::{converter_from_schema, Identifier};
use linkml_schemaview::io::from_yaml;
use linkml_schemaview::schemaview::SchemaView;
use serde_json::json;
use std::path::{Path, PathBuf};

fn data_path(name: &str) -> PathBuf {
    let mut p = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    p.push("tests");
    p.push("data");
    p.push(name);
    p
}

#[test]
fn multivalued_non_inlined_refs_remain_strings() {
    let schema = from_yaml(Path::new(&data_path("non_inlined_list_schema.yaml"))).unwrap();
    let mut sv = SchemaView::new();
    sv.add_schema(schema.clone()).unwrap();
    let conv = converter_from_schema(&schema);
    let class = sv
        .get_class(&Identifier::new("Trail"), &conv)
        .unwrap()
        .expect("class not found");

    let load_result = load_json_str(
        r#"{ "id": "trail-1", "ports": ["https://example.org/Ports/p1", "https://example.org/Ports/p2"] }"#,
        &sv,
        &class,
        &conv,
    )
    .unwrap();

    assert!(load_result.validation_issues.is_empty());

    let value = load_result
        .instance
        .expect("expected instance from load_json_str");

    assert_eq!(
        value.to_json()["ports"],
        json!([
            "https://example.org/Ports/p1",
            "https://example.org/Ports/p2"
        ])
    );
}
