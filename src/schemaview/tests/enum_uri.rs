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
fn enum_canonical_uri() {
    let enum_schema = from_yaml(Path::new(&data_path("simple_enum.yaml"))).unwrap();
    let mut sv = SchemaView::new();
    sv.add_schema(enum_schema.clone()).unwrap();
    let conv = converter_from_schemas([&enum_schema]);

    let enum_view = sv
        .get_enum(&Identifier::new("SignalTypes"), &conv)
        .unwrap()
        .expect("enum not found");

    assert_eq!(
        enum_view.canonical_uri().to_string(),
        "https://example.org/simple/SignalTypes"
    );
}
