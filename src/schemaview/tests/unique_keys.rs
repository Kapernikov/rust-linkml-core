use linkml_schemaview::identifier::Identifier;
use linkml_schemaview::io::from_yaml;
use linkml_schemaview::schemaview::SchemaView;
use std::path::PathBuf;

fn fixture() -> SchemaView {
    let mut p = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    p.push("tests/data/unique_keys.yaml");
    let schema = from_yaml(&p).unwrap();
    let mut sv = SchemaView::new();
    sv.add_schema(schema).unwrap();
    sv
}

#[test]
fn unique_keys_merge_across_is_a_and_mixins_nearest_wins() {
    let sv = fixture();
    let conv = sv.converter();
    let child = sv
        .get_class(&Identifier::new("Child"), &conv)
        .unwrap()
        .expect("class not found");
    let uks = child.unique_keys();
    let names: Vec<&str> = uks.iter().map(|(n, _)| n.as_str()).collect();
    // name-sorted, merged from Base (by_code), MixinCls (by_tag), Child (shared_name override)
    assert_eq!(names, vec!["by_code", "by_tag", "shared_name"]);
    let shared = &uks.iter().find(|(n, _)| n == "shared_name").unwrap().1;
    assert_eq!(
        shared.unique_key_slots,
        vec!["child_field".to_string()],
        "the nearest declaration must win"
    );
}

#[test]
fn class_without_unique_keys_yields_empty() {
    let sv = fixture();
    let conv = sv.converter();
    let plain = sv
        .get_class(&Identifier::new("Plain"), &conv)
        .unwrap()
        .expect("class not found");
    assert!(plain.unique_keys().is_empty());
}
