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
    assert!(child.has_any_unique_key());
}

/// `has_any_unique_key` is the memoised existence check `diff` asks of every
/// list slot before deciding how to address its elements. It must answer
/// exactly `!unique_keys().is_empty()` — including out of the `is_a` and mixin
/// walks, where a class declaring nothing still inherits an entry — or a list
/// gets the wrong addressing.
#[test]
fn has_any_unique_key_agrees_with_unique_keys() {
    let sv = fixture();
    let conv = sv.converter();
    for (name, expected) in [
        ("Base", true),
        ("MixinCls", true),
        ("Child", true),
        ("Derived", true),   // inherited via is_a
        ("MixinUser", true), // inherited via mixin
        ("Plain", false),
    ] {
        let cv = sv
            .get_class(&Identifier::new(name), &conv)
            .unwrap()
            .expect("class not found");
        assert_eq!(cv.has_any_unique_key(), expected, "{name}");
        assert_eq!(
            cv.has_any_unique_key(),
            !cv.unique_keys().is_empty(),
            "{name}: the two spellings of the question must agree"
        );
        // Memoised: the second call must give the same answer.
        assert_eq!(cv.has_any_unique_key(), expected, "{name} (cached)");
    }
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
    assert!(!plain.has_any_unique_key());
}
