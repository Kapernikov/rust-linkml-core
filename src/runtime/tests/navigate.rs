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
            // `objects` is inlined as a list of NamedThing, which declares an
            // `id` identifier: it is addressed by that label, not by position.
            // `has_medical_history` declares no identity, so it stays numeric.
            let inner = v.navigate_path([
                "objects",
                "P:002",
                "has_medical_history",
                "0",
                "diagnosis",
                "name",
            ]);
            assert!(inner.is_some());
            assert!(
                v.navigate_path(["objects", "2"]).is_none(),
                "a numeric segment must not address a label-addressed list"
            );
        }
        _ => panic!("expected map at root"),
    }
}

/// The identity fixture, loaded as a `Service` instance.
fn service(data: serde_json::Value) -> linkml_runtime::LinkMLInstance {
    let schema = from_yaml(Path::new(&info_path("identity.yaml"))).unwrap();
    let mut sv = SchemaView::new();
    sv.add_schema(schema.clone()).unwrap();
    let conv = converter_from_schema(&schema);
    let class = sv
        .get_class(&Identifier::new("Service"), &conv)
        .unwrap()
        .expect("class not found");
    linkml_runtime::load_json_str(&data.to_string(), &sv, &class, &conv)
        .unwrap()
        .into_instance()
        .unwrap()
}

fn scalar(v: Option<&linkml_runtime::LinkMLInstance>) -> Option<serde_json::Value> {
    v.map(|v| v.to_json())
}

#[test]
fn navigate_resolves_a_unique_keys_segment() {
    // `diff` addresses this list by its `unique_keys`-derived label, so a
    // delta path that names one has to be navigable.
    let v = service(serde_json::json!({"name": "svc", "hasPhoneNumber": [
        {"phoneNumber": "09/241.25.00", "hasNumberFunction": "Emergency_Number"},
        {"phoneNumber": "09/241.25.03", "hasNumberFunction": "Non_Urgent_Communication"}]}));
    assert_eq!(
        scalar(v.navigate_path(["hasPhoneNumber", "Non_Urgent_Communication", "phoneNumber"])),
        Some(serde_json::json!("09/241.25.03"))
    );
}

#[test]
fn navigate_resolves_a_composite_unique_key_segment() {
    let v = service(serde_json::json!({"name": "svc", "contacts": [
        {"kind": "Emergency", "phone": "02/111.11.11", "note": "first"},
        {"kind": "Operator", "phone": "02/333.33.33", "note": "second"}]}));
    assert_eq!(
        scalar(v.navigate_path(["contacts", r#"["Emergency","02/111.11.11"]"#, "note"])),
        Some(serde_json::json!("first"))
    );
}

#[test]
fn navigate_refuses_a_numeric_segment_into_a_label_addressed_list() {
    // `patch` refuses this ("report, never guess"); navigating must not
    // silently hand back a different element than the one the path names.
    let v = service(serde_json::json!({"name": "svc", "hasPhoneNumber": [
        {"phoneNumber": "09/241.25.00", "hasNumberFunction": "Emergency_Number"},
        {"phoneNumber": "09/241.25.03", "hasNumberFunction": "Non_Urgent_Communication"}]}));
    assert!(v.navigate_path(["hasPhoneNumber", "0"]).is_none());
}

#[test]
fn navigate_prefers_the_label_over_the_index_when_a_label_is_numeric() {
    // `lang` is a key slot, and one element's key IS "0". Index-first
    // navigation hands back the element at position 0 — the wrong one.
    let v = service(serde_json::json!({"name": "svc", "labelList": [
        {"lang": "nl", "text": "dutch"},
        {"lang": "0", "text": "zero"}]}));
    assert_eq!(
        scalar(v.navigate_path(["labelList", "0", "text"])),
        Some(serde_json::json!("zero"))
    );
}

#[test]
fn navigate_still_indexes_a_positional_list_numerically() {
    // PlainPhoneNumber declares no identity: diff emits numeric segments for
    // this list, and they must keep resolving.
    let v = service(serde_json::json!({"name": "svc", "plainPhoneNumber": [
        {"phoneNumber": "09/241.25.00", "hasNumberFunction": "Emergency_Number"},
        {"phoneNumber": "09/241.25.03", "hasNumberFunction": "Non_Urgent_Communication"}]}));
    assert_eq!(
        scalar(v.navigate_path(["plainPhoneNumber", "1", "phoneNumber"])),
        Some(serde_json::json!("09/241.25.03"))
    );
}
