use linkml_runtime::{diff, load_json_str, load_yaml_file, patch, DiffOptions, LinkMLInstance};
use linkml_schemaview::identifier::{converter_from_schema, Identifier};
use linkml_schemaview::io::from_yaml;
use linkml_schemaview::schemaview::{ClassView, SchemaView};
use linkml_schemaview::Converter;
use serde_json::Value as JsonValue;
use std::path::{Path, PathBuf};

fn data_path(name: &str) -> PathBuf {
    let mut p = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    p.push("tests");
    p.push("data");
    p.push(name);
    p
}

fn load_instance(
    path: &Path,
    sv: &SchemaView,
    class: &ClassView,
    conv: &Converter,
) -> LinkMLInstance {
    load_yaml_file(path, sv, class, conv)
        .unwrap()
        .into_instance()
        .unwrap()
}

fn load_json_instance(
    text: &str,
    sv: &SchemaView,
    class: &ClassView,
    conv: &Converter,
) -> LinkMLInstance {
    load_json_str(text, sv, class, conv)
        .unwrap()
        .into_instance()
        .unwrap()
}

#[test]
fn single_inlined_object_identifier_change_is_replacement() {
    // Use personinfo schema; diagnosis is an inlined object with identifier (via NamedThing)
    let schema = from_yaml(Path::new(&data_path("personinfo.yaml"))).unwrap();
    let mut sv = SchemaView::new();
    sv.add_schema(schema.clone()).unwrap();
    let conv = converter_from_schema(&schema);
    let container = sv
        .get_class(&Identifier::new("Container"), &conv)
        .unwrap()
        .expect("class not found");

    let src = load_instance(
        Path::new(&data_path("example_personinfo_data.yaml")),
        &sv,
        &container,
        &conv,
    );

    // Modify diagnosis.id of the first medical history event for P:002
    let mut tgt_json = src.to_json();
    if let JsonValue::Object(ref mut root) = tgt_json {
        if let Some(JsonValue::Array(objects)) = root.get_mut("objects") {
            if let Some(JsonValue::Object(p2)) = objects.get_mut(2) {
                if let Some(JsonValue::Array(mh)) = p2.get_mut("has_medical_history") {
                    if let Some(JsonValue::Object(ev0)) = mh.get_mut(0) {
                        if let Some(JsonValue::Object(diag)) = ev0.get_mut("diagnosis") {
                            diag.insert(
                                "id".to_string(),
                                JsonValue::String("CODE:D9999".to_string()),
                            );
                        }
                    }
                }
            }
        }
    }
    let tgt = load_json_instance(
        &serde_json::to_string(&tgt_json).unwrap(),
        &sv,
        &container,
        &conv,
    );

    let deltas_default = diff(&src, &tgt, DiffOptions::default());
    // Expect a single replacement at the diagnosis object path with default behaviour
    assert_eq!(deltas_default.len(), 1);
    let d = &deltas_default[0];
    assert_eq!(
        d.path,
        vec![
            "objects".to_string(),
            "P:002".to_string(),
            "has_medical_history".to_string(),
            "0".to_string(),
            "diagnosis".to_string()
        ]
    );
    assert!(d.old.is_some() && d.new.is_some());

    // Patch should yield target for default behaviour
    let (patched_default, _trace_default) = patch(
        &src,
        &deltas_default,
        linkml_runtime::diff::PatchOptions {
            ignore_no_ops: true,
            treat_missing_as_null: false,
        },
    )
    .unwrap();
    assert_eq!(patched_default.to_json(), tgt.to_json());

    // When treating identifier changes as regular field updates, expect an update on the id slot only
    let deltas_plain = diff(
        &src,
        &tgt,
        DiffOptions {
            treat_changed_identifier_as_new_object: false,
            ..DiffOptions::default()
        },
    );
    assert_eq!(deltas_plain.len(), 1);
    let id_delta = &deltas_plain[0];
    assert_eq!(
        id_delta.path,
        vec![
            "objects".to_string(),
            "P:002".to_string(),
            "has_medical_history".to_string(),
            "0".to_string(),
            "diagnosis".to_string(),
            "id".to_string()
        ]
    );
    assert!(id_delta.old.is_some() && id_delta.new.is_some());

    let (patched_plain, _trace_plain) = patch(
        &src,
        &deltas_plain,
        linkml_runtime::diff::PatchOptions {
            ignore_no_ops: true,
            treat_missing_as_null: false,
        },
    )
    .unwrap();
    assert_eq!(patched_plain.to_json(), tgt.to_json());
}

#[test]
fn single_inlined_object_non_identifier_change_is_field_delta() {
    let schema = from_yaml(Path::new(&data_path("personinfo.yaml"))).unwrap();
    let mut sv = SchemaView::new();
    sv.add_schema(schema.clone()).unwrap();
    let conv = converter_from_schema(&schema);
    let container = sv
        .get_class(&Identifier::new("Container"), &conv)
        .unwrap()
        .expect("class not found");

    let src = load_instance(
        Path::new(&data_path("example_personinfo_data.yaml")),
        &sv,
        &container,
        &conv,
    );

    // Modify diagnosis.name only
    let mut tgt_json = src.to_json();
    if let JsonValue::Object(ref mut root) = tgt_json {
        if let Some(JsonValue::Array(objects)) = root.get_mut("objects") {
            if let Some(JsonValue::Object(p2)) = objects.get_mut(2) {
                if let Some(JsonValue::Array(mh)) = p2.get_mut("has_medical_history") {
                    if let Some(JsonValue::Object(ev0)) = mh.get_mut(0) {
                        if let Some(JsonValue::Object(diag)) = ev0.get_mut("diagnosis") {
                            diag.insert(
                                "name".to_string(),
                                JsonValue::String("new name".to_string()),
                            );
                        }
                    }
                }
            }
        }
    }
    let tgt = load_json_instance(
        &serde_json::to_string(&tgt_json).unwrap(),
        &sv,
        &container,
        &conv,
    );

    let deltas = diff(&src, &tgt, DiffOptions::default());
    assert!(deltas.iter().any(|d| d.path
        == vec![
            "objects".to_string(),
            "P:002".to_string(),
            "has_medical_history".to_string(),
            "0".to_string(),
            "diagnosis".to_string(),
            "name".to_string()
        ]));
    // Must not collapse to whole-object replacement here
    assert!(!deltas.iter().any(|d| d.path
        == vec![
            "objects".to_string(),
            "P:002".to_string(),
            "has_medical_history".to_string(),
            "0".to_string(),
            "diagnosis".to_string()
        ]));

    let (patched, _trace) = patch(
        &src,
        &deltas,
        linkml_runtime::diff::PatchOptions {
            ignore_no_ops: true,
            treat_missing_as_null: false,
        },
    )
    .unwrap();
    assert_eq!(patched.to_json(), tgt.to_json());
}

#[test]
fn list_inlined_object_identifier_change_is_replacement() {
    let schema = from_yaml(Path::new(&data_path("personinfo.yaml"))).unwrap();
    let mut sv = SchemaView::new();
    sv.add_schema(schema.clone()).unwrap();
    let conv = converter_from_schema(&schema);
    let container = sv
        .get_class(&Identifier::new("Container"), &conv)
        .unwrap()
        .expect("class not found");

    let src = load_instance(
        Path::new(&data_path("example_personinfo_data.yaml")),
        &sv,
        &container,
        &conv,
    );

    // Change the id of the third object (P:002)
    let mut tgt_json = src.to_json();
    if let JsonValue::Object(ref mut root) = tgt_json {
        if let Some(JsonValue::Array(objects)) = root.get_mut("objects") {
            if let Some(JsonValue::Object(p2)) = objects.get_mut(2) {
                p2.insert("id".to_string(), JsonValue::String("P:099".to_string()));
            }
        }
    }
    let tgt = load_json_instance(
        &serde_json::to_string(&tgt_json).unwrap(),
        &sv,
        &container,
        &conv,
    );

    let deltas = diff(&src, &tgt, DiffOptions::default());
    // Expect a single replacement at the list item path
    assert!(deltas.iter().any(|d| {
        d.path == vec!["objects".to_string(), "P:002".to_string()]
            || d.path == vec!["objects".to_string(), "P:099".to_string()]
    }));
    assert!(!deltas
        .iter()
        .any(|d| { d.path == vec!["objects".to_string(), "P:002".to_string(), "id".to_string()] }));

    let (patched, _trace) = patch(
        &src,
        &deltas,
        linkml_runtime::diff::PatchOptions {
            ignore_no_ops: true,
            treat_missing_as_null: false,
        },
    )
    .unwrap();
    assert_eq!(patched.to_json(), tgt.to_json());
}

#[test]
fn mapping_inlined_identifier_change_is_add_delete() {
    // Use mapping schema with inlined_as_dict keyed by 'key'
    let schema = from_yaml(Path::new(&data_path("mapping_schema.yaml"))).unwrap();
    let mut sv = SchemaView::new();
    sv.add_schema(schema.clone()).unwrap();
    let conv = converter_from_schema(&schema);
    let bag = sv
        .get_class(&Identifier::new("Bag"), &conv)
        .unwrap()
        .expect("class not found");

    let src = linkml_runtime::load_json_file(
        Path::new(&data_path("mapping_data.json")),
        &sv,
        &bag,
        &conv,
    )
    .unwrap()
    .into_instance()
    .unwrap();

    // Rename mapping key 'alpha' to 'alpha2'
    let mut tgt_json = src.to_json();
    if let JsonValue::Object(ref mut root) = tgt_json {
        if let Some(JsonValue::Object(things)) = root.get_mut("things") {
            if let Some(alpha) = things.remove("alpha") {
                things.insert("alpha2".to_string(), alpha);
            }
        }
    }
    let tgt = load_json_instance(&serde_json::to_string(&tgt_json).unwrap(), &sv, &bag, &conv);

    let deltas = diff(&src, &tgt, DiffOptions::default());
    // Expect one delete and one add at mapping keys; no inner key-slot deltas
    assert!(deltas
        .iter()
        .any(|d| d.path == vec!["things".to_string(), "alpha".to_string()] && d.new.is_none()));
    assert!(deltas
        .iter()
        .any(|d| d.path == vec!["things".to_string(), "alpha2".to_string()] && d.old.is_none()));
    assert!(!deltas
        .iter()
        .any(|d| d.path == vec!["things".to_string(), "alpha".to_string(), "key".to_string()]));

    let (patched, _trace) = patch(
        &src,
        &deltas,
        linkml_runtime::diff::PatchOptions {
            ignore_no_ops: true,
            treat_missing_as_null: false,
        },
    )
    .unwrap();
    assert_eq!(patched.to_json(), tgt.to_json());
}
