use linkml_runtime::{
    diff, load_json_str, load_yaml_file, patch, DiffOptions, LinkMLInstance, ValidationIssueCode,
};
use linkml_schemaview::identifier::converter_from_schema;
use linkml_schemaview::io::from_yaml;
use linkml_schemaview::schemaview::{ClassView, SchemaView};
use linkml_schemaview::Converter;
use std::path::{Path, PathBuf};

fn data_path(name: &str) -> PathBuf {
    let mut p = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    p.push("tests");
    p.push("data");
    p.push(name);
    p
}

fn info_path(name: &str) -> PathBuf {
    let mut p = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    p.push("tests");
    p.push("data");
    p.push(name);
    p
}

fn class_in_schema(
    sv: &SchemaView,
    schema: &linkml_meta::SchemaDefinition,
    class_name: &str,
) -> ClassView {
    let schema_id = schema.id.as_str();
    sv.get_class_by_schema(schema_id, class_name)
        .unwrap()
        .expect("class not found")
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
fn diff_and_patch_person() {
    let schema = from_yaml(Path::new(&data_path("schema.yaml"))).unwrap();
    let mut sv = SchemaView::new();
    sv.add_schema(schema.clone()).unwrap();
    let conv = converter_from_schema(&schema);
    let class = class_in_schema(&sv, &schema, "Person");
    let src = load_instance(
        Path::new(&data_path("person_valid.yaml")),
        &sv,
        &class,
        &conv,
    );
    let tgt = load_instance(
        Path::new(&data_path("person_older.yaml")),
        &sv,
        &class,
        &conv,
    );

    let deltas = diff(&src, &tgt, DiffOptions::default());
    assert_eq!(deltas.len(), 1);
    // Ensure delta paths are navigable on respective values
    for d in &deltas {
        if d.old.is_some() {
            assert!(src.navigate_path(&d.path).is_some());
        }
        if d.new.is_some() {
            assert!(tgt.navigate_path(&d.path).is_some());
        }
    }

    let (patched, _trace) = patch(
        &src,
        &deltas,
        linkml_runtime::diff::PatchOptions {
            ignore_no_ops: true,
            treat_missing_as_null: false,
        },
    )
    .unwrap();
    let patched_json = patched.to_json();
    let target_json = tgt.to_json();
    let src_json = src.to_json();
    assert_ne!(patched_json, target_json);
    assert_eq!(patched_json.get("age"), target_json.get("age"));
    assert_eq!(patched_json.get("internal_id"), src_json.get("internal_id"));
}

#[test]
fn diff_ignore_missing_target() {
    let schema = from_yaml(Path::new(&data_path("schema.yaml"))).unwrap();
    let mut sv = SchemaView::new();
    sv.add_schema(schema.clone()).unwrap();
    let conv = converter_from_schema(&schema);
    let class = class_in_schema(&sv, &schema, "Person");
    let src = load_instance(
        Path::new(&data_path("person_valid.yaml")),
        &sv,
        &class,
        &conv,
    );
    let tgt = load_instance(
        Path::new(&data_path("person_partial.yaml")),
        &sv,
        &class,
        &conv,
    );

    let deltas = diff(&src, &tgt, DiffOptions::default());
    assert!(deltas.is_empty());
    let (patched, _trace) = patch(
        &src,
        &deltas,
        linkml_runtime::diff::PatchOptions {
            ignore_no_ops: true,
            treat_missing_as_null: false,
        },
    )
    .unwrap();
    let patched_json = patched.to_json();
    let src_json = src.to_json();
    assert_eq!(patched_json, src_json);
}

#[test]
fn diff_and_patch_personinfo() {
    let schema = from_yaml(Path::new(&info_path("personinfo.yaml"))).unwrap();
    let mut sv = SchemaView::new();
    sv.add_schema(schema.clone()).unwrap();
    let conv = converter_from_schema(&schema);
    let container = class_in_schema(&sv, &schema, "Container");
    let src = load_instance(
        Path::new(&info_path("example_personinfo_data.yaml")),
        &sv,
        &container,
        &conv,
    );
    let tgt = load_instance(
        Path::new(&info_path("example_personinfo_data_2.yaml")),
        &sv,
        &container,
        &conv,
    );

    let deltas = diff(&src, &tgt, DiffOptions::default());
    assert!(!deltas.is_empty());
    // Ensure delta paths are navigable on respective values, including mapping-list keys
    for d in &deltas {
        if d.old.is_some() {
            assert!(src.navigate_path(&d.path).is_some());
        }
        if d.new.is_some() {
            assert!(tgt.navigate_path(&d.path).is_some());
        }
    }
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
fn diff_null_and_missing_semantics() {
    use linkml_runtime::LinkMLInstance;
    let schema = from_yaml(Path::new(&data_path("schema.yaml"))).unwrap();
    let mut sv = SchemaView::new();
    sv.add_schema(schema.clone()).unwrap();
    let conv = converter_from_schema(&schema);
    let class = class_in_schema(&sv, &schema, "Person");

    let src = load_instance(
        Path::new(&data_path("person_valid.yaml")),
        &sv,
        &class,
        &conv,
    );

    // X -> null => update to null
    if let LinkMLInstance::Object { .. } = src.clone() {
        let mut tgt_json = src.to_json();
        if let serde_json::Value::Object(ref mut m) = tgt_json {
            m.insert("age".to_string(), serde_json::Value::Null);
        }
        let tgt = load_json_instance(
            &serde_json::to_string(&tgt_json).unwrap(),
            &sv,
            &class,
            &conv,
        );
        let deltas = diff(&src, &tgt, DiffOptions::default());
        assert!(deltas
            .iter()
            .any(|d| d.path == vec!["age".to_string()] && d.new == Some(serde_json::Value::Null)));
    }

    // null -> X => update from null
    if let LinkMLInstance::Object { .. } = src.clone() {
        let mut src_json = src.to_json();
        if let serde_json::Value::Object(ref mut m) = src_json {
            m.insert("age".to_string(), serde_json::Value::Null);
        }
        let src_with_null = load_json_instance(
            &serde_json::to_string(&src_json).unwrap(),
            &sv,
            &class,
            &conv,
        );
        let deltas = diff(&src_with_null, &src, DiffOptions::default());
        assert!(deltas.iter().any(|d| d.path == vec!["age".to_string()]
            && d.old == Some(serde_json::Value::Null)
            && d.new.is_some()));
    }

    // missing -> X => add
    if let LinkMLInstance::Object { .. } = src.clone() {
        let mut src_json = src.to_json();
        if let serde_json::Value::Object(ref mut m) = src_json {
            m.remove("age");
        }
        let src_missing = load_json_instance(
            &serde_json::to_string(&src_json).unwrap(),
            &sv,
            &class,
            &conv,
        );
        let deltas = diff(&src_missing, &src, DiffOptions::default());
        assert!(deltas
            .iter()
            .any(|d| d.path == vec!["age".to_string()] && d.old.is_none() && d.new.is_some()));
    }

    // X -> missing: ignored by default; produce update-to-null when treat_missing_as_null=true
    if let LinkMLInstance::Object { .. } = src.clone() {
        let mut tgt_json = src.to_json();
        if let serde_json::Value::Object(ref mut m) = tgt_json {
            m.remove("age");
        }
        let tgt_missing = load_json_instance(
            &serde_json::to_string(&tgt_json).unwrap(),
            &sv,
            &class,
            &conv,
        );
        let deltas = diff(&src, &tgt_missing, DiffOptions::default());
        assert!(deltas.iter().all(|d| d.path != vec!["age".to_string()]));
        let deltas2 = diff(
            &src,
            &tgt_missing,
            DiffOptions {
                treat_missing_as_null: true,
                ..DiffOptions::default()
            },
        );
        assert!(deltas2
            .iter()
            .any(|d| d.path == vec!["age".to_string()] && d.new == Some(serde_json::Value::Null)))
    }
}

#[test]
fn personinfo_invalid_fails() {
    let schema = from_yaml(Path::new(&info_path("personinfo.yaml"))).unwrap();
    let mut sv = SchemaView::new();
    sv.add_schema(schema.clone()).unwrap();
    let conv = converter_from_schema(&schema);
    let class = class_in_schema(&sv, &schema, "Container");
    let outcome = load_yaml_file(
        Path::new(&info_path("example_personinfo_data_invalid.yaml")),
        &sv,
        &class,
        &conv,
    )
    .unwrap();
    let diags = outcome.validation_issues;
    assert!(diags
        .iter()
        .any(|d| matches!(d.code, ValidationIssueCode::UnknownSlot)
            && d.path.iter().any(|seg| seg == "unknown_attr")));
}
