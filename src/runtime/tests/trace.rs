use linkml_runtime::{diff, load_json_str, load_yaml_file, DiffOptions, LinkMLInstance};
use linkml_schemaview::identifier::{converter_from_schema, Identifier};
use linkml_schemaview::io::from_yaml;
use linkml_schemaview::schemaview::{ClassView, SchemaView};
use linkml_schemaview::Converter;
use std::collections::HashSet;
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

fn collect_ids(v: &linkml_runtime::LinkMLInstance, out: &mut Vec<u64>) {
    out.push(v.node_id());
    match v {
        linkml_runtime::LinkMLInstance::Scalar { .. } => {}
        linkml_runtime::LinkMLInstance::Null { .. } => {}
        linkml_runtime::LinkMLInstance::List { values, .. } => {
            for c in values {
                collect_ids(c, out);
            }
        }
        linkml_runtime::LinkMLInstance::Mapping { values, .. }
        | linkml_runtime::LinkMLInstance::Object { values, .. } => {
            for c in values.values() {
                collect_ids(c, out);
            }
        }
    }
}

#[test]
fn node_ids_preserved_scalar_update() {
    let schema = from_yaml(Path::new(&data_path("schema.yaml"))).unwrap();
    let mut sv = SchemaView::new();
    sv.add_schema(schema.clone()).unwrap();
    let conv = converter_from_schema(&schema);
    let class = sv
        .get_class(&Identifier::new("Person"), &conv)
        .unwrap()
        .expect("class not found");
    let src = load_instance(
        Path::new(&data_path("person_valid.yaml")),
        &sv,
        &class,
        &conv,
    );
    let mut tgt_json = src.to_json();
    if let serde_json::Value::Object(ref mut m) = tgt_json {
        m.insert("age".to_string(), serde_json::json!(99));
    }
    let tgt = load_json_str(
        &serde_json::to_string(&tgt_json).unwrap(),
        &sv,
        &class,
        &conv,
    )
    .unwrap()
    .into_instance()
    .unwrap();

    let deltas = diff(&src, &tgt, DiffOptions::default());
    let (patched, trace) = linkml_runtime::patch(
        &src,
        &deltas,
        linkml_runtime::diff::PatchOptions {
            ignore_no_ops: true,
            treat_missing_as_null: false,
        },
    )
    .unwrap();

    assert!(trace.added.is_empty());
    assert!(trace.deleted.is_empty());
    assert!(!trace.updated.is_empty());

    let src_age = src.navigate_path(["age"]).unwrap();
    let pat_age = patched.navigate_path(["age"]).unwrap();
    assert_eq!(src_age.node_id(), pat_age.node_id());
    assert!(trace.updated.contains(&pat_age.node_id()));
}

#[test]
fn patch_trace_add_in_list() {
    let schema = from_yaml(Path::new(&info_path("personinfo.yaml"))).unwrap();
    let mut sv = SchemaView::new();
    sv.add_schema(schema.clone()).unwrap();
    let conv = converter_from_schema(&schema);
    let container = sv
        .get_class(&Identifier::new("Container"), &conv)
        .unwrap()
        .expect("class not found");
    let base = load_instance(
        Path::new(&info_path("example_personinfo_data.yaml")),
        &sv,
        &container,
        &conv,
    );

    // Add a new object to the 'objects' list
    let mut base_json = base.to_json();
    if let serde_json::Value::Object(ref mut root) = base_json {
        if let Some(serde_json::Value::Array(ref mut arr)) = root.get_mut("objects") {
            let new_obj = serde_json::json!({
                "id": "P:999",
                "name": "Added Person",
                "objecttype": "https://w3id.org/linkml/examples/personinfo/Person"
            });
            arr.push(new_obj);
        }
    }
    let target = load_json_str(
        &serde_json::to_string(&base_json).unwrap(),
        &sv,
        &container,
        &conv,
    )
    .unwrap()
    .into_instance()
    .unwrap();

    let deltas = diff(&base, &target, DiffOptions::default());
    let mut pre = Vec::new();
    collect_ids(&base, &mut pre);
    let (patched, trace) = linkml_runtime::patch(
        &base,
        &deltas,
        linkml_runtime::diff::PatchOptions {
            ignore_no_ops: true,
            treat_missing_as_null: false,
        },
    )
    .unwrap();
    let mut post = Vec::new();
    collect_ids(&patched, &mut post);

    let pre_set: HashSet<u64> = pre.into_iter().collect();
    let post_set: HashSet<u64> = post.into_iter().collect();
    let added: HashSet<u64> = post_set.difference(&pre_set).copied().collect();
    let trace_added: HashSet<u64> = trace.added.iter().copied().collect();
    assert_eq!(added, trace_added);
    assert!(!added.is_empty());
}

#[test]
fn patch_missing_to_null_semantics() {
    // Use simple schema
    let schema = from_yaml(Path::new(&data_path("schema.yaml"))).unwrap();
    let mut sv = SchemaView::new();
    sv.add_schema(schema.clone()).unwrap();
    let conv = converter_from_schema(&schema);
    let class = sv
        .get_class(&Identifier::new("Person"), &conv)
        .unwrap()
        .expect("class not found");

    let src = load_instance(
        Path::new(&data_path("person_partial.yaml")),
        &sv,
        &class,
        &conv,
    );
    // Build delta: set age to explicit null
    let deltas = vec![linkml_runtime::Delta {
        path: vec!["age".to_string()],
        op: linkml_runtime::diff::DeltaOp::Update,
        old: None,
        new: Some(serde_json::Value::Null),
    }];

    // treat_missing_as_null = true => no-op; no trace changes, no node id changes
    let pre_id = src.node_id();
    let (patched_same, trace_same) = linkml_runtime::patch(
        &src,
        &deltas,
        linkml_runtime::diff::PatchOptions {
            ignore_no_ops: true,
            treat_missing_as_null: true,
        },
    )
    .unwrap();
    assert!(
        trace_same.added.is_empty()
            && trace_same.deleted.is_empty()
            && trace_same.updated.is_empty()
    );
    assert_eq!(pre_id, patched_same.node_id());
    // Equality under treat_missing_as_null=true must hold
    assert!(src.equals(&patched_same, true));
    // And age remains absent (since explicit null is treated as omitted)
    if let linkml_runtime::LinkMLInstance::Object { values, .. } = &patched_same {
        assert!(!values.contains_key("age"));
    }

    // treat_missing_as_null = false => apply explicit null
    let (patched_null, trace_applied) = linkml_runtime::patch(
        &src,
        &deltas,
        linkml_runtime::diff::PatchOptions {
            ignore_no_ops: true,
            treat_missing_as_null: false,
        },
    )
    .unwrap();
    assert!(trace_applied.updated.contains(&patched_null.node_id()));
    // age present as Null
    if let linkml_runtime::LinkMLInstance::Object { values, .. } = &patched_null {
        assert!(matches!(
            values.get("age"),
            Some(linkml_runtime::LinkMLInstance::Null { .. })
        ));
    } else {
        panic!("expected object root");
    }
}

#[test]
fn patch_records_failed_paths() {
    let schema = from_yaml(Path::new(&data_path("schema.yaml"))).unwrap();
    let mut sv = SchemaView::new();
    sv.add_schema(schema.clone()).unwrap();
    let conv = converter_from_schema(&schema);
    let class = sv
        .get_class(&Identifier::new("Person"), &conv)
        .unwrap()
        .expect("class not found");
    let src = load_instance(
        Path::new(&data_path("person_valid.yaml")),
        &sv,
        &class,
        &conv,
    );

    let deltas = vec![linkml_runtime::Delta {
        path: vec!["no_such_slot".to_string()],
        op: linkml_runtime::diff::DeltaOp::Remove,
        old: Some(serde_json::json!("irrelevant")),
        new: None,
    }];

    let (_patched, trace) = linkml_runtime::patch(&src, &deltas, Default::default()).unwrap();
    assert_eq!(trace.failed, vec![vec!["no_such_slot".to_string()]]);
    assert!(trace.updated.is_empty());
    assert!(trace.added.is_empty());
    assert!(trace.deleted.is_empty());
}
