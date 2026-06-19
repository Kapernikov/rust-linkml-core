use linkml_runtime::{
    diff, load_json_str, load_yaml_file, patch, DeltaOp, DiffOptions, LinkMLInstance,
    ValidationProblemType,
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

    let deltas = diff(&src, &tgt, DiffOptions::new(false));
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

    let deltas = diff(&src, &tgt, DiffOptions::new(false));
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

    let deltas = diff(&src, &tgt, DiffOptions::new(false));
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
        let deltas = diff(&src, &tgt, DiffOptions::new(false));
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
        let deltas = diff(&src_with_null, &src, DiffOptions::new(false));
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
        let deltas = diff(&src_missing, &src, DiffOptions::new(false));
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
        let deltas = diff(&src, &tgt_missing, DiffOptions::new(false));
        assert!(deltas.iter().all(|d| d.path != vec!["age".to_string()]));
        let deltas2 = diff(&src, &tgt_missing, DiffOptions::new(true));
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
    assert!(diags.iter().any(
        |d| matches!(d.problem_type, ValidationProblemType::UndeclaredSlot)
            && d.subject.iter().any(|seg| seg == "unknown_attr")
    ));
}

#[test]
fn diff_and_patch_tolerate_validation_errors() {
    let schema = from_yaml(Path::new(&info_path("personinfo.yaml"))).unwrap();
    let mut sv = SchemaView::new();
    sv.add_schema(schema.clone()).unwrap();
    let conv = converter_from_schema(&schema);
    let container = class_in_schema(&sv, &schema, "Container");
    let invalid_result = load_yaml_file(
        Path::new(&info_path("container_person_bad_email.yaml")),
        &sv,
        &container,
        &conv,
    )
    .unwrap();
    assert!(invalid_result.has_errors());
    let invalid_instance = invalid_result
        .into_instance_tolerate_errors()
        .expect("instance should still be returned");
    let valid_instance = load_instance(
        Path::new(&info_path("example_personinfo_data.yaml")),
        &sv,
        &container,
        &conv,
    );
    let deltas = diff(&invalid_instance, &valid_instance, DiffOptions::new(false));
    assert!(!deltas.is_empty());
    let (patched, _trace) = patch(
        &invalid_instance,
        &deltas,
        linkml_runtime::diff::PatchOptions {
            ignore_no_ops: true,
            treat_missing_as_null: false,
        },
    )
    .unwrap();
    assert_eq!(patched.to_json(), valid_instance.to_json());
}

#[test]
fn diff_and_patch_invalid_target() {
    let schema = from_yaml(Path::new(&info_path("personinfo.yaml"))).unwrap();
    let mut sv = SchemaView::new();
    sv.add_schema(schema.clone()).unwrap();
    let conv = converter_from_schema(&schema);
    let container = class_in_schema(&sv, &schema, "Container");
    let valid_instance = load_instance(
        Path::new(&info_path("example_personinfo_data.yaml")),
        &sv,
        &container,
        &conv,
    );
    let invalid_result = load_yaml_file(
        Path::new(&info_path("container_person_bad_email.yaml")),
        &sv,
        &container,
        &conv,
    )
    .unwrap();
    assert!(invalid_result.has_errors());
    let invalid_instance = invalid_result
        .into_instance_tolerate_errors()
        .expect("instance should still be returned");
    let deltas = diff(&valid_instance, &invalid_instance, DiffOptions::new(false));
    assert!(!deltas.is_empty());
    let (patched, _trace) = patch(
        &valid_instance,
        &deltas,
        linkml_runtime::diff::PatchOptions {
            ignore_no_ops: true,
            treat_missing_as_null: false,
        },
    )
    .unwrap();
    assert_eq!(patched.to_json(), invalid_instance.to_json());
}

/// Regression: `treat_missing_as_null=false` must suppress `Remove` deltas for
/// mapping entries that are present in the source but absent in the target.
#[test]
fn diff_mapping_ignore_missing_target() {
    let schema = from_yaml(Path::new(&info_path("personinfo.yaml"))).unwrap();
    let mut sv = SchemaView::new();
    sv.add_schema(schema.clone()).unwrap();
    let conv = converter_from_schema(&schema);
    let person = class_in_schema(&sv, &schema, "Person");

    // Source has two familial relationships (mapping keyed by `role`).
    let source = load_json_instance(
        r#"{
            "id": "P:100",
            "name": "alice",
            "has_familial_relationships": {
                "brother": { "related_to": "P:001", "type": "SIBLING_OF" },
                "mother":  { "related_to": "P:002", "type": "PARENT_OF" }
            }
        }"#,
        &sv,
        &person,
        &conv,
    );

    // Target only has one — "mother" is absent.
    let target = load_json_instance(
        r#"{
            "id": "P:100",
            "name": "alice",
            "has_familial_relationships": {
                "brother": { "related_to": "P:001", "type": "SIBLING_OF" }
            }
        }"#,
        &sv,
        &person,
        &conv,
    );

    // Default: treat_missing_as_null=false → no Remove for "mother"
    let deltas = diff(&source, &target, DiffOptions::new(false));
    assert!(
        !deltas.iter().any(|d| d.op == DeltaOp::Remove),
        "expected no Remove deltas with treat_missing_as_null=false, got: {deltas:?}"
    );

    // With treat_missing_as_null=true → should produce a Remove for "mother"
    let deltas_strict = diff(&source, &target, DiffOptions::new(true));
    assert!(
        deltas_strict.iter().any(|d| d.op == DeltaOp::Remove
            && d.path
                .iter()
                .any(|seg| seg == "mother")),
        "expected a Remove delta for 'mother' with treat_missing_as_null=true, got: {deltas_strict:?}"
    );
}

/// Regression: removing an item from a list of inlined objects keyed by
/// identifier must produce a clean `Remove` delta addressed by id, not a
/// chain of shifted `Update` deltas. The bug is exposed when the removal is
/// from the middle of the list AND a trailing item also changes: positional
/// diff emits `Update(P:002→P:003')` + `Remove(P:003)`, and on patch the
/// label-resolver picks the just-updated P:003 for removal, silently
/// clobbering the trailing update.
#[test]
fn diff_and_patch_remove_from_keyed_list() {
    let schema = from_yaml(Path::new(&info_path("personinfo.yaml"))).unwrap();
    let mut sv = SchemaView::new();
    sv.add_schema(schema.clone()).unwrap();
    let conv = converter_from_schema(&schema);
    let container = class_in_schema(&sv, &schema, "Container");

    let src = load_json_instance(
        r#"{
            "objects": [
                {"id":"P:001","name":"a","objecttype":"https://w3id.org/linkml/examples/personinfo/Person"},
                {"id":"P:002","name":"b","objecttype":"https://w3id.org/linkml/examples/personinfo/Person"},
                {"id":"P:003","name":"c","objecttype":"https://w3id.org/linkml/examples/personinfo/Person"}
            ]
        }"#,
        &sv,
        &container,
        &conv,
    );
    let tgt = load_json_instance(
        r#"{
            "objects": [
                {"id":"P:001","name":"a","objecttype":"https://w3id.org/linkml/examples/personinfo/Person"},
                {"id":"P:003","name":"c-updated","objecttype":"https://w3id.org/linkml/examples/personinfo/Person"}
            ]
        }"#,
        &sv,
        &container,
        &conv,
    );

    let deltas = diff(&src, &tgt, DiffOptions::new(false));
    assert!(
        deltas.iter().any(|d| d.op == DeltaOp::Remove
            && d.path == vec!["objects".to_string(), "P:002".to_string()]),
        "expected Remove at objects/P:002 for the removed keyed item, got: {deltas:?}"
    );

    let (patched, trace) = patch(
        &src,
        &deltas,
        linkml_runtime::diff::PatchOptions {
            ignore_no_ops: true,
            treat_missing_as_null: false,
        },
    )
    .unwrap();
    assert!(
        trace.failed.is_empty(),
        "no delta should fail to apply, got failed: {:?}",
        trace.failed
    );
    assert_eq!(
        patched.to_json(),
        tgt.to_json(),
        "patched source should equal target after keyed-list remove"
    );
}

#[test]
fn diff_and_patch_multiple_removes_from_scalar_list() {
    // Regression: removing multiple items from an index-addressed list
    // must not shift indices so that later Removes target the wrong element.
    let schema = from_yaml(Path::new(&info_path("personinfo.yaml"))).unwrap();
    let mut sv = SchemaView::new();
    sv.add_schema(schema.clone()).unwrap();
    let conv = converter_from_schema(&schema);
    let class = class_in_schema(&sv, &schema, "Person");

    let src = load_json_instance(
        r#"{"id":"P:001","name":"fred","aliases":["a","b","c","d"]}"#,
        &sv,
        &class,
        &conv,
    );
    let tgt = load_json_instance(
        r#"{"id":"P:001","name":"fred","aliases":["a"]}"#,
        &sv,
        &class,
        &conv,
    );

    let deltas = diff(&src, &tgt, DiffOptions::new(false));
    let remove_count = deltas.iter().filter(|d| d.op == DeltaOp::Remove).count();
    assert_eq!(
        remove_count, 3,
        "expected 3 Remove deltas for b,c,d; got: {deltas:?}"
    );

    let (patched, trace) = patch(
        &src,
        &deltas,
        linkml_runtime::diff::PatchOptions {
            ignore_no_ops: true,
            treat_missing_as_null: false,
        },
    )
    .unwrap();
    assert!(
        trace.failed.is_empty(),
        "no delta should fail to apply, got failed: {:?}",
        trace.failed
    );
    assert_eq!(
        patched.to_json(),
        tgt.to_json(),
        "patched source should equal target after multi-remove"
    );
}

/// Regression (WI193): lists of inlined objects with **no** key/identifier
/// slot (e.g. config-driven repeat tables) must be diffed as a sequence, not
/// positionally. Deleting the first row used to shift every later row into a
/// false `Update` and remove the wrong one. An LCS keeps untouched rows quiet,
/// handles duplicate rows by multiplicity, and still reports an in-place edit
/// as a single field-level `Update` rather than a remove+add. All three cases
/// must also round-trip through `patch`.
#[test]
fn diff_and_patch_keyless_object_list_shifts() {
    let schema = from_yaml(Path::new(&info_path("personinfo.yaml"))).unwrap();
    let mut sv = SchemaView::new();
    sv.add_schema(schema.clone()).unwrap();
    let conv = converter_from_schema(&schema);
    let person = class_in_schema(&sv, &schema, "Person");

    // MedicalEvent has no key/identifier slot, so `has_medical_history` is a
    // keyless inlined-object list. Rows are distinguished by `started_at_time`.
    let e1 = r#"{"started_at_time":"2020-01-01","duration":1.0}"#;
    let e2 = r#"{"started_at_time":"2021-02-02","duration":2.0}"#;
    let e2_edited = r#"{"started_at_time":"2021-02-02","duration":9.0}"#;
    let person_with = |events: &str| -> String {
        format!(r#"{{"id":"P:001","name":"fred","has_medical_history":[{events}]}}"#)
    };
    let load = |json: &str| load_json_instance(json, &sv, &person, &conv);

    let roundtrip = |src: &LinkMLInstance, tgt: &LinkMLInstance| {
        let deltas = diff(src, tgt, DiffOptions::new(false));
        let (patched, trace) = patch(
            src,
            &deltas,
            linkml_runtime::diff::PatchOptions {
                ignore_no_ops: true,
                treat_missing_as_null: false,
            },
        )
        .unwrap();
        assert!(
            trace.failed.is_empty(),
            "no delta should fail to apply, got failed: {:?}",
            trace.failed
        );
        assert_eq!(
            patched.to_json(),
            tgt.to_json(),
            "patched source must equal target"
        );
        deltas
    };

    // Case 1: delete the first row [E1, E2] -> [E2]. Exactly one Remove, no
    // Update — the surviving row E2 must NOT be flagged as edited.
    let src = load(&person_with(&format!("{e1},{e2}")));
    let tgt = load(&person_with(e2));
    let deltas = roundtrip(&src, &tgt);
    assert_eq!(
        deltas.iter().filter(|d| d.op == DeltaOp::Remove).count(),
        1,
        "delete-first-row: expected exactly one Remove, got: {deltas:?}"
    );
    assert!(
        deltas.iter().all(|d| d.op == DeltaOp::Remove),
        "delete-first-row: only a Remove is allowed, got: {deltas:?}"
    );

    // Case 2: duplicate rows [E1, E1, E2] -> [E1, E2]. One copy of E1 is gone;
    // multiplicity-aware diff must emit exactly one Remove and nothing else.
    let src = load(&person_with(&format!("{e1},{e1},{e2}")));
    let tgt = load(&person_with(&format!("{e1},{e2}")));
    let deltas = roundtrip(&src, &tgt);
    assert_eq!(
        deltas.len(),
        1,
        "duplicate-row: expected a single delta, got: {deltas:?}"
    );
    assert_eq!(
        deltas[0].op,
        DeltaOp::Remove,
        "duplicate-row: the single delta must be a Remove, got: {deltas:?}"
    );

    // Case 3: in-place edit [E1, E2] -> [E1, E2'] (duration changed). Must be a
    // single field-level Update, not a remove+add of the whole row.
    let src = load(&person_with(&format!("{e1},{e2}")));
    let tgt = load(&person_with(&format!("{e1},{e2_edited}")));
    let deltas = roundtrip(&src, &tgt);
    assert_eq!(
        deltas.len(),
        1,
        "in-place-edit: expected a single delta, got: {deltas:?}"
    );
    assert_eq!(
        deltas[0].op,
        DeltaOp::Update,
        "in-place-edit: the single delta must be an Update, got: {deltas:?}"
    );
    assert_eq!(
        deltas[0].path,
        vec![
            "has_medical_history".to_string(),
            "1".to_string(),
            "duration".to_string()
        ],
        "in-place-edit: Update must address the changed field of the edited row, got: {deltas:?}"
    );

    // Case 4: mid-list insert [E1, E2] -> [E1, E3, E2]. The index-based patcher
    // has no insert-at-index, so this falls back to positional overwrite+append;
    // it must still round-trip exactly (regression guard for the hybrid).
    let e3 = r#"{"started_at_time":"2022-03-03","duration":3.0}"#;
    let src = load(&person_with(&format!("{e1},{e2}")));
    let tgt = load(&person_with(&format!("{e1},{e3},{e2}")));
    roundtrip(&src, &tgt);

    // Case 5: combined delete-first + append [E1, E2] -> [E2, E3]. The added row
    // E3 must read as an Add (not the deleted E1 mis-attributed as an edit), the
    // dropped E1 as a Remove, and the survivor E2 stays quiet. Round-trips
    // because the insert lands at the tail.
    let src = load(&person_with(&format!("{e1},{e2}")));
    let tgt = load(&person_with(&format!("{e2},{e3}")));
    let deltas = roundtrip(&src, &tgt);
    assert_eq!(
        deltas.iter().filter(|d| d.op == DeltaOp::Remove).count(),
        1,
        "combined: exactly one Remove (E1), got: {deltas:?}"
    );
    assert_eq!(
        deltas.iter().filter(|d| d.op == DeltaOp::Add).count(),
        1,
        "combined: exactly one Add (E3), got: {deltas:?}"
    );
    assert!(
        deltas.iter().all(|d| d.op != DeltaOp::Update),
        "combined: no row should be mis-reported as an Update, got: {deltas:?}"
    );

    // Case 6: same-position delete + add of an UNRELATED row [E1, E2] -> [E1, X],
    // where X shares no field values with E2. The LCS keeps E1 and leaves E2/X in
    // the same gap; they must NOT be paired into a field-level Update (that is the
    // reported bug where the added row reads as an edit of the deleted one). With
    // no shared fields they are dissimilar -> Remove E2 + Add X.
    let x = r#"{"started_at_time":"2099-09-09","duration":99.0}"#;
    let src = load(&person_with(&format!("{e1},{e2}")));
    let tgt = load(&person_with(&format!("{e1},{x}")));
    let deltas = roundtrip(&src, &tgt);
    assert!(
        deltas.iter().all(|d| d.op != DeltaOp::Update),
        "dissimilar replace must not be an Update, got: {deltas:?}"
    );
    assert_eq!(
        deltas.iter().filter(|d| d.op == DeltaOp::Remove).count(),
        1,
        "dissimilar replace: one Remove (E2), got: {deltas:?}"
    );
    assert_eq!(
        deltas.iter().filter(|d| d.op == DeltaOp::Add).count(),
        1,
        "dissimilar replace: one Add (X), got: {deltas:?}"
    );
}
