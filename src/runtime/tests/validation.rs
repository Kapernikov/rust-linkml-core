use linkml_runtime::{load_yaml_file, validate, validate_issues, LinkMLInstance, ValidationIssueCode};
use linkml_schemaview::identifier::{converter_from_schema, Identifier};
use linkml_schemaview::io::from_yaml;
use linkml_schemaview::schemaview::{ClassView, SchemaView};
use linkml_schemaview::Converter;
use std::path::{Path, PathBuf};

fn info_path(name: &str) -> PathBuf {
    let mut p = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    p.push("tests");
    p.push("data");
    p.push(name);
    p
}

fn load_personinfo_schema() -> (SchemaView, Converter) {
    let schema = from_yaml(Path::new(&info_path("personinfo.yaml"))).unwrap();
    let mut sv = SchemaView::new();
    sv.add_schema(schema.clone()).unwrap();
    let conv = converter_from_schema(&schema);
    (sv, conv)
}

fn load_cardinality_schema() -> (SchemaView, Converter) {
    let schema = from_yaml(Path::new(&info_path("cardinality_schema.yaml"))).unwrap();
    let mut sv = SchemaView::new();
    sv.add_schema(schema.clone()).unwrap();
    let conv = converter_from_schema(&schema);
    (sv, conv)
}

fn class_by_name(sv: &SchemaView, conv: &Converter, name: &str) -> ClassView {
    sv.get_class(&Identifier::new(name), conv)
        .unwrap()
        .expect("class not found")
}

#[test]
fn validate_personinfo_example1() {
    let (sv, conv) = load_personinfo_schema();
    let container = class_by_name(&sv, &conv, "Container");
    let v = load_yaml_file(
        Path::new(&info_path("example_personinfo_data.yaml")),
        &sv,
        &container,
        &conv,
    )
    .unwrap()
    .into_instance()
    .unwrap();
    assert!(validate(&v).is_ok());
}

#[test]
fn validate_personinfo_example2() {
    let (sv, conv) = load_personinfo_schema();
    let container = class_by_name(&sv, &conv, "Container");
    let v = load_yaml_file(
        Path::new(&info_path("example_personinfo_data_2.yaml")),
        &sv,
        &container,
        &conv,
    )
    .unwrap()
    .into_instance()
    .unwrap();
    assert!(validate(&v).is_ok());
}

#[test]
fn validate_personinfo_null_collections() {
    let (sv, conv) = load_personinfo_schema();
    let container = class_by_name(&sv, &conv, "Container");
    let v = load_yaml_file(
        Path::new(&info_path("example_personinfo_data_nulls.yaml")),
        &sv,
        &container,
        &conv,
    )
    .unwrap()
    .into_instance()
    .unwrap();
    assert!(validate(&v).is_ok());
    // Assert that nulls are preserved as LinkMLInstance::Null (not empty collections)
    if let linkml_runtime::LinkMLInstance::Object { values, .. } = &v {
        if let Some(linkml_runtime::LinkMLInstance::List { values: objs, .. }) =
            values.get("objects")
        {
            if let Some(linkml_runtime::LinkMLInstance::Object { values: person, .. }) =
                objs.first()
            {
                assert!(matches!(
                    person.get("aliases"),
                    Some(linkml_runtime::LinkMLInstance::Null { .. })
                ));
                assert!(matches!(
                    person.get("has_employment_history"),
                    Some(linkml_runtime::LinkMLInstance::Null { .. })
                ));
                assert!(matches!(
                    person.get("has_familial_relationships"),
                    Some(linkml_runtime::LinkMLInstance::Null { .. })
                ));
            } else {
                panic!("expected first object to be an Object");
            }
        } else {
            panic!("expected Container.objects to be a List");
        }
    } else {
        panic!("expected root to be an Object");
    }
}

#[test]
fn validation_issues_report_regex_pattern_violation() {
    let (sv, conv) = load_personinfo_schema();
    let person = class_by_name(&sv, &conv, "Person");
    let outcome = load_yaml_file(
        Path::new(&info_path("person_bad_email.yaml")),
        &sv,
        &person,
        &conv,
    )
    .unwrap();
    let diags = outcome.validation_issues;
    assert!(diags.iter().any(|d| {
        matches!(d.code, ValidationIssueCode::RegexMismatch)
            && d.path.last().map(|p| p == "primary_email").unwrap_or(false)
    }));
}

#[test]
fn validation_issue_paths_include_list_indices_once() {
    let (sv, conv) = load_personinfo_schema();
    let container = class_by_name(&sv, &conv, "Container");
    let load = load_yaml_file(
        Path::new(&info_path("container_person_bad_email.yaml")),
        &sv,
        &container,
        &conv,
    )
    .unwrap();
    let instance = load
        .instance
        .expect("instance should be produced even with validation errors");
    let diags = validate_issues(&instance);
    let expected_path = vec![
        "objects".to_string(),
        "1".to_string(),
        "primary_email".to_string(),
    ];
    let diag_paths: Vec<_> = diags
        .iter()
        .filter(|d| matches!(d.code, ValidationIssueCode::UnknownSlot))
        .map(|d| d.path.clone())
        .collect();
    assert!(
        diag_paths.iter().any(|p| p == &expected_path),
        "diagnostics: {:?}",
        diags
    );
}

#[test]
fn validation_issues_report_missing_required_slot() {
    let (sv, conv) = load_personinfo_schema();
    let person = class_by_name(&sv, &conv, "Person");
    let outcome = load_yaml_file(
        Path::new(&info_path("person_missing_familial_type.yaml")),
        &sv,
        &person,
        &conv,
    )
    .unwrap();
    let diags = outcome.validation_issues;
    assert!(diags.iter().any(|d| {
        matches!(d.code, ValidationIssueCode::MissingRequiredSlot)
            && d.path.last().map(|p| p == "type").unwrap_or(false)
    }));
}

#[test]
fn validation_issues_report_unknown_slot_and_fields() {
    let (sv, conv) = load_personinfo_schema();
    let person = class_by_name(&sv, &conv, "Person");
    let outcome = load_yaml_file(
        Path::new(&info_path("person_unknown_attr.yaml")),
        &sv,
        &person,
        &conv,
    )
    .unwrap();
    let diags = outcome.validation_issues;
    assert!(diags.iter().any(|d| {
        matches!(d.code, ValidationIssueCode::UnknownSlot)
            && d.path.last().map(|p| p == "unknown_attr").unwrap_or(false)
    }));
    if let Some(LinkMLInstance::Object { unknown_fields, .. }) = outcome.instance {
        assert!(unknown_fields.contains_key("unknown_attr"));
    } else {
        panic!("expected person to deserialize as object");
    }
}

#[test]
fn minimum_value_violation_reported() {
    let (sv, conv) = load_personinfo_schema();
    let person = class_by_name(&sv, &conv, "Person");
    let outcome = load_yaml_file(
        Path::new(&info_path("person_age_negative.yaml")),
        &sv,
        &person,
        &conv,
    )
    .unwrap();
    assert!(outcome.validation_issues.iter().any(|d| {
        matches!(d.code, ValidationIssueCode::MinimumValueViolation)
            && d.path.last().map(|p| p == "age_in_years").unwrap_or(false)
    }));
}

#[test]
fn maximum_value_violation_reported() {
    let (sv, conv) = load_personinfo_schema();
    let person = class_by_name(&sv, &conv, "Person");
    let outcome = load_yaml_file(
        Path::new(&info_path("person_age_too_big.yaml")),
        &sv,
        &person,
        &conv,
    )
    .unwrap();
    assert!(outcome.validation_issues.iter().any(|d| {
        matches!(d.code, ValidationIssueCode::MaximumValueViolation)
            && d.path.last().map(|p| p == "age_in_years").unwrap_or(false)
    }));
}

#[test]
fn cardinality_valid_data_passes() {
    let (sv, conv) = load_cardinality_schema();
    let bag = class_by_name(&sv, &conv, "Bag");
    let value = load_yaml_file(
        Path::new(&info_path("cardinality_valid.yaml")),
        &sv,
        &bag,
        &conv,
    )
    .unwrap()
    .into_instance()
    .unwrap();
    assert!(validate(&value).is_ok());
}

#[test]
fn min_cardinality_violation_reported() {
    let (sv, conv) = load_cardinality_schema();
    let bag = class_by_name(&sv, &conv, "Bag");
    let outcome = load_yaml_file(
        Path::new(&info_path("cardinality_too_few.yaml")),
        &sv,
        &bag,
        &conv,
    )
    .unwrap();
    assert!(outcome.validation_issues.iter().any(|d| {
        matches!(d.code, ValidationIssueCode::MinCardinalityViolation)
            && d.path.last().map(|p| p == "names").unwrap_or(false)
    }));
}

#[test]
fn max_cardinality_violation_reported() {
    let (sv, conv) = load_cardinality_schema();
    let bag = class_by_name(&sv, &conv, "Bag");
    let outcome = load_yaml_file(
        Path::new(&info_path("cardinality_too_many.yaml")),
        &sv,
        &bag,
        &conv,
    )
    .unwrap();
    assert!(outcome.validation_issues.iter().any(|d| {
        matches!(d.code, ValidationIssueCode::MaxCardinalityViolation)
            && d.path.last().map(|p| p == "tags").unwrap_or(false)
    }));
}

#[test]
fn exact_cardinality_violation_when_missing() {
    let (sv, conv) = load_cardinality_schema();
    let bag = class_by_name(&sv, &conv, "Bag");
    let outcome = load_yaml_file(
        Path::new(&info_path("cardinality_exact_missing.yaml")),
        &sv,
        &bag,
        &conv,
    )
    .unwrap();
    assert!(outcome.validation_issues.iter().any(|d| {
        matches!(d.code, ValidationIssueCode::ExactCardinalityViolation)
            && d.path.last().map(|p| p == "ids").unwrap_or(false)
    }));
}

#[test]
fn exact_cardinality_violation_when_multiple() {
    let (sv, conv) = load_cardinality_schema();
    let bag = class_by_name(&sv, &conv, "Bag");
    let outcome = load_yaml_file(
        Path::new(&info_path("cardinality_exact_too_many.yaml")),
        &sv,
        &bag,
        &conv,
    )
    .unwrap();
    assert!(outcome.validation_issues.iter().any(|d| {
        matches!(d.code, ValidationIssueCode::ExactCardinalityViolation)
            && d.path.last().map(|p| p == "ids").unwrap_or(false)
    }));
}
