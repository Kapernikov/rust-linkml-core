use linkml_runtime::{load_yaml_file, validate, DiagnosticCode, LinkMLInstance};
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
fn diagnostics_report_regex_pattern_violation() {
    let (sv, conv) = load_personinfo_schema();
    let person = class_by_name(&sv, &conv, "Person");
    let outcome = load_yaml_file(
        Path::new(&info_path("person_bad_email.yaml")),
        &sv,
        &person,
        &conv,
    )
    .unwrap();
    let diags = outcome.diagnostics;
    assert!(diags.iter().any(|d| {
        matches!(d.code, DiagnosticCode::RegexMismatch)
            && d.path.last().map(|p| p == "primary_email").unwrap_or(false)
    }));
}

#[test]
fn diagnostics_report_missing_required_slot() {
    let (sv, conv) = load_personinfo_schema();
    let person = class_by_name(&sv, &conv, "Person");
    let outcome = load_yaml_file(
        Path::new(&info_path("person_missing_familial_type.yaml")),
        &sv,
        &person,
        &conv,
    )
    .unwrap();
    let diags = outcome.diagnostics;
    assert!(diags.iter().any(|d| {
        matches!(d.code, DiagnosticCode::MissingRequiredSlot)
            && d.path.last().map(|p| p == "type").unwrap_or(false)
    }));
}

#[test]
fn diagnostics_report_unknown_slot_and_extras() {
    let (sv, conv) = load_personinfo_schema();
    let person = class_by_name(&sv, &conv, "Person");
    let outcome = load_yaml_file(
        Path::new(&info_path("person_unknown_attr.yaml")),
        &sv,
        &person,
        &conv,
    )
    .unwrap();
    let diags = outcome.diagnostics;
    assert!(diags.iter().any(|d| {
        matches!(d.code, DiagnosticCode::UnknownSlot)
            && d.path.last().map(|p| p == "unknown_attr").unwrap_or(false)
    }));
    if let Some(LinkMLInstance::Object { extras, .. }) = outcome.instance {
        assert!(extras.contains_key("unknown_attr"));
    } else {
        panic!("expected person to deserialize as object");
    }
}
