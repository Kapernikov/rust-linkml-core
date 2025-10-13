use linkml_schemaview::classview::ClassView;
use linkml_schemaview::io::from_yaml;
use linkml_schemaview::schemaview::{SchemaView, SchemaViewError};
use std::path::{Path, PathBuf};

fn data_path(name: &str) -> PathBuf {
    let mut p = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    p.push("tests");
    p.push("data");
    p.push(name);
    p
}

fn load_view() -> SchemaView {
    let schema = from_yaml(Path::new(&data_path("common_ancestors.yaml"))).unwrap();
    let mut sv = SchemaView::new();
    sv.add_schema(schema).unwrap();
    sv
}

fn class(sv: &SchemaView, name: &str) -> ClassView {
    sv.get_class_by_schema(sv.primary_schema().unwrap().id.as_str(), name)
        .unwrap()
        .unwrap()
}

#[test]
fn finds_common_superclass_without_mixins() {
    let sv = load_view();
    let car = class(&sv, "Car");
    let tractor = class(&sv, "Tractor");

    let result = ClassView::most_specific_common_ancestor(&[car, tractor], false)
        .expect("search succeeds")
        .expect("ancestor exists");

    assert_eq!(result.name(), "GroundVehicle");
}

#[test]
fn considers_mixins_when_requested() {
    let sv = load_view();
    let car = class(&sv, "Car");
    let airplane = class(&sv, "Airplane");

    let without_mixins =
        ClassView::most_specific_common_ancestor(&[car.clone(), airplane.clone()], false)
            .expect("search succeeds")
            .expect("ancestor exists");
    assert_eq!(without_mixins.name(), "Vehicle");

    let with_mixins = ClassView::most_specific_common_ancestor(&[car, airplane], true)
        .expect("search succeeds")
        .expect("ancestor exists");
    assert_eq!(with_mixins.name(), "Maintainable");
}

#[test]
fn rejects_mismatched_schema_views() {
    let sv_a = load_view();
    let sv_b = load_view();

    let car = class(&sv_a, "Car");
    let tractor = class(&sv_b, "Tractor");

    match ClassView::most_specific_common_ancestor(&[car, tractor], false) {
        Err(SchemaViewError::SchemaViewMismatch) => {}
        Err(other) => panic!("unexpected error: {other}"),
        Ok(_) => panic!("expected mismatch error, got successful result"),
    }
}
