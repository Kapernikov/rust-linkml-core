use linkml_schemaview::identifier::Identifier;
use linkml_schemaview::schemaview::SchemaView;
use linkml_schemaview::snapshot::SCHEMAVIEW_SNAPSHOT_VERSION;
const PRIMARY_SCHEMA: &str = r#"
id: https://example.org/primary
name: primary
imports:
  - ./secondary.yaml
default_prefix: ex
prefixes:
  ex:
    prefix_reference: https://example.org/
classes:
  Person:
    description: Primary person
"#;

const SECONDARY_SCHEMA: &str = r#"
id: https://example.org/secondary
name: secondary
slots:
  full_name:
    description: Full name slot
"#;

#[test]
fn schemaview_snapshot_roundtrip() {
    let primary = serde_yml::from_str(PRIMARY_SCHEMA).expect("parse primary schema");
    let secondary = serde_yml::from_str(SECONDARY_SCHEMA).expect("parse secondary schema");

    let mut sv = SchemaView::new();
    sv.add_schema(primary).expect("add primary schema");
    sv.add_schema_with_import_ref(
        secondary,
        Some((
            "https://example.org/primary".to_string(),
            "./secondary.yaml".to_string(),
        )),
    )
    .expect("add secondary schema");

    assert!(sv.get_unresolved_schemas().is_empty());

    let snapshot = sv.to_snapshot();
    assert_eq!(snapshot.format_version, SCHEMAVIEW_SNAPSHOT_VERSION);

    let yaml = serde_yml::to_string(&snapshot).expect("serialize snapshot");
    let restored = SchemaView::from_snapshot_yaml(&yaml).expect("restore schema view");

    assert!(restored.get_unresolved_schemas().is_empty());
    let conv = restored.converter();
    let class = restored
        .get_class(&Identifier::new("Person"), &conv)
        .expect("class lookup")
        .expect("class present");
    assert_eq!(class.schema_id(), "https://example.org/primary");
    assert_eq!(class.name(), "Person");

    let resolved = restored._get_resolved_schema_imports();
    assert_eq!(
        resolved.get(&(
            "https://example.org/primary".to_string(),
            "./secondary.yaml".to_string()
        )),
        Some(&"https://example.org/secondary".to_string())
    );
}
