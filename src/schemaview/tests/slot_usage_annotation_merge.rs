use linkml_schemaview::identifier::Identifier;
use linkml_schemaview::io::from_yaml;
use linkml_schemaview::schemaview::SchemaView;
use linkml_schemaview::slotview::SlotView;
use std::collections::HashMap;
use std::path::{Path, PathBuf};

fn data_path(name: &str) -> PathBuf {
    let mut p = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    p.push("tests");
    p.push("data");
    p.push(name);
    p
}

fn fixture_schema_view() -> SchemaView {
    let schema = from_yaml(Path::new(&data_path("slot_usage_annotation_merge.yaml"))).unwrap();
    let mut sv = SchemaView::new();
    sv.add_schema(schema).unwrap();
    sv
}

/// Resolve the effective `slot` of `class` through the new `ClassView::slot`
/// API. Single-schema fixture, so the primary converter is unambiguous.
fn merged_slot(sv: &SchemaView, class: &str, slot: &str) -> Option<SlotView> {
    let conv = sv.converter();
    let cv = sv.get_class(&Identifier::new(class), &conv).unwrap()?;
    cv.slot(&Identifier::new(slot))
}

fn annotation_keys(sv: &SchemaView, class: &str) -> Vec<String> {
    let slot =
        merged_slot(sv, class, "hasName").unwrap_or_else(|| panic!("hasName not found on {class}"));
    let mut keys: Vec<String> = slot
        .definition()
        .annotations
        .as_ref()
        .map(|m| m.keys().cloned().collect())
        .unwrap_or_default();
    keys.sort();
    keys
}

fn annotation_value(sv: &SchemaView, class: &str, key: &str) -> Option<String> {
    let slot =
        merged_slot(sv, class, "hasName").unwrap_or_else(|| panic!("hasName not found on {class}"));
    let ann = slot.definition().annotations.clone()?;
    let entry = ann.get(key)?;
    if let serde_value::Value::String(s) = &entry.extension_value.0 {
        return Some(s.clone());
    }
    None
}

#[test]
fn child_slot_usage_merges_annotations_with_parent() {
    let sv = fixture_schema_view();

    assert_eq!(
        annotation_keys(&sv, "Parent"),
        vec!["ex:label_en", "ex:label_fr", "ex:label_nl"],
    );

    let child_keys = annotation_keys(&sv, "Child");
    assert_eq!(
        child_keys,
        vec!["ex:label_en", "ex:label_fr", "ex:label_nl", "ex:rbac_scope",],
        "child must see parent's three label keys plus its own added rbac key",
    );

    assert_eq!(
        annotation_value(&sv, "Child", "ex:label_en").as_deref(),
        Some("Name"),
    );
    assert_eq!(
        annotation_value(&sv, "Child", "ex:label_fr").as_deref(),
        Some("Nom"),
    );
    assert_eq!(
        annotation_value(&sv, "Child", "ex:rbac_scope").as_deref(),
        Some("GI"),
    );
}

#[test]
fn grandchild_overrides_parent_key_and_keeps_siblings() {
    let sv = fixture_schema_view();

    let keys = annotation_keys(&sv, "Grandchild");
    assert_eq!(
        keys,
        vec![
            "ex:audit_only",
            "ex:label_en",
            "ex:label_fr",
            "ex:label_nl",
            "ex:rbac_scope",
        ],
        "deepest wins on conflict; sibling keys from parents survive",
    );

    assert_eq!(
        annotation_value(&sv, "Grandchild", "ex:label_fr").as_deref(),
        Some("Nom (FR-BE)"),
        "grandchild's override wins on the conflicting key",
    );
    assert_eq!(
        annotation_value(&sv, "Grandchild", "ex:label_en").as_deref(),
        Some("Name"),
        "non-conflicting parent keys are preserved",
    );
    assert_eq!(
        annotation_value(&sv, "Grandchild", "ex:label_nl").as_deref(),
        Some("Naam"),
    );
    assert_eq!(
        annotation_value(&sv, "Grandchild", "ex:rbac_scope").as_deref(),
        Some("GI"),
        "intermediate (Child) keys survive",
    );
}

#[test]
fn sibling_attribute_redeclaration_fully_replaces() {
    let sv = fixture_schema_view();

    let keys = annotation_keys(&sv, "SiblingAttribute");
    assert_eq!(
        keys,
        vec!["ex:rbac_scope"],
        "redeclaring via `attributes` (not `slot_usage`) drops parent annotations",
    );
}

#[test]
fn extensions_merge_per_key_through_chain() {
    let sv = fixture_schema_view();

    fn extension_keys(sv: &SchemaView, class: &str) -> Vec<String> {
        let slot = merged_slot(sv, class, "hasName").unwrap();
        let mut keys: Vec<String> = slot
            .definition()
            .extensions
            .as_ref()
            .map(|m| m.keys().cloned().collect())
            .unwrap_or_default();
        keys.sort();
        keys
    }

    assert_eq!(extension_keys(&sv, "Parent"), vec!["ex:ext_a", "ex:ext_b"],);
    assert_eq!(
        extension_keys(&sv, "Child"),
        vec!["ex:ext_a", "ex:ext_b", "ex:ext_c"],
    );
    assert_eq!(
        extension_keys(&sv, "Grandchild"),
        vec!["ex:ext_a", "ex:ext_b", "ex:ext_c", "ex:ext_d"],
    );
}

#[test]
fn local_names_merge_per_key_through_chain() {
    let sv = fixture_schema_view();

    fn local_name_map(sv: &SchemaView, class: &str) -> HashMap<String, String> {
        let slot = merged_slot(sv, class, "hasName").unwrap();
        slot.definition()
            .local_names
            .as_ref()
            .map(|m| {
                m.iter()
                    .map(|(k, v)| (k.clone(), v.local_name_value.clone()))
                    .collect()
            })
            .unwrap_or_default()
    }

    let parent = local_name_map(&sv, "Parent");
    assert_eq!(parent.get("en").map(String::as_str), Some("Name"));
    assert_eq!(parent.get("fr").map(String::as_str), Some("Nom"));

    let child = local_name_map(&sv, "Child");
    assert_eq!(child.get("en").map(String::as_str), Some("Name"));
    assert_eq!(child.get("fr").map(String::as_str), Some("Nom"));
    assert_eq!(child.get("nl").map(String::as_str), Some("Naam"));

    let grand = local_name_map(&sv, "Grandchild");
    assert_eq!(grand.get("en").map(String::as_str), Some("Name"));
    assert_eq!(
        grand.get("fr").map(String::as_str),
        Some("Nom (FR-BE)"),
        "grandchild overrides fr",
    );
    assert_eq!(grand.get("nl").map(String::as_str), Some("Naam"));
}

#[test]
fn slot_returns_none_for_unknown_class_or_slot() {
    let sv = fixture_schema_view();
    assert!(merged_slot(&sv, "NoSuchClass", "hasName").is_none());
    assert!(merged_slot(&sv, "Parent", "noSuchSlot").is_none());
}

#[test]
fn slot_matches_classview_slots_lookup() {
    let sv = fixture_schema_view();
    let conv = sv.converter();
    let cv = sv
        .get_class(&Identifier::new("Grandchild"), &conv)
        .unwrap()
        .unwrap();
    let from_class_slots = cv
        .slots()
        .iter()
        .find(|s| s.name == "hasName")
        .cloned()
        .unwrap();
    let from_lookup = cv.slot(&Identifier::new("hasName")).unwrap();
    assert_eq!(
        from_class_slots.definition().annotations,
        from_lookup.definition().annotations,
    );
}

#[test]
fn slot_lookup_by_uri_matches_by_name() {
    let sv = fixture_schema_view();
    let conv = sv.converter();
    let cv = sv
        .get_class(&Identifier::new("Grandchild"), &conv)
        .unwrap()
        .unwrap();
    let by_name = cv.slot(&Identifier::new("hasName")).unwrap();
    // Resolving by the slot's own canonical URI must find the same slot.
    let uri = by_name.canonical_uri();
    let by_uri = cv
        .slot(&uri)
        .expect("lookup by canonical URI should resolve");
    assert_eq!(by_name.name, by_uri.name);
    assert_eq!(
        by_name.definition().annotations,
        by_uri.definition().annotations,
    );
}
