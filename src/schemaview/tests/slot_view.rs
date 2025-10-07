use linkml_schemaview::identifier::{converter_from_schemas, Identifier};
use linkml_schemaview::io::from_yaml;
use linkml_schemaview::schemaview::SchemaView;
use std::collections::HashMap;
use std::path::{Path, PathBuf};

fn data_path(name: &str) -> PathBuf {
    let mut p = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    p.push("tests");
    p.push("data");
    p.push(name);
    p
}

#[test]
fn slot_lookup_and_class_slots() {
    let units_schema = from_yaml(Path::new(&data_path("units.yaml"))).unwrap();
    let mappings_schema = from_yaml(Path::new(&data_path("mappings.yaml"))).unwrap();

    let mut sv = SchemaView::new();
    sv.add_schema(units_schema.clone()).unwrap();
    sv.add_schema(mappings_schema.clone()).unwrap();

    let conv = converter_from_schemas([&units_schema, &mappings_schema]);

    assert!(sv
        .get_slot(&Identifier::new("abbreviation"), &conv)
        .unwrap()
        .is_some());

    let slot_by_uri = sv
        .get_slot_by_uri("http://www.w3.org/2004/02/skos/core#exactMatch")
        .unwrap()
        .expect("slot not found by URI");
    assert_eq!(
        slot_by_uri.canonical_uri().to_string(),
        "http://www.w3.org/2004/02/skos/core#exactMatch"
    );
    assert_eq!(slot_by_uri.schema_id(), "https://w3id.org/linkml/mappings");

    let slot_by_curie = sv
        .get_slot_by_uri("skos:exactMatch")
        .unwrap()
        .expect("slot not found by CURIE");
    assert_eq!(
        slot_by_curie.canonical_uri().to_string(),
        "http://www.w3.org/2004/02/skos/core#exactMatch"
    );

    // class slots with slot_usage
    let class = sv
        .get_class(&Identifier::new("UnitOfMeasure"), &conv)
        .unwrap()
        .unwrap();
    let slots = class.slots();
    let mut map: HashMap<String, usize> = HashMap::new();
    for s in slots {
        map.insert(s.name.clone(), s.definitions().len());
    }
    assert_eq!(map.get("symbol"), Some(&1usize));
    assert_eq!(map.get("exact mappings"), Some(&2usize));
}

#[test]
fn slot_usage_overrides_range_across_schemas() {
    let base_schema = from_yaml(Path::new(&data_path("slot_usage_base.yaml"))).unwrap();
    let specialized_schema =
        from_yaml(Path::new(&data_path("slot_usage_specialized.yaml"))).unwrap();

    let mut sv = SchemaView::new();
    sv.add_schema(base_schema.clone()).unwrap();
    sv.add_schema(specialized_schema.clone()).unwrap();

    let conv = converter_from_schemas([&base_schema, &specialized_schema]);

    let base_class = sv
        .get_class(&Identifier::new("base:BaseThing"), &conv)
        .unwrap()
        .unwrap();
    let base_slot = base_class
        .slots()
        .iter()
        .find(|s| s.name == "shared_slot")
        .expect("shared_slot not found");

    assert_eq!(base_slot.definition().range.as_deref(), Some("string"));

    let class = sv
        .get_class(&Identifier::new("specialized:SpecializedThing"), &conv)
        .unwrap()
        .unwrap();
    let slot = class
        .slots()
        .iter()
        .find(|s| s.name == "shared_slot")
        .expect("shared_slot not found");

    assert_eq!(
        slot.definition().range.as_deref(),
        Some("specialized:TargetClass")
    );
    assert_eq!(
        slot.get_range_class().map(|cv| cv.name().to_string()),
        Some("TargetClass".to_string())
    );
}
