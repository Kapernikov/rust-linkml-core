#![cfg(feature = "ttl")]

use linkml_runtime::rdf_import_store::{RdfImportStore, TrackingRdfImportStore};
use linkml_runtime::turtle_import::import_ntriples;
use linkml_schemaview::identifier::converter_from_schemas;
use linkml_schemaview::io::from_yaml;
use linkml_schemaview::schemaview::SchemaView;
use std::fs;
use std::io::BufReader;
use std::path::Path;

const SCHEMA_DIR: &str = "/home/kervel/projects/asset360/consolidator-server/components/py/asset360-model/asset360_model/schemas/rinf/repository/v1.0.0";
const NT_FILE: &str = "/tmp/export_4.nt";
const ERA_NT_FILE: &str = "/tmp/era-graph-enriched.nt";

#[test]
fn herefoss_json() {
    if !Path::new(SCHEMA_DIR).exists() || !Path::new(NT_FILE).exists() {
        eprintln!("Skipping: data files not found");
        return;
    }

    let schema = from_yaml(Path::new(&format!("{SCHEMA_DIR}/rinf_subset.yaml"))).unwrap();
    let types_schema = from_yaml(Path::new(&format!("{SCHEMA_DIR}/types.yaml"))).unwrap();
    let geosparql_schema = from_yaml(Path::new(&format!("{SCHEMA_DIR}/geosparql.yaml"))).unwrap();
    let enums_schema =
        from_yaml(Path::new(&format!("{SCHEMA_DIR}/rinf_subset_enums.yaml"))).unwrap();

    let mut sv = SchemaView::new();
    sv.add_schema(schema.clone()).unwrap();
    sv.add_schema_with_import_ref(
        types_schema.clone(),
        Some((schema.id.clone(), "linkml:types".to_string())),
    )
    .unwrap();
    sv.add_schema_with_import_ref(
        geosparql_schema.clone(),
        Some((schema.id.clone(), "geosparql".to_string())),
    )
    .unwrap();
    sv.add_schema_with_import_ref(
        enums_schema.clone(),
        Some((schema.id.clone(), "rinf_subset_enums".to_string())),
    )
    .unwrap();

    let conv = converter_from_schemas([&schema, &types_schema, &geosparql_schema, &enums_schema]);

    let file = fs::File::open(NT_FILE).unwrap();
    let reader = BufReader::new(file);

    let result = import_ntriples(reader, &sv, &conv, &["OperationalPoint"]).unwrap();

    let ops = result.instances.get("OperationalPoint").unwrap();
    let herefoss = ops
        .iter()
        .find(|inst| {
            let json = inst.to_json();
            json.get("opName")
                .and_then(|v| v.as_str())
                .is_some_and(|s| s.contains("Herefoss"))
        })
        .expect("Herefoss not found");

    let json = herefoss.to_json();
    let pretty = serde_json::to_string_pretty(&json).unwrap();
    eprintln!("\n{pretty}\n");
}

fn load_rinf_sv_and_conv() -> Option<(SchemaView, linkml_schemaview::Converter)> {
    if !Path::new(SCHEMA_DIR).exists() {
        return None;
    }
    let schema = from_yaml(Path::new(&format!("{SCHEMA_DIR}/rinf_subset.yaml"))).unwrap();
    let types_schema = from_yaml(Path::new(&format!("{SCHEMA_DIR}/types.yaml"))).unwrap();
    let geosparql_schema = from_yaml(Path::new(&format!("{SCHEMA_DIR}/geosparql.yaml"))).unwrap();
    let enums_schema =
        from_yaml(Path::new(&format!("{SCHEMA_DIR}/rinf_subset_enums.yaml"))).unwrap();

    let mut sv = SchemaView::new();
    sv.add_schema(schema.clone()).unwrap();
    sv.add_schema_with_import_ref(
        types_schema.clone(),
        Some((schema.id.clone(), "linkml:types".to_string())),
    )
    .unwrap();
    sv.add_schema_with_import_ref(
        geosparql_schema.clone(),
        Some((schema.id.clone(), "geosparql".to_string())),
    )
    .unwrap();
    sv.add_schema_with_import_ref(
        enums_schema.clone(),
        Some((schema.id.clone(), "rinf_subset_enums".to_string())),
    )
    .unwrap();

    let conv = converter_from_schemas([&schema, &types_schema, &geosparql_schema, &enums_schema]);
    Some((sv, conv))
}

#[test]
fn import_era_enriched_ntriples() {
    let Some((sv, conv)) = load_rinf_sv_and_conv() else {
        eprintln!("Skipping: schema not found");
        return;
    };
    if !Path::new(ERA_NT_FILE).exists() {
        eprintln!("Skipping: {ERA_NT_FILE} not found");
        return;
    }

    let file = fs::File::open(ERA_NT_FILE).unwrap();
    let reader = BufReader::new(file);

    // All classes not inlined into another class
    let root_classes = &[
        "OperationalPoint",
        "SectionOfLine",
        "RunningTrack",
        "Siding",
        "Tunnel",
        "ContactLineSystem",
        "TrainDetectionSystem",
        "PlatformEdge",
        "OrganisationRole",
        "LinearPositioningSystem",
        "ETCS",
    ];

    let start = std::time::Instant::now();
    let result = import_ntriples(reader, &sv, &conv, root_classes);
    let elapsed = start.elapsed();

    match result {
        Ok(import_result) => {
            eprintln!("\n=== ERA Enriched Import Results ===");
            eprintln!("Time: {elapsed:?}");
            let mut total = 0;
            for (class_name, instances) in &import_result.instances {
                eprintln!("  {class_name}: {} instances", instances.len());
                total += instances.len();
            }
            eprintln!("  Total instances: {total}");
            eprintln!("  Unconsumed triples: {:?}", import_result.unconsumed_count);
            eprintln!("===================================\n");

            assert!(total > 0, "Expected at least some instances");
        }
        Err(e) => {
            eprintln!("\n=== ERA Enriched Import FAILED ===");
            eprintln!("Error: {e}");
            eprintln!("===================================\n");
            panic!("Import failed: {e}");
        }
    }
}

#[test]
fn import_era_with_tracking() {
    let Some((sv, conv)) = load_rinf_sv_and_conv() else {
        eprintln!("Skipping: schema not found");
        return;
    };
    if !Path::new(ERA_NT_FILE).exists() {
        eprintln!("Skipping: {ERA_NT_FILE} not found");
        return;
    }

    let file = fs::File::open(ERA_NT_FILE).unwrap();
    let reader = BufReader::new(file);

    let store = TrackingRdfImportStore::from_ntriples(reader).unwrap();

    let root_classes = &[
        "OperationalPoint",
        "SectionOfLine",
        "RunningTrack",
        "Siding",
        "Tunnel",
        "ContactLineSystem",
        "TrainDetectionSystem",
        "PlatformEdge",
        "OrganisationRole",
        "LinearPositioningSystem",
        "ETCS",
    ];

    let result = store.import(&sv, &conv, root_classes).unwrap();

    let consumed = store.consumed_subjects();
    eprintln!("Consumed subjects: {}", consumed.len());
    assert!(
        !consumed.is_empty(),
        "Expected consumed subjects to be non-empty"
    );
    assert!(
        consumed.iter().any(|s| s.contains("matdata.eu")),
        "Expected at least one consumed subject containing 'matdata.eu'"
    );

    let unconsumed = store.unconsumed_subjects();
    eprintln!("Unconsumed subjects: {}", unconsumed.len());
    assert!(
        !unconsumed.is_empty(),
        "Expected unconsumed subjects (Switch, Signal etc. not in schema)"
    );

    let total: usize = result.instances.values().map(|v| v.len()).sum();
    eprintln!("Total instances: {total}");
}

#[test]
fn import_era_with_tracking_via_conversion() {
    let Some((sv, conv)) = load_rinf_sv_and_conv() else {
        eprintln!("Skipping: schema not found");
        return;
    };
    if !Path::new(ERA_NT_FILE).exists() {
        eprintln!("Skipping: {ERA_NT_FILE} not found");
        return;
    }

    let file = fs::File::open(ERA_NT_FILE).unwrap();
    let reader = BufReader::new(file);

    let plain_store = RdfImportStore::from_ntriples(reader).unwrap();
    let store = plain_store.with_tracking();

    let result = store.import(&sv, &conv, &["OperationalPoint"]).unwrap();

    let ops = result.instances.get("OperationalPoint");
    assert!(
        ops.is_some_and(|v| !v.is_empty()),
        "Expected OperationalPoint instances"
    );
    eprintln!("OperationalPoint instances: {}", ops.unwrap().len());

    let consumed = store.consumed_subjects();
    eprintln!("Consumed subjects: {}", consumed.len());
    assert!(
        !consumed.is_empty(),
        "Expected consumed subjects to be non-empty"
    );
}
