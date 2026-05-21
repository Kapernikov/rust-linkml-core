#![cfg(feature = "ttl")]

use linkml_runtime::rdf_import::{import_ntriples, ImportOptions};
use linkml_runtime::rdf_import_store::RdfImportStore;
use linkml_runtime::rdf_streaming::import_owned_store_streaming;
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
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

    let stream = import_ntriples(
        reader,
        sv,
        conv,
        &["OperationalPoint"],
        ImportOptions::default(),
    )
    .unwrap();

    let herefoss = stream
        .map(|r| r.unwrap())
        .filter(|(c, _)| c == "OperationalPoint")
        .map(|(_, inst)| inst)
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
    let stream_result = import_ntriples(reader, sv, conv, root_classes, ImportOptions::default());

    match stream_result {
        Ok(stream) => {
            let mut counts: HashMap<String, usize> = HashMap::new();
            let mut total = 0usize;
            for item in stream {
                let (class_name, _inst) = item.expect("harvest yields ok");
                *counts.entry(class_name).or_insert(0) += 1;
                total += 1;
            }
            let elapsed = start.elapsed();
            eprintln!("\n=== ERA Enriched Import Results ===");
            eprintln!("Time: {elapsed:?}");
            for (class_name, n) in &counts {
                eprintln!("  {class_name}: {n} instances");
            }
            eprintln!("  Total instances: {total}");
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

    let store = RdfImportStore::from_ntriples(reader).unwrap();

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

    // Drive the harvest via the low-level owned-store streaming API so
    // we keep access to the store for consumed/unconsumed introspection
    // (the new RdfStream owns its own store and exposes equivalents).
    let stream = import_owned_store_streaming(
        store,
        sv,
        conv,
        root_classes,
        Rc::new(RefCell::new(Vec::new())),
        false,
    )
    .unwrap();
    let mut total = 0usize;
    let mut owned_stream = stream;
    while let Some(item) = owned_stream.next() {
        let _ = item.expect("harvest yields ok");
        total += 1;
    }
    let store_ref = owned_stream.store();

    let consumed = store_ref.consumed_subjects();
    eprintln!("Consumed subjects: {}", consumed.len());
    assert!(
        !consumed.is_empty(),
        "Expected consumed subjects to be non-empty"
    );
    assert!(
        consumed.iter().any(|s| s.contains("matdata.eu")),
        "Expected at least one consumed subject containing 'matdata.eu'"
    );

    let unconsumed = store_ref.unconsumed_subjects();
    eprintln!("Unconsumed subjects: {}", unconsumed.len());
    assert!(
        !unconsumed.is_empty(),
        "Expected unconsumed subjects (Switch, Signal etc. not in schema)"
    );

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

    // Tracking is now always-on in RdfImportStore; the legacy
    // .with_tracking() conversion is no longer needed.
    let store = RdfImportStore::from_ntriples(reader).unwrap();

    let stream = import_owned_store_streaming(
        store,
        sv,
        conv,
        &["OperationalPoint"],
        Rc::new(RefCell::new(Vec::new())),
        false,
    )
    .unwrap();
    let mut owned_stream = stream;
    let mut ops_count = 0usize;
    while let Some(item) = owned_stream.next() {
        let (cls, _inst) = item.expect("harvest yields ok");
        if cls == "OperationalPoint" {
            ops_count += 1;
        }
    }
    assert!(ops_count > 0, "Expected OperationalPoint instances");
    eprintln!("OperationalPoint instances: {ops_count}");

    let consumed = owned_stream.store().consumed_subjects();
    eprintln!("Consumed subjects: {}", consumed.len());
    assert!(
        !consumed.is_empty(),
        "Expected consumed subjects to be non-empty"
    );
}
