#![cfg(feature = "ttl")]

use linkml_runtime::turtle_import::import_ntriples;
use linkml_schemaview::identifier::converter_from_schemas;
use linkml_schemaview::io::from_yaml;
use linkml_schemaview::schemaview::SchemaView;
use std::fs;
use std::io::BufReader;
use std::path::Path;

const SCHEMA_DIR: &str = "/home/kervel/projects/asset360/consolidator-server/components/py/asset360-model/asset360_model/schemas/rinf/repository/v1.0.0";
const NT_FILE: &str = "/tmp/export_4.nt";

#[test]
fn import_rinf_ntriples() {
    if !Path::new(SCHEMA_DIR).exists() || !Path::new(NT_FILE).exists() {
        eprintln!("Skipping rinf_import_test: data files not found");
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
            eprintln!("\n=== RINF Import Results ===");
            eprintln!("Time: {elapsed:?}");
            let mut total = 0;
            for (class_name, instances) in &import_result.instances {
                eprintln!("  {class_name}: {} instances", instances.len());
                total += instances.len();
            }
            eprintln!("  Total instances: {total}");
            eprintln!("  Unconsumed triples: {:?}", import_result.unconsumed_count);
            eprintln!("===========================\n");

            assert!(total > 0, "Expected at least some instances");
        }
        Err(e) => {
            eprintln!("\n=== RINF Import FAILED ===");
            eprintln!("Error: {e}");
            eprintln!("===========================\n");
            panic!("Import failed: {e}");
        }
    }
}
