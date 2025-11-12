use clap::Parser;
use linkml_runtime::{load_json_file, load_yaml_file};
use linkml_schemaview::identifier::Identifier;
use linkml_schemaview::io::from_yaml;
#[cfg(feature = "resolve")]
use linkml_schemaview::resolve::resolve_schemas;
use linkml_schemaview::schemaview::SchemaView;
use std::path::PathBuf;

#[derive(Parser)]
struct Args {
    /// LinkML schema YAML file
    schema: PathBuf,
    /// Name of the class to validate against
    class: String,
    /// Data file (YAML or JSON)
    data: PathBuf,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let schema_path = args.schema.to_str().ok_or("Invalid schema path")?;
    let schema = from_yaml(&args.schema)?;
    let mut sv = SchemaView::new();
    sv.add_schema_with_import_ref(
        schema.clone(),
        Some(("".to_owned(), schema_path.to_owned())),
    )
    .map_err(|e| e.to_string())?;
    #[cfg(feature = "resolve")]
    resolve_schemas(&mut sv).map_err(|e| e.to_string())?;
    let conv = sv.converter();
    let class_view = sv
        .get_class(&Identifier::new(&args.class), &conv)
        .map_err(|e| format!("{e:?}"))?
        .ok_or("class not found")?;
    let data_path = &args.data;
    let load_result = if let Some(ext) = data_path.extension() {
        if ext == "json" {
            load_json_file(data_path, &sv, &class_view, &conv)?
        } else {
            load_yaml_file(data_path, &sv, &class_view, &conv)?
        }
    } else {
        load_yaml_file(data_path, &sv, &class_view, &conv)?
    };
    let diagnostics = load_result.diagnostics;
    if diagnostics.is_empty() {
        println!("valid");
        Ok(())
    } else {
        for diag in diagnostics {
            let location = if diag.path.is_empty() {
                "<root>".to_string()
            } else {
                diag.path.join(".")
            };
            println!("{:?} at {}: {}", diag.code, location, diag.message);
        }
        std::process::exit(1);
    }
}
