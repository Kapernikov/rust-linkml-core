#[cfg(feature = "ttl")]
use clap::Parser;
#[cfg(feature = "ttl")]
use linkml_runtime::{
    load_json_file, load_yaml_file,
    turtle::{write_turtle, TurtleOptions},
    validate,
};
#[cfg(feature = "ttl")]
use linkml_schemaview::io::from_yaml;
#[cfg(all(feature = "ttl", feature = "resolve"))]
use linkml_schemaview::resolve::resolve_schemas;
#[cfg(feature = "ttl")]
use linkml_schemaview::schemaview::SchemaView;
#[cfg(feature = "ttl")]
use std::fs::File;
#[cfg(feature = "ttl")]
use std::path::PathBuf;

#[cfg(feature = "ttl")]
#[derive(Parser)]
struct Args {
    /// LinkML schema YAML file
    schema: PathBuf,
    /// Data file (YAML or JSON)
    data: PathBuf,
    /// Name of the class to use as the root object
    #[arg(short, long)]
    class: Option<String>,
    /// Output TTL file; defaults to stdout
    #[arg(short, long)]
    output: Option<PathBuf>,
    /// Use skolem IRIs instead of blank nodes
    #[arg(long)]
    skolem: bool,
}

#[cfg(not(feature = "ttl"))]
fn main() {
    eprintln!(
        "linkml-convert was built without Turtle support. Enable the `ttl` feature to use this binary.",
    );
    std::process::exit(1);
}

#[cfg(feature = "ttl")]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let schema = from_yaml(&args.schema)?;
    let mut sv = SchemaView::new();
    sv.add_schema_with_import_ref(
        schema.clone(),
        Some((
            "".to_owned(),
            args.schema.to_str().unwrap_or("unknown").to_owned(),
        )),
    )
    .map_err(|e| e.to_string())?;
    #[cfg(feature = "resolve")]
    {
        eprintln!("Resolving schemas...");
        resolve_schemas(&mut sv).map_err(|e| e.to_string())?;
        eprintln!("Schemas resolved");
    }
    let conv = sv.converter();
    let class_view = sv.get_tree_root_or(args.class.as_deref()).ok_or_else(|| {
        format!(
            "Class '{}' not found in schema '{}'",
            args.class.as_deref().unwrap_or("root"),
            args.schema.display()
        )
    })?;
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
    let value = load_result
        .into_instance()
        .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)?;
    if let Err(e) = validate(&value) {
        eprintln!("invalid: {e}");
        std::process::exit(1);
    }
    let mut writer: Box<dyn std::io::Write> = if let Some(out) = &args.output {
        Box::new(File::create(out)?)
    } else {
        Box::new(std::io::stdout())
    };
    write_turtle(
        &value,
        &sv,
        &schema,
        &conv,
        &mut writer,
        TurtleOptions {
            skolem: args.skolem,
        },
    )?;
    Ok(())
}
