use clap::Parser;
use linkml_runtime::{load_json_file, load_yaml_file, patch, Delta};
use linkml_schemaview::io::from_yaml;
#[cfg(feature = "resolve")]
use linkml_schemaview::resolve::resolve_schemas;
use linkml_schemaview::schemaview::{ClassView, SchemaView};
use linkml_schemaview::Converter;
use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};

use linkml_tools::validation_utils::report_validation_issues;

#[derive(Parser)]
#[command(name = "linkml-patch")]
struct Args {
    /// LinkML schema YAML file
    schema: PathBuf,
    /// Name of the root class
    #[arg(short, long)]
    class: Option<String>,
    /// Source data file (YAML or JSON)
    source: PathBuf,
    /// Delta file (YAML or JSON)
    delta: PathBuf,
    /// Output patched file; defaults to stdout
    #[arg(short, long)]
    output: Option<PathBuf>,
    /// Treat missing assignments as equivalent to explicit null for equality
    #[arg(long, default_value_t = true)]
    treat_missing_as_null: bool,
    /// Skip deltas that do not change the value (no-ops)
    #[arg(long, default_value_t = true)]
    ignore_noop: bool,
}

fn load_value(
    path: &Path,
    sv: &SchemaView,
    class: &ClassView,
    conv: &Converter,
) -> Result<linkml_runtime::LinkMLInstance, Box<dyn std::error::Error>> {
    let result = if let Some(ext) = path.extension().and_then(|s| s.to_str()) {
        if ext == "json" {
            load_json_file(path, sv, class, conv)
        } else {
            load_yaml_file(path, sv, class, conv)
        }
    } else {
        load_yaml_file(path, sv, class, conv)
    }?;
    report_validation_issues(path, &result.validation_issues);
    result
        .into_instance_tolerate_errors()
        .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)
}

fn write_value(
    path: Option<&Path>,
    value: &linkml_runtime::LinkMLInstance,
) -> Result<(), Box<dyn std::error::Error>> {
    let json = value.to_json();
    let mut writer: Box<dyn Write> = if let Some(p) = path {
        Box::new(File::create(p)?)
    } else {
        Box::new(std::io::stdout())
    };
    if let Some(ext) = path.and_then(|p| p.extension().and_then(|s| s.to_str())) {
        if ext == "json" {
            serde_json::to_writer_pretty(&mut writer, &json)?;
        } else {
            serde_yaml::to_writer(&mut writer, &json)?;
        }
    } else {
        serde_yaml::to_writer(&mut writer, &json)?;
    }
    writer.write_all(b"\n")?;
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let schema = from_yaml(&args.schema)?;
    let mut sv = SchemaView::new();
    sv.add_schema(schema.clone()).map_err(|e| e.to_string())?;
    #[cfg(feature = "resolve")]
    resolve_schemas(&mut sv).map_err(|e| e.to_string())?;
    let conv = sv.converter();
    let class_view = sv.get_tree_root_or(args.class.as_deref()).ok_or_else(|| {
        format!(
            "Class '{}' not found in schema '{}'",
            args.class.as_deref().unwrap_or("root"),
            args.schema.display()
        )
    })?;

    let src = load_value(&args.source, &sv, &class_view, &conv)?;
    let delta_text = std::fs::read_to_string(&args.delta)?;
    let deltas: Vec<Delta> = if let Some(ext) = args.delta.extension().and_then(|s| s.to_str()) {
        if ext == "json" {
            serde_json::from_str(&delta_text)?
        } else {
            serde_yaml::from_str(&delta_text)?
        }
    } else {
        serde_yaml::from_str(&delta_text)?
    };
    let (patched, _trace) = patch(
        &src,
        &deltas,
        linkml_runtime::diff::PatchOptions {
            ignore_no_ops: args.ignore_noop,
            treat_missing_as_null: args.treat_missing_as_null,
        },
    )?;
    write_value(args.output.as_deref(), &patched)?;
    Ok(())
}
