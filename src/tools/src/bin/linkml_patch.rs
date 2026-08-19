use clap::Parser;
use linkml_runtime::{load_json_file, load_yaml_file, patch, Delta};
use linkml_schemaview::io::from_yaml;
#[cfg(feature = "resolve")]
use linkml_schemaview::resolve::resolve_schemas_from;
use linkml_schemaview::schemaview::{ClassView, SchemaView};
use linkml_schemaview::Converter;
use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};

use linkml_tools::validation_utils::report_validation_issues;

/// Exit code for a patch that applied some, but not all, of its deltas.
///
/// Partial application is designed behaviour, not an error: a delta whose
/// target has drifted away is recorded and skipped so the rest of the batch
/// still lands. It is also not something a script should have to parse stderr
/// to notice, and the old builder-error `Err` at least gave it a non-zero
/// status. A code of its own restores the machine signal without claiming the
/// run failed.
const EXIT_PARTIAL: i32 = 2;

#[derive(Parser)]
#[command(
    name = "linkml-patch",
    about = "Apply a delta file to a LinkML instance document",
    long_about = "Apply a delta file to a LinkML instance document.

Exit codes:
  0  every delta applied
  2  partial application: some deltas could not be applied, and their paths are
     listed on stderr; the patched document is still written
  1  hard failure: bad arguments, unreadable files, schema or parse errors"
)]
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
    // Explicit: the partial-application path leaves via `process::exit`, which
    // runs no destructors.
    writer.flush()?;
    Ok(())
}

/// Report the deltas the patch could not apply.
///
/// `patch` never hard-errors on an unappliable delta: it records the delta's
/// path and applies the rest of the batch. Dropping the trace made that
/// invisible here — a patch that skipped half its deltas wrote a file that
/// looked clean. The lines go to stderr, so the patched document on stdout is
/// byte-identical to before for a fully applied patch; [`EXIT_PARTIAL`] carries
/// the same news to a caller that does not read prose.
fn report_failed_deltas(path: &Path, failed: &[Vec<String>]) {
    if failed.is_empty() {
        return;
    }
    eprintln!(
        "{} of the deltas in '{}' could not be applied; the rest were applied.",
        failed.len(),
        path.display()
    );
    for delta_path in failed {
        eprintln!(
            "  - {}",
            if delta_path.is_empty() {
                "<root>".to_string()
            } else {
                delta_path.join(".")
            }
        );
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let schema = from_yaml(&args.schema)?;
    let mut sv = SchemaView::new();
    sv.add_schema(schema.clone()).map_err(|e| e.to_string())?;
    #[cfg(feature = "resolve")]
    resolve_schemas_from(&mut sv, &args.schema).map_err(|e| e.to_string())?;
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
    let (patched, trace) = patch(
        &src,
        &deltas,
        linkml_runtime::diff::PatchOptions {
            ignore_no_ops: args.ignore_noop,
            treat_missing_as_null: args.treat_missing_as_null,
        },
    )?;
    report_failed_deltas(&args.delta, &trace.failed);
    write_value(args.output.as_deref(), &patched)?;
    if !trace.failed.is_empty() {
        // The document is written first: a partial patch is a result, not a
        // discarded run.
        std::process::exit(EXIT_PARTIAL);
    }
    Ok(())
}
