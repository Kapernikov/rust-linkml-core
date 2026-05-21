#[cfg(feature = "ttl")]
use clap::{Parser, ValueEnum};
#[cfg(feature = "ttl")]
use linkml_runtime::{
    load_json_file, load_yaml_file,
    rdf_export::{export_ntriples_many, export_turtle_many, ExportOptions},
    rdf_import::{import_ntriples, import_turtle, ImportOptions},
    validate, LinkMLInstance,
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
use std::io::BufReader;
#[cfg(feature = "ttl")]
use std::path::PathBuf;

#[cfg(feature = "ttl")]
#[derive(Clone, ValueEnum)]
enum Format {
    Yaml,
    Json,
    Turtle,
    Ntriples,
}

#[cfg(feature = "ttl")]
#[derive(Parser)]
#[command(about = "Convert between LinkML data formats (YAML, JSON, Turtle, N-Triples)")]
struct Args {
    /// LinkML schema YAML file
    schema: PathBuf,
    /// Input data file
    data: PathBuf,
    /// Input format (auto-detected from extension if omitted)
    #[arg(short = 'f', long)]
    from: Option<Format>,
    /// Output format (auto-detected from extension if omitted, defaults to turtle)
    #[arg(short = 't', long)]
    to: Option<Format>,
    /// Root class name(s). Required for RDF input; optional for YAML/JSON if schema has tree_root.
    /// Can be specified multiple times for RDF import with multiple types.
    #[arg(short, long)]
    class: Vec<String>,
    /// Output file; defaults to stdout
    #[arg(short, long)]
    output: Option<PathBuf>,
    /// Use skolem IRIs instead of blank nodes (Turtle output only)
    #[arg(long)]
    skolem: bool,
    /// Treat harvest warnings as fatal errors (RDF input only).
    #[arg(long)]
    strict: bool,
    /// Path to a disk-backed RDF graph store (fjall). Only used for RDF
    /// inputs; without this flag, the in-memory store is used. Requires
    /// the `disk_graph` build feature.
    #[cfg(feature = "disk_graph")]
    #[arg(long = "disk-graph")]
    disk_graph: Option<PathBuf>,
}

#[cfg(not(feature = "ttl"))]
fn main() {
    eprintln!(
        "linkml-convert was built without Turtle support. Enable the `ttl` feature to use this binary.",
    );
    std::process::exit(1);
}

#[cfg(feature = "ttl")]
fn detect_format(path: &std::path::Path) -> Option<Format> {
    path.extension().and_then(|ext| match ext.to_str()? {
        "yaml" | "yml" => Some(Format::Yaml),
        "json" => Some(Format::Json),
        "ttl" | "turtle" => Some(Format::Turtle),
        "nt" | "ntriples" => Some(Format::Ntriples),
        _ => None,
    })
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

    let in_fmt = args
        .from
        .or_else(|| detect_format(&args.data))
        .ok_or("Cannot detect input format. Use --from to specify.")?;

    let out_fmt = args
        .to
        .or_else(|| args.output.as_deref().and_then(detect_format))
        .unwrap_or(Format::Turtle);

    // ── Validate format combinations ──

    let is_rdf_in = matches!(in_fmt, Format::Turtle | Format::Ntriples);
    let is_rdf_out = matches!(out_fmt, Format::Turtle | Format::Ntriples);

    if is_rdf_in && is_rdf_out {
        return Err("RDF-to-RDF conversion is not supported. Use --to yaml or --to json.".into());
    }
    if is_rdf_in && args.class.is_empty() {
        return Err("--class is required when importing from RDF (Turtle/N-Triples).".into());
    }
    if !is_rdf_in && !args.class.is_empty() {
        return Err(
            "--class is not used for YAML/JSON input (root class is inferred from schema).".into(),
        );
    }

    // ── Writer ──

    let mut writer: Box<dyn std::io::Write> = if let Some(out) = &args.output {
        Box::new(File::create(out)?)
    } else {
        Box::new(std::io::stdout())
    };

    // ── Build the input iterator + warnings handle ──

    type InstanceItem = Result<LinkMLInstance, Box<dyn std::error::Error>>;
    let (instances, warnings_handle): (
        Box<dyn Iterator<Item = InstanceItem>>,
        Option<std::rc::Rc<std::cell::RefCell<Vec<linkml_runtime::ValidationResult>>>>,
    ) = match in_fmt {
        Format::Yaml | Format::Json => {
            let class_view = sv.get_tree_root_or(None).ok_or_else(|| {
                format!(
                    "No tree_root class found in schema '{}'",
                    args.schema.display()
                )
            })?;
            let load_result = match in_fmt {
                Format::Json => load_json_file(&args.data, &sv, &class_view, &conv)?,
                _ => load_yaml_file(&args.data, &sv, &class_view, &conv)?,
            };
            let value = load_result
                .into_instance()
                .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)?;
            if let Err(e) = validate(&value) {
                eprintln!("invalid: {e}");
                std::process::exit(1);
            }
            (Box::new(std::iter::once(Ok(value))), None)
        }
        Format::Turtle | Format::Ntriples => {
            let class_refs: Vec<String> = args.class.clone();
            let class_refs_borrow: Vec<&str> = class_refs.iter().map(|s| s.as_str()).collect();

            let options = ImportOptions {
                #[cfg(feature = "disk_graph")]
                disk_path: args.disk_graph.clone(),
                #[cfg(not(feature = "disk_graph"))]
                disk_path: None,
                strict: args.strict,
            };

            let file = File::open(&args.data)?;
            let reader = BufReader::new(file);
            let stream = match in_fmt {
                Format::Ntriples => import_ntriples(
                    reader,
                    sv.clone(),
                    conv.clone(),
                    &class_refs_borrow,
                    options,
                ),
                _ => import_turtle(
                    reader,
                    sv.clone(),
                    conv.clone(),
                    &class_refs_borrow,
                    options,
                ),
            }
            .map_err(|e| e.to_string())?;

            let handle = stream.warnings_handle();
            (
                Box::new(stream.map(|res| {
                    res.map(|(_class, inst)| inst)
                        .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)
                })),
                Some(handle),
            )
        }
    };

    // ── Output: drive from the iterator, write one instance at a time ──

    let mut count: usize = 0;
    match out_fmt {
        Format::Json => {
            writeln!(writer, "[")?;
            let mut first = true;
            for item in instances {
                let value = item?;
                if !first {
                    writeln!(writer, ",")?;
                }
                first = false;
                serde_json::to_writer_pretty(&mut writer, &value.to_json())?;
                count += 1;
            }
            writeln!(writer)?;
            writeln!(writer, "]")?;
        }
        Format::Yaml => {
            for item in instances {
                let value = item?;
                writeln!(writer, "---")?;
                serde_yaml::to_writer(&mut writer, &value.to_json())?;
                count += 1;
            }
        }
        Format::Turtle => {
            // Adapt the iterator: write one instance at a time via export_turtle_many.
            for item in instances {
                let value = item?;
                export_turtle_many(
                    std::iter::once(value),
                    &sv,
                    &schema,
                    &conv,
                    &mut writer,
                    ExportOptions {
                        skolem: args.skolem,
                    },
                )?;
                count += 1;
            }
        }
        Format::Ntriples => {
            for item in instances {
                let value = item?;
                export_ntriples_many(
                    std::iter::once(value),
                    &sv,
                    &schema,
                    &conv,
                    &mut writer,
                    ExportOptions {
                        skolem: args.skolem,
                    },
                )?;
                count += 1;
            }
        }
    }

    eprintln!("Wrote {count} instances (streaming, no Vec accumulation)");

    // ── Warning summary ──
    if let Some(handle) = warnings_handle {
        let drained = std::mem::take(&mut *handle.borrow_mut());
        if !drained.is_empty() {
            let mut by_type: std::collections::BTreeMap<String, usize> =
                std::collections::BTreeMap::new();
            for w in &drained {
                *by_type.entry(format!("{:?}", w.problem_type)).or_insert(0) += 1;
            }
            eprintln!("Harvest emitted {} warning(s):", drained.len());
            for (kind, n) in &by_type {
                eprintln!("  {kind}: {n}");
            }
            if std::env::var_os("LINKML_CONVERT_SHOW_WARNINGS").is_some() {
                for w in &drained {
                    eprintln!(
                        "  - [{:?}] {}: {}",
                        w.problem_type,
                        w.subject.join("/"),
                        w.detail
                    );
                }
            } else {
                eprintln!("  (set LINKML_CONVERT_SHOW_WARNINGS=1 for per-occurrence detail)");
            }
        }
    }

    Ok(())
}
