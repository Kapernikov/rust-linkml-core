#[cfg(feature = "ttl")]
use clap::{Parser, ValueEnum};
#[cfg(feature = "ttl")]
use linkml_runtime::{
    load_json_file, load_yaml_file,
    turtle::{write_ntriples, write_turtle, TurtleOptions},
    turtle_import::{import_ntriples, import_turtle},
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

    // ── Load input ──

    let instances: Vec<LinkMLInstance> = match in_fmt {
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
            vec![value]
        }
        Format::Turtle | Format::Ntriples => {
            // --class is validated above as mandatory for RDF input
            let class_refs: Vec<&str> = args.class.iter().map(|s| s.as_str()).collect();
            let file = File::open(&args.data)?;
            let reader = BufReader::new(file);
            let result = match in_fmt {
                Format::Ntriples => {
                    import_ntriples(reader, &sv, &conv, &class_refs).map_err(|e| e.to_string())?
                }
                _ => import_turtle(reader, &sv, &conv, &class_refs).map_err(|e| e.to_string())?,
            };
            let unconsumed_str = result
                .unconsumed_count
                .map(|n| format!("{n}"))
                .unwrap_or_else(|| "unknown".to_string());
            eprintln!(
                "Imported {} instances ({unconsumed_str} unconsumed triples)",
                result.instances.values().map(|v| v.len()).sum::<usize>(),
            );
            result.instances.into_values().flatten().collect()
        }
    };

    // ── Write output ──

    let mut writer: Box<dyn std::io::Write> = if let Some(out) = &args.output {
        Box::new(File::create(out)?)
    } else {
        Box::new(std::io::stdout())
    };

    match out_fmt {
        Format::Turtle => {
            for value in &instances {
                write_turtle(
                    value,
                    &sv,
                    &schema,
                    &conv,
                    &mut writer,
                    TurtleOptions {
                        skolem: args.skolem,
                    },
                )?;
            }
        }
        Format::Json => {
            if instances.len() == 1 {
                serde_json::to_writer_pretty(&mut writer, &instances[0].to_json())?;
            } else {
                let arr: Vec<_> = instances.iter().map(|i| i.to_json()).collect();
                serde_json::to_writer_pretty(&mut writer, &arr)?;
            }
            writeln!(writer)?;
        }
        Format::Yaml => {
            if instances.len() == 1 {
                serde_yaml::to_writer(&mut writer, &instances[0].to_json())?;
            } else {
                let arr: Vec<_> = instances.iter().map(|i| i.to_json()).collect();
                serde_yaml::to_writer(&mut writer, &arr)?;
            }
        }
        Format::Ntriples => {
            for value in &instances {
                write_ntriples(
                    value,
                    &sv,
                    &schema,
                    &conv,
                    &mut writer,
                    TurtleOptions {
                        skolem: args.skolem,
                    },
                )?;
            }
        }
    }

    Ok(())
}
