#[cfg(feature = "ttl")]
use clap::{Parser, ValueEnum};
#[cfg(feature = "ttl")]
use linkml_runtime::{
    load_json_file, load_yaml_file,
    turtle::{write_ntriples, write_turtle, TurtleOptions},
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

    // ── Input: build a streaming iterator of instances ──
    //
    // The iterator yields one `LinkMLInstance` at a time so the output side
    // never has to materialise a `Vec<LinkMLInstance>`. For YAML/JSON input
    // it yields exactly one element (the tree root). For RDF input it
    // yields each unclaimed root from the streaming harvest.
    //
    // For RDF inputs, `import_owned_store_streaming` keeps the parsed store
    // alive inside the iterator itself, so we don't need separate scaffolding
    // here.
    type InstanceItem = Result<LinkMLInstance, Box<dyn std::error::Error>>;
    let instances: Box<dyn Iterator<Item = InstanceItem>> = match in_fmt {
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
            Box::new(std::iter::once(Ok(value)))
        }
        Format::Turtle | Format::Ntriples => {
            use linkml_runtime::rdf_import_store::RdfImportStore;
            use linkml_runtime::rdf_streaming::import_owned_store_streaming;

            let class_refs: Vec<String> = args.class.clone();
            let class_refs_borrow: Vec<&str> = class_refs.iter().map(|s| s.as_str()).collect();

            // Branch on --disk-graph (feature-gated) vs the default
            // in-memory backend.
            #[cfg(feature = "disk_graph")]
            {
                if let Some(disk_path) = args.disk_graph.as_ref() {
                    use linkml_runtime::rdf_import_store_disk::DiskRdfImportStore;
                    let file = File::open(&args.data)?;
                    let reader = BufReader::new(file);
                    let store = match in_fmt {
                        Format::Ntriples => DiskRdfImportStore::from_ntriples(reader, disk_path)
                            .map_err(|e| e.to_string())?,
                        _ => DiskRdfImportStore::from_turtle(reader, disk_path)
                            .map_err(|e| e.to_string())?,
                    };
                    let owned_stream = import_owned_store_streaming(
                        store,
                        sv.clone(),
                        conv.clone(),
                        &class_refs_borrow,
                        std::rc::Rc::new(std::cell::RefCell::new(Vec::new())),
                        false,
                    )
                    .map_err(|e| e.to_string())?;
                    Box::new(owned_stream.map(|res| {
                        res.map(|(_class, inst)| inst)
                            .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)
                    }))
                } else {
                    let file = File::open(&args.data)?;
                    let reader = BufReader::new(file);
                    let store = match in_fmt {
                        Format::Ntriples => {
                            RdfImportStore::from_ntriples(reader).map_err(|e| e.to_string())?
                        }
                        _ => RdfImportStore::from_turtle(reader).map_err(|e| e.to_string())?,
                    };
                    let owned_stream = import_owned_store_streaming(
                        store,
                        sv.clone(),
                        conv.clone(),
                        &class_refs_borrow,
                        std::rc::Rc::new(std::cell::RefCell::new(Vec::new())),
                        false,
                    )
                    .map_err(|e| e.to_string())?;
                    Box::new(owned_stream.map(|res| {
                        res.map(|(_class, inst)| inst)
                            .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)
                    }))
                }
            }
            #[cfg(not(feature = "disk_graph"))]
            {
                let file = File::open(&args.data)?;
                let reader = BufReader::new(file);
                let store = match in_fmt {
                    Format::Ntriples => {
                        RdfImportStore::from_ntriples(reader).map_err(|e| e.to_string())?
                    }
                    _ => RdfImportStore::from_turtle(reader).map_err(|e| e.to_string())?,
                };
                let owned_stream = import_owned_store_streaming(
                    store,
                    sv.clone(),
                    conv.clone(),
                    &class_refs_borrow,
                    std::rc::Rc::new(std::cell::RefCell::new(Vec::new())),
                    false,
                )
                .map_err(|e| e.to_string())?;
                Box::new(owned_stream.map(|res| {
                    res.map(|(_class, inst)| inst)
                        .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)
                }))
            }
        }
    };

    // ── Output: drive from the iterator, write one instance at a time ──

    let mut count: usize = 0;
    match out_fmt {
        Format::Json => {
            // Always emit a JSON array, regardless of how many instances we
            // end up with. Streaming-friendly: we don't know the count up
            // front. Single-element input still yields `[ {...} ]`.
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
            // YAML stream-of-documents: each instance separated by `---`.
            for item in instances {
                let value = item?;
                writeln!(writer, "---")?;
                serde_yaml::to_writer(&mut writer, &value.to_json())?;
                count += 1;
            }
        }
        Format::Turtle => {
            for item in instances {
                let value = item?;
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
                count += 1;
            }
        }
        Format::Ntriples => {
            for item in instances {
                let value = item?;
                write_ntriples(
                    &value,
                    &sv,
                    &schema,
                    &conv,
                    &mut writer,
                    TurtleOptions {
                        skolem: args.skolem,
                    },
                )?;
                count += 1;
            }
        }
    }

    eprintln!("Wrote {count} instances (streaming, no Vec accumulation)");
    Ok(())
}
