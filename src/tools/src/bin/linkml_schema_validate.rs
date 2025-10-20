use clap::{Parser, ValueEnum};
#[cfg(feature = "resolve")]
use linkml_schemaview::resolve::resolve_schemas;
use linkml_schemaview::{identifier::Identifier, io::from_yaml, schemaview::SchemaView, Converter};
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "linkml-schema-validate")]
struct Args {
    /// LinkML schema YAML file
    schema: PathBuf,
    /// Output format
    #[arg(long, value_enum, default_value_t = OutputFormat::Text)]
    output: OutputFormat,
}

#[derive(ValueEnum, Clone)]
enum OutputFormat {
    Text,
    Json,
}

fn type_exists(
    sv: &SchemaView,
    id: &Identifier,
    conv: &Converter,
) -> Result<bool, linkml_schemaview::identifier::IdentifierError> {
    use linkml_schemaview::identifier::Identifier as Id;
    match id {
        Id::Name(n) => sv.with_schema_definitions(|schemas| {
            Ok(schemas.values().any(|schema| {
                schema
                    .types
                    .as_ref()
                    .map(|x| x.contains_key(n))
                    .unwrap_or(false)
            }))
        }),
        Id::Curie(_) | Id::Uri(_) => {
            let target_uri = id.to_uri(conv)?;
            sv.with_schema_definitions(|schemas| {
                for schema in schemas.values() {
                    if let Some(types) = &schema.types {
                        for t in types.values() {
                            if let Some(turi) = &t.type_uri {
                                if Identifier::new(turi).to_uri(conv)?.0 == target_uri.0 {
                                    return Ok(true);
                                }
                            }
                        }
                    }
                }
                Ok(false)
            })
        }
    }
}

fn enum_exists(
    sv: &SchemaView,
    id: &Identifier,
    conv: &Converter,
) -> Result<bool, linkml_schemaview::identifier::IdentifierError> {
    use linkml_schemaview::identifier::Identifier as Id;
    match id {
        Id::Name(n) => sv.with_schema_definitions(|schemas| {
            Ok(schemas.values().any(|schema| {
                schema
                    .enums
                    .as_ref()
                    .map(|x| x.contains_key(n))
                    .unwrap_or(false)
            }))
        }),
        Id::Curie(_) | Id::Uri(_) => {
            let target_uri = id.to_uri(conv)?;
            sv.with_schema_definitions(|schemas| {
                for schema in schemas.values() {
                    if let Some(enums) = &schema.enums {
                        for (name, e) in enums {
                            if let Some(euri) = &e.enum_uri {
                                if Identifier::new(euri).to_uri(conv)?.0 == target_uri.0 {
                                    return Ok(true);
                                }
                            } else {
                                let default_prefix =
                                    schema.default_prefix.as_deref().unwrap_or(&schema.name);
                                let default_uri =
                                    Identifier::new(&format!("{}:{}", default_prefix, name))
                                        .to_uri(conv)?
                                        .0;
                                if default_uri == target_uri.0 {
                                    return Ok(true);
                                }
                            }
                        }
                    }
                }
                Ok(false)
            })
        }
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let schema = from_yaml(&args.schema)?;
    let mut sv = SchemaView::new();
    sv.add_schema(schema.clone()).map_err(|e| e.to_string())?;
    #[cfg(feature = "resolve")]
    if let Err(e) = resolve_schemas(&mut sv) {
        eprintln!("{e}");
    }
    let conv = sv.converter();

    let mut errors = Vec::new();
    for uri in sv.get_unresolved_schemas() {
        errors.push(format!("Unresolved import: {}", uri.1));
    }

    sv.with_schema_definitions(|schemas| -> Result<(), String> {
        for (schema_uri, schema_def) in schemas {
            if let Some(defs) = &schema_def.slot_definitions {
                for (slot_name, slot_def) in defs {
                    if let Some(range) = &slot_def.range {
                        let id = Identifier::new(range);
                        let class_exists = sv
                            .get_class(&id, &conv)
                            .map_err(|e| format!("{e:?}"))?
                            .is_some();
                        let ty_exists =
                            type_exists(&sv, &id, &conv).map_err(|e| format!("{e:?}"))?;
                        let en_exists =
                            enum_exists(&sv, &id, &conv).map_err(|e| format!("{e:?}"))?;
                        if !class_exists && !ty_exists && !en_exists {
                            errors.push(format!(
                                "Unknown range `{}` for slot `{}` in schema `{}`",
                                range, slot_name, schema_uri
                            ));
                        }
                    }
                }
            }

            if let Some(clss) = &schema_def.classes {
                for (class_name, class_def) in clss {
                    if let Some(parent) = &class_def.is_a {
                        let id = Identifier::new(parent);
                        if sv
                            .get_class(&id, &conv)
                            .map_err(|e| format!("{e:?}"))?
                            .is_none()
                        {
                            errors.push(format!(
                                "Unknown parent class `{}` referenced by class `{}` in schema `{}`",
                                parent, class_name, schema_uri
                            ));
                        }
                    }
                    for slot in class_def.slots.as_ref().into_iter().flatten() {
                        let id = Identifier::new(slot);
                        if sv
                            .get_slot(&id, &conv)
                            .map_err(|e| format!("{e:?}"))?
                            .is_none()
                        {
                            errors.push(format!(
                                "Unknown slot `{}` used in class `{}` in schema `{}`",
                                slot, class_name, schema_uri
                            ));
                        }
                    }
                }
            }
        }
        Ok(())
    })?;

    if errors.is_empty() {
        match args.output {
            OutputFormat::Text => println!("schema valid"),
            OutputFormat::Json => println!("{}", serde_json::json!({"status":"valid"})),
        }
        Ok(())
    } else {
        match args.output {
            OutputFormat::Text => {
                for e in &errors {
                    println!("{e}");
                }
            }
            OutputFormat::Json => {
                println!("{}", serde_json::to_string_pretty(&errors)?);
            }
        }
        std::process::exit(1);
    }
}
