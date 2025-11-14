use clap::Parser;
use linkml_runtime::{
    load_json_file, load_yaml_file, ValidationResult, ValidationSeverity, ValidationValue,
};
use linkml_schemaview::identifier::Identifier;
use linkml_schemaview::io::from_yaml;
#[cfg(feature = "resolve")]
use linkml_schemaview::resolve::resolve_schemas;
use linkml_schemaview::schemaview::SchemaView;
use serde_json::json;
use std::path::PathBuf;

#[derive(Parser)]
struct Args {
    /// LinkML schema YAML file
    schema: PathBuf,
    /// Name of the class to validate against
    class: String,
    /// Data file (YAML or JSON)
    data: PathBuf,
    /// Emit machine-readable JSON instead of human-readable text
    #[arg(long)]
    json: bool,
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
    let validation_issues = load_result.validation_issues;
    let is_valid = validation_issues.is_empty();
    if args.json {
        emit_json(is_valid, &validation_issues)?;
        if is_valid {
            Ok(())
        } else {
            std::process::exit(1);
        }
    } else if is_valid {
        println!("valid");
        Ok(())
    } else {
        for issue in &validation_issues {
            let location = if issue.subject.is_empty() {
                "<root>".to_string()
            } else {
                issue.subject.join(".")
            };
            println!("{:?} at {}: {}", issue.problem_type, location, issue.detail);
        }
        std::process::exit(1);
    }
}

fn emit_json(valid: bool, issues: &[ValidationResult]) -> Result<(), serde_json::Error> {
    let issues_json: Vec<_> = issues
        .iter()
        .map(|issue| {
            let object = match &issue.object {
                ValidationValue::None => serde_json::Value::Null,
                ValidationValue::Literal(v) => json!({ "literal": v }),
                ValidationValue::Node(path) => json!({ "node": path }),
            };
            json!({
                "type": format!("{:?}", issue.problem_type),
                "severity": severity_label(&issue.severity),
                "subject": issue.subject,
                "predicate": issue.predicate,
                "instantiates": issue.instantiates,
                "node_source": issue.node_source,
                "object": object,
                "detail": issue.detail,
            })
        })
        .collect();
    let output = json!({
        "valid": valid,
        "issues": issues_json,
    });
    println!("{}", serde_json::to_string_pretty(&output)?);
    Ok(())
}

fn severity_label(severity: &ValidationSeverity) -> &'static str {
    match severity {
        ValidationSeverity::Fatal => "fatal",
        ValidationSeverity::Error => "error",
        ValidationSeverity::Warning => "warning",
        ValidationSeverity::Info => "info",
    }
}
