use clap::Parser;
use linkml_runtime::{
    lint_instance_identity, load_json_file, load_yaml_file, ValidationResult, ValidationSeverity,
    ValidationValue,
};
use linkml_schemaview::identifier::Identifier;
use linkml_schemaview::io::from_yaml;
#[cfg(feature = "resolve")]
use linkml_schemaview::resolve::resolve_schemas_from;
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
    /// Opt-in: warn where this data defeats a declared element identity — a
    /// list repeating one, or one addressed positionally because some element
    /// leaves the identity slot empty. Warnings never change the exit code.
    #[arg(long, default_value_t = false)]
    lint_identity: bool,
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
    resolve_schemas_from(&mut sv, &args.schema).map_err(|e| e.to_string())?;
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
    let instance = load_result.instance;
    let validation_issues = load_result.validation_issues;
    let is_valid = validation_issues.is_empty();
    // Opt-in instance-identity lint, deliberately skipped when the data does
    // not validate — the same stance `linkml-schema-validate` takes towards a
    // schema with errors. The lint asks how a list's elements are addressed,
    // and answering that over a tree whose loading already went wrong produces
    // answers about the damage rather than about the data. Reporting the
    // validation issues first is also the only useful output in that case.
    //
    // Warnings never change the exit code: it stays whatever validation made it.
    let identity_warnings = match (args.lint_identity, is_valid, &instance) {
        (true, true, Some(value)) => lint_instance_identity(value),
        _ => Vec::new(),
    };
    if args.json {
        emit_json(
            is_valid,
            &validation_issues,
            args.lint_identity,
            &identity_warnings,
        )?;
        if is_valid {
            Ok(())
        } else {
            std::process::exit(1);
        }
    } else if is_valid {
        println!("valid");
        for w in &identity_warnings {
            println!("warning[{}]: {}", w.subject.join("."), w.detail);
        }
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
        if args.lint_identity {
            println!("note: --lint-identity skipped: fix the validation errors above first");
        }
        std::process::exit(1);
    }
}

/// The identity warnings, in the shape `linkml-schema-validate` already emits
/// them, so the two CLIs report one lint the same way.
///
/// Uses `ValidationProblemType::label()` rather than this binary's older
/// `Debug` spelling for validation issues: the label is the shared
/// machine-readable name (the Python binding reports the same one), and a
/// variant rename cannot change it behind the CLI's back. The existing
/// `issues` shape is left exactly as it was — its `Debug` spelling is a
/// published contract of this binary.
fn identity_warnings_json(warnings: &[ValidationResult]) -> serde_json::Value {
    serde_json::Value::Array(
        warnings
            .iter()
            .map(|w| {
                json!({
                    "type": w.problem_type.label(),
                    "severity": severity_label(&w.severity),
                    "subject": w.subject,
                    "detail": w.detail,
                })
            })
            .collect(),
    )
}

fn emit_json(
    valid: bool,
    issues: &[ValidationResult],
    lint_identity: bool,
    identity_warnings: &[ValidationResult],
) -> Result<(), serde_json::Error> {
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
    // Without the flag the document is byte-identical to what it always was:
    // the lint keys are absent, not null. A consumer that never opts in cannot
    // tell this version from the previous one.
    let output = if !lint_identity {
        json!({
            "valid": valid,
            "issues": issues_json,
        })
    } else if valid {
        json!({
            "valid": valid,
            "issues": issues_json,
            "identity_warnings": identity_warnings_json(identity_warnings),
            "identity_lint_skipped": false,
            "identity_lint_skipped_reason": serde_json::Value::Null,
        })
    } else {
        json!({
            "valid": valid,
            "issues": issues_json,
            "identity_warnings": serde_json::Value::Null,
            "identity_lint_skipped": true,
            "identity_lint_skipped_reason": "data has validation issues; fix them and re-run",
        })
    };
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
