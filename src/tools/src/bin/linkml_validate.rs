use clap::Parser;
use linkml_runtime::{
    lint_instance_identity, load_json_file, load_yaml_file, ValidationResult, ValidationValue,
};
use linkml_schemaview::identifier::Identifier;
use linkml_schemaview::io::from_yaml;
#[cfg(feature = "resolve")]
use linkml_schemaview::resolve::resolve_schemas_from;
use linkml_schemaview::schemaview::SchemaView;
use linkml_tools::validation_utils::{format_path, identity_warnings_json, severity_label};
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
    // Validity is decided by ERRORS, not by diagnostics. Until the spec
    // addendum the loader could only produce errors, so `issues.is_empty()`
    // and "no errors" were one predicate; rules 2 and 5 made the loader emit
    // warnings, and the two parted company. A document whose only finding is a
    // warning is valid: it exits 0, and the warning is reported rather than
    // being dressed up as the reason for a failure.
    let is_valid = !load_result.has_errors();
    let instance = load_result.instance;
    let validation_issues = load_result.validation_issues;
    // Opt-in instance-identity lint, deliberately skipped when the data does
    // not validate — the same stance `linkml-schema-validate` takes towards a
    // schema with errors. The lint asks how a list's elements are addressed,
    // and answering that over a tree whose loading already went wrong produces
    // answers about the damage rather than about the data. Reporting the
    // validation errors first is also the only useful output in that case.
    //
    // The gate is errors alone: a warning says the document is unusual, not
    // that it failed to load, and the lint's answers over it are exactly as
    // sound as over a silent one.
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
        // A valid document may now carry diagnostics, all of them non-error.
        for issue in &validation_issues {
            print_issue(issue);
        }
        for w in &identity_warnings {
            println!("warning[{}]: {}", w.subject.join("."), w.detail);
        }
        Ok(())
    } else {
        for issue in &validation_issues {
            print_issue(issue);
        }
        if args.lint_identity {
            println!("note: --lint-identity skipped: fix the validation errors above first");
        }
        std::process::exit(1);
    }
}

/// One validation issue, in text mode, wherever it appears.
///
/// An error keeps the bare `Type at path: detail` line this binary has always
/// printed — so a document whose diagnostics are all errors prints exactly what
/// it always printed, which is every document that predates the spec addendum's
/// warnings. Anything that is *not* an error is named, because the alternative
/// is a warning that looks like the error it is listed beside: in the valid
/// branch it would be an error on a document that has none, and in the error
/// branch it would be one more thing the reader is told to fix before re-running.
fn print_issue(issue: &ValidationResult) {
    let location = format_path(&issue.subject);
    if issue.severity.is_error() {
        println!("{:?} at {}: {}", issue.problem_type, location, issue.detail);
    } else {
        println!(
            "{}: {:?} at {}: {}",
            severity_label(&issue.severity),
            issue.problem_type,
            location,
            issue.detail
        );
    }
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
            "identity_lint_skipped_reason": "data has validation errors; fix them and re-run",
        })
    };
    println!("{}", serde_json::to_string_pretty(&output)?);
    Ok(())
}
