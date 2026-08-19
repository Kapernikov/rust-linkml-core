/// Helpers shared by CLI binaries.
///
/// Everything here is a *rendering* decision that more than one binary makes,
/// and that the binaries must make identically: two CLIs printing one lint two
/// ways is a difference a reader has to explain to themselves. The rule for
/// what belongs here is that — shared vocabulary — not "utility".
pub mod validation_utils {
    use linkml_runtime::{InstancePath, ValidationResult, ValidationSeverity};
    use serde_json::json;
    use std::path::Path;

    /// Print validation diagnostics to stderr but keep execution going.
    pub fn report_validation_issues(path: &Path, issues: &[ValidationResult]) {
        if issues.is_empty() {
            return;
        }
        eprintln!(
            "Validation produced {} issue(s) while loading '{}'; continuing.",
            issues.len(),
            path.display()
        );
        for issue in issues {
            eprintln!(
                "  - [{}::{:?}] {}: {}",
                severity_label(&issue.severity),
                issue.problem_type,
                format_path(&issue.subject),
                issue.detail
            );
        }
    }

    /// The machine-readable name of a severity, as every CLI spells it.
    pub fn severity_label(severity: &ValidationSeverity) -> &'static str {
        match severity {
            ValidationSeverity::Fatal => "fatal",
            ValidationSeverity::Error => "error",
            ValidationSeverity::Warning => "warning",
            ValidationSeverity::Info => "info",
        }
    }

    /// An instance path as the CLIs display it; the empty path is the root.
    pub fn format_path(path: &InstancePath) -> String {
        if path.is_empty() {
            "<root>".to_string()
        } else {
            path.join(".")
        }
    }

    /// The identity-lint warnings, in the one shape both `linkml-validate` and
    /// `linkml-schema-validate` emit them: the two CLIs report one lint the
    /// same way, so a consumer reading either document reads the same keys.
    ///
    /// Uses [`linkml_runtime::ValidationProblemType::label`] rather than the
    /// binaries' older `Debug` spelling for validation issues: the label is the
    /// shared machine-readable name (the Python binding reports the same one),
    /// and a variant rename cannot change it behind the CLIs' back. The
    /// separate `issues` shape of `linkml-validate` keeps its `Debug` spelling
    /// — that one is a published contract of that binary.
    pub fn identity_warnings_json(warnings: &[ValidationResult]) -> serde_json::Value {
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
}
