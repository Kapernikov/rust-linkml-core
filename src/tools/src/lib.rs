use linkml_runtime::{InstancePath, ValidationSeverity};

/// Helpers shared by CLI binaries.
pub mod validation_utils {
    use super::{format_path, severity_label};
    use linkml_runtime::ValidationResult;
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
}

fn severity_label(severity: &ValidationSeverity) -> &'static str {
    match severity {
        ValidationSeverity::Fatal => "fatal",
        ValidationSeverity::Error => "error",
        ValidationSeverity::Warning => "warning",
        ValidationSeverity::Info => "info",
    }
}

fn format_path(path: &InstancePath) -> String {
    if path.is_empty() {
        "<root>".to_string()
    } else {
        path.join(".")
    }
}
