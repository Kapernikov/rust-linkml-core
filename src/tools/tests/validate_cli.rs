//! `linkml-validate`'s validity contract: **errors decide validity, warnings
//! never do**.
//!
//! The loader gained warning-severity diagnostics with the spec addendum
//! (designator canonicalisation, rule 5's dict-key reconciliation) — before it,
//! every diagnostic it could produce was an error, so "no diagnostics" and "no
//! errors" were the same predicate and the CLI could use either. They are not
//! the same predicate any more: a document whose only finding is a warning was
//! reported as invalid, exited 1, printed the warning in the error list, and
//! had `--lint-identity` suppressed with "fix the validation errors above
//! first" — an error message about a document that has none.
//!
//! Pinned here, because none of it is visible from a library test:
//!
//! * a warning-only document is **valid**: exit 0, `valid: true`, and the
//!   warning is still reported (text mode marks its severity, JSON mode carries
//!   it in `issues` with `severity: "warning"` as it always did);
//! * `--lint-identity` **runs** on such a document — the lint is gated on
//!   errors, not on diagnostics;
//! * an **error** still skips the lint, with the reason stated in both modes,
//!   and a warning listed beside errors is still marked as a warning;
//! * a warning-free document invoked without the flag produces byte-for-byte
//!   the output it always did.

use assert_cmd::Command;

const CLASS: &str = "Container";

fn data_path(name: &str) -> std::path::PathBuf {
    let mut p = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    p.push("tests/data");
    p.push(name);
    p
}

/// Run `linkml-validate` against the fixture schema, returning (exit code, stdout).
fn validate(data: &str, extra: &[&str]) -> (i32, String) {
    let mut cmd = Command::cargo_bin("linkml-validate").unwrap();
    cmd.arg(data_path("validate_warning_only.yaml"))
        .arg(CLASS)
        .arg(data_path(data))
        .args(extra);
    let out = cmd.output().unwrap();
    (
        out.status.code().unwrap_or(-1),
        String::from_utf8(out.stdout).unwrap(),
    )
}

fn validate_json(data: &str, extra: &[&str]) -> (i32, serde_json::Value) {
    let mut args = vec!["--json"];
    args.extend_from_slice(extra);
    let (code, out) = validate(data, &args);
    (code, serde_json::from_str(&out).expect("JSON on stdout"))
}

// ---------------------------------------------------------------------------
// A warning is not an error
// ---------------------------------------------------------------------------

/// Text mode. The document's only load diagnostic is rule 5's dict-key
/// divergence warning, so the document is valid — and the warning is still
/// printed, marked with its severity so it cannot be read as an error.
#[test]
fn warning_only_document_is_valid_in_text_mode() {
    let (code, out) = validate("validate_warning_only.json", &[]);
    assert_eq!(code, 0, "a warning must not fail the run: {out}");
    assert!(out.starts_with("valid\n"), "{out}");
    assert!(
        out.contains("warning: SlotRangeViolation at people.p1.pid:"),
        "the warning is reported, with its severity: {out}"
    );
    assert!(
        !out.contains("fix the validation errors above first"),
        "there are no errors to fix: {out}"
    );
}

/// JSON mode. `valid` reflects errors only; the warning stays in `issues`,
/// where it always carried `severity: "warning"`.
#[test]
fn warning_only_document_is_valid_in_json_mode() {
    let (code, doc) = validate_json("validate_warning_only.json", &[]);
    assert_eq!(code, 0);
    assert_eq!(doc["valid"], serde_json::json!(true), "{doc:#}");
    let issues = doc["issues"].as_array().expect("issues array");
    assert_eq!(issues.len(), 1, "{doc:#}");
    assert_eq!(issues[0]["severity"], serde_json::json!("warning"));
}

/// The lint is gated on *errors*. This document has a warning and a genuine
/// duplicate identity, and the flag must report the duplicate.
#[test]
fn lint_runs_on_a_warning_only_document() {
    let (code, out) = validate("validate_warning_only.json", &["--lint-identity"]);
    assert_eq!(code, 0, "{out}");
    assert!(
        out.contains("warning[readings]:") && out.contains("share the declared identity 'c1'"),
        "the instance lint ran and reported: {out}"
    );

    let (code, doc) = validate_json("validate_warning_only.json", &["--lint-identity"]);
    assert_eq!(code, 0);
    assert_eq!(doc["identity_lint_skipped"], serde_json::json!(false));
    assert_eq!(doc["identity_lint_skipped_reason"], serde_json::Value::Null);
    let warnings = doc["identity_warnings"].as_array().expect("array");
    assert_eq!(warnings.len(), 1, "{doc:#}");
    assert_eq!(warnings[0]["subject"], serde_json::json!(["readings"]));
}

// ---------------------------------------------------------------------------
// An error still is one
// ---------------------------------------------------------------------------

/// Same duplicate identity, plus an error. The lint is skipped and says so —
/// its answers over a tree whose loading went wrong would be about the damage.
#[test]
fn errors_skip_the_lint_and_say_so() {
    let (code, out) = validate("validate_errors.json", &["--lint-identity"]);
    assert_eq!(code, 1, "{out}");
    assert!(
        out.contains("note: --lint-identity skipped: fix the validation errors above first"),
        "{out}"
    );
    assert!(
        !out.contains("share the declared identity"),
        "no lint output when the lint was skipped: {out}"
    );

    let (code, doc) = validate_json("validate_errors.json", &["--lint-identity"]);
    assert_eq!(code, 1);
    assert_eq!(doc["valid"], serde_json::json!(false), "{doc:#}");
    assert_eq!(doc["identity_lint_skipped"], serde_json::json!(true));
    assert!(
        doc["identity_lint_skipped_reason"]
            .as_str()
            .unwrap_or_default()
            .contains("error"),
        "the reason names errors, since errors are what gates the lint: {doc:#}"
    );
    assert_eq!(doc["identity_warnings"], serde_json::Value::Null);
}

/// A document with both. The error decides validity, and the two lines are
/// told apart: the error keeps the bare shape this binary has always printed
/// for one, the warning says what it is. Anything else asks the reader to fix
/// "errors" that include a warning — the same misreading the exit code used to
/// force, one line further down.
#[test]
fn a_warning_beside_an_error_is_still_marked_as_a_warning() {
    let (code, out) = validate("validate_mixed.json", &[]);
    assert_eq!(code, 1, "{out}");
    assert!(
        out.contains("warning: SlotRangeViolation at people.p1.pid:"),
        "the warning names its severity: {out}"
    );
    assert!(
        out.contains("UndeclaredSlot at readings.0.not_a_slot:")
            && !out.contains("error: UndeclaredSlot"),
        "the error keeps the published unmarked shape: {out}"
    );
}

// ---------------------------------------------------------------------------
// Nothing moved for a document with no diagnostics
// ---------------------------------------------------------------------------

/// Byte-identity: a warning-free document, no flag. Every document that
/// predates the addendum's warnings is this case, which is what makes the
/// change above a fix rather than an output break.
#[test]
fn warning_free_document_without_the_flag_is_unchanged() {
    let (code, out) = validate("validate_clean.json", &[]);
    assert_eq!(code, 0);
    assert_eq!(out, "valid\n");

    let (code, out) = validate("validate_clean.json", &["--json"]);
    assert_eq!(code, 0);
    assert_eq!(
        out, "{\n  \"issues\": [],\n  \"valid\": true\n}\n",
        "the no-flag JSON document keeps exactly its two keys"
    );
}
