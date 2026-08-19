use assert_cmd::Command;
use predicates::prelude::*;
use std::path::PathBuf;

fn data_path(name: &str) -> PathBuf {
    let mut p = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    p.push("../runtime/tests/data");
    p.push(name);
    p
}

#[test]
fn detect_invalid_schema() {
    let schema = data_path("invalid_schema.yaml");
    let mut cmd = Command::cargo_bin("linkml-schema-validate").unwrap();
    cmd.arg(&schema);
    cmd.assert()
        .failure()
        .stdout(predicate::str::contains("Unknown parent class"))
        .stdout(predicate::str::contains("Unknown slot"));
}

#[test]
fn person_schema_missing_slot() {
    let schema = data_path("personinfo.yaml");
    let mut cmd = Command::cargo_bin("linkml-schema-validate").unwrap();
    cmd.arg(&schema);
    cmd.assert().success();
}

/// The JSON `--lint-identity` payload, and the process's success.
fn lint_identity_json(schema: &str) -> (bool, serde_json::Value) {
    let mut cmd = Command::cargo_bin("linkml-schema-validate").unwrap();
    cmd.arg(data_path(schema))
        .arg("--lint-identity")
        .arg("--output")
        .arg("json");
    let out = cmd.output().unwrap();
    let stdout = String::from_utf8(out.stdout).unwrap();
    let parsed =
        serde_json::from_str(&stdout).unwrap_or_else(|e| panic!("not JSON ({e}): {stdout}"));
    (out.status.success(), parsed)
}

/// The five keys the `--lint-identity --output json` contract promises, on
/// every outcome. Consumers branch on `identity_lint_skipped`, so a key that
/// appears only on one path is a key they cannot rely on.
const LINT_JSON_KEYS: [&str; 5] = [
    "errors",
    "identity_lint_skipped",
    "identity_lint_skipped_reason",
    "identity_warnings",
    "status",
];

fn keys(v: &serde_json::Value) -> Vec<String> {
    v.as_object()
        .expect("a JSON object")
        .keys()
        .cloned()
        .collect()
}

#[test]
fn lint_identity_json_shape_when_the_schema_is_valid() {
    let (ok, v) = lint_identity_json("identity.yaml");
    assert!(ok, "the identity fixture must validate: {v}");
    assert_eq!(keys(&v), LINT_JSON_KEYS);
    assert_eq!(v["status"], "valid");
    assert_eq!(v["errors"], serde_json::json!([]));
    assert_eq!(v["identity_lint_skipped"], false);
    assert!(v["identity_lint_skipped_reason"].is_null());

    let warnings = v["identity_warnings"].as_array().expect("an array");
    assert!(!warnings.is_empty(), "the fixture has flagged slots: {v}");
    for w in warnings {
        assert_eq!(keys(w), ["detail", "severity", "subject", "type"]);
        // The machine-readable spelling, shared with the Python binding's
        // `problem_type`. `Debug` formatting would make a variant rename a
        // silent change of this contract, and would spell it differently from
        // the other binding for the same value.
        assert_eq!(
            w["type"], "ambiguous_element_identity",
            "the JSON type must be the shared snake_case label: {w}"
        );
        assert_eq!(w["severity"], "warning");
        assert!(w["subject"].is_array());
        assert!(w["detail"].as_str().is_some_and(|d| !d.is_empty()));
    }
}

#[test]
fn lint_identity_json_shape_when_the_lint_is_skipped() {
    let (ok, v) = lint_identity_json("invalid_schema.yaml");
    assert!(!ok, "an invalid schema must still exit non-zero: {v}");
    assert_eq!(keys(&v), LINT_JSON_KEYS);
    assert_eq!(v["status"], "invalid");
    assert!(!v["errors"].as_array().expect("an array").is_empty());
    assert!(v["identity_warnings"].is_null());
    assert_eq!(v["identity_lint_skipped"], true);
    assert!(v["identity_lint_skipped_reason"]
        .as_str()
        .is_some_and(|r| !r.is_empty()));
}
