use assert_cmd::Command;

fn data_path(name: &str) -> std::path::PathBuf {
    let mut p = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    p.push("../runtime/tests/data");
    p.push(name);
    p
}

#[test]
fn skolem_flag_creates_named_nodes() {
    let schema = data_path("poly_schema.yaml");
    let data = data_path("poly_with_type.yaml");
    let tmp_dir = tempfile::tempdir().unwrap();
    let out_path = tmp_dir.path().join("out.ttl");

    let mut cmd = Command::cargo_bin("linkml-convert").unwrap();
    cmd.arg(&schema)
        .arg(&data)
        .arg("-o")
        .arg(&out_path)
        .arg("--skolem");
    cmd.assert().success();

    let ttl = std::fs::read_to_string(&out_path).unwrap();
    // Skolem IRIs with slashes in the local name are kept as full IRIs
    // because "poly:root/gen1" is invalid Turtle (slash in local name).
    assert!(
        ttl.contains("<https://example.com/poly/root/gen1>"),
        "Expected full IRI for skolem node with slash in local name. Got:\n{}",
        ttl
    );
}

#[test]
fn convert_personinfo_cli() {
    let schema = data_path("personinfo.yaml");
    let data = data_path("example_personinfo_data.yaml");
    let mut cmd = Command::cargo_bin("linkml-convert").unwrap();
    cmd.arg(&schema).arg(&data);
    cmd.assert()
        .success()
        .stdout(predicates::str::contains("@prefix P:"));
}

#[test]
fn convert_meta_self_hosting() {
    let schema = data_path("meta.yaml");
    let mut cmd = Command::cargo_bin("linkml-convert").unwrap();
    cmd.arg(&schema).arg(&schema);
    cmd.assert().success();
}
