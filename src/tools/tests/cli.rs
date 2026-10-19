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
    // The keyless child of the `obj` slot is named after the slot it hangs off.
    // The slash cannot appear bare in a Turtle prefixed name, so the serializer
    // must escape it (`poly:root\/obj`) or keep the full IRI; never `poly:root/obj`.
    assert!(
        ttl.contains("poly:root\\/obj") || ttl.contains("<https://example.com/poly/root/obj>"),
        "Expected an escaped prefixed name or a full IRI for the skolem node. Got:\n{}",
        ttl
    );
    assert!(
        !ttl.contains("poly:root/obj"),
        "A bare slash in a local name is invalid Turtle. Got:\n{}",
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
