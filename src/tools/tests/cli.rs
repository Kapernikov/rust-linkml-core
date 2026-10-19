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
    // The serializer may spell it as an escaped prefixed name, so compare the
    // parsed IRIs, not the text.
    let subjects: Vec<oxrdf::NamedOrBlankNode> = oxttl::TurtleParser::new()
        .for_reader(ttl.as_bytes())
        .map(|t| t.unwrap().subject)
        .collect();
    assert!(
        subjects.contains(&oxrdf::NamedOrBlankNode::NamedNode(
            oxrdf::NamedNode::new("https://example.com/poly/root/obj").unwrap()
        )),
        "Expected the skolem node IRI. Got:\n{}",
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
