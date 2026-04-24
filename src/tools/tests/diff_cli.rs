use assert_cmd::Command;
use std::path::PathBuf;

fn info_path(name: &str) -> PathBuf {
    let mut p = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    p.push("../runtime/tests/data");
    p.push(name);
    p
}

#[test]
fn cli_diff_and_patch_personinfo() {
    let schema = info_path("personinfo.yaml");
    let src = info_path("example_personinfo_data.yaml");
    let tgt = info_path("example_personinfo_data_2.yaml");
    let tmp = tempfile::tempdir().unwrap();
    let delta = tmp.path().join("delta.yaml");
    let out = tmp.path().join("out.yaml");

    let mut cmd = Command::cargo_bin("linkml-diff").unwrap();
    cmd.arg(&schema)
        .arg("-c")
        .arg("Container")
        .arg(&src)
        .arg(&tgt)
        .arg("-o")
        .arg(&delta)
        .arg("--treat-missing-as-null")
        .arg("false");
    cmd.assert().success();

    let mut cmd = Command::cargo_bin("linkml-patch").unwrap();
    cmd.arg(&schema)
        .arg("-c")
        .arg("Container")
        .arg(&src)
        .arg(&delta)
        .arg("-o")
        .arg(&out);
    cmd.assert().success();

    let out_data: serde_yaml::Value =
        serde_yaml::from_str(&std::fs::read_to_string(&out).unwrap()).unwrap();
    let tgt_data: serde_yaml::Value =
        serde_yaml::from_str(&std::fs::read_to_string(&tgt).unwrap()).unwrap();
    assert_eq!(out_data, tgt_data);
}
