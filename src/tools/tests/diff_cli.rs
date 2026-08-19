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

/// `patch` records an unappliable delta instead of voiding the batch, so the
/// CLI has to say which delta was dropped — otherwise a half-applied patch
/// writes a file that looks clean — and has to signal it in the exit status,
/// which is `2`: partial application, not a failed run.
///
/// Both ways a delta can be unappliable are exercised: an address that resolves
/// to nothing (soft before rule 4 as well) and a payload that cannot be BUILT
/// at the location it addresses — `no_such_slot` is declared by no class, so
/// the builder has no slot to box the scalar against. That second one used to
/// propagate as an `Err` and void the whole batch; this test is what pins rule
/// 4's soft path all the way out to the CLI surface.
#[test]
fn cli_patch_reports_failed_delta_paths_and_exits_2() {
    let schema = info_path("personinfo.yaml");
    let src = info_path("example_personinfo_data.yaml");
    let tmp = tempfile::tempdir().unwrap();
    let delta = tmp.path().join("delta.json");
    let out = tmp.path().join("out.yaml");
    std::fs::write(
        &delta,
        r#"[{"path": ["objects", "P:404", "name"], "op": "update",
             "old": "nobody", "new": "somebody"},
            {"path": ["objects", "P:001", "no_such_slot"], "op": "update",
             "old": "x", "new": "y"},
            {"path": ["objects", "P:001", "name"], "op": "update",
             "old": "fred bloggs", "new": "fred b."}]"#,
    )
    .unwrap();

    let mut cmd = Command::cargo_bin("linkml-patch").unwrap();
    cmd.arg(&schema)
        .arg("-c")
        .arg("Container")
        .arg(&src)
        .arg(&delta)
        .arg("-o")
        .arg(&out);
    let assert = cmd.assert().code(2);
    let stderr = String::from_utf8(assert.get_output().stderr.clone()).unwrap();
    assert!(
        stderr.contains("could not be applied")
            && stderr.contains("objects.P:404.name")
            && stderr.contains("objects.P:001.no_such_slot"),
        "both dropped deltas must be named: {stderr}"
    );
    assert!(
        !stderr.contains("objects.P:001.name\n"),
        "the applied delta must not be reported: {stderr}"
    );
    let patched = std::fs::read_to_string(&out).unwrap();
    assert!(
        patched.contains("fred b."),
        "the good delta must still land: {patched}"
    );
    assert!(
        !patched.contains("no_such_slot"),
        "the unbuildable delta must leave no trace: {patched}"
    );
}

/// The other half of the exit contract: a patch that applies every delta exits
/// `0`, and says nothing on stderr.
#[test]
fn cli_patch_exits_0_when_every_delta_applies() {
    let schema = info_path("personinfo.yaml");
    let src = info_path("example_personinfo_data.yaml");
    let tmp = tempfile::tempdir().unwrap();
    let delta = tmp.path().join("delta.json");
    let out = tmp.path().join("out.yaml");
    std::fs::write(
        &delta,
        r#"[{"path": ["objects", "P:001", "name"], "op": "update",
             "old": "fred bloggs", "new": "fred b."}]"#,
    )
    .unwrap();

    let mut cmd = Command::cargo_bin("linkml-patch").unwrap();
    cmd.arg(&schema)
        .arg("-c")
        .arg("Container")
        .arg(&src)
        .arg(&delta)
        .arg("-o")
        .arg(&out);
    let assert = cmd.assert().code(0);
    let stderr = String::from_utf8(assert.get_output().stderr.clone()).unwrap();
    assert!(
        !stderr.contains("could not be applied"),
        "a clean patch reports nothing: {stderr}"
    );
}
