#![cfg(feature = "resolve")]
//! Relative imports must resolve against the importing schema's own file, and
//! resolution must keep going until nothing is left to resolve.
//!
//! `nested_root.yaml` imports `./nested_a`, which in turn imports `./nested_b`.
//! Two independent things have to work for `nested_b` to arrive:
//!
//! 1. **Fixpoint.** `nested_a`'s import only becomes visible after `nested_a`
//!    itself is loaded, so a single pass over the initially-unresolved list can
//!    never reach `nested_b`.
//! 2. **Source-relative bases.** `./nested_a` is meaningless relative to the
//!    process CWD (these tests run from the crate root, not `tests/data`), so
//!    the base directory has to come from the file each schema was loaded from.

use linkml_schemaview::io::from_yaml;
use linkml_schemaview::resolve::{resolve_schemas, resolve_schemas_from};
use linkml_schemaview::schemaview::SchemaView;
use std::path::PathBuf;

fn data_path(name: &str) -> PathBuf {
    let mut p = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    p.push("tests");
    p.push("data");
    p.push(name);
    p
}

fn view_of(name: &str) -> (SchemaView, PathBuf) {
    let path = data_path(name);
    let schema = from_yaml(&path).unwrap();
    let mut sv = SchemaView::new();
    sv.add_schema(schema).unwrap();
    (sv, path)
}

#[test]
fn resolves_an_import_of_an_import_relative_to_each_source_file() {
    let (mut sv, path) = view_of("nested_root.yaml");
    // The process CWD is the crate root, so `./nested_a` does not exist
    // relative to it: only the seeded source directory can resolve this.
    assert!(
        !PathBuf::from("./nested_a.yaml").exists(),
        "precondition: the fixture must not be reachable from the CWD, \
         otherwise this test would pass for the wrong reason"
    );

    resolve_schemas_from(&mut sv, &path).unwrap();

    assert!(
        sv.get_unresolved_schemas().is_empty(),
        "left unresolved: {:?}",
        sv.get_unresolved_schemas()
    );
    assert!(
        sv.get_schema("http://example.com/nested_a").is_some(),
        "the root's own import must resolve against the root's directory"
    );
    assert!(
        sv.get_schema("http://example.com/nested_b").is_some(),
        "the import of an import must resolve against ITS importer's directory, \
         which requires both the fixpoint loop and per-schema source tracking"
    );
}

#[test]
fn unseeded_resolution_still_honours_cwd_relative_imports() {
    // `local_main.yaml` imports `tests/data/local_target.yaml`, a CWD-relative
    // path. Seeding a source directory must not break that older style: the
    // source directory is the first base tried, not the only one.
    let (mut sv, path) = view_of("local_main.yaml");
    resolve_schemas_from(&mut sv, &path).unwrap();
    assert!(sv.get_unresolved_schemas().is_empty());
    assert!(sv.get_schema("http://example.com/local_target").is_some());

    // ...and the unseeded entry point behaves exactly as before.
    let (mut sv, _) = view_of("local_main.yaml");
    resolve_schemas(&mut sv).unwrap();
    assert!(sv.get_schema("http://example.com/local_target").is_some());
}

#[test]
fn reports_every_unresolvable_import_rather_than_only_the_first() {
    // Two bad imports in one schema: the error must mention both, since a pass
    // that returns on the first failure hides the rest of the work.
    let (mut sv, path) = view_of("missing_imports.yaml");
    let err = resolve_schemas_from(&mut sv, &path).unwrap_err();
    assert!(
        err.contains("definitely_not_here") && err.contains("also_not_here"),
        "both failures must be reported, got: {err}"
    );
}
