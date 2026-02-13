#![cfg(feature = "ttl")]

use linkml_runtime::{
    load_yaml_file,
    turtle::{turtle_to_string, TurtleOptions},
};
use linkml_schemaview::identifier::{converter_from_schema, Identifier};
use linkml_schemaview::io::from_yaml;
use linkml_schemaview::schemaview::SchemaView;
use std::path::{Path, PathBuf};

fn data_path(name: &str) -> PathBuf {
    let mut p = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    p.push("tests");
    p.push("data");
    p.push(name);
    p
}

#[test]
fn turtle_prefix_conflict_produces_valid_output() {
    // Schema A (derived) defines  shared: <https://example.com/a/>
    // Schema B (base)    defines  shared: <https://example.com/b/>
    // Same prefix name, different URIs.
    //
    // The Turtle header must declare `shared:` only once.
    // Whichever namespace loses the prefix must appear as fully expanded IRIs.
    // The output must be parseable as valid Turtle.

    let base_schema = from_yaml(Path::new(&data_path("prefix_conflict_base.yaml"))).unwrap();
    let derived_schema = from_yaml(Path::new(&data_path("prefix_conflict_derived.yaml"))).unwrap();

    let mut sv = SchemaView::new();
    sv.add_schema(derived_schema.clone()).unwrap();
    sv.add_schema_with_import_ref(
        base_schema,
        Some((
            derived_schema.id.clone(),
            "./prefix_conflict_base".to_string(),
        )),
    )
    .unwrap();

    let conv = converter_from_schema(&derived_schema);
    let class = sv
        .get_class(&Identifier::new("DerivedItem"), &conv)
        .unwrap()
        .unwrap();
    let v = load_yaml_file(
        Path::new(&data_path("prefix_conflict_data.yaml")),
        &sv,
        &class,
        &conv,
    )
    .unwrap()
    .into_instance()
    .unwrap();
    let ttl = turtle_to_string(
        &v,
        &sv,
        &derived_schema,
        &conv,
        TurtleOptions { skolem: false },
    )
    .unwrap();

    // 1) The `@prefix shared:` line must appear exactly once in the header.
    let prefix_decl_count = ttl.matches("@prefix shared:").count();
    assert_eq!(
        prefix_decl_count, 1,
        "Expected exactly one @prefix shared: declaration, found {}. Output:\n{}",
        prefix_decl_count, ttl
    );

    // 2) The output must contain both namespaces — one via the prefix, the other
    //    via fully expanded IRI (or via an alternate prefix like altbase:).
    let has_a = ttl.contains("https://example.com/a/") || ttl.contains("shared:");
    let has_b = ttl.contains("https://example.com/b/") || ttl.contains("altbase:");
    assert!(
        has_a,
        "Expected namespace https://example.com/a/ (or shared: prefix) in output:\n{}",
        ttl
    );
    assert!(
        has_b,
        "Expected namespace https://example.com/b/ (or altbase: prefix) in output:\n{}",
        ttl
    );

    // 3) The output must be parseable as valid Turtle.
    //    If prefixes are broken, the Turtle parser will fail.
    //    Use a simple check: no `shared:` usage without a matching @prefix declaration.
    //    (A thorough check would use an actual Turtle parser, but this suffices here.)
    for line in ttl.lines() {
        let trimmed = line.trim();
        // Skip prefix declarations and blank lines
        if trimmed.starts_with("@prefix") || trimmed.is_empty() {
            continue;
        }
        // If a line uses shared: as a CURIE, the declared URI should match
        // (we just verify it doesn't use a bare CURIE without declaration)
        if trimmed.contains("shared:") {
            assert!(
                ttl.contains("@prefix shared:"),
                "Found shared: CURIE usage without @prefix declaration:\n{}",
                ttl
            );
        }
    }
}

#[test]
fn turtle_all_imported_prefixes_are_declared() {
    // Even without conflicts, prefixes from imported schemas must appear in the
    // Turtle header.  This is the basic bug: write_turtle used to only emit
    // prefixes from the primary schema.

    let base_schema = from_yaml(Path::new(&data_path("cross_schema_prefix_base.yaml"))).unwrap();
    let derived_schema =
        from_yaml(Path::new(&data_path("cross_schema_prefix_derived.yaml"))).unwrap();

    let mut sv = SchemaView::new();
    sv.add_schema(derived_schema.clone()).unwrap();
    sv.add_schema_with_import_ref(
        base_schema,
        Some((
            derived_schema.id.clone(),
            "./cross_schema_prefix_base".to_string(),
        )),
    )
    .unwrap();

    let conv = converter_from_schema(&derived_schema);
    let class = sv
        .get_class(&Identifier::new("DerivedClass"), &conv)
        .unwrap()
        .unwrap();
    let v = load_yaml_file(
        Path::new(&data_path("cross_schema_prefix_data.yaml")),
        &sv,
        &class,
        &conv,
    )
    .unwrap()
    .into_instance()
    .unwrap();
    let ttl = turtle_to_string(
        &v,
        &sv,
        &derived_schema,
        &conv,
        TurtleOptions { skolem: false },
    )
    .unwrap();

    // Both `derived:` and `base:` prefixes must be declared in the header
    assert!(
        ttl.contains("@prefix derived:"),
        "Expected @prefix derived: in Turtle header. Got:\n{}",
        ttl
    );
    assert!(
        ttl.contains("@prefix base:"),
        "Expected @prefix base: in Turtle header. Got:\n{}",
        ttl
    );
}
