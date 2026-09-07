//! Prefix collisions across the schemas of one [`SchemaView`].
//!
//! Two schemas can bind the same prefix to different namespaces. A converter
//! can only expand a prefix one way, so one binding loses — and a CURIE using
//! it then expands to a plausible *wrong* IRI, with none of the loud failure an
//! unexpandable CURIE would give. These tests pin two things: the resolution is
//! a function of the schema data alone, and the loss is reported.
//!
//! On what the determinism assertions prove: they establish that the outcome
//! does not depend on the order the schemas are supplied in, and that it
//! matches the documented lexicographic rule. They cannot prove independence
//! from `HashMap`'s per-process random seed, because a single test process has
//! one seed — asserting the *specified* order directly is the honest
//! substitute. The guarantee rests on the build reading nothing out of a
//! `HashMap` in iteration order; see `converter_from_schemas_reporting`.

use linkml_schemaview::identifier::{
    converter_from_schemas, converter_from_schemas_reporting, PrefixCollision,
};
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

fn schema(name: &str) -> linkml_meta::SchemaDefinition {
    from_yaml(Path::new(&data_path(name))).unwrap()
}

/// `shared:` is bound to `.../aaa/` by one schema and `.../bbb/` by the other.
/// The smaller namespace keeps the prefix, whichever order they arrive in.
#[test]
fn same_prefix_two_namespaces_resolves_to_the_smallest_namespace() {
    let alpha = schema("prefix_collision_alpha.yaml");
    let beta = schema("prefix_collision_beta.yaml");

    for (label, schemas) in [
        ("alpha first", vec![&alpha, &beta]),
        ("beta first", vec![&beta, &alpha]),
    ] {
        let conv = converter_from_schemas(schemas);
        assert_eq!(
            conv.expand("shared:Thing").unwrap(),
            "https://example.com/aaa/Thing",
            "{label}: input order must not decide the winner"
        );
    }
}

/// Repeated construction inside one process. Weak on its own — one process has
/// one hash seed — but it does catch a build that carries state between calls
/// or that depends on some other mutable global.
#[test]
fn same_prefix_two_namespaces_resolves_the_same_way_every_time() {
    let alpha = schema("prefix_collision_alpha.yaml");
    let beta = schema("prefix_collision_beta.yaml");

    let mut seen = std::collections::BTreeSet::new();
    for _ in 0..64 {
        let (conv, collisions) = converter_from_schemas_reporting([&alpha, &beta]);
        seen.insert((conv.expand("shared:Thing").unwrap(), collisions));
    }
    assert_eq!(
        seen.len(),
        1,
        "converter build is not deterministic: {seen:?}"
    );
}

/// The losing binding is named, not swallowed.
#[test]
fn same_prefix_two_namespaces_is_reported() {
    let alpha = schema("prefix_collision_alpha.yaml");
    let beta = schema("prefix_collision_beta.yaml");

    let (_conv, collisions) = converter_from_schemas_reporting([&alpha, &beta]);
    assert_eq!(
        collisions,
        vec![PrefixCollision {
            prefix: "shared".to_string(),
            retained_namespace: "https://example.com/aaa/".to_string(),
            discarded_namespace: "https://example.com/bbb/".to_string(),
        }]
    );
    assert_eq!(
        collisions[0].to_string(),
        "prefix 'shared' expands to <https://example.com/aaa/>, not to <https://example.com/bbb/>"
    );
}

/// Losing `shared:` must not also cost `.../bbb/` its unambiguous `altbeta:`
/// prefix, nor `.../aaa/` its unambiguous `onlyalpha:` one.
#[test]
fn a_collision_does_not_discard_unrelated_prefixes() {
    let alpha = schema("prefix_collision_alpha.yaml");
    let beta = schema("prefix_collision_beta.yaml");

    let conv = converter_from_schemas([&alpha, &beta]);
    assert_eq!(
        conv.expand("altbeta:Thing").unwrap(),
        "https://example.com/bbb/Thing"
    );
    assert_eq!(
        conv.expand("onlyalpha:Thing").unwrap(),
        "https://example.com/only-alpha/Thing"
    );
}

/// Different prefixes for one namespace still merge into a single record with
/// `prefix_synonyms`. This is the case real schemas actually hit (`geo:` and
/// `geosparql:`, `era:` and `rinf:`) and it must keep working unchanged.
#[test]
fn two_prefixes_one_namespace_merge_as_synonyms() {
    let geo = schema("prefix_synonym_geo.yaml");
    let geosparql = schema("prefix_synonym_geosparql.yaml");

    for (label, schemas) in [
        ("geo first", vec![&geo, &geosparql]),
        ("geosparql first", vec![&geosparql, &geo]),
    ] {
        let (conv, collisions) = converter_from_schemas_reporting(schemas);
        assert!(
            collisions.is_empty(),
            "{label}: a shared namespace is not a collision, got {collisions:?}"
        );
        assert_eq!(
            conv.expand("geo:Geometry").unwrap(),
            "http://www.opengis.net/ont/geosparql#Geometry",
            "{label}"
        );
        assert_eq!(
            conv.expand("geosparql:Geometry").unwrap(),
            "http://www.opengis.net/ont/geosparql#Geometry",
            "{label}"
        );

        // One record, `geo` canonical (lexicographically smaller) and
        // `geosparql` a synonym — so `compress` has one answer, not two.
        let record = conv.find_by_prefix("geosparql").unwrap();
        assert_eq!(record.prefix, "geo", "{label}");
        assert!(record.prefix_synonyms.contains("geosparql"), "{label}");
        assert_eq!(
            conv.compress("http://www.opengis.net/ont/geosparql#Geometry")
                .unwrap(),
            "geo:Geometry",
            "{label}"
        );
    }
}

/// A single schema reports nothing and expands its own prefixes, and the
/// built-in `rdfs`/`rdf`/`dcterms` fallbacks are still added.
#[test]
fn single_schema_is_unchanged() {
    let alpha = schema("prefix_collision_alpha.yaml");

    let (conv, collisions) = converter_from_schemas_reporting([&alpha]);
    assert!(collisions.is_empty(), "got {collisions:?}");
    assert_eq!(
        conv.expand("shared:Thing").unwrap(),
        "https://example.com/aaa/Thing"
    );
    assert_eq!(
        conv.expand("rdfs:label").unwrap(),
        "http://www.w3.org/2000/01/rdf-schema#label"
    );
    assert_eq!(
        conv.expand("dcterms:title").unwrap(),
        "http://purl.org/dc/terms/title"
    );
    assert!(conv.find_by_prefix("rdf").is_ok());
}

/// A schema that declares `rdfs:` itself keeps its own binding: the fallbacks
/// are add-if-missing and must stay that way.
#[test]
fn declared_prefix_wins_over_the_builtin_fallback() {
    let mut alpha = schema("prefix_collision_alpha.yaml");
    if let Some(prefixes) = alpha.prefixes.as_mut() {
        prefixes.insert(
            "rdfs".to_string(),
            linkml_meta::Prefix {
                prefix_prefix: "rdfs".to_string(),
                prefix_reference: "https://example.com/not-rdfs/".to_string(),
            },
        );
    }

    let (conv, collisions) = converter_from_schemas_reporting([&alpha]);
    assert!(collisions.is_empty(), "got {collisions:?}");
    assert_eq!(
        conv.expand("rdfs:label").unwrap(),
        "https://example.com/not-rdfs/label"
    );
}

/// The `SchemaView` helper reaches the same report. The view holds its schemas
/// in a `HashMap`, so this is the caller whose iteration order was arbitrary.
#[test]
fn schema_view_reports_the_collision() {
    let alpha = schema("prefix_collision_alpha.yaml");
    let beta = schema("prefix_collision_beta.yaml");

    let mut sv = SchemaView::new();
    sv.add_schema(alpha).unwrap();
    sv.add_schema(beta).unwrap();

    let (conv, collisions) = sv.converter_with_collisions();
    assert_eq!(collisions.len(), 1, "got {collisions:?}");
    assert_eq!(collisions[0].prefix, "shared");
    assert_eq!(collisions[0].retained_namespace, "https://example.com/aaa/");
    assert_eq!(
        collisions[0].discarded_namespace,
        "https://example.com/bbb/"
    );
    assert_eq!(
        conv.expand("shared:Thing").unwrap(),
        "https://example.com/aaa/Thing"
    );

    // `converter()` must still hand back exactly that converter.
    assert_eq!(
        sv.converter().expand("shared:Thing").unwrap(),
        "https://example.com/aaa/Thing"
    );
}
