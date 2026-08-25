#![cfg(feature = "ttl")]

//! Skolem IRIs must be a deterministic, collision-free function of the path
//! taken through the instance tree to reach a node.

use linkml_runtime::{
    load_yaml_file,
    turtle::{write_ntriples, TurtleOptions},
};
use linkml_schemaview::identifier::{converter_from_schemas, Identifier};
use linkml_schemaview::io::from_yaml;
use linkml_schemaview::schemaview::SchemaView;
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

const BASE: &str = "https://example.com/skolem-test/";

fn data_path(name: &str) -> PathBuf {
    let mut p = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    p.push("tests");
    p.push("data");
    p.push(name);
    p
}

/// Load the skolem fixture and serialise it to N-Triples with skolem IRIs on.
///
/// Everything is rebuilt from scratch on each call so that repeated calls
/// exercise fresh `HashMap`s — the instance maps are hashed with a per-instance
/// `RandomState`, so a serialiser whose output depends on map iteration order
/// gives a different answer on each call.
fn serialize_skolem() -> Vec<String> {
    let schema = from_yaml(Path::new(&data_path("skolem_schema.yaml"))).unwrap();
    let types_schema = from_yaml(Path::new(&data_path("types.yaml"))).unwrap();
    let mut sv = SchemaView::new();
    sv.add_schema(schema.clone()).unwrap();
    sv.add_schema_with_import_ref(
        types_schema.clone(),
        Some((schema.id.clone(), "linkml:types".to_string())),
    )
    .unwrap();
    let conv = converter_from_schemas([&schema, &types_schema]);
    let class = sv
        .get_class(&Identifier::new("Person"), &conv)
        .unwrap()
        .unwrap();
    let v = load_yaml_file(
        Path::new(&data_path("skolem_data.yaml")),
        &sv,
        &class,
        &conv,
    )
    .unwrap()
    .into_instance()
    .unwrap();
    let mut buf = Vec::new();
    write_ntriples(
        &v,
        &sv,
        &schema,
        &conv,
        &mut buf,
        TurtleOptions { skolem: true },
    )
    .unwrap();
    let nt = String::from_utf8(buf).unwrap();
    let mut lines: Vec<String> = nt.lines().map(|l| l.trim().to_string()).collect();
    lines.retain(|l| !l.is_empty());
    lines.sort();
    lines
}

/// Subject IRI of every triple whose object is the given literal.
fn subjects_with_literal(triples: &[String], literal: &str) -> Vec<String> {
    let needle = format!("\"{}\"", literal);
    triples
        .iter()
        .filter(|l| l.contains(&needle))
        .filter_map(|l| l.split('>').next())
        .map(|s| s.trim_start_matches('<').to_string())
        .collect()
}

/// Subject IRI of every `rdf:type <BASE + class_name>` triple.
fn subjects_of_type(triples: &[String], class_name: &str) -> Vec<String> {
    let needle = format!("<{}{}> .", BASE, class_name);
    triples
        .iter()
        .filter(|l| l.contains("22-rdf-syntax-ns#type") && l.ends_with(&needle))
        .filter_map(|l| l.split('>').next())
        .map(|s| s.trim_start_matches('<').to_string())
        .collect()
}

#[test]
fn skolem_iris_do_not_collide_across_slots() {
    let triples = serialize_skolem();

    // One Address hangs off home_addresses and one off work_addresses. Both are
    // keyless and both sit at index 0 of their own list, so an IRI built from
    // the index alone gives them the same name and merges them into one node.
    let addresses = subjects_of_type(&triples, "Address");
    let distinct: BTreeSet<&String> = addresses.iter().collect();
    assert_eq!(
        distinct.len(),
        2,
        "the two addresses share a skolem IRI: {:?}\n{}",
        addresses,
        triples.join("\n")
    );

    // The same for the two single-valued Contact slots.
    let contacts = subjects_of_type(&triples, "Contact");
    let distinct: BTreeSet<&String> = contacts.iter().collect();
    assert_eq!(
        distinct.len(),
        2,
        "the two contacts share a skolem IRI: {:?}\n{}",
        contacts,
        triples.join("\n")
    );

    // A collision is visible from the data side too: the merged subject ends up
    // carrying both street values.
    let home = subjects_with_literal(&triples, "home street");
    let work = subjects_with_literal(&triples, "work street");
    assert_eq!(home.len(), 1, "{}", triples.join("\n"));
    assert_eq!(work.len(), 1, "{}", triples.join("\n"));
    assert_ne!(
        home[0],
        work[0],
        "home and work address collapsed onto one subject\n{}",
        triples.join("\n")
    );
}

#[test]
fn skolem_iris_encode_the_slot_they_hang_off() {
    let triples = serialize_skolem();
    let mut found: BTreeSet<String> = BTreeSet::new();
    found.extend(subjects_of_type(&triples, "Address"));
    found.extend(subjects_of_type(&triples, "Contact"));

    let expected: BTreeSet<String> = [
        "p1/home_addresses/0",
        "p1/work_addresses/0",
        "p1/primary_contact",
        "p1/backup_contact",
    ]
    .iter()
    .map(|s| format!("{}{}", BASE, s))
    .collect();

    assert_eq!(found, expected, "\n{}", triples.join("\n"));
}

#[test]
fn skolem_iris_are_keyed_by_key_not_index() {
    let triples = serialize_skolem();
    let found: BTreeSet<String> = subjects_of_type(&triples, "Account").into_iter().collect();

    // Slot names and key values are snake_case far more often than not, and
    // `_` is unreserved in RFC 3986 — it must survive into the path. Characters
    // that genuinely cannot appear in a path segment still get escaped.
    let expected: BTreeSet<String> = [
        "p1/accounts/savings_2024",
        "p1/accounts/odd%20label%2Fwith%20slash",
    ]
    .iter()
    .map(|s| format!("{}{}", BASE, s))
    .collect();

    assert_eq!(found, expected, "\n{}", triples.join("\n"));
}

#[test]
fn skolem_iris_are_stable_across_serializations() {
    // A counter walked in hash-map order hands the same node a different IRI on
    // each run: the set of IRIs stays {gen1, gen2} but which contact gets which
    // flips, so the triple set as a whole changes. A path-derived IRI does not.
    let first = serialize_skolem();
    for i in 1..8 {
        assert_eq!(
            serialize_skolem(),
            first,
            "skolem IRIs changed on repetition {}",
            i
        );
    }
}
