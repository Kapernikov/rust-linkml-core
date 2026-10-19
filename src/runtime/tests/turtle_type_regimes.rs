#![cfg(feature = "ttl")]

//! Type regimes in the writers: the flat writers keep the partitions their
//! regime names, the quad writers route each partition to its graph, and the
//! schema triples publish the matching subclass edges.

use linkml_meta::SchemaDefinition;
use linkml_runtime::schema_rdf::{
    schema_triples, SchemaRdfOptions, SchemaRdfProfile, TypePartition, TypeRegime, RDF_TYPE,
    SESAME_DIRECT_SUBCLASS_OF, SESAME_DIRECT_TYPE,
};
use linkml_runtime::turtle::{
    turtle_to_string, write_nquads, write_ntriples, write_trig, QuadOptions, TurtleOptions,
};
use linkml_runtime::{load_yaml_file, LinkMLInstance};
use linkml_schemaview::converter::Converter;
use linkml_schemaview::identifier::{converter_from_schemas, Identifier};
use linkml_schemaview::io::from_yaml;
use linkml_schemaview::schemaview::SchemaView;
use oxrdf::{GraphName, NamedNode};
use oxttl::{NQuadsParser, NTriplesParser, TriGParser};
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

const NS: &str = "https://example.com/type-regimes/";

fn data_path(name: &str) -> PathBuf {
    let mut p = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    p.push("tests");
    p.push("data");
    p.push(name);
    p
}

struct Fixture {
    sv: SchemaView,
    schema: SchemaDefinition,
    conv: Converter,
    instance: LinkMLInstance,
}

fn load(schema_file: &str, data_file: &str, class: &str) -> Fixture {
    let schema = from_yaml(Path::new(&data_path(schema_file))).unwrap();
    let types_schema = from_yaml(Path::new(&data_path("types.yaml"))).unwrap();
    let mut sv = SchemaView::new();
    sv.add_schema(schema.clone()).unwrap();
    sv.add_schema_with_import_ref(
        types_schema.clone(),
        Some((schema.id.clone(), "linkml:types".to_string())),
    )
    .unwrap();
    let conv = converter_from_schemas([&schema, &types_schema]);
    let cv = sv
        .get_class(&Identifier::new(class), &conv)
        .unwrap()
        .unwrap();
    let instance = load_yaml_file(Path::new(&data_path(data_file)), &sv, &cv, &conv)
        .unwrap()
        .into_instance()
        .unwrap();
    Fixture {
        sv,
        schema,
        conv,
        instance,
    }
}

fn fixture() -> Fixture {
    load(
        "type_regimes_schema.yaml",
        "type_regimes_data.yaml",
        "Registry",
    )
}

type Spo = (String, String, String);

fn flat(f: &Fixture, regime: TypeRegime, skolem: bool) -> BTreeSet<Spo> {
    let mut buf = Vec::new();
    write_ntriples(
        &f.instance,
        &f.sv,
        &f.schema,
        &f.conv,
        &mut buf,
        TurtleOptions::default()
            .with_skolem(skolem)
            .with_regime(regime),
    )
    .unwrap();
    NTriplesParser::new()
        .for_reader(buf.as_slice())
        .map(|t| {
            let t = t.unwrap();
            (
                t.subject.to_string(),
                t.predicate.to_string(),
                t.object.to_string(),
            )
        })
        .collect()
}

fn iri(local: &str) -> String {
    format!("<{NS}{local}>")
}

fn rdf_type() -> String {
    format!("<{RDF_TYPE}>")
}

fn direct_type() -> String {
    format!("<{SESAME_DIRECT_TYPE}>")
}

fn types_of(set: &BTreeSet<Spo>, subject: &str, predicate: &str) -> BTreeSet<String> {
    set.iter()
        .filter(|(s, p, _)| s == subject && p == predicate)
        .map(|(_, _, o)| o.clone())
        .collect()
}

fn names(list: &[&str]) -> BTreeSet<String> {
    list.iter().map(|n| iri(n)).collect()
}

/// Golden output of the writer before type regimes existed (skolem, sorted;
/// captured from the pre-change writer). `Asserted` must stay identical to it.
const ASSERTED_GOLDEN: &str = include_str!("data/type_regimes_asserted_golden.nt");

#[test]
fn asserted_is_the_pre_regime_output() {
    let f = fixture();
    let mut buf = Vec::new();
    write_ntriples(
        &f.instance,
        &f.sv,
        &f.schema,
        &f.conv,
        &mut buf,
        TurtleOptions::default().with_skolem(true),
    )
    .unwrap();
    let mut lines: Vec<String> = String::from_utf8(buf)
        .unwrap()
        .lines()
        .map(|l| l.to_string())
        .collect();
    lines.sort();
    assert_eq!(lines.join("\n"), ASSERTED_GOLDEN.trim_end());
}

#[test]
fn flat_writer_honours_the_regime() {
    let f = fixture();
    let s1 = "<https://example.com/type-regimes/s1>";
    let m1 = "<https://example.com/type-regimes/m1>";
    let asserted = flat(&f, TypeRegime::Asserted, true);
    let is_a = flat(&f, TypeRegime::IsA, true);
    let mixins = flat(&f, TypeRegime::IsAAndMixins, true);

    assert_eq!(types_of(&asserted, s1, &rdf_type()), names(&["Signal"]));
    assert!(types_of(&asserted, s1, &direct_type()).is_empty());

    assert_eq!(
        types_of(&is_a, s1, &rdf_type()),
        names(&["Signal", "Asset", "Thing"])
    );
    assert_eq!(types_of(&is_a, s1, &direct_type()), names(&["Signal"]));
    assert_eq!(
        types_of(&mixins, s1, &rdf_type()),
        names(&["Signal", "Asset", "Thing", "Located", "Placed"])
    );
    assert_eq!(types_of(&mixins, s1, &direct_type()), names(&["Signal"]));
    assert_eq!(
        types_of(&mixins, m1, &rdf_type()),
        names(&[
            "MainSignal",
            "Signal",
            "Asset",
            "Thing",
            "Located",
            "Placed"
        ])
    );
    assert_eq!(
        types_of(&mixins, m1, &direct_type()),
        names(&["MainSignal"])
    );

    // Inlined objects are typed too, and the type designator is never written.
    let spot = format!("<{NS}s1/location>");
    assert_eq!(
        types_of(&asserted, &spot, &rdf_type()),
        names(&["SpotLocation"])
    );
    assert_eq!(
        types_of(&is_a, &spot, &rdf_type()),
        names(&["SpotLocation", "Location"])
    );
    assert_eq!(
        types_of(&is_a, &spot, &direct_type()),
        names(&["SpotLocation"])
    );
    for set in [&asserted, &is_a, &mixins] {
        assert!(set.iter().all(|(_, p, _)| !p.ends_with("typeURI>")));
    }

    // Each regime only adds to the previous one.
    assert!(asserted.is_subset(&is_a));
    assert!(is_a.is_subset(&mixins));

    // A class that is its own mixin gets no extra type and no panic.
    let own = "<https://example.com/type-regimes/self1>";
    assert_eq!(
        types_of(&mixins, own, &rdf_type()),
        names(&["SelfMixin", "Thing"])
    );
}

#[test]
fn turtle_string_follows_the_regime() {
    let f = fixture();
    let ttl = turtle_to_string(
        &f.instance,
        &f.sv,
        &f.schema,
        &f.conv,
        TurtleOptions::default().with_regime(TypeRegime::IsAAndMixins),
    )
    .unwrap();
    assert!(ttl.contains("directType"), "{ttl}");
    assert!(ttl.contains("Placed"), "{ttl}");
}

fn graph(name: &str) -> NamedNode {
    NamedNode::new(format!("https://example.com/graphs/{name}")).unwrap()
}

type Quads = BTreeMap<String, BTreeSet<Spo>>;

fn group(quads: impl Iterator<Item = oxrdf::Quad>) -> Quads {
    let mut out = Quads::new();
    for q in quads {
        let g = match q.graph_name {
            GraphName::NamedNode(n) => n.into_string(),
            other => panic!("unexpected graph {other}"),
        };
        out.entry(g).or_default().insert((
            q.subject.to_string(),
            q.predicate.to_string(),
            q.object.to_string(),
        ));
    }
    out
}

fn nquads(f: &Fixture, options: &QuadOptions) -> Quads {
    let mut buf = Vec::new();
    write_nquads(&f.instance, &f.sv, &f.schema, &f.conv, &mut buf, options).unwrap();
    group(
        NQuadsParser::new()
            .for_reader(buf.as_slice())
            .map(|q| q.unwrap()),
    )
}

fn trig(f: &Fixture, options: &QuadOptions) -> Quads {
    let mut buf = Vec::new();
    write_trig(&f.instance, &f.sv, &f.schema, &f.conv, &mut buf, options).unwrap();
    group(
        TriGParser::new()
            .for_reader(buf.as_slice())
            .map(|q| q.unwrap()),
    )
}

fn mapping(parts: &[(TypePartition, &str)]) -> QuadOptions {
    QuadOptions::new(parts.iter().map(|(p, g)| (*p, graph(g))).collect()).with_skolem(true)
}

#[test]
fn quad_graph_equals_flat_output_of_its_partitions() {
    let f = fixture();
    for regime in TypeRegime::ALL {
        // All of the regime's partitions share one graph.
        let options = mapping(
            &regime
                .partitions()
                .iter()
                .map(|p| (*p, "g"))
                .collect::<Vec<_>>(),
        );
        let expected = flat(&f, regime, true);
        for quads in [nquads(&f, &options), trig(&f, &options)] {
            assert_eq!(quads.len(), 1);
            assert_eq!(quads[&graph("g").into_string()], expected, "{regime:?}");
        }
    }
}

#[test]
fn quad_partitions_go_to_separate_graphs() {
    let f = fixture();
    let options = mapping(&[
        (TypePartition::Own, "own"),
        (TypePartition::IsA, "isa"),
        (TypePartition::Mixin, "mixin"),
        (TypePartition::Direct, "direct"),
    ]);
    let all = flat(&f, TypeRegime::IsAAndMixins, true);
    for quads in [nquads(&f, &options), trig(&f, &options)] {
        let union: BTreeSet<Spo> = quads.values().flatten().cloned().collect();
        assert_eq!(union, all);
        let own = &quads[&graph("own").into_string()];
        assert_eq!(*own, flat(&f, TypeRegime::Asserted, true));
        assert!(quads[&graph("isa").into_string()]
            .iter()
            .all(|(_, p, _)| *p == rdf_type()));
        assert!(quads[&graph("direct").into_string()]
            .iter()
            .all(|(_, p, _)| *p == direct_type()));
        let mixin_objects: BTreeSet<String> = quads[&graph("mixin").into_string()]
            .iter()
            .map(|(_, _, o)| o.clone())
            .collect();
        assert_eq!(mixin_objects, names(&["Located", "Placed"]));
    }

    // Partitions missing from the map are not written; two may share a graph.
    let partial = mapping(&[(TypePartition::Own, "g"), (TypePartition::Direct, "g")]);
    let quads = nquads(&f, &partial);
    let mut expected = flat(&f, TypeRegime::Asserted, true);
    expected.extend(
        flat(&f, TypeRegime::IsA, true)
            .into_iter()
            .filter(|(_, p, _)| *p == direct_type()),
    );
    assert_eq!(quads[&graph("g").into_string()], expected);
}

#[test]
fn quad_blank_nodes_agree_across_graphs() {
    let f = fixture();
    let mut options = mapping(&[
        (TypePartition::Own, "own"),
        (TypePartition::Direct, "direct"),
    ]);
    options.skolem = false;
    let quads = nquads(&f, &options);
    let bnodes = |g: &str, subject_only: bool| -> BTreeSet<String> {
        quads[&graph(g).into_string()]
            .iter()
            .flat_map(|(s, _, o)| {
                let mut v = vec![s.clone()];
                if !subject_only {
                    v.push(o.clone());
                }
                v
            })
            .filter(|n| n.starts_with("_:"))
            .collect()
    };
    let direct = bnodes("direct", true);
    assert!(!direct.is_empty());
    assert!(direct.is_subset(&bnodes("own", false)));
}

#[test]
fn designates_type_on_rdf_type_is_rejected() {
    let f = load(
        "type_regimes_rdftype_designator_schema.yaml",
        "type_regimes_rdftype_designator_data.yaml",
        "Base",
    );
    let check = |err: std::io::Error| {
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
        let msg = err.to_string();
        assert!(msg.contains("Base") && msg.contains("kind"), "{msg}");
    };
    let mut buf = Vec::new();
    check(
        write_ntriples(
            &f.instance,
            &f.sv,
            &f.schema,
            &f.conv,
            &mut buf,
            TurtleOptions::default(),
        )
        .unwrap_err(),
    );
    check(
        turtle_to_string(
            &f.instance,
            &f.sv,
            &f.schema,
            &f.conv,
            TurtleOptions::default(),
        )
        .unwrap_err(),
    );
    let options = mapping(&[(TypePartition::Own, "g")]);
    check(write_nquads(&f.instance, &f.sv, &f.schema, &f.conv, &mut buf, &options).unwrap_err());
    check(write_trig(&f.instance, &f.sv, &f.schema, &f.conv, &mut buf, &options).unwrap_err());
}

fn schema_edges(f: &Fixture, regime: TypeRegime, predicate: &str) -> BTreeSet<(String, String)> {
    let triples = schema_triples(
        &f.sv,
        &SchemaRdfOptions::new(SchemaRdfProfile::Discovery).with_regime(regime),
    );
    triples
        .triples
        .iter()
        .filter(|t| t.predicate.as_str() == predicate && t.object.is_named_node())
        .map(|t| (t.subject.to_string(), t.object.to_string()))
        .collect()
}

fn edges(list: &[(&str, &str)]) -> BTreeSet<(String, String)> {
    list.iter().map(|(a, b)| (iri(a), iri(b))).collect()
}

#[test]
fn schema_triples_publish_the_regime_edges() {
    let f = fixture();
    let sub = "http://www.w3.org/2000/01/rdf-schema#subClassOf";

    // Asserted: direct is_a only, no directSubClassOf.
    let asserted = schema_edges(&f, TypeRegime::Asserted, sub);
    assert!(asserted.contains(&(iri("Signal"), iri("Asset"))));
    assert!(!asserted.contains(&(iri("MainSignal"), iri("Thing"))));
    assert!(schema_edges(&f, TypeRegime::Asserted, SESAME_DIRECT_SUBCLASS_OF).is_empty());

    // IsA: transitive closure, no mixin edges, no self edges.
    let closure = schema_edges(&f, TypeRegime::IsA, sub);
    for pair in [
        ("MainSignal", "Signal"),
        ("MainSignal", "Asset"),
        ("MainSignal", "Thing"),
        ("Signal", "Asset"),
        ("Signal", "Thing"),
        ("Asset", "Thing"),
        ("Located", "Placed"),
    ] {
        assert!(closure.contains(&(iri(pair.0), iri(pair.1))), "{pair:?}");
    }
    assert!(!closure.iter().any(|(_, o)| *o == iri("Located")));
    assert!(closure.iter().all(|(s, o)| s != o));
    assert!(!closure.contains(&(iri("SelfMixin"), iri("SelfMixin"))));

    // Direct edges: immediate superclass only.
    let direct = schema_edges(&f, TypeRegime::IsA, SESAME_DIRECT_SUBCLASS_OF);
    assert!(direct.is_superset(&edges(&[
        ("MainSignal", "Signal"),
        ("Signal", "Asset"),
        ("Asset", "Thing"),
        ("Located", "Placed"),
    ])));
    assert!(!direct.contains(&(iri("MainSignal"), iri("Asset"))));
    assert!(!direct.contains(&(iri("Signal"), iri("Thing"))));

    // IsAAndMixins: mixins are superclasses, and the nearer is_a/mixin edges
    // are the direct ones.
    let with_mixins = schema_edges(&f, TypeRegime::IsAAndMixins, sub);
    for pair in [
        ("Signal", "Located"),
        ("Signal", "Placed"),
        ("MainSignal", "Located"),
        ("MainSignal", "Placed"),
    ] {
        assert!(
            with_mixins.contains(&(iri(pair.0), iri(pair.1))),
            "{pair:?}"
        );
    }
    assert!(with_mixins.is_superset(&closure));
    assert!(with_mixins.iter().all(|(s, o)| s != o));
    let direct_mixins = schema_edges(&f, TypeRegime::IsAAndMixins, SESAME_DIRECT_SUBCLASS_OF);
    assert!(direct_mixins.contains(&(iri("Signal"), iri("Located"))));
    assert!(!direct_mixins.contains(&(iri("Signal"), iri("Placed"))));
    assert!(!direct_mixins.contains(&(iri("MainSignal"), iri("Located"))));
}
