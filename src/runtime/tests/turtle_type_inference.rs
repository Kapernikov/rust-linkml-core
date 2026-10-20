#![cfg(feature = "ttl")]

//! Type inference in the writers: the flat writers keep the partitions their
//! inference names, the quad writers route each partition to its graph, and the
//! schema triples publish the matching subclass edges.

use linkml_meta::SchemaDefinition;
use linkml_runtime::rdf_export::{
    export_ntriples_many, export_turtle_many, RdfOutputFormat, RdfStreamWriter,
};
use linkml_runtime::schema_rdf::{
    schema_triples, SchemaRdfOptions, SchemaRdfProfile, TypeInference, TypePartition, RDF_TYPE,
    SESAME_DIRECT_SUBCLASS_OF, SESAME_DIRECT_TYPE,
};
use linkml_runtime::turtle::{turtle_to_string, write_ntriples, write_turtle, WriteOptions};
use linkml_runtime::{load_yaml_file, LinkMLInstance};
use linkml_schemaview::converter::Converter;
use linkml_schemaview::identifier::{converter_from_schemas, Identifier};
use linkml_schemaview::io::from_yaml;
use linkml_schemaview::schemaview::SchemaView;
use oxrdf::{GraphName, NamedNode};
use oxttl::{NQuadsParser, NTriplesParser, TriGParser, TurtleParser};
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

/// Write one instance as `format` through a `RdfStreamWriter`.
fn write_via_stream<W: std::io::Write>(
    format: RdfOutputFormat,
    value: &LinkMLInstance,
    sv: &SchemaView,
    schema: &SchemaDefinition,
    conv: &Converter,
    w: &mut W,
    options: &WriteOptions,
) -> std::io::Result<()> {
    let mut stream = RdfStreamWriter::new(w, format, sv, schema, conv, options)?;
    stream.write(value)?;
    stream.finish()?;
    Ok(())
}

fn write_nquads<W: std::io::Write>(
    value: &LinkMLInstance,
    sv: &SchemaView,
    schema: &SchemaDefinition,
    conv: &Converter,
    w: &mut W,
    options: &WriteOptions,
) -> std::io::Result<()> {
    write_via_stream(RdfOutputFormat::NQuads, value, sv, schema, conv, w, options)
}

fn write_trig<W: std::io::Write>(
    value: &LinkMLInstance,
    sv: &SchemaView,
    schema: &SchemaDefinition,
    conv: &Converter,
    w: &mut W,
    options: &WriteOptions,
) -> std::io::Result<()> {
    write_via_stream(RdfOutputFormat::TriG, value, sv, schema, conv, w, options)
}

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

fn flat(f: &Fixture, inference: TypeInference, skolem: bool) -> BTreeSet<Spo> {
    let mut buf = Vec::new();
    write_ntriples(
        &f.instance,
        &f.sv,
        &f.schema,
        &f.conv,
        &mut buf,
        &WriteOptions::flat(inference).with_skolem(skolem),
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

/// Golden output of the writer before type inference existed (skolem, sorted;
/// captured from the pre-change writer). `Asserted` must stay identical to it.
const ASSERTED_GOLDEN: &str = include_str!("data/type_regimes_asserted_golden.nt");

#[test]
fn asserted_is_the_pre_inference_output() {
    let f = fixture();
    let mut buf = Vec::new();
    write_ntriples(
        &f.instance,
        &f.sv,
        &f.schema,
        &f.conv,
        &mut buf,
        &WriteOptions::default().with_skolem(true),
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
fn flat_writer_honours_the_inference() {
    let f = fixture();
    let s1 = "<https://example.com/type-regimes/s1>";
    let m1 = "<https://example.com/type-regimes/m1>";
    let asserted = flat(&f, TypeInference::ASSERTED, true);
    let is_a = flat(&f, TypeInference::IS_A, true);
    let mixins = flat(&f, TypeInference::IS_A_AND_MIXINS, true);

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

    // Each inference only adds to the previous one.
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
fn turtle_string_follows_the_inference() {
    let f = fixture();
    let ttl = turtle_to_string(
        &f.instance,
        &f.sv,
        &f.schema,
        &f.conv,
        &WriteOptions::flat(TypeInference::IS_A_AND_MIXINS),
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

fn nquads(f: &Fixture, options: &WriteOptions) -> Quads {
    let mut buf = Vec::new();
    write_nquads(&f.instance, &f.sv, &f.schema, &f.conv, &mut buf, options).unwrap();
    group(
        NQuadsParser::new()
            .for_reader(buf.as_slice())
            .map(|q| q.unwrap()),
    )
}

fn trig(f: &Fixture, options: &WriteOptions) -> Quads {
    let mut buf = Vec::new();
    write_trig(&f.instance, &f.sv, &f.schema, &f.conv, &mut buf, options).unwrap();
    group(
        TriGParser::new()
            .for_reader(buf.as_slice())
            .map(|q| q.unwrap()),
    )
}

fn mapping(parts: &[(TypePartition, &str)]) -> WriteOptions {
    WriteOptions::quads(
        parts
            .iter()
            .map(|(p, g)| (*p, GraphName::NamedNode(graph(g))))
            .collect(),
    )
    .with_skolem(true)
}

#[test]
fn quad_graph_equals_flat_output_of_its_partitions() {
    let f = fixture();
    for &inference in TypeInference::NAMED {
        // All of the inference's partitions share one graph.
        let options = mapping(&inference.partitions().map(|p| (p, "g")).collect::<Vec<_>>());
        let expected = flat(&f, inference, true);
        for quads in [nquads(&f, &options), trig(&f, &options)] {
            assert_eq!(quads.len(), 1);
            assert_eq!(quads[&graph("g").into_string()], expected, "{inference:?}");
        }
    }
}

#[test]
fn quad_partitions_go_to_separate_graphs() {
    let f = fixture();
    let options = mapping(&[
        (TypePartition::Own, "own"),
        (TypePartition::IsA, "is_a"),
        (TypePartition::Mixin, "mixin"),
        (TypePartition::Direct, "direct"),
    ]);
    let all = flat(&f, TypeInference::IS_A_AND_MIXINS, true);
    for quads in [nquads(&f, &options), trig(&f, &options)] {
        let union: BTreeSet<Spo> = quads.values().flatten().cloned().collect();
        assert_eq!(union, all);
        let own = &quads[&graph("own").into_string()];
        assert_eq!(*own, flat(&f, TypeInference::ASSERTED, true));
        assert!(quads[&graph("is_a").into_string()]
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
    let mut expected = flat(&f, TypeInference::ASSERTED, true);
    expected.extend(
        flat(&f, TypeInference::IS_A, true)
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
            &WriteOptions::default(),
        )
        .unwrap_err(),
    );
    check(
        turtle_to_string(
            &f.instance,
            &f.sv,
            &f.schema,
            &f.conv,
            &WriteOptions::default(),
        )
        .unwrap_err(),
    );
    let options = mapping(&[(TypePartition::Own, "g")]);
    check(write_nquads(&f.instance, &f.sv, &f.schema, &f.conv, &mut buf, &options).unwrap_err());
    check(write_trig(&f.instance, &f.sv, &f.schema, &f.conv, &mut buf, &options).unwrap_err());
}

fn schema_edges(
    f: &Fixture,
    inference: TypeInference,
    predicate: &str,
) -> BTreeSet<(String, String)> {
    let triples = schema_triples(
        &f.sv,
        &SchemaRdfOptions::new(SchemaRdfProfile::Discovery).with_inference(inference),
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
fn schema_triples_publish_the_inference_edges() {
    let f = fixture();
    let sub = "http://www.w3.org/2000/01/rdf-schema#subClassOf";

    // Asserted: direct is_a only, no directSubClassOf.
    let asserted = schema_edges(&f, TypeInference::ASSERTED, sub);
    assert!(asserted.contains(&(iri("Signal"), iri("Asset"))));
    assert!(!asserted.contains(&(iri("MainSignal"), iri("Thing"))));
    assert!(schema_edges(&f, TypeInference::ASSERTED, SESAME_DIRECT_SUBCLASS_OF).is_empty());

    // IsA: transitive closure, no mixin edges, no self edges.
    let closure = schema_edges(&f, TypeInference::IS_A, sub);
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
    let direct = schema_edges(&f, TypeInference::IS_A, SESAME_DIRECT_SUBCLASS_OF);
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
    let with_mixins = schema_edges(&f, TypeInference::IS_A_AND_MIXINS, sub);
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
    let direct_mixins = schema_edges(
        &f,
        TypeInference::IS_A_AND_MIXINS,
        SESAME_DIRECT_SUBCLASS_OF,
    );
    assert!(direct_mixins.contains(&(iri("Signal"), iri("Located"))));
    assert!(!direct_mixins.contains(&(iri("Signal"), iri("Placed"))));
    assert!(!direct_mixins.contains(&(iri("MainSignal"), iri("Located"))));
}

/// OWL restriction blank nodes hang off their class with `rdfs:subClassOf`.
/// The inferences only change the edges between *named* classes: every
/// restriction triple is the same in every inference (none lost, none
/// duplicated), restrictions are never closed over as if they were classes
/// (a class does not inherit its ancestors' restriction nodes — it already
/// carries its own for every induced slot, with `slot_usage` applied), and a
/// restriction is never a `sesame:directSubClassOf` object.
#[test]
fn schema_inferences_leave_owl_restrictions_alone() {
    let f = fixture();
    let sub = "http://www.w3.org/2000/01/rdf-schema#subClassOf";
    let non_class_edges = |inference: TypeInference| -> BTreeSet<String> {
        schema_triples(
            &f.sv,
            &SchemaRdfOptions::new(SchemaRdfProfile::Discovery).with_inference(inference),
        )
        .triples
        .iter()
        .filter(|t| {
            let class_edge = (t.predicate.as_str() == sub
                || t.predicate.as_str() == SESAME_DIRECT_SUBCLASS_OF)
                && t.object.is_named_node();
            !class_edge
        })
        .map(|t| t.to_string())
        .collect()
    };
    let asserted = non_class_edges(TypeInference::ASSERTED);
    let restriction_edges: Vec<&String> = asserted
        .iter()
        .filter(|t| t.contains(&format!("<{sub}> _:")))
        .collect();
    assert!(
        !restriction_edges.is_empty(),
        "fixture must carry OWL restrictions for this test to mean anything"
    );
    for inference in [TypeInference::IS_A, TypeInference::IS_A_AND_MIXINS] {
        assert_eq!(
            non_class_edges(inference),
            asserted,
            "{inference:?}: restriction / non-class triples differ from Asserted"
        );
        let all = schema_triples(
            &f.sv,
            &SchemaRdfOptions::new(SchemaRdfProfile::Discovery).with_inference(inference),
        );
        for t in &all.triples {
            let p = t.predicate.as_str();
            if p == sub || p == SESAME_DIRECT_SUBCLASS_OF {
                assert!(
                    t.subject.is_named_node(),
                    "{inference:?}: a restriction node was treated as a class: {t}"
                );
            }
            if p == SESAME_DIRECT_SUBCLASS_OF {
                assert!(
                    t.object.is_named_node(),
                    "{inference:?}: a restriction is a direct superclass: {t}"
                );
            }
        }
    }
}

/// Distinct blank nodes (as subject or object) in a set of terms.
fn blank_nodes<'a>(terms: impl Iterator<Item = &'a String>) -> BTreeSet<String> {
    terms.filter(|t| t.starts_with("_:")).cloned().collect()
}

#[test]
fn many_exporters_label_blank_nodes_uniquely_across_instances() {
    let f = fixture();
    let one = flat(&f, TypeInference::ASSERTED, false);
    let per_instance = blank_nodes(one.iter().flat_map(|(s, _, o)| [s, o])).len();
    assert!(per_instance > 0);
    let two = || vec![f.instance.clone(), f.instance.clone()];
    let options = WriteOptions::default().with_skolem(false);
    let quad_options = mapping(&[
        (TypePartition::Own, "own"),
        (TypePartition::Direct, "direct"),
    ])
    .with_skolem(false);

    let mut ttl = Vec::new();
    export_turtle_many(two(), &f.sv, &f.schema, &f.conv, &mut ttl, &options).unwrap();
    let triples: Vec<Spo> = TurtleParser::new()
        .for_reader(ttl.as_slice())
        .map(|t| t.unwrap())
        .map(|t| {
            (
                t.subject.to_string(),
                t.predicate.to_string(),
                t.object.to_string(),
            )
        })
        .collect();
    let mut nt = Vec::new();
    export_ntriples_many(two(), &f.sv, &f.schema, &f.conv, &mut nt, &options).unwrap();
    let nt_triples: Vec<Spo> = NTriplesParser::new()
        .for_reader(nt.as_slice())
        .map(|t| t.unwrap())
        .map(|t| {
            (
                t.subject.to_string(),
                t.predicate.to_string(),
                t.object.to_string(),
            )
        })
        .collect();
    for (name, t) in [("turtle", triples), ("ntriples", nt_triples)] {
        let n = blank_nodes(t.iter().flat_map(|(s, _, o)| [s, o])).len();
        assert_eq!(n, 2 * per_instance, "{name}: blank nodes merged");
    }

    let mut nq = Vec::new();
    let mut stream = RdfStreamWriter::new(
        &mut nq,
        RdfOutputFormat::NQuads,
        &f.sv,
        &f.schema,
        &f.conv,
        &quad_options,
    )
    .unwrap();
    for instance in two() {
        stream.write(&instance).unwrap();
    }
    stream.finish().unwrap();
    let mut tg = Vec::new();
    let mut stream = RdfStreamWriter::new(
        &mut tg,
        RdfOutputFormat::TriG,
        &f.sv,
        &f.schema,
        &f.conv,
        &quad_options,
    )
    .unwrap();
    for instance in two() {
        stream.write(&instance).unwrap();
    }
    stream.finish().unwrap();
    for (name, quads) in [
        (
            "nquads",
            group(
                NQuadsParser::new()
                    .for_reader(nq.as_slice())
                    .map(|q| q.unwrap()),
            ),
        ),
        (
            "trig",
            group(
                TriGParser::new()
                    .for_reader(tg.as_slice())
                    .map(|q| q.unwrap()),
            ),
        ),
    ] {
        let own = &quads[&graph("own").into_string()];
        let n = blank_nodes(own.iter().flat_map(|(s, _, o)| [s, o])).len();
        assert_eq!(n, 2 * per_instance, "{name}: blank nodes merged");
    }
}

#[test]
fn schema_edges_follow_partition_membership() {
    let f = fixture();
    let sub = "http://www.w3.org/2000/01/rdf-schema#subClassOf";
    let no_direct: TypeInference = [TypePartition::Own, TypePartition::IsA]
        .into_iter()
        .collect();
    let closure = schema_edges(&f, no_direct, sub);
    assert_eq!(closure, schema_edges(&f, TypeInference::IS_A, sub));
    assert!(schema_edges(&f, no_direct, SESAME_DIRECT_SUBCLASS_OF).is_empty());
    // A set without Own still publishes the asserted hierarchy.
    let mixin_only = TypeInference::from(TypePartition::Mixin);
    let edges_m = schema_edges(&f, mixin_only, sub);
    assert!(edges_m.contains(&(iri("Signal"), iri("Asset"))));
    assert!(edges_m.contains(&(iri("Signal"), iri("Located"))));
    assert!(!edges_m.contains(&(iri("MainSignal"), iri("Thing"))));
}

#[test]
fn write_options_are_validated_per_writer() {
    let f = fixture();
    let named = mapping(&[(TypePartition::Own, "g")]);
    let empty = WriteOptions::quads(BTreeMap::new());
    let mut buf = Vec::new();
    let invalid = |r: std::io::Result<()>| {
        assert_eq!(r.unwrap_err().kind(), std::io::ErrorKind::InvalidInput);
    };
    // Every writer refuses an empty map.
    invalid(write_turtle(
        &f.instance,
        &f.sv,
        &f.schema,
        &f.conv,
        &mut buf,
        &empty,
    ));
    invalid(write_ntriples(
        &f.instance,
        &f.sv,
        &f.schema,
        &f.conv,
        &mut buf,
        &empty,
    ));
    invalid(write_nquads(
        &f.instance,
        &f.sv,
        &f.schema,
        &f.conv,
        &mut buf,
        &empty,
    ));
    invalid(write_trig(
        &f.instance,
        &f.sv,
        &f.schema,
        &f.conv,
        &mut buf,
        &empty,
    ));
    invalid(export_turtle_many(
        Vec::new(),
        &f.sv,
        &f.schema,
        &f.conv,
        &mut buf,
        &empty,
    ));
    assert!(turtle_to_string(
        &f.instance,
        &f.sv,
        &f.schema,
        &f.conv,
        &WriteOptions::flat(TypeInference::empty())
    )
    .is_err());
    // The flat writers refuse a named graph; the quad writers take it.
    invalid(write_turtle(
        &f.instance,
        &f.sv,
        &f.schema,
        &f.conv,
        &mut buf,
        &named,
    ));
    invalid(write_ntriples(
        &f.instance,
        &f.sv,
        &f.schema,
        &f.conv,
        &mut buf,
        &named,
    ));
    assert!(buf.is_empty());
    write_nquads(&f.instance, &f.sv, &f.schema, &f.conv, &mut buf, &named).unwrap();
    write_trig(&f.instance, &f.sv, &f.schema, &f.conv, &mut buf, &named).unwrap();
}

#[test]
fn quad_writers_can_route_a_partition_to_the_default_graph() {
    let f = fixture();
    let mut options = mapping(&[(TypePartition::Direct, "direct")]);
    options
        .partition_graphs
        .insert(TypePartition::Own, GraphName::DefaultGraph);
    let mut nq = Vec::new();
    write_nquads(&f.instance, &f.sv, &f.schema, &f.conv, &mut nq, &options).unwrap();
    let mut tg = Vec::new();
    write_trig(&f.instance, &f.sv, &f.schema, &f.conv, &mut tg, &options).unwrap();
    let nq: Vec<oxrdf::Quad> = NQuadsParser::new()
        .for_reader(nq.as_slice())
        .map(|q| q.unwrap())
        .collect();
    let tg: Vec<oxrdf::Quad> = TriGParser::new()
        .for_reader(tg.as_slice())
        .map(|q| q.unwrap())
        .collect();
    for quads in [nq, tg] {
        let own = flat(&f, TypeInference::ASSERTED, true);
        let (default, named): (Vec<_>, Vec<_>) = quads
            .iter()
            .partition(|q| q.graph_name == GraphName::DefaultGraph);
        assert_eq!(default.len(), own.len());
        assert!(named
            .iter()
            .all(|q| q.predicate.as_str() == SESAME_DIRECT_TYPE
                && q.graph_name == GraphName::NamedNode(graph("direct"))));
        assert!(!named.is_empty());
    }
}

#[test]
fn trig_declares_the_same_prefixes_as_turtle() {
    let f = fixture();
    let header = |text: &str| -> Vec<String> {
        text.lines()
            .take_while(|l| l.starts_with("@prefix"))
            .map(str::to_string)
            .collect()
    };
    let ttl = turtle_to_string(
        &f.instance,
        &f.sv,
        &f.schema,
        &f.conv,
        &WriteOptions::default(),
    )
    .unwrap();
    let mut buf = Vec::new();
    write_trig(
        &f.instance,
        &f.sv,
        &f.schema,
        &f.conv,
        &mut buf,
        &WriteOptions::quads([(TypePartition::Own, GraphName::DefaultGraph)].into()),
    )
    .unwrap();
    let trig = String::from_utf8(buf).unwrap();
    assert!(!header(&ttl).is_empty());
    assert_eq!(header(&trig), header(&ttl));
}

#[test]
fn type_partitions_does_not_list_direct() {
    let f = fixture();
    let cv =
        f.sv.get_class(&Identifier::new("Signal"), &f.conv)
            .unwrap()
            .unwrap();
    let parts = linkml_runtime::schema_rdf::type_partitions(&cv).unwrap();
    assert_eq!(parts[0].0, TypePartition::Own);
    assert!(parts.iter().all(|(p, _)| *p != TypePartition::Direct));
    // The writer still emits the directType when the options route it.
    let direct = flat(&f, TypeInference::from(TypePartition::Direct), true);
    assert!(!direct.is_empty());
    assert!(direct.iter().all(|(_, p, _)| *p == direct_type()));
}

/// Compacting IRIs to CURIEs must never touch a string literal that merely
/// looks like an IRI: the writers compact through the serializer, not by
/// rewriting the serialized text.
#[test]
fn iri_lookalikes_inside_literals_survive_turtle_and_trig() {
    let text = "see <https://example.com/type-regimes/x> and <https://example.com/type-regimes/s1>";
    // Quotes escaped inside the literal, an IRI after them.
    let quoted = r#"say "hi" then <https://example.com/type-regimes/s2> and "<https://example.com/type-regimes/x>""#;
    let quoted_escaped = quoted.replace('"', "\\\"");
    let f = load(
        "type_regimes_schema.yaml",
        "type_regimes_literal_iri_data.yaml",
        "Registry",
    );
    let literals = |quads: Vec<oxrdf::Quad>| -> Vec<String> {
        quads
            .into_iter()
            .filter_map(|q| match q.object {
                oxrdf::Term::Literal(l) => Some(l.value().to_string()),
                _ => None,
            })
            .collect()
    };

    let ttl = turtle_to_string(
        &f.instance,
        &f.sv,
        &f.schema,
        &f.conv,
        &WriteOptions::default(),
    )
    .unwrap();
    assert!(ttl.contains(&format!("\"{text}\"")), "turtle:\n{ttl}");
    assert!(ttl.contains(&quoted_escaped), "turtle:\n{ttl}");
    let parsed = TurtleParser::new()
        .for_reader(ttl.as_bytes())
        .map(|t| t.unwrap().in_graph(GraphName::DefaultGraph))
        .collect();
    assert!(literals(parsed).iter().any(|v| v == text));
    let parsed = TurtleParser::new()
        .for_reader(ttl.as_bytes())
        .map(|t| t.unwrap().in_graph(GraphName::DefaultGraph))
        .collect();
    assert!(literals(parsed).iter().any(|v| v == quoted));

    let mut buf = Vec::new();
    write_trig(
        &f.instance,
        &f.sv,
        &f.schema,
        &f.conv,
        &mut buf,
        &mapping(&[(TypePartition::Own, "own")]),
    )
    .unwrap();
    let trig_text = String::from_utf8(buf).unwrap();
    assert!(
        trig_text.contains(&format!("\"{text}\"")),
        "trig:\n{trig_text}"
    );
    let parsed = TriGParser::new()
        .for_reader(trig_text.as_bytes())
        .map(|q| q.unwrap())
        .collect();
    assert!(literals(parsed).iter().any(|v| v == text));
    assert!(trig_text.contains(&quoted_escaped), "trig:\n{trig_text}");
}

/// Two instances written one `write` at a time through one `RdfStreamWriter`
/// never share a blank node label, in any format, and the options are checked
/// when the writer is built, not at the first write.
#[test]
fn stream_writer_keeps_blank_nodes_distinct_in_every_format() {
    let f = fixture();
    let per_instance = {
        let one = flat(&f, TypeInference::ASSERTED, false);
        blank_nodes(one.iter().flat_map(|(s, _, o)| [s, o])).len()
    };
    assert!(per_instance > 0);
    let flat_options = WriteOptions::default().with_skolem(false);
    let quad_options = mapping(&[(TypePartition::Own, "own")]).with_skolem(false);
    for (format, options) in [
        (RdfOutputFormat::Turtle, &flat_options),
        (RdfOutputFormat::NTriples, &flat_options),
        (RdfOutputFormat::NQuads, &quad_options),
        (RdfOutputFormat::TriG, &quad_options),
    ] {
        let mut stream =
            RdfStreamWriter::new(Vec::new(), format, &f.sv, &f.schema, &f.conv, options).unwrap();
        stream.write(&f.instance).unwrap();
        stream.write(&f.instance).unwrap();
        let out = stream.finish().unwrap();
        let terms: Vec<String> = match format {
            RdfOutputFormat::Turtle => TurtleParser::new()
                .for_reader(out.as_slice())
                .map(|t| t.unwrap())
                .flat_map(|t| [t.subject.to_string(), t.object.to_string()])
                .collect(),
            RdfOutputFormat::NTriples => NTriplesParser::new()
                .for_reader(out.as_slice())
                .map(|t| t.unwrap())
                .flat_map(|t| [t.subject.to_string(), t.object.to_string()])
                .collect(),
            RdfOutputFormat::NQuads => NQuadsParser::new()
                .for_reader(out.as_slice())
                .map(|q| q.unwrap())
                .flat_map(|q| [q.subject.to_string(), q.object.to_string()])
                .collect(),
            _ => TriGParser::new()
                .for_reader(out.as_slice())
                .map(|q| q.unwrap())
                .flat_map(|q| [q.subject.to_string(), q.object.to_string()])
                .collect(),
        };
        assert_eq!(
            blank_nodes(terms.iter()).len(),
            2 * per_instance,
            "{format:?}: blank nodes merged"
        );
    }

    // Options are validated in `new`, with the writers' errors.
    for (format, options) in [
        (
            RdfOutputFormat::Turtle,
            mapping(&[(TypePartition::Own, "g")]),
        ),
        (
            RdfOutputFormat::NTriples,
            mapping(&[(TypePartition::Own, "g")]),
        ),
        (
            RdfOutputFormat::NQuads,
            WriteOptions::quads(BTreeMap::new()),
        ),
        (RdfOutputFormat::TriG, WriteOptions::quads(BTreeMap::new())),
    ] {
        let err = RdfStreamWriter::new(Vec::new(), format, &f.sv, &f.schema, &f.conv, &options)
            .err()
            .unwrap_or_else(|| panic!("{format:?}: expected InvalidInput"));
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
    }
}

/// Prefix names declared by the `@prefix` lines of a Turtle or TriG document.
fn declared_prefixes(doc: &str) -> Vec<String> {
    doc.lines()
        .filter_map(|l| l.strip_prefix("@prefix "))
        .map(|l| l.split(':').next().unwrap().to_string())
        .collect()
}

/// A Turtle or TriG stream declares each prefix once, the first time an
/// instance uses it, and parses back to the union of its instances. A second
/// instance that brings a new prefix adds just that one line.
#[test]
fn stream_declares_each_prefix_once() {
    let mut f = fixture();
    f.conv
        .add_record(linkml_schemaview::converter::Record::new(
            "ex",
            "http://example.org/",
        ))
        .unwrap();
    let cv =
        f.sv.get_class(&Identifier::new("Registry"), &f.conv)
            .unwrap()
            .unwrap();
    let other = load_yaml_file(
        Path::new(&data_path("type_regimes_second_prefix_data.yaml")),
        &f.sv,
        &cv,
        &f.conv,
    )
    .unwrap()
    .into_instance()
    .unwrap();

    // Skolemized, so the same instance twice is the same triples twice.
    let flat_options = WriteOptions::default().with_skolem(true);
    let quad_options = mapping(&[(TypePartition::Own, "own")]).with_skolem(true);
    for (format, options) in [
        (RdfOutputFormat::Turtle, &flat_options),
        (RdfOutputFormat::TriG, &quad_options),
    ] {
        let stream_of = |instances: &[&LinkMLInstance]| -> String {
            let mut stream =
                RdfStreamWriter::new(Vec::new(), format, &f.sv, &f.schema, &f.conv, options)
                    .unwrap();
            for instance in instances {
                stream.write(instance).unwrap();
            }
            String::from_utf8(stream.finish().unwrap()).unwrap()
        };
        let parse = |doc: &str| -> BTreeSet<(String, String, String, String)> {
            TriGParser::new()
                .for_reader(doc.as_bytes())
                .map(|q| q.unwrap())
                .map(|q| {
                    (
                        q.subject.to_string(),
                        q.predicate.to_string(),
                        q.object.to_string(),
                        q.graph_name.to_string(),
                    )
                })
                .collect()
        };

        // Same prefixes in both instances: each declared once.
        let same = stream_of(&[&f.instance, &f.instance]);
        let declared = declared_prefixes(&same);
        assert_eq!(
            declared.iter().collect::<BTreeSet<_>>().len(),
            declared.len(),
            "{format:?}: a prefix repeats:\n{same}"
        );
        assert_eq!(declared, declared_prefixes(&stream_of(&[&f.instance])));
        assert_eq!(
            parse(&same),
            parse(&stream_of(&[&f.instance])),
            "{format:?}"
        );

        // A new prefix in the second instance: just that line is added.
        let both = stream_of(&[&f.instance, &other]);
        let first = declared_prefixes(&stream_of(&[&f.instance]));
        let all = declared_prefixes(&both);
        assert_eq!(all.len(), first.len() + 1, "{format:?}:\n{both}");
        assert_eq!(all[..first.len()], first[..]);
        assert_eq!(all.last().unwrap(), "ex");
        assert_eq!(
            both.matches("@prefix ex:").count(),
            1,
            "{format:?}:\n{both}"
        );
        let union: BTreeSet<_> = parse(&stream_of(&[&f.instance]))
            .union(&parse(&stream_of(&[&other])))
            .cloned()
            .collect();
        assert_eq!(parse(&both), union, "{format:?}");
    }
}
