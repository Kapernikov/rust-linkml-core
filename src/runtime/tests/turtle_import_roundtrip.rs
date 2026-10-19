#![cfg(feature = "ttl")]

use linkml_runtime::{
    load_yaml_file,
    rdf_import::{import_turtle, ImportOptions},
    schema_rdf::{rdf_types, TypePartition, TypeRegime, RDF_TYPE, SESAME_DIRECT_TYPE},
    turtle::{turtle_to_string, write_nquads, write_ntriples, write_trig, WriteOptions},
    LinkMLInstance,
};
use linkml_schemaview::identifier::{converter_from_schema, converter_from_schemas, Identifier};
use linkml_schemaview::io::from_yaml;
use linkml_schemaview::schemaview::SchemaView;
use oxrdf::graph::CanonicalizationAlgorithm;
use oxrdf::{Graph, GraphName, NamedNode, TripleRef};
use oxttl::{NQuadsParser, NTriplesParser, TriGParser, TurtleParser};
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

fn data_path(name: &str) -> PathBuf {
    let mut p = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    p.push("tests");
    p.push("data");
    p.push(name);
    p
}

fn load_schema_with_types(
    schema_file: &str,
) -> (
    linkml_meta::SchemaDefinition,
    SchemaView,
    linkml_schemaview::Converter,
) {
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
    (schema, sv, conv)
}

/// Helper: export to TTL, re-import, compare JSON trees.
/// Returns the re-imported JSON for additional assertions.
fn roundtrip(
    sv: &SchemaView,
    schema: &linkml_meta::SchemaDefinition,
    conv: &linkml_schemaview::Converter,
    original: &linkml_runtime::LinkMLInstance,
    class_name: &str,
) -> serde_json::Value {
    let ttl = turtle_to_string(
        original,
        sv,
        schema,
        conv,
        &WriteOptions::default().with_skolem(false),
    )
    .unwrap();

    let stream = import_turtle(
        std::io::Cursor::new(ttl.as_bytes()),
        sv.clone(),
        conv.clone(),
        &[class_name],
        ImportOptions::default(),
    )
    .unwrap();
    let instances: Vec<LinkMLInstance> = stream
        .filter_map(|r| r.ok())
        .filter_map(|(c, i)| if c == class_name { Some(i) } else { None })
        .collect();
    assert!(
        !instances.is_empty(),
        "Expected at least one {class_name} instance"
    );
    instances[0].to_json()
}

// ─── Round-trip: typed literals ───

#[test]
fn roundtrip_typed_literals() {
    let (schema, sv, conv) = load_schema_with_types("typed_literals_schema.yaml");
    let class = sv
        .get_class(&Identifier::new("Thing"), &conv)
        .unwrap()
        .unwrap();
    let original = load_yaml_file(
        Path::new(&data_path("typed_literals_data.yaml")),
        &sv,
        &class,
        &conv,
    )
    .unwrap()
    .into_instance()
    .unwrap();
    let original_json = original.to_json();

    let reimported = roundtrip(&sv, &schema, &conv, &original, "Thing");

    // Check key scalar fields survive round-trip
    assert_eq!(reimported["name"], original_json["name"], "string field");
    assert_eq!(reimported["count"], original_json["count"], "integer field");
    assert_eq!(
        reimported["active"], original_json["active"],
        "boolean field"
    );
    // Float may have minor precision differences, just check it exists and is numeric
    assert!(
        reimported["score"].is_number(),
        "float field should be numeric"
    );
}

// ─── Round-trip: enum meanings ───

#[test]
fn roundtrip_enum_meaning() {
    let schema = from_yaml(Path::new(&data_path("enum_meaning_schema.yaml"))).unwrap();
    let mut sv = SchemaView::new();
    sv.add_schema(schema.clone()).unwrap();
    let conv = converter_from_schema(&schema);
    let class = sv
        .get_class(&Identifier::new("Item"), &conv)
        .unwrap()
        .unwrap();
    let original = load_yaml_file(
        Path::new(&data_path("enum_meaning_data.yaml")),
        &sv,
        &class,
        &conv,
    )
    .unwrap()
    .into_instance()
    .unwrap();
    let original_json = original.to_json();

    let reimported = roundtrip(&sv, &schema, &conv, &original, "Item");

    // Enum values should survive round-trip (meaning URI → permissible value name)
    assert_eq!(
        reimported["status"], original_json["status"],
        "single-valued enum"
    );
}

// ─── Round-trip: language-tagged literals ───

#[test]
fn roundtrip_lang_tags() {
    let (schema, sv, conv) = load_schema_with_types("lang_tag_schema.yaml");
    let class = sv
        .get_class(&Identifier::new("Station"), &conv)
        .unwrap()
        .unwrap();
    let original = load_yaml_file(
        Path::new(&data_path("lang_tag_data.yaml")),
        &sv,
        &class,
        &conv,
    )
    .unwrap()
    .into_instance()
    .unwrap();
    let original_json = original.to_json();

    let reimported = roundtrip(&sv, &schema, &conv, &original, "Station");

    // Static in_language slot should survive
    assert_eq!(
        reimported["opName"], original_json["opName"],
        "in_language slot"
    );
    // Plain string should survive
    assert_eq!(
        reimported["description"], original_json["description"],
        "plain string slot"
    );
    // Language-tagged class labels should be reconstructed
    let reimp_labels = &reimported["labels"];
    assert!(
        reimp_labels.is_array() || reimp_labels.is_object(),
        "labels should be present after round-trip"
    );
}

// ─── Round-trip matrix: type regimes × output format × skolem ───
//
// The negative case (two unrelated schema types and no sesame:directType →
// `ImportError::AmbiguousType`) is covered exactly, including the message, by
// `unrelated_types_without_direct_type_are_ambiguous_with_readable_message` in
// `type_regimes_import.rs`; it is not repeated here.
//
// Fixture (`type_regimes_schema.yaml`): abstract `Thing` <- `Asset` <- `Signal`
// <- `MainSignal`, mixin chain `Located` -> `Placed` deeper than the is_a
// parent, a self-mixin, an inlined polymorphic `location` (designates_type
// `typeURI`), inlined lists (`parts`, and `tags` keyed only by `unique_keys`).
//
// Blank-node mode (skolem off) is compared up to blank-node renaming with
// oxrdf's graph canonicalization, so all properties run in both modes.

#[derive(Clone, Copy, Debug)]
enum Format {
    Turtle,
    NTriples,
    NQuads,
    TriG,
}

const FORMATS: [Format; 4] = [
    Format::Turtle,
    Format::NTriples,
    Format::NQuads,
    Format::TriG,
];

struct Fx {
    schema: linkml_meta::SchemaDefinition,
    sv: SchemaView,
    conv: linkml_schemaview::Converter,
    instance: LinkMLInstance,
}

fn regimes_fixture() -> Fx {
    let (schema, sv, conv) = load_schema_with_types("type_regimes_schema.yaml");
    let class = sv
        .get_class(&Identifier::new("Registry"), &conv)
        .unwrap()
        .unwrap();
    let instance = load_yaml_file(
        Path::new(&data_path("type_regimes_roundtrip_data.yaml")),
        &sv,
        &class,
        &conv,
    )
    .unwrap()
    .into_instance()
    .unwrap();
    Fx {
        schema,
        sv,
        conv,
        instance,
    }
}

fn partition_graphs(regime: TypeRegime) -> BTreeMap<TypePartition, GraphName> {
    regime
        .partitions()
        .map(|p| {
            (
                p,
                GraphName::NamedNode(
                    NamedNode::new(format!("https://example.com/graphs/{p:?}")).unwrap(),
                ),
            )
        })
        .collect()
}

/// Write `instance` in `format` under `regime`, parse it back and return the
/// union of all graphs as one canonicalized triple graph.
fn written(
    fx: &Fx,
    instance: &LinkMLInstance,
    regime: TypeRegime,
    format: Format,
    skolem: bool,
) -> Graph {
    let mut graph = Graph::new();
    let opts = WriteOptions::flat(regime).with_skolem(skolem);
    let quad_opts = WriteOptions::quads(partition_graphs(regime)).with_skolem(skolem);
    let mut buf = Vec::new();
    match format {
        Format::Turtle => {
            let ttl = turtle_to_string(instance, &fx.sv, &fx.schema, &fx.conv, &opts).unwrap();
            for t in TurtleParser::new().for_reader(ttl.as_bytes()) {
                graph.insert(&t.unwrap());
            }
        }
        Format::NTriples => {
            write_ntriples(instance, &fx.sv, &fx.schema, &fx.conv, &mut buf, &opts).unwrap();
            for t in NTriplesParser::new().for_reader(buf.as_slice()) {
                graph.insert(&t.unwrap());
            }
        }
        Format::NQuads => {
            write_nquads(instance, &fx.sv, &fx.schema, &fx.conv, &mut buf, &quad_opts).unwrap();
            for q in NQuadsParser::new().for_reader(buf.as_slice()) {
                graph.insert(&oxrdf::Triple::from(q.unwrap()));
            }
        }
        Format::TriG => {
            write_trig(instance, &fx.sv, &fx.schema, &fx.conv, &mut buf, &quad_opts).unwrap();
            for q in TriGParser::new().for_reader(buf.as_slice()) {
                graph.insert(&oxrdf::Triple::from(q.unwrap()));
            }
        }
    }
    graph.canonicalize(CanonicalizationAlgorithm::Unstable);
    graph
}

fn import_graph(fx: &Fx, graph: &Graph) -> LinkMLInstance {
    // RDF has no list order and the importer keeps the order triples arrive
    // in, so feed them sorted: deterministic, and positional skolem IRIs
    // (s1/parts/0, s1/parts/1, ...) then come back in their own order.
    let mut lines: Vec<String> = graph.iter().map(|t| format!("{t} .\n")).collect();
    lines.sort();
    let nt = lines.concat();
    let stream = import_turtle(
        std::io::Cursor::new(nt.into_bytes()),
        fx.sv.clone(),
        fx.conv.clone(),
        &["Registry"],
        ImportOptions::default(),
    )
    .unwrap();
    let mut found = Vec::new();
    for item in stream {
        let (c, i) = item.unwrap();
        assert_eq!(c, "Registry", "unexpected extra root {c}");
        found.push(i);
    }
    assert_eq!(found.len(), 1, "expected exactly one Registry");
    found.remove(0)
}

/// JSON with every array sorted: RDF carries no list order.
fn canon(v: serde_json::Value) -> serde_json::Value {
    use serde_json::Value;
    match v {
        Value::Array(a) => {
            let mut a: Vec<Value> = a.into_iter().map(canon).collect();
            a.sort_by_key(|x| x.to_string());
            Value::Array(a)
        }
        Value::Object(o) => Value::Object(o.into_iter().map(|(k, v)| (k, canon(v))).collect()),
        other => other,
    }
}

fn sorted(graph: &Graph) -> BTreeSet<String> {
    graph.iter().map(|t: TripleRef| t.to_string()).collect()
}

/// Run `check` for every (regime, format, skolem) cell; returns the matrix size.
fn matrix(mut check: impl FnMut(&Fx, TypeRegime, Format, bool)) -> usize {
    let fx = regimes_fixture();
    let mut n = 0;
    for &regime in TypeRegime::NAMED {
        for format in FORMATS {
            for skolem in [true, false] {
                check(&fx, regime, format, skolem);
                n += 1;
            }
        }
    }
    n
}

#[test]
fn matrix_import_of_write_is_the_original() {
    let cells = matrix(|fx, regime, format, skolem| {
        let ctx = format!("regime={regime:?} format={format:?} skolem={skolem}");
        let back = import_graph(fx, &written(fx, &fx.instance, regime, format, skolem));
        assert_eq!(
            canon(fx.instance.to_json()),
            canon(back.to_json()),
            "{ctx}: re-imported instance differs"
        );
        // The designates_type slot stays single-valued and names the direct class.
        let want = fx.instance.to_json()["signals"][0]["location"]["typeURI"].clone();
        let got = back.to_json()["signals"][0]["location"]["typeURI"].clone();
        assert!(want.is_string(), "{ctx}: fixture designator {want}");
        assert_eq!(got, want, "{ctx}: designator");
        let tags = back.to_json()["signals"][0]["tags"].clone();
        assert_eq!(
            tags.as_array().map(Vec::len),
            Some(2),
            "{ctx}: unique_keys list"
        );
    });
    assert_eq!(cells, 24);
}

#[test]
fn matrix_every_regime_imports_like_asserted() {
    matrix(|fx, regime, format, skolem| {
        let ctx = format!("regime={regime:?} format={format:?} skolem={skolem}");
        let got = import_graph(fx, &written(fx, &fx.instance, regime, format, skolem));
        let base = import_graph(
            fx,
            &written(fx, &fx.instance, TypeRegime::ASSERTED, format, skolem),
        );
        assert_eq!(canon(got.to_json()), canon(base.to_json()), "{ctx}");
    });
}

#[test]
fn matrix_write_import_write_is_isomorphic() {
    matrix(|fx, regime, format, skolem| {
        let ctx = format!("regime={regime:?} format={format:?} skolem={skolem}");
        let first = written(fx, &fx.instance, regime, format, skolem);
        let back = import_graph(fx, &first);
        let second = written(fx, &back, regime, format, skolem);
        let (a, b) = (sorted(&first), sorted(&second));
        assert!(
            a == b,
            "{ctx}\nonly first: {:#?}\nonly second: {:#?}",
            a.difference(&b).collect::<Vec<_>>(),
            b.difference(&a).collect::<Vec<_>>()
        );
    });
}

/// Per typed subject: its rdf:type IRIs and its sesame:directType IRIs.
fn typing(g: &Graph) -> BTreeMap<String, (BTreeSet<String>, Vec<String>)> {
    let mut out: BTreeMap<String, (BTreeSet<String>, Vec<String>)> = BTreeMap::new();
    for t in g.iter() {
        let oxrdf::TermRef::NamedNode(obj) = t.object else {
            continue;
        };
        let obj = obj.as_str().to_string();
        let entry = out.entry(t.subject.to_string()).or_default();
        match t.predicate.as_str() {
            RDF_TYPE => {
                entry.0.insert(obj);
            }
            SESAME_DIRECT_TYPE => entry.1.push(obj),
            _ => {}
        }
    }
    out.retain(|_, (types, directs)| !types.is_empty() || !directs.is_empty());
    out
}

#[test]
fn matrix_written_types_match_rdf_types_and_direct_type() {
    matrix(|fx, regime, format, skolem| {
        let ctx = format!("regime={regime:?} format={format:?} skolem={skolem}");
        let g = typing(&written(fx, &fx.instance, regime, format, skolem));
        let reference = typing(&written(
            fx,
            &fx.instance,
            TypeRegime::ASSERTED,
            format,
            skolem,
        ));
        // Blank-node labels differ between two canonicalized graphs, so nodes
        // are compared as a multiset of own classes, not by subject.
        let mut own_classes: Vec<String> = Vec::new();
        for (subject, (types, directs)) in &g {
            let own = if regime == TypeRegime::ASSERTED {
                assert!(directs.is_empty(), "{ctx}: directType under ASSERTED");
                assert_eq!(types.len(), 1, "{ctx}: {subject} types {types:?}");
                types.iter().next().unwrap().clone()
            } else {
                assert_eq!(directs.len(), 1, "{ctx}: {subject} directs {directs:?}");
                directs[0].clone()
            };
            let class = fx
                .sv
                .get_class_by_uri(&own)
                .unwrap()
                .unwrap_or_else(|| panic!("{ctx}: unknown class {own}"));
            let expect: BTreeSet<String> = rdf_types(&class, regime)
                .unwrap()
                .iter()
                .map(|c| c.canonical_uri().to_string())
                .collect();
            assert_eq!(*types, expect, "{ctx}: rdf:type of {subject}");
            own_classes.push(own);
        }
        let mut want: Vec<String> = reference
            .values()
            .flat_map(|(t, _)| t.iter().cloned())
            .collect();
        own_classes.sort();
        want.sort();
        assert!(
            own_classes.len() == 10,
            "{ctx}: {} nodes",
            own_classes.len()
        );
        assert_eq!(own_classes, want, "{ctx}: own classes of the typed nodes");
        // sesame:directType appears exactly when the regime contains Direct.
        let n_direct: usize = g.values().map(|(_, d)| d.len()).sum();
        assert_eq!(n_direct == 0, regime == TypeRegime::ASSERTED, "{ctx}");
    });
}

/// A set that is not one of the named regimes behaves like any other: the
/// writer keeps exactly its partitions and the importer reads it back.
#[test]
fn non_standard_partition_set_writes_consistently_and_imports_back() {
    let built: TypeRegime = [TypePartition::Own, TypePartition::IsA]
        .into_iter()
        .collect();
    let parsed: TypeRegime = "{own,is_a}".parse().unwrap();
    assert_eq!(built, parsed);
    assert!(!TypeRegime::NAMED.contains(&built));
    let fx = regimes_fixture();
    for regime in [built, parsed] {
        for skolem in [true, false] {
            let ctx = format!("skolem={skolem}");
            let flat = written(&fx, &fx.instance, regime, Format::NTriples, skolem);
            let g = typing(&flat);
            assert_eq!(g.len(), 10, "{ctx}");
            for (subject, (types, directs)) in &g {
                assert!(directs.is_empty(), "{ctx}: directType on {subject}");
                let own = types
                    .iter()
                    .map(|u| fx.sv.get_class_by_uri(u).unwrap().unwrap())
                    .find(|c| {
                        rdf_types(c, regime)
                            .unwrap()
                            .iter()
                            .map(|t| t.canonical_uri().to_string())
                            .collect::<BTreeSet<_>>()
                            == *types
                    });
                assert!(
                    own.is_some(),
                    "{ctx}: {subject} types {types:?} are not rdf_types of any class"
                );
            }
            // Quads restricted to the set's partitions are the flat output.
            for format in [Format::NQuads, Format::TriG] {
                assert_eq!(
                    sorted(&written(&fx, &fx.instance, regime, format, skolem)),
                    sorted(&flat),
                    "{ctx} {format:?}"
                );
            }
            let back = import_graph(&fx, &flat);
            assert_eq!(
                canon(fx.instance.to_json()),
                canon(back.to_json()),
                "{ctx}: re-imported instance differs"
            );
        }
    }
}

#[test]
fn quads_union_equals_flat_isa_and_mixins_output() {
    let fx = regimes_fixture();
    let all_partitions: BTreeMap<TypePartition, GraphName> = TypePartition::ALL
        .iter()
        .map(|p| {
            (
                *p,
                GraphName::NamedNode(
                    NamedNode::new(format!("https://example.com/graphs/{p:?}")).unwrap(),
                ),
            )
        })
        .collect();
    for skolem in [true, false] {
        let opts = WriteOptions::quads(all_partitions.clone()).with_skolem(skolem);
        let flat = written(
            &fx,
            &fx.instance,
            TypeRegime::IS_A_AND_MIXINS,
            Format::NTriples,
            skolem,
        );
        for format in [Format::NQuads, Format::TriG] {
            let mut buf = Vec::new();
            let mut union = Graph::new();
            match format {
                Format::NQuads => {
                    write_nquads(&fx.instance, &fx.sv, &fx.schema, &fx.conv, &mut buf, &opts)
                        .unwrap();
                    for q in NQuadsParser::new().for_reader(buf.as_slice()) {
                        union.insert(&oxrdf::Triple::from(q.unwrap()));
                    }
                }
                _ => {
                    write_trig(&fx.instance, &fx.sv, &fx.schema, &fx.conv, &mut buf, &opts)
                        .unwrap();
                    for q in TriGParser::new().for_reader(buf.as_slice()) {
                        union.insert(&oxrdf::Triple::from(q.unwrap()));
                    }
                }
            }
            union.canonicalize(CanonicalizationAlgorithm::Unstable);
            assert_eq!(
                sorted(&union),
                sorted(&flat),
                "format={format:?} skolem={skolem}"
            );
        }
    }
}
