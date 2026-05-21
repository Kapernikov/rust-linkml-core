#![cfg(feature = "ttl")]

use linkml_runtime::rdf_import_store::RdfImportStore;
use linkml_runtime::rdf_streaming::import_from_store_streaming;
use std::cell::RefCell;
use std::rc::Rc;
use linkml_runtime::triple_source::TripleSource;
use linkml_runtime::LinkMLInstance;
use linkml_schemaview::identifier::converter_from_schemas;
use linkml_schemaview::schemaview::SchemaView;

fn two_trains_sharing_operator() -> (SchemaView, linkml_schemaview::Converter, String) {
    let schema_yaml = r#"
id: https://example.org/trains
name: trains
prefixes:
  ex: http://example.org/
default_prefix: ex
default_range: string
classes:
  Train:
    attributes:
      id:
        identifier: true
      operator:
        range: Operator
        inlined: true
  Operator:
    attributes:
      id:
        identifier: true
      name:
"#;
    let schema: linkml_meta::SchemaDefinition = serde_yaml::from_str(schema_yaml).unwrap();
    let mut sv = SchemaView::new();
    sv.add_schema(schema.clone()).unwrap();
    let conv = converter_from_schemas([&schema]);

    let ttl = r#"
        @prefix ex: <http://example.org/> .
        ex:t1 a ex:Train ; ex:id "T1" ; ex:operator ex:op1 .
        ex:t2 a ex:Train ; ex:id "T2" ; ex:operator ex:op1 .
        ex:op1 a ex:Operator ; ex:id "OP1" ; ex:name "ACME" .
        ex:op2 a ex:Operator ; ex:id "OP2" ; ex:name "Standalone" .
    "#
    .to_string();
    (sv, conv, ttl)
}

#[test]
fn streaming_two_trains_with_shared_operator() {
    let (sv, conv, ttl) = two_trains_sharing_operator();
    let store = RdfImportStore::from_turtle(std::io::Cursor::new(ttl.as_bytes())).unwrap();
    let stream =
        import_from_store_streaming(
            &store,
            &sv,
            &conv,
            &["Train", "Operator"],
            Rc::new(RefCell::new(Vec::new())),
            false,
        )
        .unwrap();

    let mut trains: Vec<LinkMLInstance> = Vec::new();
    let mut operators: Vec<LinkMLInstance> = Vec::new();
    for item in stream {
        let (cls, inst) = item.unwrap();
        match cls.as_str() {
            "Train" => trains.push(inst),
            "Operator" => operators.push(inst),
            other => panic!("unexpected class {other}"),
        }
    }
    assert_eq!(trains.len(), 2);
    assert_eq!(operators.len(), 1);
    // The standalone Operator emitted is op2 (op1 is suppressed because it
    // was inlined into both trains). `id` is populated from the subject IRI.
    assert_eq!(operators[0].to_json()["id"], "http://example.org/op2");

    // Both trains must have an inlined operator with id "<op1>".
    for t in &trains {
        let j = t.to_json();
        assert_eq!(j["operator"]["id"], "http://example.org/op1");
    }

    // The inlined op1 subtree must have distinct node_ids across the two
    // trains (so blame/diff sees them as independent denormalised copies).
    fn op_node_id(inst: &LinkMLInstance) -> u64 {
        if let LinkMLInstance::Object { values, .. } = inst {
            if let Some(LinkMLInstance::Object { node_id, .. }) = values.get("operator") {
                return *node_id;
            }
        }
        panic!("operator slot not found or not an Object");
    }
    assert_ne!(
        op_node_id(&trains[0]),
        op_node_id(&trains[1]),
        "denormalised op1 instances must have distinct node_ids"
    );
}

/// Wraps an `RdfImportStore` and counts how many times
/// `objects_for_subject_predicate` and `triples_for_subject` are called.
/// Used to assert the cache avoids redundant store reads.
struct CountingStore<'a> {
    inner: &'a RdfImportStore,
    triples_for_subject_calls: std::cell::RefCell<std::collections::HashMap<String, usize>>,
}

impl<'a> TripleSource for CountingStore<'a> {
    fn subjects_for_predicate_object<'b>(
        &'b self,
        p: &oxrdf::NamedNode,
        o: &oxrdf::NamedNode,
    ) -> Box<dyn Iterator<Item = oxrdf::NamedOrBlankNodeRef<'b>> + 'b> {
        self.inner.subjects_for_predicate_object(p, o)
    }
    fn objects_for_subject_predicate<'b>(
        &'b self,
        s: &oxrdf::NamedOrBlankNode,
        p: &oxrdf::NamedNode,
    ) -> Box<dyn Iterator<Item = oxrdf::TermRef<'b>> + 'b> {
        self.inner.objects_for_subject_predicate(s, p)
    }
    fn triples_for_subject<'b>(
        &'b self,
        s: &oxrdf::NamedOrBlankNode,
    ) -> Box<dyn Iterator<Item = oxrdf::TripleRef<'b>> + 'b> {
        let k = s.to_string();
        *self.triples_for_subject_calls.borrow_mut().entry(k).or_insert(0) += 1;
        self.inner.triples_for_subject(s)
    }
    fn len(&self) -> Option<usize> {
        self.inner.len()
    }
    fn on_consumed(&self, _s: &str, _p: &str, _o: &str) {}
}

#[test]
fn cache_avoids_rebuilding_shared_operator() {
    let (sv, conv, ttl) = two_trains_sharing_operator();
    let inner = RdfImportStore::from_turtle(std::io::Cursor::new(ttl.as_bytes())).unwrap();
    let store = CountingStore {
        inner: &inner,
        triples_for_subject_calls: std::cell::RefCell::new(std::collections::HashMap::new()),
    };

    let stream =
        import_from_store_streaming(
            &store,
            &sv,
            &conv,
            &["Train", "Operator"],
            Rc::new(RefCell::new(Vec::new())),
            false,
        )
        .unwrap();
    let _: Vec<_> = stream.collect::<Result<_, _>>().unwrap();

    // With the cache enabled, harvest_subject runs exactly once for op1
    // (in Pass 2), so `triples_for_subject` is called twice in total for
    // <op1>: once in Pass 1 (structural walk) and once in Pass 2 (harvest).
    let calls = store.triples_for_subject_calls.borrow();
    let op1_calls = calls
        .get("<http://example.org/op1>")
        .copied()
        .unwrap_or(0);
    assert_eq!(
        op1_calls, 2,
        "op1's triples should be read exactly twice with caching enabled \
         (once in Pass 1, once in Pass 2 build); got {op1_calls}"
    );
}
