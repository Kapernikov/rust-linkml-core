#![cfg(feature = "ttl")]

use linkml_runtime::rdf_import_store::RdfImportStore;
use linkml_runtime::rdf_streaming::compute_inline_structure;
use linkml_schemaview::identifier::converter_from_schemas;
use linkml_schemaview::schemaview::SchemaView;

/// Schema: Train { id, operator inline -> Operator }; Operator { id, name }.
/// Two trains, both pointing at the same operator. No train->train inlining.
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
        @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
        ex:t1 a ex:Train ; ex:id "T1" ; ex:operator ex:op1 .
        ex:t2 a ex:Train ; ex:id "T2" ; ex:operator ex:op1 .
        ex:op1 a ex:Operator ; ex:id "OP1" ; ex:name "ACME" .
        ex:op2 a ex:Operator ; ex:id "OP2" ; ex:name "Standalone" .
    "#
    .to_string();
    (sv, conv, ttl)
}

#[test]
fn pass1_claims_shared_inlined_operator_only() {
    let (sv, conv, ttl) = two_trains_sharing_operator();
    let store = RdfImportStore::from_turtle(std::io::Cursor::new(ttl.as_bytes())).unwrap();

    let s = compute_inline_structure(&store, &sv, &conv, &["Train", "Operator"]).unwrap();

    // subject.to_string() formats named nodes as `<iri>` with angle brackets.
    let op1 = "<http://example.org/op1>";
    let op2 = "<http://example.org/op2>";
    let t1 = "<http://example.org/t1>";
    let t2 = "<http://example.org/t2>";

    assert!(
        s.claimed.contains(op1),
        "op1 should be claimed (inlined by both trains), got {:?}",
        s.claimed
    );
    assert!(
        !s.claimed.contains(op2),
        "op2 is not inlined anywhere, must not be claimed"
    );
    assert!(
        !s.claimed.contains(t1),
        "t1 is not inlined, must not be claimed"
    );
    assert!(
        !s.claimed.contains(t2),
        "t2 is not inlined, must not be claimed"
    );

    let t1_edges = s.inline_edges.get(t1).cloned().unwrap_or_default();
    assert_eq!(t1_edges, vec![op1.to_string()]);
    let t2_edges = s.inline_edges.get(t2).cloned().unwrap_or_default();
    assert_eq!(t2_edges, vec![op1.to_string()]);
}

#[test]
fn pass1_materializations_for_two_trains_sharing_operator() {
    let (sv, conv, ttl) = two_trains_sharing_operator();
    let store = RdfImportStore::from_turtle(std::io::Cursor::new(ttl.as_bytes())).unwrap();
    let s = compute_inline_structure(&store, &sv, &conv, &["Train", "Operator"]).unwrap();

    let mats = |iri: &str| s.materializations.get(iri).copied().unwrap_or(0);
    assert_eq!(mats("<http://example.org/t1>"), 1, "t1 unclaimed root");
    assert_eq!(mats("<http://example.org/t2>"), 1, "t2 unclaimed root");
    assert_eq!(
        mats("<http://example.org/op1>"),
        2,
        "op1 inlined by both t1 and t2 — built twice in Pass 2"
    );
    assert_eq!(mats("<http://example.org/op2>"), 1, "op2 unclaimed root");
}

/// t1 inlines t2 (via next_train) and op1 (via operator).
/// t2 inlines op1 (via operator).
/// Expected: materializations[op1] = 2, materializations[t2] = 1.
#[test]
fn pass1_materializations_for_train_inlining_train() {
    let schema_yaml = r#"
id: https://example.org/trains2
name: trains2
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
      next_train:
        range: Train
        inlined: true
  Operator:
    attributes:
      id:
        identifier: true
"#;
    let schema: linkml_meta::SchemaDefinition = serde_yaml::from_str(schema_yaml).unwrap();
    let mut sv = SchemaView::new();
    sv.add_schema(schema.clone()).unwrap();
    let conv = converter_from_schemas([&schema]);

    let ttl = r#"
        @prefix ex: <http://example.org/> .
        ex:t1 a ex:Train ; ex:id "T1" ; ex:operator ex:op1 ; ex:next_train ex:t2 .
        ex:t2 a ex:Train ; ex:id "T2" ; ex:operator ex:op1 .
        ex:op1 a ex:Operator ; ex:id "OP1" .
    "#;
    let store = RdfImportStore::from_turtle(std::io::Cursor::new(ttl.as_bytes())).unwrap();
    let s = compute_inline_structure(&store, &sv, &conv, &["Train", "Operator"]).unwrap();

    let mats = |iri: &str| s.materializations.get(iri).copied().unwrap_or(0);
    assert_eq!(
        mats("<http://example.org/t1>"),
        1,
        "t1 is the only unclaimed root"
    );
    assert_eq!(
        mats("<http://example.org/t2>"),
        1,
        "t2 claimed (by t1), reached once"
    );
    assert_eq!(
        mats("<http://example.org/op1>"),
        2,
        "op1 reached once directly from t1 and once via t1->t2"
    );
}

#[test]
fn pass1_detects_inline_cycle() {
    let schema_yaml = r#"
id: https://example.org/cyc
name: cyc
prefixes:
  ex: http://example.org/
default_prefix: ex
default_range: string
classes:
  Node:
    attributes:
      id:
        identifier: true
      child:
        range: Node
        inlined: true
"#;
    let schema: linkml_meta::SchemaDefinition = serde_yaml::from_str(schema_yaml).unwrap();
    let mut sv = SchemaView::new();
    sv.add_schema(schema.clone()).unwrap();
    let conv = converter_from_schemas([&schema]);

    let ttl = r#"
        @prefix ex: <http://example.org/> .
        ex:a a ex:Node ; ex:id "A" ; ex:child ex:b .
        ex:b a ex:Node ; ex:id "B" ; ex:child ex:a .
    "#;
    let store = RdfImportStore::from_turtle(std::io::Cursor::new(ttl.as_bytes())).unwrap();

    let err = compute_inline_structure(&store, &sv, &conv, &["Node"]).unwrap_err();
    match err {
        linkml_runtime::turtle_import::ImportError::InlinedCycle { .. } => {}
        other => panic!("expected InlinedCycle, got {:?}", other),
    }
}
