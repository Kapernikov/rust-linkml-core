//! Spec addendum rule 2 — identity compares meaning, not spelling (D1 + D6).
//!
//! Two halves, pinned together because they only hold together:
//!
//! * designator values are canonicalised at the boxing chokepoint, so every
//!   spelling of "this element is a `Circle`" becomes the one value the class
//!   declares, and `to_json` emits it;
//! * identity-label components whose range descends from `uri`/`uriorcurie`
//!   are IRI-expanded before comparison at *every* resolve site, so a CURIE and
//!   its expansion are one identity in diff, patch, navigate and the lint.

use linkml_runtime::{
    diff, lint_instance_identity, load_json_str, patch, Delta, DeltaOp, DiffOptions,
    LinkMLInstance, PatchOptions, ValidationProblemType, ValidationResult,
};
use linkml_schemaview::identifier::{converter_from_schema, Identifier};
use linkml_schemaview::io::from_yaml;
use linkml_schemaview::schemaview::{ClassView, SchemaView};
use linkml_schemaview::Converter;
use serde_json::{json, Value as JsonValue};
use std::path::PathBuf;

const CIRCLE_CURIE: &str = "canon:Circle";
/// `LeafNode`'s canonical designator value: its `class_uri`, expanded (the slot
/// range is `uri`).
const LEAF_URI: &str = "https://example.org/canon/Leaf";
/// The same class's *schema-native* URI — a different spelling of the same
/// meaning, and one the accepted-value set must recognise.
const LEAF_NATIVE_URI: &str = "https://w3id.org/linkml/examples/identity_canonical/LeafNode";
const FANCY_CURIE: &str = "canon:FancyWidget";
const WGS84_CURIE: &str = "ex:WGS84";
const WGS84_URI: &str = "https://example.org/canon/WGS84";
const ETRS89_CURIE: &str = "ex:ETRS89";
const ETRS89_URI: &str = "https://example.org/canon/ETRS89";

struct Fixture {
    sv: SchemaView,
    conv: Converter,
    container: ClassView,
}

fn fixture() -> Fixture {
    let mut p = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    p.push("tests/data/identity_canonical.yaml");
    let schema = from_yaml(&p).unwrap();
    let mut sv = SchemaView::new();
    sv.add_schema(schema.clone()).unwrap();
    let conv = converter_from_schema(&schema);
    let container = sv
        .get_class(&Identifier::new("Container"), &conv)
        .unwrap()
        .expect("class not found");
    Fixture {
        sv,
        conv,
        container,
    }
}

impl Fixture {
    fn load_result(&self, v: JsonValue) -> (LinkMLInstance, Vec<ValidationResult>) {
        let r = load_json_str(&v.to_string(), &self.sv, &self.container, &self.conv).unwrap();
        let issues = r.validation_issues.clone();
        (r.into_instance().expect("instance must load"), issues)
    }
    fn load(&self, v: JsonValue) -> LinkMLInstance {
        self.load_result(v).0
    }
    fn json(&self, v: JsonValue) -> JsonValue {
        self.load(v).to_json()
    }
}

fn diff2(f: &Fixture, before: JsonValue, after: JsonValue) -> Vec<Delta> {
    diff(&f.load(before), &f.load(after), DiffOptions::new(true))
}

fn only(deltas: &[Delta]) -> &Delta {
    assert_eq!(deltas.len(), 1, "expected exactly one delta: {deltas:#?}");
    &deltas[0]
}

fn systems(items: Vec<JsonValue>) -> JsonValue {
    json!({ "systems": items })
}

// ---------------------------------------------------------------------------
// D1 — designator values canonicalised at load
// ---------------------------------------------------------------------------

/// CURIE, full URI and omitted-entirely are three spellings of one fact. All
/// three must box to the class's canonical designator value, and `to_json`
/// must emit that value.
#[test]
fn every_designator_spelling_boxes_to_the_one_canonical_value() {
    let f = fixture();
    let out = f.json(json!({
        "shapes": [
            {"typeURI": CIRCLE_CURIE, "label": "a", "radius": 1.0},
            {"typeURI": "https://w3id.org/linkml/examples/identity_canonical/Circle",
             "label": "b", "radius": 2.0},
            {"label": "c", "radius": 3.0}
        ]
    }));
    let shapes = out.get("shapes").and_then(JsonValue::as_array).unwrap();
    let spellings: Vec<&JsonValue> = shapes.iter().map(|s| &s["typeURI"]).collect();
    assert_eq!(
        spellings,
        vec![
            &json!(CIRCLE_CURIE),
            &json!(CIRCLE_CURIE),
            &json!(CIRCLE_CURIE)
        ],
        "one canonical designator value, whatever the data spelled: {out:#?}"
    );
}

/// The canonical form follows the designator slot's *range*: a `uri`-ranged
/// designator canonicalises to the expanded URI, not to a CURIE.
///
/// `LeafNode` declares a `class_uri`, so it has two legitimate URI spellings and
/// two CURIE spellings. All four — and the omitted case — mean the same class
/// and must land on the one canonical value, silently.
#[test]
fn uri_ranged_designator_canonicalises_to_the_expanded_class_uri() {
    let f = fixture();
    let doc = json!({
        "nodes": [
            {"kind": "canon:LeafNode", "name": "n1", "depth": 1},
            {"kind": LEAF_NATIVE_URI, "name": "n2", "depth": 2},
            {"kind": "ex:Leaf", "name": "n3", "depth": 3},
            {"kind": LEAF_URI, "name": "n4", "depth": 4},
            {"name": "n5", "depth": 5}
        ]
    });
    let (inst, issues) = f.load_result(doc);
    assert!(
        issues.is_empty(),
        "every spelling above is an accepted designator value: {issues:#?}"
    );
    let out = inst.to_json();
    let nodes = out.get("nodes").and_then(JsonValue::as_array).unwrap();
    assert_eq!(nodes.len(), 5);
    for n in nodes {
        assert_eq!(n["kind"], json!(LEAF_URI), "in {out:#?}");
    }
}

/// The dict (mapping) boxing chokepoint canonicalises too — `build_mapping_
/// entry_for_slot` builds objects the list path never sees.
#[test]
fn dict_form_designator_is_canonicalised() {
    let f = fixture();
    let out = f.json(json!({
        "widgets": {
            "w1": {"wid": "w1", "typeURI":
                "https://w3id.org/linkml/examples/identity_canonical/FancyWidget",
                "sparkle": "yes"},
            "w2": {"wid": "w2", "typeURI": FANCY_CURIE, "sparkle": "no"}
        }
    }));
    let widgets = out.get("widgets").unwrap();
    assert_eq!(widgets["w1"]["typeURI"], json!(FANCY_CURIE), "in {out:#?}");
    assert_eq!(widgets["w2"]["typeURI"], json!(FANCY_CURIE), "in {out:#?}");
}

/// The *compact* dict entry — a bare scalar rather than an object — is built by
/// a second arm of `build_mapping_entry_for_slot` that hardwires the slot's
/// range class. When the scalar slot it fills is the class's designator, the
/// entry names its own class, and canonicalising against the range class would
/// rewrite `FancyWidget` to `Widget` and warn about data that was right.
#[test]
fn compact_dict_entry_naming_a_subclass_is_not_rewritten_to_the_range_class() {
    let f = fixture();
    let (inst, issues) = f.load_result(json!({
        "widgets": {
            "w1": "https://w3id.org/linkml/examples/identity_canonical/FancyWidget",
            "w2": FANCY_CURIE
        }
    }));
    assert!(
        issues.is_empty(),
        "the entry names an accepted designator value of a real subclass: {issues:#?}"
    );
    let out = inst.to_json();
    let widgets = out.get("widgets").unwrap();
    assert_eq!(
        widgets["w1"]["typeURI"],
        json!(FANCY_CURIE),
        "canonicalised, and to the *subclass* the entry named: {out:#?}"
    );
    assert_eq!(widgets["w2"]["typeURI"], json!(FANCY_CURIE), "in {out:#?}");
}

/// The guard for `ClassView::get_uri(native, expand)`: `BareLeaf` declares a
/// `class_uri` and no distinguishing slot, so its schema-native URI is the only
/// thing that can name it. If that spelling ever drops out of the accepted set
/// the element silently becomes a `Node` and its designator is rewritten to
/// `Node`'s value — this test is what makes that loud.
#[test]
fn a_subclass_is_selected_by_its_native_uri_alone() {
    let f = fixture();
    let (inst, issues) = f.load_result(json!({
        "nodes": [{
            "kind": "https://w3id.org/linkml/examples/identity_canonical/BareLeaf",
            "name": "b1"
        }]
    }));
    assert!(issues.is_empty(), "an accepted spelling: {issues:#?}");
    let out = inst.to_json();
    assert_eq!(
        out["nodes"][0]["kind"],
        json!("https://example.org/canon/BareLeaf"),
        "selected as BareLeaf and canonicalised to its class_uri: {out:#?}"
    );
}

/// A designator value matching *no* accepted designator value is data the
/// loader cannot honour. Following the loader-tolerance precedent it is a
/// warning, not an error: the instance still loads, and the slot is filled
/// with the canonical value of the class that was actually selected.
#[test]
fn junk_designator_value_warns_and_is_replaced_by_the_canonical_value() {
    let f = fixture();
    let (inst, issues) = f.load_result(json!({
        "shapes": [{"typeURI": "Circle", "label": "a", "radius": 1.0}]
    }));
    let out = inst.to_json();
    assert_eq!(
        out["shapes"][0]["typeURI"],
        json!(CIRCLE_CURIE),
        "junk designator must be replaced by the canonical value: {out:#?}"
    );
    assert!(
        !issues.iter().any(|i| i.severity.is_error()),
        "junk designator must not be an error: {issues:#?}"
    );
    let warned: Vec<&ValidationResult> = issues
        .iter()
        .filter(|i| {
            i.problem_type == ValidationProblemType::SlotRangeViolation
                && i.subject.last().map(String::as_str) == Some("typeURI")
        })
        .collect();
    assert_eq!(
        warned.len(),
        1,
        "exactly one designator warning expected: {issues:#?}"
    );
    assert!(
        warned[0].detail.contains("Circle"),
        "the warning must name the rejected value: {:?}",
        warned[0].detail
    );
}

/// The canonical spelling is what makes a *matching* spelling silent: nothing
/// warns when the data already spelled an accepted value.
#[test]
fn accepted_designator_spellings_do_not_warn() {
    let f = fixture();
    let (_, issues) = f.load_result(json!({
        "shapes": [
            {"typeURI": CIRCLE_CURIE, "label": "a", "radius": 1.0},
            {"typeURI": "https://w3id.org/linkml/examples/identity_canonical/Circle",
             "label": "b", "radius": 2.0},
            {"label": "c", "radius": 3.0}
        ]
    }));
    assert!(issues.is_empty(), "no diagnostics expected: {issues:#?}");
}

// ---------------------------------------------------------------------------
// D6 — uri/uriorcurie identity components compared as IRIs
// ---------------------------------------------------------------------------

/// A reorder is invisible even when the list is addressed by IRI labels and the
/// document mixes spellings — the labels are stable, the order is not identity.
#[test]
fn mixed_spelling_reorder_is_a_zero_delta_diff() {
    let f = fixture();
    let deltas = diff2(
        &f,
        systems(vec![
            json!({"systemType": WGS84_CURIE, "value": "one"}),
            json!({"systemType": ETRS89_URI, "value": "two"}),
        ]),
        systems(vec![
            json!({"systemType": ETRS89_URI, "value": "two"}),
            json!({"systemType": WGS84_CURIE, "value": "one"}),
        ]),
    );
    assert!(deltas.is_empty(), "reorder must be invisible: {deltas:#?}");
}

/// The label a keyed list is addressed by is the *expanded* IRI, even when the
/// document spells the CURIE. This is what every other site then agrees with.
#[test]
fn identity_path_segments_are_expanded_iris() {
    let f = fixture();
    let deltas = diff2(
        &f,
        systems(vec![json!({"systemType": WGS84_CURIE, "value": "one"})]),
        systems(vec![json!({"systemType": WGS84_CURIE, "value": "two"})]),
    );
    let d = only(&deltas);
    assert_eq!(d.op, DeltaOp::Update);
    assert_eq!(
        d.path,
        vec![
            "systems".to_string(),
            WGS84_URI.to_string(),
            "value".to_string()
        ],
        "path segments are IRIs: {deltas:#?}"
    );
}

/// The two sides spell one element's identity differently. Rule 2 makes them
/// one identity, so the element is *matched* and the change is described
/// field-by-field under its IRI — never a Remove + Add of the whole element.
///
/// The re-spelling itself stays a delta: only the identity *comparison* is
/// IRI-normalised. The stored value of an ordinary `uri`-ranged slot is the
/// author's data, unlike a type designator, whose value is a function of the
/// class and is therefore rewritten at load.
#[test]
fn respelled_element_is_matched_never_replaced() {
    let f = fixture();
    let deltas = diff2(
        &f,
        systems(vec![json!({"systemType": WGS84_CURIE, "value": "one"})]),
        systems(vec![json!({"systemType": WGS84_URI, "value": "two"})]),
    );
    assert!(
        deltas.iter().all(|d| d.op == DeltaOp::Update),
        "no element churn: {deltas:#?}"
    );
    let mut paths: Vec<Vec<String>> = deltas.iter().map(|d| d.path.clone()).collect();
    paths.sort();
    assert_eq!(
        paths,
        vec![
            vec![
                "systems".to_string(),
                WGS84_URI.to_string(),
                "systemType".to_string()
            ],
            vec![
                "systems".to_string(),
                WGS84_URI.to_string(),
                "value".to_string()
            ],
        ],
        "one element, addressed by its IRI: {deltas:#?}"
    );
}

/// The invariant the two halves share: whatever segment diff emits,
/// `resolve_list_segment` must find — even when the document patch is applied
/// to spells the identity the other way round.
#[test]
fn patch_locates_the_element_under_spelling_drift() {
    let f = fixture();
    let deltas = diff2(
        &f,
        systems(vec![json!({"systemType": WGS84_URI, "value": "one"})]),
        systems(vec![json!({"systemType": WGS84_URI, "value": "two"})]),
    );
    // The document being patched spells the same system as a CURIE.
    let drifted = f.load(systems(vec![
        json!({"systemType": WGS84_CURIE, "value": "one"}),
    ]));
    let (patched, trace) = patch(&drifted, &deltas, PatchOptions::default()).unwrap();
    assert!(trace.failed.is_empty(), "delta must locate: {trace:#?}");
    assert_eq!(patched.to_json()["systems"][0]["value"], json!("two"));
}

/// The symmetric half: a segment that arrives already spelled as a CURIE — a
/// stored delta, a hand-written patch — must address the element whose label
/// expanded to the IRI. Labels are normalised on the way out; the incoming
/// segment is normalised through the same slot on the way in.
#[test]
fn a_curie_spelled_segment_applies_against_expanded_labels() {
    let f = fixture();
    let doc = systems(vec![
        json!({"systemType": WGS84_URI, "value": "one"}),
        json!({"systemType": ETRS89_URI, "value": "two"}),
    ]);
    let hand_written = vec![Delta {
        path: vec![
            "systems".to_string(),
            WGS84_CURIE.to_string(),
            "value".to_string(),
        ],
        op: DeltaOp::Update,
        old: Some(json!("one")),
        new: Some(json!("two")),
    }];
    let src = f.load(doc);
    let (patched, trace) = patch(&src, &hand_written, PatchOptions::default()).unwrap();
    assert!(
        trace.failed.is_empty(),
        "a curie segment and an expanded label are one identity: {trace:#?}"
    );
    assert_eq!(patched.to_json()["systems"][0]["value"], json!("two"));
    // …and the same segment navigates.
    assert_eq!(
        src.navigate_path(["systems", WGS84_CURIE, "value"])
            .map(LinkMLInstance::to_json),
        Some(json!("one"))
    );
}

/// `navigate_path` shares the resolver, so either spelling addresses the
/// element regardless of how the document spells it.
#[test]
fn navigate_finds_the_element_by_the_expanded_iri() {
    let f = fixture();
    let inst = f.load(systems(vec![
        json!({"systemType": WGS84_CURIE, "value": "one"}),
        json!({"systemType": ETRS89_CURIE, "value": "two"}),
    ]));
    let hit = inst
        .navigate_path(["systems", WGS84_URI, "value"])
        .expect("expanded IRI must address the CURIE-spelled element");
    assert_eq!(hit.to_json(), json!("one"));
}

/// Two spellings that expand to the same IRI are a duplicate identity, and the
/// instance lint is the voice that says so.
#[test]
fn instance_lint_sees_a_duplicate_across_spellings() {
    let f = fixture();
    let inst = f.load(systems(vec![
        json!({"systemType": WGS84_CURIE, "value": "one"}),
        json!({"systemType": WGS84_URI, "value": "two"}),
    ]));
    let warnings = lint_instance_identity(&inst);
    let dups: Vec<&ValidationResult> = warnings
        .iter()
        .filter(|w| w.problem_type == ValidationProblemType::DuplicateElementIdentity)
        .collect();
    assert_eq!(
        dups.len(),
        1,
        "a curie and its expansion collide: {warnings:#?}"
    );
    assert!(
        dups[0].detail.contains(WGS84_URI),
        "the duplicate is reported by its expanded IRI: {:?}",
        dups[0].detail
    );
}

/// Explicit round-trip pin for the shared-rule invariant: the segments diff
/// emits over a mixed-spelling pair are exactly what the resolver computes, so
/// every one of them addresses a real node and re-applying the diff reproduces
/// the target (list order is not identity, so the comparison is order-free).
#[test]
fn mixed_spelling_diff_segments_round_trip_through_the_resolver() {
    let f = fixture();
    let before = systems(vec![
        json!({"systemType": WGS84_CURIE, "value": "one"}),
        json!({"systemType": ETRS89_URI, "value": "two"}),
    ]);
    let after = systems(vec![
        json!({"systemType": ETRS89_CURIE, "value": "TWO"}),
        json!({"systemType": WGS84_URI, "value": "ONE"}),
        json!({"systemType": "ex:LAMBERT", "value": "three"}),
    ]);
    let deltas = diff2(&f, before.clone(), after.clone());
    assert!(!deltas.is_empty(), "there is real change here");
    let src = f.load(before);
    for d in &deltas {
        // Every emitted path but that of an Add must address an existing node.
        if d.op != DeltaOp::Add {
            assert!(
                src.navigate_path(d.path.iter()).is_some(),
                "diff emitted a segment its own resolver cannot follow: {:?}",
                d.path
            );
        }
    }
    let (patched, trace) = patch(&src, &deltas, PatchOptions::default()).unwrap();
    assert!(trace.failed.is_empty(), "{trace:#?}");
    assert_eq!(
        sorted_systems(&patched.to_json()),
        sorted_systems(&f.json(after))
    );
}

/// `systems` as a set: keyed patching edits in place, so the surviving element
/// order is the source's, and order is not what a keyed list is compared by.
fn sorted_systems(v: &JsonValue) -> Vec<String> {
    let mut out: Vec<String> = v["systems"]
        .as_array()
        .unwrap()
        .iter()
        .map(JsonValue::to_string)
        .collect();
    out.sort();
    out
}

// ---------------------------------------------------------------------------
// JSON / RDF agreement
// ---------------------------------------------------------------------------

/// The RDF loader has always written the canonical designator (it never
/// harvests the designator predicate, it fills it from the class). Once JSON
/// canonicalises too, the two loaders agree on a document that spelled the
/// designator non-canonically.
#[cfg(feature = "ttl")]
#[test]
fn json_and_rdf_loaders_agree_on_the_designator_spelling() {
    use linkml_runtime::rdf_import::{import_turtle, ImportOptions};
    use linkml_runtime::turtle::{turtle_to_string, TurtleOptions};

    let mut p = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    p.push("tests/data/identity_canonical.yaml");
    let schema = from_yaml(&p).unwrap();
    let mut sv = SchemaView::new();
    sv.add_schema(schema.clone()).unwrap();
    let conv = converter_from_schema(&schema);
    let container = sv
        .get_class(&Identifier::new("Container"), &conv)
        .unwrap()
        .expect("class not found");

    let doc = json!({
        "shapes": [{
            "typeURI": "https://w3id.org/linkml/examples/identity_canonical/Circle",
            "label": "a",
            "radius": 1.0
        }]
    });
    let from_json = load_json_str(&doc.to_string(), &sv, &container, &conv)
        .unwrap()
        .into_instance()
        .unwrap();

    let ttl = turtle_to_string(
        &from_json,
        &sv,
        &schema,
        &conv,
        TurtleOptions { skolem: false },
    )
    .unwrap();
    let stream = import_turtle(
        std::io::Cursor::new(ttl.as_bytes()),
        sv.clone(),
        conv.clone(),
        &["Container"],
        ImportOptions::default(),
    )
    .unwrap();
    let from_rdf = stream
        .filter_map(|r| r.ok())
        .find_map(|(c, i)| if c == "Container" { Some(i) } else { None })
        .expect("one Container");

    assert_eq!(
        from_rdf.to_json()["shapes"][0]["typeURI"],
        json!(CIRCLE_CURIE),
        "the RDF loader's canonical designator"
    );
    assert_eq!(
        from_json.to_json()["shapes"][0]["typeURI"],
        from_rdf.to_json()["shapes"][0]["typeURI"],
        "JSON and RDF loaders must agree"
    );
}
