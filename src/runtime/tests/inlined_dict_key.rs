//! Spec addendum rule 5 — the inlined-dict key is real data (D5).
//!
//! LinkML's `inlined` dict form says the mapping key *is* the element's
//! key/identifier value; the payload is allowed to leave it out. The loader
//! used never to write it back, so a `required` key slot produced a
//! `MissingSlotValue` error on perfectly legal data, and the object that came
//! out of the load did not carry its own key.
//!
//! Three facts are pinned here:
//!
//! * the dict key is **injected** into the key slot when the payload omits it,
//!   before the required/cardinality constraints run and before the type
//!   designator is filled, so it flows through the same canonicalisation every
//!   other designator value does (rule 2);
//! * a payload value that **disagrees** with the dict key is a warning naming
//!   both. The payload value is data and is what gets *stored*; the dict key is
//!   the address and is what the mapping stays keyed by;
//! * for a dict whose key slot **is** the type designator, a dict key that is
//!   no accepted designator value is a warning.

use linkml_runtime::{
    load_json_str, LinkMLInstance, ValidationProblemType, ValidationResult, ValidationSeverity,
};
use linkml_schemaview::identifier::{converter_from_schema, Identifier};
use linkml_schemaview::io::from_yaml;
use linkml_schemaview::schemaview::{ClassView, SchemaView};
use linkml_schemaview::Converter;
use serde_json::{json, Value as JsonValue};
use std::path::PathBuf;

/// `LinearCoordinate`'s canonical designator value: its `class_uri` (the slot
/// range is `uriorcurie`, so the canonical spelling is the CURIE).
const LINEAR_CANONICAL: &str = "ex:Linear";
/// The same class's *schema-native* URI — a second accepted spelling, and the
/// one asset360's committed data uses as the dict key while spelling the
/// payload with the `class_uri`.
const LINEAR_NATIVE: &str = "https://w3id.org/linkml/examples/inlined_dict_key/LinearCoordinate";
const WGS84_CURIE: &str = "ex:WGS84";
const WGS84_URI: &str = "https://example.org/dk/WGS84";

struct Fixture {
    sv: SchemaView,
    conv: Converter,
    container: ClassView,
}

fn fixture() -> Fixture {
    let mut p = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    p.push("tests/data/inlined_dict_key.yaml");
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
}

fn errors(issues: &[ValidationResult]) -> Vec<&ValidationResult> {
    issues.iter().filter(|i| i.severity.is_error()).collect()
}

// ---------------------------------------------------------------------------
// Injection
// ---------------------------------------------------------------------------

/// The payload omits the key slot, as the `inlined` contract allows. The dict
/// key fills it: no `MissingSlotValue` for the `required` key, and the object
/// carries its own key out through `to_json`.
#[test]
fn dict_key_fills_the_key_slot_the_payload_omitted() {
    let f = fixture();
    let (inst, issues) = f.load_result(json!({
        "people": {"p1": {"name": "Ann"}, "p2": {"name": "Bo"}}
    }));
    assert!(
        errors(&issues).is_empty(),
        "the dict key satisfies the required key slot: {issues:#?}"
    );
    assert!(
        !issues
            .iter()
            .any(|i| i.problem_type == ValidationProblemType::MissingSlotValue),
        "no missing-value diagnostic of any severity: {issues:#?}"
    );
    let out = inst.to_json();
    assert_eq!(out["people"]["p1"]["pid"], json!("p1"), "in {out:#?}");
    assert_eq!(out["people"]["p2"]["pid"], json!("p2"), "in {out:#?}");
}

/// The payload states the same key the dict does. Nothing to inject, nothing
/// to say.
#[test]
fn payload_key_equal_to_the_dict_key_is_silent() {
    let f = fixture();
    let (inst, issues) = f.load_result(json!({
        "people": {"p1": {"pid": "p1", "name": "Ann"}}
    }));
    assert!(issues.is_empty(), "no diagnostics expected: {issues:#?}");
    assert_eq!(inst.to_json()["people"]["p1"]["pid"], json!("p1"));
}

/// The payload contradicts the dict key. Rule 5 makes this a **warning**, not
/// an error, and the ruling on which value survives is recorded here: the
/// payload is the element's data and is stored; the dict key is the address and
/// the mapping stays keyed by it. Both values are named in the message so the
/// author can see which one to change.
#[test]
fn payload_key_disagreeing_with_the_dict_key_warns_and_the_payload_is_stored() {
    let f = fixture();
    let (inst, issues) = f.load_result(json!({
        "people": {"p1": {"pid": "p2", "name": "Ann"}}
    }));
    assert!(
        errors(&issues).is_empty(),
        "a divergence is a warning, never an error: {issues:#?}"
    );
    let warned: Vec<&ValidationResult> = issues
        .iter()
        .filter(|i| i.severity == ValidationSeverity::Warning)
        .collect();
    assert_eq!(warned.len(), 1, "exactly one warning: {issues:#?}");
    let detail = &warned[0].detail;
    assert!(
        detail.contains("p1") && detail.contains("p2"),
        "the warning must name both values: {detail:?}"
    );
    assert_eq!(
        warned[0].subject,
        vec!["people".to_string(), "p1".to_string(), "pid".to_string()],
        "reported at the key slot of the entry the dict key addresses"
    );
    let out = inst.to_json();
    assert_eq!(
        out["people"]["p1"]["pid"],
        json!("p2"),
        "the payload value is the stored data: {out:#?}"
    );
    assert!(
        out["people"].get("p2").is_none(),
        "the mapping stays addressed by its dict key: {out:#?}"
    );
}

/// A key slot whose range descends from `uri` compares as an IRI, not as a
/// string (rule 2): a CURIE dict key and its expansion in the payload are one
/// identity and must not be reported as a divergence.
#[test]
fn a_curie_dict_key_and_its_expanded_payload_are_one_identity() {
    let f = fixture();
    let (_, issues) = f.load_result(json!({
        "systems": {WGS84_CURIE: {"systemType": WGS84_URI, "label": "one"}}
    }));
    assert!(
        issues.is_empty(),
        "a curie and its expansion are one identity: {issues:#?}"
    );
}

// ---------------------------------------------------------------------------
// Designator-keyed dicts
// ---------------------------------------------------------------------------

/// The dict key names the entry's class. An accepted spelling selects that
/// class, and the injected value is canonicalised on the way in exactly as a
/// payload-supplied designator is — the stored value stays canonical.
#[test]
fn an_accepted_designator_dict_key_selects_the_class_and_is_canonicalised() {
    let f = fixture();
    let (inst, issues) = f.load_result(json!({
        "coords": {LINEAR_NATIVE: {"measure": 1.5}}
    }));
    assert!(
        issues.is_empty(),
        "the native URI is an accepted designator value: {issues:#?}"
    );
    let out = inst.to_json();
    let entry = &out["coords"][LINEAR_NATIVE];
    assert_eq!(
        entry["typeURI"],
        json!(LINEAR_CANONICAL),
        "injected from the dict key and canonicalised: {out:#?}"
    );
    assert_eq!(
        entry["measure"],
        json!(1.5),
        "the subclass's own slot survives, so the subclass was selected: {out:#?}"
    );
}

/// A dict key that is no accepted designator value at all is a warning — the
/// case D5 found silently masked, because `populate_type_designator` filled the
/// slot from the range class and nothing ever looked at the key.
#[test]
fn a_designator_dict_key_that_is_not_accepted_warns() {
    let f = fixture();
    let (inst, issues) = f.load_result(json!({
        "coords": {"Whatever": {"value": "v"}}
    }));
    assert!(
        errors(&issues).is_empty(),
        "an unaccepted key is a warning, never an error: {issues:#?}"
    );
    assert_eq!(issues.len(), 1, "exactly one warning: {issues:#?}");
    assert!(
        issues[0].detail.contains("Whatever"),
        "the warning must name the rejected key: {:?}",
        issues[0].detail
    );
    let out = inst.to_json();
    assert_eq!(
        out["coords"]["Whatever"]["typeURI"],
        json!("dk:Coordinate"),
        "the range class's canonical value is what is stored: {out:#?}"
    );
}

/// The payload is fine on its own terms but the dict key is junk. The key's own
/// rejection has to be heard even when nothing else is wrong with the entry.
#[test]
fn an_unaccepted_designator_key_is_heard_even_when_the_payload_is_accepted() {
    let f = fixture();
    let (_, issues) = f.load_result(json!({
        "coords": {"Whatever": {"typeURI": LINEAR_CANONICAL, "measure": 2.0}}
    }));
    assert!(errors(&issues).is_empty(), "warnings only: {issues:#?}");
    assert!(
        issues.iter().any(|i| i.detail.contains("Whatever")
            && i.detail.contains("not an accepted designator value")),
        "the unaccepted dict key must be named: {issues:#?}"
    );
}

/// The compact form of a designator-*keyed* dict: the dict key is the only
/// thing that names the class, and the bare scalar fills the first ordinary
/// slot. Selection, injection and canonicalisation all have to come off the key
/// alone.
#[test]
fn a_compact_entry_takes_its_class_from_a_designator_dict_key() {
    let f = fixture();
    let (inst, issues) = f.load_result(json!({
        "coords": {LINEAR_NATIVE: "somevalue"}
    }));
    assert!(issues.is_empty(), "no diagnostics expected: {issues:#?}");
    let out = inst.to_json();
    let entry = &out["coords"][LINEAR_NATIVE];
    assert_eq!(
        entry["typeURI"],
        json!(LINEAR_CANONICAL),
        "the class the key named, canonicalised: {out:#?}"
    );
    assert_eq!(
        entry["value"],
        json!("somevalue"),
        "the compact scalar lands in the first ordinary slot: {out:#?}"
    );
}

/// The asset360 shape, reduced: the dict key spells the class with its
/// schema-native URI and the payload spells it with the `class_uri`. Both are
/// accepted designator values of the same class, but the payload is
/// canonicalised at load and the key is not, so the *loaded* entry really is
/// addressed by one IRI and carries another. That contradiction is the warning.
#[test]
fn native_uri_key_against_class_uri_payload_is_a_divergence() {
    let f = fixture();
    let (inst, issues) = f.load_result(json!({
        "coords": {LINEAR_NATIVE: {"typeURI": LINEAR_CANONICAL, "measure": 3.0}}
    }));
    assert!(errors(&issues).is_empty(), "warnings only: {issues:#?}");
    let divergences: Vec<&ValidationResult> = issues
        .iter()
        .filter(|i| i.detail.contains("disagrees"))
        .collect();
    assert_eq!(divergences.len(), 1, "one divergence: {issues:#?}");
    assert!(
        divergences[0].detail.contains(LINEAR_NATIVE)
            && divergences[0].detail.contains(LINEAR_CANONICAL),
        "both spellings named: {:?}",
        divergences[0].detail
    );
    let out = inst.to_json();
    assert_eq!(
        out["coords"][LINEAR_NATIVE]["typeURI"],
        json!(LINEAR_CANONICAL),
        "the payload value, canonicalised, is the stored data: {out:#?}"
    );
}

/// The mirror image of the test above, and the reason the divergence check
/// compares the value **as stored** rather than the raw payload: here the key is
/// the canonical spelling and the payload is the native URI, so rule 2 rewrites
/// the payload to the key's own spelling. The entry that comes out agrees with
/// the key it is written under, and reloading that output is silent — so the
/// load must be silent too. Comparing raw payloads would warn here, and the
/// message would name a "stored" value that is not what was stored.
#[test]
fn a_payload_canonicalised_onto_the_dict_key_is_not_a_divergence() {
    let f = fixture();
    let (inst, issues) = f.load_result(json!({
        "coords": {LINEAR_CANONICAL: {"typeURI": LINEAR_NATIVE, "measure": 4.0}}
    }));
    assert!(
        issues.is_empty(),
        "canonicalisation made the two agree before anything was stored: {issues:#?}"
    );
    let out = inst.to_json();
    assert_eq!(
        out["coords"][LINEAR_CANONICAL]["typeURI"],
        json!(LINEAR_CANONICAL),
        "in {out:#?}"
    );
    // The invariant that makes the silence correct: the emitted document reloads
    // without a word.
    let (_, reloaded) = f.load_result(out.clone());
    assert!(reloaded.is_empty(), "round-trip must stay silent: {out:#?}");
}

/// The same invariant from the other side: the asset360 divergence is real
/// precisely because it *survives* the round-trip — the emitted document is
/// still addressed by one IRI and still carries another.
#[test]
fn a_real_divergence_survives_the_round_trip() {
    let f = fixture();
    let (inst, _) = f.load_result(json!({
        "coords": {LINEAR_NATIVE: {"typeURI": LINEAR_CANONICAL, "measure": 3.0}}
    }));
    let (_, reloaded) = f.load_result(inst.to_json());
    assert_eq!(
        reloaded
            .iter()
            .filter(|i| i.detail.contains("disagrees"))
            .count(),
        1,
        "the contradiction is in the data, not in the load: {reloaded:#?}"
    );
}
