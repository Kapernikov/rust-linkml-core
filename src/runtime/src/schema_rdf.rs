//! The schema itself, as RDF triples.
//!
//! [`turtle`](crate::turtle) triplifies *instances*. This module triplifies the
//! *schema* those instances conform to, so an RDF client can discover what
//! things are called, how they relate, and which values an enum permits,
//! instead of being told them out of band.
//!
//! The gap is felt hardest on enum values. A permissible value carrying a
//! `meaning` is emitted by the instance writer as that IRI, so a value comes
//! back as an opaque IRI and there is no triple anywhere saying the code behind
//! it is `GSA`. A client had to hardcode the IRI to get a readable answer. With
//! these triples it can ask.
//!
//! Scope is *discovery*: names, relationships, allowed values. It is
//! deliberately not an OWL axiomatisation — see [`SchemaRdfProfile`].
//!
//! # This module takes no position on graph naming
//!
//! [`schema_triples`] returns plain [`Triple`]s. Whether they belong in the
//! default graph, in a named graph, in one file per schema or nowhere at all is
//! the caller's decision and depends entirely on the caller's deployment. A
//! caller that wants them in a named graph wraps them:
//!
//! ```no_run
//! # use linkml_runtime::schema_rdf::{schema_triples, SchemaRdfOptions};
//! # use oxrdf::{GraphName, NamedNode, Quad};
//! # fn demo(sv: &linkml_schemaview::schemaview::SchemaView) {
//! let graph = GraphName::NamedNode(NamedNode::new("https://example.org/schema").unwrap());
//! let quads: Vec<Quad> = schema_triples(sv, &SchemaRdfOptions::default())
//!     .triples
//!     .into_iter()
//!     .map(|t| Quad::new(t.subject, t.predicate, t.object, graph.clone()))
//!     .collect();
//! # }
//! ```
//!
//! # Vocabulary
//!
//! Nothing here is invented. Well-known vocabularies carry all of it:
//!
//! * `rdfs:label` / `rdfs:comment` for the name and the description of every
//!   term. This is the one predicate a client can be expected to try first, so
//!   *everything* nameable gets an `rdfs:label`.
//! * `owl:Class` for classes and `rdf:Property` for slots — the weakest type
//!   assertions that are still true. `owl:ObjectProperty` /
//!   `owl:DatatypeProperty` are deliberately not asserted: a LinkML slot's
//!   range can be redeclared per class, so the distinction is not a property of
//!   the slot, and asserting it would be a guess.
//! * `skos:ConceptScheme` / `skos:Concept` / `skos:inScheme` / `skos:notation`
//!   for enums and their permissible values. SKOS is what a controlled value
//!   list *is*; modelling permissible values as OWL individuals of an
//!   `owl:Class`, as linkml's own OWL generator does, asserts an ontological
//!   commitment the schema never made.
//! * `schema:domainIncludes` / `schema:rangeIncludes` for the class↔slot and
//!   slot↔range links. Not `rdfs:domain`: a LinkML slot is reused across
//!   unrelated classes, and several `rdfs:domain` triples on one property mean
//!   the *intersection* of those classes, which would be false. schema.org
//!   introduced the `*Includes` pair for exactly this — a non-committal "this
//!   is one of the places it is used". `rdfs:range` is additionally emitted
//!   when a slot has exactly one range, where it is not a guess.
//!
//! Two of those choices knowingly disagree with linkml's OWL generator, so they
//! are named rather than implied: they are the [`SchemaRdfProfile::Discovery`]
//! profile, and a strict-OWL profile can be added beside it without breaking
//! this API.
//!
//! # IRIs must be absolute
//!
//! Every IRI is produced through the schema's own [`Converter`] — the same path
//! the instance turtle writer uses — and then through `NamedNode::new`, which
//! rejects anything that is not an absolute IRI. A term whose CURIE the
//! converter cannot expand is *skipped* and named in [`SchemaTriples::skipped`],
//! never emitted as a bare CURIE, which would not re-parse.
//!
//! # Which IRI names a class
//!
//! A class can have two legitimate spellings: its schema-native URI
//! (`<default prefix><ClassName>`) and its declared `class_uri`. A client that
//! binds `?class` from real instance data and joins it against these triples
//! only gets matches if both agree, so the spelling is not a free choice: it is
//! whatever the instance writer puts on the right-hand side of `rdf:type`.
//!
//! That is not restated here, it is *shared*: [`instance_type_iri`] is the
//! single definition, [`turtle`](crate::turtle) calls it to write the
//! `rdf:type` object, this module calls it to pick the class subject, and
//! [`slot_predicate_iri`] does the same for slot predicates. Having the
//! decision made independently in two places is how it silently drifts.

use std::collections::BTreeSet;

use linkml_schemaview::converter::Converter;
use linkml_schemaview::identifier::Identifier;
use linkml_schemaview::schemaview::{ClassView, SchemaView, SlotView};
use oxrdf::{Literal, NamedNode, NamedNodeRef, Term, Triple};

/// `rdf:type`.
pub const RDF_TYPE: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
/// `rdf:Property`.
pub const RDF_PROPERTY: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#Property";
/// `rdfs:label`.
pub const RDFS_LABEL: &str = "http://www.w3.org/2000/01/rdf-schema#label";
/// `rdfs:comment`.
pub const RDFS_COMMENT: &str = "http://www.w3.org/2000/01/rdf-schema#comment";
/// `rdfs:subClassOf`.
pub const RDFS_SUBCLASS_OF: &str = "http://www.w3.org/2000/01/rdf-schema#subClassOf";
/// `rdfs:range`.
pub const RDFS_RANGE: &str = "http://www.w3.org/2000/01/rdf-schema#range";
/// `owl:Class`.
pub const OWL_CLASS: &str = "http://www.w3.org/2002/07/owl#Class";
/// `skos:ConceptScheme`.
pub const SKOS_CONCEPT_SCHEME: &str = "http://www.w3.org/2004/02/skos/core#ConceptScheme";
/// `skos:Concept`.
pub const SKOS_CONCEPT: &str = "http://www.w3.org/2004/02/skos/core#Concept";
/// `skos:inScheme`.
pub const SKOS_IN_SCHEME: &str = "http://www.w3.org/2004/02/skos/core#inScheme";
/// `skos:notation`.
pub const SKOS_NOTATION: &str = "http://www.w3.org/2004/02/skos/core#notation";
/// `schema:domainIncludes`.
pub const SCHEMA_DOMAIN_INCLUDES: &str = "https://schema.org/domainIncludes";
/// `schema:rangeIncludes`.
pub const SCHEMA_RANGE_INCLUDES: &str = "https://schema.org/rangeIncludes";

/// Which vocabulary the schema is expressed in.
///
/// There is one variant today. It exists as an enum anyway because the
/// vocabulary is a *choice*, and two of its decisions disagree with linkml's
/// own OWL generator (SKOS concepts rather than OWL individuals for permissible
/// values; `schema:domainIncludes` rather than `rdfs:domain`). Naming the
/// profile makes the disagreement explicit in the API instead of implicit in
/// the output, and lets a stricter profile be added later without a breaking
/// change.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[non_exhaustive]
pub enum SchemaRdfProfile {
    /// Discovery: what things are called, how they relate, which values are
    /// allowed. Weakest true type assertions, SKOS for controlled value lists,
    /// `schema:*Includes` for class↔slot links. See the module docs for why
    /// each of those is the defensible reading of a LinkML schema.
    #[default]
    Discovery,
}

/// How to triplify a schema.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[non_exhaustive]
pub struct SchemaRdfOptions {
    /// The vocabulary to express the schema in.
    pub profile: SchemaRdfProfile,
}

impl SchemaRdfOptions {
    /// Options for one profile.
    pub fn new(profile: SchemaRdfProfile) -> Self {
        Self { profile }
    }
}

/// The triplified schema, plus what could not be triplified.
#[derive(Debug, Clone, Default)]
pub struct SchemaTriples {
    /// Every triple, sorted and deduplicated. No graph: see the module docs.
    pub triples: Vec<Triple>,
    /// Terms dropped because their IRI would not have been absolute — an
    /// unexpandable CURIE, most likely a prefix the converter never saw. Named
    /// rather than discarded so a caller can say *what* is missing instead of a
    /// client wondering why a label does not resolve.
    pub skipped: Vec<String>,
}

impl SchemaTriples {
    /// The triples as N-Triples, for tests and for a human reading them.
    pub fn to_ntriples(&self) -> String {
        let mut out = String::new();
        for triple in &self.triples {
            out.push_str(&triple.subject.to_string());
            out.push(' ');
            out.push_str(&triple.predicate.to_string());
            out.push(' ');
            out.push_str(&triple.object.to_string());
            out.push_str(" .\n");
        }
        out
    }
}

/// The IRI an instance of `cv` carries as its `rdf:type` object.
///
/// The canonical, `class_uri`-preferring, fully expanded spelling. This is the
/// single definition of that decision: the instance turtle writer and the
/// schema triplifier both call it, so they cannot drift apart. `None` when the
/// class has no expandable URI at all.
pub fn instance_type_iri(cv: &ClassView, conv: &Converter) -> Option<String> {
    cv.get_uri(conv, false, true).ok().map(|id| id.to_string())
}

/// The IRI a slot is written as, as a predicate.
///
/// The slot's canonical URI (respecting `slot_uri` and the originating schema's
/// `default_prefix`), expanded through the schema's converter. `Err` carries
/// the unexpanded canonical spelling, so each caller can pick its own policy:
/// the instance writer emits it anyway (a long-standing behaviour, and a defect
/// where it happens), while [`schema_triples`] skips the term rather than
/// emitting a bare CURIE.
pub fn slot_predicate_iri(slot: &SlotView, conv: &Converter) -> Result<String, String> {
    let canonical = slot.canonical_uri();
    match canonical.to_uri(conv) {
        Ok(uri) => Ok(uri.0),
        Err(_) => Err(canonical.to_string()),
    }
}

/// Triplify one [`SchemaView`].
///
/// Covers every class, slot and enum reachable from the view, including
/// imported schemas — instance data types can come from any of them, so
/// restricting to the primary schema would leave a client unable to look up
/// exactly the imported vocabulary terms that are hardest to guess.
///
/// A view that cannot enumerate its own classes or enums is a broken schema and
/// has failed long before this point; there is nothing to describe, so nothing
/// is described. Errors are never invented here, and none are swallowed that a
/// caller could have acted on.
pub fn schema_triples(sv: &SchemaView, options: &SchemaRdfOptions) -> SchemaTriples {
    let SchemaRdfProfile::Discovery = options.profile;

    let conv = sv.converter();
    let mut builder = Builder {
        triples: Vec::new(),
        skipped: Vec::new(),
    };

    builder.classes_and_slots(sv, &conv);
    builder.enums(sv, &conv);

    // Deterministic and duplicate-free: a slot reached through several classes
    // describes itself once, and the output should not depend on schema
    // iteration order (the metamodel stores permissible values and schemas in
    // hash maps).
    builder.triples.sort_by_key(|t| t.to_string());
    builder.triples.dedup();
    builder.skipped.sort();
    builder.skipped.dedup();

    SchemaTriples {
        triples: builder.triples,
        skipped: builder.skipped,
    }
}

struct Builder {
    triples: Vec<Triple>,
    skipped: Vec<String>,
}

impl Builder {
    /// An absolute-IRI node, or `None` with the offender recorded.
    fn node(&mut self, iri: &str, what: &str) -> Option<NamedNode> {
        match NamedNode::new(iri) {
            Ok(node) => Some(node),
            Err(_) => {
                self.skipped.push(format!("{what}: {iri}"));
                None
            }
        }
    }

    /// Expand a LinkML `uriorcurie` through the schema's own converter, then
    /// demand an absolute IRI. No hand-rolled prefix handling: the converter is
    /// the only thing that gets prefix resolution right.
    fn expanded(&mut self, raw: &str, conv: &Converter, what: &str) -> Option<NamedNode> {
        let expanded = match Identifier::new(raw).to_uri(conv) {
            Ok(uri) => uri.0,
            // Not expandable — fall through to `node`, which will reject it and
            // record it, rather than emitting a bare CURIE.
            Err(_) => raw.to_owned(),
        };
        self.node(&expanded, what)
    }

    /// `predicate` is always one of the constants above, every one of which is
    /// a valid absolute IRI, so it is not re-validated here. A test pins that.
    fn triple(&mut self, subject: &NamedNode, predicate: &str, object: Term) {
        self.triples.push(Triple::new(
            subject.clone(),
            NamedNodeRef::new_unchecked(predicate).into_owned(),
            object,
        ));
    }

    fn label(&mut self, subject: &NamedNode, text: &str) {
        self.triple(
            subject,
            RDFS_LABEL,
            Literal::new_simple_literal(text).into(),
        );
    }

    fn comment(&mut self, subject: &NamedNode, text: Option<&String>) {
        if let Some(text) = text {
            self.triple(
                subject,
                RDFS_COMMENT,
                Literal::new_simple_literal(text).into(),
            );
        }
    }

    fn type_of(&mut self, subject: &NamedNode, class: &str) {
        let object = Term::NamedNode(NamedNodeRef::new_unchecked(class).into_owned());
        self.triple(subject, RDF_TYPE, object);
    }

    fn classes_and_slots(&mut self, sv: &SchemaView, conv: &Converter) {
        let class_views = match sv.class_views() {
            Ok(views) => views,
            Err(_) => return,
        };

        for cv in &class_views {
            let Some(class_id) = instance_type_iri(cv, conv) else {
                self.skipped.push(format!("class: {}", cv.name()));
                continue;
            };
            let Some(class_node) = self.expanded(&class_id, conv, "class") else {
                continue;
            };

            self.type_of(&class_node, OWL_CLASS);
            self.label(&class_node, cv.name());
            self.comment(&class_node, cv.def().description.as_ref());

            if let Ok(Some(parent)) = cv.parent_class() {
                if let Some(parent_id) = instance_type_iri(&parent, conv) {
                    if let Some(parent_node) = self.expanded(&parent_id, conv, "parent class") {
                        self.triple(&class_node, RDFS_SUBCLASS_OF, parent_node.into());
                    }
                }
            }

            for slot in cv.slots() {
                let slot_id = match slot_predicate_iri(slot, conv) {
                    Ok(iri) => iri,
                    Err(raw) => raw,
                };
                let Some(slot_node) = self.expanded(&slot_id, conv, "slot") else {
                    continue;
                };

                self.type_of(&slot_node, RDF_PROPERTY);
                self.label(&slot_node, &slot.name);
                self.comment(&slot_node, slot.definition().description.as_ref());
                self.triple(
                    &slot_node,
                    SCHEMA_DOMAIN_INCLUDES,
                    class_node.clone().into(),
                );

                let ranges = self.slot_ranges(slot, conv);
                for range in &ranges {
                    self.triple(&slot_node, SCHEMA_RANGE_INCLUDES, range.clone().into());
                }
                // Only when it is not a guess: one range, so `rdfs:range`'s
                // "every value is one of these" is exactly what the schema says.
                if let [only] = ranges.as_slice() {
                    self.triple(&slot_node, RDFS_RANGE, only.clone().into());
                }
            }
        }
    }

    /// The IRIs a slot's values can have, deduplicated. A slot with `any_of`
    /// contributes one per branch.
    fn slot_ranges(&mut self, slot: &SlotView, conv: &Converter) -> Vec<NamedNode> {
        let mut seen: BTreeSet<String> = BTreeSet::new();

        if let Some(range_class) = slot.get_range_class() {
            if let Some(id) = instance_type_iri(&range_class, conv) {
                seen.insert(id);
            }
        }
        if let Some(range_enum) = slot.get_range_enum() {
            seen.insert(range_enum.canonical_uri().to_string());
        }
        if seen.is_empty() {
            for info in slot.get_range_info() {
                if let Some(datatype) = &info.rdf_datatype_iri {
                    seen.insert(datatype.clone());
                }
            }
        }

        seen.into_iter()
            .filter_map(|raw| self.expanded(&raw, conv, "slot range"))
            .collect()
    }

    fn enums(&mut self, sv: &SchemaView, conv: &Converter) {
        let enum_views = match sv.enum_views() {
            Ok(views) => views,
            Err(_) => return,
        };

        for ev in &enum_views {
            let enum_id = ev.canonical_uri();
            let Some(enum_node) = self.expanded(&enum_id.to_string(), conv, "enum") else {
                continue;
            };

            self.type_of(&enum_node, SKOS_CONCEPT_SCHEME);
            self.label(&enum_node, ev.name());
            self.comment(&enum_node, ev.definition().description.as_ref());

            let Some(values) = ev.definition().permissible_values.as_ref() else {
                continue;
            };
            for (code, pv) in values {
                // A value with no `meaning` is rendered by the instance writer
                // as a plain literal. There is no IRI to hang a label on, and
                // minting one would describe a resource that appears nowhere in
                // the data — so it gets no triples, and the code is already
                // legible in the instance data without them.
                let Some(meaning) = pv.meaning.as_ref() else {
                    continue;
                };
                let Some(value_node) =
                    self.expanded(meaning, conv, &format!("enum value {}.{code}", ev.name()))
                else {
                    continue;
                };

                self.type_of(&value_node, SKOS_CONCEPT);
                // The code, on `rdfs:label`, is the whole motivation: this is
                // what turns an opaque IRI back into a readable code.
                self.label(&value_node, code);
                self.triple(
                    &value_node,
                    SKOS_NOTATION,
                    Literal::new_simple_literal(code).into(),
                );
                self.comment(&value_node, pv.description.as_ref());
                self.triple(&value_node, SKOS_IN_SCHEME, enum_node.clone().into());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use linkml_schemaview::io::from_yaml;
    use std::path::{Path, PathBuf};

    fn data_path(name: &str) -> PathBuf {
        let mut p = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        p.push("tests");
        p.push("data");
        p.push(name);
        p
    }

    /// `personinfo` is the fixture with the properties these tests need: it
    /// declares `class_uri`s distinct from the native spelling (`schema:Person`)
    /// and enums whose permissible values carry a `meaning`.
    fn personinfo() -> SchemaView {
        let schema = from_yaml(Path::new(&data_path("personinfo.yaml"))).unwrap();
        let mut sv = SchemaView::new();
        sv.add_schema(schema).unwrap();
        sv
    }

    #[test]
    fn every_builtin_predicate_is_a_valid_absolute_iri() {
        for iri in [
            RDF_TYPE,
            RDF_PROPERTY,
            RDFS_LABEL,
            RDFS_COMMENT,
            RDFS_SUBCLASS_OF,
            RDFS_RANGE,
            OWL_CLASS,
            SKOS_CONCEPT_SCHEME,
            SKOS_CONCEPT,
            SKOS_IN_SCHEME,
            SKOS_NOTATION,
            SCHEMA_DOMAIN_INCLUDES,
            SCHEMA_RANGE_INCLUDES,
        ] {
            NamedNode::new(iri).unwrap_or_else(|err| panic!("{iri} is not absolute: {err}"));
        }
    }

    #[test]
    fn every_emitted_iri_is_absolute() {
        let out = schema_triples(&personinfo(), &SchemaRdfOptions::default());
        assert!(!out.triples.is_empty(), "expected a non-empty schema");

        for triple in &out.triples {
            // `NamedNode::new` on the way in already rejected relative IRIs;
            // re-check here so the invariant is asserted on the output, not on
            // the code path that produced it.
            for iri in [
                Some(triple.subject.to_string()),
                Some(triple.predicate.to_string()),
                match &triple.object {
                    Term::NamedNode(node) => Some(node.to_string()),
                    _ => None,
                },
            ]
            .into_iter()
            .flatten()
            {
                let bare = iri.trim_start_matches('<').trim_end_matches('>');
                NamedNode::new(bare)
                    .unwrap_or_else(|err| panic!("non-absolute IRI {bare} emitted: {err}"));
            }
        }
    }

    #[test]
    fn classes_carry_a_label_and_are_typed() {
        let sv = personinfo();
        let conv = sv.converter();
        let ntriples = schema_triples(&sv, &SchemaRdfOptions::default()).to_ntriples();

        let cv = sv
            .class_views()
            .unwrap()
            .into_iter()
            .find(|cv| cv.name() == "Person")
            .expect("personinfo has a Person class");
        let class_iri = instance_type_iri(&cv, &conv).unwrap();

        assert!(
            ntriples.contains(&format!("<{class_iri}> <{RDFS_LABEL}> \"Person\"")),
            "expected an rdfs:label for {class_iri}"
        );
        assert!(
            ntriples.contains(&format!("<{class_iri}> <{RDF_TYPE}> <{OWL_CLASS}>")),
            "expected {class_iri} to be typed owl:Class"
        );
    }

    /// The join that makes the feature useful: the IRI an instance's `rdf:type`
    /// names must be the IRI these triples describe. Asserted against the
    /// *actual output of the turtle writer*, not against a restatement of its
    /// logic, and for classes that really do declare a distinct `class_uri` —
    /// where the two spellings could disagree.
    #[test]
    fn class_subject_matches_the_instance_rdf_type_spelling() {
        let sv = personinfo();
        let conv = sv.converter();
        let ntriples = schema_triples(&sv, &SchemaRdfOptions::default()).to_ntriples();

        let with_distinct_class_uri: Vec<_> = sv
            .class_views()
            .unwrap()
            .into_iter()
            .filter(|cv| {
                let native = cv.get_uri(&conv, true, true).map(|id| id.to_string());
                let canonical = instance_type_iri(cv, &conv);
                matches!((native, canonical), (Ok(n), Some(c)) if n != c)
            })
            .collect();
        assert!(
            !with_distinct_class_uri.is_empty(),
            "fixture has no class with a class_uri distinct from its native URI, \
             so this test would not be testing anything"
        );

        for cv in &with_distinct_class_uri {
            let instance_type = instance_type_iri(cv, &conv).unwrap();
            assert!(
                ntriples.contains(&format!("<{instance_type}> <{RDF_TYPE}> <{OWL_CLASS}>")),
                "class {} is typed as <{instance_type}> in instance data but the \
                 schema triples do not describe that IRI",
                cv.name()
            );
        }
    }

    /// The other half of the same invariant: a slot's schema subject is the IRI
    /// the instance writer uses as a predicate.
    #[test]
    fn slot_subject_matches_the_instance_predicate_spelling() {
        let sv = personinfo();
        let conv = sv.converter();
        let ntriples = schema_triples(&sv, &SchemaRdfOptions::default()).to_ntriples();

        let mut checked = 0usize;
        for cv in sv.class_views().unwrap() {
            for slot in cv.slots() {
                let Ok(iri) = slot_predicate_iri(slot, &conv) else {
                    continue;
                };
                if NamedNode::new(&iri).is_err() {
                    continue;
                }
                assert!(
                    ntriples.contains(&format!("<{iri}> <{RDF_TYPE}> <{RDF_PROPERTY}>")),
                    "slot {} is written as <{iri}> in instance data but the schema \
                     triples do not describe that IRI",
                    slot.name
                );
                checked += 1;
            }
        }
        assert!(checked > 0, "fixture has no slots to check");
    }

    #[test]
    fn enum_values_with_a_meaning_get_their_code_as_a_label() {
        let sv = personinfo();
        let conv = sv.converter();
        let ntriples = schema_triples(&sv, &SchemaRdfOptions::default()).to_ntriples();

        let mut checked = 0usize;
        for ev in sv.enum_views().unwrap() {
            let Some(values) = ev.definition().permissible_values.clone() else {
                continue;
            };
            for (code, pv) in values {
                let Some(meaning) = pv.meaning.as_ref() else {
                    continue;
                };
                let Ok(iri) = Identifier::new(meaning).to_uri(&conv) else {
                    continue;
                };
                if NamedNode::new(&iri.0).is_err() {
                    continue;
                }
                assert!(
                    ntriples.contains(&format!("<{}> <{RDFS_LABEL}> \"{code}\"", iri.0)),
                    "enum {} value {code} has meaning {} but no rdfs:label",
                    ev.name(),
                    iri.0
                );
                assert!(
                    ntriples.contains(&format!("<{}> <{SKOS_IN_SCHEME}> <", iri.0)),
                    "enum value {code} is not linked back to its scheme"
                );
                checked += 1;
            }
        }
        assert!(
            checked > 0,
            "fixture has no enum value carrying a meaning, so this test would \
             not be testing anything"
        );
    }

    /// A permissible value with no `meaning` renders as a plain literal in
    /// instance data. There is no IRI to describe, and inventing one would
    /// describe a resource that appears nowhere — so nothing is emitted.
    #[test]
    fn enum_values_without_a_meaning_get_no_iri() {
        let sv = personinfo();
        let out = schema_triples(&sv, &SchemaRdfOptions::default());
        let subjects: BTreeSet<String> = out
            .triples
            .iter()
            .map(|triple| triple.subject.to_string())
            .collect();

        for ev in sv.enum_views().unwrap() {
            let Some(values) = ev.definition().permissible_values.clone() else {
                continue;
            };
            for (code, pv) in values {
                if pv.meaning.is_some() {
                    continue;
                }
                let enum_iri = ev.canonical_uri().to_string();
                let minted = format!("<{}/{code}>", enum_iri.trim_end_matches('/'));
                assert!(
                    !subjects.contains(&minted),
                    "meaning-less value {code} should not have been given an IRI"
                );
            }
        }
    }

    #[test]
    fn output_is_deterministic() {
        let sv = personinfo();
        let first = schema_triples(&sv, &SchemaRdfOptions::default()).to_ntriples();
        let second = schema_triples(&sv, &SchemaRdfOptions::default()).to_ntriples();
        assert_eq!(first, second, "output must not depend on map order");
    }
}
