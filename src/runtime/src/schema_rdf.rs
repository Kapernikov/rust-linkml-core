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
//! Scope is *discovery*: names, relationships, allowed values, and how many
//! values are allowed *where* — see [`SchemaRdfProfile`].
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
//! * `owl:Restriction` with `owl:onProperty` plus `owl:minCardinality`,
//!   `owl:maxCardinality` or `owl:allValuesFrom`, hung off the class with
//!   `rdfs:subClassOf`, for per-class cardinality and per-class range. See
//!   *Cardinality* below.
//!
//! # The governing principle
//!
//! **Match linkml's own OWL generator (`gen-owl`) wherever it has a spelling;
//! diverge only where this module already has a stated reason.** Two
//! divergences are stated above and stay: SKOS concepts rather than OWL
//! individuals for permissible values, and `schema:domainIncludes` rather than
//! `rdfs:domain`. Everything else follows `gen-owl` so that a client, or a
//! reasoner, that already understands linkml's OWL output understands this too.
//! They are named rather than implied: they are the
//! [`SchemaRdfProfile::Discovery`] profile, and a strict-OWL profile can be
//! added beside it without breaking this API.
//!
//! Deliberately *not* taken from `gen-owl` are three of its defaults, each an
//! opinion separate from the ones above: `metaclasses` (which puns every class
//! as an individual of `ClassDefinition`, the same class/individual punning
//! this module already refuses for permissible values), `type_objects` (which
//! mints object shadows for literal types), and its enum encoding.
//!
//! # Cardinality
//!
//! "Is this slot required here, can it repeat here, what can it hold *here*"
//! is a discovery question — the answer is per class, not per slot, because
//! LinkML lets a class refine an inherited slot. It is encoded the way
//! `gen-owl` encodes it, in OWL restrictions rather than in SHACL shapes:
//!
//! ```text
//! <Class> rdfs:subClassOf [ a owl:Restriction ;
//!                           owl:onProperty <slot> ;
//!                           owl:minCardinality "1"^^xsd:integer ] .
//! ```
//!
//! Three restrictions can appear per (class, slot) pair: `owl:minCardinality`
//! (`1` when required, and an explicit `0` when not — `gen-owl` emits the zero,
//! so so does this), `owl:maxCardinality 1` when the slot is single-valued, and
//! `owl:allValuesFrom` for the range. Following `gen-owl`, `required` and
//! `multivalued` are true if they are true on *either* the class-scoped slot
//! (`slot_usage`, `attributes`) or the schema-level slot of the same name.
//!
//! Also following `gen-owl`, the restrictions are *not* left inside an
//! `owl:intersectionOf` list. `gen-owl` unrolls that list, adding each member
//! as its own `rdfs:subClassOf` triple on the class (its `simplify` option,
//! on by default). Matching that matters for more than fidelity: unrolled, the
//! answer is one hop plus one blank node,
//!
//! ```sparql
//! ?class rdfs:subClassOf [ owl:onProperty ?slot ; owl:minCardinality ?n ]
//! ```
//!
//! where an intersection list would have forced a client to walk `rdf:first` /
//! `rdf:rest`.
//!
//! ## The per-class range is redundant with the slot-level range, on purpose
//!
//! `owl:allValuesFrom` on the restriction and `schema:rangeIncludes` /
//! `rdfs:range` on the slot say overlapping things, and both are emitted. This
//! is a choice, not an oversight. They answer different questions — "what can
//! this slot hold anywhere" versus "what can it hold on this class" — and they
//! cost differently: the slot-level form is a one-hop lookup, the restriction
//! needs blank-node traversal. Dropping the slot-level form would make the
//! cheap, common question expensive; dropping the restriction would make the
//! per-class answer unavailable.
//!
//! ## What `minCardinality` means, honestly
//!
//! A LinkML constraint is closed-world: `required: true` is a rule a validator
//! checks, and the absence of a value is a *violation*. An OWL axiom is
//! open-world: `owl:minCardinality 1` is an assertion a reasoner *uses*, and
//! from it a reasoner concludes that a value exists but is unstated, rather
//! than flagging that one is missing. Converting the first into the second
//! changes a validation rule into an inference. Similarly, `owl:maxCardinality
//! 1` licenses a reasoner to conclude two differently-named values are the same
//! individual, where LinkML meant that having two is an error.
//!
//! That caveat has not gone away; the encoding is adopted anyway, for ecosystem
//! harmony with `gen-owl`, and the open-world reading is a known and accepted
//! consequence. A consumer that needs closed-world validation semantics must
//! validate against the LinkML schema (or a SHACL rendering of it), not reason
//! over these triples. What these triples are *for* is discovery, where "the
//! schema says at least one, at most one" is exactly the answer wanted.
//!
//! For the same reason no `owl:allValuesFrom owl:Thing` is emitted where a
//! range cannot be resolved. `gen-owl` falls back to `owl:Thing` because it is
//! building an intersection list that has to have a member; here the
//! restriction is standalone, and a vacuous "all values are things" costs four
//! triples to say nothing. An unresolvable range is simply not described, in
//! keeping with the skip-and-record rule below.
//!
//! # IRIs must be absolute
//!
//! Every IRI is produced through the schema's own [`Converter`] — the same path
//! the instance turtle writer uses — and then through `NamedNode::new`, which
//! rejects anything that is not an absolute IRI. A term whose CURIE the
//! converter cannot expand is *skipped* and named in [`SchemaTriples::skipped`],
//! never emitted as a bare CURIE, which would not re-parse.
//!
//! The converter comes from every schema in the view, so two schemas that bind
//! one prefix to different namespaces make some IRIs here expand through the
//! wrong namespace. That is strictly worse than a skip — the term is present
//! and looks fine — so it is reported too, in
//! [`SchemaTriples::prefix_collisions`].
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
use linkml_schemaview::identifier::{Identifier, PrefixCollision};
use linkml_schemaview::schemaview::{ClassView, SchemaView, SlotView};
use oxrdf::{BlankNode, Literal, NamedNode, NamedNodeRef, NamedOrBlankNode, Term, Triple};

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
/// `owl:Restriction`.
pub const OWL_RESTRICTION: &str = "http://www.w3.org/2002/07/owl#Restriction";
/// `owl:onProperty`.
pub const OWL_ON_PROPERTY: &str = "http://www.w3.org/2002/07/owl#onProperty";
/// `owl:allValuesFrom`.
pub const OWL_ALL_VALUES_FROM: &str = "http://www.w3.org/2002/07/owl#allValuesFrom";
/// `owl:minCardinality`.
pub const OWL_MIN_CARDINALITY: &str = "http://www.w3.org/2002/07/owl#minCardinality";
/// `owl:maxCardinality`.
pub const OWL_MAX_CARDINALITY: &str = "http://www.w3.org/2002/07/owl#maxCardinality";
/// `xsd:integer` — the datatype `gen-owl` gives its cardinality literals,
/// because rdflib maps a Python `int` to it. OWL 2 asks for
/// `xsd:nonNegativeInteger`; harmony with `gen-owl` wins, per the module docs.
pub const XSD_INTEGER: &str = "http://www.w3.org/2001/XMLSchema#integer";

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
    /// allowed, and how many values are allowed on each class.
    ///
    /// Weakest true type assertions, SKOS for controlled value lists,
    /// `schema:*Includes` for class↔slot links — see the module docs for why
    /// each of those is the defensible reading of a LinkML schema.
    ///
    /// It is *not* axiom-free. Per-class cardinality and per-class range are
    /// genuine OWL restrictions, spelled as linkml's own OWL generator spells
    /// them (`owl:minCardinality` / `owl:maxCardinality` /
    /// `owl:allValuesFrom`, unrolled onto the class with `rdfs:subClassOf`),
    /// and a reasoner will read them open-world. It is still not a full OWL
    /// axiomatisation: `gen-owl`'s metaclass punning, type objects and enum
    /// individuals are all left out. The module docs give the reasoning and the
    /// caveat.
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
    /// Prefixes the view's schemas bound to more than one namespace, so only
    /// one binding could survive. Empty for any schema set whose prefixes are
    /// unambiguous, which is the normal case.
    ///
    /// This is reported for the same reason `skipped` is, and is the more
    /// dangerous of the two: a term named here is not missing from `triples`,
    /// it is *present under an IRI that may be wrong*, and nothing downstream
    /// can tell. A caller that treats a non-empty `skipped` as a problem should
    /// treat this as at least as serious.
    pub prefix_collisions: Vec<PrefixCollision>,
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

    let (conv, prefix_collisions) = sv.converter_with_collisions();
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
        prefix_collisions,
    }
}

struct Builder {
    triples: Vec<Triple>,
    skipped: Vec<String>,
}

/// A cardinality as `gen-owl` writes it: rdflib turns a Python `int` into an
/// `xsd:integer`, so this does too.
fn cardinality_literal(n: u32) -> Literal {
    Literal::new_typed_literal(n.to_string(), NamedNodeRef::new_unchecked(XSD_INTEGER))
}

/// FNV-1a, 64 bit. Used only to give a blank node a label that depends on
/// nothing but the restriction's own content, so that two runs over the same
/// schema produce byte-identical output. Not a security hash; a collision would
/// merge two restrictions, which at the few-thousand-restriction scale of a
/// schema is a probability around 1e-12.
fn fnv1a64(s: &str) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in s.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100_0000_01b3);
    }
    hash
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
        self.triple_from(subject.clone().into(), predicate, object);
    }

    /// As [`Builder::triple`], but the subject may be a blank node — which the
    /// restrictions are.
    fn triple_from(&mut self, subject: NamedOrBlankNode, predicate: &str, object: Term) {
        self.triples.push(Triple::new(
            subject,
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

    /// One unrolled OWL restriction: the blank node with its `rdf:type`,
    /// `owl:onProperty` and constraining predicate, plus the
    /// `rdfs:subClassOf` triple that hangs it off the class.
    ///
    /// The blank node's label is a content hash of what the restriction says,
    /// which is what keeps [`schema_triples`]'s dedup and sort meaningful: a
    /// freshly minted label would differ between two runs over the same schema
    /// and the output would stop being deterministic.
    fn restriction(
        &mut self,
        class_node: &NamedNode,
        slot_node: &NamedNode,
        predicate: &str,
        object: Term,
    ) {
        let node = BlankNode::new_unchecked(format!(
            "r{:016x}",
            fnv1a64(&format!("{class_node}|{slot_node}|{predicate}|{object}"))
        ));
        let subject: NamedOrBlankNode = node.clone().into();

        self.triple_from(
            subject.clone(),
            RDF_TYPE,
            Term::NamedNode(NamedNodeRef::new_unchecked(OWL_RESTRICTION).into_owned()),
        );
        self.triple_from(subject.clone(), OWL_ON_PROPERTY, slot_node.clone().into());
        self.triple_from(subject, predicate, object);
        self.triple(class_node, RDFS_SUBCLASS_OF, node.into());
    }

    /// The per-class cardinality and range restrictions for one (class, slot)
    /// pair, spelled as `gen-owl` spells them. See the module docs.
    fn slot_restrictions(
        &mut self,
        class_node: &NamedNode,
        slot_node: &NamedNode,
        slot: &SlotView,
        ranges: &[NamedNode],
    ) {
        // `gen-owl` takes `slot.required or top_slot.required`: the class-scoped
        // refinement and the schema-level slot, either one being true making it
        // true. A `ClassView`'s `SlotView` carries exactly that chain in
        // `definitions()` — the top-level slot's definitions first, then the
        // `slot_usage` refinements — so "any link in the chain says so" is the
        // same disjunction, and generalises correctly when the chain is deeper
        // than two.
        let any = |pick: fn(&linkml_meta::SlotDefinition) -> Option<bool>| {
            slot.definitions()
                .iter()
                .any(|def| pick(def).unwrap_or(false))
        };
        let required = any(|def| def.required);
        let multivalued = any(|def| def.multivalued);

        // Only where it is not a guess: exactly one resolved range, the same
        // condition under which `rdfs:range` is emitted on the slot. Several
        // ranges would need an `owl:unionOf` list, which is the blank-node
        // walking this encoding exists to avoid; none is not described at all,
        // rather than falling back to `gen-owl`'s vacuous `owl:Thing`.
        if let [only] = ranges {
            self.restriction(
                class_node,
                slot_node,
                OWL_ALL_VALUES_FROM,
                only.clone().into(),
            );
        }

        // The explicit `0` is `gen-owl`'s, not an accident.
        self.restriction(
            class_node,
            slot_node,
            OWL_MIN_CARDINALITY,
            cardinality_literal(if required { 1 } else { 0 }).into(),
        );
        if !multivalued {
            self.restriction(
                class_node,
                slot_node,
                OWL_MAX_CARDINALITY,
                cardinality_literal(1).into(),
            );
        }
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

                // Per class, alongside the per-slot statements above and
                // knowingly overlapping them: see the module docs.
                self.slot_restrictions(&class_node, &slot_node, slot, &ranges);
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
            OWL_RESTRICTION,
            OWL_ON_PROPERTY,
            OWL_ALL_VALUES_FROM,
            OWL_MIN_CARDINALITY,
            OWL_MAX_CARDINALITY,
            XSD_INTEGER,
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
            // Blank nodes are exempt: a restriction has no IRI by design.
            for iri in [
                match &triple.subject {
                    NamedOrBlankNode::NamedNode(node) => Some(node.to_string()),
                    NamedOrBlankNode::BlankNode(_) => None,
                },
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

    /// Evaluate, by hand, the exact basic graph pattern a client writes:
    ///
    /// ```sparql
    /// ?class rdfs:subClassOf ?r . ?r owl:onProperty ?slot . ?r <predicate> ?n
    /// ```
    ///
    /// The point is that the restriction is reachable in *one hop plus one
    /// blank node* — no `rdf:first`/`rdf:rest` walk — which is the whole reason
    /// `gen-owl`'s unrolled form is matched rather than its intersection list.
    /// There is no SPARQL engine in this crate, so the join is spelled out;
    /// downstream (asset360-rust) runs the same shape through oxigraph.
    fn cardinality_of(
        out: &SchemaTriples,
        class_iri: &str,
        slot_iri: &str,
        predicate: &str,
    ) -> Vec<String> {
        let restrictions: BTreeSet<String> = out
            .triples
            .iter()
            .filter(|t| {
                t.subject.to_string() == format!("<{class_iri}>")
                    && t.predicate.as_str() == RDFS_SUBCLASS_OF
            })
            .filter_map(|t| match &t.object {
                Term::BlankNode(node) => Some(node.to_string()),
                _ => None,
            })
            .collect();

        let on_slot: BTreeSet<String> = out
            .triples
            .iter()
            .filter(|t| {
                t.predicate.as_str() == OWL_ON_PROPERTY
                    && t.object.to_string() == format!("<{slot_iri}>")
                    && restrictions.contains(&t.subject.to_string())
            })
            .map(|t| t.subject.to_string())
            .collect();

        // Every restriction reached this way must really be typed as one.
        for r in &on_slot {
            assert!(
                out.triples.iter().any(|t| t.subject.to_string() == *r
                    && t.predicate.as_str() == RDF_TYPE
                    && t.object.to_string() == format!("<{OWL_RESTRICTION}>")),
                "{r} is hung off a class with owl:onProperty but is not an owl:Restriction"
            );
        }

        out.triples
            .iter()
            .filter(|t| {
                t.predicate.as_str() == predicate && on_slot.contains(&t.subject.to_string())
            })
            .map(|t| t.object.to_string())
            .collect()
    }

    fn class_iri(sv: &SchemaView, name: &str) -> String {
        let conv = sv.converter();
        let cv = sv
            .class_views()
            .unwrap()
            .into_iter()
            .find(|cv| cv.name() == name)
            .unwrap_or_else(|| panic!("personinfo has a {name} class"));
        instance_type_iri(&cv, &conv).unwrap_or_else(|| panic!("{name} has no expandable URI"))
    }

    fn slot_iri(sv: &SchemaView, class: &str, slot_name: &str) -> String {
        let conv = sv.converter();
        let cv = sv
            .class_views()
            .unwrap()
            .into_iter()
            .find(|cv| cv.name() == class)
            .unwrap_or_else(|| panic!("personinfo has a {class} class"));
        let slot = cv
            .slots()
            .iter()
            .find(|s| s.name == slot_name)
            .unwrap_or_else(|| panic!("{class} has no slot {slot_name}"));
        slot_predicate_iri(slot, &conv)
            .unwrap_or_else(|raw| panic!("slot URI {raw} not expandable"))
    }

    /// `FamilialRelationship.type` is `required: true` in `slot_usage` only —
    /// the schema-level `type` slot is not required. `gen-owl` takes
    /// `slot.required or top_slot.required`, so the refinement wins and the
    /// class gets `owl:minCardinality 1`. `Relationship.type` is the same slot
    /// without the refinement and must get the explicit `0`, which is what
    /// makes the answer per-class rather than per-slot.
    #[test]
    fn a_required_slot_is_queryable_as_min_cardinality_one() {
        let sv = personinfo();
        let out = schema_triples(&sv, &SchemaRdfOptions::default());
        let one = format!("\"1\"^^<{XSD_INTEGER}>");
        let zero = format!("\"0\"^^<{XSD_INTEGER}>");

        let refined = class_iri(&sv, "FamilialRelationship");
        let base = class_iri(&sv, "Relationship");
        let slot = slot_iri(&sv, "FamilialRelationship", "type");
        assert_eq!(
            slot,
            slot_iri(&sv, "Relationship", "type"),
            "the two classes must share the slot for this to be about the class"
        );

        assert_eq!(
            cardinality_of(&out, &refined, &slot, OWL_MIN_CARDINALITY),
            vec![one.clone()],
            "FamilialRelationship.type is required in slot_usage"
        );
        assert_eq!(
            cardinality_of(&out, &base, &slot, OWL_MIN_CARDINALITY),
            vec![zero],
            "Relationship.type is not required, and gen-owl emits the explicit 0"
        );
        // Single-valued, so it is also capped.
        assert_eq!(
            cardinality_of(&out, &refined, &slot, OWL_MAX_CARDINALITY),
            vec![one],
        );
    }

    /// `Person.has_familial_relationships` is `multivalued: true` on the
    /// *schema-level* slot, the other half of `gen-owl`'s disjunction. It must
    /// therefore get no `owl:maxCardinality` at all, and — not being required —
    /// an `owl:minCardinality 0`. Its range resolves to exactly one class, so
    /// the per-class `owl:allValuesFrom` is there too.
    #[test]
    fn a_multivalued_slot_gets_no_max_cardinality() {
        let sv = personinfo();
        let out = schema_triples(&sv, &SchemaRdfOptions::default());

        let person = class_iri(&sv, "Person");
        let slot = slot_iri(&sv, "Person", "has_familial_relationships");

        assert_eq!(
            cardinality_of(&out, &person, &slot, OWL_MIN_CARDINALITY),
            vec![format!("\"0\"^^<{XSD_INTEGER}>")],
        );
        assert!(
            cardinality_of(&out, &person, &slot, OWL_MAX_CARDINALITY).is_empty(),
            "a multivalued slot must not be capped at one"
        );
        assert_eq!(
            cardinality_of(&out, &person, &slot, OWL_ALL_VALUES_FROM),
            vec![format!("<{}>", class_iri(&sv, "FamilialRelationship"))],
        );
    }

    /// A restriction's blank node is a content hash, so nothing about the
    /// output depends on iteration order or on how many times a schema is
    /// triplified. `output_is_deterministic` covers the whole graph; this pins
    /// the reason, because a randomly minted label would break it invisibly
    /// only once blank nodes appeared.
    #[test]
    fn restriction_blank_nodes_are_content_addressed() {
        let sv = personinfo();
        let out = schema_triples(&sv, &SchemaRdfOptions::default());
        let blanks: BTreeSet<String> = out
            .triples
            .iter()
            .filter_map(|t| match &t.object {
                Term::BlankNode(node) => Some(node.to_string()),
                _ => None,
            })
            .collect();
        assert!(!blanks.is_empty(), "expected restrictions in the output");

        let again = schema_triples(&sv, &SchemaRdfOptions::default());
        let blanks_again: BTreeSet<String> = again
            .triples
            .iter()
            .filter_map(|t| match &t.object {
                Term::BlankNode(node) => Some(node.to_string()),
                _ => None,
            })
            .collect();
        assert_eq!(blanks, blanks_again);

        // Every blank node used as an object is a restriction hung off a class,
        // and every one of them carries all three of its triples.
        for blank in &blanks {
            let predicates: BTreeSet<&str> = out
                .triples
                .iter()
                .filter(|t| t.subject.to_string() == *blank)
                .map(|t| t.predicate.as_str())
                .collect();
            assert!(
                predicates.contains(RDF_TYPE) && predicates.contains(OWL_ON_PROPERTY),
                "restriction {blank} is incomplete: {predicates:?}"
            );
            assert!(
                predicates.contains(OWL_MIN_CARDINALITY)
                    || predicates.contains(OWL_MAX_CARDINALITY)
                    || predicates.contains(OWL_ALL_VALUES_FROM),
                "restriction {blank} constrains nothing: {predicates:?}"
            );
        }
    }

    /// `gen-owl` falls back to `owl:allValuesFrom owl:Thing` when it cannot
    /// resolve a range, because it is building an intersection list that needs
    /// a member. This module does not, so `owl:Thing` must appear nowhere.
    #[test]
    fn no_vacuous_owl_thing_range() {
        let out = schema_triples(&personinfo(), &SchemaRdfOptions::default());
        assert!(
            !out.to_ntriples()
                .contains("<http://www.w3.org/2002/07/owl#Thing>"),
            "owl:Thing says nothing and should not have been emitted"
        );
    }

    #[test]
    fn output_is_deterministic() {
        let sv = personinfo();
        let first = schema_triples(&sv, &SchemaRdfOptions::default()).to_ntriples();
        let second = schema_triples(&sv, &SchemaRdfOptions::default()).to_ntriples();
        assert_eq!(first, second, "output must not depend on map order");
    }
    /// A schema set with unambiguous prefixes reports no collision, so a
    /// non-empty `prefix_collisions` really does mean something is wrong.
    #[test]
    fn an_unambiguous_schema_reports_no_prefix_collision() {
        let built = schema_triples(&personinfo(), &SchemaRdfOptions::default());
        assert!(
            built.prefix_collisions.is_empty(),
            "got {:?}",
            built.prefix_collisions
        );
    }

    /// The hazard this field exists for: two schemas in one view bind `shared:`
    /// to different namespaces, so some emitted IRI is built from the wrong
    /// namespace and nothing about the triples themselves gives that away.
    /// Unlike `skipped`, the term is present — just possibly misnamed.
    #[test]
    fn a_prefix_collision_across_schemas_is_reported() {
        let base = from_yaml(Path::new(&data_path("prefix_conflict_base.yaml"))).unwrap();
        let derived = from_yaml(Path::new(&data_path("prefix_conflict_derived.yaml"))).unwrap();
        let mut sv = SchemaView::new();
        sv.add_schema(derived.clone()).unwrap();
        sv.add_schema_with_import_ref(
            base,
            Some((derived.id.clone(), "./prefix_conflict_base".to_string())),
        )
        .unwrap();

        let built = schema_triples(&sv, &SchemaRdfOptions::default());
        assert_eq!(
            built
                .prefix_collisions
                .iter()
                .map(|c| c.to_string())
                .collect::<Vec<_>>(),
            vec![concat!(
                "prefix 'shared' expands to <https://example.com/a/>,",
                " not to <https://example.com/b/>"
            )
            .to_string()]
        );
        assert!(!built.triples.is_empty(), "the triples are still produced");
    }
}
