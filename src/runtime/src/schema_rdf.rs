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
//!   commitment the schema never made. *Every* permissible value is a member of
//!   its scheme, and is named by the IRI `gen-owl` names it by — see
//!   *Permissible values, and which IRI each one gets* below.
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
//! * `skos:exactMatch` / `closeMatch` / `relatedMatch` / `narrowMatch` /
//!   `broadMatch` / `mappingRelation` for the mappings a class or slot declares
//!   to terms in another vocabulary. Not chosen here: each is the `slot_uri`
//!   the LinkML metamodel itself gives that mapping slot. See *Mappings and
//!   keys* below.
//! * `owl:hasKey` for `unique_keys`. See *Mappings and keys* below.
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
//! mints object shadows for literal types), and its enum *encoding* — the
//! `owl:unionOf` list of permissible-value classes. Its enum *identifiers*,
//! which are a separate decision, are taken: see below.
//!
//! # Permissible values, and which IRI each one gets
//!
//! `meaning` is optional in LinkML and, in practice, usually absent: a schema
//! can easily declare forty enums and give exactly one of their several hundred
//! values a `meaning`. An enum whose values were described only when they
//! carried one would answer "which values does this permit?" with silence for
//! almost every enum, which is the question this module exists to answer. So
//! *every* permissible value is described.
//!
//! `meaning` decides not *whether* a value is described but which IRI names it,
//! and that decision is `gen-owl`'s, taken verbatim from its
//! `_permissible_value_uri` — per the governing principle above, this module
//! does not mint IRIs of its own design where `gen-owl` has a spelling:
//!
//! * With a `meaning`: that IRI, expanded through the schema's converter.
//! * Without one: `<enum_uri>#<code>`, the code percent-encoded (`gen-owl`'s
//!   `enum_iri_separator`, whose default `#` is [`ENUM_IRI_SEPARATOR`], and its
//!   `quote(text.strip(), safe="")`).
//!
//! ## Joining the schema graph to instance data
//!
//! The two cases join to instance data differently, and a client has to know
//! which it is in. The asymmetry is the *instance* writer's, not this module's:
//!
//! * A value with a `meaning` is written by [`turtle`](crate::turtle) as that
//!   same IRI, through the same converter. So the instance object *is* the
//!   concept subject: `?s ?slot ?concept` joins `?concept skos:notation ?code`.
//! * A value with no `meaning` is written by [`turtle`](crate::turtle) as a
//!   plain literal — the code itself. Its `<enum_uri>#<code>` IRI appears
//!   nowhere in the data, so that join finds nothing; the join that works is on
//!   the literal, `?s ?slot ?code` against `?concept skos:notation ?code`.
//!
//! Both cases therefore meet at `skos:notation`, which is the one term a client
//! can rely on for every value of every enum, and the reason `skos:notation` is
//! emitted for values that carry a `meaning` too.
//!
//! # Mappings and keys
//!
//! `gen-owl` reaches both of these through its `add_metadata`, and both are
//! taken from it rather than designed here.
//!
//! **Mappings.** `add_metadata` walks every set metamodel slot whose own
//! `slot_uri` is not `linkml:`-prefixed and emits it under exactly that
//! `slot_uri`. For the six mapping slots those URIs are declared in the
//! metamodel's `mappings.yaml` and are the `SKOS_*` constants above; the
//! `uriorcurie` range is why the object is expanded through the schema's
//! converter instead of written as a literal. Mappings are emitted on classes
//! and slots, which is where `gen-owl` calls `add_metadata` — not on enums or
//! permissible values, where it does not.
//!
//! Two things `gen-owl` does here are deliberately *not* matched:
//!
//! * It also emits `skos:exactMatch` between a class's **native** URI and its
//!   `class_uri`, bridging two spellings of one class. This module has only one
//!   spelling for a class (see *Which IRI names a class*), so there is no second
//!   subject for that bridge to connect, and emitting it would state that a
//!   class is a mapping of itself. On the asset360 datamodel that accounts for
//!   54 of `gen-owl`'s 185 `skos:exactMatch` triples.
//! * It **drops all metadata**, mappings included, for any attribute whose slot
//!   URI is shared by attributes on more than one class — its "Ambiguous
//!   attribute" branch returns before `add_metadata` runs. That is a bailout,
//!   not a spelling: this module already holds that a slot is legitimately
//!   reused across unrelated classes (which is why it emits
//!   `schema:domainIncludes` rather than `rdfs:domain`), so a shared slot
//!   carries the union of the mappings declared on it. On asset360 that is 17
//!   `skos:exactMatch` triples `gen-owl` omits.
//!
//! **Keys.** `unique_keys` becomes `?class owl:hasKey ( ?slot … )`, one per
//! declared entry, name-sorted, with the members in their declared order. Three
//! details are `gen-owl`'s: `unique_keys` is the *only* source — an `identifier`
//! or a `key` slot produces no `owl:hasKey`, in `gen-owl` or here — the entries
//! are the ones the class itself declares rather than an inheritance-merged
//! view, and the members are an RDF collection.
//!
//! That collection is the one place this module walks `rdf:first` / `rdf:rest`,
//! which it avoids everywhere else. It is not avoidable here: `owl:hasKey`
//! takes a list, and a one-hop spelling of a composite key would no longer be
//! `owl:hasKey`. A key's members are named by [`slot_predicate_iri`] — the same
//! IRI the graph uses for that slot as a predicate and the instance writer
//! writes in data — so the key joins to what it is a key of. `gen-owl`, whose
//! `use_native_uris` defaults on, names them by the native spelling instead,
//! which on asset360 differs for two RSM classes.
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
/// `skos:mappingRelation` — the metamodel's `slot_uri` for LinkML `mappings`.
pub const SKOS_MAPPING_RELATION: &str = "http://www.w3.org/2004/02/skos/core#mappingRelation";
/// `skos:exactMatch` — the metamodel's `slot_uri` for `exact_mappings`.
pub const SKOS_EXACT_MATCH: &str = "http://www.w3.org/2004/02/skos/core#exactMatch";
/// `skos:closeMatch` — the metamodel's `slot_uri` for `close_mappings`.
pub const SKOS_CLOSE_MATCH: &str = "http://www.w3.org/2004/02/skos/core#closeMatch";
/// `skos:relatedMatch` — the metamodel's `slot_uri` for `related_mappings`.
pub const SKOS_RELATED_MATCH: &str = "http://www.w3.org/2004/02/skos/core#relatedMatch";
/// `skos:narrowMatch` — the metamodel's `slot_uri` for `narrow_mappings`.
pub const SKOS_NARROW_MATCH: &str = "http://www.w3.org/2004/02/skos/core#narrowMatch";
/// `skos:broadMatch` — the metamodel's `slot_uri` for `broad_mappings`.
pub const SKOS_BROAD_MATCH: &str = "http://www.w3.org/2004/02/skos/core#broadMatch";
/// `owl:hasKey`.
pub const OWL_HAS_KEY: &str = "http://www.w3.org/2002/07/owl#hasKey";
/// `rdf:first` — one cell of the `owl:hasKey` collection.
pub const RDF_FIRST: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#first";
/// `rdf:rest` — the tail of the `owl:hasKey` collection.
pub const RDF_REST: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#rest";
/// `rdf:nil` — the end of the `owl:hasKey` collection.
pub const RDF_NIL: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#nil";
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
/// What `gen-owl` puts between an enum's IRI and a permissible value's code
/// when the value has no `meaning` to be identified by — its
/// `enum_iri_separator`, whose default is `#`. Matched rather than chosen: see
/// the module docs.
pub const ENUM_IRI_SEPARATOR: &str = "#";
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

/// The IRI that names one permissible value.
///
/// `gen-owl`'s `_permissible_value_uri`, term for term. Shared — like
/// [`slot_predicate_iri`] — with every consumer that has to name the same
/// value: the concept subject [`schema_triples`] emits, the term the instance
/// writer renders, and the term a SQL pushdown has to reproduce in order to
/// agree with it. One spelling, so the three cannot drift apart.
///
/// * With a `meaning`: that IRI, expanded through the schema's converter. `Err`
///   carries the unexpanded spelling, leaving the policy to the caller — as
///   [`slot_predicate_iri`] does.
/// * Without one: `<enum_uri>#<code>`, the code trimmed and percent-encoded
///   ([`ENUM_IRI_SEPARATOR`] and `gen-owl`'s `quote(text.strip(), safe="")`).
///
/// Every permissible value has an IRI, whether or not its schema bothered to
/// map it to an ontology. Which of the two branches produced it is not a
/// distinction a caller should have to make — that it *was* one is the whole
/// reason enum values used to reach instance data as two different kinds of
/// term.
pub fn permissible_value_iri(
    enum_uri: &str,
    code: &str,
    pv: &linkml_meta::PermissibleValue,
    conv: &Converter,
) -> Result<String, String> {
    let Some(meaning) = pv.meaning.as_ref() else {
        return Ok(format!(
            "{enum_uri}{ENUM_IRI_SEPARATOR}{}",
            crate::turtle::encode_path_part(code.trim())
        ));
    };
    match Identifier::new(meaning).to_uri(conv) {
        Ok(uri) => Ok(uri.0),
        Err(_) => Err(meaning.clone()),
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

/// The six LinkML mapping slots paired with the SKOS predicate the metamodel
/// gives each of them, in one place, for a `ClassDefinition` or a
/// `SlotDefinition`.
///
/// A macro rather than a function because the two metamodel structs share the
/// field names but no trait that exposes them.
macro_rules! mapping_groups {
    ($def:expr) => {{
        let def = $def;
        [
            (SKOS_MAPPING_RELATION, def.mappings.as_ref()),
            (SKOS_EXACT_MATCH, def.exact_mappings.as_ref()),
            (SKOS_CLOSE_MATCH, def.close_mappings.as_ref()),
            (SKOS_RELATED_MATCH, def.related_mappings.as_ref()),
            (SKOS_NARROW_MATCH, def.narrow_mappings.as_ref()),
            (SKOS_BROAD_MATCH, def.broad_mappings.as_ref()),
        ]
    }};
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

    /// The IRI that identifies one permissible value's `skos:Concept`.
    ///
    /// The spelling is [`permissible_value_iri`]'s, not this module's, so the
    /// concept subject described here and the term instance data carries are
    /// the same IRI by construction. What is left here is this module's own
    /// policy on a CURIE that will not expand: skip the term and record it,
    /// rather than emit a bare CURIE.
    fn concept_iri(
        &mut self,
        enum_name: &str,
        code: &str,
        pv: &linkml_meta::PermissibleValue,
        enum_node: &NamedNode,
        conv: &Converter,
    ) -> Option<NamedNode> {
        let what = format!("enum value {enum_name}.{code}");
        match permissible_value_iri(enum_node.as_str(), code, pv, conv) {
            Ok(iri) => self.node(&iri, &what),
            // Unexpandable: `node` rejects it and records it, as `expanded` did.
            Err(raw) => self.node(&raw, &what),
        }
    }

    /// The mappings declared on one class or slot, each under the SKOS
    /// predicate the *metamodel* gives that mapping slot.
    ///
    /// No predicate is chosen here. `gen-owl` reaches mappings through
    /// `add_metadata`, which emits every set metamodel slot whose own
    /// `slot_uri` is not `linkml:`-prefixed under exactly that `slot_uri`; for
    /// the six mapping slots those URIs are declared in the metamodel's
    /// `mappings.yaml` and are the constants above. The `uriorcurie` range is
    /// why the object is expanded through the schema's converter rather than
    /// written as a literal — again `gen-owl`'s rule, for the same reason.
    ///
    /// An object whose CURIE will not expand is skipped and recorded, as
    /// everywhere else in this module; `gen-owl` would emit the bare CURIE.
    fn mappings(
        &mut self,
        subject: &NamedNode,
        conv: &Converter,
        what: &str,
        groups: &[(&str, Option<&Vec<String>>)],
    ) {
        for (predicate, values) in groups {
            let Some(values) = values else { continue };
            for raw in *values {
                if let Some(object) = self.expanded(raw, conv, &format!("{what} mapping")) {
                    self.triple(subject, predicate, object.into());
                }
            }
        }
    }

    /// `?class owl:hasKey ( ?slot … )` for each `unique_keys` entry the class
    /// *declares*, spelled as `gen-owl` spells it.
    ///
    /// Three details are `gen-owl`'s and not fresh decisions. The source is
    /// `unique_keys` alone — `gen-owl` reads no other keyword here, so an
    /// `identifier` or a `key` slot produces no `owl:hasKey`, in this module or
    /// in `gen-owl`. The entries are the ones the class declares
    /// ([`ClassView::def`]), not [`ClassView::unique_keys`]'s inheritance-merged
    /// view, because `gen-owl` reads the raw `ClassDefinition`. And the members
    /// are an RDF collection, which is the one place this module walks
    /// `rdf:first` / `rdf:rest` — `owl:hasKey` takes a list and there is no
    /// one-hop spelling of it that a reasoner would still read as a key.
    ///
    /// The entries are emitted name-sorted and their cells carry content-hashed
    /// labels, for the determinism reason [`Builder::restriction`] gives.
    ///
    /// A key none of whose slots resolve to an IRI is dropped whole and
    /// recorded: a key missing one member is not a weaker key, it is a
    /// different and false one.
    fn has_keys(&mut self, class_node: &NamedNode, cv: &ClassView, conv: &Converter) {
        let Some(unique_keys) = cv.def().unique_keys.as_ref() else {
            return;
        };
        let mut entries: Vec<_> = unique_keys.iter().collect();
        entries.sort_by(|a, b| a.0.cmp(b.0));

        for (name, uk) in entries {
            let mut members: Vec<NamedNode> = Vec::new();
            for slot_name in &uk.unique_key_slots {
                let Some(slot) = cv.slots().iter().find(|s| s.name == *slot_name) else {
                    self.skipped
                        .push(format!("unique key {}.{name} slot: {slot_name}", cv.name()));
                    members.clear();
                    break;
                };
                let slot_id = match slot_predicate_iri(slot, conv) {
                    Ok(iri) => iri,
                    Err(raw) => raw,
                };
                let Some(slot_node) = self.expanded(
                    &slot_id,
                    conv,
                    &format!("unique key {}.{name} slot", cv.name()),
                ) else {
                    members.clear();
                    break;
                };
                members.push(slot_node);
            }
            if members.is_empty() {
                continue;
            }

            // Built tail first, so each cell's content hash covers the whole
            // remainder of the list and two equal keys collapse on dedup.
            let mut rest: Term = NamedNodeRef::new_unchecked(RDF_NIL).into_owned().into();
            for member in members.iter().rev() {
                let cell = BlankNode::new_unchecked(format!(
                    "k{:016x}",
                    fnv1a64(&format!("{class_node}|{name}|{member}|{rest}"))
                ));
                self.triple_from(cell.clone().into(), RDF_FIRST, member.clone().into());
                self.triple_from(cell.clone().into(), RDF_REST, rest);
                rest = cell.into();
            }
            self.triple(class_node, OWL_HAS_KEY, rest);
        }
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
            self.mappings(&class_node, conv, cv.name(), &mapping_groups!(cv.def()));
            self.has_keys(&class_node, cv, conv);

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
                self.mappings(
                    &slot_node,
                    conv,
                    &slot.name,
                    &mapping_groups!(slot.definition()),
                );
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
                // Every permissible value is a member of the scheme. What
                // differs between values is only what *identifies* the concept:
                // see `concept_iri`.
                let Some(value_node) = self.concept_iri(ev.name(), code, pv, &enum_node, conv)
                else {
                    continue;
                };

                self.type_of(&value_node, SKOS_CONCEPT);
                // The code, on `rdfs:label`, as `gen-owl` puts it there: this is
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
            SKOS_MAPPING_RELATION,
            SKOS_EXACT_MATCH,
            SKOS_CLOSE_MATCH,
            SKOS_RELATED_MATCH,
            SKOS_NARROW_MATCH,
            SKOS_BROAD_MATCH,
            OWL_HAS_KEY,
            RDF_FIRST,
            RDF_REST,
            RDF_NIL,
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

    /// `StatusEnum` mixes the two cases in one scheme: `active` and `retired`
    /// carry a `meaning`, `unknown` does not. It is the fixture the instance
    /// writer's own enum tests use, which is what makes the IRI comparisons
    /// below comparisons against the real instance-side spelling.
    fn mixed_enum() -> SchemaView {
        let schema = from_yaml(Path::new(&data_path("enum_meaning_schema.yaml"))).unwrap();
        let mut sv = SchemaView::new();
        sv.add_schema(schema).unwrap();
        sv
    }

    /// The members of one scheme, as `(subject, notation)`.
    fn members_of(out: &SchemaTriples, scheme_iri: &str) -> BTreeSet<(String, String)> {
        let in_scheme: BTreeSet<String> = out
            .triples
            .iter()
            .filter(|t| {
                t.predicate.as_str() == SKOS_IN_SCHEME
                    && t.object.to_string() == format!("<{scheme_iri}>")
            })
            .map(|t| t.subject.to_string())
            .collect();

        out.triples
            .iter()
            .filter(|t| t.predicate.as_str() == SKOS_NOTATION)
            .filter(|t| in_scheme.contains(&t.subject.to_string()))
            .map(|t| {
                let Term::Literal(lit) = &t.object else {
                    panic!("skos:notation object is not a literal: {}", t.object);
                };
                (t.subject.to_string(), lit.value().to_string())
            })
            .collect()
    }

    /// The regression this was reported for: on a real datamodel, 43 schemes
    /// with 1 member between them, because 357 of its 358 permissible values
    /// carry no `meaning` and only meaning-carrying values were emitted. Every
    /// permissible value is a member of its scheme, `meaning` or not.
    #[test]
    fn every_permissible_value_is_a_member_of_its_scheme() {
        for sv in [mixed_enum(), personinfo()] {
            let out = schema_triples(&sv, &SchemaRdfOptions::default());

            let mut checked = 0usize;
            for ev in sv.enum_views().unwrap() {
                let Some(values) = ev.definition().permissible_values.clone() else {
                    continue;
                };
                if values.is_empty() {
                    continue;
                }
                let scheme = ev.canonical_uri().to_uri(&sv.converter()).unwrap().0;
                let codes: BTreeSet<String> = members_of(&out, &scheme)
                    .into_iter()
                    .map(|(_, code)| code)
                    .collect();
                let expected: BTreeSet<String> = values.keys().cloned().collect();
                assert_eq!(
                    codes,
                    expected,
                    "enum {} does not have every permissible value as a member",
                    ev.name()
                );
                checked += 1;
            }
            assert!(checked > 0, "fixture has no enum with values");
        }
    }

    /// A scheme that mixes meaning-carrying and meaning-less values emits
    /// *all* of them, each exactly once, each typed `skos:Concept`.
    #[test]
    fn a_scheme_emits_all_of_its_values_once_each() {
        let sv = mixed_enum();
        let out = schema_triples(&sv, &SchemaRdfOptions::default());
        let scheme = "https://example.com/enum-meaning-test/StatusEnum";

        let members = members_of(&out, scheme);
        let codes: BTreeSet<String> = members.iter().map(|(_, code)| code.clone()).collect();
        assert_eq!(
            codes,
            ["active", "retired", "unknown"]
                .into_iter()
                .map(str::to_string)
                .collect::<BTreeSet<String>>()
        );
        assert_eq!(
            members.len(),
            3,
            "one subject per value, no duplicates: {members:?}"
        );

        let ntriples = out.to_ntriples();
        for (subject, code) in &members {
            assert!(
                ntriples.contains(&format!("{subject} <{RDF_TYPE}> <{SKOS_CONCEPT}>")),
                "{code} is a member but not a skos:Concept"
            );
            assert!(
                ntriples.contains(&format!("{subject} <{RDFS_LABEL}> \"{code}\"")),
                "{code} is a member but has no rdfs:label"
            );
        }
    }

    /// The consistency that makes the schema graph joinable to instance data:
    /// the subject a meaning-carrying value gets here is, IRI for IRI, the term
    /// [`crate::turtle`] writes for that same value. `enum_map` is not a
    /// re-derivation — it is the very table the turtle writer indexes.
    #[test]
    fn a_meaning_carrying_concept_is_the_instance_side_iri() {
        let sv = mixed_enum();
        let conv = sv.converter();
        let out = schema_triples(&sv, &SchemaRdfOptions::default());
        let scheme = "https://example.com/enum-meaning-test/StatusEnum";
        let members = members_of(&out, scheme);

        let slot = sv
            .get_class(&Identifier::new("Item"), &conv)
            .unwrap()
            .unwrap()
            .slot(&Identifier::Name("status".to_string()))
            .expect("Item.status");
        let descriptor = slot
            .term_descriptor(&conv)
            .expect("status has a descriptor");
        assert!(
            !descriptor.enum_map.is_empty(),
            "fixture has no meaning-carrying value, so this would test nothing"
        );

        for (code, instance_iri) in &descriptor.enum_map {
            let found = members
                .iter()
                .find(|(_, notation)| notation == code)
                .unwrap_or_else(|| panic!("{code} is not a member of its scheme"));
            assert_eq!(
                found.0,
                format!("<{instance_iri}>"),
                "instance data writes {code} as <{instance_iri}>, but the schema \
                 graph describes it under {}",
                found.0
            );
        }
    }

    /// A permissible value with no `meaning` is named the way `gen-owl` names
    /// it: `<enum_uri>#<percent-encoded code>`. That IRI is *not* the term
    /// instance data carries for the value (a plain literal), which is why
    /// `skos:notation` is emitted and documented as the join for these.
    #[test]
    fn enum_values_without_a_meaning_are_named_as_gen_owl_names_them() {
        let sv = mixed_enum();
        let conv = sv.converter();
        let out = schema_triples(&sv, &SchemaRdfOptions::default());
        let scheme = "https://example.com/enum-meaning-test/StatusEnum";
        let members = members_of(&out, scheme);

        let mut checked = 0usize;
        for ev in sv.enum_views().unwrap() {
            let Some(values) = ev.definition().permissible_values.clone() else {
                continue;
            };
            for (code, pv) in values {
                if pv.meaning.is_some() {
                    continue;
                }
                let (subject, _) = members
                    .iter()
                    .find(|(_, notation)| notation == &code)
                    .unwrap_or_else(|| panic!("{code} is not a member of its scheme"));

                let enum_iri = ev.canonical_uri().to_uri(&conv).unwrap().0;
                assert_eq!(
                    subject,
                    &format!("<{enum_iri}{ENUM_IRI_SEPARATOR}{code}>"),
                    "meaning-less value {code} is not named the gen-owl way"
                );
                checked += 1;
            }
        }
        assert!(checked > 0, "fixture has no meaning-less value");
    }

    /// A code that is not IRI-safe is percent-encoded into the minted IRI, as
    /// `gen-owl`'s `quote(text.strip(), safe="")` encodes it — so a code with a
    /// space cannot produce a term that is not a valid IRI at all.
    #[test]
    fn a_minted_concept_iri_percent_encodes_the_code() {
        let schema = from_yaml(Path::new(&data_path("enum_code_needs_encoding.yaml"))).unwrap();
        let mut sv = SchemaView::new();
        sv.add_schema(schema).unwrap();
        let out = schema_triples(&sv, &SchemaRdfOptions::default());
        let scheme = "https://example.com/enum-encoding-test/StatusEnum";

        let members = members_of(&out, scheme);
        let subjects: Vec<&String> = members.iter().map(|(subject, _)| subject).collect();
        assert!(
            subjects.contains(&&format!("<{scheme}#not%20known>")),
            "expected a percent-encoded minted IRI, got {subjects:?}"
        );
        // The notation stays the code as written, spaces and all: that literal
        // is what instance data carries.
        let codes: Vec<String> = members.into_iter().map(|(_, code)| code).collect();
        assert!(codes.contains(&"not known".to_string()), "{codes:?}");
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

    /// `identity` is the fixture that declares `unique_keys`, both the
    /// single-slot and the composite shape, alongside classes that have an
    /// `identifier` and classes that have neither.
    fn identity() -> SchemaView {
        let schema = from_yaml(Path::new(&data_path("identity.yaml"))).unwrap();
        let mut sv = SchemaView::new();
        sv.add_schema(schema).unwrap();
        sv
    }

    /// The `(subject, predicate, object)` triples under one predicate, as
    /// plain strings without their angle brackets.
    fn under(out: &SchemaTriples, predicate: &str) -> BTreeSet<(String, String)> {
        out.triples
            .iter()
            .filter(|t| t.predicate.as_str() == predicate)
            .map(|t| {
                let strip = |s: String| s.trim_matches(['<', '>']).to_owned();
                (strip(t.subject.to_string()), strip(t.object.to_string()))
            })
            .collect()
    }

    /// Every `owl:hasKey` on one class, each as its ordered member IRIs —
    /// walking `rdf:first` / `rdf:rest` the way a client would have to.
    fn key_lists(out: &SchemaTriples, class_iri: &str) -> Vec<Vec<String>> {
        let object_of = |subject: &str, predicate: &str| -> Option<String> {
            out.triples
                .iter()
                .find(|t| t.subject.to_string() == subject && t.predicate.as_str() == predicate)
                .map(|t| t.object.to_string())
        };

        let mut lists = Vec::new();
        for (_, head) in under(out, OWL_HAS_KEY)
            .iter()
            .filter(|(s, _)| s == class_iri)
        {
            let mut members = Vec::new();
            let mut cell = head.clone();
            while cell != RDF_NIL {
                let first = object_of(&cell, RDF_FIRST)
                    .unwrap_or_else(|| panic!("list cell {cell} has no rdf:first"));
                members.push(first.trim_matches(['<', '>']).to_owned());
                cell = object_of(&cell, RDF_REST)
                    .unwrap_or_else(|| panic!("list cell {cell} has no rdf:rest"))
                    .trim_matches(['<', '>'])
                    .to_owned();
            }
            lists.push(members);
        }
        lists.sort();
        lists
    }

    /// The mappings a class and a slot declare reach the graph under the SKOS
    /// predicate the *metamodel* gives that mapping slot — `exact_mappings`
    /// under `skos:exactMatch`, `close_mappings` under `skos:closeMatch` —
    /// with the object expanded, as `gen-owl` expands a `uriorcurie`.
    #[test]
    fn declared_mappings_reach_the_graph_under_the_metamodel_predicate() {
        let sv = personinfo();
        let out = schema_triples(&sv, &SchemaRdfOptions::default());

        // `NamedThing` declares `close_mappings: [schema:Thing]`.
        assert!(
            under(&out, SKOS_CLOSE_MATCH).contains(&(
                class_iri(&sv, "NamedThing"),
                "http://schema.org/Thing".to_owned(),
            )),
            "close_mappings on a class must be skos:closeMatch, got {:?}",
            under(&out, SKOS_CLOSE_MATCH)
        );
        // `Person.aliases` declares `exact_mappings: [schema:alternateName]`.
        assert!(
            under(&out, SKOS_EXACT_MATCH).contains(&(
                slot_iri(&sv, "Person", "aliases"),
                "http://schema.org/alternateName".to_owned(),
            )),
            "exact_mappings on a slot must be skos:exactMatch, got {:?}",
            under(&out, SKOS_EXACT_MATCH)
        );
    }

    /// `gen-owl` also emits `skos:exactMatch` between a class's *native* URI
    /// and its `class_uri` — a bridge between two spellings of one class. This
    /// module has only one spelling for a class (see *Which IRI names a class*),
    /// so that bridge has no second subject to connect and is not emitted. The
    /// failure mode it guards against is a self-referential `?c exactMatch ?c`.
    #[test]
    fn no_mapping_is_invented_for_a_class_with_a_class_uri() {
        let out = schema_triples(&personinfo(), &SchemaRdfOptions::default());
        for (subject, object) in under(&out, SKOS_EXACT_MATCH) {
            assert_ne!(
                subject, object,
                "a class is not a mapping of itself: {subject}"
            );
        }
    }

    /// A `unique_keys` entry becomes `?class owl:hasKey ( ?slot … )`, the
    /// members in declaration order and named by the same IRI the graph uses
    /// for that slot as a predicate — a key spelled any other way would not
    /// join to anything.
    #[test]
    fn unique_keys_become_owl_has_key_lists() {
        let sv = identity();
        let out = schema_triples(&sv, &SchemaRdfOptions::default());

        // A composite key: `Contact.contact_identity` is `[kind, phone]`.
        assert_eq!(
            key_lists(&out, &class_iri(&sv, "Contact")),
            vec![vec![
                slot_iri(&sv, "Contact", "kind"),
                slot_iri(&sv, "Contact", "phone"),
            ]],
            "a composite key must keep its declared slot order"
        );
        // A single-slot key still gets a one-member list, as `gen-owl` writes it.
        assert_eq!(
            key_lists(&out, &class_iri(&sv, "ServicePhoneNumber")),
            vec![vec![slot_iri(
                &sv,
                "ServicePhoneNumber",
                "hasNumberFunction"
            )]],
        );
    }

    /// `gen-owl` reads `unique_keys` and nothing else here: an `identifier` or
    /// a `key` slot produces no `owl:hasKey`, however natural it would be to
    /// treat it as one. `personinfo` declares `identifier` slots and no
    /// `unique_keys`, so its graph must carry no `owl:hasKey` at all.
    #[test]
    fn an_identifier_slot_is_not_a_has_key() {
        let sv = personinfo();
        let out = schema_triples(&sv, &SchemaRdfOptions::default());

        let cv = sv
            .class_views()
            .unwrap()
            .into_iter()
            .find(|cv| cv.name() == "Person")
            .expect("personinfo has a Person class");
        assert!(
            cv.identifier_slot().is_some(),
            "fixture has no identifier slot, so this test would not be testing anything"
        );
        assert!(
            under(&out, OWL_HAS_KEY).is_empty(),
            "identifier and key are not unique_keys: {:?}",
            under(&out, OWL_HAS_KEY)
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
