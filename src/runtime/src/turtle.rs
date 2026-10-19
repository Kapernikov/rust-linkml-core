//! RDF writers for [`LinkMLInstance`]: Turtle, N-Triples, N-Quads and TriG.
//!
//! The instance is walked once and every triple is tagged with a
//! [`TypePartition`]: non-type triples and a node's own `rdf:type` are `Own`;
//! an `rdf:type` per `is_a` ancestor is `IsA`, per mixin-only class `Mixin`;
//! one `sesame:directType` naming the node's own class is `Direct`. This holds
//! for every typed node, records and inlined objects alike.
//!
//! Every writer takes a [`WriteOptions`], whose `partition_graphs` says which
//! partitions are written and into which graph; a partition not in the map is
//! not written. The flat writers ([`write_turtle`], [`turtle_to_string`],
//! [`write_ntriples`]) cannot carry a graph name, so they accept only the
//! default graph as a target ([`WriteOptions::flat`] builds that). The quad
//! formats (N-Quads, TriG), written through
//! [`RdfStreamWriter`](crate::rdf_export::RdfStreamWriter), accept any graph,
//! the default graph included. With the default options the output is exactly the
//! single-`rdf:type` output.
//!
//! A `designates_type` slot is never written; one mapped onto `rdf:type` is
//! rejected with an `InvalidInput` error.

use linkml_meta::SchemaDefinition;
use linkml_schemaview::Converter;

use linkml_schemaview::identifier::Identifier;
use linkml_schemaview::schemaview::{ClassView, SchemaView};
use linkml_schemaview::slotview::{SlotView, TermDescriptor, TermKind};
use serde_json::Value as JsonValue;
use std::io::{Result as IoResult, Write};

use oxrdf::{BlankNode, GraphName, Literal, NamedNode, NamedOrBlankNode, QuadRef, Term, Triple};
use oxttl::turtle::WriterTurtleSerializer;
use oxttl::{NQuadsSerializer, NTriplesSerializer, TriGSerializer, TurtleSerializer};

use crate::rdf_export::{RdfOutputFormat, RdfStreamWriter};
use crate::schema_rdf::{TypePartition, TypeRegime, RDF_TYPE, SESAME_DIRECT_TYPE};
use crate::LinkMLInstance;
use std::collections::{BTreeMap, BTreeSet};

/// Options shared by every RDF writer.
///
/// `partition_graphs` routes each written [`TypePartition`] to a graph;
/// partitions not in the map are not written, and several partitions may share
/// one graph. The library holds no graph IRIs of its own: the caller supplies
/// stable ones. The map must not be empty (nothing would be written), and the
/// flat writers (Turtle and N-Triples) additionally require every target to be
/// the default graph; either violation is an `InvalidInput` error from the
/// writer.
///
/// The struct is `#[non_exhaustive]`: build it with [`WriteOptions::flat`] or
/// [`WriteOptions::quads`] and the `with_*` methods.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct WriteOptions {
    /// Use skolem IRIs instead of blank nodes for objects without an identifier.
    pub skolem: bool,
    /// Where each written partition goes; partitions not in the map are not
    /// written.
    pub partition_graphs: BTreeMap<TypePartition, GraphName>,
}

impl WriteOptions {
    /// Write every partition of `regime` into the default graph. This is what
    /// the flat writers need.
    pub fn flat(regime: TypeRegime) -> Self {
        Self::quads(
            regime
                .partitions()
                .map(|p| (p, GraphName::DefaultGraph))
                .collect(),
        )
    }

    /// Write each partition in `partition_graphs` into its graph.
    pub fn quads(partition_graphs: BTreeMap<TypePartition, GraphName>) -> Self {
        Self {
            skolem: false,
            partition_graphs,
        }
    }

    /// The same options with skolem IRIs on or off.
    pub fn with_skolem(mut self, skolem: bool) -> Self {
        self.skolem = skolem;
        self
    }

    /// Reject options no writer can honour; `flat` also rejects a named graph.
    pub(crate) fn validate(&self, format: &str, flat: bool) -> IoResult<()> {
        if self.partition_graphs.is_empty() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("{format}: partition_graphs is empty, so nothing would be written"),
            ));
        }
        if flat {
            if let Some((partition, graph)) = self
                .partition_graphs
                .iter()
                .find(|(_, g)| !matches!(g, GraphName::DefaultGraph))
            {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!(
                        "{format} cannot carry a graph name, but partition '{partition}' is \
                         routed to {graph}; use a quad writer or route it to the default graph"
                    ),
                ));
            }
        }
        Ok(())
    }
}

impl Default for WriteOptions {
    /// [`WriteOptions::flat`] of [`TypeRegime::ASSERTED`], without skolem IRIs.
    fn default() -> Self {
        Self::flat(TypeRegime::ASSERTED)
    }
}

enum Node {
    Named(String),
    Blank(String),
}

impl Node {
    fn as_subject(&self) -> NamedOrBlankNode {
        match self {
            Node::Named(iri) => NamedOrBlankNode::NamedNode(NamedNode::new_unchecked(iri.clone())),
            Node::Blank(id) => NamedOrBlankNode::BlankNode(BlankNode::new_unchecked(id.clone())),
        }
    }

    fn as_term(&self) -> Term {
        match self {
            Node::Named(iri) => Term::NamedNode(NamedNode::new_unchecked(iri.clone())),
            Node::Blank(id) => Term::BlankNode(BlankNode::new_unchecked(id.clone())),
        }
    }

    fn id(&self) -> &str {
        match self {
            Node::Named(iri) => iri,
            Node::Blank(id) => id,
        }
    }
}

struct State {
    counter: usize,
    base: String,
    skolem: bool,
}

impl State {
    fn next_subject(&mut self) -> Node {
        self.counter += 1;
        if self.skolem {
            Node::Named(format!("{}gen{}", self.base, self.counter))
        } else {
            Node::Blank(format!("b{}", self.counter))
        }
    }

    fn child_subject(&mut self, parent: &Node, part: &str) -> Node {
        if self.skolem {
            let parent_id = parent.id();
            let delim = if parent_id.ends_with('/') { "" } else { "/" };
            Node::Named(format!("{}{}{}", parent_id, delim, part))
        } else {
            self.next_subject()
        }
    }
}

fn literal_value(v: &JsonValue) -> String {
    match v {
        JsonValue::String(s) => s.to_string(),
        JsonValue::Number(n) => n.to_string(),
        JsonValue::Bool(b) => b.to_string(),
        _ => v.to_string(),
    }
}

/// Characters that may appear literally in one segment of a skolem IRI path:
/// the RFC 3986 unreserved set (`ALPHA / DIGIT / "-" / "." / "_" / "~"`).
/// Everything else — `/` and space above all — is percent-encoded, so a key
/// value can never introduce a path segment of its own.
///
/// Owned by [`linkml_schemaview::enumview`], which needs the same encoding to
/// name a permissible value and sits below this crate. One set, so a skolem
/// path segment and an enum value's IRI cannot be encoded differently.
pub(crate) use linkml_schemaview::enumview::encode_path_part;

fn slot_predicate_iri(slot: &SlotView, conv: &Converter) -> String {
    // The canonical URI (respects slot_uri and the originating schema's
    // default_prefix), expanded. Shared with `schema_rdf` so the predicate an
    // instance is written with and the subject the schema describes cannot
    // drift apart; on a CURIE the converter cannot expand this writer emits it
    // anyway, which is the long-standing behaviour.
    crate::schema_rdf::slot_predicate_iri(slot, conv).unwrap_or_else(|raw| raw)
}

fn literal_and_type(value: &JsonValue, slot: &SlotView) -> (String, Option<String>) {
    let lit = literal_value(value);
    let dt = slot
        .get_range_info()
        .first()
        .and_then(|ri| ri.rdf_datatype_iri.clone());
    (lit, dt)
}

/// Apply a [`TermDescriptor`] to one value.
///
/// The value-dependent half of [`SlotView::term_descriptor`]: the descriptor
/// resolves the precedence from the slot once, this turns a single value into the
/// `Term` that precedence calls for. Exposed so a consumer that has to render
/// values without serializing them — pushing a query down to SQL over stored
/// JSON, say — produces the same terms this writer does.
pub fn term_for(descriptor: &TermDescriptor, value: &JsonValue, conv: &Converter) -> Term {
    let lit = literal_value(value);
    match descriptor.kind {
        TermKind::Iri => {
            // The stored value may be a CURIE; expand it, and fall back to the
            // value itself when it is not one.
            let iri = Identifier::new(&lit)
                .to_uri(conv)
                .map(|u| u.0)
                .unwrap_or(lit);
            Term::NamedNode(NamedNode::new_unchecked(iri))
        }
        TermKind::EnumIri => {
            // Only a string can name a permissible value, and every permissible
            // value has an IRI. The literal fallback is for a stored value the
            // enum does not permit — dirty data, kept visible rather than
            // silently renamed.
            if matches!(value, JsonValue::String(_)) {
                if let Ok(idx) = descriptor
                    .enum_map
                    .binary_search_by(|(text, _)| text.as_str().cmp(lit.as_str()))
                {
                    let iri = descriptor.enum_map[idx].1.clone();
                    return Term::NamedNode(NamedNode::new_unchecked(iri));
                }
            }
            literal_term(lit, descriptor)
        }
        TermKind::Literal => literal_term(lit, descriptor),
    }
}

/// A typed, language-tagged or plain literal, per the descriptor. `datatype` and
/// `lang` are already mutually exclusive by construction.
fn literal_term(lit: String, descriptor: &TermDescriptor) -> Term {
    if let Some(dt) = &descriptor.datatype {
        return Term::Literal(Literal::new_typed_literal(
            lit,
            NamedNode::new_unchecked(dt.clone()),
        ));
    }
    if let Some(lang) = &descriptor.lang {
        if let Ok(tagged) = Literal::new_language_tagged_literal(lit.clone(), lang) {
            return Term::Literal(tagged);
        }
    }
    Term::Literal(Literal::new_simple_literal(lit))
}

/// The object term for one scalar value of `slot`.
///
/// A slot whose values are not a reproducible term has no descriptor — an
/// inlined structure is a blank node — but a scalar can still turn up there
/// (an `Anything` range, for instance), and it serializes as a literal.
fn scalar_object_term(value: &JsonValue, slot: &SlotView, conv: &Converter) -> Term {
    match slot.term_descriptor(conv) {
        Some(descriptor) => term_for(&descriptor, value, conv),
        None => literal_term(
            literal_value(value),
            &TermDescriptor {
                kind: TermKind::Literal,
                datatype: slot
                    .get_range_info()
                    .first()
                    .and_then(|ri| ri.rdf_datatype_iri.clone()),
                lang: slot.definition().in_language.clone(),
                enum_map: Vec::new(),
            },
        ),
    }
}

/// Try to collapse an Object into a language-tagged literal if its class
/// implements the `jsonld:language` / `jsonld:value` pattern.
///
/// Returns `Some(Term)` with the language-tagged literal on success, or `None`
/// if the class is not a lang-tag class or the required fields are missing.
fn try_lang_tag_collapse(
    values: &std::collections::HashMap<String, LinkMLInstance>,
    class: &ClassView,
) -> Option<Term> {
    let (lang_slot, value_slot) = class.lang_tag_slots()?;
    let lang = match values.get(&lang_slot.name) {
        Some(LinkMLInstance::Scalar { value, .. }) => literal_value(value),
        _ => return None,
    };
    let val = match values.get(&value_slot.name) {
        Some(LinkMLInstance::Scalar { value, .. }) => literal_value(value),
        _ => return None,
    };
    Literal::new_language_tagged_literal(val, &lang)
        .ok()
        .map(Term::Literal)
}

/// Pick the subject node for `map`, an object reached through `slot_name` on
/// `parent`.
///
/// An object carrying an identifier names itself. Otherwise, in skolem mode the
/// IRI is built from the path walked to reach the object: `<parent>/<slot>` for
/// a single-valued slot, and `<parent>/<slot>/<member>` for a member of a
/// multivalued one, where `member` is the object's own key if it has one and
/// the caller-supplied discriminator (a mapping key, or a list index) otherwise.
///
/// The slot has to be in the path: two keyless objects under different slots of
/// the same parent otherwise both land on `<parent>/0` and get merged into one
/// node.
fn identifier_node(
    map: &std::collections::HashMap<String, LinkMLInstance>,
    class: &ClassView,
    conv: &Converter,
    state: &mut State,
    parent: Option<&Node>,
    slot_name: &str,
    member: Option<&str>,
) -> (Node, Option<String>) {
    if let Some(id_slot) = class.identifier_slot() {
        if let Some(LinkMLInstance::Scalar { value, .. }) = map.get(&id_slot.name) {
            let lit = literal_value(value);
            if let Ok(iri) = Identifier::new(&lit).to_uri(conv) {
                return (Node::Named(iri.0), Some(id_slot.name.clone()));
            } else {
                return (Node::Named(lit), Some(id_slot.name.clone()));
            }
        }
    }
    if state.skolem {
        if let Some(p) = parent {
            let key_part = class
                .key_or_identifier_slot()
                .and_then(|ks| {
                    map.get(&ks.name).and_then(|v| match v {
                        LinkMLInstance::Scalar { value, .. } => {
                            Some(encode_path_part(&literal_value(value)))
                        }
                        _ => None,
                    })
                })
                .or_else(|| member.map(encode_path_part));
            let slot_part = encode_path_part(slot_name);
            let part = match key_part {
                Some(k) => format!("{}/{}", slot_part, k),
                None => slot_part,
            };
            let node = state.child_subject(p, &part);
            return (node, None);
        }
    }
    (state.next_subject(), None)
}

/// Receives every triple the walk produces, tagged with the [`TypePartition`]
/// it belongs to. The flat writers keep the partitions their regime names; the
/// quad writers route each one to its graph.
trait TripleSink {
    /// Whether triples of `partition` are kept at all. Lets the walk skip
    /// computing inferred types nobody will write.
    fn wants(&self, partition: TypePartition) -> bool;
    /// Write one triple of `partition` (a no-op when it is not wanted).
    fn emit(&mut self, partition: TypePartition, triple: &Triple) -> IoResult<()>;
}

/// Flat output: the partitions the options route, as Turtle triples. The
/// options are validated to target the default graph only.
struct FlatSink<'a, W: Write> {
    ser: WriterTurtleSerializer<W>,
    graphs: &'a BTreeMap<TypePartition, GraphName>,
}

impl<W: Write> TripleSink for FlatSink<'_, W> {
    fn wants(&self, partition: TypePartition) -> bool {
        self.graphs.contains_key(&partition)
    }

    fn emit(&mut self, partition: TypePartition, triple: &Triple) -> IoResult<()> {
        if self.wants(partition) {
            self.ser.serialize_triple(triple.as_ref())?;
        }
        Ok(())
    }
}

/// N-Triples output: the partitions the options route, serialized as they
/// arrive. The options are validated to target the default graph only.
struct NTriplesSink<'a, W: Write> {
    ser: oxttl::ntriples::WriterNTriplesSerializer<W>,
    graphs: &'a BTreeMap<TypePartition, GraphName>,
}

impl<W: Write> TripleSink for NTriplesSink<'_, W> {
    fn wants(&self, partition: TypePartition) -> bool {
        self.graphs.contains_key(&partition)
    }

    fn emit(&mut self, partition: TypePartition, triple: &Triple) -> IoResult<()> {
        if self.wants(partition) {
            self.ser.serialize_triple(triple.as_ref())?;
        }
        Ok(())
    }
}

/// Quad output: each partition goes to its graph; unmapped partitions are
/// dropped.
struct QuadSink<'a, F: FnMut(QuadRef<'_>) -> IoResult<()>> {
    graphs: &'a BTreeMap<TypePartition, GraphName>,
    write: F,
}

impl<F: FnMut(QuadRef<'_>) -> IoResult<()>> TripleSink for QuadSink<'_, F> {
    fn wants(&self, partition: TypePartition) -> bool {
        self.graphs.contains_key(&partition)
    }

    fn emit(&mut self, partition: TypePartition, triple: &Triple) -> IoResult<()> {
        if let Some(graph) = self.graphs.get(&partition) {
            (self.write)(QuadRef::new(
                &triple.subject,
                &triple.predicate,
                &triple.object,
                graph,
            ))?;
        }
        Ok(())
    }
}

fn rdf_type_node() -> NamedNode {
    NamedNode::new_unchecked(RDF_TYPE.to_string())
}

/// A `designates_type` slot mapped onto `rdf:type` would write the designator
/// value as a second, unrelated type of the node and collide with the type
/// regimes. Refuse it rather than guess.
fn reject_type_designator_on_rdf_type(cv: &ClassView, conv: &Converter) -> IoResult<()> {
    for slot in cv.slots() {
        if slot.definition().designates_type.unwrap_or(false)
            && slot_predicate_iri(slot, conv) == RDF_TYPE
        {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "slot '{}' of class '{}' designates the type but is mapped to rdf:type; \
                     use a different slot_uri",
                    slot.name,
                    cv.name()
                ),
            ));
        }
    }
    Ok(())
}

/// The type triples of one node: the own `rdf:type` (`Own`), one `rdf:type`
/// per inferred class (`IsA` / `Mixin`) and the `sesame:directType` naming the
/// own class (`Direct`).
fn emit_type_triples<S: TripleSink>(
    subject: &Node,
    cv: &ClassView,
    conv: &Converter,
    sink: &mut S,
) -> IoResult<()> {
    // Shared with `schema_rdf` so the `rdf:type` object here and the class
    // subject the schema triples describe are the same spelling by
    // construction, not by coincidence.
    let type_triple = |predicate: NamedNode, class: &ClassView| {
        crate::schema_rdf::instance_type_iri(class, conv).map(|iri| Triple {
            subject: subject.as_subject(),
            predicate,
            object: Term::NamedNode(NamedNode::new_unchecked(iri)),
        })
    };
    if let Some(triple) = type_triple(rdf_type_node(), cv) {
        sink.emit(TypePartition::Own, &triple)?;
    }
    if sink.wants(TypePartition::Direct) {
        if let Some(triple) =
            type_triple(NamedNode::new_unchecked(SESAME_DIRECT_TYPE.to_string()), cv)
        {
            sink.emit(TypePartition::Direct, &triple)?;
        }
    }
    if !sink.wants(TypePartition::IsA) && !sink.wants(TypePartition::Mixin) {
        return Ok(());
    }
    let partitions = crate::schema_rdf::type_partitions(cv)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;
    for (partition, class) in partitions {
        if matches!(partition, TypePartition::IsA | TypePartition::Mixin) {
            if let Some(triple) = type_triple(rdf_type_node(), &class) {
                sink.emit(partition, &triple)?;
            }
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn serialize_map<S: TripleSink>(
    subject: &Node,
    map: &std::collections::HashMap<String, LinkMLInstance>,
    class: Option<&ClassView>,
    formatter: &mut S,
    _sv: &SchemaView,
    conv: &Converter,
    state: &mut State,
    id_slot: Option<&str>,
) -> IoResult<()> {
    if let Some(cv) = class {
        reject_type_designator_on_rdf_type(cv, conv)?;
        emit_type_triples(subject, cv, conv, formatter)?;
    } else {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "Class view is required for serialization",
        ));
    }
    for (k, v) in map {
        if id_slot.map(|s| s == k.as_str()).unwrap_or(false) {
            continue;
        }
        let skip = match v {
            LinkMLInstance::Scalar { slot, .. } => {
                slot.definition().designates_type.unwrap_or(false)
            }
            LinkMLInstance::List { slot, .. } => slot.definition().designates_type.unwrap_or(false),
            _ => false,
        };
        if skip {
            continue;
        }
        // Get predicate IRI from the slot's canonical URI
        let pred_iri = match v {
            LinkMLInstance::Scalar { slot, .. }
            | LinkMLInstance::Null { slot, .. }
            | LinkMLInstance::List { slot, .. }
            | LinkMLInstance::Mapping { slot, .. } => slot_predicate_iri(slot, conv),
            LinkMLInstance::Object { .. } => {
                // For Object, look up the slot from the parent class
                if let Some(cv) = class {
                    if let Some(slot) = cv.slots().iter().find(|s| s.name == *k) {
                        slot_predicate_iri(slot, conv)
                    } else {
                        // Slot not found in class - this shouldn't happen for valid data
                        let canonical = cv.canonical_uri();
                        let base = canonical.to_uri(conv).map(|u| u.0).unwrap_or_default();
                        format!("{}/{}", base.trim_end_matches('/'), k)
                    }
                } else {
                    // No class context - shouldn't happen
                    k.clone()
                }
            }
        };
        let predicate = NamedNode::new_unchecked(pred_iri.clone());
        match v {
            LinkMLInstance::Scalar { value, slot, .. } => {
                let triple = Triple {
                    subject: subject.as_subject(),
                    predicate: predicate.clone(),
                    object: scalar_object_term(value, slot, conv),
                };
                formatter.emit(TypePartition::Own, &triple)?;
            }
            LinkMLInstance::Null { .. } => {
                // Null is treated as absent; emit nothing
            }
            LinkMLInstance::Object { values, class, .. } => {
                let class_ref = &class;
                // Try lang-tag collapse: emit "value"@lang instead of a blank node
                if let Some(lang_term) = try_lang_tag_collapse(values, class_ref) {
                    let triple = Triple {
                        subject: subject.as_subject(),
                        predicate: predicate.clone(),
                        object: lang_term,
                    };
                    formatter.emit(TypePartition::Own, &triple)?;
                } else {
                    let (obj, child_id) =
                        identifier_node(values, class_ref, conv, state, Some(subject), k, None);
                    let triple = Triple {
                        subject: subject.as_subject(),
                        predicate: predicate.clone(),
                        object: obj.as_term(),
                    };
                    formatter.emit(TypePartition::Own, &triple)?;
                    serialize_map(
                        &obj,
                        values,
                        Some(class_ref),
                        formatter,
                        _sv,
                        conv,
                        state,
                        child_id.as_deref(),
                    )?;
                }
            }
            LinkMLInstance::List { values, slot, .. } => {
                for (idx, item) in values.iter().enumerate() {
                    match item {
                        LinkMLInstance::Scalar { value, .. } => {
                            let triple = Triple {
                                subject: subject.as_subject(),
                                predicate: predicate.clone(),
                                object: scalar_object_term(value, slot, conv),
                            };
                            formatter.emit(TypePartition::Own, &triple)?;
                        }
                        LinkMLInstance::Null { .. } => {
                            // Skip null items
                        }
                        LinkMLInstance::Object {
                            values: mv, class, ..
                        } => {
                            let class_ref = &class;
                            if let Some(lang_term) = try_lang_tag_collapse(mv, class_ref) {
                                let triple = Triple {
                                    subject: subject.as_subject(),
                                    predicate: predicate.clone(),
                                    object: lang_term,
                                };
                                formatter.emit(TypePartition::Own, &triple)?;
                            } else {
                                // Position in the list is the discriminator for
                                // members that carry no key of their own.
                                let position = idx.to_string();
                                let (obj, child_id) = identifier_node(
                                    mv,
                                    class_ref,
                                    conv,
                                    state,
                                    Some(subject),
                                    k,
                                    Some(position.as_str()),
                                );
                                let triple = Triple {
                                    subject: subject.as_subject(),
                                    predicate: predicate.clone(),
                                    object: obj.as_term(),
                                };
                                formatter.emit(TypePartition::Own, &triple)?;
                                serialize_map(
                                    &obj,
                                    mv,
                                    Some(class_ref),
                                    formatter,
                                    _sv,
                                    conv,
                                    state,
                                    child_id.as_deref(),
                                )?;
                            }
                        }
                        LinkMLInstance::List { .. } => {}
                        LinkMLInstance::Mapping { .. } => {}
                    }
                }
            }
            LinkMLInstance::Mapping { values, .. } => {
                for (member_key, item) in values.iter() {
                    match item {
                        LinkMLInstance::Scalar { value: v, slot, .. } => {
                            let triple = Triple {
                                subject: subject.as_subject(),
                                predicate: predicate.clone(),
                                object: scalar_object_term(v, slot, conv),
                            };
                            formatter.emit(TypePartition::Own, &triple)?;
                        }
                        LinkMLInstance::Null { .. } => {
                            // nothing
                        }
                        LinkMLInstance::Object {
                            values: mv, class, ..
                        } => {
                            let class_ref = class;
                            if let Some(lang_term) = try_lang_tag_collapse(mv, class_ref) {
                                let triple = Triple {
                                    subject: subject.as_subject(),
                                    predicate: predicate.clone(),
                                    object: lang_term,
                                };
                                formatter.emit(TypePartition::Own, &triple)?;
                            } else {
                                let (obj, child_id) = identifier_node(
                                    mv,
                                    class_ref,
                                    conv,
                                    state,
                                    Some(subject),
                                    k,
                                    Some(member_key.as_str()),
                                );
                                let triple = Triple {
                                    subject: subject.as_subject(),
                                    predicate: predicate.clone(),
                                    object: obj.as_term(),
                                };
                                formatter.emit(TypePartition::Own, &triple)?;
                                serialize_map(
                                    &obj,
                                    mv,
                                    Some(class_ref),
                                    formatter,
                                    _sv,
                                    conv,
                                    state,
                                    child_id.as_deref(),
                                )?;
                            }
                        }
                        LinkMLInstance::List { .. } => {}
                        LinkMLInstance::Mapping { .. } => {}
                    }
                }
            }
        }
    }
    Ok(())
}

/// Walk `value` once, handing every triple to `sink`. Blank node labels come
/// from one counter, so they are consistent across everything the sink routes.
/// The counter continues from `*counter` and is written back, so several walks
/// into one stream can share it and never reuse a label.
fn walk<S: TripleSink>(
    value: &LinkMLInstance,
    sv: &SchemaView,
    schema: &SchemaDefinition,
    conv: &Converter,
    formatter: &mut S,
    skolem: bool,
    counter: &mut usize,
) -> IoResult<()> {
    let base = schema.id.trim_end_matches('#').to_string();
    let mut state = State {
        counter: *counter,
        base: if base.ends_with('/') {
            base
        } else {
            format!("{}/", base)
        },
        skolem,
    };
    match value {
        LinkMLInstance::Object { values, class, .. } => {
            let cv = &class;
            let mut id_slot_name = None;
            let subj = if let Some(id_slot) = cv.identifier_slot() {
                if let Some(LinkMLInstance::Scalar { value, .. }) = values.get(&id_slot.name) {
                    let lit = literal_value(value);
                    let iri = Identifier::new(&lit)
                        .to_uri(conv)
                        .map(|u| u.0)
                        .unwrap_or(lit);
                    id_slot_name = Some(id_slot.name.clone());
                    Node::Named(iri)
                } else {
                    Node::Named(format!("{}root", state.base))
                }
            } else {
                Node::Named(format!("{}root", state.base))
            };
            serialize_map(
                &subj,
                values,
                Some(cv),
                formatter,
                sv,
                conv,
                &mut state,
                id_slot_name.as_deref(),
            )?;
        }
        LinkMLInstance::Mapping { values, .. } => {
            for (idx, item) in values.values().enumerate() {
                let subj = if skolem {
                    Node::Named(format!("{}root/{}", state.base, idx))
                } else {
                    state.next_subject()
                };
                match item {
                    LinkMLInstance::Object {
                        values: mv, class, ..
                    } => {
                        let class = Some(class);
                        serialize_map(&subj, mv, class, formatter, sv, conv, &mut state, None)?;
                    }
                    LinkMLInstance::Scalar { value: v, slot, .. } => {
                        let (lit, dt_opt) = literal_and_type(v, slot);
                        if let Some(dt) = dt_opt {
                            let object = Term::Literal(Literal::new_typed_literal(
                                lit.clone(),
                                NamedNode::new_unchecked(dt.clone()),
                            ));
                            let triple = Triple {
                                subject: subj.as_subject(),
                                predicate: NamedNode::new_unchecked(
                                    "http://www.w3.org/1999/02/22-rdf-syntax-ns#value".to_string(),
                                ),
                                object,
                            };
                            formatter.emit(TypePartition::Own, &triple)?;
                        } else {
                            let object = Term::Literal(Literal::new_simple_literal(lit.clone()));
                            let triple = Triple {
                                subject: subj.as_subject(),
                                predicate: NamedNode::new_unchecked(
                                    "http://www.w3.org/1999/02/22-rdf-syntax-ns#value".to_string(),
                                ),
                                object,
                            };
                            formatter.emit(TypePartition::Own, &triple)?;
                        }
                    }
                    LinkMLInstance::Null { .. } => {}
                    LinkMLInstance::List { .. } => {}
                    LinkMLInstance::Mapping { .. } => {}
                }
            }
        }
        LinkMLInstance::List { values, .. } => {
            for (idx, item) in values.iter().enumerate() {
                let subj = if skolem {
                    Node::Named(format!("{}root/{}", state.base, idx))
                } else {
                    state.next_subject()
                };
                match item {
                    LinkMLInstance::Object {
                        values: mv, class, ..
                    } => {
                        let class = Some(class);
                        serialize_map(&subj, mv, class, formatter, sv, conv, &mut state, None)?;
                    }
                    LinkMLInstance::Scalar { value, slot, .. } => {
                        let (lit, dt_opt) = literal_and_type(value, slot);
                        if let Some(dt) = dt_opt {
                            let object = Term::Literal(Literal::new_typed_literal(
                                lit.clone(),
                                NamedNode::new_unchecked(dt.clone()),
                            ));
                            let triple = Triple {
                                subject: subj.as_subject(),
                                predicate: NamedNode::new_unchecked(
                                    "http://www.w3.org/1999/02/22-rdf-syntax-ns#value".to_string(),
                                ),
                                object,
                            };
                            formatter.emit(TypePartition::Own, &triple)?;
                        } else {
                            let object = Term::Literal(Literal::new_simple_literal(lit.clone()));
                            let triple = Triple {
                                subject: subj.as_subject(),
                                predicate: NamedNode::new_unchecked(
                                    "http://www.w3.org/1999/02/22-rdf-syntax-ns#value".to_string(),
                                ),
                                object,
                            };
                            formatter.emit(TypePartition::Own, &triple)?;
                        }
                    }
                    LinkMLInstance::Null { .. } => {
                        // nothing
                    }
                    LinkMLInstance::List { .. } => {}
                    LinkMLInstance::Mapping { .. } => {}
                }
            }
        }
        LinkMLInstance::Scalar { .. } => {}
        LinkMLInstance::Null { .. } => {}
    }
    *counter = state.counter;
    Ok(())
}

/// Write `value` as Turtle, keeping the partitions `options` routes.
///
/// Turtle cannot carry a graph name: an empty `partition_graphs`, or a
/// partition routed to a named graph, is an `InvalidInput` error.
pub fn write_turtle<W: Write>(
    value: &LinkMLInstance,
    sv: &SchemaView,
    schema: &SchemaDefinition,
    conv: &Converter,
    w: &mut W,
    options: &WriteOptions,
) -> IoResult<()> {
    let mut stream = RdfStreamWriter::new(w, RdfOutputFormat::Turtle, sv, schema, conv, options)?;
    stream.write(value)?;
    stream.finish()?;
    Ok(())
}

/// One Turtle instance with the blank node counter and the set of prefixes
/// already declared supplied by the caller, so several instances written into
/// one stream neither reuse labels nor repeat `@prefix` lines.
#[allow(clippy::too_many_arguments)]
pub(crate) fn write_turtle_counted<W: Write>(
    value: &LinkMLInstance,
    sv: &SchemaView,
    schema: &SchemaDefinition,
    conv: &Converter,
    w: &mut W,
    options: &WriteOptions,
    counter: &mut usize,
    declared: &mut BTreeSet<String>,
) -> IoResult<()> {
    options.validate("Turtle", true)?;
    let mut sink = FlatSink {
        ser: TurtleSerializer::new().for_writer(Vec::new()),
        graphs: &options.partition_graphs,
    };
    walk(value, sv, schema, conv, &mut sink, options.skolem, counter)?;
    let out_buf = sink.ser.finish()?;
    let out = String::from_utf8(out_buf)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    w.write_all(with_prefix_header(&out, conv, declared).as_bytes())
}

/// Compact IRIs in serialized `out` (Turtle or TriG, written without
/// prefixes) to prefixed names and put the `@prefix` header for exactly the
/// prefixes used in front of it, leaving out the prefixes `declared` already
/// holds and adding the ones it writes. A fresh `declared` gives the header of
/// a single document.
///
/// An IRI is compacted only when its local name is a plain Turtle `PN_LOCAL`
/// that needs no escape, so output stays readable by any parser (the
/// serializer's own `with_prefix` would write `ex:Status\#unknown`). String
/// literals are copied through untouched, whatever they contain.
fn with_prefix_header(out: &str, conv: &Converter, declared: &mut BTreeSet<String>) -> String {
    let (body, mut used_prefixes) = compact_iris(out, conv);
    used_prefixes
        .entry("xsd".to_string())
        .or_insert_with(|| "http://www.w3.org/2001/XMLSchema#".to_string());
    let mut header = String::new();
    for (pfx, uri) in &used_prefixes {
        if declared.insert(pfx.clone()) {
            header.push_str(&format!("@prefix {}: <{}> .\n", pfx, uri));
        }
    }
    if !header.is_empty() {
        header.push('\n');
    }
    header.push_str(&body);
    header
}

/// Replace `<IRI>` tokens outside string literals with prefixed names where
/// [`plain_curie`] allows, returning the text and the prefix to namespace map
/// of every prefix used.
///
/// The scanner steps over string literals (`"…"`, `'…'` and their long forms,
/// a backslash escaping the next character) and treats every `<` outside one
/// as the start of an IRI, which is what the serializer writes. All delimiters
/// are ASCII, so every slice below falls on a character boundary.
fn compact_iris(out: &str, conv: &Converter) -> (String, BTreeMap<String, String>) {
    let bytes = out.as_bytes();
    let mut result = String::with_capacity(out.len());
    let mut used: BTreeMap<String, String> = BTreeMap::new();
    let mut last = 0; // start of text not yet copied
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            quote @ (b'"' | b'\'') => {
                let long = bytes.get(i + 1) == Some(&quote) && bytes.get(i + 2) == Some(&quote);
                i = skip_literal(bytes, i, quote, long);
            }
            b'<' => {
                let Some(len) = bytes[i + 1..].iter().position(|&b| b == b'>') else {
                    break;
                };
                let end = i + 1 + len; // index of `>`
                let iri = &out[i + 1..end];
                if let Some((prefix, local)) = plain_curie(iri, conv) {
                    result.push_str(&out[last..i]);
                    result.push_str(&prefix);
                    result.push(':');
                    result.push_str(local);
                    used.entry(prefix)
                        .or_insert_with(|| iri[..iri.len() - local.len()].to_string());
                    last = end + 1;
                }
                i = end + 1;
            }
            _ => i += 1,
        }
    }
    result.push_str(&out[last..]);
    (result, used)
}

/// Index just past the string literal starting at `start` (whose quote
/// character is `quote`); the rest of the text when it is unterminated.
fn skip_literal(bytes: &[u8], start: usize, quote: u8, long: bool) -> usize {
    let mut i = start + if long { 3 } else { 1 };
    while i < bytes.len() {
        match bytes[i] {
            b'\\' => i += 2,
            b if b == quote && !long => return i + 1,
            b if b == quote
                && bytes.get(i + 1) == Some(&quote)
                && bytes.get(i + 2) == Some(&quote) =>
            {
                return i + 3
            }
            _ => i += 1,
        }
    }
    bytes.len()
}

/// The prefix and local name `conv` compacts `iri` to, when the local name is
/// a plain `PN_LOCAL` (no escape, no percent sequence) and the prefix a valid
/// `PN_PREFIX`.
fn plain_curie<'a>(iri: &'a str, conv: &Converter) -> Option<(String, &'a str)> {
    let curie = conv.compress(iri).ok()?;
    let (prefix, local) = curie.split_once(':')?;
    // `local` is the tail of `iri`; it must be, for the namespace to be
    // recoverable as `iri` minus `local`.
    let local = iri.strip_suffix(local).map(|ns| &iri[ns.len()..])?;
    (is_plain_pn_local(local) && is_pn_prefix(prefix)).then(|| (prefix.to_string(), local))
}

fn is_pn_chars_base(c: char) -> bool {
    matches!(c,
        'A'..='Z' | 'a'..='z' | '\u{00C0}'..='\u{00D6}' | '\u{00D8}'..='\u{00F6}'
        | '\u{00F8}'..='\u{02FF}' | '\u{0370}'..='\u{037D}' | '\u{037F}'..='\u{1FFF}'
        | '\u{200C}'..='\u{200D}' | '\u{2070}'..='\u{218F}' | '\u{2C00}'..='\u{2FEF}'
        | '\u{3001}'..='\u{D7FF}' | '\u{F900}'..='\u{FDCF}' | '\u{FDF0}'..='\u{FFFD}'
        | '\u{10000}'..='\u{EFFFF}')
}

fn is_pn_chars_u(c: char) -> bool {
    is_pn_chars_base(c) || c == '_'
}

fn is_pn_chars(c: char) -> bool {
    is_pn_chars_u(c)
        || matches!(c, '-' | '0'..='9' | '\u{00B7}' | '\u{0300}'..='\u{036F}' | '\u{203F}'..='\u{2040}')
}

/// Turtle `PN_PREFIX`: `PN_CHARS_BASE ((PN_CHARS | '.')* PN_CHARS)?`.
fn is_pn_prefix(prefix: &str) -> bool {
    let mut chars = prefix.chars();
    match chars.next() {
        None => true,
        Some(first) => {
            is_pn_chars_base(first)
                && prefix.chars().all(|c| is_pn_chars(c) || c == '.')
                && !prefix.ends_with('.')
        }
    }
}

/// Turtle `PN_LOCAL` restricted to its escape-free alternatives:
/// `(PN_CHARS_U | ':' | [0-9]) ((PN_CHARS | '.' | ':')* (PN_CHARS | ':'))?`.
/// The empty local name is allowed (`ex:`); `PLX` (percent sequences and
/// backslash escapes) is not.
fn is_plain_pn_local(local: &str) -> bool {
    let mut chars = local.chars();
    match chars.next() {
        None => true,
        Some(first) => {
            (is_pn_chars_u(first) || first == ':' || first.is_ascii_digit())
                && local
                    .chars()
                    .all(|c| is_pn_chars(c) || c == '.' || c == ':')
                && !local.ends_with('.')
        }
    }
}

/// [`write_turtle`] into a `String`.
pub fn turtle_to_string(
    value: &LinkMLInstance,
    sv: &SchemaView,
    schema: &SchemaDefinition,
    conv: &Converter,
    options: &WriteOptions,
) -> Result<String, std::io::Error> {
    let mut buf = Vec::new();
    write_turtle(value, sv, schema, conv, &mut buf, options)?;
    String::from_utf8(buf).map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
}

/// Write `value` as N-Triples (one triple per line, full IRIs, no prefixes),
/// keeping the partitions `options` routes.
///
/// N-Triples cannot carry a graph name: an empty `partition_graphs`, or a
/// partition routed to a named graph, is an `InvalidInput` error.
pub fn write_ntriples<W: Write>(
    value: &LinkMLInstance,
    sv: &SchemaView,
    schema: &SchemaDefinition,
    conv: &Converter,
    w: &mut W,
    options: &WriteOptions,
) -> IoResult<()> {
    let mut stream = RdfStreamWriter::new(w, RdfOutputFormat::NTriples, sv, schema, conv, options)?;
    stream.write(value)?;
    stream.finish()?;
    Ok(())
}

/// One N-Triples instance with a caller-supplied blank node counter.
pub(crate) fn write_ntriples_counted<W: Write>(
    value: &LinkMLInstance,
    sv: &SchemaView,
    schema: &SchemaDefinition,
    conv: &Converter,
    w: &mut W,
    options: &WriteOptions,
    counter: &mut usize,
) -> IoResult<()> {
    options.validate("N-Triples", true)?;
    let mut sink = NTriplesSink {
        ser: NTriplesSerializer::new().for_writer(&mut *w),
        graphs: &options.partition_graphs,
    };
    walk(value, sv, schema, conv, &mut sink, options.skolem, counter)?;
    sink.ser.finish();
    Ok(())
}

/// One N-Quads instance, each partition into the graph `options` routes it to
/// (the default graph included), with a caller-supplied blank node counter.
///
/// The instance is walked once, so blank node labels agree across graphs.
/// An empty `partition_graphs` is an `InvalidInput` error, and so is a
/// `designates_type` slot mapped to `rdf:type`, as in the flat writers.
pub(crate) fn write_nquads_counted<W: Write>(
    value: &LinkMLInstance,
    sv: &SchemaView,
    schema: &SchemaDefinition,
    conv: &Converter,
    w: &mut W,
    options: &WriteOptions,
    counter: &mut usize,
) -> IoResult<()> {
    options.validate("N-Quads", false)?;
    let mut ser = NQuadsSerializer::new().for_writer(Vec::new());
    let mut sink = QuadSink {
        graphs: &options.partition_graphs,
        write: |quad: QuadRef<'_>| ser.serialize_quad(quad),
    };
    walk(value, sv, schema, conv, &mut sink, options.skolem, counter)?;
    w.write_all(&ser.finish())?;
    Ok(())
}

/// One TriG instance with the same content as [`write_nquads_counted`] and the
/// `@prefix` header of [`write_turtle`] (only for prefixes `declared` does not
/// hold yet). Errors as [`write_nquads_counted`] does.
#[allow(clippy::too_many_arguments)]
pub(crate) fn write_trig_counted<W: Write>(
    value: &LinkMLInstance,
    sv: &SchemaView,
    schema: &SchemaDefinition,
    conv: &Converter,
    w: &mut W,
    options: &WriteOptions,
    counter: &mut usize,
    declared: &mut BTreeSet<String>,
) -> IoResult<()> {
    options.validate("TriG", false)?;
    let mut ser = TriGSerializer::new().for_writer(Vec::new());
    let mut sink = QuadSink {
        graphs: &options.partition_graphs,
        write: |quad: QuadRef<'_>| ser.serialize_quad(quad),
    };
    walk(value, sv, schema, conv, &mut sink, options.skolem, counter)?;
    let out = String::from_utf8(ser.finish()?)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    w.write_all(with_prefix_header(&out, conv, declared).as_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;
    use linkml_schemaview::converter::Record;

    fn make_converter(prefixes: &[(&str, &str)]) -> Converter {
        let mut conv = Converter::default();
        for (prefix, uri_prefix) in prefixes {
            conv.add_record(Record::new(prefix, uri_prefix)).unwrap();
        }
        conv
    }

    #[test]
    fn compact_keeps_iri_when_local_name_starts_with_hash() {
        // RSM maps to a URI without trailing # or /, so the IRI would compress
        // to RSM:#EAID_xxx, which is not a Turtle prefixed name.
        let conv = make_converter(&[("RSM", "http://rsm.uic.org/RSM12")]);
        let input = "<http://rsm.uic.org/RSM12#EAID_55DDBCD9> a RSM:Thing .";
        let (result, used) = compact_iris(input, &conv);
        assert_eq!(result, input);
        assert!(!used.contains_key("RSM"));
    }

    #[test]
    fn compact_keeps_iri_when_local_name_contains_slash() {
        let conv = make_converter(&[("asset360", "https://data.example.com/asset360/")]);
        let input = "<https://data.example.com/asset360/unit/Meter> a asset360:Thing .";
        let (result, used) = compact_iris(input, &conv);
        assert_eq!(result, input);
        assert!(!used.contains_key("asset360"));
    }

    #[test]
    fn compact_keeps_iri_when_local_name_needs_an_escape() {
        let conv = make_converter(&[("ex", "http://example.com/")]);
        for iri in [
            "http://example.com/StatusEnum#unknown",
            "http://example.com/a%20b",
            "http://example.com/end.",
            "http://example.com/a(b)",
        ] {
            let input = format!("<{iri}> a <http://example.com/Class> .");
            let (result, used) = compact_iris(&input, &conv);
            assert_eq!(result, format!("<{iri}> a ex:Class ."));
            assert_eq!(used["ex"], "http://example.com/");
        }
    }

    #[test]
    fn compact_compresses_plain_local_names() {
        let conv = make_converter(&[("ex", "http://example.com/")]);
        let input = "<http://example.com/Thing> a <http://example.com/Class_1.x-y> .";
        let (result, used) = compact_iris(input, &conv);
        assert_eq!(result, "ex:Thing a ex:Class_1.x-y .");
        assert_eq!(used["ex"], "http://example.com/");
    }

    #[test]
    fn compact_never_touches_literals() {
        let conv = make_converter(&[("ex", "http://example.com/")]);
        let iri = "<http://example.com/x>";
        for lit in [
            format!(r#""see {iri}""#),
            format!("'see {iri}'"),
            format!(r#""say \"hi\" {iri}""#),
            format!(r#""""a "quoted" {iri} and "" more""""#),
            format!("'''it's {iri}'''"),
            format!(r#""é <http://example.com/é> {iri}""#),
        ] {
            let input = format!("{iri} <http://example.com/p> {lit} .");
            let (result, _) = compact_iris(&input, &conv);
            assert_eq!(result, format!("ex:x ex:p {lit} ."), "input: {input}");
        }
    }

    #[test]
    fn compact_is_safe_on_unterminated_input() {
        let conv = make_converter(&[("ex", "http://example.com/")]);
        for input in [
            "\"abc",
            "\"\"\"abc\"\"",
            "<http://example.com/x",
            "\"é\\",
            "é<é>é",
        ] {
            let _ = compact_iris(input, &conv);
        }
    }

    #[test]
    fn header_matches_the_golden_the_regex_writer_produced() {
        // `iri_compaction_golden.ttl` is what the pre-serializer-prefix writer
        // (regex compaction) made of this input, which has no IRI-lookalike
        // inside a literal: the output must stay byte-identical.
        let conv = make_converter(&[
            ("ex", "http://example.com/"),
            ("vocab", "https://example.com/vocab#"),
            ("xsd", "http://www.w3.org/2001/XMLSchema#"),
        ]);
        let input = include_str!("../tests/data/iri_compaction_input.ttl");
        let golden = include_str!("../tests/data/iri_compaction_golden.ttl");
        assert_eq!(
            with_prefix_header(input, &conv, &mut BTreeSet::new()),
            golden
        );
    }

    #[test]
    fn hash_local_names_stay_full_iris_and_plain_ones_compact() {
        let conv = make_converter(&[("ex", "http://example.com/")]);
        let out = with_prefix_header(
            "<http://example.com/a> <http://example.com/p> <http://example.com/StatusEnum#unknown> .",
            &conv,
            &mut BTreeSet::new(),
        );
        assert!(
            out.ends_with("ex:a ex:p <http://example.com/StatusEnum#unknown> ."),
            "{out}"
        );
        assert!(!out.contains('\\'));
    }

    #[test]
    fn pn_local_accepts_plain_and_rejects_escaped() {
        assert!(is_plain_pn_local("Thing"));
        assert!(is_plain_pn_local("1abc"));
        assert!(is_plain_pn_local(""));
        assert!(!is_plain_pn_local("a#b"));
        assert!(!is_plain_pn_local("a/b"));
        assert!(!is_plain_pn_local("a%41"));
        assert!(!is_plain_pn_local("-a"));
        assert!(!is_plain_pn_local("a."));
    }
}
