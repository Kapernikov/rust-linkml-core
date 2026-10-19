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
//! writers ([`write_nquads`], [`write_trig`]) accept any graph, the default
//! graph included. With the default options the output is exactly the
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

use oxrdf::{
    BlankNode, GraphName, Literal, NamedNode, NamedOrBlankNode, Quad, QuadRef, Term, Triple,
};
use oxttl::{NQuadsSerializer, NTriplesSerializer, TriGSerializer, TurtleSerializer};

use crate::schema_rdf::{TypePartition, TypeRegime, RDF_TYPE, SESAME_DIRECT_TYPE};
use crate::LinkMLInstance;
use std::collections::BTreeMap;

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

/// Flat output: the partitions the options route, collected as triples. The
/// options are validated to target the default graph only.
struct FlatSink<'a> {
    graphs: &'a BTreeMap<TypePartition, GraphName>,
    triples: Vec<Triple>,
}

impl TripleSink for FlatSink<'_> {
    fn wants(&self, partition: TypePartition) -> bool {
        self.graphs.contains_key(&partition)
    }

    fn emit(&mut self, partition: TypePartition, triple: &Triple) -> IoResult<()> {
        if self.wants(partition) {
            self.triples.push(triple.clone());
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
    write_turtle_counted(value, sv, schema, conv, w, options, &mut 0)
}

/// [`write_turtle`] with the blank node counter supplied by the caller, so
/// several instances written into one stream do not reuse labels.
///
/// The instance's triples are collected first, so the `@prefix` header can
/// name exactly the prefixes its IRIs use; the serializer then compacts them.
pub(crate) fn write_turtle_counted<W: Write>(
    value: &LinkMLInstance,
    sv: &SchemaView,
    schema: &SchemaDefinition,
    conv: &Converter,
    w: &mut W,
    options: &WriteOptions,
    counter: &mut usize,
) -> IoResult<()> {
    options.validate("Turtle", true)?;
    let mut sink = FlatSink {
        graphs: &options.partition_graphs,
        triples: Vec::new(),
    };
    walk(value, sv, schema, conv, &mut sink, options.skolem, counter)?;
    let mut prefixes = Prefixes::default();
    for triple in &sink.triples {
        prefixes.note_triple(triple, conv);
    }
    let mut ser = prefixes
        .apply(TurtleSerializer::new(), |ser, name, iri| {
            ser.with_prefix(name, iri)
        })?
        .for_writer(&mut *w);
    for triple in &sink.triples {
        ser.serialize_triple(triple.as_ref())?;
    }
    ser.finish()?;
    Ok(())
}

/// The `@prefix` declarations one serialized document needs: the converter's
/// prefix for every IRI the document writes (so none is declared unused), and
/// `xsd` always.
#[derive(Default)]
struct Prefixes(BTreeMap<String, String>);

impl Prefixes {
    /// Record the prefix `conv` would compact `iri` with, if the result is a
    /// valid Turtle prefixed name. `rdf:type` is skipped: it is written as `a`.
    fn note_iri(&mut self, iri: &str, conv: &Converter) {
        if iri == RDF_TYPE {
            return;
        }
        let Ok(curie) = conv.compress(iri) else {
            return;
        };
        if !is_valid_turtle_local_name(&curie) {
            return;
        }
        let Some((prefix, local)) = curie.split_once(':') else {
            return;
        };
        let Some(uri_prefix) = iri.strip_suffix(local) else {
            return;
        };
        self.0
            .entry(prefix.to_string())
            .or_insert_with(|| uri_prefix.to_string());
    }

    fn note_term(&mut self, term: &Term, conv: &Converter) {
        match term {
            Term::NamedNode(n) => self.note_iri(n.as_str(), conv),
            // `xsd:string` and language-tagged strings are written bare.
            Term::Literal(l) if l.language().is_none() && l.datatype().as_str() != XSD_STRING => {
                self.note_iri(l.datatype().as_str(), conv)
            }
            _ => {}
        }
    }

    fn note_triple(&mut self, triple: &Triple, conv: &Converter) {
        if let NamedOrBlankNode::NamedNode(n) = &triple.subject {
            self.note_iri(n.as_str(), conv);
        }
        self.note_iri(triple.predicate.as_str(), conv);
        self.note_term(&triple.object, conv);
    }

    fn note_quad(&mut self, quad: &Quad, conv: &Converter) {
        if let NamedOrBlankNode::NamedNode(n) = &quad.subject {
            self.note_iri(n.as_str(), conv);
        }
        self.note_iri(quad.predicate.as_str(), conv);
        self.note_term(&quad.object, conv);
        if let GraphName::NamedNode(n) = &quad.graph_name {
            self.note_iri(n.as_str(), conv);
        }
    }

    /// Declare every collected prefix (and `xsd`) on a serializer through its
    /// `with_prefix`.
    fn apply<S>(
        mut self,
        mut ser: S,
        with_prefix: impl Fn(S, &str, &str) -> Result<S, oxrdf::IriParseError>,
    ) -> IoResult<S> {
        self.0
            .entry("xsd".to_string())
            .or_insert_with(|| XSD_NS.to_string());
        for (name, iri) in &self.0 {
            ser = with_prefix(ser, name, iri).map_err(|e| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!("prefix {name}: <{iri}> is not a valid IRI: {e}"),
                )
            })?;
        }
        Ok(ser)
    }
}

const XSD_NS: &str = "http://www.w3.org/2001/XMLSchema#";
const XSD_STRING: &str = "http://www.w3.org/2001/XMLSchema#string";

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
    write_ntriples_counted(value, sv, schema, conv, w, options, &mut 0)
}

/// [`write_ntriples`] with a caller-supplied blank node counter.
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

/// Check if the local name part of a CURIE is valid for Turtle syntax.
/// Characters like `#` and `/` are not allowed in Turtle local names.
fn is_valid_turtle_local_name(curie: &str) -> bool {
    let Some((_prefix, local)) = curie.split_once(':') else {
        return false;
    };
    !local.contains('#') && !local.contains('/')
}

/// Write `value` as N-Quads, each partition into the graph `options` routes
/// it to (the default graph included).
///
/// The instance is walked once, so blank node labels agree across graphs.
/// An empty `partition_graphs` is an `InvalidInput` error, and so is a
/// `designates_type` slot mapped to `rdf:type`, as in the flat writers.
pub fn write_nquads<W: Write>(
    value: &LinkMLInstance,
    sv: &SchemaView,
    schema: &SchemaDefinition,
    conv: &Converter,
    w: &mut W,
    options: &WriteOptions,
) -> IoResult<()> {
    write_nquads_counted(value, sv, schema, conv, w, options, &mut 0)
}

/// [`write_nquads`] with a caller-supplied blank node counter.
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

/// Write `value` as TriG, each partition into the graph `options` routes it
/// to (the default graph included).
///
/// Same content as [`write_nquads`], with the same `@prefix` header as
/// [`write_turtle`]. Errors as [`write_nquads`] does.
pub fn write_trig<W: Write>(
    value: &LinkMLInstance,
    sv: &SchemaView,
    schema: &SchemaDefinition,
    conv: &Converter,
    w: &mut W,
    options: &WriteOptions,
) -> IoResult<()> {
    write_trig_counted(value, sv, schema, conv, w, options, &mut 0)
}

/// [`write_trig`] with a caller-supplied blank node counter.
pub(crate) fn write_trig_counted<W: Write>(
    value: &LinkMLInstance,
    sv: &SchemaView,
    schema: &SchemaDefinition,
    conv: &Converter,
    w: &mut W,
    options: &WriteOptions,
    counter: &mut usize,
) -> IoResult<()> {
    options.validate("TriG", false)?;
    let mut quads: Vec<Quad> = Vec::new();
    let mut sink = QuadSink {
        graphs: &options.partition_graphs,
        write: |quad: QuadRef<'_>| {
            quads.push(quad.into_owned());
            Ok(())
        },
    };
    walk(value, sv, schema, conv, &mut sink, options.skolem, counter)?;
    let mut prefixes = Prefixes::default();
    for quad in &quads {
        prefixes.note_quad(quad, conv);
    }
    let mut ser = prefixes
        .apply(TriGSerializer::new(), |ser, name, iri| {
            ser.with_prefix(name, iri)
        })?
        .for_writer(&mut *w);
    for quad in &quads {
        ser.serialize_quad(quad.as_ref())?;
    }
    ser.finish()?;
    Ok(())
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

    fn used(iris: &[&str], conv: &Converter) -> BTreeMap<String, String> {
        let mut p = Prefixes::default();
        for iri in iris {
            p.note_iri(iri, conv);
        }
        p.0
    }

    #[test]
    fn prefixes_skip_a_local_name_starting_with_hash() {
        // RSM prefix maps to a URI without trailing # or /, so the IRI would
        // compress to RSM:#EAID_xxx, which is not a Turtle prefixed name.
        let conv = make_converter(&[("RSM", "http://rsm.uic.org/RSM12")]);
        assert!(used(&["http://rsm.uic.org/RSM12#EAID_55DDBCD9"], &conv).is_empty());
    }

    #[test]
    fn prefixes_skip_a_local_name_containing_slash() {
        let conv = make_converter(&[("asset360", "https://data.example.com/asset360/")]);
        assert!(used(&["https://data.example.com/asset360/unit/Meter"], &conv).is_empty());
    }

    #[test]
    fn prefixes_record_valid_curies_and_ignore_rdf_type() {
        let conv = make_converter(&[("ex", "http://example.com/")]);
        let got = used(
            &[
                "http://example.com/Thing",
                "http://example.com/Class",
                RDF_TYPE,
            ],
            &conv,
        );
        assert_eq!(got.len(), 1);
        assert_eq!(got["ex"], "http://example.com/");
    }

    #[test]
    fn is_valid_turtle_local_name_rejects_hash() {
        assert!(!is_valid_turtle_local_name("RSM:#EAID_xxx"));
    }

    #[test]
    fn is_valid_turtle_local_name_rejects_slash() {
        assert!(!is_valid_turtle_local_name("asset360:unit/Meter"));
    }

    #[test]
    fn is_valid_turtle_local_name_accepts_valid() {
        assert!(is_valid_turtle_local_name("ex:Thing"));
        assert!(is_valid_turtle_local_name("schema:Person"));
    }
}
