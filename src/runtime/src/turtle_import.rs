//! RDF/Turtle import: reconstruct LinkML instances from flat RDF triples.
//!
//! This is the inverse of the [`turtle`](super::turtle) exporter. Given a Turtle
//! file and a schema, it parses the triples into an in-memory `oxrdf::Graph`,
//! then "harvests" typed instances by walking the graph guided by the schema's
//! class and slot definitions.

use std::collections::{HashMap, HashSet};
use std::io::Read;

use oxrdf::{
    vocab::{rdf, xsd},
    Graph, Literal, NamedNode, NamedOrBlankNode, TermRef,
};
use oxttl::{NTriplesParser, TurtleParser};
use serde_json::Value as JsonValue;

use linkml_schemaview::identifier::{Identifier, IdentifierError};
use linkml_schemaview::schemaview::{
    ClassView, SchemaView, SchemaViewError, SlotContainerMode, SlotInlineMode,
};
use linkml_schemaview::Converter;

use crate::{new_node_id, LinkMLInstance};

// ── Error types ──────────────────────────────────────────────────────────────

/// Errors that can occur during RDF/Turtle import.
#[derive(Debug)]
pub enum ImportError {
    /// I/O or parse error from oxttl
    Parse(String),
    /// A subject's rdf:type is not defined in the schema
    UnknownType { subject: String, type_iri: String },
    /// A cycle was detected when recursively inlining objects
    InlinedCycle { subject: String },
    /// A root class name was not found in the schema
    UnknownClass(String),
    /// A required rdf:type triple is missing for a subject
    MissingType(String),
    /// Schema lookup failed
    SchemaError(SchemaViewError),
    /// Literal value could not be converted
    LiteralConversion {
        value: String,
        expected_type: String,
    },
}

impl std::fmt::Display for ImportError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ImportError::Parse(msg) => write!(f, "Turtle parse error: {msg}"),
            ImportError::UnknownType { subject, type_iri } => {
                write!(f, "Unknown rdf:type <{type_iri}> for subject <{subject}>")
            }
            ImportError::InlinedCycle { subject } => {
                write!(f, "Cycle detected when inlining subject <{subject}>")
            }
            ImportError::UnknownClass(name) => write!(f, "Root class not found: {name}"),
            ImportError::MissingType(subject) => {
                write!(f, "No rdf:type triple for subject <{subject}>")
            }
            ImportError::SchemaError(e) => write!(f, "Schema error: {e}"),
            ImportError::LiteralConversion {
                value,
                expected_type,
            } => write!(f, "Cannot convert \"{value}\" to {expected_type}"),
        }
    }
}

impl std::error::Error for ImportError {}

impl From<SchemaViewError> for ImportError {
    fn from(e: SchemaViewError) -> Self {
        ImportError::SchemaError(e)
    }
}

impl From<IdentifierError> for ImportError {
    fn from(e: IdentifierError) -> Self {
        ImportError::Parse(format!("Identifier error: {e:?}"))
    }
}

/// Result of a successful import.
pub struct ImportResult {
    /// Instances grouped by class name.
    pub instances: HashMap<String, Vec<LinkMLInstance>>,
    /// Number of triples in the graph that were not consumed during harvesting.
    pub unconsumed_count: usize,
}

// ── Parsing ──────────────────────────────────────────────────────────────────

/// Parse a Turtle document into an `oxrdf::Graph`.
fn parse_turtle(reader: impl Read) -> Result<Graph, ImportError> {
    let mut graph = Graph::new();
    let parser = TurtleParser::new().for_reader(reader);
    for result in parser {
        let triple = result.map_err(|e| ImportError::Parse(e.to_string()))?;
        graph.insert(&triple);
    }
    Ok(graph)
}

/// Parse an N-Triples document into an `oxrdf::Graph`.
fn parse_ntriples(reader: impl Read) -> Result<Graph, ImportError> {
    let mut graph = Graph::new();
    let parser = NTriplesParser::new().for_reader(reader);
    for result in parser {
        let triple = result.map_err(|e| ImportError::Parse(e.to_string()))?;
        graph.insert(&triple);
    }
    Ok(graph)
}

/// Supported RDF serialization formats for import.
pub enum RdfFormat {
    Turtle,
    NTriples,
}

// ── Literal conversion ──────────────────────────────────────────────────────

/// Convert an RDF literal to a JSON value based on its datatype.
fn literal_to_json(literal: &Literal) -> Result<JsonValue, ImportError> {
    let value_str = literal.value();

    // Language-tagged literals are always strings
    if literal.language().is_some() {
        return Ok(JsonValue::String(value_str.to_string()));
    }

    let dt = literal.datatype();

    if dt == xsd::BOOLEAN {
        return match value_str {
            "true" | "1" => Ok(JsonValue::Bool(true)),
            "false" | "0" => Ok(JsonValue::Bool(false)),
            _ => Err(ImportError::LiteralConversion {
                value: value_str.to_string(),
                expected_type: "boolean".to_string(),
            }),
        };
    }

    if dt == xsd::INTEGER
        || dt == xsd::INT
        || dt == xsd::LONG
        || dt == xsd::SHORT
        || dt == xsd::BYTE
        || dt == xsd::NON_NEGATIVE_INTEGER
        || dt == xsd::POSITIVE_INTEGER
        || dt == xsd::NON_POSITIVE_INTEGER
        || dt == xsd::NEGATIVE_INTEGER
        || dt == xsd::UNSIGNED_INT
        || dt == xsd::UNSIGNED_LONG
        || dt == xsd::UNSIGNED_SHORT
        || dt == xsd::UNSIGNED_BYTE
    {
        let n: i64 = value_str
            .parse()
            .map_err(|_| ImportError::LiteralConversion {
                value: value_str.to_string(),
                expected_type: "integer".to_string(),
            })?;
        return Ok(JsonValue::Number(n.into()));
    }

    if dt == xsd::FLOAT || dt == xsd::DOUBLE || dt == xsd::DECIMAL {
        let n: f64 = value_str
            .parse()
            .map_err(|_| ImportError::LiteralConversion {
                value: value_str.to_string(),
                expected_type: "float".to_string(),
            })?;
        return serde_json::Number::from_f64(n)
            .map(JsonValue::Number)
            .ok_or_else(|| ImportError::LiteralConversion {
                value: value_str.to_string(),
                expected_type: "float (non-finite)".to_string(),
            });
    }

    // Everything else (xsd:string, xsd:date, xsd:dateTime, unknown types) → string
    Ok(JsonValue::String(value_str.to_string()))
}

// ── Enum reverse resolution ─────────────────────────────────────────────────

/// Given an IRI used as the object of an enum-ranged slot, find the
/// permissible value name whose `meaning` matches.
fn resolve_enum_value(
    iri: &str,
    slot: &linkml_schemaview::slotview::SlotView,
    conv: &Converter,
) -> Option<String> {
    let enum_view = slot.get_range_enum()?;
    let pv_map = enum_view.definition().permissible_values.as_ref()?;
    for (name, pv) in pv_map {
        if let Some(meaning) = &pv.meaning {
            let meaning_iri = Identifier::new(meaning)
                .to_uri(conv)
                .map(|u| u.0)
                .unwrap_or_else(|_| meaning.clone());
            if meaning_iri == iri {
                return Some(name.clone());
            }
        }
    }
    None
}

// ── Schema helpers ──────────────────────────────────────────────────────────

/// Resolve the ClassView for a subject by looking at its rdf:type triple(s).
/// Picks the most specific type defined in the schema.
fn resolve_class(
    graph: &Graph,
    sv: &SchemaView,
    _conv: &Converter,
    subject: &NamedOrBlankNode,
) -> Result<ClassView, ImportError> {
    let type_node = rdf::TYPE.into_owned();
    let types: Vec<_> = graph
        .objects_for_subject_predicate(subject, &type_node)
        .collect();

    if types.is_empty() {
        return Err(ImportError::MissingType(subject.to_string()));
    }

    let mut candidates: Vec<ClassView> = Vec::new();
    for term in types {
        if let TermRef::NamedNode(nn) = term {
            let iri = nn.as_str();
            // Try both expanded IRI and as-is (could be CURIE)
            if let Ok(Some(cv)) = sv.get_class_by_uri(iri) {
                candidates.push(cv);
            }
        }
    }

    if candidates.is_empty() {
        // Report the first type IRI for the error
        let first_type = graph
            .objects_for_subject_predicate(subject, &type_node)
            .next()
            .map(|t| t.to_string())
            .unwrap_or_default();
        return Err(ImportError::UnknownType {
            subject: subject.to_string(),
            type_iri: first_type,
        });
    }

    if candidates.len() == 1 {
        return Ok(candidates
            .into_iter()
            .next()
            .unwrap_or_else(|| unreachable!()));
    }

    // Multiple types: pick the most specific (deepest in is_a hierarchy)
    // The one with the most ancestors is the most specific
    let mut best = candidates[0].clone();
    let mut best_depth = ancestor_depth(&best);
    for cv in &candidates[1..] {
        let depth = ancestor_depth(cv);
        if depth > best_depth {
            best = cv.clone();
            best_depth = depth;
        }
    }
    Ok(best)
}

/// Count the depth of the is_a chain for a class (more ancestors = more specific).
fn ancestor_depth(cv: &ClassView) -> usize {
    let mut depth = 0;
    let mut current = cv.clone();
    while let Ok(Some(parent)) = current.parent_class() {
        depth += 1;
        current = parent;
    }
    depth
}

// ── Harvest context & main algorithm ────────────────────────────────────────

/// State carried through recursive instance assembly.
struct HarvestContext<'a> {
    graph: &'a Graph,
    sv: &'a SchemaView,
    conv: &'a Converter,
    /// Subjects currently being assembled (for cycle detection on inlined paths).
    visit_stack: HashSet<String>,
    /// Subjects that have been inlined into a parent (should not appear as top-level).
    claimed: HashSet<String>,
    /// Count of triples consumed during harvesting.
    consumed_count: usize,
}

impl<'a> HarvestContext<'a> {
    fn new(graph: &'a Graph, sv: &'a SchemaView, conv: &'a Converter) -> Self {
        Self {
            graph,
            sv,
            conv,
            visit_stack: HashSet::new(),
            claimed: HashSet::new(),
            consumed_count: 0,
        }
    }
}

/// Assemble a single subject into a `LinkMLInstance::Object`.
fn harvest_subject(
    ctx: &mut HarvestContext<'_>,
    subject: &NamedOrBlankNode,
    class: &ClassView,
) -> Result<LinkMLInstance, ImportError> {
    let subject_key = subject.to_string();

    // Cycle detection
    if ctx.visit_stack.contains(&subject_key) {
        return Err(ImportError::InlinedCycle {
            subject: subject_key,
        });
    }
    ctx.visit_stack.insert(subject_key.clone());

    // Count rdf:type triple as consumed
    ctx.consumed_count += 1;

    let mut values: HashMap<String, LinkMLInstance> = HashMap::new();
    let mut consumed_predicates: HashSet<String> = HashSet::new();

    // Always mark rdf:type as consumed
    consumed_predicates.insert(rdf::TYPE.as_str().to_string());

    // Populate identifier slot from subject IRI (if class has one and subject is a named node)
    let id_slot_name = match (class.identifier_slot(), subject) {
        (Some(id_slot), NamedOrBlankNode::NamedNode(nn)) => {
            values.insert(
                id_slot.name.clone(),
                LinkMLInstance::Scalar {
                    node_id: new_node_id(),
                    value: JsonValue::String(nn.as_str().to_string()),
                    slot: id_slot.clone(),
                    class: Some(class.clone()),
                    sv: ctx.sv.clone(),
                },
            );
            Some(id_slot.name.clone())
        }
        _ => None,
    };

    // Process each slot defined on the class
    for slot in class.slots() {
        // Skip identifier slot (already handled above)
        if id_slot_name.as_deref() == Some(&slot.name) {
            continue;
        }

        // Skip designates_type slots during harvesting — they'll be
        // populated by LinkMLInstance::populate_type_designator() below.
        if slot.definition().designates_type.unwrap_or(false) {
            continue;
        }

        // Get the predicate IRI for this slot
        let canonical = slot.canonical_uri();
        let pred_iri = canonical
            .to_uri(ctx.conv)
            .map(|u| u.0)
            .unwrap_or_else(|_| canonical.to_string());

        consumed_predicates.insert(pred_iri.clone());

        let predicate = NamedNode::new_unchecked(&pred_iri);
        let objects: Vec<_> = ctx
            .graph
            .objects_for_subject_predicate(subject, &predicate)
            .collect();

        if objects.is_empty() {
            continue;
        }

        ctx.consumed_count += objects.len();

        let range_infos = slot.get_range_info();
        let range_info = range_infos.first();

        let inline_mode = slot.determine_slot_inline_mode();
        let container_mode = slot.determine_slot_container_mode();

        // Check for enum range
        let has_enum = range_info.is_some_and(|ri| ri.range_enum.is_some());
        let is_range_iri = range_info.is_some_and(|ri| ri.is_range_iri);

        // Convert each object term to a LinkMLInstance
        let mut items: Vec<LinkMLInstance> = Vec::new();
        for obj_term in &objects {
            match *obj_term {
                TermRef::Literal(lit) => {
                    let lit = lit.into_owned();
                    // Check for language-tagged literal on an inlined slot
                    // whose range class has lang_tag_slots
                    if lit.language().is_some() && inline_mode == SlotInlineMode::Inline {
                        if let Some(range_class) = range_info.and_then(|ri| ri.range_class.as_ref())
                        {
                            if let Some((lang_slot, value_slot)) = range_class.lang_tag_slots() {
                                let lang = lit.language().unwrap_or_default();
                                let val = lit.value();
                                let mut lang_values = HashMap::new();
                                lang_values.insert(
                                    lang_slot.name.clone(),
                                    LinkMLInstance::Scalar {
                                        node_id: new_node_id(),
                                        value: JsonValue::String(lang.to_string()),
                                        slot: lang_slot.clone(),
                                        class: Some(range_class.clone()),
                                        sv: ctx.sv.clone(),
                                    },
                                );
                                lang_values.insert(
                                    value_slot.name.clone(),
                                    LinkMLInstance::Scalar {
                                        node_id: new_node_id(),
                                        value: JsonValue::String(val.to_string()),
                                        slot: value_slot.clone(),
                                        class: Some(range_class.clone()),
                                        sv: ctx.sv.clone(),
                                    },
                                );
                                items.push(LinkMLInstance::Object {
                                    node_id: new_node_id(),
                                    values: lang_values,
                                    class: range_class.clone(),
                                    sv: ctx.sv.clone(),
                                    unknown_fields: HashMap::new(),
                                });
                                continue;
                            }
                        }
                    }

                    // Regular literal → scalar
                    let json_val = literal_to_json(&lit)?;
                    items.push(LinkMLInstance::Scalar {
                        node_id: new_node_id(),
                        value: json_val,
                        slot: slot.clone(),
                        class: Some(class.clone()),
                        sv: ctx.sv.clone(),
                    });
                }
                TermRef::NamedNode(nn) => {
                    let iri_str = nn.as_str();

                    // Enum resolution
                    if has_enum {
                        if let Some(pv_name) = resolve_enum_value(iri_str, slot, ctx.conv) {
                            items.push(LinkMLInstance::Scalar {
                                node_id: new_node_id(),
                                value: JsonValue::String(pv_name),
                                slot: slot.clone(),
                                class: Some(class.clone()),
                                sv: ctx.sv.clone(),
                            });
                            continue;
                        }
                    }

                    // Reference or IRI-typed slot — keep full IRI
                    if inline_mode == SlotInlineMode::Reference || is_range_iri {
                        items.push(LinkMLInstance::Scalar {
                            node_id: new_node_id(),
                            value: JsonValue::String(iri_str.to_string()),
                            slot: slot.clone(),
                            class: Some(class.clone()),
                            sv: ctx.sv.clone(),
                        });
                    } else if inline_mode == SlotInlineMode::Inline {
                        // Inline: recursively harvest
                        let obj_subject = NamedOrBlankNode::NamedNode(nn.into_owned());
                        let obj_class = resolve_class(ctx.graph, ctx.sv, ctx.conv, &obj_subject)?;
                        let child = harvest_subject(ctx, &obj_subject, &obj_class)?;
                        ctx.claimed.insert(obj_subject.to_string());
                        items.push(child);
                    } else {
                        // Primitive NamedNode — store as string
                        items.push(LinkMLInstance::Scalar {
                            node_id: new_node_id(),
                            value: JsonValue::String(iri_str.to_string()),
                            slot: slot.clone(),
                            class: Some(class.clone()),
                            sv: ctx.sv.clone(),
                        });
                    }
                }
                TermRef::BlankNode(bn) => {
                    // Blank nodes are always inlined
                    let obj_subject = NamedOrBlankNode::BlankNode(bn.into_owned());
                    let obj_class = resolve_class(ctx.graph, ctx.sv, ctx.conv, &obj_subject)?;
                    let child = harvest_subject(ctx, &obj_subject, &obj_class)?;
                    ctx.claimed.insert(obj_subject.to_string());
                    items.push(child);
                }
                #[allow(unreachable_patterns)]
                _ => {
                    // Future oxrdf types (e.g. Triple term in RDF 1.2) — skip
                }
            }
        }

        if items.is_empty() {
            continue;
        }

        // Wrap items according to container mode
        let instance = match container_mode {
            SlotContainerMode::SingleValue => {
                items
                    .into_iter()
                    .next()
                    .unwrap_or_else(|| LinkMLInstance::Null {
                        node_id: new_node_id(),
                        slot: slot.clone(),
                        class: Some(class.clone()),
                        sv: ctx.sv.clone(),
                    })
            }
            SlotContainerMode::List => LinkMLInstance::List {
                node_id: new_node_id(),
                values: items,
                slot: slot.clone(),
                class: Some(class.clone()),
                sv: ctx.sv.clone(),
            },
            SlotContainerMode::Mapping => {
                let mut map = HashMap::new();
                for item in items {
                    // Use the key/identifier slot value as the map key
                    let key = extract_mapping_key(&item);
                    map.insert(key, item);
                }
                LinkMLInstance::Mapping {
                    node_id: new_node_id(),
                    values: map,
                    slot: slot.clone(),
                    class: Some(class.clone()),
                    sv: ctx.sv.clone(),
                }
            }
        };

        values.insert(slot.name.clone(), instance);
    }

    // Collect unknown fields (predicates on subject not matching any slot)
    let mut unknown_fields: HashMap<String, JsonValue> = HashMap::new();
    for triple in ctx.graph.triples_for_subject(subject) {
        let pred = triple.predicate;
        let obj = triple.object;
        if !consumed_predicates.contains(pred.as_str()) {
            let key = pred.as_str().to_string();
            let val = match obj {
                TermRef::Literal(lit) => JsonValue::String(lit.value().to_string()),
                TermRef::NamedNode(nn) => JsonValue::String(nn.as_str().to_string()),
                _ => JsonValue::String(obj.to_string()),
            };
            unknown_fields.insert(key, val);
        }
    }

    ctx.visit_stack.remove(&subject_key);

    // Populate designates_type slot (e.g. typeURI) from the class, same as
    // the JSON/YAML parser does.
    LinkMLInstance::populate_type_designator(&mut values, class, ctx.sv, ctx.conv);

    Ok(LinkMLInstance::Object {
        node_id: new_node_id(),
        values,
        class: class.clone(),
        sv: ctx.sv.clone(),
        unknown_fields,
    })
}

/// Extract a mapping key from a harvested instance (identifier or key slot value).
fn extract_mapping_key(instance: &LinkMLInstance) -> String {
    if let LinkMLInstance::Object { values, class, .. } = instance {
        // Try identifier slot first, then key slot
        if let Some(id_slot) = class.identifier_slot() {
            if let Some(LinkMLInstance::Scalar { value, .. }) = values.get(&id_slot.name) {
                return match value {
                    JsonValue::String(s) => s.clone(),
                    other => other.to_string(),
                };
            }
        }
        if let Some(key_slot) = class.key_or_identifier_slot() {
            if let Some(LinkMLInstance::Scalar { value, .. }) = values.get(&key_slot.name) {
                return match value {
                    JsonValue::String(s) => s.clone(),
                    other => other.to_string(),
                };
            }
        }
    }
    // Fallback: use auto-generated key
    format!("_key_{}", new_node_id())
}

// ── Public API ──────────────────────────────────────────────────────────────

/// Harvest instances from a pre-built graph.
fn import_from_graph(
    graph: Graph,
    sv: &SchemaView,
    conv: &Converter,
    root_classes: &[&str],
) -> Result<ImportResult, ImportError> {
    let total_triples = graph.len();

    // Resolve root classes (names, CURIEs, or full URIs) to ClassViews and their URIs
    let mut root_class_info: Vec<(ClassView, NamedNode)> = Vec::new();
    for &name in root_classes {
        let cv = sv
            .get_class(&Identifier::new(name), conv)
            .map_err(ImportError::SchemaError)?
            .ok_or_else(|| ImportError::UnknownClass(name.to_string()))?;
        let class_uri = cv.get_uri(conv, false, true)?;
        let class_uri_node = NamedNode::new_unchecked(class_uri.to_string());
        root_class_info.push((cv, class_uri_node));
    }

    let mut ctx = HarvestContext::new(&graph, sv, conv);

    // Collect all (subject, class) pairs for root classes
    let rdf_type_node = rdf::TYPE.into_owned();
    let mut candidates: Vec<(NamedOrBlankNode, ClassView)> = Vec::new();
    for (cv, class_uri_node) in &root_class_info {
        for subject in graph.subjects_for_predicate_object(&rdf_type_node, class_uri_node) {
            candidates.push((subject.into_owned(), cv.clone()));
        }
    }

    // Harvest all candidates
    let mut harvested: Vec<(String, NamedOrBlankNode, LinkMLInstance)> = Vec::new();
    for (subject, cv) in &candidates {
        let instance = harvest_subject(&mut ctx, subject, cv)?;
        harvested.push((cv.name().to_string(), subject.clone(), instance));
    }

    // Filter: only keep instances whose subject is not claimed (not inlined somewhere)
    let mut instances: HashMap<String, Vec<LinkMLInstance>> = HashMap::new();
    for (class_name, subject, instance) in harvested {
        let subject_key = subject.to_string();
        if !ctx.claimed.contains(&subject_key) {
            instances.entry(class_name).or_default().push(instance);
        }
    }

    let unconsumed_count = total_triples.saturating_sub(ctx.consumed_count);

    Ok(ImportResult {
        instances,
        unconsumed_count,
    })
}

/// Import RDF/Turtle data into LinkML instances.
///
/// Parses the Turtle from `reader`, then harvests instances of the specified
/// `root_classes` from the graph. Each root class can be specified as a plain
/// name (`"OperationalPoint"`), CURIE (`"rinf:OperationalPoint"`), or full
/// URI (`"http://data.europa.eu/949/OperationalPoint"`).
///
/// Subjects reachable as inlined sub-objects from another root instance are
/// inlined there, not emitted as top-level.
pub fn import_turtle(
    reader: impl Read,
    sv: &SchemaView,
    conv: &Converter,
    root_classes: &[&str],
) -> Result<ImportResult, ImportError> {
    let graph = parse_turtle(reader)?;
    import_from_graph(graph, sv, conv, root_classes)
}

/// Import RDF/N-Triples data into LinkML instances.
pub fn import_ntriples(
    reader: impl Read,
    sv: &SchemaView,
    conv: &Converter,
    root_classes: &[&str],
) -> Result<ImportResult, ImportError> {
    let graph = parse_ntriples(reader)?;
    import_from_graph(graph, sv, conv, root_classes)
}

/// Import RDF data in the specified format into LinkML instances.
pub fn import_rdf(
    reader: impl Read,
    format: RdfFormat,
    sv: &SchemaView,
    conv: &Converter,
    root_classes: &[&str],
) -> Result<ImportResult, ImportError> {
    let graph = match format {
        RdfFormat::Turtle => parse_turtle(reader)?,
        RdfFormat::NTriples => parse_ntriples(reader)?,
    };
    import_from_graph(graph, sv, conv, root_classes)
}

/// Convenience: import from a Turtle string.
pub fn import_turtle_from_string(
    ttl: &str,
    sv: &SchemaView,
    conv: &Converter,
    root_classes: &[&str],
) -> Result<ImportResult, ImportError> {
    import_turtle(std::io::Cursor::new(ttl.as_bytes()), sv, conv, root_classes)
}

// ── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use oxrdf::Term;

    #[test]
    fn test_parse_turtle_basic() {
        let ttl = r#"
            @prefix ex: <http://example.org/> .
            @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
            ex:person1 rdf:type ex:Person ;
                       ex:name "Alice" .
            ex:org1 rdf:type ex:Organization ;
                    ex:name "ACME" .
        "#;

        let graph = parse_turtle(std::io::Cursor::new(ttl.as_bytes())).unwrap();

        // Should have 4 triples (2 rdf:type + 2 ex:name)
        assert_eq!(graph.len(), 4);

        // Check subjects_for_predicate_object for Person
        let rdf_type = rdf::TYPE.into_owned();
        let person_type = NamedNode::new_unchecked("http://example.org/Person");
        let person_subjects: Vec<_> = graph
            .subjects_for_predicate_object(&rdf_type, &person_type)
            .collect();
        assert_eq!(person_subjects.len(), 1);

        // Check objects_for_subject_predicate
        let person1 =
            NamedOrBlankNode::NamedNode(NamedNode::new_unchecked("http://example.org/person1"));
        let name_pred = NamedNode::new_unchecked("http://example.org/name");
        let names: Vec<_> = graph
            .objects_for_subject_predicate(&person1, &name_pred)
            .collect();
        assert_eq!(names.len(), 1);
        let expected = Term::Literal(Literal::new_simple_literal("Alice"));
        let expected_ref: TermRef<'_> = expected.as_ref();
        assert_eq!(names[0], expected_ref);
    }

    #[test]
    fn test_parse_turtle_blank_nodes() {
        let ttl = r#"
            @prefix ex: <http://example.org/> .
            ex:person1 a ex:Person ;
                       ex:address [ a ex:Address ; ex:street "123 Main St" ] .
        "#;

        let graph = parse_turtle(std::io::Cursor::new(ttl.as_bytes())).unwrap();
        // person1 has: rdf:type, ex:address
        // blank node has: rdf:type, ex:street
        // plus the link from person1 to blank node
        assert!(graph.len() >= 4);
    }

    #[test]
    fn test_literal_to_json_string() {
        let lit = Literal::new_simple_literal("hello");
        assert_eq!(
            literal_to_json(&lit).unwrap(),
            JsonValue::String("hello".to_string())
        );
    }

    #[test]
    fn test_literal_to_json_integer() {
        let lit = Literal::new_typed_literal("42", xsd::INTEGER);
        assert_eq!(literal_to_json(&lit).unwrap(), JsonValue::Number(42.into()));
    }

    #[test]
    fn test_literal_to_json_float() {
        let lit = Literal::new_typed_literal("1.5", xsd::DOUBLE);
        let val = literal_to_json(&lit).unwrap();
        assert!(val.is_number());
        let n = val.as_f64().unwrap();
        assert!((n - 1.5_f64).abs() < 0.001);
    }

    #[test]
    fn test_literal_to_json_boolean() {
        let lit_true = Literal::new_typed_literal("true", xsd::BOOLEAN);
        assert_eq!(literal_to_json(&lit_true).unwrap(), JsonValue::Bool(true));

        let lit_false = Literal::new_typed_literal("false", xsd::BOOLEAN);
        assert_eq!(literal_to_json(&lit_false).unwrap(), JsonValue::Bool(false));
    }

    #[test]
    fn test_literal_to_json_date() {
        let lit = Literal::new_typed_literal("2024-01-15", xsd::DATE);
        assert_eq!(
            literal_to_json(&lit).unwrap(),
            JsonValue::String("2024-01-15".to_string())
        );
    }

    #[test]
    fn test_literal_to_json_language_tagged() {
        let lit = Literal::new_language_tagged_literal("bonjour", "fr").unwrap();
        assert_eq!(
            literal_to_json(&lit).unwrap(),
            JsonValue::String("bonjour".to_string())
        );
    }
}
