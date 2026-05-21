//! RDF/Turtle import: reconstruct LinkML instances from flat RDF triples.
//!
//! This is the inverse of the [`turtle`](super::turtle) exporter. Given a Turtle
//! file and a schema, it parses the triples into an in-memory `oxrdf::Graph`,
//! then "harvests" typed instances by walking the graph guided by the schema's
//! class and slot definitions.

use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;

use oxrdf::{
    vocab::{rdf, xsd},
    Literal, NamedOrBlankNode, Term, TermRef,
};
use serde_json::Value as JsonValue;

use linkml_schemaview::identifier::{Identifier, IdentifierError};
use linkml_schemaview::schemaview::{
    ClassView, SchemaView, SchemaViewError, SlotContainerMode, SlotInlineMode,
};
use linkml_schemaview::Converter;

use crate::triple_source::TripleSource;
use crate::{new_node_id, LinkMLInstance, ValidationProblemType, ValidationResult};

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
    /// In strict mode, a harvest warning is promoted to an error.
    ValidationFailure(ValidationResult),
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
            ImportError::ValidationFailure(vr) => {
                write!(f, "Validation failure in strict mode: {}", vr.detail)
            }
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
pub(crate) fn resolve_class<T: TripleSource>(
    store: &T,
    sv: &SchemaView,
    _conv: &Converter,
    subject: &NamedOrBlankNode,
) -> Result<ClassView, ImportError> {
    let type_node = rdf::TYPE.into_owned();
    let types: Vec<_> = store
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
        let first_type = store
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
pub struct HarvestContext<'a, T: TripleSource> {
    pub(crate) store: &'a T,
    pub(crate) sv: &'a SchemaView,
    pub(crate) conv: &'a Converter,
    /// Subjects currently being assembled (for cycle detection on inlined paths).
    pub(crate) visit_stack: HashSet<String>,
    /// Subjects that have been inlined into a parent (should not appear as top-level).
    pub(crate) claimed: HashSet<String>,
    /// Count of triples consumed during harvesting. Kept for legacy callers
    /// that read it; the new public API (`RdfStream`) does not expose it.
    pub(crate) consumed_count: usize,
    /// Schema/data mismatches discovered during harvest. Drained by
    /// `RdfStream::pop_warnings`. Shared with the parent `RdfStream`.
    pub(crate) warnings: Rc<RefCell<Vec<ValidationResult>>>,
    /// If true, `emit_warning` returns `Err(ImportError::ValidationFailure)`
    /// instead of pushing to the buffer.
    pub(crate) strict: bool,
}

impl<'a, T: TripleSource> HarvestContext<'a, T> {
    pub fn new(store: &'a T, sv: &'a SchemaView, conv: &'a Converter) -> Self {
        Self {
            store,
            sv,
            conv,
            visit_stack: HashSet::new(),
            claimed: HashSet::new(),
            consumed_count: 0,
            warnings: Rc::new(RefCell::new(Vec::new())),
            strict: false,
        }
    }

    pub fn new_with_warnings(
        store: &'a T,
        sv: &'a SchemaView,
        conv: &'a Converter,
        warnings: Rc<RefCell<Vec<ValidationResult>>>,
        strict: bool,
    ) -> Self {
        Self {
            store,
            sv,
            conv,
            visit_stack: HashSet::new(),
            claimed: HashSet::new(),
            consumed_count: 0,
            warnings,
            strict,
        }
    }

    pub fn consumed_count(&self) -> usize {
        self.consumed_count
    }

    pub fn claimed(&self) -> &HashSet<String> {
        &self.claimed
    }

    /// Push a warning into the shared buffer. In strict mode, returns an
    /// error that aborts the import; otherwise returns Ok(()).
    pub(crate) fn emit_warning(&self, vr: ValidationResult) -> Result<(), ImportError> {
        if self.strict {
            return Err(ImportError::ValidationFailure(vr));
        }
        self.warnings.borrow_mut().push(vr);
        Ok(())
    }
}

/// Assemble a single subject into a `LinkMLInstance::Object`.
///
/// Recursive inline calls (named-node Inline-mode objects, all blank-node
/// objects) are routed through `materializer.materialise(...)` so the
/// streaming path can deduplicate shared subtrees. For the legacy
/// non-streaming path, the caller passes an empty `Materializer` which
/// always builds without caching — behaviour identical to the pre-streaming
/// code.
pub fn harvest_subject<T: TripleSource>(
    ctx: &mut HarvestContext<'_, T>,
    materializer: &mut crate::rdf_streaming::Materializer,
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
    ctx.store.on_consumed(&subject_key, rdf::TYPE.as_str(), "");

    let mut values: HashMap<String, LinkMLInstance> = HashMap::new();
    let mut consumed_predicates: HashSet<String> = HashSet::new();

    // Always mark rdf:type as consumed
    consumed_predicates.insert(rdf::TYPE.as_str().to_string());

    // One scan over the subject's triples; bucket objects by predicate IRI.
    // Owned `Term`s outlive the slot loop and the unknown-fields pass so we
    // never need a second `triples_for_subject` call.
    let mut by_predicate: HashMap<String, Vec<Term>> = HashMap::new();
    for triple in ctx.store.triples_for_subject(subject) {
        let pred = triple.predicate.as_str().to_string();
        let obj_owned: Term = match triple.object {
            TermRef::Literal(l) => Term::Literal(l.into_owned()),
            TermRef::NamedNode(n) => Term::NamedNode(n.into_owned()),
            TermRef::BlankNode(b) => Term::BlankNode(b.into_owned()),
            #[allow(unreachable_patterns)]
            _ => continue,
        };
        by_predicate.entry(pred).or_default().push(obj_owned);
    }

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
            // Suppress UndeclaredSlot warning for the id slot's predicate:
            // we don't iterate it through the normal slot loop (because
            // the value comes from the subject IRI, not a separate triple),
            // but it IS a declared slot. Remove it from the by_predicate
            // bucket so the unknown-fields pass doesn't see it, and from
            // consumed_predicates for symmetry.
            let id_canonical = id_slot.canonical_uri();
            let id_pred_iri = id_canonical
                .to_uri(ctx.conv)
                .map(|u| u.0)
                .unwrap_or_else(|_| id_canonical.to_string());
            consumed_predicates.insert(id_pred_iri.clone());
            by_predicate.remove(&id_pred_iri);
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

        // Take the objects for this predicate out of the bucket so the
        // unknown-fields pass below doesn't see them.
        let owned_objects: Vec<Term> = by_predicate.remove(&pred_iri).unwrap_or_default();
        if owned_objects.is_empty() {
            continue;
        }

        for obj_term in &owned_objects {
            let obj_str = match obj_term {
                Term::Literal(l) => l.to_string(),
                Term::NamedNode(n) => n.as_str().to_string(),
                Term::BlankNode(b) => b.to_string(),
                #[allow(unreachable_patterns)]
                _ => String::new(),
            };
            ctx.store.on_consumed(&subject_key, &pred_iri, &obj_str);
        }
        ctx.consumed_count += owned_objects.len();

        let range_infos = slot.get_range_info();
        let range_info = range_infos.first();

        let inline_mode = slot.determine_slot_inline_mode();
        let container_mode = slot.determine_slot_container_mode();

        // Check for enum range
        let has_enum = range_info.is_some_and(|ri| ri.range_enum.is_some());
        let is_range_iri = range_info.is_some_and(|ri| ri.is_range_iri);

        // Convert each object term to a LinkMLInstance
        let mut items: Vec<LinkMLInstance> = Vec::new();
        for obj_term in owned_objects {
            match obj_term {
                Term::Literal(lit) => {
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

                    // Regular literal → scalar. If conversion to the
                    // declared range fails (e.g. "abc"^^xsd:string in an
                    // xsd:integer-ranged slot), fall back to the raw string
                    // and emit a SlotRangeViolation warning so the caller
                    // sees the mismatch rather than the whole import dying.
                    let json_val = match literal_to_json(&lit) {
                        Ok(v) => v,
                        Err(ImportError::LiteralConversion {
                            value,
                            expected_type,
                        }) => {
                            ctx.emit_warning(ValidationResult::warning(
                                ValidationProblemType::SlotRangeViolation,
                                vec![subject_key.clone(), slot.name.clone()],
                                format!(
                                    "literal {value:?} could not be converted to slot `{}`'s range (`{expected_type}`); kept as raw string",
                                    slot.name,
                                ),
                            ))?;
                            JsonValue::String(value)
                        }
                        Err(other) => return Err(other),
                    };
                    items.push(LinkMLInstance::Scalar {
                        node_id: new_node_id(),
                        value: json_val,
                        slot: slot.clone(),
                        class: Some(class.clone()),
                        sv: ctx.sv.clone(),
                    });
                }
                Term::NamedNode(nn) => {
                    let iri_str = nn.as_str().to_string();

                    // Enum resolution
                    if has_enum {
                        if let Some(pv_name) = resolve_enum_value(&iri_str, slot, ctx.conv) {
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
                            value: JsonValue::String(iri_str),
                            slot: slot.clone(),
                            class: Some(class.clone()),
                            sv: ctx.sv.clone(),
                        });
                    } else if inline_mode == SlotInlineMode::Inline {
                        // Inline: recursively harvest via the materializer so
                        // shared subjects can be cached.
                        let obj_subject = NamedOrBlankNode::NamedNode(nn);
                        let obj_class = resolve_class(ctx.store, ctx.sv, ctx.conv, &obj_subject)?;
                        let child = materializer.materialise(ctx, &obj_subject, &obj_class)?;
                        ctx.claimed.insert(obj_subject.to_string());
                        items.push(child);
                    } else {
                        // Primitive NamedNode — store as string
                        items.push(LinkMLInstance::Scalar {
                            node_id: new_node_id(),
                            value: JsonValue::String(iri_str),
                            slot: slot.clone(),
                            class: Some(class.clone()),
                            sv: ctx.sv.clone(),
                        });
                    }
                }
                Term::BlankNode(bn) => {
                    // Blank nodes are always inlined; route through the
                    // materializer for cache awareness.
                    let obj_subject = NamedOrBlankNode::BlankNode(bn);
                    let obj_class = resolve_class(ctx.store, ctx.sv, ctx.conv, &obj_subject)?;
                    let child = materializer.materialise(ctx, &obj_subject, &obj_class)?;
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
                if items.len() > 1 {
                    // Schema says single-valued; RDF has multiple objects.
                    // First wins (HashMap order — nondeterministic across runs);
                    // emit a warning so the caller can see they have either
                    // a schema bug or unexpectedly multi-valued source data.
                    let dropped = items.len() - 1;
                    ctx.emit_warning(ValidationResult::warning(
                        ValidationProblemType::MaxCountViolation,
                        vec![subject_key.clone(), slot.name.clone()],
                        format!(
                            "slot `{}` is single-valued but RDF has {} objects; keeping first, dropping {} others",
                            slot.name,
                            items.len(),
                            dropped,
                        ),
                    ))?;
                }
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

    // Unknown fields = predicates left in the by_predicate bucket after the
    // slot loop. consumed_predicates is redundant with the bucket-removal
    // strategy but kept for clarity.
    let mut unknown_fields: HashMap<String, JsonValue> = HashMap::new();
    for (pred_iri, objs) in by_predicate.into_iter() {
        if consumed_predicates.contains(&pred_iri) {
            continue;
        }
        // The subject's class has no slot matching this predicate. The data
        // carries information the schema doesn't model. Emit a warning per
        // distinct predicate (not per occurrence — N objects for the same
        // unknown predicate is still one "you forgot this slot" finding).
        ctx.emit_warning(ValidationResult::warning(
            ValidationProblemType::UndeclaredSlot,
            vec![subject_key.clone(), pred_iri.clone()],
            format!(
                "subject of class `{}` has predicate <{}> that is not a slot of that class; {} object(s) dropped into unknown_fields",
                class.name(),
                pred_iri,
                objs.len(),
            ),
        ))?;
        if let Some(obj) = objs.into_iter().last() {
            let val = match obj {
                Term::Literal(lit) => JsonValue::String(lit.value().to_string()),
                Term::NamedNode(nn) => JsonValue::String(nn.as_str().to_string()),
                Term::BlankNode(bn) => JsonValue::String(bn.to_string()),
                #[allow(unreachable_patterns)]
                other => JsonValue::String(other.to_string()),
            };
            unknown_fields.insert(pred_iri, val);
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
//
// The top-level RDF entry points (import_turtle / import_ntriples /
// export_turtle / export_ntriples) live in `crate::rdf_import` and
// `crate::rdf_export`. The legacy HashMap-shaped ImportResult and the
// import_from_store wrapper were removed in Phase 3 — the new RdfStream
// iterator is the single user-facing harvest surface.

// ── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rdf_import_store::RdfImportStore;
    use oxrdf::{NamedNode, Term};

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

        let store = RdfImportStore::from_turtle(std::io::Cursor::new(ttl.as_bytes())).unwrap();

        // Should have 4 triples (2 rdf:type + 2 ex:name)
        assert_eq!(store.len(), Some(4));

        // Check subjects_for_predicate_object for Person
        let rdf_type = rdf::TYPE.into_owned();
        let person_type = NamedNode::new_unchecked("http://example.org/Person");
        let person_subjects: Vec<_> = store
            .subjects_for_predicate_object(&rdf_type, &person_type)
            .collect();
        assert_eq!(person_subjects.len(), 1);

        // Check objects_for_subject_predicate
        let person1 =
            NamedOrBlankNode::NamedNode(NamedNode::new_unchecked("http://example.org/person1"));
        let name_pred = NamedNode::new_unchecked("http://example.org/name");
        let names: Vec<_> = store
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

        let store = RdfImportStore::from_turtle(std::io::Cursor::new(ttl.as_bytes())).unwrap();
        // person1 has: rdf:type, ex:address
        // blank node has: rdf:type, ex:street
        // plus the link from person1 to blank node
        assert!(store.len().unwrap() >= 4);
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
