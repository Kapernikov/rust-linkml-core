use linkml_meta::SchemaDefinition;
use linkml_schemaview::Converter;

use linkml_schemaview::identifier::Identifier;
use linkml_schemaview::schemaview::{ClassView, SchemaView};
use linkml_schemaview::slotview::{SlotInlineMode, SlotView};
use serde_json::Value as JsonValue;
use std::io::{Result as IoResult, Write};

use oxrdf::{BlankNode, Literal, NamedNode, Subject, Term, Triple};
use oxttl::turtle::WriterTurtleSerializer;
use oxttl::TurtleSerializer;
use percent_encoding::{utf8_percent_encode, NON_ALPHANUMERIC};

use crate::regex_support::Regex;
use crate::LinkMLInstance;

pub struct TurtleOptions {
    pub skolem: bool,
}

enum Node {
    Named(String),
    Blank(String),
}

impl Node {
    fn as_subject(&self) -> Subject {
        match self {
            Node::Named(iri) => Subject::NamedNode(NamedNode::new_unchecked(iri.clone())),
            Node::Blank(id) => Subject::BlankNode(BlankNode::new_unchecked(id.clone())),
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

fn encode_path_part(s: &str) -> String {
    utf8_percent_encode(s, NON_ALPHANUMERIC).to_string()
}

fn slot_predicate_iri(slot: &SlotView, conv: &Converter) -> String {
    // Get the canonical URI from the slot (respects slot_uri and originating schema's default_prefix)
    let canonical = slot.canonical_uri();
    canonical
        .to_uri(conv)
        .map(|u| u.0)
        .unwrap_or_else(|_| canonical.to_string())
}

fn literal_and_type(value: &JsonValue, slot: &SlotView) -> (String, Option<String>) {
    let lit = literal_value(value);
    let dt = match slot.definition().range.as_deref() {
        Some("date") => Some("http://www.w3.org/2001/XMLSchema#date".to_string()),
        Some("datetime") => Some("http://www.w3.org/2001/XMLSchema#dateTime".to_string()),
        Some("integer") => Some("http://www.w3.org/2001/XMLSchema#integer".to_string()),
        Some("float") => Some("http://www.w3.org/2001/XMLSchema#float".to_string()),
        Some("double") => Some("http://www.w3.org/2001/XMLSchema#double".to_string()),
        Some("boolean") => Some("http://www.w3.org/2001/XMLSchema#boolean".to_string()),
        Some("decimal") => Some("http://www.w3.org/2001/XMLSchema#decimal".to_string()),
        _ => None,
    };
    (lit, dt)
}

fn identifier_node(
    map: &std::collections::HashMap<String, LinkMLInstance>,
    class: &ClassView,
    conv: &Converter,
    state: &mut State,
    parent: Option<&Node>,
    index: Option<usize>,
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
            let part_opt = class.key_or_identifier_slot().and_then(|ks| {
                map.get(&ks.name).and_then(|v| match v {
                    LinkMLInstance::Scalar { value, .. } => {
                        if let JsonValue::String(s) = value {
                            Some(encode_path_part(s))
                        } else {
                            Some(encode_path_part(&literal_value(value)))
                        }
                    }
                    _ => None,
                })
            });
            let part = part_opt
                .or_else(|| index.map(|i| i.to_string()))
                .unwrap_or_else(|| {
                    state.counter += 1;
                    format!("gen{}", state.counter)
                });
            let node = state.child_subject(p, &part);
            return (node, None);
        }
    }
    (state.next_subject(), None)
}

#[allow(clippy::too_many_arguments)]
fn serialize_map<W: Write>(
    subject: &Node,
    map: &std::collections::HashMap<String, LinkMLInstance>,
    class: Option<&ClassView>,
    formatter: &mut WriterTurtleSerializer<W>,
    _sv: &SchemaView,
    conv: &Converter,
    state: &mut State,
    id_slot: Option<&str>,
) -> IoResult<()> {
    if let Some(cv) = class {
        if let Ok(id) = cv.get_uri(conv, false, true) {
            let id_string = id.to_string();
            let triple = Triple {
                subject: subject.as_subject(),
                predicate: NamedNode::new_unchecked(
                    "http://www.w3.org/1999/02/22-rdf-syntax-ns#type".to_string(),
                ),
                object: Term::NamedNode(NamedNode::new_unchecked(id_string)),
            };
            formatter.serialize_triple(triple.as_ref())?;
        }
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
                let inline_mode = slot.determine_slot_inline_mode();
                if inline_mode == SlotInlineMode::Reference {
                    let lit = literal_value(value);
                    let iri = Identifier::new(&lit)
                        .to_uri(conv)
                        .map(|u| u.0)
                        .unwrap_or(lit);
                    let triple = Triple {
                        subject: subject.as_subject(),
                        predicate: predicate.clone(),
                        object: Term::NamedNode(NamedNode::new_unchecked(iri)),
                    };
                    formatter.serialize_triple(triple.as_ref())?;
                } else {
                    let (lit, dt_opt) = literal_and_type(value, slot);
                    if let Some(dt) = dt_opt {
                        let object = Term::Literal(Literal::new_typed_literal(
                            lit.clone(),
                            NamedNode::new_unchecked(dt.clone()),
                        ));
                        let triple = Triple {
                            subject: subject.as_subject(),
                            predicate: predicate.clone(),
                            object,
                        };
                        formatter.serialize_triple(triple.as_ref())?;
                    } else {
                        let object = Term::Literal(Literal::new_simple_literal(lit.clone()));
                        let triple = Triple {
                            subject: subject.as_subject(),
                            predicate: predicate.clone(),
                            object,
                        };
                        formatter.serialize_triple(triple.as_ref())?;
                    }
                }
            }
            LinkMLInstance::Null { .. } => {
                // Null is treated as absent; emit nothing
            }
            LinkMLInstance::Object { values, class, .. } => {
                let class_ref = &class;
                let (obj, child_id) =
                    identifier_node(values, class_ref, conv, state, Some(subject), None);
                let triple = Triple {
                    subject: subject.as_subject(),
                    predicate: predicate.clone(),
                    object: obj.as_term(),
                };
                formatter.serialize_triple(triple.as_ref())?;
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
            LinkMLInstance::List { values, slot, .. } => {
                for (idx, item) in values.iter().enumerate() {
                    match item {
                        LinkMLInstance::Scalar { value, .. } => {
                            let inline_mode = slot.determine_slot_inline_mode();
                            if inline_mode == SlotInlineMode::Reference {
                                let lit = literal_value(value);
                                let iri = Identifier::new(&lit)
                                    .to_uri(conv)
                                    .map(|u| u.0)
                                    .unwrap_or(lit);
                                let triple = Triple {
                                    subject: subject.as_subject(),
                                    predicate: predicate.clone(),
                                    object: Term::NamedNode(NamedNode::new_unchecked(iri)),
                                };
                                formatter.serialize_triple(triple.as_ref())?;
                            } else {
                                let (lit, dt_opt) = literal_and_type(value, slot);
                                if let Some(dt) = dt_opt {
                                    let object = Term::Literal(Literal::new_typed_literal(
                                        lit.clone(),
                                        NamedNode::new_unchecked(dt.clone()),
                                    ));
                                    let triple = Triple {
                                        subject: subject.as_subject(),
                                        predicate: predicate.clone(),
                                        object,
                                    };
                                    formatter.serialize_triple(triple.as_ref())?;
                                } else {
                                    let object =
                                        Term::Literal(Literal::new_simple_literal(lit.clone()));
                                    let triple = Triple {
                                        subject: subject.as_subject(),
                                        predicate: predicate.clone(),
                                        object,
                                    };
                                    formatter.serialize_triple(triple.as_ref())?;
                                }
                            }
                        }
                        LinkMLInstance::Null { .. } => {
                            // Skip null items
                        }
                        LinkMLInstance::Object {
                            values: mv, class, ..
                        } => {
                            let class_ref = &class;
                            let (obj, child_id) = identifier_node(
                                mv,
                                class_ref,
                                conv,
                                state,
                                Some(subject),
                                Some(idx),
                            );
                            let triple = Triple {
                                subject: subject.as_subject(),
                                predicate: predicate.clone(),
                                object: obj.as_term(),
                            };
                            formatter.serialize_triple(triple.as_ref())?;
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
                        LinkMLInstance::List { .. } => {}
                        LinkMLInstance::Mapping { .. } => {}
                    }
                }
            }
            LinkMLInstance::Mapping { values, .. } => {
                for (idx, item) in values.values().enumerate() {
                    match item {
                        LinkMLInstance::Scalar { value: v, slot, .. } => {
                            let inline_mode = slot.determine_slot_inline_mode();
                            if inline_mode == SlotInlineMode::Reference {
                                let lit = literal_value(v);
                                let iri = Identifier::new(&lit)
                                    .to_uri(conv)
                                    .map(|u| u.0)
                                    .unwrap_or(lit);
                                let triple = Triple {
                                    subject: subject.as_subject(),
                                    predicate: predicate.clone(),
                                    object: Term::NamedNode(NamedNode::new_unchecked(iri)),
                                };
                                formatter.serialize_triple(triple.as_ref())?;
                            } else {
                                let (lit, dt_opt) = literal_and_type(v, slot);
                                if let Some(dt) = dt_opt {
                                    let object = Term::Literal(Literal::new_typed_literal(
                                        lit.clone(),
                                        NamedNode::new_unchecked(dt.clone()),
                                    ));
                                    let triple = Triple {
                                        subject: subject.as_subject(),
                                        predicate: predicate.clone(),
                                        object,
                                    };
                                    formatter.serialize_triple(triple.as_ref())?;
                                } else {
                                    let object =
                                        Term::Literal(Literal::new_simple_literal(lit.clone()));
                                    let triple = Triple {
                                        subject: subject.as_subject(),
                                        predicate: predicate.clone(),
                                        object,
                                    };
                                    formatter.serialize_triple(triple.as_ref())?;
                                }
                            }
                        }
                        LinkMLInstance::Null { .. } => {
                            // nothing
                        }
                        LinkMLInstance::Object {
                            values: mv, class, ..
                        } => {
                            let class_ref = class;
                            let (obj, child_id) = identifier_node(
                                mv,
                                class_ref,
                                conv,
                                state,
                                Some(subject),
                                Some(idx),
                            );
                            let triple = Triple {
                                subject: subject.as_subject(),
                                predicate: predicate.clone(),
                                object: obj.as_term(),
                            };
                            formatter.serialize_triple(triple.as_ref())?;
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
                        LinkMLInstance::List { .. } => {}
                        LinkMLInstance::Mapping { .. } => {}
                    }
                }
            }
        }
    }
    Ok(())
}

pub fn write_turtle<W: Write>(
    value: &LinkMLInstance,
    sv: &SchemaView,
    schema: &SchemaDefinition,
    conv: &Converter,
    w: &mut W,
    options: TurtleOptions,
) -> IoResult<()> {
    // Collect prefixes from ALL loaded schemas (not just the primary one) so
    // that Turtle output is valid even when classes/slots use prefixes defined
    // in imported schemas.  On conflict (same prefix name, different URI) the
    // first one wins; the loser's IRIs stay fully expanded (handled by
    // replace_iris further down).
    let mut prefix_map: std::collections::HashMap<String, String> =
        std::collections::HashMap::new();
    sv.with_schema_definitions(|schemas| {
        for schema_def in schemas.values() {
            if let Some(prefixes) = &schema_def.prefixes {
                for (pfx, pref) in prefixes {
                    prefix_map
                        .entry(pfx.clone())
                        .or_insert_with(|| pref.prefix_reference.clone());
                }
            }
        }
    });
    let mut header = String::new();
    for (pfx, uri) in &prefix_map {
        header.push_str(&format!("@prefix {}: <{}> .\n", pfx, uri));
    }
    if !prefix_map.contains_key("rdf") {
        header.push_str("@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .\n");
    }
    if !prefix_map.contains_key("xsd") {
        header.push_str("@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .\n");
    }
    header.push('\n');
    let base = schema.id.trim_end_matches('#').to_string();
    let mut state = State {
        counter: 0,
        base: if base.ends_with('/') {
            base
        } else {
            format!("{}/", base)
        },
        skolem: options.skolem,
    };
    let mut formatter = TurtleSerializer::new().for_writer(Vec::new());
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
                &mut formatter,
                sv,
                conv,
                &mut state,
                id_slot_name.as_deref(),
            )?;
        }
        LinkMLInstance::Mapping { values, .. } => {
            for (idx, item) in values.values().enumerate() {
                let subj = if options.skolem {
                    Node::Named(format!("{}root/{}", state.base, idx))
                } else {
                    state.next_subject()
                };
                match item {
                    LinkMLInstance::Object {
                        values: mv, class, ..
                    } => {
                        let class = Some(class);
                        serialize_map(
                            &subj,
                            mv,
                            class,
                            &mut formatter,
                            sv,
                            conv,
                            &mut state,
                            None,
                        )?;
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
                            formatter.serialize_triple(triple.as_ref())?;
                        } else {
                            let object = Term::Literal(Literal::new_simple_literal(lit.clone()));
                            let triple = Triple {
                                subject: subj.as_subject(),
                                predicate: NamedNode::new_unchecked(
                                    "http://www.w3.org/1999/02/22-rdf-syntax-ns#value".to_string(),
                                ),
                                object,
                            };
                            formatter.serialize_triple(triple.as_ref())?;
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
                let subj = if options.skolem {
                    Node::Named(format!("{}root/{}", state.base, idx))
                } else {
                    state.next_subject()
                };
                match item {
                    LinkMLInstance::Object {
                        values: mv, class, ..
                    } => {
                        let class = Some(class);
                        serialize_map(
                            &subj,
                            mv,
                            class,
                            &mut formatter,
                            sv,
                            conv,
                            &mut state,
                            None,
                        )?;
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
                            formatter.serialize_triple(triple.as_ref())?;
                        } else {
                            let object = Term::Literal(Literal::new_simple_literal(lit.clone()));
                            let triple = Triple {
                                subject: subj.as_subject(),
                                predicate: NamedNode::new_unchecked(
                                    "http://www.w3.org/1999/02/22-rdf-syntax-ns#value".to_string(),
                                ),
                                object,
                            };
                            formatter.serialize_triple(triple.as_ref())?;
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
    let out_buf = formatter.finish()?;
    let out = String::from_utf8(out_buf).unwrap_or_default();
    let out = replace_iris(&out, conv)?;
    w.write_all(header.as_bytes())?;
    w.write_all(out.as_bytes())?;
    Ok(())
}

pub fn turtle_to_string(
    value: &LinkMLInstance,
    sv: &SchemaView,
    schema: &SchemaDefinition,
    conv: &Converter,
    options: TurtleOptions,
) -> Result<String, std::io::Error> {
    let mut buf = Vec::new();
    write_turtle(value, sv, schema, conv, &mut buf, options)?;
    String::from_utf8(buf).map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
}

fn replace_iris(out: &str, conv: &Converter) -> IoResult<String> {
    let iri_re = Regex::new(r"<([^>]+)>")
        .map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidInput, err))?;
    let mut result = String::with_capacity(out.len());
    let mut last = 0;
    for caps in iri_re.captures_iter(out) {
        let Some(full_match) = caps.get(0) else {
            continue;
        };
        let Some(iri_match) = caps.get(1) else {
            continue;
        };
        result.push_str(&out[last..full_match.start()]);
        let replacement = if iri_match.as_str() == "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
        {
            "a".to_string()
        } else if let Ok(curie) = conv.compress(iri_match.as_str()) {
            if is_valid_turtle_local_name(&curie) {
                curie
            } else {
                full_match.as_str().to_string()
            }
        } else {
            full_match.as_str().to_string()
        };
        result.push_str(&replacement);
        last = full_match.end();
    }
    result.push_str(&out[last..]);
    Ok(result)
}

/// Check if the local name part of a CURIE is valid for Turtle syntax.
/// Characters like `#` and `/` are not allowed in Turtle local names.
fn is_valid_turtle_local_name(curie: &str) -> bool {
    let Some((_prefix, local)) = curie.split_once(':') else {
        return false;
    };
    !local.contains('#') && !local.contains('/')
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
    fn replace_iris_keeps_iri_when_local_name_starts_with_hash() {
        // RSM prefix maps to URI without trailing # or /
        // So http://rsm.uic.org/RSM12#EAID_xxx compresses to RSM:#EAID_xxx (invalid)
        let conv = make_converter(&[("RSM", "http://rsm.uic.org/RSM12")]);
        let input = r#"<http://rsm.uic.org/RSM12#EAID_55DDBCD9> a RSM:Thing ."#;
        let result = replace_iris(input, &conv).unwrap();
        // Should keep the full IRI in angle brackets, not RSM:#EAID_55DDBCD9
        assert!(
            !result.contains("RSM:#"),
            "Local name starting with # is invalid Turtle. Got: {}",
            result
        );
        assert!(
            result.contains("<http://rsm.uic.org/RSM12#EAID_55DDBCD9>"),
            "Should keep angle-bracketed IRI. Got: {}",
            result
        );
    }

    #[test]
    fn replace_iris_keeps_iri_when_local_name_contains_slash() {
        // asset360 prefix maps to URI with trailing /
        // So https://data.example.com/asset360/unit/Meter compresses to asset360:unit/Meter (invalid)
        let conv = make_converter(&[("asset360", "https://data.example.com/asset360/")]);
        let input = r#"<https://data.example.com/asset360/unit/Meter> a asset360:Thing ."#;
        let result = replace_iris(input, &conv).unwrap();
        // Should keep the full IRI in angle brackets, not asset360:unit/Meter
        assert!(
            !result.contains("asset360:unit/Meter"),
            "Local name with / is invalid Turtle. Got: {}",
            result
        );
        assert!(
            result.contains("<https://data.example.com/asset360/unit/Meter>"),
            "Should keep angle-bracketed IRI. Got: {}",
            result
        );
    }

    #[test]
    fn replace_iris_compresses_valid_curies() {
        // Normal case: prefix with trailing / or #, clean local name
        let conv = make_converter(&[("ex", "http://example.com/")]);
        let input = r#"<http://example.com/Thing> a <http://example.com/Class> ."#;
        let result = replace_iris(input, &conv).unwrap();
        assert!(
            result.contains("ex:Thing"),
            "Valid CURIE should be compressed. Got: {}",
            result
        );
        assert!(
            result.contains("ex:Class"),
            "Valid CURIE should be compressed. Got: {}",
            result
        );
    }

    #[test]
    fn replace_iris_replaces_rdf_type_with_a() {
        let conv = make_converter(&[]);
        let input = r#"<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"#;
        let result = replace_iris(input, &conv).unwrap();
        assert_eq!(result, "a");
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
