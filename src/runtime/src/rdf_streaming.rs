//! Streaming RDF/Turtle import.
//!
//! Implements the two-pass algorithm described in
//! `docs/superpowers/specs/2026-05-21-rdf-import-less-ram-design.md`:
//!
//! - Pass 1 ([`compute_inline_structure`]) walks the inline subgraph from each
//!   root candidate to produce [`InlineStructure`] with `claimed`,
//!   `inline_edges`, and `materializations`. No `LinkMLInstance` is built in
//!   this pass.
//! - Pass 2 (the [`ImportStream`] iterator returned by
//!   [`import_from_store_streaming`]) yields one root tree at a time.
//!   [`Materializer`] caches subjects with `materializations >= 2` until
//!   their last consumer.

#![cfg(feature = "ttl")]

use std::collections::{HashMap, HashSet};

use oxrdf::{vocab::rdf, NamedNode, NamedOrBlankNode, TermRef};

use linkml_schemaview::identifier::Identifier;
use linkml_schemaview::schemaview::{SchemaView, SlotInlineMode};
use linkml_schemaview::Converter;

use crate::triple_source::TripleSource;
use crate::turtle_import::{resolve_class, ImportError};

/// Output of Pass 1. Drives Pass 2's caching and top-level suppression.
#[derive(Debug, Default)]
pub struct InlineStructure {
    /// Subjects that are inlined into another root somewhere. These are
    /// suppressed from the top-level result.
    pub claimed: HashSet<String>,
    /// For each subject reachable via inline edges, the list of subjects it
    /// inlines (one entry per inline edge, multiplicity preserved).
    pub inline_edges: HashMap<String, Vec<String>>,
    /// Exact number of times each subject's `LinkMLInstance` tree will be
    /// produced during Pass 2. Subjects not in the map are treated as 0.
    pub materializations: HashMap<String, usize>,
}

/// Pass 1: walk the inline subgraph from every root candidate.
///
/// Reads `rdf:type` and the triples of every reachable subject. Does NOT
/// build any `LinkMLInstance` — pure structural analysis.
pub fn compute_inline_structure<T: TripleSource>(
    store: &T,
    sv: &SchemaView,
    conv: &Converter,
    root_classes: &[&str],
) -> Result<InlineStructure, ImportError> {
    // Resolve root classes to their class URIs.
    let mut root_class_uris: Vec<NamedNode> = Vec::new();
    for &name in root_classes {
        let cv = sv
            .get_class(&Identifier::new(name), conv)
            .map_err(ImportError::SchemaError)?
            .ok_or_else(|| ImportError::UnknownClass(name.to_string()))?;
        let class_uri = cv.get_uri(conv, false, true)?;
        root_class_uris.push(NamedNode::new_unchecked(class_uri.to_string()));
    }

    let rdf_type = rdf::TYPE.into_owned();

    let mut structure = InlineStructure::default();
    let mut visited: HashSet<String> = HashSet::new();
    let mut visit_stack: HashSet<String> = HashSet::new();

    // Gather root candidates.
    let mut roots: Vec<NamedOrBlankNode> = Vec::new();
    for class_uri in &root_class_uris {
        for subj in store.subjects_for_predicate_object(&rdf_type, class_uri) {
            roots.push(subj.into_owned());
        }
    }

    for root in &roots {
        walk(
            store,
            sv,
            conv,
            root,
            &mut structure,
            &mut visited,
            &mut visit_stack,
        )?;
    }

    // Compute materialization counts via topological propagation over the
    // inline-edge DAG. Cycles were already caught in `walk`.
    initialize_materializations(&mut structure, &roots);
    propagate_materializations(&mut structure);

    Ok(structure)
}

fn walk<T: TripleSource>(
    store: &T,
    sv: &SchemaView,
    conv: &Converter,
    subject: &NamedOrBlankNode,
    structure: &mut InlineStructure,
    visited: &mut HashSet<String>,
    visit_stack: &mut HashSet<String>,
) -> Result<(), ImportError> {
    let key = subject.to_string();
    if visit_stack.contains(&key) {
        return Err(ImportError::InlinedCycle { subject: key });
    }
    if visited.contains(&key) {
        return Ok(());
    }
    visit_stack.insert(key.clone());

    // Resolve the subject's class so we can look up which slots are inline.
    // If the subject has no rdf:type, it isn't a schema subject and we have
    // no slot information for it. We still mark it visited so we don't loop.
    let class = match resolve_class(store, sv, conv, subject) {
        Ok(c) => c,
        Err(_) => {
            visit_stack.remove(&key);
            visited.insert(key);
            return Ok(());
        }
    };

    // Build a predicate -> inline_mode lookup for this class.
    let mut slot_modes: HashMap<String, SlotInlineMode> = HashMap::new();
    for slot in class.slots() {
        let canonical = slot.canonical_uri();
        let pred_iri = canonical
            .to_uri(conv)
            .map(|u| u.0)
            .unwrap_or_else(|_| canonical.to_string());
        slot_modes.insert(pred_iri, slot.determine_slot_inline_mode());
    }

    let rdf_type_str = rdf::TYPE.as_str();

    // One scan over the subject's triples.
    let mut edges: Vec<String> = Vec::new();
    for triple in store.triples_for_subject(subject) {
        let pred_iri = triple.predicate.as_str();
        if pred_iri == rdf_type_str {
            continue;
        }
        let child_subject: Option<NamedOrBlankNode> = match triple.object {
            TermRef::BlankNode(bn) => {
                // Blank nodes are always inlined regardless of slot mode.
                Some(NamedOrBlankNode::BlankNode(bn.into_owned()))
            }
            TermRef::NamedNode(nn) => {
                let is_inline = matches!(slot_modes.get(pred_iri), Some(SlotInlineMode::Inline));
                if is_inline {
                    Some(NamedOrBlankNode::NamedNode(nn.into_owned()))
                } else {
                    None
                }
            }
            _ => None,
        };
        if let Some(child) = child_subject {
            let child_key = child.to_string();
            structure.claimed.insert(child_key.clone());
            edges.push(child_key);
            walk(store, sv, conv, &child, structure, visited, visit_stack)?;
        }
    }
    if !edges.is_empty() {
        structure.inline_edges.insert(key.clone(), edges);
    }

    visit_stack.remove(&key);
    visited.insert(key);
    Ok(())
}

fn initialize_materializations(structure: &mut InlineStructure, roots: &[NamedOrBlankNode]) {
    // Unclaimed roots start at 1 (will be harvested top-level once).
    // Claimed roots start at 0 — they get count via incoming inline edges.
    for root in roots {
        let key = root.to_string();
        let init = if structure.claimed.contains(&key) { 0 } else { 1 };
        structure.materializations.insert(key, init);
    }
    // Every subject reachable via inline edges (as parent or child) must
    // appear in materializations, even if 0, so the propagation step finds
    // them.
    let mut reachable: HashSet<String> = HashSet::new();
    for (parent, children) in &structure.inline_edges {
        reachable.insert(parent.clone());
        for c in children {
            reachable.insert(c.clone());
        }
    }
    for s in reachable {
        structure.materializations.entry(s).or_insert(0);
    }
}

fn propagate_materializations(structure: &mut InlineStructure) {
    let order = topo_order(&structure.inline_edges);
    for parent in order {
        let parent_count = structure.materializations.get(&parent).copied().unwrap_or(0);
        if parent_count == 0 {
            continue;
        }
        if let Some(children) = structure.inline_edges.get(&parent).cloned() {
            for child in children {
                *structure.materializations.entry(child).or_insert(0) += parent_count;
            }
        }
    }
}

/// Kahn's algorithm topological sort over the inline-edge DAG.
fn topo_order(edges: &HashMap<String, Vec<String>>) -> Vec<String> {
    let mut in_degree: HashMap<String, usize> = HashMap::new();
    for parent in edges.keys() {
        in_degree.entry(parent.clone()).or_insert(0);
    }
    for children in edges.values() {
        for c in children {
            *in_degree.entry(c.clone()).or_insert(0) += 1;
        }
    }
    let mut queue: Vec<String> = in_degree
        .iter()
        .filter(|(_, &deg)| deg == 0)
        .map(|(k, _)| k.clone())
        .collect();
    queue.sort();
    let mut order: Vec<String> = Vec::with_capacity(in_degree.len());
    while let Some(n) = queue.pop() {
        order.push(n.clone());
        if let Some(children) = edges.get(&n) {
            for c in children {
                if let Some(d) = in_degree.get_mut(c) {
                    *d -= 1;
                    if *d == 0 {
                        queue.push(c.clone());
                    }
                }
            }
        }
    }
    order
}

/// Pass 2 state — owns the cache and the per-subject `remaining` counter.
///
/// Constructed empty (no caching) for the legacy `import_from_store` path,
/// or populated from an [`InlineStructure`] for the streaming path.
pub struct Materializer {
    pub(crate) materializations: HashMap<String, usize>,
    pub(crate) remaining: HashMap<String, usize>,
    pub(crate) cache: HashMap<String, crate::LinkMLInstance>,
}

impl Materializer {
    /// Build a Materializer that performs no caching.
    pub fn empty() -> Self {
        Self {
            materializations: HashMap::new(),
            remaining: HashMap::new(),
            cache: HashMap::new(),
        }
    }

    /// Build a Materializer pre-loaded with a Pass-1 structure.
    pub fn new(structure: &InlineStructure) -> Self {
        Self {
            materializations: structure.materializations.clone(),
            remaining: structure.materializations.clone(),
            cache: HashMap::new(),
        }
    }
}
