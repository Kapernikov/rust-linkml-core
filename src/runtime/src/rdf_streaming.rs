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
use crate::turtle_import::{harvest_subject, resolve_class, HarvestContext, ImportError};
use crate::LinkMLInstance;
use linkml_schemaview::schemaview::ClassView;

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
    materializations: HashMap<String, usize>,
    remaining: HashMap<String, usize>,
    cache: HashMap<String, LinkMLInstance>,
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

    /// Build a single subject's tree, using the cache if appropriate.
    ///
    /// Behaviour:
    /// - If `materializations[subject] < 2` → not worth caching, just build.
    /// - Otherwise: build once, cache the original; clone-with-fresh-ids for
    ///   every consumer except the last, which receives the cached tree by
    ///   move and frees the slot.
    pub fn materialise<T: TripleSource>(
        &mut self,
        ctx: &mut HarvestContext<'_, T>,
        subject: &NamedOrBlankNode,
        class: &ClassView,
    ) -> Result<LinkMLInstance, ImportError> {
        let key = subject.to_string();
        let mats = self.materializations.get(&key).copied().unwrap_or(0);

        if mats < 2 {
            // single-use → build, do not cache
            return harvest_subject(ctx, self, subject, class);
        }

        if !self.cache.contains_key(&key) {
            let tree = harvest_subject(ctx, self, subject, class)?;
            self.cache.insert(key.clone(), tree);
        }
        let r = self.remaining.entry(key.clone()).or_insert(mats);
        if *r == 0 {
            // Defensive: should never happen — Pass 1 counted exactly `mats`
            // calls. Surface as a structural error rather than panic.
            return Err(ImportError::Parse(format!(
                "rdf_streaming: materialise called more than {mats} times for {key}"
            )));
        }
        *r -= 1;
        if *r == 0 {
            // Last consumer: move out, no clone.
            return self.cache.remove(&key).ok_or_else(|| {
                ImportError::Parse(format!(
                    "rdf_streaming: cache lost subject {key} between insert and last-consumer take"
                ))
            });
        }
        // Earlier consumer: clone with fresh node_ids so denormalised copies
        // have independent identities.
        self.cache
            .get(&key)
            .map(|t| t.clone_with_fresh_node_ids())
            .ok_or_else(|| {
                ImportError::Parse(format!(
                    "rdf_streaming: cache lost subject {key} between insert and clone read"
                ))
            })
    }
}

/// Iterator returned by [`import_from_store_streaming`].
///
/// Each call to `next()` yields the next unclaimed root candidate's
/// fully-built tree (or an `ImportError`). After it yields, the caller owns
/// the tree and we drop our reference, so peak RAM stays bounded by the
/// cache (shared subtrees still with future consumers) plus one in-flight
/// root tree.
pub struct ImportStream<'a, T: TripleSource> {
    ctx: HarvestContext<'a, T>,
    materializer: Materializer,
    candidates: std::vec::IntoIter<(NamedOrBlankNode, ClassView)>,
    claimed: HashSet<String>,
}

impl<'a, T: TripleSource> ImportStream<'a, T> {
    /// Total triples consumed so far during Pass 2. Useful for computing
    /// `unconsumed_count` once the iterator is exhausted.
    pub fn consumed_count(&self) -> usize {
        self.ctx.consumed_count()
    }
}

impl<T: TripleSource> Iterator for ImportStream<'_, T> {
    type Item = Result<(String, LinkMLInstance), ImportError>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let (subj, cv) = self.candidates.next()?;
            if self.claimed.contains(&subj.to_string()) {
                continue;
            }
            let class_name = cv.name().to_string();
            let res = self.materializer.materialise(&mut self.ctx, &subj, &cv);
            return Some(res.map(|inst| (class_name, inst)));
        }
    }
}

/// An owned-store streaming iterator: the parsed `TripleSource`
/// implementation is kept alive inside the iterator itself.
///
/// Used by FFI callers (Python) that can't easily keep a separate store
/// alive alongside the iterator. The store is boxed (so its address is
/// stable) and we erase the lifetime of the borrowing `ImportStream` via
/// one carefully-scoped `unsafe` block. Sound because the only reference
/// into the store is held by `iter`, which is dropped before `_store`
/// thanks to drop order (fields are dropped top-to-bottom).
pub struct OwnedImportStream<S: TripleSource + 'static> {
    iter: ImportStream<'static, S>,
    // Kept alive for `iter`'s reference. Must be declared AFTER `iter` so it
    // is dropped after; otherwise the &'static reference would dangle.
    #[allow(dead_code)]
    store: Box<S>,
    // Same for the SchemaView and Converter.
    #[allow(dead_code)]
    sv: Box<SchemaView>,
    #[allow(dead_code)]
    conv: Box<Converter>,
}

impl<S: TripleSource + 'static> OwnedImportStream<S> {
    pub fn consumed_count(&self) -> usize {
        self.iter.consumed_count()
    }

    /// Borrow the underlying store. Useful for backend-specific
    /// post-import diagnostics (e.g. unconsumed-subject reporting on a
    /// tracking store).
    pub fn store(&self) -> &S {
        &self.store
    }
}

impl<S: TripleSource + 'static> Iterator for OwnedImportStream<S> {
    type Item = Result<(String, LinkMLInstance), ImportError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
}

/// Build an owned streaming iterator from a parsed `TripleSource` and the
/// caller's schema/converter (which we take by value and box).
pub fn import_owned_store_streaming<S: TripleSource + 'static>(
    store: S,
    sv: SchemaView,
    conv: Converter,
    root_classes: &[&str],
) -> Result<OwnedImportStream<S>, ImportError> {
    let store_box: Box<S> = Box::new(store);
    let sv_box: Box<SchemaView> = Box::new(sv);
    let conv_box: Box<Converter> = Box::new(conv);

    // SAFETY: the references handed to `import_from_store_streaming` live as
    // long as the boxes that own them. The boxes are stored as later fields
    // of `OwnedImportStream`; Rust drops struct fields in declaration order,
    // so `iter` (which holds the references) is dropped BEFORE `store`,
    // `sv`, `conv`. Until then the references remain valid.
    let store_ref: &'static S = unsafe { &*(store_box.as_ref() as *const _) };
    let sv_ref: &'static SchemaView = unsafe { &*(sv_box.as_ref() as *const _) };
    let conv_ref: &'static Converter = unsafe { &*(conv_box.as_ref() as *const _) };

    let iter = import_from_store_streaming(store_ref, sv_ref, conv_ref, root_classes)?;

    Ok(OwnedImportStream {
        iter,
        store: store_box,
        sv: sv_box,
        conv: conv_box,
    })
}

/// Streaming entry point. Pass 1 runs eagerly here, so structural errors
/// (cycles, unknown classes) surface before any item is yielded.
pub fn import_from_store_streaming<'a, T: TripleSource>(
    store: &'a T,
    sv: &'a SchemaView,
    conv: &'a Converter,
    root_classes: &[&str],
) -> Result<ImportStream<'a, T>, ImportError> {
    let structure = compute_inline_structure(store, sv, conv, root_classes)?;

    // Enumerate candidates again, this time with ClassView attached.
    let mut root_class_info: Vec<(ClassView, NamedNode)> = Vec::new();
    for &name in root_classes {
        let cv = sv
            .get_class(&Identifier::new(name), conv)
            .map_err(ImportError::SchemaError)?
            .ok_or_else(|| ImportError::UnknownClass(name.to_string()))?;
        let class_uri = cv.get_uri(conv, false, true)?;
        root_class_info.push((cv, NamedNode::new_unchecked(class_uri.to_string())));
    }
    let rdf_type = rdf::TYPE.into_owned();
    let mut candidates: Vec<(NamedOrBlankNode, ClassView)> = Vec::new();
    for (cv, class_uri) in &root_class_info {
        for subj in store.subjects_for_predicate_object(&rdf_type, class_uri) {
            candidates.push((subj.into_owned(), cv.clone()));
        }
    }

    let claimed = structure.claimed.clone();
    let materializer = Materializer::new(&structure);
    let ctx = HarvestContext::new(store, sv, conv);
    Ok(ImportStream {
        ctx,
        materializer,
        candidates: candidates.into_iter(),
        claimed,
    })
}
