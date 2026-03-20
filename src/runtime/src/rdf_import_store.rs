//! In-memory RDF stores implementing the [`TripleSource`] trait.
//!
//! Two variants are provided:
//! - [`RdfImportStore`] — lightweight, no tracking overhead
//! - [`TrackingRdfImportStore`] — records consumed subjects for diagnostics

use std::cell::RefCell;
use std::collections::HashSet;
use std::io::Read;

use oxrdf::{Graph, NamedNode, NamedOrBlankNode, NamedOrBlankNodeRef, TermRef, TripleRef};
use oxttl::{NTriplesParser, TurtleParser};

use crate::triple_source::TripleSource;
use crate::turtle_import::{import_from_store, ImportError, ImportResult, RdfFormat};
use linkml_schemaview::schemaview::SchemaView;
use linkml_schemaview::Converter;

// ── Parsing helpers ─────────────────────────────────────────────────────────

fn parse_turtle(reader: impl Read) -> Result<Graph, ImportError> {
    let mut graph = Graph::new();
    let parser = TurtleParser::new().for_reader(reader);
    for result in parser {
        let triple = result.map_err(|e| ImportError::Parse(e.to_string()))?;
        graph.insert(&triple);
    }
    Ok(graph)
}

fn parse_ntriples(reader: impl Read) -> Result<Graph, ImportError> {
    let mut graph = Graph::new();
    let parser = NTriplesParser::new().for_reader(reader);
    for result in parser {
        let triple = result.map_err(|e| ImportError::Parse(e.to_string()))?;
        graph.insert(&triple);
    }
    Ok(graph)
}

// ── RdfImportStore ──────────────────────────────────────────────────────────

/// In-memory RDF store backed by `oxrdf::Graph`. No consumption tracking.
pub struct RdfImportStore {
    graph: Graph,
}

impl RdfImportStore {
    pub fn from_turtle(reader: impl Read) -> Result<Self, ImportError> {
        Ok(Self {
            graph: parse_turtle(reader)?,
        })
    }

    pub fn from_ntriples(reader: impl Read) -> Result<Self, ImportError> {
        Ok(Self {
            graph: parse_ntriples(reader)?,
        })
    }

    pub fn from_rdf(reader: impl Read, format: RdfFormat) -> Result<Self, ImportError> {
        let graph = match format {
            RdfFormat::Turtle => parse_turtle(reader)?,
            RdfFormat::NTriples => parse_ntriples(reader)?,
        };
        Ok(Self { graph })
    }

    pub fn from_graph(graph: Graph) -> Self {
        Self { graph }
    }

    /// Run a full LinkML import against this store.
    pub fn import(
        &self,
        sv: &SchemaView,
        conv: &Converter,
        root_classes: &[&str],
    ) -> Result<ImportResult, ImportError> {
        import_from_store(self, sv, conv, root_classes)
    }

    /// Convert into a tracking store.
    pub fn with_tracking(self) -> TrackingRdfImportStore {
        TrackingRdfImportStore {
            graph: self.graph,
            consumed: RefCell::new(HashSet::new()),
        }
    }
}

impl TripleSource for RdfImportStore {
    fn subjects_for_predicate_object<'a>(
        &'a self,
        predicate: &NamedNode,
        object: &NamedNode,
    ) -> Box<dyn Iterator<Item = NamedOrBlankNodeRef<'a>> + 'a> {
        Box::new(self.graph.subjects_for_predicate_object(predicate, object))
    }

    fn objects_for_subject_predicate<'a>(
        &'a self,
        subject: &NamedOrBlankNode,
        predicate: &NamedNode,
    ) -> Box<dyn Iterator<Item = TermRef<'a>> + 'a> {
        Box::new(self.graph.objects_for_subject_predicate(subject, predicate))
    }

    fn triples_for_subject<'a>(
        &'a self,
        subject: &NamedOrBlankNode,
    ) -> Box<dyn Iterator<Item = TripleRef<'a>> + 'a> {
        Box::new(self.graph.triples_for_subject(subject))
    }

    fn len(&self) -> Option<usize> {
        Some(self.graph.len())
    }

    fn on_consumed(&self, _subject: &str, _predicate: &str, _object: &str) {
        // no-op
    }
}

// ── TrackingRdfImportStore ──────────────────────────────────────────────────

/// In-memory RDF store with subject consumption tracking.
///
/// After importing, use [`consumed_subjects()`](Self::consumed_subjects) or
/// [`unconsumed_subjects()`](Self::unconsumed_subjects) to inspect which
/// subjects were / were not touched by the harvest.
pub struct TrackingRdfImportStore {
    graph: Graph,
    consumed: RefCell<HashSet<String>>,
}

impl TrackingRdfImportStore {
    pub fn from_turtle(reader: impl Read) -> Result<Self, ImportError> {
        Ok(Self {
            graph: parse_turtle(reader)?,
            consumed: RefCell::new(HashSet::new()),
        })
    }

    pub fn from_ntriples(reader: impl Read) -> Result<Self, ImportError> {
        Ok(Self {
            graph: parse_ntriples(reader)?,
            consumed: RefCell::new(HashSet::new()),
        })
    }

    pub fn from_rdf(reader: impl Read, format: RdfFormat) -> Result<Self, ImportError> {
        let graph = match format {
            RdfFormat::Turtle => parse_turtle(reader)?,
            RdfFormat::NTriples => parse_ntriples(reader)?,
        };
        Ok(Self {
            graph,
            consumed: RefCell::new(HashSet::new()),
        })
    }

    /// Returns the set of subject IRIs that were consumed during harvest.
    pub fn consumed_subjects(&self) -> HashSet<String> {
        self.consumed.borrow().clone()
    }

    /// Returns subjects in the graph that were NOT consumed,
    /// each with the count of triples for that subject.
    pub fn unconsumed_subjects(&self) -> Vec<(String, usize)> {
        let consumed = self.consumed.borrow();
        let mut seen: HashSet<String> = HashSet::new();
        let mut result: Vec<(String, usize)> = Vec::new();
        for triple in &self.graph {
            let subj = triple.subject.to_string();
            if !consumed.contains(&subj) && seen.insert(subj.clone()) {
                let count = self.graph.triples_for_subject(triple.subject).count();
                result.push((subj, count));
            }
        }
        result
    }

    /// Total number of triples belonging to unconsumed subjects.
    pub fn unconsumed_triple_count(&self) -> usize {
        self.unconsumed_subjects().iter().map(|(_, c)| c).sum()
    }

    /// Run a full LinkML import against this store.
    pub fn import(
        &self,
        sv: &SchemaView,
        conv: &Converter,
        root_classes: &[&str],
    ) -> Result<ImportResult, ImportError> {
        import_from_store(self, sv, conv, root_classes)
    }
}

impl TripleSource for TrackingRdfImportStore {
    fn subjects_for_predicate_object<'a>(
        &'a self,
        predicate: &NamedNode,
        object: &NamedNode,
    ) -> Box<dyn Iterator<Item = NamedOrBlankNodeRef<'a>> + 'a> {
        Box::new(self.graph.subjects_for_predicate_object(predicate, object))
    }

    fn objects_for_subject_predicate<'a>(
        &'a self,
        subject: &NamedOrBlankNode,
        predicate: &NamedNode,
    ) -> Box<dyn Iterator<Item = TermRef<'a>> + 'a> {
        Box::new(self.graph.objects_for_subject_predicate(subject, predicate))
    }

    fn triples_for_subject<'a>(
        &'a self,
        subject: &NamedOrBlankNode,
    ) -> Box<dyn Iterator<Item = TripleRef<'a>> + 'a> {
        Box::new(self.graph.triples_for_subject(subject))
    }

    fn len(&self) -> Option<usize> {
        Some(self.graph.len())
    }

    fn on_consumed(&self, subject: &str, _predicate: &str, _object: &str) {
        // Track at subject granularity — ignore predicate and object
        self.consumed.borrow_mut().insert(subject.to_string());
    }
}
