//! In-memory RDF store implementing the [`TripleSource`] trait.
//!
//! Consumption tracking (which subjects were touched by the harvest) is
//! always on: the cost is bounded by the number of harvested instances
//! and the data is useful both for the streaming public API
//! (`RdfStream::unconsumed_subjects`) and for direct Rust consumers.

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

/// In-memory RDF store backed by `oxrdf::Graph`. Always tracks consumed
/// subjects (the cost is small — bounded by the number of harvested
/// instances).
pub struct RdfImportStore {
    graph: Graph,
    consumed: RefCell<HashSet<String>>,
}

impl RdfImportStore {
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

    pub fn from_graph(graph: Graph) -> Self {
        Self {
            graph,
            consumed: RefCell::new(HashSet::new()),
        }
    }

    /// Run a full LinkML import against this store. Kept for direct Rust
    /// callers; the new public API (`import_turtle` / `import_ntriples` in
    /// `crate::rdf_import`) is preferred.
    pub fn import(
        &self,
        sv: &SchemaView,
        conv: &Converter,
        root_classes: &[&str],
    ) -> Result<ImportResult, ImportError> {
        import_from_store(self, sv, conv, root_classes)
    }

    /// Subjects that were touched (had at least one triple read) during
    /// the most recent harvest. Empty before any harvest runs.
    pub fn consumed_subjects(&self) -> HashSet<String> {
        self.consumed.borrow().clone()
    }

    /// Subjects present in the graph but NOT touched by the harvest,
    /// paired with their triple counts. Useful for schema-debugging: are
    /// there orphan subjects that the schema doesn't know about?
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

    fn on_consumed(&self, subject: &str, _predicate: &str, _object: &str) {
        // Track at subject granularity — ignore predicate and object.
        self.consumed.borrow_mut().insert(subject.to_string());
    }
}
