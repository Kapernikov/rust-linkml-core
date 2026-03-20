use oxrdf::{NamedNode, NamedOrBlankNode, NamedOrBlankNodeRef, TermRef, TripleRef};

/// Trait abstracting read access to an RDF triple store.
///
/// The harvest algorithm is generic over this trait, so the same code
/// works with an in-memory `oxrdf::Graph`, a SPARQL endpoint, or any
/// other backend that can answer SPO-pattern queries.
///
/// All methods take `&self`. Implementations that need interior mutability
/// (e.g. for tracking consumed triples) should use `RefCell` or similar.
pub trait TripleSource {
    /// Find all subjects with a given (predicate, object) pair.
    /// Used to discover instances of a given rdf:type.
    fn subjects_for_predicate_object<'a>(
        &'a self,
        predicate: &NamedNode,
        object: &NamedNode,
    ) -> Box<dyn Iterator<Item = NamedOrBlankNodeRef<'a>> + 'a>;

    /// Find all objects for a given (subject, predicate) pair.
    /// Used to read slot values for a subject.
    fn objects_for_subject_predicate<'a>(
        &'a self,
        subject: &NamedOrBlankNode,
        predicate: &NamedNode,
    ) -> Box<dyn Iterator<Item = TermRef<'a>> + 'a>;

    /// Iterate all triples for a given subject.
    /// Used to detect unknown fields (predicates not in the schema).
    fn triples_for_subject<'a>(
        &'a self,
        subject: &NamedOrBlankNode,
    ) -> Box<dyn Iterator<Item = TripleRef<'a>> + 'a>;

    /// Total number of triples, if cheaply available.
    /// Returns `None` when counting would be expensive (e.g. remote store).
    fn len(&self) -> Option<usize>;

    /// Returns `true` if the store is known to be empty.
    /// Returns `false` if the store is non-empty or the count is unavailable.
    fn is_empty(&self) -> bool {
        self.len() == Some(0)
    }

    /// Called when a triple is consumed by the harvest algorithm.
    /// Implementations may record this for tracking or ignore it.
    /// The full (subject, predicate, object) is provided so backends
    /// can decide their own granularity of tracking.
    fn on_consumed(&self, subject: &str, predicate: &str, object: &str);
}
