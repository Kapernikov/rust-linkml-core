use crate::converter::{Converter, ConverterError, Record};
use linkml_meta::SchemaDefinition;
use std::collections::{BTreeMap, BTreeSet};
use std::str::FromStr;

/// Error type for Identifier conversions
#[derive(Debug)]
pub enum IdentifierError {
    /// Conversion failed because the identifier is just a name
    NameNotResolvable(String),
    /// Error from the internal converter while expanding or compressing
    CurieError(ConverterError),
    /// Attempted to convert an [`Identifier`] into the wrong variant
    WrongVariant,
    NoConverter,
}

impl From<ConverterError> for IdentifierError {
    fn from(err: ConverterError) -> Self {
        IdentifierError::CurieError(err)
    }
}

/// Newtype representing a URI.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Uri(pub String);

impl From<Uri> for Identifier {
    fn from(u: Uri) -> Self {
        Identifier::Uri(u)
    }
}

impl std::fmt::Display for Uri {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl AsRef<str> for Uri {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl TryFrom<Identifier> for Uri {
    type Error = IdentifierError;

    fn try_from(value: Identifier) -> Result<Self, Self::Error> {
        match value {
            Identifier::Uri(u) => Ok(u),
            _ => Err(IdentifierError::WrongVariant),
        }
    }
}

impl<'a> TryFrom<&'a Identifier> for &'a Uri {
    type Error = IdentifierError;

    fn try_from(value: &'a Identifier) -> Result<Self, Self::Error> {
        match value {
            Identifier::Uri(u) => Ok(u),
            _ => Err(IdentifierError::WrongVariant),
        }
    }
}

/// Newtype representing a CURIE.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Curie(pub String);

impl From<Curie> for Identifier {
    fn from(c: Curie) -> Self {
        Identifier::Curie(c)
    }
}

impl std::fmt::Display for Curie {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl AsRef<str> for Curie {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl TryFrom<Identifier> for Curie {
    type Error = IdentifierError;

    fn try_from(value: Identifier) -> Result<Self, Self::Error> {
        match value {
            Identifier::Curie(c) => Ok(c),
            _ => Err(IdentifierError::WrongVariant),
        }
    }
}

impl<'a> TryFrom<&'a Identifier> for &'a Curie {
    type Error = IdentifierError;

    fn try_from(value: &'a Identifier) -> Result<Self, Self::Error> {
        match value {
            Identifier::Curie(c) => Ok(c),
            _ => Err(IdentifierError::WrongVariant),
        }
    }
}

/// Enum representing either a URI, CURIE, or bare name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Identifier {
    Uri(Uri),
    Curie(Curie),
    Name(String),
}

impl Identifier {
    /// Create a new `Identifier` from a string, auto-detecting if it's a URI,
    /// CURIE, or name.
    pub fn new(s: &str) -> Self {
        if s.contains("://") {
            Identifier::Uri(Uri(s.to_string()))
        } else if s.contains(':') {
            Identifier::Curie(Curie(s.to_string()))
        } else {
            Identifier::Name(s.to_string())
        }
    }

    /// Convert this identifier to a URI using the provided prefix registry.
    ///
    /// Returns a [`Uri`] on success.
    pub fn to_uri(&self, conv: &Converter) -> Result<Uri, IdentifierError> {
        match self {
            Identifier::Uri(u) => Ok(u.clone()),
            Identifier::Curie(c) => Ok(Uri(conv.expand(&c.0)?.to_string())),
            Identifier::Name(_) => Err(IdentifierError::NameNotResolvable(format!(
                "Cannot convert name '{}' to URI",
                self
            ))),
        }
    }

    /// Convert this identifier to a CURIE using the provided prefix registry
    ///
    /// Returns a [`Curie`] on success.
    pub fn to_curie(&self, conv: &Converter) -> Result<Curie, IdentifierError> {
        match self {
            Identifier::Curie(c) => Ok(c.clone()),
            Identifier::Uri(u) => Ok(Curie(conv.compress(&u.0)?.to_string())),
            Identifier::Name(_) => Err(IdentifierError::NameNotResolvable(format!(
                "Cannot convert name '{}' to CURIE",
                self
            ))),
        }
    }
}

impl FromStr for Identifier {
    type Err = std::convert::Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Identifier::new(s))
    }
}

impl std::fmt::Display for Identifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Identifier::Uri(u) => write!(f, "{}", u.0),
            Identifier::Curie(c) => write!(f, "{}", c.0),
            Identifier::Name(n) => write!(f, "{}", n),
        }
    }
}

impl From<Identifier> for String {
    fn from(id: Identifier) -> Self {
        match id {
            Identifier::Uri(u) => u.0,
            Identifier::Curie(c) => c.0,
            Identifier::Name(n) => n,
        }
    }
}

fn add_missing_prefix(prefix: &str, uri: &str, conv: &mut Converter) {
    if conv.find_by_prefix(prefix).is_err() {
        let _ = conv.add_prefix(prefix, uri);
    }
}

/// One prefix bound to two different namespaces by the schemas a [`Converter`]
/// was built from.
///
/// A converter can only expand a prefix one way, so [`converter_from_schemas`]
/// has to drop one of the two bindings. That is a silent-wrong-answer hazard:
/// an *unexpandable* CURIE fails loudly, but a *mis-expanded* one produces a
/// plausible IRI with no signal at all. Every dropped binding is reported here
/// so a caller that cares can refuse to trust the result, or name the conflict.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct PrefixCollision {
    /// The prefix (or prefix synonym) that two schemas bound differently.
    pub prefix: String,
    /// The namespace `prefix` expands to in the returned converter.
    pub retained_namespace: String,
    /// The namespace that is *not* reachable through `prefix`.
    pub discarded_namespace: String,
}

impl std::fmt::Display for PrefixCollision {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "prefix '{}' expands to <{}>, not to <{}>",
            self.prefix, self.retained_namespace, self.discarded_namespace
        )
    }
}

/// Build a [`Converter`] from one or more [`SchemaDefinition`]s.
///
/// All prefixes declared in the schemas are added to the converter. Schemas
/// that bind *different* prefixes to the *same* namespace merge into a single
/// record carrying the extra prefixes as `prefix_synonyms`.
///
/// Schemas that bind the *same* prefix to *different* namespaces cannot be
/// merged: only one namespace keeps the prefix, and a CURIE using it then
/// expands to a plausible but possibly wrong IRI. Use
/// [`converter_from_schemas_reporting`] if you need to know when that happened;
/// this function discards the report, which is safe only when you know the
/// schema set has no such conflict.
///
/// # Resolution rule
///
/// Every choice the build has to make is pinned to lexicographic order of the
/// schema data, never to iteration order of a hash map, so the same schema set
/// always yields the same converter — across runs of the same program included:
///
/// * of several namespaces claiming one prefix, the lexicographically smallest
///   namespace keeps it;
/// * the canonical `prefix` of a namespace is the lexicographically smallest of
///   the prefixes still available to it; the rest become `prefix_synonyms`.
///
/// A namespace that loses one prefix to a collision keeps its other prefixes —
/// the underlying `add_record` is all-or-nothing per record, so the prefixes
/// are split across records here rather than letting one clash discard an
/// unrelated, unambiguous binding for the same namespace.
///
/// None of this depends on the order `schemas` are supplied in.
pub fn converter_from_schemas<'a, I>(schemas: I) -> Converter
where
    I: IntoIterator<Item = &'a SchemaDefinition>,
{
    converter_from_schemas_reporting(schemas).0
}

/// [`converter_from_schemas`], plus every prefix collision it had to resolve.
///
/// The vector is empty for every schema set whose prefixes are unambiguous,
/// which is the normal case. It is sorted, and its contents do not depend on
/// hash-map iteration order.
pub fn converter_from_schemas_reporting<'a, I>(schemas: I) -> (Converter, Vec<PrefixCollision>)
where
    I: IntoIterator<Item = &'a SchemaDefinition>,
{
    let mut conv = Converter::default();
    // Keyed on the *namespace*, so different prefixes for one namespace collapse
    // into one record with synonyms. `BTreeMap`/`BTreeSet` rather than the hash
    // equivalents: `SchemaDefinition::prefixes` is a `HashMap` and so is the
    // schema set behind `SchemaView::converter`, and Rust's `HashMap` is
    // randomly seeded per process, so anything read out of one in order would
    // make the converter differ between runs of the same binary.
    let mut namespaces: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for schema in schemas {
        if let Some(prefixes) = &schema.prefixes {
            for (pfx, pref) in prefixes {
                namespaces
                    .entry(pref.prefix_reference.clone())
                    .or_default()
                    .insert(pfx.clone());
            }
        }
    }

    let mut collisions: Vec<PrefixCollision> = Vec::new();
    for (namespace, prefixes) in namespaces {
        // A prefix already bound by an earlier (lexicographically smaller)
        // namespace cannot be rebound: `add_record` rejects the whole record if
        // any single prefix is taken. Split the prefixes rather than lose the
        // record wholesale, so a collision on one prefix does not also throw
        // away an unrelated, unambiguous prefix for the same namespace.
        let (taken, free): (Vec<String>, Vec<String>) = prefixes
            .into_iter()
            .partition(|pfx| conv.find_by_prefix(pfx).is_ok());

        // `add_record`'s `Err` is the collision signal. Report it instead of
        // discarding it: an unexpandable CURIE fails loudly later, a
        // mis-expanded one never does.
        for pfx in taken {
            let retained = match conv.find_by_prefix(&pfx) {
                Ok(rec) => rec.uri_prefix.clone(),
                // Unreachable: `partition` just established the binding exists.
                Err(_) => continue,
            };
            collisions.push(PrefixCollision {
                prefix: pfx,
                retained_namespace: retained,
                discarded_namespace: namespace.clone(),
            });
        }

        let mut free = free.into_iter();
        let canonical = match free.next() {
            Some(p) => p,
            // Every prefix for this namespace was already taken, so the
            // namespace has no CURIE spelling at all. Already reported above.
            None => continue,
        };
        let mut record = Record::new(&canonical, &namespace);
        for synonym in free {
            record.prefix_synonyms.insert(synonym);
        }
        // Records are keyed on namespace and every prefix here is unbound, so
        // this cannot fail. Report it rather than swallow it if it ever does.
        if let Err(err) = conv.add_record(record) {
            collisions.push(PrefixCollision {
                prefix: canonical,
                retained_namespace: String::new(),
                discarded_namespace: format!("{namespace} (rejected by converter: {err})"),
            });
        }
    }
    collisions.sort();

    add_missing_prefix("rdfs", "http://www.w3.org/2000/01/rdf-schema#", &mut conv);
    add_missing_prefix(
        "rdf",
        "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
        &mut conv,
    );
    add_missing_prefix("dcterms", "http://purl.org/dc/terms/", &mut conv);

    (conv, collisions)
}

/// Convenience function for a single [`SchemaDefinition`].
pub fn converter_from_schema(schema: &SchemaDefinition) -> Converter {
    converter_from_schemas([schema])
}
