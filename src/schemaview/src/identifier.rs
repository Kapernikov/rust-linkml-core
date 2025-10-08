use crate::converter::{Converter, ConverterError, Record};
use linkml_meta::SchemaDefinition;
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

/// Build a [`Converter`] from one or more [`SchemaDefinition`]s.
///
/// All prefixes declared in the schemas are added to the converter. Duplicate
/// prefixes are ignored.
pub fn converter_from_schemas<'a, I>(schemas: I) -> Converter
where
    I: IntoIterator<Item = &'a SchemaDefinition>,
{
    let mut conv = Converter::default();
    use std::collections::HashMap;
    let mut map: HashMap<String, Record> = HashMap::new();
    for schema in schemas {
        if let Some(prefixes) = &schema.prefixes {
            for (pfx, pref) in prefixes {
                match map.get_mut(&pref.prefix_reference) {
                    Some(rec) => {
                        rec.prefix_synonyms.insert(pfx.clone());
                    }
                    None => {
                        let r = Record::new(pfx, &pref.prefix_reference);
                        map.insert(pref.prefix_reference.clone(), r);
                    }
                }
            }
        }
    }
    for record in map.into_values() {
        let _ = conv.add_record(record);
    }
    add_missing_prefix("rdfs", "http://www.w3.org/2000/01/rdf-schema#", &mut conv);
    add_missing_prefix(
        "rdf",
        "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
        &mut conv,
    );
    add_missing_prefix("dcterms", "http://purl.org/dc/terms/", &mut conv);

    conv
}

/// Convenience function for a single [`SchemaDefinition`].
pub fn converter_from_schema(schema: &SchemaDefinition) -> Converter {
    converter_from_schemas([schema])
}
