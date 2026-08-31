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

// ── `default_curi_maps` + builtin `linkml:` import prefix resolution ──────
//
// `linkml_runtime.SchemaView`/`SchemaLoader` resolve prefixes from three
// sources, in order, never letting a later source override an earlier one
// (`Namespaces.add_prefixmap`'s `elif k not in self` rule):
//   1. `prefixes:` declared inline on the schema (and, once loaded, on every
//      imported schema — Python recursively loads `imports:` and merges each
//      imported schema's own `prefixes:` in the same way).
//   2. `default_curi_maps:` — named, well-known prefix bundles (e.g.
//      `semweb_context`) resolved against the `prefixcommons`/`prefixmaps`
//      registries.
//   3. A tiny hardcoded fallback (this crate only; not present in Python).
//
// Previously this function implemented only (1) (for schemas already loaded
// into the `SchemaView`) and (3). `default_curi_maps` was parsed onto
// `SchemaDefinition` but never read here, and prefixes from **builtin**
// `linkml:` imports (`linkml:types`, `linkml:mappings`, `linkml:extensions`,
// `linkml:annotations`, `linkml:units` — the schemas bundled with the LinkML
// language itself) were unreachable unless the `resolve` feature's network
// fetch (`resolve::resolve_schemas`) had already run and added them as
// separate schemas — which never happens automatically during
// `SchemaView::add_schema_with_import_ref`'s own indexing step, the exact
// point where CURIE resolution is needed and errors are raised. A schema
// that relied on either mechanism to resolve prefixes like `schema:`/`xsd:`
// (very common: LinkML's own metamodel does this) failed to load at all.
//
// `builtin_prefix_contributions` closes both gaps **synchronously, without
// network access**, by bundling the prefix tables of the 5 known-fixed
// builtin schemas (see `resolve::get_uri_for_id` for the same identity list,
// used there for full-content resolution) and a small registry of named
// curie maps. It does not attempt to resolve arbitrary/non-builtin imports
// or curie maps — those remain unresolved exactly as before, with no
// behaviour change for schemas that don't use these builtins.
fn builtin_prefix_contributions(schema: &SchemaDefinition) -> Vec<(&'static str, &'static str)> {
    let mut out = Vec::new();
    if let Some(maps) = &schema.default_curi_maps {
        for name in maps {
            if let Some(entries) = named_curi_map(name) {
                out.extend_from_slice(entries);
            }
        }
    }
    if let Some(imports) = &schema.imports {
        for import in imports {
            if let Some(entries) = builtin_linkml_schema_prefixes(import) {
                out.extend_from_slice(entries);
            }
        }
    }
    out
}

/// A bundled `default_curi_maps` entry, or `None` if we don't recognise
/// `name`. Only `semweb_context` — the map LinkML's own metamodel and
/// mappings schemas declare, and the one most commonly seen in the wild —
/// is bundled today. Add more `BIOCONTEXT_CONTEXTS`/`PREFIXMAPS_CONTEXTS`
/// entries here as they come up; an unrecognised name is simply left
/// unresolved, matching the pre-existing "unresolved is not fatal here"
/// behaviour for anything this crate can't supply.
fn named_curi_map(name: &str) -> Option<&'static [(&'static str, &'static str)]> {
    match name {
        "semweb_context" => Some(SEMWEB_CONTEXT),
        _ => None,
    }
}

/// The `semweb_context` prefix map, transcribed verbatim from
/// `prefixcommons/prefixcommons-py`'s
/// `prefixcommons/registry/semweb_context.jsonld` `@context` object — the
/// exact source `linkml_runtime.Namespaces.add_prefixmap` reads via
/// `curie_util.read_biocontext("semweb_context")`. Verified against a real
/// `linkml-runtime` install to match key-for-key, value-for-value.
const SEMWEB_CONTEXT: &[(&str, &str)] = &[
    ("dc", "http://purl.org/dc/terms/"),
    ("dcat", "http://www.w3.org/ns/dcat#"),
    ("dcterms", "http://purl.org/dc/terms/"),
    ("faldo", "http://biohackathon.org/resource/faldo#"),
    ("foaf", "http://xmlns.com/foaf/0.1/"),
    ("idot", "http://identifiers.org/"),
    ("oa", "http://www.w3.org/ns/oa#"),
    ("oboInOwl", "http://www.geneontology.org/formats/oboInOwl#"),
    ("owl", "http://www.w3.org/2002/07/owl#"),
    ("prov", "http://www.w3.org/ns/prov#"),
    ("rdf", "http://www.w3.org/1999/02/22-rdf-syntax-ns#"),
    ("rdfs", "http://www.w3.org/2000/01/rdf-schema#"),
    ("void", "http://rdfs.org/ns/void#"),
    ("xsd", "http://www.w3.org/2001/XMLSchema#"),
];

/// The prefixes declared by one of the 5 core LinkML schemas bundled with
/// the language itself, or `None` if `import` isn't one of them. Recognises
/// both the CURIE form (`linkml:types`) and the fully-expanded URI form
/// (`https://w3id.org/linkml/types`), since schema authors may write
/// either — matching by literal import identity, the same identity list
/// `resolve::get_uri_for_id` uses for full-content network resolution, so
/// this never duplicates or conflicts with that mechanism (which still owns
/// resolving imported *classes/slots*; this only ever contributes prefixes).
fn builtin_linkml_schema_prefixes(import: &str) -> Option<&'static [(&'static str, &'static str)]> {
    match import {
        "linkml:types" | "https://w3id.org/linkml/types" => Some(LINKML_TYPES_PREFIXES),
        "linkml:mappings" | "https://w3id.org/linkml/mappings" => Some(LINKML_MAPPINGS_PREFIXES),
        "linkml:extensions" | "https://w3id.org/linkml/extensions" => {
            Some(LINKML_EXTENSIONS_PREFIXES)
        }
        "linkml:annotations" | "https://w3id.org/linkml/annotations" => {
            Some(LINKML_ANNOTATIONS_PREFIXES)
        }
        "linkml:units" | "https://w3id.org/linkml/units" => Some(LINKML_UNITS_PREFIXES),
        _ => None,
    }
}

/// `prefixes:` block of `linkml_model/model/schema/types.yaml`. Importing
/// `linkml:types` is how a schema makes `schema:`/`xsd:` resolvable without
/// listing them inline — a very common pattern (LinkML's own metamodel uses
/// it). Transcribed verbatim from the canonical source; each of the 5
/// tables below is refreshed the same way.
const LINKML_TYPES_PREFIXES: &[(&str, &str)] = &[
    ("linkml", "https://w3id.org/linkml/"),
    ("xsd", "http://www.w3.org/2001/XMLSchema#"),
    ("shex", "http://www.w3.org/ns/shex#"),
    ("schema", "http://schema.org/"),
];

/// `prefixes:` block of `linkml_model/model/schema/mappings.yaml`.
const LINKML_MAPPINGS_PREFIXES: &[(&str, &str)] = &[
    ("linkml", "https://w3id.org/linkml/"),
    ("skos", "http://www.w3.org/2004/02/skos/core#"),
    ("OIO", "http://www.geneontology.org/formats/oboInOwl#"),
    ("IAO", "http://purl.obolibrary.org/obo/IAO_"),
];

/// `prefixes:` block of `linkml_model/model/schema/extensions.yaml`.
const LINKML_EXTENSIONS_PREFIXES: &[(&str, &str)] = &[("linkml", "https://w3id.org/linkml/")];

/// `prefixes:` block of `linkml_model/model/schema/annotations.yaml`.
const LINKML_ANNOTATIONS_PREFIXES: &[(&str, &str)] = &[("linkml", "https://w3id.org/linkml/")];

/// `prefixes:` block of `linkml_model/model/schema/units.yaml`.
const LINKML_UNITS_PREFIXES: &[(&str, &str)] = &[
    ("linkml", "https://w3id.org/linkml/"),
    ("qudt", "http://qudt.org/schema/qudt/"),
];

/// Build a [`Converter`] from one or more [`SchemaDefinition`]s.
///
/// All prefixes declared in the schemas are added to the converter. Duplicate
/// prefixes are ignored. Prefixes contributed by a schema's
/// `default_curi_maps` or its builtin `linkml:` imports (see
/// [`builtin_prefix_contributions`]) are then merged in without overriding
/// anything already registered — matching `linkml_runtime`'s
/// "explicit always wins" precedence.
pub fn converter_from_schemas<'a, I>(schemas: I) -> Converter
where
    I: IntoIterator<Item = &'a SchemaDefinition>,
{
    let mut conv = Converter::default();
    use std::collections::{HashMap, HashSet};
    // Keyed by URI so that prefixes sharing a URI (e.g. `dc`/`dcterms`, both
    // `http://purl.org/dc/terms/` in `semweb_context`) become synonyms on one
    // `Record`, rather than separate records — `Converter::add_record`
    // rejects a second record for a URI that is already claimed, so merging
    // same-URI prefixes as flat, independent `add_prefix` calls would
    // silently drop every prefix after the first for that URI.
    let mut map: HashMap<String, Record> = HashMap::new();
    let mut known_prefixes: HashSet<String> = HashSet::new();
    let mut builtin_contributions: Vec<(&'static str, &'static str)> = Vec::new();
    for schema in schemas {
        if let Some(prefixes) = &schema.prefixes {
            for (pfx, pref) in prefixes {
                known_prefixes.insert(pfx.clone());
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
        builtin_contributions.extend(builtin_prefix_contributions(schema));
    }
    // PARITY: explicit `prefixes:` (merged above) always win over
    // default_curi_maps / builtin-import-derived prefixes, matching
    // `Namespaces.add_prefixmap`'s `k not in self` rule — checked per-prefix
    // via `known_prefixes`, independent of any URI-sharing among builtins.
    for (pfx, uri) in builtin_contributions {
        if !known_prefixes.insert(pfx.to_string()) {
            continue;
        }
        match map.get_mut(uri) {
            Some(rec) => {
                rec.prefix_synonyms.insert(pfx.to_string());
            }
            None => {
                map.insert(uri.to_string(), Record::new(pfx, uri));
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
