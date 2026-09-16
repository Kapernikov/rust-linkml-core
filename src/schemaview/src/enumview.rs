use std::collections::HashSet;
use std::sync::{Arc, OnceLock};

use linkml_meta::{EnumDefinition, PermissibleValue};

use crate::converter::Converter;
use crate::identifier::Identifier;
use crate::schemaview::{SchemaView, SchemaViewError};

/// What `gen-owl` puts between an enum's IRI and a permissible value's code
/// when the value has no `meaning` to be identified by — its
/// `enum_iri_separator`, whose default is `#`. Matched rather than chosen.
pub const ENUM_IRI_SEPARATOR: &str = "#";

/// The IRI that names one permissible value.
///
/// `gen-owl`'s `_permissible_value_uri`, term for term, and the one spelling
/// every consumer shares: the concept subject the schema graph emits, the term
/// the instance writer renders, and the term a SQL pushdown reproduces in order
/// to agree with both. It lives here, beside the enum itself, because all three
/// sit above this crate and a copy each is how they drifted apart before.
///
/// * With a `meaning`: that IRI, expanded through the schema's converter. `Err`
///   carries the unexpanded spelling, leaving the policy to the caller.
/// * Without one: `<enum_uri>#<code>`, the code trimmed and percent-encoded.
///
/// Every permissible value has an IRI, whether or not its schema bothered to
/// map it to an ontology. Which branch produced it is not a distinction a
/// caller should have to make — that it *was* one is precisely why enum values
/// used to reach instance data as two different kinds of term.
pub fn permissible_value_iri(
    enum_uri: &str,
    code: &str,
    pv: &PermissibleValue,
    conv: &Converter,
) -> Result<String, String> {
    let Some(meaning) = pv.meaning.as_ref() else {
        return Ok(format!(
            "{enum_uri}{ENUM_IRI_SEPARATOR}{}",
            encode_path_part(code.trim())
        ));
    };
    match Identifier::new(meaning).to_uri(conv) {
        Ok(uri) => Ok(uri.0),
        Err(_) => Err(meaning.clone()),
    }
}

/// The unreserved set (`A-Z a-z 0-9 - . _ ~`) survives; everything else — `/`,
/// `#` and space above all — is percent-encoded, so a value can never introduce
/// structure of its own into an IRI. This is Python's `quote(..., safe="")`,
/// which is what `gen-owl` encodes with.
pub const PATH_SEGMENT: &percent_encoding::AsciiSet = &percent_encoding::NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'.')
    .remove(b'_')
    .remove(b'~');

/// Percent-encode one IRI path segment, per [`PATH_SEGMENT`].
pub fn encode_path_part(s: &str) -> String {
    percent_encoding::utf8_percent_encode(s, PATH_SEGMENT).to_string()
}

pub struct EnumViewData {
    pub enum_def: EnumDefinition,
    pub schema_uri: String,
    pub sv: SchemaView,
    cached_pv_keys: OnceLock<Vec<String>>,
}

impl EnumViewData {
    pub fn new(enum_def: &EnumDefinition, sv: &SchemaView, schema_uri: &str) -> Self {
        EnumViewData {
            enum_def: enum_def.clone(),
            sv: sv.clone(),
            schema_uri: schema_uri.to_string(),
            cached_pv_keys: OnceLock::new(),
        }
    }
}

// NOTE: `enum_def` is a cloned snapshot. If mutable schema updates are added,
// revisit this to ensure EnumView stays in sync with the underlying data.

/// Lightweight view over a LinkML enum definition.
///
/// Cloning this type is cheap because it only clones an internal `Arc` handle.
#[derive(Clone)]
pub struct EnumView {
    data: Arc<EnumViewData>,
}

impl EnumView {
    pub fn new(enum_def: &EnumDefinition, sv: &SchemaView, schema_uri: &str) -> Self {
        EnumView {
            data: Arc::new(EnumViewData::new(enum_def, sv, schema_uri)),
        }
    }

    pub fn name(&self) -> &str {
        &self.data.enum_def.name
    }

    pub fn schema_id(&self) -> &str {
        &self.data.schema_uri
    }

    pub fn definition(&self) -> &EnumDefinition {
        &self.data.enum_def
    }

    /// Returns the canonical URI for this enum, preferring explicit `enum_uri`
    /// declarations when available.
    pub fn canonical_uri(&self) -> Identifier {
        if let Some(ids) = self
            .data
            .sv
            .enum_canonical_ids(&self.data.schema_uri, &self.data.enum_def.name)
        {
            return ids.canonical_uri();
        }

        if let Some(explicit_uri) = &self.data.enum_def.enum_uri {
            let id = Identifier::new(explicit_uri);
            if let Some(conv) = self.data.sv.converter_for_schema(&self.data.schema_uri) {
                if let Ok(uri) = id.to_uri(&conv) {
                    return Identifier::Uri(uri);
                }
            }
            return id;
        }

        let fallback = self
            .data
            .sv
            .get_uri(&self.data.schema_uri, &self.data.enum_def.name);
        if let Some(conv) = self.data.sv.converter_for_schema(&self.data.schema_uri) {
            if let Ok(uri) = fallback.to_uri(&conv) {
                return Identifier::Uri(uri);
            }
        }
        fallback
    }

    /// Returns the sorted keys of all permissible values for this enum,
    /// including values inherited via `inherits`.
    pub fn permissible_value_keys(&self) -> Result<&Vec<String>, SchemaViewError> {
        Ok(self.data.cached_pv_keys.get_or_init(|| {
            let mut keys: HashSet<String> = HashSet::new();
            if let Some(pv_map) = &self.data.enum_def.permissible_values {
                for k in pv_map.keys() {
                    keys.insert(k.clone());
                }
            }
            // Basic inheritance: merge permissible values from inherited enums by name.
            if let Some(inherits) = &self.data.enum_def.inherits {
                for e in inherits {
                    if let Some(def) = self.data.sv.get_enum_definition(&Identifier::new(e)) {
                        if let Some(pv_map) = def.permissible_values {
                            for k in pv_map.keys() {
                                keys.insert(k.clone());
                            }
                        }
                    }
                }
            }
            let mut v: Vec<String> = keys.into_iter().collect();
            v.sort();
            v
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::encode_path_part as encode_enum_code;

    /// `gen-owl` encodes with Python's `quote(..., safe="")`, whose unreserved
    /// set is exactly `A-Z a-z 0-9 - . _ ~`. These are the characters a code
    /// realistically carries that must not reach the IRI raw — `/` and `#`
    /// above all, which would otherwise introduce structure of their own.
    #[test]
    fn only_the_unreserved_set_survives() {
        assert_eq!(encode_enum_code("VSS_LO"), "VSS_LO");
        assert_eq!(encode_enum_code("REP_H-D.1~a"), "REP_H-D.1~a");
        assert_eq!(encode_enum_code("a/b"), "a%2Fb");
        assert_eq!(encode_enum_code("a#b"), "a%23b");
        assert_eq!(encode_enum_code("big stop"), "big%20stop");
        assert_eq!(encode_enum_code("100%"), "100%25");
    }

    /// Multi-byte characters are encoded byte by byte over UTF-8, upper-case.
    #[test]
    fn non_ascii_is_encoded_per_utf8_byte() {
        assert_eq!(encode_enum_code("Liège"), "Li%C3%A8ge");
    }
}
