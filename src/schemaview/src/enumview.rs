use std::collections::HashSet;
use std::sync::{Arc, OnceLock};

use linkml_meta::EnumDefinition;

use crate::identifier::Identifier;
use crate::schemaview::{SchemaView, SchemaViewError};

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
