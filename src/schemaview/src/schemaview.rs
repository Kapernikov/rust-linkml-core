use std::collections::HashMap;
use std::fmt;
use std::fs::File;
use std::io::{Read, Write};
use std::path::Path;
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};

use crate::converter::Converter;
use crate::identifier::{
    converter_from_schema, converter_from_schemas, Identifier, IdentifierError,
};
use crate::snapshot::{
    ResolvedImport, SchemaEntry, SchemaViewSnapshot, SCHEMAVIEW_SNAPSHOT_VERSION,
};
use arc_swap::{ArcSwap, Guard};
use linkml_meta::{EnumDefinition, SchemaDefinition, TypeDefinition};

use crate::curie::curie2uri;
// re-export views from submodules
pub use crate::classview::ClassView;
pub use crate::enumview::EnumView;
pub use crate::slotview::{SlotContainerMode, SlotInlineMode, SlotView};

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct SlotKey {
    schema_uri: String,
    owner: Option<String>,
    name: String,
}

impl SlotKey {
    fn new(schema_uri: impl Into<String>, owner: Option<String>, name: impl Into<String>) -> Self {
        SlotKey {
            schema_uri: schema_uri.into(),
            owner,
            name: name.into(),
        }
    }

    fn scoped_identifier(&self) -> String {
        match &self.owner {
            Some(owner) => format!("{}::{}::{}", self.schema_uri, owner, self.name),
            None => format!("{}::{}", self.schema_uri, self.name),
        }
    }
}

#[derive(Debug)]
pub enum SchemaViewError {
    IdentifierError(IdentifierError),
    NoPrimarySchema(String),
    NotFound,
    NoConverterForSchema(String),
    AddSchemaError(String),
    SnapshotVersionMismatch { expected: u32, actual: u32 },
    Serialization(String),
    Deserialization(String),
    Io(String),
    SchemaViewMismatch,
}

impl fmt::Display for SchemaViewError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SchemaViewError::IdentifierError(err) => write!(f, "identifier error: {:?}", err),
            SchemaViewError::NoPrimarySchema(schema) => {
                write!(f, "no primary schema set (last seen: {schema})")
            }
            SchemaViewError::NotFound => write!(f, "item not found"),
            SchemaViewError::NoConverterForSchema(schema) => {
                write!(f, "no converter for schema {schema}")
            }
            SchemaViewError::AddSchemaError(msg) => write!(f, "failed to add schema: {msg}"),
            SchemaViewError::SnapshotVersionMismatch { expected, actual } => write!(
                f,
                "snapshot version mismatch (expected {expected}, got {actual})"
            ),
            SchemaViewError::Serialization(msg) => write!(f, "serialization error: {msg}"),
            SchemaViewError::Deserialization(msg) => write!(f, "deserialization error: {msg}"),
            SchemaViewError::Io(msg) => write!(f, "io error: {msg}"),
            SchemaViewError::SchemaViewMismatch => {
                write!(f, "class views originate from different schema views")
            }
        }
    }
}

impl std::error::Error for SchemaViewError {}

impl From<IdentifierError> for SchemaViewError {
    fn from(err: IdentifierError) -> Self {
        SchemaViewError::IdentifierError(err)
    }
}

#[derive(Clone)]
pub(crate) struct SchemaViewData {
    pub(crate) schema_definitions: HashMap<String, SchemaDefinition>,
    /**
     * A map of all resolved schema imports, having (schema ID where the import was, import name):  imported schema ID
     * So, if schema with ID X imported a schema using the uri ./y.yml that had schema ID Y, this would contain a tuple (X, ./y.yml): Y
     * we need to keep this because sometimes the ID of the imported schema is different from the URL the schema was refered to by the import
     */
    pub(crate) resolved_schema_imports: HashMap<(String, String), String>,
    pub(crate) primary_schema: Option<String>,
    pub(crate) converters: HashMap<String, Converter>,
}

pub(crate) struct SchemaViewCache {
    pub(crate) class_uri_index: HashMap<String, (String, String)>,
    pub(crate) class_name_index: HashMap<String, (String, String)>,
    pub(crate) slot_uri_index: HashMap<String, Vec<SlotKey>>,
    pub(crate) slot_name_index: HashMap<String, Vec<SlotKey>>,
    pub(crate) slot_entries: HashMap<SlotKey, SlotView>,
    pub(crate) clas_view_cache: HashMap<(String, String), ClassView>,
    pub(crate) enum_uri_index: HashMap<String, (String, String)>,
    pub(crate) enum_name_index: HashMap<String, (String, String)>,
}

impl SchemaViewCache {
    pub fn new() -> Self {
        SchemaViewCache {
            class_uri_index: HashMap::new(),
            class_name_index: HashMap::new(),
            slot_uri_index: HashMap::new(),
            clas_view_cache: HashMap::new(),
            slot_name_index: HashMap::new(),
            slot_entries: HashMap::new(),
            enum_uri_index: HashMap::new(),
            enum_name_index: HashMap::new(),
        }
    }
}

impl SchemaViewData {
    pub fn new() -> Self {
        SchemaViewData {
            schema_definitions: HashMap::new(),
            resolved_schema_imports: HashMap::new(),
            primary_schema: None,
            converters: HashMap::new(),
        }
    }
}

#[derive(Clone)]
pub struct SchemaView {
    pub(crate) data: Arc<ArcSwap<SchemaViewData>>,
    pub(crate) cache: Arc<RwLock<SchemaViewCache>>,
}

impl Default for SchemaView {
    fn default() -> Self {
        Self::new()
    }
}

impl SchemaView {
    pub fn new() -> Self {
        SchemaView {
            data: Arc::new(ArcSwap::from_pointee(SchemaViewData::new())),
            cache: Arc::new(RwLock::new(SchemaViewCache::new())),
        }
    }

    fn data(&self) -> Guard<Arc<SchemaViewData>> {
        self.data.load()
    }

    fn with_data<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&SchemaViewData) -> R,
    {
        let guard = self.data();
        f(guard.as_ref())
    }

    fn update_data<F, R>(&self, mutator: F) -> R
    where
        F: FnOnce(&mut SchemaViewData) -> R,
    {
        let current = self.data.load_full();
        let mut cloned = (*current).clone();
        let result = mutator(&mut cloned);
        self.data.store(Arc::new(cloned));
        result
    }

    fn register_slot_view<I, J>(&self, key: SlotKey, slot_view: SlotView, names: I, uris: J)
    where
        I: IntoIterator<Item = String>,
        J: IntoIterator<Item = String>,
    {
        let mut cache = self.write_cache();
        cache.slot_entries.insert(key.clone(), slot_view);
        for name in names {
            let entry = cache.slot_name_index.entry(name).or_default();
            if !entry.contains(&key) {
                entry.push(key.clone());
            }
        }
        for uri in uris {
            let entry = cache.slot_uri_index.entry(uri).or_default();
            if !entry.contains(&key) {
                entry.push(key.clone());
            }
        }
        let scoped = key.scoped_identifier();
        let entry = cache.slot_uri_index.entry(scoped).or_default();
        if !entry.contains(&key) {
            entry.push(key);
        }
    }

    /// Check whether two views reference the same underlying schema data.
    pub fn is_same(&self, other: &SchemaView) -> bool {
        Arc::ptr_eq(&self.data, &other.data)
    }

    /// Build a serializable snapshot of this `SchemaView`.
    ///
    /// The snapshot contains every schema definition, resolved import entry, and primary-schema
    /// pointer so the view can be reconstructed elsewhere without re-running import resolution.
    pub fn to_snapshot(&self) -> SchemaViewSnapshot {
        let data = self.data();
        let primary_schema = data.primary_schema.clone();
        let mut schemas: Vec<SchemaEntry> = data
            .schema_definitions
            .iter()
            .map(|(schema_id, definition)| SchemaEntry {
                schema_id: schema_id.clone(),
                definition: definition.clone(),
            })
            .collect();
        if let Some(primary) = &primary_schema {
            schemas.sort_by(|a, b| {
                let a_key = (
                    a.schema_id.as_str() != primary.as_str(),
                    a.schema_id.as_str(),
                );
                let b_key = (
                    b.schema_id.as_str() != primary.as_str(),
                    b.schema_id.as_str(),
                );
                a_key.cmp(&b_key)
            });
        } else {
            schemas.sort_by(|a, b| a.schema_id.cmp(&b.schema_id));
        }

        let mut resolved_imports: Vec<ResolvedImport> = data
            .resolved_schema_imports
            .iter()
            .map(
                |((importer_schema_id, import_reference), resolved_schema_id)| ResolvedImport {
                    importer_schema_id: importer_schema_id.clone(),
                    import_reference: import_reference.clone(),
                    resolved_schema_id: resolved_schema_id.clone(),
                },
            )
            .collect();
        resolved_imports.sort_by(|a, b| {
            let a_key = (
                a.importer_schema_id.as_str(),
                a.import_reference.as_str(),
                a.resolved_schema_id.as_str(),
            );
            let b_key = (
                b.importer_schema_id.as_str(),
                b.import_reference.as_str(),
                b.resolved_schema_id.as_str(),
            );
            a_key.cmp(&b_key)
        });

        SchemaViewSnapshot {
            format_version: SCHEMAVIEW_SNAPSHOT_VERSION,
            primary_schema,
            schemas,
            resolved_imports,
        }
    }

    /// Create a `SchemaView` from a previously serialized snapshot.
    ///
    /// The resulting view mirrors the original including class/slot indexes once lazily rebuilt.
    pub fn from_snapshot(snapshot: SchemaViewSnapshot) -> Result<Self, SchemaViewError> {
        if snapshot.format_version != SCHEMAVIEW_SNAPSHOT_VERSION {
            return Err(SchemaViewError::SnapshotVersionMismatch {
                expected: SCHEMAVIEW_SNAPSHOT_VERSION,
                actual: snapshot.format_version,
            });
        }
        let mut view = SchemaView::new();
        let mut entries = snapshot.schemas;

        if let Some(primary) = snapshot.primary_schema.clone() {
            if let Some(idx) = entries.iter().position(|entry| entry.schema_id == primary) {
                let primary_entry = entries.remove(idx);
                view.add_schema(primary_entry.definition)
                    .map_err(SchemaViewError::AddSchemaError)?;
            }
        }

        for entry in entries.into_iter() {
            view.add_schema(entry.definition)
                .map_err(SchemaViewError::AddSchemaError)?;
        }

        let primary_schema = snapshot.primary_schema;
        let imports = snapshot.resolved_imports;
        view.update_data(move |data| {
            data.primary_schema = primary_schema;
            data.resolved_schema_imports = imports
                .into_iter()
                .map(|ri| {
                    (
                        (ri.importer_schema_id, ri.import_reference),
                        ri.resolved_schema_id,
                    )
                })
                .collect();
        });

        Ok(view)
    }

    /// Serialize this view into a YAML snapshot string.
    pub fn to_snapshot_yaml(&self) -> Result<String, SchemaViewError> {
        serde_yml::to_string(&self.to_snapshot())
            .map_err(|e| SchemaViewError::Serialization(e.to_string()))
    }

    /// Deserialize a `SchemaView` from a YAML snapshot string.
    pub fn from_snapshot_yaml(data: &str) -> Result<Self, SchemaViewError> {
        let snapshot: SchemaViewSnapshot = serde_yml::from_str(data)
            .map_err(|e| SchemaViewError::Deserialization(e.to_string()))?;
        SchemaView::from_snapshot(snapshot)
    }

    /// Persist this view's snapshot to `path` in YAML format.
    pub fn to_snapshot_file<P: AsRef<Path>>(&self, path: P) -> Result<(), SchemaViewError> {
        let yaml = self.to_snapshot_yaml()?;
        let mut file = File::create(path).map_err(|e| SchemaViewError::Io(e.to_string()))?;
        file.write_all(yaml.as_bytes())
            .map_err(|e| SchemaViewError::Io(e.to_string()))
    }

    /// Load a `SchemaView` snapshot stored in YAML format on disk.
    pub fn from_snapshot_file<P: AsRef<Path>>(path: P) -> Result<Self, SchemaViewError> {
        let mut file = File::open(path).map_err(|e| SchemaViewError::Io(e.to_string()))?;
        let mut contents = String::new();
        file.read_to_string(&mut contents)
            .map_err(|e| SchemaViewError::Io(e.to_string()))?;
        SchemaView::from_snapshot_yaml(&contents)
    }

    pub fn _get_resolved_schema_imports(&self) -> HashMap<(String, String), String> {
        // this is a private method to get the resolved schema imports
        // it is used in tests and should not be used in production code
        self.data().resolved_schema_imports.clone()
    }

    fn cache(&self) -> RwLockReadGuard<'_, SchemaViewCache> {
        self.cache
            .read()
            .unwrap_or_else(|poison| poison.into_inner())
    }

    fn write_cache(&self) -> RwLockWriteGuard<'_, SchemaViewCache> {
        self.cache
            .write()
            .unwrap_or_else(|poison| poison.into_inner())
    }

    pub fn get_tree_root_or(&self, class_name: Option<&str>) -> Option<ClassView> {
        let converter = self.converter_for_primary_schema()?;
        // if there is a class_name then its simple!
        if let Some(name) = class_name {
            if let Ok(Some(cv)) = self.get_class(&Identifier::new(name), &converter) {
                return Some(cv);
            }
        } else {
            // find a class with tree_root set to true in the primary schema
            let data = self.data();
            if let Some(primary) = &data.primary_schema {
                if let Some(schema) = data.schema_definitions.get(primary) {
                    if let Some(classes) = &schema.classes {
                        for (name, class_def) in classes {
                            if class_def.tree_root.is_some_and(|x| x) {
                                if let Ok(Some(cv)) =
                                    self.get_class(&Identifier::new(name), &converter)
                                {
                                    return Some(cv);
                                }
                            }
                        }
                    }
                }
            }
        }
        None
        // only search in the primary schema for a class having an attribute `tree_root`
    }

    pub fn get_default_prefix_for_schema(&self, schema_id: &str, expand: bool) -> Option<String> {
        self.with_data(|data| {
            let not_expanded = data
                .schema_definitions
                .get(schema_id)
                .and_then(|s| s.default_prefix.clone());
            if !expand {
                return not_expanded;
            }
            let prefixes = data
                .schema_definitions
                .get(schema_id)
                .and_then(|s| s.prefixes.clone());
            match prefixes {
                Some(prefixes) => prefixes
                    .get(not_expanded.as_deref()?)
                    .map(|p| p.prefix_reference.clone()),
                None => not_expanded,
            }
        })
    }

    pub fn get_uri(&self, schema_id: &str, class_name: &str) -> Identifier {
        let res = if class_name.contains(':') {
            Identifier::new(class_name)
        } else {
            let s = self
                .get_default_prefix_for_schema(schema_id, true)
                .unwrap_or(schema_id.to_string());

            let base = format!("{}{}", s, class_name);
            Identifier::new(&base)
        };
        res
    }

    pub fn add_schema(&mut self, schema: SchemaDefinition) -> Result<bool, String> {
        self.add_schema_with_import_ref(schema, None)
    }

    /// Adds `schema` to the view and optionally records which unresolved import triggered it.
    ///
    /// # Arguments
    /// * `schema` - The parsed [`SchemaDefinition`] to merge into this view.
    /// * `import_reference` - When `Some`, identifies the importing schema (`schema_id`) and the
    ///   import target `uri` that yielded `schema`. This mirrors the tuples returned by
    ///   [`SchemaView::get_unresolved_schemas`].
    ///
    /// # Examples
    /// ```no_run
    /// use linkml_schemaview::schemaview::SchemaView;
    /// use linkml_meta::SchemaDefinition;
    ///
    /// let mut sv = SchemaView::new();
    /// let schema: SchemaDefinition = serde_yml::from_str(
    ///     "id: https://example.org/schema\nname: ExampleSchema\n",
    /// )
    /// .unwrap();
    /// sv.add_schema_with_import_ref(schema, Some((
    ///     "personinfo".to_string(),
    ///     "https://example.org/personinfo.yaml".to_string(),
    /// ))).unwrap();
    /// ```
    pub fn add_schema_with_import_ref(
        &mut self,
        schema: SchemaDefinition,
        import_reference: Option<(String, String)>,
    ) -> Result<bool, String> {
        let schema_uri = schema.id.clone();
        let conv = converter_from_schema(&schema);
        let conv_clone = conv.clone();
        let schema_uri_for_converter = schema_uri.clone();
        self.update_data(move |d| {
            d.converters.insert(schema_uri_for_converter, conv_clone);
        });
        self.index_schema_classes(&schema_uri, &schema, &conv)
            .map_err(|e| format!("{:?}", e))?;
        self.index_schema_slots(&schema_uri, &schema, &conv)
            .map_err(|e| format!("{:?}", e))?;
        self.index_schema_enums(&schema_uri, &schema, &conv)
            .map_err(|e| format!("{:?}", e))?;
        let schema_uri_for_import = schema_uri.clone();
        let mut schema_opt = Some(schema);
        let added = self.update_data(move |d| {
            if let Some((schema_id, import_ref)) = import_reference.as_ref() {
                d.resolved_schema_imports.insert(
                    (schema_id.clone(), import_ref.clone()),
                    schema_uri_for_import.clone(),
                );
            }
            if d.schema_definitions.contains_key(&schema_uri) {
                false
            } else {
                if let Some(schema_value) = schema_opt.take() {
                    d.schema_definitions
                        .insert(schema_uri.clone(), schema_value);
                } else {
                    // schema already consumed; treat as no-op to avoid panic
                    return false;
                }
                if d.primary_schema.is_none() {
                    d.primary_schema = Some(schema_uri.clone());
                }
                true
            }
        });
        if added {
            self.write_cache().clas_view_cache.clear();
        }
        Ok(added)
    }

    /// Returns an owned `SchemaDefinition` for the given ID.
    ///
    /// Prefer [`with_schema_definition`] when you only need a borrow; it avoids cloning the
    /// entire schema and is therefore cheaper for large documents.
    pub fn get_schema(&self, id: &str) -> Option<SchemaDefinition> {
        self.with_data(|data| data.schema_definitions.get(id).cloned())
    }

    /// Applies `f` to the schema with the given ID, if present, borrowing the definition.
    pub fn with_schema_definition<R>(
        &self,
        id: &str,
        f: impl FnOnce(&SchemaDefinition) -> R,
    ) -> Option<R> {
        self.with_data(|data| data.schema_definitions.get(id).map(f))
    }

    /// Returns cloned schema definitions for every loaded schema.
    ///
    /// Prefer [`with_schema_definitions`] when you only need to inspect the data; this helper is
    /// kept for callers that require owned copies of every schema.
    pub fn iter_schemas(&self) -> Vec<(String, SchemaDefinition)> {
        self.with_data(|data| {
            data.schema_definitions
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect()
        })
    }

    /// Returns a converter built from every schema loaded into this view.
    ///
    /// Note that prefix collisions across schemas will resolve to whichever
    /// expansion appears last; avoid this helper if you expect conflicting
    /// CURIE mappings and instead use `converter_for_schema` with a specific
    /// schema URI.
    pub fn converter(&self) -> Converter {
        self.with_data(|data| converter_from_schemas(data.schema_definitions.values()))
    }

    pub fn converter_for_schema(&self, schema_uri: &str) -> Option<Converter> {
        self.with_data(|data| data.converters.get(schema_uri).cloned())
    }

    pub fn converter_for_primary_schema(&self) -> Option<Converter> {
        self.with_data(|data| {
            data.primary_schema
                .as_ref()
                .and_then(|uri| data.converters.get(uri))
                .cloned()
        })
    }

    pub fn get_enum_definition(
        &self,
        _identifier: &Identifier,
    ) -> Option<linkml_meta::EnumDefinition> {
        match _identifier {
            Identifier::Name(name) => {
                let candidates = vec![name.clone()];
                let data = self.data();
                for n in candidates {
                    if let Some((schema_uri, enum_name)) =
                        self.cache().enum_name_index.get(&n).cloned()
                    {
                        if let Some(schema) = data.schema_definitions.get(&schema_uri) {
                            if let Some(enums) = &schema.enums {
                                if let Some(e) = enums.get(&enum_name) {
                                    return Some(e.clone());
                                }
                            }
                        }
                    }
                }
                None
            }
            Identifier::Curie(_) | Identifier::Uri(_) => {
                let conv = self.converter_for_primary_schema()?;
                let target_uri = _identifier.to_uri(&conv).ok()?;
                let data = self.data();
                if let Some((schema_uri, enum_name)) =
                    self.cache().enum_uri_index.get(&target_uri.0).cloned()
                {
                    if let Some(schema) = data.schema_definitions.get(&schema_uri) {
                        if let Some(enums) = &schema.enums {
                            if let Some(e) = enums.get(&enum_name) {
                                return Some(e.clone());
                            }
                        }
                    }
                }
                None
            }
        }
    }

    fn index_schema_classes(
        &mut self,
        schema_uri: &str,
        schema: &SchemaDefinition,
        conv: &Converter,
    ) -> Result<(), IdentifierError> {
        let default_prefix = schema.default_prefix.as_deref().unwrap_or(&schema.name);
        if let Some(classes) = &schema.classes {
            for (class_name, class_def) in classes {
                let default_id = if class_name.contains(':') && class_def.class_uri.is_none() {
                    Identifier::new(class_name)
                } else {
                    Identifier::new(&format!("{}:{}", default_prefix, class_name))
                };
                let default_uri = default_id.to_uri(conv).map(|u| u.0).unwrap_or_else(|_| {
                    format!("{}/{}", schema.id.trim_end_matches('/'), class_name)
                });
                self.write_cache()
                    .class_name_index
                    .entry(class_name.clone())
                    .or_insert_with(|| (schema_uri.to_string(), class_name.clone()));

                if let Some(curi) = &class_def.class_uri {
                    let explicit_uri = Identifier::new(curi).to_uri(conv)?.0;
                    self.write_cache()
                        .class_uri_index
                        .entry(explicit_uri.clone())
                        .or_insert_with(|| (schema_uri.to_string(), class_name.clone()));
                    if explicit_uri != default_uri {
                        self.write_cache()
                            .class_uri_index
                            .entry(default_uri.clone())
                            .or_insert_with(|| (schema_uri.to_string(), class_name.clone()));
                    }
                } else {
                    self.write_cache()
                        .class_uri_index
                        .entry(default_uri)
                        .or_insert_with(|| (schema_uri.to_string(), class_name.clone()));
                }

                self.index_class_attributes(schema_uri, schema, class_name, class_def);
            }
        }
        Ok(())
    }

    /// Provides read-only access to all loaded schema definitions without cloning them.
    ///
    /// This is cheaper than `iter_schemas` because it keeps the internal snapshot borrowed
    /// rather than materializing owned copies of every schema.
    pub fn with_schema_definitions<R>(
        &self,
        f: impl FnOnce(&HashMap<String, SchemaDefinition>) -> R,
    ) -> R {
        self.with_data(|data| f(&data.schema_definitions))
    }

    fn index_schema_slots(
        &mut self,
        schema_uri: &str,
        schema: &SchemaDefinition,
        conv: &Converter,
    ) -> Result<(), IdentifierError> {
        let default_prefix = schema.default_prefix.as_deref().unwrap_or(&schema.name);
        if let Some(definitions) = &schema.slot_definitions {
            for (slot_name, slot_def) in definitions {
                let default_id = if slot_name.contains(':') && slot_def.slot_uri.is_none() {
                    Identifier::new(slot_name)
                } else {
                    Identifier::new(&format!("{}:{}", default_prefix, slot_name))
                };
                let default_uri = default_id.to_uri(conv).map(|u| u.0).unwrap_or_else(|_| {
                    format!("{}/{}", schema.id.trim_end_matches('/'), slot_name)
                });

                let mut uris = Vec::new();
                uris.push(default_uri.clone());
                if let Some(suri) = &slot_def.slot_uri {
                    let explicit_uri = Identifier::new(suri).to_uri(conv)?.0;
                    if !uris.contains(&explicit_uri) {
                        uris.push(explicit_uri);
                    }
                }

                let slot_view =
                    SlotView::new(slot_name.clone(), vec![slot_def.clone()], &schema.id, self);
                let canonical_string = slot_view.canonical_uri().to_string();
                if !uris.iter().any(|u| u == &canonical_string) {
                    uris.push(canonical_string);
                }
                let key = SlotKey::new(schema_uri.to_string(), None, slot_name.clone());
                self.register_slot_view(key, slot_view, vec![slot_name.clone()], uris);
            }
        }
        Ok(())
    }

    fn index_class_attributes(
        &mut self,
        schema_uri: &str,
        schema: &SchemaDefinition,
        class_name: &str,
        class_def: &linkml_meta::ClassDefinition,
    ) {
        if let Some(attributes) = &class_def.attributes {
            for (attr_name, attr_def) in attributes {
                let mut defs = vec![*attr_def.clone()];
                if let Some(slot_usage) = &class_def.slot_usage {
                    if let Some(usage) = slot_usage.get(attr_name) {
                        defs.push(*usage.clone());
                    }
                }

                for def in &mut defs {
                    def.owner = Some(class_name.to_string());
                    if def.from_schema.is_none() {
                        def.from_schema = Some(schema.id.clone());
                    }
                }

                let slot_view = SlotView::new(attr_name.clone(), defs, schema_uri, self);
                let key = SlotKey::new(
                    schema_uri.to_string(),
                    Some(class_name.to_string()),
                    attr_name.clone(),
                );
                let canonical = slot_view.canonical_uri().to_string();
                let uri_strings = vec![canonical];
                self.register_slot_view(key, slot_view, vec![attr_name.clone()], uri_strings);
            }
        }
    }

    fn index_schema_enums(
        &mut self,
        schema_uri: &str,
        schema: &SchemaDefinition,
        conv: &Converter,
    ) -> Result<(), IdentifierError> {
        let default_prefix = schema.default_prefix.as_deref().unwrap_or(&schema.name);
        if let Some(definitions) = &schema.enums {
            for (enum_name, enum_def) in definitions {
                let default_id = if enum_name.contains(':') && enum_def.enum_uri.is_none() {
                    Identifier::new(enum_name)
                } else {
                    Identifier::new(&format!("{}:{}", default_prefix, enum_name))
                };
                let default_uri = default_id.to_uri(conv).map(|u| u.0).unwrap_or_else(|_| {
                    format!("{}/{}", schema.id.trim_end_matches('/'), enum_name)
                });
                self.write_cache()
                    .enum_name_index
                    .entry(enum_name.clone())
                    .or_insert_with(|| (schema_uri.to_string(), enum_name.clone()));

                if let Some(euri) = &enum_def.enum_uri {
                    let explicit_uri = Identifier::new(euri).to_uri(conv)?.0;
                    self.write_cache()
                        .enum_uri_index
                        .entry(explicit_uri.clone())
                        .or_insert_with(|| (schema_uri.to_string(), enum_name.clone()));
                    if explicit_uri != default_uri {
                        self.write_cache()
                            .enum_uri_index
                            .entry(default_uri.clone())
                            .or_insert_with(|| (schema_uri.to_string(), enum_name.clone()));
                    }
                } else {
                    self.write_cache()
                        .enum_uri_index
                        .entry(default_uri)
                        .or_insert_with(|| (schema_uri.to_string(), enum_name.clone()));
                }
            }
        }
        Ok(())
    }

    pub fn get_enum(
        &self,
        id: &Identifier,
        conv: &Converter,
    ) -> Result<Option<EnumView>, IdentifierError> {
        let data = self.data();
        match id {
            Identifier::Name(name) => {
                if let Some((schema_uri, enum_name)) =
                    self.cache().enum_name_index.get(name).cloned()
                {
                    if let Some(schema) = data.schema_definitions.get(&schema_uri) {
                        if let Some(enums) = &schema.enums {
                            if let Some(def) = enums.get(&enum_name) {
                                return Ok(Some(EnumView::new(def, self, &schema.id)));
                            }
                        }
                    }
                }
                Ok(None)
            }
            Identifier::Curie(_) | Identifier::Uri(_) => {
                let target_uri = id.to_uri(conv)?;
                if let Some((schema_uri, enum_name)) =
                    self.cache().enum_uri_index.get(&target_uri.0).cloned()
                {
                    if let Some(schema) = data.schema_definitions.get(&schema_uri) {
                        if let Some(enums) = &schema.enums {
                            if let Some(def) = enums.get(&enum_name) {
                                return Ok(Some(EnumView::new(def, self, &schema.id)));
                            }
                        }
                    }
                }
                Ok(None)
            }
        }
    }

    pub fn get_resolution_uri_of_schema(&self, schema_id: &str) -> Option<String> {
        let data = self.data();
        data.resolved_schema_imports
            .iter()
            .find_map(|((_, uri), resolved_id)| {
                if resolved_id == schema_id {
                    Some(uri.clone())
                } else {
                    None
                }
            })
    }

    pub fn get_unresolved_schemas(&self) -> Vec<(String, String)> {
        // every schemadefinition has imports. check if an import is not in our list
        let mut unresolved = Vec::new();
        let data = self.data();
        for schema in data.schema_definitions.values() {
            if let Some(imports) = &schema.imports {
                for import in imports {
                    let import_uri = curie2uri(import, schema.prefixes.as_ref());
                    match import_uri {
                        Some(uri) => {
                            if !data.schema_definitions.contains_key(&uri)
                                && !data
                                    .resolved_schema_imports
                                    .contains_key(&(schema.id.clone(), uri.clone()))
                            {
                                unresolved.push((schema.id.clone(), uri));
                            }
                        }
                        None => {
                            // if the import cannot be expanded to a URI, treat it as a
                            // potential local file path and attempt to resolve later
                            let path = import.to_string();
                            if !data.schema_definitions.contains_key(&path)
                                && !data
                                    .resolved_schema_imports
                                    .contains_key(&(schema.id.clone(), path.clone()))
                            {
                                unresolved.push((schema.id.clone(), path));
                            }
                        }
                    }
                }
            }
        }
        unresolved
    }

    /// Returns an owned `SchemaDefinition`; prefer [`with_schema_definition`] to borrow without
    /// cloning when possible.
    pub fn get_schema_definition(&self, id: &str) -> Option<SchemaDefinition> {
        self.with_data(|data| data.schema_definitions.get(id).cloned())
    }

    pub fn get_class_by_schema(
        &self,
        schema_uri: &str,
        class_name: &str,
    ) -> Result<Option<ClassView>, SchemaViewError> {
        let cache_key = (schema_uri.to_string(), class_name.to_string());
        if let Some(cv) = self.cache().clas_view_cache.get(&cache_key) {
            return Ok(Some(cv.clone()));
        }

        let maybe_result = self.with_schema_definition(schema_uri, |schema_def| {
            let classes = schema_def.classes.as_ref()?;
            let class_def = classes.get(class_name)?;
            let converter = match self.converter_for_schema(schema_uri) {
                Some(conv) => conv,
                None => {
                    return Some(Err(SchemaViewError::NoConverterForSchema(
                        schema_uri.to_string(),
                    )))
                }
            };
            Some(ClassView::new(
                class_def, self, schema_uri, schema_def, &converter,
            ))
        });

        let class_view_result = match maybe_result {
            Some(Some(result)) => result,
            Some(None) | None => return Ok(None),
        };

        let class_view = class_view_result?;
        _ = class_view.get_descendants(true, false);
        self.write_cache()
            .clas_view_cache
            .insert(cache_key, class_view.clone());
        Ok(Some(class_view))
    }

    pub fn identifier_equals(
        &self,
        id1: &Identifier,
        id2: &Identifier,
        conv: &Converter,
    ) -> Result<bool, IdentifierError> {
        let uri1 = id1.to_uri(conv)?;
        let uri2 = id2.to_uri(conv)?;
        Ok(uri1.0 == uri2.0)
    }

    pub fn exists_class(&self, id: &Identifier, conv: &Converter) -> Result<bool, SchemaViewError> {
        // avoid doing get_class here because this needs to be cheap
        match id {
            Identifier::Name(name) => {
                let lk = self.cache().class_name_index.get(name).cloned();
                if lk.is_some() {
                    Ok(true)
                } else {
                    Ok(false)
                }
            }
            Identifier::Curie(_) | Identifier::Uri(_) => {
                let target_uri = id.to_uri(conv).map_err(SchemaViewError::IdentifierError)?;
                let lk = self
                    .cache()
                    .class_uri_index
                    .get(target_uri.0.as_str())
                    .cloned();
                if lk.is_some() {
                    Ok(true)
                } else {
                    Ok(false)
                }
            }
        }
    }

    pub fn get_classdefinition(
        &self,
        id: &Identifier,
        conv: &Converter,
    ) -> Result<Option<linkml_meta::ClassDefinition>, SchemaViewError> {
        // avoid using get_class
        match id {
            Identifier::Name(name) => {
                let lk = self.cache().class_name_index.get(name).cloned();
                if let Some((schema, class_name)) = lk {
                    let class_opt = self.with_data(|data| {
                        data.schema_definitions.get(&schema).and_then(|schema_def| {
                            schema_def
                                .classes
                                .as_ref()
                                .and_then(|classes| classes.get(&class_name).cloned())
                        })
                    });
                    return Ok(class_opt);
                }
                Ok(None)
            }
            Identifier::Curie(_) | Identifier::Uri(_) => {
                let target_uri = id.to_uri(conv).map_err(SchemaViewError::IdentifierError)?;
                let lk = self
                    .cache()
                    .class_uri_index
                    .get(target_uri.0.as_str())
                    .cloned();
                if let Some((schema_uri, class_name)) = lk {
                    let class_opt = self.with_data(|data| {
                        data.schema_definitions
                            .get(&schema_uri)
                            .and_then(|schema_def| {
                                schema_def
                                    .classes
                                    .as_ref()
                                    .and_then(|classes| classes.get(&class_name).cloned())
                            })
                    });
                    return Ok(class_opt);
                }
                Ok(None)
            }
        }
    }

    pub fn get_class(
        &self,
        id: &Identifier,
        conv: &Converter,
    ) -> Result<Option<ClassView>, SchemaViewError> {
        match id {
            Identifier::Name(name) => {
                let lk = self.cache().class_name_index.get(name).cloned();
                if let Some((schema, class_name)) = lk {
                    return self.get_class_by_schema(&schema, &class_name);
                }
                Ok(None)
            }
            Identifier::Curie(_) | Identifier::Uri(_) => {
                let target_uri = id.to_uri(conv).map_err(SchemaViewError::IdentifierError)?;
                let lk = self
                    .cache()
                    .class_uri_index
                    .get(target_uri.0.as_str())
                    .cloned();
                if let Some((schema_uri, class_name)) = lk {
                    return self.get_class_by_schema(&schema_uri, &class_name);
                }
                Ok(None)
            }
        }
    }

    pub fn get_class_ids(&self) -> Vec<String> {
        return self
            .cache()
            .class_uri_index
            .keys()
            .cloned()
            .collect::<Vec<String>>();
    }

    /// Retrieve a [`ClassView`] by its expanded URI.
    pub fn get_class_by_uri(&self, uri: &str) -> Result<Option<ClassView>, SchemaViewError> {
        let location = {
            let cache = self.cache();
            cache.class_uri_index.get(uri).cloned()
        };
        if let Some((schema_uri, class_name)) = location {
            return self.get_class_by_schema(&schema_uri, &class_name);
        }
        if uri.contains("://") {
            return Ok(None);
        }
        let conv = self.converter();
        if let Ok(expanded) = Identifier::new(uri).to_uri(&conv) {
            return self.get_class_by_uri(expanded.0.as_str());
        }
        Ok(None)
    }

    pub fn get_slot_ids(&self) -> Vec<String> {
        let mut ids: Vec<String> = self
            .cache()
            .slot_entries
            .values()
            .map(|view| view.canonical_uri().to_string())
            .collect();
        ids.sort();
        ids.dedup();
        ids
    }

    /// Retrieve a [`SlotView`] by its expanded URI.
    pub fn get_slot_by_uri(&self, uri: &str) -> Result<Option<SlotView>, SchemaViewError> {
        let keys = {
            let cache = self.cache();
            cache.slot_uri_index.get(uri).cloned()
        };
        if let Some(keys) = keys {
            let cache = self.cache();
            let pairs: Vec<(SlotKey, SlotView)> = keys
                .into_iter()
                .filter_map(|key| {
                    cache
                        .slot_entries
                        .get(&key)
                        .cloned()
                        .map(|view| (key, view))
                })
                .collect();
            drop(cache);
            if let Some(preferred) = select_preferred_slot_view(&pairs) {
                return Ok(Some(preferred));
            }
            return Ok(pairs.into_iter().next().map(|(_, view)| view));
        }
        if uri.contains("://") {
            return Ok(None);
        }
        let conv = self.converter();
        if let Ok(expanded) = Identifier::new(uri).to_uri(&conv) {
            return self.get_slot_by_uri(expanded.0.as_str());
        }
        Ok(None)
    }

    /// Return all classes as [`ClassView`]s across every schema in the view.
    pub fn class_views(&self) -> Result<Vec<ClassView>, SchemaViewError> {
        let pairs: Vec<(String, String)> = self.with_data(|data| {
            let mut acc = Vec::new();
            for (schema_uri, schema) in &data.schema_definitions {
                if let Some(classes) = &schema.classes {
                    for class_name in classes.keys() {
                        acc.push((schema_uri.clone(), class_name.clone()));
                    }
                }
            }
            acc
        });
        let mut views = Vec::new();
        for (schema_uri, class_name) in pairs {
            if let Some(cv) = self.get_class_by_schema(&schema_uri, &class_name)? {
                views.push(cv);
            }
        }
        Ok(views)
    }

    /// Return all enums as [`EnumView`]s across every schema in the view.
    pub fn enum_views(&self) -> Result<Vec<EnumView>, SchemaViewError> {
        let enums_to_build: Vec<(String, EnumDefinition, String)> = self.with_data(|data| {
            let mut acc = Vec::new();
            for (schema_uri, schema) in &data.schema_definitions {
                if let Some(enums) = &schema.enums {
                    for enum_def in enums.values() {
                        acc.push((schema_uri.clone(), enum_def.clone(), schema.id.clone()));
                    }
                }
            }
            acc
        });
        let mut out = Vec::new();
        for (schema_uri, enum_def, schema_id) in enums_to_build {
            if self.converter_for_schema(&schema_uri).is_none() {
                return Err(SchemaViewError::NoConverterForSchema(schema_uri.clone()));
            }
            out.push(EnumView::new(&enum_def, self, &schema_id));
        }
        Ok(out)
    }

    /// Return all unique slots referenced by the schemas as [`SlotView`]s.
    pub fn slot_views(&self) -> Result<Vec<SlotView>, SchemaViewError> {
        Ok(self.cache().slot_entries.values().cloned().collect())
    }

    pub fn get_slot(
        &self,
        id: &Identifier,
        conv: &Converter,
    ) -> Result<Option<SlotView>, IdentifierError> {
        match id {
            Identifier::Name(name) => {
                let cache = self.cache();
                let mut pairs: Vec<(SlotKey, SlotView)> = Vec::new();
                if let Some(keys) = cache.slot_name_index.get(name) {
                    for key in keys {
                        if let Some(view) = cache.slot_entries.get(key) {
                            pairs.push((key.clone(), view.clone()));
                        }
                    }
                }
                drop(cache);
                if let Some(preferred) = select_preferred_slot_view(&pairs) {
                    Ok(Some(preferred))
                } else {
                    Ok(pairs.into_iter().next().map(|(_, view)| view))
                }
            }
            Identifier::Curie(_) | Identifier::Uri(_) => {
                let target_uri = id.to_uri(conv)?;
                let cache = self.cache();
                let mut pairs: Vec<(SlotKey, SlotView)> = Vec::new();
                if let Some(keys) = cache.slot_uri_index.get(&target_uri.0) {
                    for key in keys {
                        if let Some(view) = cache.slot_entries.get(key) {
                            pairs.push((key.clone(), view.clone()));
                        }
                    }
                }
                drop(cache);
                if let Some(preferred) = select_preferred_slot_view(&pairs) {
                    Ok(Some(preferred))
                } else {
                    Ok(pairs.into_iter().next().map(|(_, view)| view))
                }
            }
        }
    }

    pub fn type_ancestors(
        &self,
        id: &Identifier,
        conv: &Converter,
    ) -> Result<Vec<Identifier>, IdentifierError> {
        fn get_type(
            sv: &SchemaView,
            id: &Identifier,
            conv: &Converter,
        ) -> Result<Option<TypeDefinition>, IdentifierError> {
            let data = sv.data();
            match id {
                Identifier::Name(n) => {
                    for schema in data.schema_definitions.values() {
                        if let Some(types) = &schema.types {
                            if let Some(t) = types.get(n) {
                                return Ok(Some(t.clone()));
                            }
                        }
                    }
                    Ok(None)
                }
                Identifier::Curie(_) | Identifier::Uri(_) => {
                    let target_uri = id.to_uri(conv)?;
                    for schema in data.schema_definitions.values() {
                        if let Some(types) = &schema.types {
                            for t in types.values() {
                                if let Some(turi) = &t.type_uri {
                                    if Identifier::new(turi).to_uri(conv)?.0 == target_uri.0 {
                                        return Ok(Some(t.clone()));
                                    }
                                }
                            }
                        }
                    }
                    Ok(None)
                }
            }
        }

        let mut out = Vec::new();
        let mut cur = get_type(self, id, conv)?;
        while let Some(ref t) = cur {
            out.push(Identifier::Name(t.name.clone()));
            if let Some(parent) = &t.typeof_ {
                cur = get_type(self, &Identifier::new(parent), conv)?;
            } else {
                break;
            }
        }
        if out.is_empty() {
            out.push(id.clone());
        }
        Ok(out)
    }

    pub fn primary_schema(&self) -> Option<SchemaDefinition> {
        self.with_data(|data| {
            data.primary_schema
                .as_ref()
                .and_then(|uri| data.schema_definitions.get(uri))
                .cloned()
        })
    }
}

fn select_preferred_slot_view(pairs: &[(SlotKey, SlotView)]) -> Option<SlotView> {
    pairs
        .iter()
        .min_by(|(a_key, _), (b_key, _)| {
            let a_order = (
                a_key.owner.is_some(),
                a_key.schema_uri.as_str(),
                a_key.owner.as_deref().unwrap_or(""),
                a_key.name.as_str(),
            );
            let b_order = (
                b_key.owner.is_some(),
                b_key.schema_uri.as_str(),
                b_key.owner.as_deref().unwrap_or(""),
                b_key.name.as_str(),
            );
            a_order.cmp(&b_order)
        })
        .map(|(_, view)| view.clone())
}
