use std::collections::{HashMap, HashSet};
use std::sync::{Arc, OnceLock};

use crate::converter::Converter;
use linkml_meta::{ClassDefinition, SchemaDefinition, SlotDefinition};

use crate::identifier::{Identifier, IdentifierError};
use crate::schemaview::{SchemaView, SchemaViewError};
use crate::slotview::SlotView;

type DescendantsIndex = HashMap<(bool, bool), OnceLock<Vec<(String, String)>>>;

pub struct ClassViewData {
    pub class: ClassDefinition,
    pub slots: Vec<SlotView>,
    pub schema_uri: String,
    pub sv: SchemaView,
    descendants_index: DescendantsIndex,
}

impl ClassViewData {
    pub fn new(
        class: &ClassDefinition,
        slots: Vec<SlotView>,
        sv: &SchemaView,
        schema_uri: &str,
    ) -> Self {
        ClassViewData {
            class: class.clone(),
            slots,
            sv: sv.clone(),
            schema_uri: schema_uri.to_string(),
            descendants_index: HashMap::new(),
        }
    }
}

/// Lightweight view over a LinkML class definition.
///
/// Cloning this type is cheap because it only clones an internal `Arc` handle.
#[derive(Clone)]
pub struct ClassView {
    data: Arc<ClassViewData>,
}

impl ClassView {
    pub fn name(&self) -> &str {
        &self.data.class.name
    }

    pub fn schema_id(&self) -> &str {
        &self.data.schema_uri
    }
    pub fn new(
        class: &ClassDefinition,
        sv: &SchemaView,
        schema_uri: &str,
        _schema_definition: &SchemaDefinition,
        conv: &Converter,
    ) -> Result<Self, SchemaViewError> {
        fn gather(
            class_def: &ClassDefinition,
            schema_uri: &str,
            sv: &SchemaView,
            conv: &Converter,
            visited: &mut HashSet<String>,
            acc: &mut HashMap<String, SlotView>,
        ) -> Result<(), SchemaViewError> {
            if !visited.insert(class_def.name.clone()) {
                return Ok(());
            }

            if let Some(parent) = &class_def.is_a {
                if let Some(cv) = sv.get_classdefinition(&Identifier::new(parent), conv)? {
                    gather(&cv, schema_uri, sv, conv, visited, acc)?;
                }
            }
            if let Some(mixins) = &class_def.mixins {
                for mixin in mixins {
                    if let Some(cv) = sv.get_classdefinition(&Identifier::new(mixin), conv)? {
                        gather(&cv, schema_uri, sv, conv, visited, acc)?;
                    }
                }
            }

            if let Some(slots) = &class_def.slots {
                for slot_ref in slots {
                    let mut slot_schema_uri = schema_uri.to_owned();
                    let mut defs: Vec<SlotDefinition> = Vec::new();
                    if let Some(base) = sv.get_slot(&Identifier::new(slot_ref), conv)? {
                        slot_schema_uri = base.schema_uri.to_owned();
                        defs.extend(base.definitions().iter().cloned());
                    }
                    if let Some(cu) = &class_def.slot_usage {
                        if let Some(usage) = cu.get(slot_ref) {
                            slot_schema_uri = schema_uri.to_owned();
                            defs.push(*usage.clone());
                        }
                    }
                    acc.insert(
                        slot_ref.clone(),
                        SlotView::new(slot_ref.clone(), defs, &slot_schema_uri, sv),
                    );
                }
            }

            if let Some(attribs) = &class_def.attributes {
                for (attr_name, attr_def) in attribs {
                    let mut defs = vec![*attr_def.clone()];
                    if let Some(cu) = &class_def.slot_usage {
                        if let Some(usage) = cu.get(attr_name) {
                            defs.push(*usage.clone());
                        }
                    }
                    acc.insert(
                        attr_name.clone(),
                        SlotView::new(attr_name.clone(), defs, schema_uri, sv),
                    );
                }
            }
            Ok(())
        }

        let mut visited = HashSet::new();
        let mut acc: HashMap<String, SlotView> = HashMap::new();
        gather(class, schema_uri, sv, conv, &mut visited, &mut acc)?;
        let mut hm = HashMap::new();
        for a in [false, true] {
            for b in [false, true] {
                hm.insert((a, b), OnceLock::new());
            }
        }
        Ok(Self {
            data: Arc::new(ClassViewData {
                class: class.clone(),
                slots: acc.into_values().collect(),
                schema_uri: schema_uri.to_owned(),
                sv: sv.clone(),
                descendants_index: hm,
            }),
        })
    }

    pub fn slots(&self) -> &[SlotView] {
        &self.data.slots
    }

    pub fn def(&self) -> &ClassDefinition {
        &self.data.class
    }

    pub fn get_uri(
        &self,
        conv: &Converter,
        native: bool,
        expand: bool,
    ) -> Result<Identifier, IdentifierError> {
        if native && expand {
            return Ok(self.canonical_uri());
        }
        let schema = self
            .data
            .sv
            .get_schema(&self.data.schema_uri)
            .ok_or_else(|| {
                IdentifierError::NameNotResolvable(format!(
                    "cannot find schema for {}",
                    self.data.schema_uri
                ))
            })?;
        let default_prefix = schema.default_prefix.as_deref().unwrap_or(&schema.name);
        let base = if native || self.data.class.class_uri.is_none() {
            if self.data.class.name.contains(":") {
                self.data.class.name.clone()
            } else {
                format!("{}:{}", default_prefix, self.data.class.name)
            }
        } else {
            self.data.class.class_uri.clone().ok_or_else(|| {
                IdentifierError::NameNotResolvable(format!(
                    "class {} missing URI",
                    self.data.class.name
                ))
            })?
        };

        if expand {
            Ok(Identifier::Uri(Identifier::new(&base).to_uri(conv)?))
        } else {
            match Identifier::new(&base) {
                Identifier::Curie(c) => Ok(Identifier::Curie(c)),
                Identifier::Uri(_) => Ok(Identifier::Curie(Identifier::new(&base).to_curie(conv)?)),
                Identifier::Name(_) => {
                    Ok(Identifier::Curie(Identifier::new(&base).to_curie(conv)?))
                }
            }
        }
    }

    pub fn get_type_designator_slot(&self) -> Option<&SlotDefinition> {
        self.data.slots.iter().find_map(|s| {
            if s.definition().designates_type.unwrap_or(false) {
                Some(s.definition())
            } else {
                None
            }
        })
    }

    pub fn get_type_designator_value(
        &self,
        type_slot: &SlotDefinition,
        conv: &Converter,
    ) -> Result<Identifier, IdentifierError> {
        if let Some(range) = &type_slot.range {
            let slot_types = self.data.sv.type_ancestors(&Identifier::new(range), conv)?;
            if slot_types.iter().any(|t| t.to_string() == "uri") {
                return self.get_uri(conv, false, true);
            } else if slot_types.iter().any(|t| t.to_string() == "uriorcurie") {
                return self.get_uri(conv, false, false);
            } else if slot_types.iter().any(|t| t.to_string() == "string") {
                return Ok(Identifier::Name(self.data.class.name.clone()));
            }
        }
        self.get_uri(conv, false, false)
    }

    pub fn get_accepted_type_designator_values(
        &self,
        type_slot: &SlotDefinition,
        conv: &Converter,
    ) -> Result<Vec<Identifier>, IdentifierError> {
        let mut vals = vec![
            self.get_uri(conv, true, true)?,
            self.get_uri(conv, false, true)?,
            self.get_uri(conv, true, false)?,
            self.get_uri(conv, false, false)?,
        ];
        let mut seen = std::collections::HashSet::new();
        vals.retain(|v| seen.insert(v.to_string()));

        if let Some(range) = &type_slot.range {
            let slot_types = self.data.sv.type_ancestors(&Identifier::new(range), conv)?;
            if slot_types
                .iter()
                .any(|t| t.to_string() == "uri" || t.to_string() == "uriorcurie")
            {
                return Ok(vals);
            } else if range == "string" {
                return Ok(vec![Identifier::Name(self.data.class.name.clone())]);
            }
        }
        Ok(vals)
    }

    /// Returns the canonical URI for this class, preferring explicit
    /// `class_uri` declarations when available.
    pub fn canonical_uri(&self) -> Identifier {
        if let Some(explicit_uri) = &self.data.class.class_uri {
            let id = Identifier::new(explicit_uri);
            if let Some(conv) = self.data.sv.converter_for_schema(&self.data.schema_uri) {
                if let Ok(uri) = id.to_uri(conv) {
                    return Identifier::Uri(uri);
                }
            }
            return id;
        }

        let fallback = self
            .data
            .sv
            .get_uri(&self.data.schema_uri, &self.data.class.name);
        if let Some(conv) = self.data.sv.converter_for_schema(&self.data.schema_uri) {
            if let Ok(uri) = fallback.to_uri(conv) {
                return Identifier::Uri(uri);
            }
        }
        fallback
    }

    fn compute_descendant_identifiers(
        &self,
        recurse: bool,
        include_mixins: bool,
        schema_uri: &str,
        class_uri: &Identifier,
        class_name: &str,
        result: &mut Vec<(String, String)>,
    ) -> Result<(), SchemaViewError> {
        let conv = self
            .data
            .sv
            .converter_for_schema(schema_uri)
            .ok_or(SchemaViewError::NotFound)?;
        for (_, schema) in self.data.sv.all_schema_definitions() {
            if let Some(classes) = &schema.classes {
                for (cls_name, cls_def) in classes {
                    let mut is_descendant = false;
                    if let Some(parent) = &cls_def.is_a {
                        if !(parent.contains(":") || parent.contains("/")) {
                            if parent == class_name {
                                is_descendant = true;
                            }
                        } else if self.data.sv.identifier_equals(
                            &self.data.sv.get_uri(&schema.id, parent),
                            class_uri,
                            conv,
                        )? {
                            is_descendant = true;
                        }
                    }
                    if !is_descendant && include_mixins {
                        if let Some(mixins) = &cls_def.mixins {
                            for mixin in mixins {
                                if self.data.sv.identifier_equals(
                                    &self.data.sv.get_uri(&schema.id, mixin),
                                    class_uri,
                                    conv,
                                )? {
                                    is_descendant = true;
                                    break;
                                }
                            }
                        }
                    }
                    if is_descendant {
                        let tpl = (schema.id.clone(), cls_name.clone());
                        if !result.contains(&tpl) {
                            result.push(tpl);
                            if recurse {
                                self.compute_descendant_identifiers(
                                    recurse,
                                    include_mixins,
                                    &schema.id,
                                    &self.data.sv.get_uri(&schema.id, cls_name),
                                    cls_name,
                                    result,
                                )?;
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }

    pub fn get_descendants(
        &self,
        recurse: bool,
        include_mixins: bool,
    ) -> Result<Vec<ClassView>, SchemaViewError> {
        let idx_lock = self
            .data
            .descendants_index
            .get(&(recurse, include_mixins))
            .ok_or_else(|| {
                SchemaViewError::CacheMissing(format!(
                    "descendants index missing entry for recurse={recurse}, include_mixins={include_mixins}"
                ))
            })?;
        let idx = if let Some(existing) = idx_lock.get() {
            existing
        } else {
            let mut res = Vec::new();
            self.compute_descendant_identifiers(
                recurse,
                include_mixins,
                &self.data.schema_uri,
                &self.canonical_uri(),
                self.name(),
                &mut res,
            )?;
            if let Err(res) = idx_lock.set(res) {
                drop(res);
            }
            idx_lock.get().ok_or_else(|| {
                SchemaViewError::CacheMissing(
                    "failed to initialize descendants index after computation".to_string(),
                )
            })?
        };
        idx.iter()
            .map(|(schema_uri, class_name)| {
                self.data
                    .sv
                    .get_class_by_schema(schema_uri, class_name)
                    .and_then(|opt| opt.ok_or(SchemaViewError::NotFound))
            })
            .collect()
    }

    pub fn parent_class(&self) -> Result<Option<ClassView>, SchemaViewError> {
        let conv = match self.data.sv.converter_for_schema(&self.data.schema_uri) {
            Some(c) => c,
            None => return Err(SchemaViewError::NotFound),
        };
        match &self.data.class.is_a {
            Some(parent) => self.data.sv.get_class(&Identifier::new(parent), conv),
            None => Ok(None),
        }
    }

    pub fn key_or_identifier_slot(&self) -> Option<&SlotView> {
        self.data.slots.iter().find(|s| {
            let d = s.definition();
            d.identifier.unwrap_or(false) || d.key.unwrap_or(false)
        })
    }

    pub fn identifier_slot(&self) -> Option<&SlotView> {
        self.data
            .slots
            .iter()
            .find(|s| s.definition().identifier.unwrap_or(false))
    }
}
