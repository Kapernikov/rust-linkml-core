use std::collections::{HashMap, HashSet};
use std::sync::{Arc, OnceLock};

use crate::converter::Converter;
use linkml_meta::{ClassDefinition, SchemaDefinition, SlotDefinition};

use crate::identifier::{Identifier, IdentifierError};
use crate::schemaview::{CanonicalIds, SchemaView, SchemaViewError};
use crate::slotview::SlotView;

type DescendantsIndex = HashMap<(bool, bool), OnceLock<Vec<(String, String)>>>;

pub struct ClassViewData {
    pub class: ClassDefinition,
    pub slots: Vec<SlotView>,
    pub schema_uri: String,
    pub sv: SchemaView,
    descendants_index: DescendantsIndex,
}

// NOTE: `class` and `slots` are cloned snapshots taken at construction time.
// If we add mutable schema updates in the future, consider sharing live
// state instead of storing copies here.

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
                if let Some((cv, parent_schema_uri)) =
                    sv.get_classdefinition(&Identifier::new(parent), conv)?
                {
                    gather(&cv, &parent_schema_uri, sv, conv, visited, acc)?;
                }
            }
            if let Some(mixins) = &class_def.mixins {
                for mixin in mixins {
                    if let Some((cv, mixin_schema_uri)) =
                        sv.get_classdefinition(&Identifier::new(mixin), conv)?
                    {
                        gather(&cv, &mixin_schema_uri, sv, conv, visited, acc)?;
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
                    for def in &mut defs {
                        if def.owner.is_none() {
                            def.owner = Some(class_def.name.clone());
                        }
                        if def.from_schema.is_none() {
                            def.from_schema = Some(schema_uri.to_string());
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
                    for def in &mut defs {
                        if def.owner.is_none() {
                            def.owner = Some(class_def.name.clone());
                        }
                        if def.from_schema.is_none() {
                            def.from_schema = Some(schema_uri.to_string());
                        }
                    }
                    acc.insert(
                        attr_name.clone(),
                        SlotView::new(attr_name.clone(), defs, schema_uri, sv),
                    );
                }
            }

            if let Some(slot_usage) = &class_def.slot_usage {
                for (slot_name, usage) in slot_usage {
                    let mut defs = if let Some(existing) = acc.get(slot_name) {
                        existing.definitions().clone()
                    } else {
                        Vec::new()
                    };
                    let mut usage_def = *usage.clone();
                    if usage_def.owner.is_none() {
                        usage_def.owner = Some(class_def.name.clone());
                    }
                    if usage_def.from_schema.is_none() {
                        usage_def.from_schema = Some(schema_uri.to_string());
                    }
                    defs.push(usage_def);
                    acc.insert(
                        slot_name.clone(),
                        SlotView::new(slot_name.clone(), defs, schema_uri, sv),
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

    /// Returns the effective slots for this class, including slots inherited
    /// from `is_a` parents and mixins, with `slot_usage` overrides applied.
    pub fn slots(&self) -> &[SlotView] {
        &self.data.slots
    }

    /// Returns the raw [`ClassDefinition`] for this class (without inheritance).
    pub fn def(&self) -> &ClassDefinition {
        &self.data.class
    }

    /// Returns a URI for this class, controlled by `native` and `expand`.
    ///
    /// - `native=true` uses the schema's default prefix; `native=false` prefers
    ///   an explicit `class_uri` when set.
    /// - `expand=true` returns a full URI; `expand=false` returns a CURIE.
    pub fn get_uri(
        &self,
        conv: &Converter,
        native: bool,
        expand: bool,
    ) -> Result<Identifier, IdentifierError> {
        if let Some(ids) = self
            .data
            .sv
            .class_canonical_ids(&self.data.schema_uri, &self.data.class.name)
        {
            return self.cached_get_uri(ids, conv, native, expand);
        }

        self.compute_get_uri_fallback(conv, native, expand)
    }

    fn cached_get_uri(
        &self,
        ids: CanonicalIds,
        conv: &Converter,
        native: bool,
        expand: bool,
    ) -> Result<Identifier, IdentifierError> {
        match (native, expand) {
            (true, true) => Ok(ids.canonical_uri()),
            (true, false) => {
                if let Some(curie) = ids.native_curie() {
                    Ok(curie)
                } else {
                    let uri = ids.native_uri();
                    uri.to_curie(conv).map(Identifier::Curie)
                }
            }
            (false, true) => Ok(ids.canonical_uri()),
            (false, false) => {
                if let Some(curie) = ids.canonical_curie() {
                    Ok(curie)
                } else {
                    let uri = ids.canonical_uri();
                    uri.to_curie(conv).map(Identifier::Curie)
                }
            }
        }
    }

    fn compute_get_uri_fallback(
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

    /// Returns the slot marked with `designates_type=True`, if any.
    ///
    /// This is the type designator slot used during polymorphic
    /// serialization/deserialization to indicate which subclass an instance
    /// belongs to.
    pub fn get_type_designator_slot(&self) -> Option<&SlotDefinition> {
        self.data.slots.iter().find_map(|s| {
            if s.definition().designates_type.unwrap_or(false) {
                Some(s.definition())
            } else {
                None
            }
        })
    }

    /// Returns the value this class writes into the type designator slot.
    ///
    /// The format depends on the slot's range type: a full URI for `uri`
    /// ranges, a CURIE for `uriorcurie`, or the plain class name for `string`.
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

    /// Returns all values that should be accepted as valid type designators
    /// for this class during deserialization.
    ///
    /// This includes the canonical URI, native URI, and CURIE forms so that
    /// data produced with different prefix settings can be round-tripped.
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
        if let Some(ids) = self
            .data
            .sv
            .class_canonical_ids(&self.data.schema_uri, &self.data.class.name)
        {
            return ids.canonical_uri();
        }

        let fallback = self
            .data
            .sv
            .get_uri(&self.data.schema_uri, &self.data.class.name);
        if let Some(conv) = self.data.sv.converter_for_schema(&self.data.schema_uri) {
            if let Ok(uri) = fallback.to_uri(&conv) {
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
        self.data
            .sv
            .with_schema_definitions(|schemas| -> Result<(), SchemaViewError> {
                for schema in schemas.values() {
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
                                    &conv,
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
                                            &conv,
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
            })?;
        Ok(())
    }

    /// Returns classes that inherit from this class.
    ///
    /// - `recurse` — when `true`, returns the full transitive closure of
    ///   descendants; when `false`, only direct children.
    /// - `include_mixins` — when `true`, also considers classes that use this
    ///   class as a mixin.
    pub fn get_descendants(
        &self,
        recurse: bool,
        include_mixins: bool,
    ) -> Result<Vec<ClassView>, SchemaViewError> {
        let idx_lock = self
            .data
            .descendants_index
            .get(&(recurse, include_mixins))
            .expect("descendants index is initialized for all recurse/include_mixins combinations");
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
            idx_lock
                .get()
                .expect("descendants index should be initialized after computation")
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

    /// Returns the `is_a` parent class, or `None` if this is a root class.
    pub fn parent_class(&self) -> Result<Option<ClassView>, SchemaViewError> {
        let conv = match self.data.sv.converter_for_schema(&self.data.schema_uri) {
            Some(c) => c,
            None => return Err(SchemaViewError::NotFound),
        };
        match &self.data.class.is_a {
            Some(parent) => self.data.sv.get_class(&Identifier::new(parent), &conv),
            None => Ok(None),
        }
    }

    /// Returns the slot marked as `key=True` or `identifier=True`, if any.
    ///
    /// This is the slot whose value uniquely identifies instances of this class.
    /// Unlike [`identifier_slot`](Self::identifier_slot), this also matches
    /// `key` slots (which are unique within a container but not globally).
    pub fn key_or_identifier_slot(&self) -> Option<&SlotView> {
        self.data.slots.iter().find(|s| {
            let d = s.definition();
            d.identifier.unwrap_or(false) || d.key.unwrap_or(false)
        })
    }

    /// Returns the slot marked as `identifier=True`, if any.
    ///
    /// An identifier slot provides a globally unique key for instances of this
    /// class (as opposed to `key`, which is unique only within a container).
    pub fn identifier_slot(&self) -> Option<&SlotView> {
        self.data
            .slots
            .iter()
            .find(|s| s.definition().identifier.unwrap_or(false))
    }

    fn collect_ancestors_map(
        class_view: &ClassView,
        include_mixins: bool,
    ) -> Result<HashMap<String, (ClassView, usize)>, SchemaViewError> {
        fn collect_recursive(
            current: &ClassView,
            depth: usize,
            include_mixins: bool,
            acc: &mut HashMap<String, (ClassView, usize)>,
        ) -> Result<(), SchemaViewError> {
            let key = current.canonical_uri().to_string();
            let mut should_descend = false;
            match acc.get_mut(&key) {
                Some((_, existing_depth)) => {
                    if depth < *existing_depth {
                        *existing_depth = depth;
                        should_descend = true;
                    }
                }
                None => {
                    acc.insert(key.clone(), (current.clone(), depth));
                    should_descend = true;
                }
            }

            if !should_descend {
                return Ok(());
            }

            if let Some(parent) = current.parent_class()? {
                collect_recursive(&parent, depth + 1, include_mixins, acc)?;
            }

            if include_mixins {
                if let Some(mixins) = &current.data.class.mixins {
                    let conv = current
                        .data
                        .sv
                        .converter_for_schema(&current.data.schema_uri)
                        .ok_or_else(|| {
                            SchemaViewError::NoConverterForSchema(current.data.schema_uri.clone())
                        })?;
                    for mixin in mixins {
                        if let Some(mixin_view) =
                            current.data.sv.get_class(&Identifier::new(mixin), &conv)?
                        {
                            collect_recursive(&mixin_view, depth + 1, include_mixins, acc)?;
                        }
                    }
                }
            }

            Ok(())
        }

        let mut acc = HashMap::new();
        collect_recursive(class_view, 0, include_mixins, &mut acc)?;
        Ok(acc)
    }

    /// Determine the most specific common ancestor across one or more class views.
    pub fn most_specific_common_ancestor(
        class_views: &[ClassView],
        include_mixins: bool,
    ) -> Result<Option<ClassView>, SchemaViewError> {
        if class_views.is_empty() {
            return Ok(None);
        }

        let reference_sv = &class_views[0].data.sv;
        for cv in class_views.iter().skip(1) {
            if !reference_sv.is_same(&cv.data.sv) {
                return Err(SchemaViewError::SchemaViewMismatch);
            }
        }

        let mut iter = class_views.iter();
        let first = iter.next().expect("non-empty slice has a first element");

        struct AncestorAggregate {
            class_view: ClassView,
            max_depth: usize,
            total_depth: usize,
        }

        let mut common: HashMap<String, AncestorAggregate> =
            Self::collect_ancestors_map(first, include_mixins)?
                .into_iter()
                .map(|(key, (class_view, depth))| {
                    (
                        key,
                        AncestorAggregate {
                            class_view,
                            max_depth: depth,
                            total_depth: depth,
                        },
                    )
                })
                .collect();

        for other in iter {
            let candidates = Self::collect_ancestors_map(other, include_mixins)?;
            common.retain(|key, aggregate| {
                if let Some((_, depth)) = candidates.get(key) {
                    if *depth > aggregate.max_depth {
                        aggregate.max_depth = *depth;
                    }
                    aggregate.total_depth += *depth;
                    true
                } else {
                    false
                }
            });

            if common.is_empty() {
                return Ok(None);
            }
        }

        Ok(common
            .into_iter()
            .min_by(|(_, a), (_, b)| {
                a.max_depth
                    .cmp(&b.max_depth)
                    .then_with(|| a.total_depth.cmp(&b.total_depth))
                    .then_with(|| a.class_view.name().cmp(b.class_view.name()))
            })
            .map(|(_, aggregate)| aggregate.class_view))
    }
}
