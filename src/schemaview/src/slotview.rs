use std::sync::{Arc, OnceLock};

use crate::classview::ClassView;
use crate::identifier::Identifier;
use crate::schemaview::{EnumView, SchemaView};
use linkml_meta::poly::SlotExpression;
use linkml_meta::{SlotDefinition, SlotExpressionOrSubtype};

/// Resolved container shape for a slot's serialized form.
///
/// In the Python LinkML runtime, container behavior is controlled by the
/// interacting `multivalued`, `inlined`, and `inlined_as_list` booleans on
/// [`SlotDefinition`]. These booleans can conflict; `SlotContainerMode` is the
/// resolved outcome after considering the slot definition, its range class,
/// and whether that class has a key or identifier slot.
///
/// If you need the raw booleans, use [`SlotView::definition()`] to access the
/// underlying [`SlotDefinition`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SlotContainerMode {
    /// The slot holds a single value (Python: `multivalued=False`).
    SingleValue,
    /// The slot serializes as a dictionary keyed by the range class's
    /// key/identifier slot (Python: `multivalued=True`, `inlined=True`,
    /// and the range class has a key or identifier slot).
    Mapping,
    /// The slot serializes as a list (Python: `multivalued=True` and either
    /// the range is a scalar, `inlined_as_list=True`, or the range class has
    /// no key/identifier slot).
    List,
}

/// Resolved inline behavior for a slot's serialized form.
///
/// In the Python LinkML runtime, inline behavior is controlled by the
/// interacting `inlined` and `inlined_as_list` booleans, plus whether the
/// range class has an identifier slot. `SlotInlineMode` is the resolved
/// outcome.
///
/// If you need the raw booleans, use [`SlotView::definition()`] to access the
/// underlying [`SlotDefinition`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SlotInlineMode {
    /// The range object is serialized inline (nested) at this slot's position.
    /// This is the case when the range class has no identifier slot (it
    /// *must* be inlined), or when `inlined=True` / `inlined_as_list=True`.
    Inline,
    /// The range is a primitive type or enum — there is no class to inline.
    Primitive,
    /// The range class has an identifier slot and `inlined` is false, so the
    /// slot holds a reference (e.g. a foreign key) rather than the object.
    Reference,
}

/// Pre-computed range information for a slot expression.
///
/// Caches the resolved range class/enum, whether the range is scalar, and the
/// resolved [`SlotContainerMode`] and [`SlotInlineMode`] for a single slot
/// expression (or any-of branch).
#[derive(Clone)]
pub struct RangeInfo {
    pub e: SlotExpressionOrSubtype,
    pub slotview: SlotView,
    pub range_class: Option<ClassView>,
    pub range_enum: Option<EnumView>,
    pub is_range_scalar: bool,
    pub slot_container_mode: SlotContainerMode,
    pub slot_inline_mode: SlotInlineMode,
    /// The fully-expanded RDF datatype IRI for this slot's range type, if the
    /// type defines one that differs from `xsd:string`.  For example,
    /// `range: wktLiteral` resolves to `Some("http://www.opengis.net/ont/geosparql#wktLiteral")`,
    /// `range: float` resolves to `Some("http://www.w3.org/2001/XMLSchema#float")`,
    /// and `range: string` resolves to `None` (plain literal).
    pub rdf_datatype_iri: Option<String>,
    /// `true` when the range type's `typeof` chain includes `uri` or
    /// `uriorcurie`, meaning the value should be serialized as an RDF named
    /// node (IRI) rather than a literal.
    pub is_range_iri: bool,
}

impl RangeInfo {
    /// Well-known XSD string IRI — plain string literals should not carry an
    /// explicit `^^xsd:string` datatype annotation since they are equivalent.
    const XSD_STRING: &'static str = "http://www.w3.org/2001/XMLSchema#string";

    pub fn new(e: SlotExpressionOrSubtype, slotview: SlotView) -> Self {
        let range_class = Self::determine_range_class(&e, &slotview);
        let range_enum = Self::determine_range_enum(&e, &slotview);
        let is_range_scalar = Self::determine_range_scalar(&range_class);
        let slot_container_mode = Self::determine_slot_container_mode(&range_class, &e);
        let slot_inline_mode = Self::determine_slot_inline_mode(&range_class, &e);
        let (rdf_datatype_iri, is_range_iri) =
            Self::determine_rdf_type_info(&e, &slotview, &range_class, &range_enum);
        Self {
            e,
            slotview,
            range_class,
            range_enum,
            is_range_scalar,
            slot_container_mode,
            slot_inline_mode,
            rdf_datatype_iri,
            is_range_iri,
        }
    }

    fn determine_range_class(
        e: &SlotExpressionOrSubtype,
        slotview: &SlotView,
    ) -> Option<ClassView> {
        e.range().and_then(|r| {
            if let Some(conv) = slotview.sv.converter_for_schema(&slotview.schema_uri) {
                if let Ok(Some(cv)) = slotview.sv.get_class(&Identifier::new(r), &conv) {
                    return Some(cv);
                }
            }
            let conv = slotview.sv.converter();
            slotview
                .sv
                .get_class(&Identifier::new(r), &conv)
                .ok()
                .flatten()
        })
    }

    fn determine_range_enum(e: &SlotExpressionOrSubtype, slotview: &SlotView) -> Option<EnumView> {
        e.range().and_then(|r| {
            if let Some(conv) = slotview.sv.converter_for_schema(&slotview.schema_uri) {
                if let Ok(Some(ev)) = slotview.sv.get_enum(&Identifier::new(r), &conv) {
                    return Some(ev);
                }
            }
            let conv = slotview.sv.converter();
            slotview
                .sv
                .get_enum(&Identifier::new(r), &conv)
                .ok()
                .flatten()
        })
    }

    fn determine_range_scalar(range_class: &Option<ClassView>) -> bool {
        // its scalar if its not a class range
        if let Some(cr) = range_class {
            if cr.name() == "Anything" || cr.name() == "AnyValue" {
                return true;
            }
            return false;
        }
        true
    }

    fn determine_slot_container_mode(
        range_class: &Option<ClassView>,
        e: &SlotExpressionOrSubtype,
    ) -> SlotContainerMode {
        let multivalued = e.multivalued().unwrap_or(false);
        if range_class.is_none() {
            return if multivalued {
                SlotContainerMode::List
            } else {
                SlotContainerMode::SingleValue
            };
        }
        if multivalued && e.inlined_as_list().unwrap_or(false) {
            return SlotContainerMode::List;
        }
        let key_slot = range_class
            .as_ref()
            .and_then(|cv| cv.key_or_identifier_slot());
        let identifier_slot = range_class.as_ref().and_then(|cv| cv.identifier_slot());
        let mut inlined = e.inlined().unwrap_or(false);
        if identifier_slot.is_none() {
            inlined = true;
        }
        if !multivalued {
            return SlotContainerMode::SingleValue;
        }
        if !inlined {
            return SlotContainerMode::List;
        }
        if key_slot.is_some() {
            SlotContainerMode::Mapping
        } else {
            SlotContainerMode::List
        }
    }

    fn determine_slot_inline_mode(
        range_class: &Option<ClassView>,
        e: &SlotExpressionOrSubtype,
    ) -> SlotInlineMode {
        let multivalued = e.multivalued().unwrap_or(false);

        if range_class.is_none() {
            return SlotInlineMode::Primitive;
        }

        if multivalued && e.inlined_as_list().unwrap_or(false) {
            return SlotInlineMode::Inline;
        }

        let identifier_slot = range_class.as_ref().and_then(|cv| cv.identifier_slot());

        let mut inlined = e.inlined().unwrap_or(false);
        if identifier_slot.is_none() {
            inlined = true;
        }

        if !multivalued {
            return if inlined {
                SlotInlineMode::Inline
            } else {
                SlotInlineMode::Reference
            };
        }

        if !inlined {
            SlotInlineMode::Reference
        } else {
            SlotInlineMode::Inline
        }
    }

    /// Resolves the RDF datatype IRI and IRI-vs-literal disposition for a
    /// scalar range type by walking the LinkML type hierarchy.
    ///
    /// Returns `(rdf_datatype_iri, is_range_iri)` where:
    /// - `rdf_datatype_iri` is `Some(iri)` when the type's `uri` field
    ///   resolves to something other than `xsd:string`, meaning the literal
    ///   should carry a `^^<iri>` annotation.
    /// - `is_range_iri` is `true` when the type hierarchy contains `uri` or
    ///   `uriorcurie` (both map to `xsd:anyURI`), meaning the value should
    ///   be emitted as a named node rather than a literal.
    fn determine_rdf_type_info(
        e: &SlotExpressionOrSubtype,
        slotview: &SlotView,
        range_class: &Option<ClassView>,
        range_enum: &Option<EnumView>,
    ) -> (Option<String>, bool) {
        // Only relevant for scalar ranges (not classes or enums).
        if range_class.is_some() || range_enum.is_some() {
            return (None, false);
        }
        let range_name = match e.range() {
            Some(r) => r.to_string(),
            None => return (None, false),
        };

        let conv = slotview
            .sv
            .converter_for_schema(&slotview.schema_uri)
            .unwrap_or_else(|| slotview.sv.converter());

        let id = Identifier::Name(range_name);
        let ancestors = match slotview.sv.type_ancestors(&id, &conv) {
            Ok(a) => a,
            Err(_) => return (None, false),
        };
        // Check if any ancestor is the `uri` or `uriorcurie` type by name.
        let is_iri = ancestors
            .iter()
            .any(|a| matches!(a, Identifier::Name(n) if n == "uri" || n == "uriorcurie"));

        if is_iri {
            return (None, true);
        }

        // Walk the type hierarchy to find the most specific type_uri.
        // The SchemaView stores TypeDefinitions; we look up each ancestor
        // by name and check its type_uri field.
        let data = slotview.sv.data();
        let mut best_uri: Option<String> = None;
        'outer: for ancestor in &ancestors {
            let name = match ancestor {
                Identifier::Name(n) => n,
                _ => continue,
            };
            for (schema_uri, schema) in data.schema_definitions.iter() {
                if let Some(types) = &schema.types {
                    if let Some(td) = types.get(name.as_str()) {
                        if let Some(type_uri_curie) = &td.type_uri {
                            // Use the converter for the schema that defines this type,
                            // since the CURIE prefix (e.g. "xsd:") is declared there.
                            let schema_conv = slotview
                                .sv
                                .converter_for_schema(schema_uri)
                                .unwrap_or_else(|| conv.clone());
                            if let Ok(full) = Identifier::new(type_uri_curie).to_uri(&schema_conv) {
                                if best_uri.is_none() {
                                    best_uri = Some(full.0);
                                }
                            }
                        }
                        continue 'outer; // found this ancestor, move to next
                    }
                }
            }
        }

        // Suppress xsd:string — plain literals are equivalent.
        if best_uri.as_deref() == Some(Self::XSD_STRING) {
            best_uri = None;
        }

        (best_uri, false)
    }
}

pub struct SlotViewData {
    pub definitions: Vec<SlotDefinition>,
    cached_definition: OnceLock<SlotDefinition>,
    cached_range_info: OnceLock<Vec<RangeInfo>>,
}

// NOTE: The cached members above are derived from cloned slot definitions.
// If we ever introduce in-place schema mutation, these fields should switch
// to reference shared live state (e.g. via ArcSwap) rather than storing
// snapshots that must be refreshed manually.

/// Lightweight view over an effective LinkML slot definition.
///
/// Cloning this type is cheap because it only clones an internal `Arc` handle.
#[derive(Clone)]
pub struct SlotView {
    pub name: String,
    pub(crate) schema_uri: String,
    pub sv: SchemaView,
    data: Arc<SlotViewData>,
}

impl SlotView {
    pub fn new(
        name: String,
        definitions: Vec<SlotDefinition>,
        schema_uri: &str,
        schemaview: &SchemaView,
    ) -> Self {
        Self {
            name,
            schema_uri: schema_uri.to_owned(),
            sv: schemaview.clone(),
            data: Arc::new(SlotViewData {
                definitions,
                cached_definition: OnceLock::new(),
                cached_range_info: OnceLock::new(),
            }),
        }
    }

    /// Returns the effective (merged) slot definition.
    ///
    /// When a slot is inherited and refined via `slot_usage`, the definition
    /// chain is merged so that overrides take precedence. For the raw,
    /// unmerged definition list, use [`definitions()`](Self::definitions).
    pub fn definition(&self) -> &SlotDefinition {
        self.data.cached_definition.get_or_init(|| {
            let mut b = self.data.definitions[0].clone();
            for d in self.data.definitions.iter().skip(1) {
                b.merge_with(d);
                // the merge crate only provides `option::overwrite_none`, so specialized
                // slot_usage ranges would be dropped without manually copying them here;
                // replace once we have an official overwrite_except_none strategy upstream
                if let Some(range) = &d.range {
                    b.range = Some(range.clone());
                }
                if let Some(expr) = &d.range_expression {
                    b.range_expression = Some(expr.clone());
                }
                if let Some(enum_range) = &d.enum_range {
                    b.enum_range = Some(enum_range.clone());
                }
            }
            b
        })
    }

    /// Returns the raw, unmerged slot definition chain (base slot first,
    /// then `slot_usage` overrides in inheritance order).
    pub fn definitions(&self) -> &Vec<SlotDefinition> {
        &self.data.definitions
    }

    pub fn schema_id(&self) -> &str {
        &self.schema_uri
    }

    /// Returns the canonical URI for this slot, preferring explicit `slot_uri`
    /// declarations when available.
    pub fn canonical_uri(&self) -> Identifier {
        let owner = self.definition().owner.clone();
        if let Some(ids) =
            self.sv
                .slot_canonical_ids(&self.schema_uri, owner.as_deref(), &self.name)
        {
            return ids.canonical_uri();
        }

        if let Some(explicit_uri) = &self.definition().slot_uri {
            let id = Identifier::new(explicit_uri);
            if let Some(conv) = self.sv.converter_for_schema(&self.schema_uri) {
                if let Ok(uri) = id.to_uri(&conv) {
                    return Identifier::Uri(uri);
                }
            }
            return id;
        }

        let fallback = self.sv.get_uri(&self.schema_uri, &self.name);
        if let Some(conv) = self.sv.converter_for_schema(&self.schema_uri) {
            if let Ok(uri) = fallback.to_uri(&conv) {
                return Identifier::Uri(uri);
            }
        }
        fallback
    }

    /// Returns pre-computed [`RangeInfo`] for this slot's range expressions.
    ///
    /// When the slot uses `any_of`, one entry is returned per branch;
    /// otherwise a single entry covers the slot's range.
    pub fn get_range_info(&self) -> &Vec<RangeInfo> {
        self.data.cached_range_info.get_or_init(|| {
            let def = self.definition();
            if let Some(any_of) = def.any_of.clone() {
                if !any_of.is_empty() {
                    let sv = self.clone();
                    let iter = any_of.clone().into_iter().map(move |expr| -> RangeInfo {
                        RangeInfo::new(
                            SlotExpressionOrSubtype::from(expr.as_ref().clone()),
                            sv.clone(),
                        )
                    });
                    return iter.collect();
                }
            }
            std::iter::once(RangeInfo::new(
                SlotExpressionOrSubtype::from(def.clone()),
                self.clone(),
            ))
            .collect()
        })
    }

    /// Returns the range class for the primary range expression, if the range
    /// is a class (as opposed to a type or enum).
    pub fn get_range_class(&self) -> Option<ClassView> {
        self.get_range_info()
            .first()
            .and_then(|ri| ri.range_class.clone())
    }

    /// Returns the range enum for the primary range expression, if the range
    /// is an enum.
    pub fn get_range_enum(&self) -> Option<EnumView> {
        self.get_range_info()
            .first()
            .and_then(|ri| ri.range_enum.clone())
    }

    /// Returns `true` when the range is a scalar (type, enum, or the special
    /// `Anything`/`AnyValue` classes) rather than a regular class.
    pub fn is_range_scalar(&self) -> bool {
        self.get_range_info()
            .first()
            .is_none_or(|ri| ri.is_range_scalar)
    }

    /// Returns the resolved container shape for this slot.
    ///
    /// This resolves the interacting `multivalued`, `inlined`, and
    /// `inlined_as_list` booleans from the slot definition into a single
    /// [`SlotContainerMode`] value, also considering whether the range class
    /// has a key or identifier slot.
    ///
    /// See [`SlotContainerMode`] for the possible values and their meaning.
    /// For the raw booleans, use [`SlotView::definition()`].
    pub fn determine_slot_container_mode(&self) -> SlotContainerMode {
        self.get_range_info()
            .first()
            .map_or(SlotContainerMode::SingleValue, |ri| ri.slot_container_mode)
    }

    /// Returns the resolved inline behavior for this slot.
    ///
    /// This resolves the interacting `inlined` and `inlined_as_list` booleans
    /// from the slot definition into a single [`SlotInlineMode`] value, also
    /// considering whether the range class has an identifier slot.
    ///
    /// See [`SlotInlineMode`] for the possible values and their meaning.
    /// For the raw booleans, use [`SlotView::definition()`].
    pub fn determine_slot_inline_mode(&self) -> SlotInlineMode {
        self.get_range_info()
            .first()
            .map_or(SlotInlineMode::Primitive, |ri| ri.slot_inline_mode)
    }
}
