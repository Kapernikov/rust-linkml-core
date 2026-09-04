use std::sync::{Arc, OnceLock};

use crate::classview::ClassView;
use crate::identifier::Identifier;
use crate::schemaview::{EnumView, SchemaView};
use crate::Converter;
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
    /// XSD IRIs of the real-valued (non-integer) number types. A value typed
    /// against one of these is semantically a real number, so `114` and `114.0`
    /// denote the same value even though their JSON representations differ.
    const XSD_FLOAT: &'static str = "http://www.w3.org/2001/XMLSchema#float";
    const XSD_DOUBLE: &'static str = "http://www.w3.org/2001/XMLSchema#double";
    const XSD_DECIMAL: &'static str = "http://www.w3.org/2001/XMLSchema#decimal";
    /// XSD IRI of the integer number type.
    const XSD_INTEGER: &'static str = "http://www.w3.org/2001/XMLSchema#integer";

    /// Builtin LinkML type names for the number types, used as a fallback when
    /// the `linkml:types` schema that defines the XSD IRIs above has not been
    /// loaded (e.g. bare test schemas). There is no schema-free way to name a
    /// builtin type other than by its reserved name.
    const FLOAT_TYPE_NAMES: &'static [&'static str] = &["float", "double", "decimal"];
    const INTEGER_TYPE_NAMES: &'static [&'static str] = &["integer"];
    /// Every builtin LinkML type name that denotes a number. The XSD numeric
    /// datatypes beyond these four have no builtin LinkML name at all, so a
    /// schema can only reach them by declaring its own type — which is why
    /// [`is_numeric`](Self::is_numeric) has to recognise them by IRI.
    const NUMERIC_TYPE_NAMES: &'static [&'static str] = &["integer", "float", "double", "decimal"];

    /// XSD IRIs of every numeric datatype.
    const XSD_NUMERIC: &'static [&'static str] = &[
        Self::XSD_INTEGER,
        Self::XSD_FLOAT,
        Self::XSD_DOUBLE,
        Self::XSD_DECIMAL,
        "http://www.w3.org/2001/XMLSchema#int",
        "http://www.w3.org/2001/XMLSchema#long",
        "http://www.w3.org/2001/XMLSchema#short",
        "http://www.w3.org/2001/XMLSchema#byte",
        "http://www.w3.org/2001/XMLSchema#unsignedInt",
        "http://www.w3.org/2001/XMLSchema#unsignedLong",
        "http://www.w3.org/2001/XMLSchema#unsignedShort",
        "http://www.w3.org/2001/XMLSchema#unsignedByte",
        "http://www.w3.org/2001/XMLSchema#nonNegativeInteger",
        "http://www.w3.org/2001/XMLSchema#positiveInteger",
        "http://www.w3.org/2001/XMLSchema#negativeInteger",
        "http://www.w3.org/2001/XMLSchema#nonPositiveInteger",
    ];

    /// `true` when this range is a real-valued number type (`float`, `double`
    /// or `decimal`), for which an integer JSON literal should be canonicalised
    /// to a floating-point representation at boxing time.
    ///
    /// Prefers the resolved RDF datatype IRI (which also catches user-defined
    /// subtypes of `float`/`double`/`decimal`), and falls back to the builtin
    /// LinkML type names so detection still works when the `linkml:types`
    /// schema that defines those IRIs has not been loaded.
    pub fn is_floating_point(&self) -> bool {
        if matches!(
            self.rdf_datatype_iri.as_deref(),
            Some(Self::XSD_FLOAT | Self::XSD_DOUBLE | Self::XSD_DECIMAL)
        ) {
            return true;
        }
        self.range_name_matches(Self::FLOAT_TYPE_NAMES)
    }

    /// `true` when this range is the integer number type, for which a whole
    /// float JSON literal (`114.0`) should be canonicalised to an integer.
    ///
    /// Same IRI-primary, name-fallback resolution as [`is_floating_point`](Self::is_floating_point).
    pub fn is_integer(&self) -> bool {
        if matches!(self.rdf_datatype_iri.as_deref(), Some(Self::XSD_INTEGER)) {
            return true;
        }
        self.range_name_matches(Self::INTEGER_TYPE_NAMES)
    }

    /// `true` when this range is any numeric type — every XSD numeric datatype,
    /// not only the four that need JSON canonicalisation.
    ///
    /// This is the question a consumer asks when it has to decide whether values
    /// compare as numbers or as text: `'9' >= '10'` holds as text and fails as a
    /// number, so getting it wrong is silent in both directions. It is
    /// deliberately broader than [`is_integer`](Self::is_integer) and
    /// [`is_floating_point`](Self::is_floating_point), which answer the narrower
    /// question of *which* canonicalisation to apply at boxing time.
    ///
    /// Same IRI-primary, name-fallback resolution as those two: the resolved
    /// datatype IRI also catches user-defined subtypes through the `typeof`
    /// chain, and the builtin type names keep detection working when the
    /// `linkml:types` schema that defines those IRIs has not been loaded.
    pub fn is_numeric(&self) -> bool {
        if self
            .rdf_datatype_iri
            .as_deref()
            .is_some_and(|iri| Self::XSD_NUMERIC.contains(&iri))
        {
            return true;
        }
        self.range_name_matches(Self::NUMERIC_TYPE_NAMES)
    }

    /// Fallback range check by builtin type name. Only the range itself, never
    /// a class or enum, can be a number type.
    fn range_name_matches(&self, names: &[&str]) -> bool {
        if self.range_class.is_some() || self.range_enum.is_some() {
            return false;
        }
        self.e.range().is_some_and(|r| names.contains(&r))
    }

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
            .unwrap_or_else(|| Arc::new(slotview.sv.converter()));

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

/// What kind of RDF term a slot's values become.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TermKind {
    /// A named node — the range is IRI-ish, or the slot holds a reference to an
    /// object identified by its own URI.
    Iri,
    /// A literal, possibly typed or language-tagged.
    Literal,
    /// An enum whose permissible values carry `meaning` IRIs. A value present in
    /// [`TermDescriptor::enum_map`] becomes that IRI; a value without a meaning
    /// falls back to a literal, which is what the turtle writer does.
    EnumIri,
}

/// How a slot's stored values render as RDF terms.
///
/// Decided from the slot alone, so it can be resolved once and applied per
/// value — which is what a consumer that has to render values *before* it sees
/// any of them needs (e.g. pushing a query down to SQL over stored JSON, where
/// the rendering has to be decided at plan time and must match, term for term,
/// what the turtle writer would have produced for the same data).
///
/// Obtain one from [`SlotView::term_descriptor`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TermDescriptor {
    pub kind: TermKind,
    /// Datatype IRI for a typed literal. `None` means a plain literal.
    pub datatype: Option<String>,
    /// Language tag. Mutually exclusive with `datatype`, per RDF.
    pub lang: Option<String>,
    /// Stored value → expanded meaning IRI, sorted by stored value. Non-empty
    /// only for [`TermKind::EnumIri`].
    ///
    /// Sorted because it crosses into generated code downstream, where an
    /// unstable order makes queries and their tests flap.
    pub enum_map: Vec<(String, String)>,
}

impl TermDescriptor {
    /// A named node with nothing else to decide: an IRI-ish range, or a
    /// reference to an object identified by its own URI.
    fn iri() -> Self {
        Self {
            kind: TermKind::Iri,
            datatype: None,
            lang: None,
            enum_map: Vec::new(),
        }
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

    /// Returns `true` when the primary range is a real-valued number type
    /// (`float`, `double` or `decimal`). Used at boxing time to canonicalise an
    /// integer JSON literal (`114`) to a float (`114.0`) so it compares equal to
    /// a server-authored float regardless of which path produced the value.
    pub fn is_range_floating_point(&self) -> bool {
        self.get_range_info()
            .first()
            .is_some_and(|ri| ri.is_floating_point())
    }

    /// Returns `true` when the primary range is the integer number type. Used at
    /// boxing time to canonicalise a whole float JSON literal (`114.0`) to an
    /// integer (`114`).
    pub fn is_range_integer(&self) -> bool {
        self.get_range_info()
            .first()
            .is_some_and(|ri| ri.is_integer())
    }

    /// Returns `true` when the primary range's type hierarchy contains `uri` or
    /// `uriorcurie` — the value denotes an IRI, whatever it is spelled as.
    ///
    /// RDF serialization uses this to emit a named node rather than a literal;
    /// the runtime's identity machinery uses it to know that a CURIE and its
    /// expansion are the same value and must compare equal.
    pub fn is_range_iri(&self) -> bool {
        self.get_range_info()
            .first()
            .is_some_and(|ri| ri.is_range_iri)
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

    /// Returns how this slot's values render as RDF terms, or `None` when they
    /// are not a term anything can reproduce.
    ///
    /// The whole decision depends on the slot, never on the value, so it can be
    /// resolved once and then applied per value. The precedence, in order:
    ///
    /// 1. an enum value carrying a `meaning` → that IRI;
    /// 2. an IRI-ish range (`uri`/`uriorcurie` in the `typeof` chain) → a named
    ///    node;
    /// 3. `in_language` on the slot, only when there is no datatype (RDF allows
    ///    one or the other) → a language-tagged literal;
    /// 4. a custom RDF datatype → a typed literal;
    /// 5. otherwise → a plain literal.
    ///
    /// `conv` expands the enum `meaning` CURIEs, so pass the same converter the
    /// values will be serialized with.
    ///
    /// Rules 1 and 2 cannot both apply, so their order is immaterial:
    /// `determine_rdf_type_info` yields `(None, false)` for an enum range, so
    /// `is_range_iri` is never true for one. Stating the chain in one place is
    /// what makes that invariant visible.
    pub fn term_descriptor(&self, conv: &Converter) -> Option<TermDescriptor> {
        // A class range needs care, and the two cases differ. A *reference*
        // stores the target's URI, so the stored value is exactly the named node
        // that gets emitted. An *inlined* structure serializes as a blank node
        // whose label nothing can reproduce — not a second serialization run,
        // not a consumer reading the stored value back — so there is no term to
        // describe. Such a slot is still traversable; it is just never a value.
        if self.get_range_class().is_some() {
            return match self.determine_slot_inline_mode() {
                SlotInlineMode::Reference => Some(TermDescriptor::iri()),
                _ => None,
            };
        }

        let info = self.get_range_info().first();
        let datatype = info.and_then(|ri| ri.rdf_datatype_iri.clone());
        // Rules 3 and 4 collide — RDF allows a datatype or a language tag, not
        // both — and the datatype wins.
        let lang = if datatype.is_none() {
            self.definition().in_language.clone()
        } else {
            None
        };

        // Rule 1. The map is finite (the permissible values) so materializing it
        // is safe, unlike walking the schema graph. `lang` is carried through
        // because a value with no meaning falls back to a literal.
        let enum_map = self.enum_meanings(conv);
        if !enum_map.is_empty() {
            return Some(TermDescriptor {
                kind: TermKind::EnumIri,
                datatype,
                lang,
                enum_map,
            });
        }

        // Rule 2.
        if info.is_some_and(|ri| ri.is_range_iri) {
            return Some(TermDescriptor::iri());
        }

        // Rules 3, 4 and 5.
        Some(TermDescriptor {
            kind: TermKind::Literal,
            datatype,
            lang,
            enum_map: Vec::new(),
        })
    }

    /// Permissible value → expanded meaning IRI, sorted. Empty when the range is
    /// not an enum, or when no permissible value carries a `meaning`.
    fn enum_meanings(&self, conv: &Converter) -> Vec<(String, String)> {
        let Some(enum_view) = self.get_range_enum() else {
            return Vec::new();
        };
        let Some(values) = enum_view.definition().permissible_values.as_ref() else {
            return Vec::new();
        };
        let mut out: Vec<(String, String)> = values
            .iter()
            .filter_map(|(text, pv)| {
                let meaning = pv.meaning.as_ref()?;
                let iri = Identifier::new(meaning)
                    .to_uri(conv)
                    .map(|u| u.0)
                    .unwrap_or_else(|_| meaning.clone());
                Some((text.clone(), iri))
            })
            .collect();
        out.sort();
        out
    }
}
