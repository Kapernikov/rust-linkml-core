#![cfg_attr(
    not(test),
    deny(
        clippy::expect_used,
        clippy::panic,
        clippy::panic_in_result_fn,
        clippy::todo,
        clippy::unwrap_used
    )
)]

use linkml_schemaview::identifier::Identifier;
use linkml_schemaview::schemaview::{
    ClassView, SchemaView, SlotContainerMode, SlotInlineMode, SlotView,
};
use linkml_schemaview::Converter;
use serde_json::Value as JsonValue;
use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

mod constraints;
mod regex_support;
use constraints::{run_object_constraints, run_slot_constraints};

pub mod blame;
pub mod diff;
pub mod identity_lint;
#[cfg(feature = "ttl")]
pub mod rdf_export;
#[cfg(feature = "ttl")]
pub mod rdf_import;
#[cfg(feature = "ttl")]
pub mod rdf_import_store;
#[cfg(feature = "disk_graph")]
pub mod rdf_import_store_disk;
#[cfg(feature = "ttl")]
pub mod rdf_streaming;
#[cfg(feature = "ttl")]
pub mod triple_source;
#[cfg(feature = "ttl")]
pub mod turtle;
#[cfg(feature = "ttl")]
pub mod turtle_import;
pub use blame::{
    blame_map_to_paths, format_blame_map, format_blame_map_with, get_blame_info, patch_with_blame,
    record_blame_from_trace,
};
pub use diff::{
    diff, patch, Delta, DeltaOp, DiffOptions, PatchOptions, PatchTrace, OPAQUE_ANNOTATION,
};
pub use identity_lint::{lint_element_identity, lint_instance_identity};
#[derive(Debug)]
pub struct LinkMLError {
    validation_issues: Vec<ValidationResult>,
}

impl LinkMLError {
    pub fn new(validation_issues: Vec<ValidationResult>) -> Self {
        Self { validation_issues }
    }

    pub fn single(
        problem_type: ValidationProblemType,
        path: InstancePath,
        detail: impl Into<String>,
    ) -> Self {
        Self {
            validation_issues: vec![ValidationResult::error(problem_type, path, detail.into())],
        }
    }

    pub fn validation_issues(&self) -> &[ValidationResult] {
        &self.validation_issues
    }

    pub fn into_validation_issues(self) -> Vec<ValidationResult> {
        self.validation_issues
    }
}

impl std::fmt::Display for LinkMLError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(first) = self.validation_issues.first() {
            write!(f, "{}", first.detail)
        } else {
            write!(f, "LinkML error")
        }
    }
}

impl std::error::Error for LinkMLError {}

pub type LResult<T> = std::result::Result<T, LinkMLError>;

fn path_to_string(path: &[String]) -> String {
    if path.is_empty() {
        "<root>".to_string()
    } else {
        path.join(".")
    }
}

/// Convert a float to `i64` only when it is exactly integral and within range,
/// so canonicalising `114.0` to an integer never rounds or loses data. Returns
/// `None` for fractional, infinite, NaN, or out-of-range values.
fn f64_to_exact_i64(f: f64) -> Option<i64> {
    if f.fract() == 0.0 && (i64::MIN as f64..=i64::MAX as f64).contains(&f) {
        Some(f as i64)
    } else {
        None
    }
}

fn alt_names(name: &str) -> Vec<String> {
    let mut v = Vec::new();
    if name.contains('_') {
        v.push(name.replace('_', " "));
    }
    if name.contains(' ') {
        v.push(name.replace(' ', "_"));
    }
    v
}

fn slot_matches_key(slot: &SlotView, key: &str) -> bool {
    if slot.name == key || alt_names(key).contains(&slot.name) {
        return true;
    }
    if let Some(alias) = &slot.definition().alias {
        if alias == key || alt_names(key).iter().any(|a| alias == a) {
            return true;
        }
    }
    false
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ValidationSeverity {
    Fatal,
    Error,
    Warning,
    Info,
}

impl ValidationSeverity {
    pub fn is_error(&self) -> bool {
        matches!(self, Self::Fatal | Self::Error)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ValidationProblemType {
    UndeclaredSlot,
    InapplicableSlot,
    MissingSlotValue,
    SlotRangeViolation,
    MaxCountViolation,
    ParsingError,
    /// Element identity that cannot address a list unambiguously: none is
    /// declared, several declarations compete, two classes answer to one URI
    /// (opt-in, [`crate::lint_element_identity`]) — or one is declared but the
    /// data leaves it empty, so the list is positional after all (opt-in,
    /// [`crate::lint_instance_identity`]).
    AmbiguousElementIdentity,
    /// Elements of one list repeat a declared identity (opt-in,
    /// [`crate::lint_instance_identity`]).
    DuplicateElementIdentity,
}

impl ValidationProblemType {
    /// The stable machine-readable name of this variant.
    ///
    /// Every machine surface — the Python binding's `problem_type`, the CLI's
    /// JSON `type` — reports the value through this one function, so the two
    /// cannot spell the same variant differently and a variant rename cannot
    /// silently change either contract. `Debug` formatting is a Rust
    /// implementation detail and is deliberately not it.
    pub fn label(&self) -> &'static str {
        match self {
            Self::UndeclaredSlot => "undeclared_slot",
            Self::InapplicableSlot => "inapplicable_slot",
            Self::MissingSlotValue => "missing_slot_value",
            Self::SlotRangeViolation => "slot_range_violation",
            Self::MaxCountViolation => "max_count_violation",
            Self::ParsingError => "parsing_error",
            Self::AmbiguousElementIdentity => "ambiguous_element_identity",
            Self::DuplicateElementIdentity => "duplicate_element_identity",
        }
    }
}

pub type InstancePath = Vec<String>;

#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub enum ValidationValue {
    #[default]
    None,
    Node(InstancePath),
    Literal(String),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ValidationResult {
    pub problem_type: ValidationProblemType,
    pub severity: ValidationSeverity,
    pub subject: InstancePath,
    pub predicate: InstancePath,
    pub instantiates: Option<String>,
    pub node_source: InstancePath,
    pub object: ValidationValue,
    pub detail: String,
}

impl ValidationResult {
    fn from_path<C: Into<String>>(
        problem_type: ValidationProblemType,
        severity: ValidationSeverity,
        path: InstancePath,
        detail: C,
    ) -> Self {
        let subject = path.clone();
        Self {
            problem_type,
            severity,
            subject: subject.clone(),
            predicate: subject.clone(),
            instantiates: None,
            node_source: subject,
            object: ValidationValue::None,
            detail: detail.into(),
        }
    }

    pub fn error<C: Into<String>>(
        problem_type: ValidationProblemType,
        path: InstancePath,
        detail: C,
    ) -> Self {
        Self::from_path(problem_type, ValidationSeverity::Error, path, detail)
    }

    pub fn warning<C: Into<String>>(
        problem_type: ValidationProblemType,
        path: InstancePath,
        detail: C,
    ) -> Self {
        Self::from_path(problem_type, ValidationSeverity::Warning, path, detail)
    }
}

#[cfg(feature = "python")]
mod python_error {
    use super::{path_to_string, LinkMLError, ValidationResult, ValidationSeverity};
    use pyo3::exceptions::PyValueError;
    use pyo3::PyErr;

    impl From<LinkMLError> for PyErr {
        fn from(error: LinkMLError) -> Self {
            fn format_issue(issue: &ValidationResult) -> String {
                let severity = match issue.severity {
                    ValidationSeverity::Fatal => "fatal",
                    ValidationSeverity::Error => "error",
                    ValidationSeverity::Warning => "warning",
                    ValidationSeverity::Info => "info",
                };
                let path = path_to_string(&issue.subject);
                format!(
                    "- [{}::{:?}] {}: {}",
                    severity, issue.problem_type, path, issue.detail
                )
            }

            let issues = error.validation_issues();
            if issues.is_empty() {
                return PyValueError::new_err("LinkML validation error");
            }

            let details = issues.iter().map(format_issue).collect::<Vec<_>>();
            let summary = format!(
                "LinkML validation failed with {} issue(s):\n{}",
                details.len(),
                details.join("\n")
            );
            PyValueError::new_err(summary)
        }
    }
}

#[derive(Default)]
pub struct ValidationResultSink {
    validation_issues: Vec<ValidationResult>,
}

impl ValidationResultSink {
    pub fn push(&mut self, validation_issue: ValidationResult) {
        self.validation_issues.push(validation_issue);
    }

    pub fn push_error<C: Into<String>>(
        &mut self,
        problem_type: ValidationProblemType,
        path: InstancePath,
        detail: C,
    ) {
        self.push(ValidationResult::error(problem_type, path, detail));
    }

    pub fn push_warning<C: Into<String>>(
        &mut self,
        problem_type: ValidationProblemType,
        path: InstancePath,
        detail: C,
    ) {
        self.push(ValidationResult::warning(problem_type, path, detail));
    }

    pub fn has_errors(&self) -> bool {
        self.validation_issues.iter().any(|d| d.severity.is_error())
    }

    pub fn into_vec(self) -> Vec<ValidationResult> {
        self.validation_issues
    }
}

pub struct LoadResult {
    pub instance: Option<LinkMLInstance>,
    pub validation_issues: Vec<ValidationResult>,
}

impl LoadResult {
    pub fn has_errors(&self) -> bool {
        self.validation_issues.iter().any(|d| d.severity.is_error())
    }

    pub fn into_instance(self) -> std::result::Result<LinkMLInstance, LinkMLError> {
        if self.has_errors() {
            return Err(LinkMLError::new(self.validation_issues));
        }
        self.instance
            .ok_or_else(|| LinkMLError::new(self.validation_issues))
    }

    /// Return the instance even if validation errors were recorded, as long as
    /// an instance exists. Useful for workflows that can inspect diagnostics
    /// separately but still need the parsed value.
    pub fn into_instance_tolerate_errors(self) -> std::result::Result<LinkMLInstance, LinkMLError> {
        self.instance
            .ok_or_else(|| LinkMLError::new(self.validation_issues))
    }
}

#[derive(Clone)]
pub enum LinkMLInstance {
    Scalar {
        node_id: NodeId,
        value: JsonValue,
        slot: SlotView,
        class: Option<ClassView>,
        sv: SchemaView,
    },
    Null {
        node_id: NodeId,
        slot: SlotView,
        class: Option<ClassView>,
        sv: SchemaView,
    },
    List {
        node_id: NodeId,
        values: Vec<LinkMLInstance>,
        slot: SlotView,
        class: Option<ClassView>,
        sv: SchemaView,
    },
    Mapping {
        node_id: NodeId,
        values: HashMap<String, LinkMLInstance>,
        slot: SlotView,
        class: Option<ClassView>,
        sv: SchemaView,
    },
    Object {
        node_id: NodeId,
        values: HashMap<String, LinkMLInstance>,
        class: ClassView,
        sv: SchemaView,
        unknown_fields: HashMap<String, JsonValue>,
    },
}

/// Internal node identifier used for provenance and update tracking.
///
/// Node IDs are assigned to every `LinkMLInstance` node when values are constructed or
/// transformed. They exist solely as technical identifiers to help with patching and
/// provenance (for example, `PatchTrace.added`/`deleted` collect `NodeId`s of affected
/// subtrees). They are not intended to identify domain objects — for that, use LinkML
/// identifier or key slots as defined in the schema.
///
/// Important properties:
/// - Local and ephemeral: loading the same data twice will yield different `NodeId`s.
/// - Non-persistent: never serialize or expose as a model identifier.
/// - Useful for tracking modifications within a single in-memory value.
pub type NodeId = u64;

static NEXT_NODE_ID: AtomicU64 = AtomicU64::new(1);

pub(crate) fn new_node_id() -> NodeId {
    NEXT_NODE_ID.fetch_add(1, Ordering::Relaxed)
}

impl LinkMLInstance {
    /// Convert this instance into a plain `serde_json::Value` tree.
    pub fn to_json(&self) -> JsonValue {
        match self {
            LinkMLInstance::Scalar { value, .. } => value.clone(),
            LinkMLInstance::Null { .. } => JsonValue::Null,
            LinkMLInstance::List { values, .. } => {
                JsonValue::Array(values.iter().map(|v| v.to_json()).collect())
            }
            LinkMLInstance::Mapping { values, .. } => JsonValue::Object(
                values
                    .iter()
                    .map(|(k, v)| (k.clone(), v.to_json()))
                    .collect(),
            ),
            LinkMLInstance::Object {
                values,
                unknown_fields,
                ..
            } => {
                let mut map = serde_json::Map::new();
                for (k, v) in values.iter() {
                    map.insert(k.clone(), v.to_json());
                }
                for (k, v) in unknown_fields.iter() {
                    map.entry(k.clone()).or_insert_with(|| v.clone());
                }
                JsonValue::Object(map)
            }
        }
    }

    /// Recursively clone the tree but allocate a fresh [`NodeId`] at every node.
    ///
    /// Use this when the same harvested subtree needs to be emitted multiple
    /// times (e.g. a shared inlined subject denormalised across two parents).
    /// Each emitted occurrence gets unique IDs so blame/diff tracking is
    /// correct.
    pub fn clone_with_fresh_node_ids(&self) -> Self {
        match self {
            LinkMLInstance::Scalar {
                value,
                slot,
                class,
                sv,
                ..
            } => LinkMLInstance::Scalar {
                node_id: new_node_id(),
                value: value.clone(),
                slot: slot.clone(),
                class: class.clone(),
                sv: sv.clone(),
            },
            LinkMLInstance::Null {
                slot, class, sv, ..
            } => LinkMLInstance::Null {
                node_id: new_node_id(),
                slot: slot.clone(),
                class: class.clone(),
                sv: sv.clone(),
            },
            LinkMLInstance::List {
                values,
                slot,
                class,
                sv,
                ..
            } => LinkMLInstance::List {
                node_id: new_node_id(),
                values: values
                    .iter()
                    .map(|v| v.clone_with_fresh_node_ids())
                    .collect(),
                slot: slot.clone(),
                class: class.clone(),
                sv: sv.clone(),
            },
            LinkMLInstance::Mapping {
                values,
                slot,
                class,
                sv,
                ..
            } => LinkMLInstance::Mapping {
                node_id: new_node_id(),
                values: values
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone_with_fresh_node_ids()))
                    .collect(),
                slot: slot.clone(),
                class: class.clone(),
                sv: sv.clone(),
            },
            LinkMLInstance::Object {
                values,
                class,
                sv,
                unknown_fields,
                ..
            } => LinkMLInstance::Object {
                node_id: new_node_id(),
                values: values
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone_with_fresh_node_ids()))
                    .collect(),
                class: class.clone(),
                sv: sv.clone(),
                unknown_fields: unknown_fields.clone(),
            },
        }
    }

    /// Returns the internal [`NodeId`] of this node.
    ///
    /// This ID is only for internal provenance/update tracking and is not a
    /// semantic identifier of the represented object.
    pub fn node_id(&self) -> NodeId {
        match self {
            LinkMLInstance::Scalar { node_id, .. }
            | LinkMLInstance::List { node_id, .. }
            | LinkMLInstance::Mapping { node_id, .. }
            | LinkMLInstance::Object { node_id, .. }
            | LinkMLInstance::Null { node_id, .. } => *node_id,
        }
    }

    /// Return the [`SchemaView`] associated with this instance.
    pub fn schema_view(&self) -> &SchemaView {
        match self {
            LinkMLInstance::Scalar { sv, .. }
            | LinkMLInstance::Null { sv, .. }
            | LinkMLInstance::List { sv, .. }
            | LinkMLInstance::Mapping { sv, .. }
            | LinkMLInstance::Object { sv, .. } => sv,
        }
    }

    /// Returns the associated [`ClassView`], if any.
    pub fn class(&self) -> Option<&ClassView> {
        match self {
            LinkMLInstance::Object { class, .. } => Some(class),
            LinkMLInstance::Scalar { class, .. }
            | LinkMLInstance::Null { class, .. }
            | LinkMLInstance::List { class, .. }
            | LinkMLInstance::Mapping { class, .. } => class.as_ref(),
        }
    }

    /// Returns the associated [`SlotView`], if any.
    pub fn slot(&self) -> Option<&SlotView> {
        match self {
            LinkMLInstance::Scalar { slot, .. }
            | LinkMLInstance::Null { slot, .. }
            | LinkMLInstance::List { slot, .. }
            | LinkMLInstance::Mapping { slot, .. } => Some(slot),
            LinkMLInstance::Object { .. } => None,
        }
    }

    /// Navigate the value by a path of strings, where each segment is a slot
    /// name, a mapping key, or — for lists — whatever addresses an element:
    /// its identity label (identifier/key slot value, or a value derived from
    /// the range class's `unique_keys`) for a list whose elements all carry
    /// unique labels, and a numeric index otherwise.
    ///
    /// List segments resolve through the same rule [`diff()`](crate::diff())
    /// emits and [`patch()`](crate::patch()) applies, so a delta path is
    /// navigable by construction.
    /// In particular a numeric segment aimed at a label-addressed list resolves
    /// to nothing rather than to that position: when a label happens to be
    /// `"0"`, position and label name different elements, and guessing which
    /// one was meant is exactly the wrong-element hazard.
    ///
    /// Returns `Some(&LinkMLInstance)` if the full path can be resolved, otherwise `None`.
    pub fn navigate_path<I, S>(&self, path: I) -> Option<&LinkMLInstance>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let mut current: &LinkMLInstance = self;
        for seg in path {
            let key = seg.as_ref();
            match current {
                LinkMLInstance::Object { values, .. } => {
                    current = values.get(key)?;
                }
                LinkMLInstance::List { values, .. } => {
                    current = values.get(diff::resolve_list_segment(values, key)?)?;
                }
                LinkMLInstance::Mapping { values, .. } => {
                    current = values.get(key)?;
                }
                LinkMLInstance::Scalar { .. } => return None,
                LinkMLInstance::Null { .. } => return None,
            }
        }
        Some(current)
    }

    /// Compare two LinkMLInstance instances for semantic equality per the
    /// LinkML Instances specification (Identity conditions).
    ///
    /// Key points implemented:
    /// - Null equals Null.
    /// - Scalars: equal iff same underlying atomic value and compatible typed context
    ///   (same Enum range when present; otherwise same TypeDefinition range name when present).
    /// - Lists: equal iff same length and pairwise equal in order.
    /// - Mappings: equal iff same keys and values equal for each key (order-insensitive).
    /// - Objects: equal iff same instantiated class (by identity) and slot assignments match; when
    ///   `treat_missing_as_null` is true, Null is treated as omitted (normalized), otherwise Null is
    ///   distinct from missing.
    pub fn equals(&self, other: &LinkMLInstance, treat_missing_as_null: bool) -> bool {
        use LinkMLInstance::*;
        match (self, other) {
            (Null { .. }, Null { .. }) => true,
            (
                Scalar {
                    value: v1,
                    slot: s1,
                    ..
                },
                Scalar {
                    value: v2,
                    slot: s2,
                    ..
                },
            ) => {
                // If either slot has an enum range, both must and enum names must match
                let e1 = s1.get_range_enum();
                let e2 = s2.get_range_enum();
                if e1.is_some() || e2.is_some() {
                    match (e1, e2) {
                        (Some(ev1), Some(ev2)) => {
                            if ev1.schema_id() != ev2.schema_id() || ev1.name() != ev2.name() {
                                return false;
                            }
                        }
                        _ => return false,
                    }
                } else {
                    // Compare type ranges if explicitly set on both
                    let t1 = s1.definition().range.as_ref();
                    let t2 = s2.definition().range.as_ref();
                    if let (Some(r1), Some(r2)) = (t1, t2) {
                        if r1 != r2 {
                            return false;
                        }
                    }
                }
                v1 == v2
            }
            (List { values: a, .. }, List { values: b, .. }) => {
                if a.len() != b.len() {
                    return false;
                }
                for (x, y) in a.iter().zip(b.iter()) {
                    if !x.equals(y, treat_missing_as_null) {
                        return false;
                    }
                }
                true
            }
            (Mapping { values: a, .. }, Mapping { values: b, .. }) => {
                if a.len() != b.len() {
                    return false;
                }
                for (k, va) in a.iter() {
                    match b.get(k) {
                        Some(vb) => {
                            if !va.equals(vb, treat_missing_as_null) {
                                return false;
                            }
                        }
                        None => return false,
                    }
                }
                true
            }
            (
                Object {
                    values: a,
                    class: ca,
                    sv: sva,
                    unknown_fields: unknown_a,
                    ..
                },
                Object {
                    values: b,
                    class: cb,
                    sv: svb,
                    unknown_fields: unknown_b,
                    ..
                },
            ) => {
                // Compare class identity via canonical URIs if possible
                let ida = ca.canonical_uri();
                let idb = cb.canonical_uri();
                let class_equal = if let Some(conv) = sva.converter_for_schema(ca.schema_id()) {
                    // Use 'sva' for comparison; identifiers are global across schemas
                    sva.identifier_equals(&ida, &idb, &conv).unwrap_or(false)
                } else if let Some(conv) = svb.converter_for_schema(cb.schema_id()) {
                    svb.identifier_equals(&ida, &idb, &conv).unwrap_or(false)
                } else {
                    ca.name() == cb.name()
                };
                if !class_equal {
                    return false;
                }

                if unknown_a.len() != unknown_b.len() {
                    return false;
                }
                for (k, va) in unknown_a.iter() {
                    match unknown_b.get(k) {
                        Some(vb) if vb == va => {}
                        _ => return false,
                    }
                }

                if treat_missing_as_null {
                    // Normalize conceptually by ignoring entries whose value is Null
                    let count_a = a.iter().filter(|(_, v)| !matches!(v, Null { .. })).count();
                    let count_b = b.iter().filter(|(_, v)| !matches!(v, Null { .. })).count();
                    if count_a != count_b {
                        return false;
                    }
                    for (k, va) in a.iter().filter(|(_, v)| !matches!(v, Null { .. })) {
                        match b.get(k) {
                            Some(vb) => {
                                if matches!(vb, Null { .. }) {
                                    return false;
                                }
                                if !va.equals(vb, treat_missing_as_null) {
                                    return false;
                                }
                            }
                            None => return false,
                        }
                    }
                    // Ensure b has no extra non-null keys not in a
                    for (k, _vb) in b.iter().filter(|(_, v)| !matches!(v, Null { .. })) {
                        match a.get(k) {
                            Some(va) => {
                                if matches!(va, Null { .. }) {
                                    return false;
                                }
                            }
                            None => return false,
                        }
                    }
                    true
                } else {
                    if a.len() != b.len() {
                        return false;
                    }
                    for (k, va) in a.iter() {
                        match b.get(k) {
                            Some(vb) => {
                                if !va.equals(vb, treat_missing_as_null) {
                                    return false;
                                }
                            }
                            None => return false,
                        }
                    }
                    true
                }
            }
            _ => false,
        }
    }
    fn find_scalar_slot_for_inlined_map(
        class: &ClassView,
        key_slot_name: &str,
    ) -> Option<SlotView> {
        for s in class.slots() {
            if s.name == key_slot_name {
                continue;
            }
            let def = s.definition();
            if def.multivalued.unwrap_or(false) {
                continue;
            }
            if !s.is_range_scalar() {
                continue;
            }
            return Some(s.clone());
        }
        None
    }
    fn select_class(
        map: &serde_json::Map<String, JsonValue>,
        base: &ClassView,
        sv: &SchemaView,
        conv: &Converter,
    ) -> ClassView {
        let descendants = base.get_descendants(true, false).unwrap_or_default();
        let mut cand_refs: Vec<&ClassView> = vec![base];
        for d in &descendants {
            cand_refs.push(d);
        }

        let mut preferred: Option<&ClassView> = None;
        if let Some(td_slot) = base
            .slots()
            .iter()
            .find(|s| s.definition().designates_type.unwrap_or(false))
            .map(|s| s.definition())
        {
            if let Some(JsonValue::String(tv)) = map.get(&td_slot.name) {
                for c in &cand_refs {
                    if let Ok(vals) = c.get_accepted_type_designator_values(td_slot, conv) {
                        if vals.iter().any(|v| v.to_string() == *tv) {
                            preferred = Some(*c);
                            break;
                        }
                    }
                }
            }
        }
        if let Some(p) = preferred {
            return p.clone();
        }

        let mut soft_match: Option<(&ClassView, usize)> = None;
        for c in &cand_refs {
            let mut tmp_diags = ValidationResultSink::default();
            if let Ok(_tmp) =
                Self::parse_object_fixed_class(map.clone(), c, sv, conv, Vec::new(), &mut tmp_diags)
            {
                let diag_vec = tmp_diags.into_vec();
                let has_blocking_error = diag_vec.iter().any(|d| {
                    d.severity.is_error() && d.problem_type != ValidationProblemType::UndeclaredSlot
                });
                if has_blocking_error {
                    continue;
                }
                let unknown_count = diag_vec
                    .iter()
                    .filter(|d| {
                        d.severity.is_error()
                            && d.problem_type == ValidationProblemType::UndeclaredSlot
                    })
                    .count();
                if unknown_count == 0 {
                    return (*c).clone();
                }
                match soft_match {
                    Some((_, best)) if best <= unknown_count => {}
                    _ => soft_match = Some((*c, unknown_count)),
                }
            }
        }
        soft_match
            .map(|(c, _)| c.clone())
            .unwrap_or_else(|| base.clone())
    }

    /// Rewrite an already-present type designator value to the canonical value
    /// of the class that was actually selected — identity compares meaning, not
    /// spelling (spec addendum rule 2, finding D1).
    ///
    /// A designator says "this element is a `Circle`", and a `uriorcurie`-ranged
    /// one can say it as `canon:Circle`, as the expanded IRI, or not at all.
    /// Stored verbatim, those are three different strings: the same element
    /// authored by two producers diffs as a whole-element Remove + Add, and the
    /// RDF loader — which never harvests the designator predicate and always
    /// fills it from the class ([`Self::populate_type_designator`], called from
    /// `turtle_import`) — disagrees with the JSON loader on every document.
    /// Canonicalising at the boxing chokepoint, exactly as
    /// [`Self::coerce_scalar_to_range`] does for the int/float ambiguity, makes
    /// the representation canonical before any diff/equals/patch/`to_json` sees
    /// it, and makes the two loaders agree by construction.
    ///
    /// It is also what makes `diff`'s changed-key check ("a changed designator
    /// means a different class") unconditionally true rather than true only for
    /// consistently-spelled data.
    ///
    /// A supplied value that is *no* accepted designator value of the selected
    /// class (a bare class name against a `uri` range, a typo, a stale class
    /// name) is data the loader cannot honour. Following the loader-tolerance
    /// precedent it is a **warning**, not an error: the instance still loads and
    /// the slot carries the canonical value of the class `select_class` picked.
    ///
    /// Only a `Scalar` is rewritten. An explicit `null` and a structurally wrong
    /// value (list, object) are left for the range checks that already ran to
    /// describe; replacing them would hide the shape of the offending data.
    fn canonicalize_type_designator(
        values: &mut HashMap<String, LinkMLInstance>,
        class: &ClassView,
        conv: &Converter,
        path: &[String],
        validation_issues: &mut ValidationResultSink,
    ) {
        let Some(type_slot_def) = class.get_type_designator_slot() else {
            return;
        };
        // Cheapest checks first: the two commonest cases — the designator is
        // absent (the RDF-shaped and the omit-it-and-let-the-loader-fill-it
        // documents), or it is already canonical (every document a previous
        // load emitted) — must not pay for a URI computation. Neither
        // `get_type_designator_value` nor `get_accepted_type_designator_values`
        // is cached: each walks `type_ancestors` and expands URIs through the
        // converter, and this runs once per object built.
        let Some(LinkMLInstance::Scalar { value, .. }) = values.get_mut(&type_slot_def.name) else {
            return;
        };
        let supplied = match &*value {
            JsonValue::String(s) => s.clone(),
            other => other.to_string(),
        };
        let Ok(canonical) = class.get_type_designator_value(type_slot_def, conv) else {
            return;
        };
        let canonical = canonical.to_string();
        if supplied == canonical {
            return;
        }
        // Only a value that differs from the canonical one is worth the second
        // walk. An `Err` here means the accepted set is unknown, not empty:
        // warning-and-rewriting on it would punish the data for a schema the
        // view cannot resolve, so leave the value alone — the same posture the
        // canonical-value line above takes.
        let Ok(accepted) = class.get_accepted_type_designator_values(type_slot_def, conv) else {
            return;
        };
        if !accepted.iter().any(|v| v.to_string() == supplied) {
            let mut p = path.to_vec();
            p.push(type_slot_def.name.clone());
            validation_issues.push_warning(
                ValidationProblemType::SlotRangeViolation,
                p,
                format!(
                    "type designator value `{supplied}` is not an accepted designator \
                     value for class `{}`; stored as `{canonical}`",
                    class.name()
                ),
            );
        }
        *value = JsonValue::String(canonical);
    }

    /// Hear what an inlined-dict entry's key says when the payload also states
    /// it (spec addendum rule 5, finding D5).
    ///
    /// Called only when the payload *did* supply the key slot — the omitted
    /// case is filled from the dict key by the caller, and any complaint about
    /// that injected value is then [`Self::canonicalize_type_designator`]'s to
    /// make, in its own words, on the value that was actually stored.
    ///
    /// Two things are checked, and both are warnings, never errors: the
    /// document loads, because the LinkML `inlined` contract makes both halves
    /// legal on their own and only their *combination* is contradictory.
    ///
    /// 1. **Divergence.** The dict key and the element's key-slot value say
    ///    different things about the same slot. The payload is the element's
    ///    data and is what gets stored; the dict key is the address and is what
    ///    the mapping stays keyed by (`parse_mapping_slot` inserts under the raw
    ///    key). Neither can be dropped without losing information the document
    ///    contains, so both are named and the author decides.
    ///
    ///    The comparison is against the value **as stored**, which is why this
    ///    runs after [`Self::canonicalize_type_designator`] and
    ///    [`Self::populate_type_designator`] rather than against the raw
    ///    payload. A designator payload is rewritten to its class's canonical
    ///    spelling at load, so comparing the raw payload would call
    ///    `{"<class_uri>": {"typeURI": "<native URI>"}}` a divergence while the
    ///    two ends of the message — key and "the stored value" — had in fact
    ///    already been made to agree, and reloading the emitted document would
    ///    be silent. The rule is therefore: warn when the entry, *as it will be
    ///    written back out*, contradicts the key it is written under.
    ///
    ///    Both sides go through [`crate::diff::canonical_identity_component`],
    ///    the same function every identity label is built from, so a CURIE key
    ///    and an expanded stored value are one identity (rule 2) rather than a
    ///    divergence. What survives is the genuinely contradictory case:
    ///    `{"<native URI>": {"typeURI": "<class_uri>"}}`, where both spellings
    ///    name one class but only the payload is canonicalised, so the loaded
    ///    entry really is addressed by one IRI and carries another. That is
    ///    asset360's committed `SpotLocation_coordinates` shape.
    ///
    /// 2. **An unaccepted designator key.** When the key slot *is* the class's
    ///    type designator the dict key names the entry's class, and a key that
    ///    is no accepted designator value of the class actually selected names
    ///    nothing. Before this rule the case was silent twice over: nothing read
    ///    the key, and `populate_type_designator` filled the slot from the range
    ///    class, so the entry looked complete.
    ///
    /// The key slot itself is [`ClassView::key_or_identifier_slot`] on the
    /// slot's **range** class, not on the class finally selected: the key has to
    /// be read (and injected) before `select_class` can run, so a key declared
    /// only by a subclass — via `slot_usage` on a descendant — is chicken-and-egg
    /// and gets neither the injection nor these checks.
    fn reconcile_dict_key_with_payload(
        entry_key: &str,
        key_slot: &SlotView,
        values: &HashMap<String, LinkMLInstance>,
        selected: &ClassView,
        conv: &Converter,
        path: &[String],
        validation_issues: &mut ValidationResultSink,
    ) {
        let mut p = path.to_vec();
        p.push(key_slot.name.clone());

        if key_slot.definition().designates_type == Some(true) {
            // Deliberately a raw string compare, unlike the divergence half
            // below: the accepted set is what `canonicalize_type_designator`
            // (Task 12, rule 2) matches against, verbatim, and a key that this
            // check waved through on an IRI-equal-but-differently-spelled basis
            // would then be rewritten by that function and reported by *it*
            // instead — two voices for one fact.
            //
            // `Err` means the accepted set is unknown, not empty — the same
            // posture `canonicalize_type_designator` takes: do not punish the
            // data for a schema the view cannot resolve.
            if let Some(td) = selected.get_type_designator_slot() {
                if let Ok(accepted) = selected.get_accepted_type_designator_values(td, conv) {
                    if !accepted.iter().any(|v| v.to_string() == entry_key) {
                        validation_issues.push_warning(
                            ValidationProblemType::SlotRangeViolation,
                            p.clone(),
                            format!(
                                "inlined-dict key `{entry_key}` is not an accepted designator \
                                 value for class `{}`",
                                selected.name()
                            ),
                        );
                    }
                }
            }
        }

        // A `Null` and a structurally wrong value (list, object) are left for
        // the range checks that already ran to describe; calling them a
        // divergence would say the wrong thing about the wrong problem.
        let Some(LinkMLInstance::Scalar { value, .. }) = values.get(&key_slot.name) else {
            return;
        };
        let stored = match value {
            JsonValue::String(s) => s.clone(),
            other => other.to_string(),
        };
        if crate::diff::canonical_identity_component(entry_key, key_slot)
            == crate::diff::canonical_identity_component(&stored, key_slot)
        {
            return;
        }
        validation_issues.push_warning(
            ValidationProblemType::SlotRangeViolation,
            p,
            format!(
                "inlined-dict key `{entry_key}` disagrees with `{stored}`, the value stored in \
                 key slot `{}`; the payload is the element's data and the dict key is its \
                 address, so the entry keeps both and stays addressed by its dict key",
                key_slot.name
            ),
        );
    }

    /// The dict key of an inlined mapping entry, as the value its key slot
    /// takes when the payload omitted it (spec addendum rule 5).
    ///
    /// A JSON object key is a string and is injected as one. A numerically
    /// ranged key slot therefore receives the string spelling of its key: the
    /// loader runs no scalar type check (see [`crate::constraints`] — enum,
    /// regex and min/max only), and inventing a second, untested numeric parse
    /// here would be a coercion path of its own rather than this rule.
    fn dict_key_value(entry_key: &str) -> JsonValue {
        JsonValue::String(entry_key.to_string())
    }

    /// If the class has a `designates_type` slot and the values map does not
    /// already contain it, insert a Scalar value with the class's type
    /// designator value. This ensures round-trip fidelity for formats like
    /// JSON that lack an intrinsic typing mechanism (unlike RDF's rdf:type).
    ///
    /// The complement of [`Self::canonicalize_type_designator`], which settles
    /// the case where the data *did* supply a value; JSON boxing runs both, RDF
    /// harvesting only ever needs this one (it skips the designator predicate).
    fn populate_type_designator(
        values: &mut HashMap<String, LinkMLInstance>,
        class: &ClassView,
        sv: &SchemaView,
        conv: &Converter,
    ) {
        if let Some(type_slot_def) = class.get_type_designator_slot() {
            let slot_name = &type_slot_def.name;
            if !values.contains_key(slot_name) {
                if let Ok(id) = class.get_type_designator_value(type_slot_def, conv) {
                    // Find the SlotView for the type designator slot.
                    if let Some(slot_view) = class
                        .slots()
                        .iter()
                        .find(|s| s.definition().designates_type.unwrap_or(false))
                    {
                        values.insert(
                            slot_name.clone(),
                            LinkMLInstance::Scalar {
                                node_id: new_node_id(),
                                value: JsonValue::String(id.to_string()),
                                slot: slot_view.clone(),
                                class: Some(class.clone()),
                                sv: sv.clone(),
                            },
                        );
                    }
                }
            }
        }
    }

    fn parse_object_fixed_class(
        map: serde_json::Map<String, JsonValue>,
        class: &ClassView,
        sv: &SchemaView,
        conv: &Converter,
        path: Vec<String>,
        validation_issues: &mut ValidationResultSink,
    ) -> LResult<Self> {
        let mut values = HashMap::new();
        for (k, v) in map.into_iter() {
            let slot_ref = class
                .slots()
                .iter()
                .find(|s| slot_matches_key(s, &k))
                .cloned();
            let mut p = path.clone();
            p.push(k.clone());
            let key_name = slot_ref
                .as_ref()
                .map(|s| s.name.clone())
                .unwrap_or_else(|| k.clone());
            values.insert(
                key_name,
                Self::from_json_internal(
                    v,
                    class.clone(),
                    slot_ref,
                    sv,
                    conv,
                    false,
                    p,
                    validation_issues,
                )?,
            );
        }
        run_object_constraints(
            class,
            &values,
            &HashMap::new(),
            path.clone(),
            validation_issues,
        );
        Self::canonicalize_type_designator(&mut values, class, conv, &path, validation_issues);
        Self::populate_type_designator(&mut values, class, sv, conv);
        Ok(LinkMLInstance::Object {
            node_id: new_node_id(),
            values,
            class: class.clone(),
            sv: sv.clone(),
            unknown_fields: HashMap::new(),
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn parse_list_slot(
        value: JsonValue,
        class: ClassView,
        sl: SlotView,
        sv: &SchemaView,
        conv: &Converter,
        inside_list: bool,
        path: Vec<String>,
        validation_issues: &mut ValidationResultSink,
    ) -> LResult<Self> {
        match (inside_list, value) {
            (false, JsonValue::Array(arr)) => {
                let mut values = Vec::new();
                for (i, v) in arr.into_iter().enumerate() {
                    let mut p = path.clone();
                    p.push(i.to_string());
                    values.push(Self::build_list_item_for_slot(
                        &sl,
                        Some(&class),
                        v,
                        sv,
                        conv,
                        p,
                        validation_issues,
                    )?);
                }
                Ok(LinkMLInstance::List {
                    node_id: new_node_id(),
                    values,
                    slot: sl.clone(),
                    class: Some(class.clone()),
                    sv: sv.clone(),
                })
            }
            // Preserve explicit null as a Null value for list-valued slot
            (false, JsonValue::Null) => Ok(LinkMLInstance::Null {
                node_id: new_node_id(),
                slot: sl.clone(),
                class: Some(class.clone()),
                sv: sv.clone(),
            }),
            (false, other) => {
                let msg = format!(
                    "expected list for slot `{}`, found {:?} at {}",
                    sl.name,
                    other,
                    path_to_string(&path)
                );
                Err(LinkMLError::single(
                    ValidationProblemType::SlotRangeViolation,
                    path.clone(),
                    msg,
                ))
            }
            (true, other) => Ok(LinkMLInstance::Scalar {
                node_id: new_node_id(),
                value: Self::coerce_scalar_to_range(other, Some(&sl)),
                slot: sl.clone(),
                class: Some(class.clone()),
                sv: sv.clone(),
            }),
        }
    }

    fn parse_mapping_slot(
        value: JsonValue,
        class: Option<ClassView>,
        sl: &SlotView,
        sv: &SchemaView,
        conv: &Converter,
        path: Vec<String>,
        validation_issues: &mut ValidationResultSink,
    ) -> LResult<Self> {
        match value {
            JsonValue::Object(map) => {
                let mut values = HashMap::new();
                for (k, v) in map.into_iter() {
                    let child = Self::build_mapping_entry_for_slot(
                        sl,
                        &k,
                        v,
                        sv,
                        conv,
                        {
                            let mut p = path.clone();
                            p.push(k.clone());
                            p
                        },
                        validation_issues,
                    )?;
                    values.insert(k, child);
                }
                Ok(LinkMLInstance::Mapping {
                    node_id: new_node_id(),
                    values,
                    slot: sl.clone(),
                    class: class.clone(),
                    sv: sv.clone(),
                })
            }
            // Preserve explicit null as a Null value for mapping-valued slot
            JsonValue::Null => Ok(LinkMLInstance::Null {
                node_id: new_node_id(),
                slot: sl.clone(),
                class: class.clone(),
                sv: sv.clone(),
            }),
            other => {
                let msg = format!(
                    "expected mapping for slot `{}`, found {:?} at {}",
                    sl.name,
                    other,
                    path_to_string(&path)
                );
                Err(LinkMLError::single(
                    ValidationProblemType::SlotRangeViolation,
                    path.clone(),
                    msg,
                ))
            }
        }
    }

    fn parse_array_value(
        arr: Vec<JsonValue>,
        class: ClassView,
        slot: Option<SlotView>,
        sv: &SchemaView,
        conv: &Converter,
        path: Vec<String>,
        validation_issues: &mut ValidationResultSink,
    ) -> LResult<Self> {
        let sl = slot.ok_or_else(|| {
            LinkMLError::single(
                ValidationProblemType::ParsingError,
                path.clone(),
                format!(
                    "list requires slot at {} for class={}",
                    path_to_string(&path),
                    class.name()
                ),
            )
        })?;
        let mut values = Vec::new();
        for (i, v) in arr.into_iter().enumerate() {
            let mut p = path.clone();
            p.push(i.to_string());
            values.push(Self::build_list_item_for_slot(
                &sl,
                Some(&class),
                v,
                sv,
                conv,
                p,
                validation_issues,
            )?);
        }
        Ok(LinkMLInstance::List {
            node_id: new_node_id(),
            values,
            slot: sl,
            class: Some(class),
            sv: sv.clone(),
        })
    }

    fn parse_object_value(
        map: serde_json::Map<String, JsonValue>,
        class: ClassView,
        slot: Option<SlotView>,
        sv: &SchemaView,
        conv: &Converter,
        path: Vec<String>,
        validation_issues: &mut ValidationResultSink,
    ) -> LResult<Self> {
        let base_class = match slot {
            Some(sl) => sl.get_range_class(),
            None => Some(class.clone()),
        }
        .ok_or_else(|| {
            LinkMLError::single(
                ValidationProblemType::ParsingError,
                path.clone(),
                format!("object requires class or slot at {}", path_to_string(&path)),
            )
        })?;
        let chosen = Self::select_class(&map, &base_class, sv, conv);

        let mut values = HashMap::new();
        let mut unknown_fields = HashMap::new();
        for (k, v) in map.into_iter() {
            let slot_tmp: Option<SlotView> = chosen
                .slots()
                .iter()
                .find(|s| slot_matches_key(s, &k))
                .cloned();
            let mut p = path.clone();
            p.push(k.clone());
            let key_name = slot_tmp
                .as_ref()
                .map(|s| s.name.clone())
                .unwrap_or_else(|| k.clone());
            if let Some(slot_view) = slot_tmp {
                values.insert(
                    key_name,
                    Self::from_json_internal(
                        v,
                        chosen.clone(),
                        Some(slot_view),
                        sv,
                        conv,
                        false,
                        p,
                        validation_issues,
                    )?,
                );
            } else {
                unknown_fields.insert(key_name, v);
            }
        }
        run_object_constraints(
            &chosen,
            &values,
            &unknown_fields,
            path.clone(),
            validation_issues,
        );
        Self::canonicalize_type_designator(&mut values, &chosen, conv, &path, validation_issues);
        Self::populate_type_designator(&mut values, &chosen, sv, conv);
        Ok(LinkMLInstance::Object {
            node_id: new_node_id(),
            values,
            class: chosen,
            sv: sv.clone(),
            unknown_fields,
        })
    }

    /// Canonicalise a numeric JSON scalar to the representation its declared
    /// range expects, so the same value boxes identically regardless of which
    /// path produced it.
    ///
    /// `114` and `114.0` denote the same number but box to distinct
    /// `serde_json::Number` variants and compare unequal, so a value authored
    /// server-side and the same value round-tripped through a layer with no
    /// int/float distinction (notably a browser — JavaScript collapses `114.0`
    /// to `114`) would diff as a spurious update. Coercing at this single boxing
    /// chokepoint makes the representation canonical before any diff/equals/patch
    /// sees it. See design doc `57-int-float-spurious-delta`.
    ///
    /// Only the int/float distinction is representationally ambiguous across
    /// JSON producers; every other scalar range has a single unambiguous JSON
    /// form and is left untouched.
    fn coerce_scalar_to_range(value: JsonValue, slot: Option<&SlotView>) -> JsonValue {
        let (JsonValue::Number(n), Some(slot)) = (&value, slot) else {
            return value;
        };
        if slot.is_range_floating_point() {
            // Integer literal for a real-valued range -> float, so it compares
            // equal to a server-authored float (114 == 114.0).
            if !n.is_f64() {
                if let Some(f) = n.as_f64().and_then(serde_json::Number::from_f64) {
                    return JsonValue::Number(f);
                }
            }
        } else if slot.is_range_integer() {
            // Whole float literal for an integer range -> integer. Fractional or
            // out-of-range values are left intact so validation can flag them
            // rather than silently losing data.
            if n.is_f64() {
                if let Some(i) = n.as_f64().and_then(f64_to_exact_i64) {
                    return JsonValue::Number(i.into());
                }
            }
        }
        value
    }

    fn parse_scalar_value(
        value: JsonValue,
        class: ClassView,
        slot: Option<SlotView>,
        sv: &SchemaView,
        path: Vec<String>,
        validation_issues: &mut ValidationResultSink,
    ) -> LResult<Self> {
        let sl = slot.ok_or_else(|| {
            let classview_name = class.name().to_string();
            LinkMLError::single(
                ValidationProblemType::ParsingError,
                path.clone(),
                format!(
                    "scalar requires slot for at {} {}",
                    path_to_string(&path),
                    classview_name
                ),
            )
        })?;
        let value = Self::coerce_scalar_to_range(value, Some(&sl));
        run_slot_constraints(Some(&class), &sl, &value, path.clone(), validation_issues);
        if value.is_null() {
            Ok(LinkMLInstance::Null {
                node_id: new_node_id(),
                slot: sl,
                class: Some(class.clone()),
                sv: sv.clone(),
            })
        } else {
            Ok(LinkMLInstance::Scalar {
                node_id: new_node_id(),
                value,
                slot: sl,
                class: Some(class.clone()),
                sv: sv.clone(),
            })
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn from_json_internal(
        value: JsonValue,
        classview: ClassView,
        slot: Option<SlotView>,
        sv: &SchemaView,
        conv: &Converter,
        inside_list: bool,
        path: Vec<String>,
        validation_issues: &mut ValidationResultSink,
    ) -> LResult<Self> {
        if let Some(ref sl) = slot {
            let container_mode = sl.determine_slot_container_mode();
            match container_mode {
                SlotContainerMode::List => {
                    return Self::parse_list_slot(
                        value,
                        classview,
                        sl.clone(),
                        sv,
                        conv,
                        inside_list,
                        path,
                        validation_issues,
                    );
                }
                SlotContainerMode::Mapping => {
                    return Self::parse_mapping_slot(
                        value,
                        Some(classview),
                        sl,
                        sv,
                        conv,
                        path,
                        validation_issues,
                    );
                }
                SlotContainerMode::SingleValue => {}
            }
        }

        match value {
            JsonValue::Array(arr) => {
                Self::parse_array_value(arr, classview, slot, sv, conv, path, validation_issues)
            }
            JsonValue::Object(map) => {
                Self::parse_object_value(map, classview, slot, sv, conv, path, validation_issues)
            }
            other => Self::parse_scalar_value(other, classview, slot, sv, path, validation_issues),
        }
    }

    pub fn from_json(
        value: JsonValue,
        class: ClassView,
        slot: Option<SlotView>,
        sv: &SchemaView,
        conv: &Converter,
        inside_list: bool,
    ) -> LoadResult {
        let mut sink = ValidationResultSink::default();
        let result = Self::from_json_internal(
            value,
            class,
            slot,
            sv,
            conv,
            inside_list,
            Vec::new(),
            &mut sink,
        );
        let mut validation_issues = sink.into_vec();
        let instance = match result {
            Ok(v) => Some(v),
            Err(err) => {
                validation_issues.extend(err.into_validation_issues());
                None
            }
        };
        LoadResult {
            instance,
            validation_issues,
        }
    }

    // Shared builders (used by loaders and patch logic)
    pub(crate) fn build_list_item_for_slot(
        list_slot: &SlotView,
        list_class: Option<&ClassView>,
        value: JsonValue,
        sv: &SchemaView,
        conv: &Converter,
        path: Vec<String>,
        validation_issues: &mut ValidationResultSink,
    ) -> LResult<Self> {
        let class_range: Option<ClassView> = list_slot.get_range_class();
        let inline_mode = list_slot.determine_slot_inline_mode();
        let slot_for_item = match inline_mode {
            SlotInlineMode::Inline => None,
            SlotInlineMode::Primitive | SlotInlineMode::Reference => Some(list_slot.clone()),
        };

        let mut v_transformed = value;
        if matches!(inline_mode, SlotInlineMode::Inline) {
            if let (Some(cr), JsonValue::String(s)) = (class_range.as_ref(), &v_transformed) {
                if let Some(id_slot) = cr.identifier_slot() {
                    let mut m = serde_json::Map::new();
                    m.insert(id_slot.name.clone(), JsonValue::String(s.clone()));
                    v_transformed = JsonValue::Object(m);
                }
            }
        }
        Self::from_json_internal(
            v_transformed,
            class_range
                .as_ref()
                .or(list_class)
                .cloned()
                .ok_or_else(|| {
                    LinkMLError::single(
                        ValidationProblemType::ParsingError,
                        path.clone(),
                        "list item class context",
                    )
                })?,
            slot_for_item,
            sv,
            conv,
            true,
            path,
            validation_issues,
        )
    }

    /// Build one entry of an inlined mapping.
    ///
    /// `entry_key` is the entry's dict key, which the LinkML `inlined` contract
    /// makes the element's key/identifier *value* — real data, not just an
    /// address (spec addendum rule 5, finding D5). It is injected into the key
    /// slot when the payload omits it, which happens before class selection (so
    /// a designator-keyed dict selects the class its key names), before the
    /// object constraints run (so a `required` key slot is satisfied by the key
    /// the document did supply, instead of erroring on legal data), and before
    /// [`Self::canonicalize_type_designator`] (so an injected designator value
    /// is canonicalised exactly as a payload-supplied one is). When the payload
    /// does supply it, [`Self::reconcile_dict_key_with_payload`] speaks.
    ///
    /// The key slot is read from the slot's **range** class only. It has to be:
    /// the key is injected *before* `select_class` runs, so consulting the
    /// selected class would be circular. A key declared solely by a descendant
    /// — `slot_usage: {k: {key: true}}` on a subclass, the range class having no
    /// key of its own — therefore gets neither the injection nor the checks.
    pub(crate) fn build_mapping_entry_for_slot(
        map_slot: &SlotView,
        entry_key: &str,
        value: JsonValue,
        sv: &SchemaView,
        conv: &Converter,
        path: Vec<String>,
        validation_issues: &mut ValidationResultSink,
    ) -> LResult<Self> {
        let range_cv = map_slot
            .definition()
            .range
            .as_ref()
            .and_then(|r| sv.get_class(&Identifier::new(r), conv).ok().flatten())
            .ok_or_else(|| {
                LinkMLError::single(
                    ValidationProblemType::ParsingError,
                    path.clone(),
                    format!(
                        "mapping slot must have class range at {}",
                        path_to_string(&path)
                    ),
                )
            })?;
        match value {
            JsonValue::Object(mut m) => {
                let key_slot = range_cv.key_or_identifier_slot().cloned();
                // Alias-aware, because the payload may spell the slot by any
                // name `slot_matches_key` accepts — the same lookup the child
                // loop below uses, so "did the payload supply it?" and "which
                // entry is it?" cannot disagree.
                let payload_states_key = key_slot
                    .as_ref()
                    .is_some_and(|ks| m.keys().any(|ck| slot_matches_key(ks, ck)));
                if let Some(ks) = &key_slot {
                    if !payload_states_key {
                        m.insert(ks.name.clone(), Self::dict_key_value(entry_key));
                    }
                }
                let selected = Self::select_class(&m, &range_cv, sv, conv);
                let mut child_values = HashMap::new();
                for (ck, cv) in m.into_iter() {
                    let slot_tmp = selected
                        .slots()
                        .iter()
                        .find(|s| slot_matches_key(s, &ck))
                        .cloned();
                    let mut p = path.clone();
                    p.push(ck.clone());
                    let key_name = slot_tmp
                        .as_ref()
                        .map(|s| s.name.clone())
                        .unwrap_or_else(|| ck.clone());
                    child_values.insert(
                        key_name,
                        Self::from_json_internal(
                            cv,
                            selected.clone(),
                            slot_tmp,
                            sv,
                            conv,
                            false,
                            p,
                            validation_issues,
                        )?,
                    );
                }
                run_object_constraints(
                    &selected,
                    &child_values,
                    &HashMap::new(),
                    path.clone(),
                    validation_issues,
                );
                Self::canonicalize_type_designator(
                    &mut child_values,
                    &selected,
                    conv,
                    &path,
                    validation_issues,
                );
                Self::populate_type_designator(&mut child_values, &selected, sv, conv);
                // After both designator passes, so the comparison is against
                // the value the entry will actually carry out. Skipped when the
                // key was injected: the entry then agrees with its key by
                // construction, and a rejected *injected* designator value is
                // `canonicalize_type_designator`'s to report.
                if let (Some(ks), true) = (key_slot.as_ref(), payload_states_key) {
                    Self::reconcile_dict_key_with_payload(
                        entry_key,
                        ks,
                        &child_values,
                        &selected,
                        conv,
                        &path,
                        validation_issues,
                    );
                }
                Ok(LinkMLInstance::Object {
                    node_id: new_node_id(),
                    values: child_values,
                    class: selected,
                    sv: sv.clone(),
                    unknown_fields: HashMap::new(),
                })
            }
            other => {
                let key_slot = range_cv.key_or_identifier_slot().cloned();
                let key_slot_name = key_slot.as_ref().map(|s| s.name.as_str()).unwrap_or("");
                let scalar_slot = Self::find_scalar_slot_for_inlined_map(&range_cv, key_slot_name)
                    .ok_or_else(|| {
                        LinkMLError::single(
                            ValidationProblemType::ParsingError,
                            path.clone(),
                            format!(
                                "no scalar slot available for inlined mapping at {}",
                                path_to_string(&path)
                            ),
                        )
                    })?;
                // The compact form states exactly two things: the dict key and
                // one scalar. Both are named here, because either can be the
                // class's *designator* and so name the entry's class.
                //
                // `find_scalar_slot_for_inlined_map` picks the first non-key
                // scalar slot, which can be the designator: then
                // `{"w1": "canon:FancyWidget"}` says "widget w1 is a
                // FancyWidget", exactly as the object form's `{"typeURI": ...}`
                // does. The dict key is the designator whenever the key slot
                // and the designator slot are one — the asset360
                // `PositioningSystemCoordinate` shape. This arm otherwise
                // hardwires the slot's range class, so without selecting here
                // canonicalisation would rewrite that designator to the *range*
                // class's value and warn about data that was right — the
                // misclassification spec rule 2 exists to prevent. Selection
                // stays scoped to that one question: a compact entry that names
                // no designator at all names no class and keeps the range class.
                let mut named = serde_json::Map::new();
                named.insert(scalar_slot.name.clone(), other.clone());
                if let Some(ks) = &key_slot {
                    named.insert(ks.name.clone(), Self::dict_key_value(entry_key));
                }
                let entry_class = match range_cv.get_type_designator_slot() {
                    Some(td) if named.contains_key(&td.name) => {
                        Self::select_class(&named, &range_cv, sv, conv)
                    }
                    _ => range_cv.clone(),
                };
                let mut child_values = HashMap::new();
                child_values.insert(
                    scalar_slot.name.clone(),
                    LinkMLInstance::Scalar {
                        node_id: new_node_id(),
                        value: Self::coerce_scalar_to_range(other, Some(&scalar_slot)),
                        slot: scalar_slot.clone(),
                        class: Some(entry_class.clone()),
                        sv: sv.clone(),
                    },
                );
                // Rule 5: the compact form has no payload for the key slot at
                // all, so the dict key is the only thing that can fill it —
                // there is nothing here to diverge from.
                if let Some(ks) = &key_slot {
                    child_values.insert(
                        ks.name.clone(),
                        LinkMLInstance::Scalar {
                            node_id: new_node_id(),
                            value: Self::dict_key_value(entry_key),
                            slot: ks.clone(),
                            class: Some(entry_class.clone()),
                            sv: sv.clone(),
                        },
                    );
                }
                run_object_constraints(
                    &entry_class,
                    &child_values,
                    &HashMap::new(),
                    path.clone(),
                    validation_issues,
                );
                Self::canonicalize_type_designator(
                    &mut child_values,
                    &entry_class,
                    conv,
                    &path,
                    validation_issues,
                );
                Self::populate_type_designator(&mut child_values, &entry_class, sv, conv);
                Ok(LinkMLInstance::Object {
                    node_id: new_node_id(),
                    values: child_values,
                    class: entry_class,
                    sv: sv.clone(),
                    unknown_fields: HashMap::new(),
                })
            }
        }
    }
}

pub fn load_yaml_file(
    path: &Path,
    sv: &SchemaView,
    class: &ClassView,
    conv: &Converter,
) -> std::result::Result<LoadResult, Box<dyn std::error::Error>> {
    let text = fs::read_to_string(path)?;
    load_yaml_str(&text, sv, class, conv)
}

pub fn load_yaml_str(
    data: &str,
    sv: &SchemaView,
    class: &ClassView,
    conv: &Converter,
) -> std::result::Result<LoadResult, Box<dyn std::error::Error>> {
    let value: serde_yaml::Value = serde_yaml::from_str(data)?;
    let json = serde_json::to_value(value)?;
    Ok(LinkMLInstance::from_json(
        json,
        class.clone(),
        None,
        sv,
        conv,
        false,
    ))
}

pub fn load_json_file(
    path: &Path,
    sv: &SchemaView,
    class: &ClassView,
    conv: &Converter,
) -> std::result::Result<LoadResult, Box<dyn std::error::Error>> {
    let text = fs::read_to_string(path)?;
    load_json_str(&text, sv, class, conv)
}

pub fn load_json_str(
    data: &str,
    sv: &SchemaView,
    class: &ClassView,
    conv: &Converter,
) -> std::result::Result<LoadResult, Box<dyn std::error::Error>> {
    let value: JsonValue = serde_json::from_str(data)?;
    Ok(LinkMLInstance::from_json(
        value,
        class.clone(),
        None,
        sv,
        conv,
        false,
    ))
}

fn collect_validation_issues(
    value: &LinkMLInstance,
    path: &mut Vec<String>,
    sink: &mut ValidationResultSink,
) {
    match value {
        LinkMLInstance::Scalar {
            value: jv,
            slot,
            class,
            ..
        } => {
            run_slot_constraints(class.as_ref(), slot, jv, path.clone(), sink);
        }
        LinkMLInstance::Null { slot, class, .. } => {
            let null_value = JsonValue::Null;
            run_slot_constraints(class.as_ref(), slot, &null_value, path.clone(), sink);
        }
        LinkMLInstance::List { values, .. } => {
            for (idx, child) in values.iter().enumerate() {
                path.push(idx.to_string());
                collect_validation_issues(child, path, sink);
                path.pop();
            }
        }
        LinkMLInstance::Mapping { values, .. } => {
            for (key, child) in values {
                path.push(key.clone());
                collect_validation_issues(child, path, sink);
                path.pop();
            }
        }
        LinkMLInstance::Object {
            values,
            class,
            unknown_fields,
            ..
        } => {
            run_object_constraints(class, values, unknown_fields, path.clone(), sink);
            for (key, child) in values {
                path.push(key.clone());
                collect_validation_issues(child, path, sink);
                path.pop();
            }
        }
    }
}

pub fn validate_issues(value: &LinkMLInstance) -> Vec<ValidationResult> {
    let mut sink = ValidationResultSink::default();
    let mut path = Vec::new();
    collect_validation_issues(value, &mut path, &mut sink);
    sink.into_vec()
}

pub fn validate(value: &LinkMLInstance) -> std::result::Result<(), String> {
    let validation_issues = validate_issues(value);
    match validation_issues.iter().find(|d| d.severity.is_error()) {
        Some(diag) => Err(diag.detail.clone()),
        None => Ok(()),
    }
}
