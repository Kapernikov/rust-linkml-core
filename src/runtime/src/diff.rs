use crate::{LResult, LinkMLInstance, NodeId, ValidationResultSink};
use linkml_schemaview::{
    converter::Converter,
    schemaview::{ClassView, SchemaView, SlotView},
};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::collections::hash_map::Entry;

const IGNORE_ANNOTATION: &str = "diff.linkml.io/ignore";

fn slot_is_ignored(slot: &SlotView) -> bool {
    if slot.definitions().is_empty() {
        return false;
    }
    slot.definition()
        .annotations
        .as_ref()
        .map(|a| a.contains_key(IGNORE_ANNOTATION))
        .unwrap_or(false)
}

/// Operation applied by a [`Delta`].
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DeltaOp {
    /// Insert a new value at the given path.
    Add,
    /// Remove the value at the given path (produces a missing entry).
    Remove,
    /// Update an existing value, including transitions to `null`.
    Update,
}

/// Semantic delta emitted by [`diff`] and consumed by [`patch`].
///
/// The `path` identifies the location within the instance tree. Each segment is a
/// slot name, mapping key, list index, or (when available) the identifier/key slot
/// value for inlined objects in lists.
///
/// Operations are expressed jointly via [`Delta::op`], `old`, and `new`:
///
/// | `op` | `old` | `new` | Description |
/// | --- | --- | --- | --- |
/// | `Add` | `None` | `Some(value)` | Insert `value` into a list/mapping/object slot |
/// | `Remove` | `Some(value)` | `None` | Remove the addressed entry (value becomes missing) |
/// | `Update` | `Some(before)` | `Some(after)` | Replace an existing value; `after` may be `JsonValue::Null` |
///
/// Consumers that need additional semantics (e.g. fuzzy patching) can rely on the
/// explicit `op` instead of inferring behaviour from the optional payloads.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Delta {
    pub path: Vec<String>,
    pub op: DeltaOp,
    pub old: Option<JsonValue>,
    pub new: Option<JsonValue>,
}

#[derive(Clone, Copy, Debug)]
pub struct DiffOptions {
    /// When `false` (the default), entries present in the source but absent in
    /// the target are silently ignored for **object slots** and **mapping keys**.
    /// This supports partial-update semantics where the target only supplies the
    /// fields / keys it cares about.
    ///
    /// When `true`, every absent entry is treated as an explicit removal:
    /// - Object slots produce an `Update` delta with `new = null`.
    /// - Mapping keys produce a `Remove` delta.
    ///
    /// **Lists** are always treated as complete regardless of this flag: a
    /// shorter target list produces `Remove` deltas for the trailing source
    /// elements.
    ///
    /// **Note:** detecting a mapping key *rename* (delete old key + add new key)
    /// requires `treat_missing_as_null = true`, because the old key is absent
    /// from the target and would otherwise be silently ignored.
    pub treat_missing_as_null: bool,
    pub treat_changed_identifier_as_new_object: bool,
}

impl Default for DiffOptions {
    fn default() -> Self {
        Self {
            treat_missing_as_null: false,
            treat_changed_identifier_as_new_object: true,
        }
    }
}

/// Compute a semantic diff between two LinkMLInstance trees.
///
/// Semantics of nulls and missing values:
/// - X → null: `Update` (old = X, new = null).
/// - null → X: `Update` (old = null, new = X).
/// - missing → X: `Add` (old = None, new = X).
/// - X → missing (object slot): ignored by default; `Update` to null when `treat_missing_as_null`.
/// - X → missing (mapping key): ignored by default; `Remove` when `treat_missing_as_null`.
/// - X → missing (list element): always `Remove` (lists are positional/complete).
pub fn diff(source: &LinkMLInstance, target: &LinkMLInstance, opts: DiffOptions) -> Vec<Delta> {
    fn inner(
        path: &mut Vec<String>,
        slot: Option<&SlotView>,
        s: &LinkMLInstance,
        t: &LinkMLInstance,
        opts: DiffOptions,
        out: &mut Vec<Delta>,
    ) {
        if let Some(sl) = slot {
            if slot_is_ignored(sl) {
                return;
            }
        }
        match (s, t) {
            (
                LinkMLInstance::Object {
                    values: sm,
                    class: sc,
                    ..
                },
                LinkMLInstance::Object {
                    values: tm,
                    class: tc,
                    ..
                },
            ) => {
                // If objects have an identifier or key slot and it changed, treat as whole-object replacement
                // This applies for single-valued and list-valued inlined objects.
                if opts.treat_changed_identifier_as_new_object {
                    let key_slot_name = sc
                        .key_or_identifier_slot()
                        .or_else(|| tc.key_or_identifier_slot())
                        .map(|s| s.name.clone());
                    if let Some(ks) = key_slot_name {
                        let sid = sm.get(&ks);
                        let tid = tm.get(&ks);
                        if let (
                            Some(LinkMLInstance::Scalar { value: s_id, .. }),
                            Some(LinkMLInstance::Scalar { value: t_id, .. }),
                        ) = (sid, tid)
                        {
                            if s_id != t_id {
                                out.push(Delta {
                                    path: path.clone(),
                                    op: DeltaOp::Update,
                                    old: Some(s.to_json()),
                                    new: Some(t.to_json()),
                                });
                                return;
                            }
                        }
                    }
                }
                for (k, sv) in sm {
                    let slot_view = sc
                        .slots()
                        .iter()
                        .find(|s| s.name == *k)
                        .or_else(|| tc.slots().iter().find(|s| s.name == *k));
                    path.push(k.clone());
                    match tm.get(k) {
                        Some(tv) => inner(path, slot_view, sv, tv, opts, out),
                        None => {
                            if !slot_view.is_some_and(slot_is_ignored) {
                                // Missing target slot: either ignore (default) or treat as update to null
                                if opts.treat_missing_as_null {
                                    out.push(Delta {
                                        path: path.clone(),
                                        op: DeltaOp::Update,
                                        old: Some(sv.to_json()),
                                        new: Some(JsonValue::Null),
                                    });
                                }
                            }
                        }
                    }
                    path.pop();
                }
                for (k, tv) in tm {
                    if !sm.contains_key(k) {
                        let slot_view = sc
                            .slots()
                            .iter()
                            .find(|s| s.name == *k)
                            .or_else(|| tc.slots().iter().find(|s| s.name == *k));
                        if !slot_view.is_some_and(slot_is_ignored) {
                            path.push(k.clone());
                            out.push(Delta {
                                path: path.clone(),
                                op: DeltaOp::Add,
                                old: None,
                                new: Some(tv.to_json()),
                            });
                            path.pop();
                        }
                    }
                }
            }
            (LinkMLInstance::List { values: sl, .. }, LinkMLInstance::List { values: tl, .. }) => {
                // Prefer identifier-based addressing when possible, fall back to index
                let max_len = std::cmp::max(sl.len(), tl.len());
                for i in 0..max_len {
                    let label = |v: &LinkMLInstance| -> Option<String> {
                        if let LinkMLInstance::Object { values, class, .. } = v {
                            if let Some(id_slot) = class.key_or_identifier_slot() {
                                if let Some(LinkMLInstance::Scalar { value, .. }) =
                                    values.get(&id_slot.name)
                                {
                                    return match value {
                                        JsonValue::String(s) => Some(s.clone()),
                                        other => Some(other.to_string()),
                                    };
                                }
                            }
                        }
                        None
                    };
                    let step = if let Some(sv) = sl.get(i) {
                        label(sv)
                            .or_else(|| tl.get(i).and_then(label))
                            .unwrap_or_else(|| i.to_string())
                    } else {
                        tl.get(i).and_then(label).unwrap_or_else(|| i.to_string())
                    };
                    path.push(step);
                    match (sl.get(i), tl.get(i)) {
                        (Some(sv), Some(tv)) => inner(path, None, sv, tv, opts, out),
                        (Some(sv), None) => out.push(Delta {
                            path: path.clone(),
                            op: DeltaOp::Remove,
                            old: Some(sv.to_json()),
                            new: None,
                        }),
                        (None, Some(tv)) => out.push(Delta {
                            path: path.clone(),
                            op: DeltaOp::Add,
                            old: None,
                            new: Some(tv.to_json()),
                        }),
                        (None, None) => {}
                    }
                    path.pop();
                }
            }
            (
                LinkMLInstance::Mapping { values: sm, .. },
                LinkMLInstance::Mapping { values: tm, .. },
            ) => {
                use std::collections::BTreeSet;
                let keys: BTreeSet<_> = sm.keys().chain(tm.keys()).cloned().collect();
                for k in keys {
                    path.push(k.clone());
                    match (sm.get(&k), tm.get(&k)) {
                        (Some(sv), Some(tv)) => inner(path, None, sv, tv, opts, out),
                        (Some(sv), None) => {
                            if opts.treat_missing_as_null {
                                out.push(Delta {
                                    path: path.clone(),
                                    op: DeltaOp::Remove,
                                    old: Some(sv.to_json()),
                                    new: None,
                                });
                            }
                        }
                        (None, Some(tv)) => out.push(Delta {
                            path: path.clone(),
                            op: DeltaOp::Add,
                            old: None,
                            new: Some(tv.to_json()),
                        }),
                        (None, None) => {}
                    }
                    path.pop();
                }
            }
            (LinkMLInstance::Null { .. }, LinkMLInstance::Null { .. }) => {}
            (LinkMLInstance::Null { .. }, tv) => {
                out.push(Delta {
                    path: path.clone(),
                    op: DeltaOp::Update,
                    old: Some(JsonValue::Null),
                    new: Some(tv.to_json()),
                });
            }
            (sv, LinkMLInstance::Null { .. }) => {
                out.push(Delta {
                    path: path.clone(),
                    op: DeltaOp::Update,
                    old: Some(sv.to_json()),
                    new: Some(JsonValue::Null),
                });
            }
            (sv, tv) => {
                let sj = sv.to_json();
                let tj = tv.to_json();
                if sj != tj {
                    out.push(Delta {
                        path: path.clone(),
                        op: DeltaOp::Update,
                        old: Some(sj),
                        new: Some(tj),
                    });
                }
            }
        }
    }
    let mut out = Vec::new();
    inner(&mut Vec::new(), None, source, target, opts, &mut out);
    out
}

#[derive(Debug, Clone, Default)]
pub struct PatchTrace {
    /// Node IDs of subtrees that were newly created by the patch.
    ///
    /// See [`crate::NodeId`] for semantics: these are internal, ephemeral IDs
    /// that are useful for tooling and provenance, not object identifiers.
    pub added: Vec<NodeId>,
    /// Node IDs of subtrees that were removed by the patch.
    pub deleted: Vec<NodeId>,
    /// Node IDs of nodes that were directly updated (e.g., parent containers, scalars).
    pub updated: Vec<NodeId>,
    /// Paths of deltas that could not be applied (missing targets, etc.).
    pub failed: Vec<Vec<String>>,
}

#[derive(Clone, Copy, Debug)]
pub struct PatchOptions {
    pub ignore_no_ops: bool,
    pub treat_missing_as_null: bool,
}

impl Default for PatchOptions {
    fn default() -> Self {
        Self {
            ignore_no_ops: true,
            treat_missing_as_null: true,
        }
    }
}

pub fn patch(
    source: &LinkMLInstance,
    deltas: &[Delta],
    opts: PatchOptions,
) -> LResult<(LinkMLInstance, PatchTrace)> {
    let mut out = source.clone();
    let mut trace = PatchTrace::default();
    for d in deltas {
        let applied = apply_delta_linkml(&mut out, d, &mut trace, opts)?;
        if !applied {
            trace.failed.push(d.path.clone());
        }
    }
    Ok((out, trace))
}

fn collect_all_ids(value: &LinkMLInstance, ids: &mut Vec<NodeId>) {
    ids.push(value.node_id());
    match value {
        LinkMLInstance::Scalar { .. } => {}
        LinkMLInstance::Null { .. } => {}
        LinkMLInstance::List { values, .. } => {
            for v in values {
                collect_all_ids(v, ids);
            }
        }
        LinkMLInstance::Mapping { values, .. } | LinkMLInstance::Object { values, .. } => {
            for v in values.values() {
                collect_all_ids(v, ids);
            }
        }
    }
}

fn mark_added_subtree(v: &LinkMLInstance, trace: &mut PatchTrace) {
    collect_all_ids(v, &mut trace.added);
}

fn mark_deleted_subtree(v: &LinkMLInstance, trace: &mut PatchTrace) {
    collect_all_ids(v, &mut trace.deleted);
}

fn with_converter<F>(
    schema_view: &SchemaView,
    value: JsonValue,
    builder: F,
) -> LResult<LinkMLInstance>
where
    F: FnOnce(JsonValue, &SchemaView, &Converter) -> LResult<LinkMLInstance>,
{
    let conv = schema_view.converter();
    builder(value, schema_view, &conv)
}

fn current_class_and_slot(current: &LinkMLInstance) -> (Option<ClassView>, Option<SlotView>) {
    match current {
        LinkMLInstance::Object { class, .. } => (Some(class.clone()), None),
        LinkMLInstance::List { class, slot, .. }
        | LinkMLInstance::Mapping { class, slot, .. }
        | LinkMLInstance::Scalar { class, slot, .. }
        | LinkMLInstance::Null { class, slot, .. } => (class.clone(), Some(slot.clone())),
    }
}

fn should_skip_update(
    old: &LinkMLInstance,
    new_child: &LinkMLInstance,
    opts: PatchOptions,
) -> bool {
    opts.ignore_no_ops && old.equals(new_child, opts.treat_missing_as_null)
}

fn should_skip_add_null(new_child: &LinkMLInstance, opts: PatchOptions) -> bool {
    opts.ignore_no_ops
        && opts.treat_missing_as_null
        && matches!(new_child, LinkMLInstance::Null { .. })
}

fn should_skip_remove_null(old_child: &LinkMLInstance, opts: PatchOptions) -> bool {
    opts.ignore_no_ops
        && opts.treat_missing_as_null
        && matches!(old_child, LinkMLInstance::Null { .. })
}

fn replace_child_subtree(
    target: &mut LinkMLInstance,
    new_child: LinkMLInstance,
    parent_id: NodeId,
    trace: &mut PatchTrace,
    mark_parent: bool,
) {
    let old_snapshot = std::mem::replace(target, new_child);
    mark_deleted_subtree(&old_snapshot, trace);
    mark_added_subtree(target, trace);
    if mark_parent {
        trace.updated.push(parent_id);
    }
}

fn resolve_list_index(values: &[LinkMLInstance], key: &str) -> Option<usize> {
    if let Ok(idx) = key.parse::<usize>() {
        if idx < values.len() {
            return Some(idx);
        }
    }
    values.iter().enumerate().find_map(|(i, v)| {
        if let LinkMLInstance::Object {
            values: mv, class, ..
        } = v
        {
            class
                .key_or_identifier_slot()
                .and_then(|id_slot| mv.get(&id_slot.name))
                .and_then(|child| match child {
                    LinkMLInstance::Scalar { value, .. } => match value {
                        JsonValue::String(s) => (s == key).then_some(i),
                        other => {
                            let key_json = JsonValue::String(key.to_string());
                            (other == &key_json).then_some(i)
                        }
                    },
                    _ => None,
                })
        } else {
            None
        }
    })
}

fn try_update_scalar_in_place(
    existing: &mut LinkMLInstance,
    new_child: &LinkMLInstance,
    trace: &mut PatchTrace,
) -> bool {
    if let LinkMLInstance::Scalar {
        value: old_value,
        node_id,
        ..
    } = existing
    {
        if let LinkMLInstance::Scalar {
            value: new_value, ..
        } = new_child
        {
            *old_value = new_value.clone();
            trace.updated.push(*node_id);
            return true;
        }
    }
    false
}

#[derive(Clone, Copy)]
struct HashmapDeltaConfig {
    allow_scalar_in_place: bool,
    skip_add_null: bool,
    skip_remove_null: bool,
}

const OBJECT_DELTA_CONFIG: HashmapDeltaConfig = HashmapDeltaConfig {
    allow_scalar_in_place: true,
    skip_add_null: true,
    skip_remove_null: false,
};

const MAPPING_DELTA_CONFIG: HashmapDeltaConfig = HashmapDeltaConfig {
    allow_scalar_in_place: false,
    skip_add_null: false,
    skip_remove_null: true,
};

#[allow(clippy::too_many_arguments)]
fn apply_hashmap_leaf_delta<F>(
    values: &mut std::collections::HashMap<String, LinkMLInstance>,
    key: &str,
    owner_id: NodeId,
    trace: &mut PatchTrace,
    opts: PatchOptions,
    op: &DeltaOp,
    build_child: F,
    config: HashmapDeltaConfig,
) -> LResult<bool>
where
    F: FnOnce() -> LResult<LinkMLInstance>,
{
    match op {
        DeltaOp::Add | DeltaOp::Update => {
            let new_child = build_child()?;
            match values.entry(key.to_string()) {
                Entry::Occupied(mut entry) => {
                    let existing = entry.get_mut();
                    if should_skip_update(existing, &new_child, opts) {
                        return Ok(true);
                    }
                    if config.allow_scalar_in_place
                        && try_update_scalar_in_place(existing, &new_child, trace)
                    {
                        return Ok(true);
                    }
                    replace_child_subtree(existing, new_child, owner_id, trace, false);
                    Ok(true)
                }
                Entry::Vacant(entry) => {
                    if config.skip_add_null && should_skip_add_null(&new_child, opts) {
                        return Ok(true);
                    }
                    mark_added_subtree(&new_child, trace);
                    entry.insert(new_child);
                    trace.updated.push(owner_id);
                    Ok(true)
                }
            }
        }
        DeltaOp::Remove => {
            if let Some(old_child) = values.get(key) {
                if config.skip_remove_null && should_skip_remove_null(old_child, opts) {
                    return Ok(true);
                }
            }
            if let Some(old_child) = values.remove(key) {
                mark_deleted_subtree(&old_child, trace);
                trace.updated.push(owner_id);
                Ok(true)
            } else {
                Ok(false)
            }
        }
    }
}

fn apply_list_leaf_delta<F>(
    values: &mut Vec<LinkMLInstance>,
    idx_opt: Option<usize>,
    owner_id: NodeId,
    trace: &mut PatchTrace,
    opts: PatchOptions,
    op: &DeltaOp,
    build_child: F,
) -> LResult<bool>
where
    F: FnOnce() -> LResult<LinkMLInstance>,
{
    match op {
        DeltaOp::Add | DeltaOp::Update => {
            let new_child = build_child()?;
            if let Some(idx) = idx_opt {
                let existing = &mut values[idx];
                if should_skip_update(existing, &new_child, opts) {
                    return Ok(true);
                }
                if try_update_scalar_in_place(existing, &new_child, trace) {
                    return Ok(true);
                }
                replace_child_subtree(existing, new_child, owner_id, trace, false);
                Ok(true)
            } else {
                mark_added_subtree(&new_child, trace);
                values.push(new_child);
                trace.updated.push(owner_id);
                Ok(true)
            }
        }
        DeltaOp::Remove => {
            if let Some(idx) = idx_opt {
                let old_child = values.remove(idx);
                mark_deleted_subtree(&old_child, trace);
                trace.updated.push(owner_id);
                Ok(true)
            } else {
                Ok(false)
            }
        }
    }
}

fn apply_delta_linkml(
    current: &mut LinkMLInstance,
    delta: &Delta,
    trace: &mut PatchTrace,
    opts: PatchOptions,
) -> LResult<bool> {
    apply_delta_linkml_inner(
        current,
        &delta.path,
        &delta.op,
        delta.new.as_ref(),
        trace,
        opts,
    )
}

fn apply_delta_linkml_inner(
    current: &mut LinkMLInstance,
    path: &[String],
    op: &DeltaOp,
    newv: Option<&JsonValue>,
    trace: &mut PatchTrace,
    opts: PatchOptions,
) -> LResult<bool> {
    let schema_view = current.schema_view().clone();
    if path.is_empty() {
        return apply_delta_root(current, op, newv, trace, opts, &schema_view);
    }

    match current {
        LinkMLInstance::Object {
            values,
            class,
            node_id,
            ..
        } => apply_delta_object(
            values,
            class,
            *node_id,
            &schema_view,
            path,
            op,
            newv,
            trace,
            opts,
        ),
        LinkMLInstance::Mapping {
            values,
            slot,
            node_id,
            ..
        } => apply_delta_mapping(
            values,
            slot,
            *node_id,
            &schema_view,
            path,
            op,
            newv,
            trace,
            opts,
        ),
        LinkMLInstance::List {
            values,
            slot,
            class,
            node_id,
            ..
        } => apply_delta_list(
            values,
            slot,
            class,
            *node_id,
            &schema_view,
            path,
            op,
            newv,
            trace,
            opts,
        ),
        LinkMLInstance::Scalar { .. } | LinkMLInstance::Null { .. } => Ok(false),
    }
}

fn apply_delta_root(
    current: &mut LinkMLInstance,
    op: &DeltaOp,
    newv: Option<&JsonValue>,
    trace: &mut PatchTrace,
    opts: PatchOptions,
    schema_view: &SchemaView,
) -> LResult<bool> {
    match op {
        DeltaOp::Add => {
            let v = newv.cloned().unwrap_or(JsonValue::Null);
            let (class_opt, slot_opt) = current_class_and_slot(current);
            if let Some(cls) = class_opt {
                let slot_clone = slot_opt.clone();
                let new_node = with_converter(schema_view, v, move |value, sv, conv| {
                    LinkMLInstance::from_json(value, cls, slot_clone, sv, conv, false)
                        .into_instance_tolerate_errors()
                })?;
                mark_added_subtree(&new_node, trace);
                *current = new_node;
                Ok(true)
            } else {
                Ok(false)
            }
        }
        DeltaOp::Remove => Ok(false),
        DeltaOp::Update => {
            if let Some(v) = newv.cloned() {
                let (class_opt, slot_opt) = current_class_and_slot(current);
                if let Some(cls) = class_opt {
                    let slot_clone = slot_opt.clone();
                    let new_node = with_converter(schema_view, v, move |value, sv, conv| {
                        LinkMLInstance::from_json(value, cls, slot_clone, sv, conv, false)
                            .into_instance_tolerate_errors()
                    })?;
                    if should_skip_update(current, &new_node, opts) {
                        return Ok(true);
                    }
                    mark_deleted_subtree(current, trace);
                    mark_added_subtree(&new_node, trace);
                    *current = new_node;
                    return Ok(true);
                }
            }
            Ok(false)
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn apply_delta_object(
    values: &mut std::collections::HashMap<String, LinkMLInstance>,
    class: &ClassView,
    owner_id: NodeId,
    schema_view: &SchemaView,
    path: &[String],
    op: &DeltaOp,
    newv: Option<&JsonValue>,
    trace: &mut PatchTrace,
    opts: PatchOptions,
) -> LResult<bool> {
    let key = &path[0];
    if path.len() == 1 {
        let value = newv.cloned().unwrap_or(JsonValue::Null);
        let slot = class.slots().iter().find(|s| s.name == *key).cloned();
        let class_clone = class.clone();
        let slot_clone = slot.clone();
        return apply_hashmap_leaf_delta(
            values,
            key,
            owner_id,
            trace,
            opts,
            op,
            || {
                with_converter(schema_view, value, move |val, sv, conv| {
                    LinkMLInstance::from_json(val, class_clone, slot_clone, sv, conv, false)
                        .into_instance_tolerate_errors()
                })
            },
            OBJECT_DELTA_CONFIG,
        );
    }
    if let Some(child) = values.get_mut(key) {
        return apply_delta_linkml_inner(child, &path[1..], op, newv, trace, opts);
    }
    Ok(false)
}

#[allow(clippy::too_many_arguments)]
fn apply_delta_mapping(
    values: &mut std::collections::HashMap<String, LinkMLInstance>,
    slot: &SlotView,
    owner_id: NodeId,
    schema_view: &SchemaView,
    path: &[String],
    op: &DeltaOp,
    newv: Option<&JsonValue>,
    trace: &mut PatchTrace,
    opts: PatchOptions,
) -> LResult<bool> {
    let key = &path[0];
    if path.len() == 1 {
        let value = newv.cloned().unwrap_or(JsonValue::Null);
        let slot_clone = slot.clone();
        return apply_hashmap_leaf_delta(
            values,
            key,
            owner_id,
            trace,
            opts,
            op,
            || {
                with_converter(schema_view, value, move |val, sv, conv| {
                    let mut diags = ValidationResultSink::default();
                    let value = LinkMLInstance::build_mapping_entry_for_slot(
                        &slot_clone,
                        val,
                        sv,
                        conv,
                        Vec::new(),
                        &mut diags,
                    )?;
                    // Diagnostics recorded in `diags` are intentionally ignored here so patching
                    // can proceed even if validation failures were observed.
                    Ok(value)
                })
            },
            MAPPING_DELTA_CONFIG,
        );
    }
    if let Some(child) = values.get_mut(key) {
        return apply_delta_linkml_inner(child, &path[1..], op, newv, trace, opts);
    }
    Ok(false)
}

#[allow(clippy::too_many_arguments)]
fn apply_delta_list(
    values: &mut Vec<LinkMLInstance>,
    slot: &SlotView,
    class: &Option<ClassView>,
    owner_id: NodeId,
    schema_view: &SchemaView,
    path: &[String],
    op: &DeltaOp,
    newv: Option<&JsonValue>,
    trace: &mut PatchTrace,
    opts: PatchOptions,
) -> LResult<bool> {
    let key = &path[0];
    let idx_opt = resolve_list_index(values, key);
    if path.len() == 1 {
        let value = newv.cloned().unwrap_or(JsonValue::Null);
        let slot_clone = slot.clone();
        let class_clone = class.clone();
        return apply_list_leaf_delta(values, idx_opt, owner_id, trace, opts, op, || {
            with_converter(schema_view, value, move |val, sv, conv| {
                let mut diags = ValidationResultSink::default();
                let value = LinkMLInstance::build_list_item_for_slot(
                    &slot_clone,
                    class_clone.as_ref(),
                    val,
                    sv,
                    conv,
                    Vec::new(),
                    &mut diags,
                )?;
                // Diagnostics recorded in `diags` are intentionally ignored here so patching
                // can proceed even if validation failures were observed.
                Ok(value)
            })
        });
    }
    if let Some(idx) = idx_opt {
        return apply_delta_linkml_inner(&mut values[idx], &path[1..], op, newv, trace, opts);
    }
    Ok(false)
}
