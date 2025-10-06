use crate::{LResult, LinkMLInstance, NodeId};
use linkml_schemaview::schemaview::SlotView;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

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
/// - X -> null: update to null (old = X, new = null).
/// - null -> X: update from null (old = null, new = X).
/// - missing -> X: add (old = None, new = X).
/// - X -> missing: ignored by default; if `treat_missing_as_null` is true, update to null (old = X, new = null).
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

// Removed thin wrappers; call LinkMLInstance builders directly at call sites.

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
        return match op {
            DeltaOp::Add => {
                let v = newv.unwrap_or_else(|| &JsonValue::Null);
                let (class_opt, slot_opt) = match current {
                    LinkMLInstance::Object { class, .. } => (Some(class.clone()), None),
                    LinkMLInstance::List { class, slot, .. } => (class.clone(), Some(slot.clone())),
                    LinkMLInstance::Mapping { class, slot, .. }
                    | LinkMLInstance::Scalar { class, slot, .. }
                    | LinkMLInstance::Null { class, slot, .. } => {
                        (class.clone(), Some(slot.clone()))
                    }
                };
                if let Some(cls) = class_opt {
                    let conv = schema_view.converter();
                    let new_node = LinkMLInstance::from_json(
                        v.clone(),
                        cls,
                        slot_opt,
                        &schema_view,
                        &conv,
                        false,
                    )?;
                    mark_added_subtree(&new_node, trace);
                    *current = new_node;
                    Ok(true)
                } else {
                    Ok(false)
                }
            }
            DeltaOp::Remove => Ok(false),
            DeltaOp::Update => {
                if let Some(v) = newv {
                    let (class_opt, slot_opt) = match current {
                        LinkMLInstance::Object { class, .. } => (Some(class.clone()), None),
                        LinkMLInstance::List { class, slot, .. }
                        | LinkMLInstance::Mapping { class, slot, .. }
                        | LinkMLInstance::Scalar { class, slot, .. }
                        | LinkMLInstance::Null { class, slot, .. } => {
                            (class.clone(), Some(slot.clone()))
                        }
                    };
                    if let Some(cls) = class_opt {
                        let conv = schema_view.converter();
                        let new_node = LinkMLInstance::from_json(
                            v.clone(),
                            cls,
                            slot_opt,
                            &schema_view,
                            &conv,
                            false,
                        )?;
                        if opts.ignore_no_ops
                            && current.equals(&new_node, opts.treat_missing_as_null)
                        {
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
        };
    }

    match current {
        LinkMLInstance::Object { values, class, .. } => {
            let key = &path[0];
            if path.len() == 1 {
                return match op {
                    DeltaOp::Add | DeltaOp::Update => {
                        let v = newv.unwrap_or_else(|| &JsonValue::Null);
                        let conv = schema_view.converter();
                        let slot = class.slots().iter().find(|s| s.name == *key).cloned();
                        let new_child = LinkMLInstance::from_json(
                            v.clone(),
                            class.clone(),
                            slot.clone(),
                            &schema_view,
                            &conv,
                            false,
                        )?;
                        if let Some(old_child) = values.get_mut(key) {
                            if opts.ignore_no_ops
                                && old_child.equals(&new_child, opts.treat_missing_as_null)
                            {
                                return Ok(true);
                            }
                            match (&mut *old_child, &new_child) {
                                (
                                    LinkMLInstance::Scalar { value: ov, .. },
                                    LinkMLInstance::Scalar { value: nv, .. },
                                ) if !v.is_object() && !v.is_array() => {
                                    *ov = nv.clone();
                                    trace.updated.push(old_child.node_id());
                                }
                                _ => {
                                    let old_snapshot = std::mem::replace(old_child, new_child);
                                    mark_deleted_subtree(&old_snapshot, trace);
                                    mark_added_subtree(old_child, trace);
                                    trace.updated.push(current.node_id());
                                }
                            }
                        } else {
                            if opts.ignore_no_ops
                                && opts.treat_missing_as_null
                                && matches!(new_child, LinkMLInstance::Null { .. })
                            {
                                return Ok(true);
                            }
                            mark_added_subtree(&new_child, trace);
                            values.insert(key.clone(), new_child);
                            trace.updated.push(current.node_id());
                        }
                        Ok(true)
                    }
                    DeltaOp::Remove => {
                        if let Some(old_child) = values.get(key) {
                            if opts.ignore_no_ops
                                && opts.treat_missing_as_null
                                && matches!(old_child, LinkMLInstance::Null { .. })
                            {
                                return Ok(true);
                            }
                        }
                        if let Some(old_child) = values.remove(key) {
                            mark_deleted_subtree(&old_child, trace);
                            trace.updated.push(current.node_id());
                            Ok(true)
                        } else {
                            Ok(false)
                        }
                    }
                };
            }
            if let Some(child) = values.get_mut(key) {
                return apply_delta_linkml_inner(child, &path[1..], op, newv, trace, opts);
            }
            Ok(false)
        }
        LinkMLInstance::Mapping { values, slot, .. } => {
            let key = &path[0];
            if path.len() == 1 {
                return match op {
                    DeltaOp::Add | DeltaOp::Update => {
                        let v = newv.unwrap_or_else(|| &JsonValue::Null);
                        let conv = schema_view.converter();
                        let new_child = LinkMLInstance::build_mapping_entry_for_slot(
                            slot,
                            v.clone(),
                            &schema_view,
                            &conv,
                            Vec::new(),
                        )?;
                        if let Some(old_child) = values.get(key) {
                            if opts.ignore_no_ops
                                && old_child.equals(&new_child, opts.treat_missing_as_null)
                            {
                                return Ok(true);
                            }
                            mark_deleted_subtree(old_child, trace);
                        }
                        mark_added_subtree(&new_child, trace);
                        values.insert(key.clone(), new_child);
                        trace.updated.push(current.node_id());
                        Ok(true)
                    }
                    DeltaOp::Remove => {
                        if let Some(old_child) = values.remove(key) {
                            mark_deleted_subtree(&old_child, trace);
                            trace.updated.push(current.node_id());
                            Ok(true)
                        } else {
                            Ok(false)
                        }
                    }
                };
            }
            if let Some(child) = values.get_mut(key) {
                return apply_delta_linkml_inner(child, &path[1..], op, newv, trace, opts);
            }
            Ok(false)
        }
        LinkMLInstance::List {
            values,
            slot,
            class,
            ..
        } => {
            let key = &path[0];
            let idx_opt = key
                .parse::<usize>()
                .ok()
                .filter(|i| *i < values.len())
                .or_else(|| {
                    values.iter().enumerate().find_map(|(i, v)| {
                        if let LinkMLInstance::Object {
                            values: mv, class, ..
                        } = v
                        {
                            class
                                .key_or_identifier_slot()
                                .and_then(|id_slot| mv.get(&id_slot.name))
                                .and_then(|child| match child {
                                    LinkMLInstance::Scalar { value, .. } => Some(match value {
                                        serde_json::Value::String(s) => s.clone(),
                                        other => other.to_string(),
                                    }),
                                    _ => None,
                                })
                                .and_then(|s| if &s == key { Some(i) } else { None })
                        } else {
                            None
                        }
                    })
                });
            if path.len() == 1 {
                return match op {
                    DeltaOp::Add | DeltaOp::Update => {
                        let v = newv.unwrap_or_else(|| &JsonValue::Null);
                        if let Some(idx) = idx_opt.filter(|i| *i < values.len()) {
                            let conv = schema_view.converter();
                            let new_child = LinkMLInstance::build_list_item_for_slot(
                                slot,
                                class.as_ref(),
                                v.clone(),
                                &schema_view,
                                &conv,
                                Vec::new(),
                            )?;
                            if opts.ignore_no_ops
                                && values[idx].equals(&new_child, opts.treat_missing_as_null)
                            {
                                return Ok(true);
                            }
                            match (&mut values[idx], &new_child) {
                                (
                                    LinkMLInstance::Scalar { value: ov, .. },
                                    LinkMLInstance::Scalar { value: nv, .. },
                                ) if !v.is_object() && !v.is_array() => {
                                    *ov = nv.clone();
                                    trace.updated.push(values[idx].node_id());
                                }
                                _ => {
                                    let old = std::mem::replace(&mut values[idx], new_child);
                                    mark_deleted_subtree(&old, trace);
                                    mark_added_subtree(&values[idx], trace);
                                    trace.updated.push(current.node_id());
                                }
                            }
                        } else {
                            let conv = schema_view.converter();
                            let new_child = LinkMLInstance::build_list_item_for_slot(
                                slot,
                                class.as_ref(),
                                v.clone(),
                                &schema_view,
                                &conv,
                                Vec::new(),
                            )?;
                            mark_added_subtree(&new_child, trace);
                            values.push(new_child);
                            trace.updated.push(current.node_id());
                        }
                        Ok(true)
                    }
                    DeltaOp::Remove => {
                        if let Some(idx) = idx_opt.filter(|i| *i < values.len()) {
                            let old = values.remove(idx);
                            mark_deleted_subtree(&old, trace);
                            trace.updated.push(current.node_id());
                            Ok(true)
                        } else {
                            Ok(false)
                        }
                    }
                };
            }
            if let Some(idx) = idx_opt.filter(|i| *i < values.len()) {
                return apply_delta_linkml_inner(
                    &mut values[idx],
                    &path[1..],
                    op,
                    newv,
                    trace,
                    opts,
                );
            }
            Ok(false)
        }
        LinkMLInstance::Scalar { .. } => Ok(false),
        LinkMLInstance::Null { .. } => Ok(false),
    }
}
