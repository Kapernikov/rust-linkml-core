//! Blame tracking helpers for LinkML patch operations.
//!
//! This module provides utilities to maintain a last-writer-wins blame map while
//! applying LinkML deltas. Callers can either rely on [`patch_with_blame`] to run
//! a patch and record metadata automatically, or feed an existing [`PatchTrace`]
//! into [`record_blame_from_trace`] when they already applied the patch through
//! other means. Formatting helpers are provided to inspect blame maps in a
//! deterministic order or render them alongside a YAML-style view of the
//! instance tree.

use crate::diff::{patch, Delta, PatchOptions, PatchTrace};
use crate::{LResult, LinkMLInstance, NodeId};
use serde_json::Value as JsonValue;
use std::collections::{BTreeMap, HashMap};
use std::fmt;

/// Apply a patch and record blame metadata for all added and updated nodes.
///
/// The generic parameter `M` represents the caller-supplied metadata that
/// identifies the change being applied (for example a patch ID, author/date
/// tuple, or other provenance payload). Blame entries use a last-writer-wins
/// strategy: every node touched by the patch (added or updated according to the
/// [`PatchTrace`]) will have the supplied metadata cloned into the provided
/// `blame` map.
pub fn patch_with_blame<M: Clone>(
    value: &LinkMLInstance,
    deltas: &[Delta],
    options: PatchOptions,
    meta: M,
    blame: &mut HashMap<NodeId, M>,
) -> LResult<(LinkMLInstance, PatchTrace)> {
    let (updated, trace) = patch(value, deltas, options)?;
    record_blame_from_trace(&trace, meta, blame);
    Ok((updated, trace))
}

/// Record blame metadata for every node listed in a [`PatchTrace`].
///
/// `M` carries caller-defined metadata describing the change (patch hash, user
/// information, etc.). Nodes reported as `added` or `updated` receive the
/// supplied metadata. The most recent metadata always wins when the same node
/// is written multiple times.
pub fn record_blame_from_trace<M: Clone>(
    trace: &PatchTrace,
    meta: M,
    blame: &mut HashMap<NodeId, M>,
) {
    for node_id in &trace.deleted {
        blame.remove(node_id);
    }
    for node_id in trace.added.iter().chain(trace.updated.iter()) {
        blame.insert(*node_id, meta.clone());
    }
}

/// Convert a blame map into ordered `(path_segments, metadata)` tuples.
///
/// The metadata payload `M` is whatever the caller recorded when writing the
/// blame map (patch IDs, user information, etc.). The returned vector is sorted
/// lexicographically by the path components to offer a deterministic ordering
/// regardless of traversal order in the original instance.
pub fn blame_map_to_paths<M: Clone>(
    value: &LinkMLInstance,
    blame: &HashMap<NodeId, M>,
) -> Vec<(Vec<String>, M)> {
    fn collect_paths<M: Clone>(
        node: &LinkMLInstance,
        blame: &HashMap<NodeId, M>,
        path: &mut Vec<String>,
        out: &mut BTreeMap<Vec<String>, M>,
    ) {
        if let Some(meta) = blame.get(&node.node_id()) {
            out.insert(path.clone(), meta.clone());
        }

        match node {
            LinkMLInstance::Object { values, .. } | LinkMLInstance::Mapping { values, .. } => {
                let mut entries: Vec<_> = values.iter().collect();
                entries.sort_by(|(ka, _), (kb, _)| ka.cmp(kb));
                for (key, child) in entries {
                    path.push(key.clone());
                    collect_paths(child, blame, path, out);
                    path.pop();
                }
            }
            LinkMLInstance::List { values, .. } => {
                for (idx, child) in values.iter().enumerate() {
                    path.push(idx.to_string());
                    collect_paths(child, blame, path, out);
                    path.pop();
                }
            }
            LinkMLInstance::Scalar { .. } | LinkMLInstance::Null { .. } => {}
        }
    }

    let mut ordered = BTreeMap::new();
    collect_paths(value, blame, &mut Vec::new(), &mut ordered);
    ordered.into_iter().collect()
}

const META_COL_WIDTH: usize = 72;

fn scalar_to_string(value: &JsonValue) -> String {
    serde_json::to_string(value).unwrap_or_else(|_| "<unserializable>".into())
}

enum Marker {
    None,
    Dash,
}

fn meta_column<M>(meta: Option<&M>, format_meta: &impl Fn(&M) -> String) -> String {
    let mut text = meta.map(format_meta).unwrap_or_default();
    if text.len() > META_COL_WIDTH {
        text.truncate(META_COL_WIDTH);
    }
    format!("{text:<width$}", width = META_COL_WIDTH)
}

/// Render a blame map next to a YAML-like dump of the instance tree.
///
/// The metadata payload `M` is the caller-defined blame entry (patch hash,
/// author, etc.). The `format_meta` closure controls how metadata is
/// represented; callers can use this to inject custom columns or formatting.
pub fn format_blame_map_with<M>(
    value: &LinkMLInstance,
    blame: &HashMap<NodeId, M>,
    format_meta: impl Fn(&M) -> String,
) -> String {
    fn walk<M>(
        node: &LinkMLInstance,
        blame: &HashMap<NodeId, M>,
        indent: usize,
        key: Option<&str>,
        marker: Marker,
        lines: &mut Vec<String>,
        format_meta: &impl Fn(&M) -> String,
    ) {
        let meta = blame.get(&node.node_id());
        let meta_str = meta_column(meta, format_meta);
        let indent_str = "  ".repeat(indent);
        match node {
            LinkMLInstance::Scalar { value, .. } => {
                let val = scalar_to_string(value);
                let yaml_line = match (&marker, key) {
                    (Marker::None, Some(k)) => format!("{indent_str}{k}: {val}"),
                    (Marker::None, None) => format!("{indent_str}{val}"),
                    (Marker::Dash, Some(k)) => format!("{indent_str}- {k}: {val}"),
                    (Marker::Dash, None) => format!("{indent_str}- {val}"),
                };
                lines.push(format!("{meta_str} | {yaml_line}"));
            }
            LinkMLInstance::Null { .. } => {
                let yaml_line = match (&marker, key) {
                    (Marker::None, Some(k)) => format!("{indent_str}{k}: null"),
                    (Marker::None, None) => format!("{indent_str}null"),
                    (Marker::Dash, Some(k)) => format!("{indent_str}- {k}: null"),
                    (Marker::Dash, None) => format!("{indent_str}- null"),
                };
                lines.push(format!("{meta_str} | {yaml_line}"));
            }
            LinkMLInstance::Object { values, class, .. } => {
                let type_hint = format!(" ({})", class.name());
                let header = match (&marker, key) {
                    (Marker::None, Some(k)) => format!("{indent_str}{k}:{type_hint}"),
                    (Marker::None, None) => format!("{indent_str}<root>{type_hint}"),
                    (Marker::Dash, Some(k)) => format!("{indent_str}- {k}:{type_hint}"),
                    (Marker::Dash, None) => format!("{indent_str}-{type_hint}"),
                };
                lines.push(format!("{meta_str} | {header}"));
                let mut entries: Vec<_> = values.iter().collect();
                entries.sort_by(|(a, _), (b, _)| a.cmp(b));
                for (child_key, child_value) in entries {
                    walk(
                        child_value,
                        blame,
                        indent + 1,
                        Some(child_key),
                        Marker::None,
                        lines,
                        format_meta,
                    );
                }
            }
            LinkMLInstance::Mapping { values, .. } => {
                let header = match (&marker, key) {
                    (Marker::None, Some(k)) => format!("{indent_str}{k}:"),
                    (Marker::None, None) => format!("{indent_str}<mapping>"),
                    (Marker::Dash, Some(k)) => format!("{indent_str}- {k}:"),
                    (Marker::Dash, None) => format!("{indent_str}-"),
                };
                lines.push(format!("{meta_str} | {header}"));
                let mut entries: Vec<_> = values.iter().collect();
                entries.sort_by(|(a, _), (b, _)| a.cmp(b));
                for (child_key, child_value) in entries {
                    walk(
                        child_value,
                        blame,
                        indent + 1,
                        Some(child_key),
                        Marker::None,
                        lines,
                        format_meta,
                    );
                }
            }
            LinkMLInstance::List { values, .. } => {
                let header = match (&marker, key) {
                    (Marker::None, Some(k)) => format!("{indent_str}{k}:"),
                    (Marker::None, None) => format!("{indent_str}<list>"),
                    (Marker::Dash, Some(k)) => format!("{indent_str}- {k}:"),
                    (Marker::Dash, None) => format!("{indent_str}-"),
                };
                lines.push(format!("{meta_str} | {header}"));
                for child in values {
                    walk(
                        child,
                        blame,
                        indent + 1,
                        None,
                        Marker::Dash,
                        lines,
                        format_meta,
                    );
                }
            }
        }
    }

    let mut lines = Vec::new();
    walk(
        value,
        blame,
        0,
        None,
        Marker::None,
        &mut lines,
        &format_meta,
    );
    if lines.is_empty() {
        "<empty blame map>".to_string()
    } else {
        lines.join("\n")
    }
}

/// Convenience wrapper over [`format_blame_map_with`] that requires `Display`.
///
/// The metadata payload `M` must implement [`fmt::Display`]; each blame entry
/// will be formatted with `to_string()`.
pub fn format_blame_map<M: fmt::Display>(
    value: &LinkMLInstance,
    blame: &HashMap<NodeId, M>,
) -> String {
    format_blame_map_with(value, blame, |m| m.to_string())
}

/// Retrieve blame metadata for a given node, if present.
///
/// Returns the caller-defined metadata `M` that was recorded for the node
/// (e.g., the patch identifier responsible for the most recent change).
pub fn get_blame_info<'a, M>(
    value: &LinkMLInstance,
    blame: &'a HashMap<NodeId, M>,
) -> Option<&'a M> {
    blame.get(&value.node_id())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::diff::{diff, DiffOptions};
    use crate::{load_json_str, load_yaml_file};
    use linkml_schemaview::identifier::{converter_from_schema, Identifier};
    use linkml_schemaview::io::from_yaml;
    use linkml_schemaview::schemaview::SchemaView;
    use serde_json::{json, Value as JsonValue};
    use std::fmt;
    use std::path::PathBuf;

    #[derive(Clone, Debug, PartialEq, Eq)]
    struct DummyMeta(&'static str);

    impl fmt::Display for DummyMeta {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "meta:{}", self.0)
        }
    }

    fn data_path(name: &str) -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests")
            .join("data")
            .join(name)
    }

    fn load_person() -> (
        linkml_schemaview::converter::Converter,
        linkml_schemaview::schemaview::ClassView,
        LinkMLInstance,
    ) {
        let schema = from_yaml(&data_path("schema.yaml")).expect("schema should load");
        let mut sv = SchemaView::new();
        sv.add_schema(schema.clone()).unwrap();
        let conv = converter_from_schema(&schema);
        let class = sv
            .get_class(&Identifier::new("Person"), &conv)
            .unwrap()
            .expect("class not found");
        let person = load_yaml_file(&data_path("person_valid.yaml"), &sv, &class, &conv)
            .unwrap()
            .into_instance()
            .unwrap();
        (conv, class, person)
    }

    #[test]
    fn patch_with_blame_tracks_last_writer() {
        let (conv, class, base) = load_person();
        let mut blame = HashMap::new();

        let mut stage1_json = base.to_json();
        if let JsonValue::Object(ref mut obj) = stage1_json {
            obj.insert("age".to_string(), json!(42));
        }
        let stage1 = load_json_str(
            &serde_json::to_string(&stage1_json).unwrap(),
            base.schema_view(),
            &class,
            &conv,
        )
        .unwrap()
        .into_instance()
        .unwrap();
        let deltas_stage1 = diff(&base, &stage1, DiffOptions::default());
        let (value_after_stage1, trace1) = patch_with_blame(
            &base,
            &deltas_stage1,
            PatchOptions::default(),
            DummyMeta("stage1"),
            &mut blame,
        )
        .unwrap();
        assert!(trace1.failed.is_empty());

        // Second stage updates multiple slots using the same node ids
        let mut stage2_json = value_after_stage1.to_json();
        if let JsonValue::Object(ref mut obj) = stage2_json {
            obj.insert("age".to_string(), json!(50));
            obj.insert("name".to_string(), json!("Updated"));
        }
        let stage2 = load_json_str(
            &serde_json::to_string(&stage2_json).unwrap(),
            value_after_stage1.schema_view(),
            &class,
            &conv,
        )
        .unwrap()
        .into_instance()
        .unwrap();

        let deltas_stage2 = diff(
            &value_after_stage1,
            &stage2,
            DiffOptions {
                treat_missing_as_null: true,
                ..DiffOptions::default()
            },
        );
        let (final_value, trace2) = patch_with_blame(
            &value_after_stage1,
            &deltas_stage2,
            PatchOptions::default(),
            DummyMeta("stage2"),
            &mut blame,
        )
        .unwrap();
        assert!(trace2.failed.is_empty());

        let age_node = final_value.navigate_path(["age"]).expect("age present");
        let name_node = final_value.navigate_path(["name"]).expect("name present");

        assert_eq!(get_blame_info(age_node, &blame), Some(&DummyMeta("stage2")));
        assert_eq!(
            get_blame_info(name_node, &blame),
            Some(&DummyMeta("stage2"))
        );

        let paths = blame_map_to_paths(&final_value, &blame);
        assert!(paths.iter().any(|(path, meta)| {
            path == &vec![String::from("age")] && meta == &DummyMeta("stage2")
        }));

        let rendered = format_blame_map(&final_value, &blame);
        assert!(rendered.contains("meta:stage2"));
        assert!(rendered.contains("name: \"Updated\""));
    }

    #[test]
    fn removing_nodes_clears_blame_entries() {
        let (conv, class, base) = load_person();
        let mut blame = HashMap::new();

        let mut stage1_json = base.to_json();
        if let JsonValue::Object(ref mut obj) = stage1_json {
            obj.insert("age".to_string(), json!(42));
        }
        let stage1 = load_json_str(
            &serde_json::to_string(&stage1_json).unwrap(),
            base.schema_view(),
            &class,
            &conv,
        )
        .unwrap()
        .into_instance()
        .unwrap();

        let deltas_stage1 = diff(&base, &stage1, DiffOptions::default());
        let (value_after_stage1, trace1) = patch_with_blame(
            &base,
            &deltas_stage1,
            PatchOptions::default(),
            DummyMeta("stage1"),
            &mut blame,
        )
        .unwrap();
        assert!(trace1.failed.is_empty());

        let age_node = value_after_stage1
            .navigate_path(["age"])
            .expect("age present after update");
        let age_node_id = age_node.node_id();
        assert!(blame.contains_key(&age_node_id));

        let mut stage2_json = value_after_stage1.to_json();
        if let JsonValue::Object(ref mut obj) = stage2_json {
            obj.remove("age");
        }
        let stage2 = load_json_str(
            &serde_json::to_string(&stage2_json).unwrap(),
            value_after_stage1.schema_view(),
            &class,
            &conv,
        )
        .unwrap()
        .into_instance()
        .unwrap();

        let deltas_stage2 = diff(
            &value_after_stage1,
            &stage2,
            DiffOptions {
                treat_missing_as_null: true,
                ..DiffOptions::default()
            },
        );
        assert!(!deltas_stage2.is_empty());
        let (final_value, trace2) = patch_with_blame(
            &value_after_stage1,
            &deltas_stage2,
            PatchOptions::default(),
            DummyMeta("stage2"),
            &mut blame,
        )
        .unwrap();
        assert!(trace2.failed.is_empty());
        assert!(trace2.deleted.contains(&age_node_id));

        assert!(!blame.contains_key(&age_node_id));
        let paths = blame_map_to_paths(&final_value, &blame);
        let age_path = vec![String::from("age")];
        assert!(paths
            .iter()
            .filter(|(path, _)| path == &age_path)
            .all(|(_, meta)| meta == &DummyMeta("stage2")));
        if let Some(age_after) = final_value.navigate_path(["age"]) {
            assert_ne!(age_after.node_id(), age_node_id);
            assert_eq!(
                get_blame_info(age_after, &blame),
                Some(&DummyMeta("stage2"))
            );
        }
        let rendered = format_blame_map(&final_value, &blame);
        assert!(!rendered.contains("age: 42"));
    }

    #[test]
    fn parent_objects_marked_for_nested_identifier_change() {
        let schema = from_yaml(&data_path("personinfo.yaml")).expect("schema should load");
        let mut sv = SchemaView::new();
        sv.add_schema(schema.clone()).unwrap();
        let conv = converter_from_schema(&schema);
        let container = sv
            .get_class(&Identifier::new("Container"), &conv)
            .unwrap()
            .expect("container class");

        let base = load_yaml_file(
            &data_path("example_personinfo_data.yaml"),
            &sv,
            &container,
            &conv,
        )
        .expect("load base instance")
        .into_instance()
        .expect("invalid base data");

        let mut target_json = base.to_json();
        if let JsonValue::Object(ref mut root) = target_json {
            if let Some(JsonValue::Array(objects)) = root.get_mut("objects") {
                if let Some(JsonValue::Object(p2)) = objects.get_mut(2) {
                    if let Some(JsonValue::Array(mh)) = p2.get_mut("has_medical_history") {
                        if let Some(JsonValue::Object(ev0)) = mh.get_mut(0) {
                            if let Some(JsonValue::Object(diag)) = ev0.get_mut("diagnosis") {
                                diag.insert(
                                    "id".to_string(),
                                    JsonValue::String("CODE:D9999".to_string()),
                                );
                            }
                        }
                    }
                }
            }
        }

        let target = load_json_str(
            &serde_json::to_string(&target_json).unwrap(),
            &sv,
            &container,
            &conv,
        )
        .expect("load target instance")
        .into_instance()
        .expect("invalid target");

        let deltas = diff(&base, &target, DiffOptions::default());
        println!("deltas = {:#?}", deltas);

        let mut blame = HashMap::new();
        let (_patched, trace) = patch_with_blame(
            &base,
            &deltas,
            PatchOptions::default(),
            DummyMeta("change"),
            &mut blame,
        )
        .expect("patch should succeed");
        println!("trace = {:?}", trace);
        println!("blame map\n{}", format_blame_map(&_patched, &blame));

        let event_node = _patched
            .navigate_path(["objects", "P:002", "has_medical_history", "0"])
            .expect("event node present");
        assert!(
            !blame.contains_key(&event_node.node_id()),
            "parent event should not be blamed"
        );
    }

    #[test]
    fn format_with_custom_meta() {
        let (_conv, _class, base) = load_person();
        let mut blame = HashMap::new();
        blame.insert(base.node_id(), DummyMeta("root"));

        let rendered = format_blame_map_with(&base, &blame, |meta| format!("tag={}", meta.0));
        assert!(rendered.contains("tag=root"));

        let empty_render = format_blame_map_with(
            &base,
            &HashMap::<NodeId, DummyMeta>::new(),
            |_| unreachable!(),
        );
        assert!(empty_render.contains("<root>"));
        assert!(!empty_render.contains("tag="));
    }
}
