//! Serializable snapshots for `SchemaView`.
//!
//! A snapshot captures the entire state needed to rebuild a [`SchemaView`](crate::schemaview::SchemaView)
//! without re-resolving imports. This is useful when computing an expanded schema on the backend and
//! transporting it to clients (Python, WebAssembly, etc.) that should operate on an identical view.

use serde::{Deserialize, Serialize};

use linkml_meta::SchemaDefinition;

/// Incremented when the [`SchemaViewSnapshot`] contract changes.
pub const SCHEMAVIEW_SNAPSHOT_VERSION: u32 = 1;

/// Serializable representation of a [`SchemaView`].
///
/// Snapshots are self-contained: every schema definition, resolved import mapping, and the
/// primary-schema pointer are serialized so that `SchemaView::from_snapshot` can rebuild an
/// equivalent view without fetching remote documents or repeating resolution logic.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaViewSnapshot {
    /// Format version to ease future migrations.
    pub format_version: u32,
    /// Optional identifier of the primary schema in this view.
    pub primary_schema: Option<String>,
    /// All schema definitions currently loaded in the view.
    pub schemas: Vec<SchemaEntry>,
    /// Mapping of import references that were resolved when building the view.
    pub resolved_imports: Vec<ResolvedImport>,
}

/// A single schema definition entry stored in the snapshot.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaEntry {
    pub schema_id: String,
    pub definition: SchemaDefinition,
}

/// Captures a resolved import for later reconstruction.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResolvedImport {
    pub importer_schema_id: String,
    pub import_reference: String,
    pub resolved_schema_id: String,
}

impl Default for SchemaViewSnapshot {
    fn default() -> Self {
        Self {
            format_version: SCHEMAVIEW_SNAPSHOT_VERSION,
            primary_schema: None,
            schemas: Vec::new(),
            resolved_imports: Vec::new(),
        }
    }
}
